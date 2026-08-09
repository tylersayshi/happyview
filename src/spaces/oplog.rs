use crate::db::{DatabaseBackend, adapt_sql};
use crate::error::AppError;
use crate::spaces::types::{OplogAction, OplogEntry};

/// How long a repo's ops are kept before `spawn_retention_cleanup` drops them.
///
/// This is the window in which a client can be offline and still catch up from
/// its cursor. Past it, `list_repo_ops` reports `reset` and the client takes a
/// fresh snapshot instead — correct, just more expensive, so the window only has
/// to cover ordinary absences rather than every possible one.
pub const RETENTION_DAYS: i64 = 30;

/// A position in one repo's op log: everything at or before `(rev, idx)` has
/// been delivered.
///
/// The `idx` half is load-bearing. Ops are ordered by `(rev, idx)` and served
/// under a `LIMIT`, so a page can end in the middle of a revision — an
/// `applyWrites` batch shares one revision across all of its ops. A cursor that
/// only carried `rev` would resume at the *next* revision and silently drop the
/// rest of that batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpCursor {
    pub rev: String,
    pub idx: i32,
}

impl OpCursor {
    /// Parse the wire form, `"<rev>:<idx>"`.
    ///
    /// A bare `"<rev>"` is also accepted and means "everything up to and
    /// including that revision", which is what a `rev`-only cursor meant before
    /// `idx` existed. Anything else is not a cursor.
    pub fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        match raw.split_once(':') {
            Some((rev, idx)) => Some(Self {
                rev: rev.to_string(),
                idx: idx.parse().ok()?,
            }),
            // `i32::MAX` rather than `-1`: the old form covered the whole
            // revision, so resuming must start after its last op.
            None => Some(Self {
                rev: raw.to_string(),
                idx: i32::MAX,
            }),
        }
    }

    pub fn format(rev: &str, idx: i32) -> String {
        format!("{rev}:{idx}")
    }

    pub fn to_wire(&self) -> String {
        Self::format(&self.rev, self.idx)
    }
}

pub async fn append_op(
    pool: &sqlx::AnyPool,
    backend: DatabaseBackend,
    entry: &OplogEntry,
) -> Result<(), AppError> {
    let sql = adapt_sql(
        "INSERT INTO happyview_space_record_oplog (id, space_id, author_did, rev, idx, action, collection, rkey, cid, prev, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        backend,
    );
    crate::db::query(&sql)
        .bind(&entry.id)
        .bind(&entry.space_id)
        .bind(&entry.author_did)
        .bind(&entry.rev)
        .bind(entry.idx)
        .bind(entry.action.as_str())
        .bind(&entry.collection)
        .bind(&entry.rkey)
        .bind(&entry.cid)
        .bind(&entry.prev)
        .bind(&entry.created_at)
        .execute(pool)
        .await
        .map_err(|e| AppError::Internal(format!("failed to append oplog entry: {e}")))?;
    Ok(())
}

/// The newest op in a repo, which is that repo's published head.
///
/// The head is read from the log rather than from the allocator's high-water
/// mark (`happyview_space_repo_state.rev`) on purpose: a client that sees a head
/// must be able to fetch every op up to it, and only the log can promise that.
pub async fn head(
    pool: &sqlx::AnyPool,
    backend: DatabaseBackend,
    space_id: &str,
    author_did: &str,
) -> Result<Option<OpCursor>, AppError> {
    let sql = adapt_sql(
        "SELECT rev, idx FROM happyview_space_record_oplog WHERE space_id = ? AND author_did = ? ORDER BY rev DESC, idx DESC LIMIT 1",
        backend,
    );
    let row: Option<(String, i32)> = crate::db::query_as(&sql)
        .bind(space_id)
        .bind(author_did)
        .fetch_optional(pool)
        .await
        .map_err(|e| AppError::Internal(format!("failed to read oplog head: {e}")))?;
    Ok(row.map(|(rev, idx)| OpCursor { rev, idx }))
}

/// The oldest op still retained for a repo. A cursor below this is a cursor we
/// can no longer serve — see `list_repo_ops`' `reset`.
pub async fn floor(
    pool: &sqlx::AnyPool,
    backend: DatabaseBackend,
    space_id: &str,
    author_did: &str,
) -> Result<Option<OpCursor>, AppError> {
    let sql = adapt_sql(
        "SELECT rev, idx FROM happyview_space_record_oplog WHERE space_id = ? AND author_did = ? ORDER BY rev ASC, idx ASC LIMIT 1",
        backend,
    );
    let row: Option<(String, i32)> = crate::db::query_as(&sql)
        .bind(space_id)
        .bind(author_did)
        .fetch_optional(pool)
        .await
        .map_err(|e| AppError::Internal(format!("failed to read oplog floor: {e}")))?;
    Ok(row.map(|(rev, idx)| OpCursor { rev, idx }))
}

/// Whether `cursor` still sits inside the retained window — i.e. whether the ops
/// immediately after it are ones we can still hand over.
///
/// A cursor at or after the floor is fine: everything newer than it survives.
/// A cursor below the floor may be missing ops that were pruned, and an empty
/// log can't vouch for any cursor at all. Both answers are "no", which the
/// caller turns into "take a snapshot".
pub async fn can_serve(
    pool: &sqlx::AnyPool,
    backend: DatabaseBackend,
    space_id: &str,
    author_did: &str,
    cursor: &OpCursor,
) -> Result<bool, AppError> {
    let Some(floor) = floor(pool, backend, space_id, author_did).await? else {
        return Ok(false);
    };
    Ok((cursor.rev.as_str(), cursor.idx) >= (floor.rev.as_str(), floor.idx))
}

/// SQL fragment restricting a query to ops strictly after a cursor. Ordering is
/// `(rev, idx)`, so "after" is the lexicographic tuple comparison spelled out —
/// no database we target supports row-value comparison on both backends.
fn after_cursor_sql(prefix: &str) -> String {
    format!("AND (({prefix}rev > ?) OR ({prefix}rev = ? AND {prefix}idx > ?)) ")
}

pub async fn list_ops(
    pool: &sqlx::AnyPool,
    backend: DatabaseBackend,
    space_id: &str,
    author_did: &str,
    cursor: Option<&OpCursor>,
    limit: i64,
) -> Result<Vec<OplogEntry>, AppError> {
    let mut sql = String::from(
        "SELECT id, space_id, author_did, rev, idx, action, collection, rkey, cid, prev, created_at FROM happyview_space_record_oplog WHERE space_id = ? AND author_did = ? ",
    );
    if cursor.is_some() {
        sql.push_str(&after_cursor_sql(""));
    }
    sql.push_str("ORDER BY rev, idx LIMIT ?");
    let sql = adapt_sql(&sql, backend);

    type OplogRow = (
        String,
        String,
        String,
        String,
        i32,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        String,
    );

    let mut query = crate::db::query_as::<OplogRow>(&sql)
        .bind(space_id)
        .bind(author_did);
    if let Some(c) = cursor {
        query = query.bind(&c.rev).bind(&c.rev).bind(c.idx);
    }
    query = query.bind(limit);

    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|e| AppError::Internal(format!("failed to list oplog entries: {e}")))?;

    rows.into_iter()
        .map(|r| {
            let action = OplogAction::parse(&r.5)
                .ok_or_else(|| AppError::Internal(format!("invalid oplog action: {}", r.5)))?;
            Ok(OplogEntry {
                id: r.0,
                space_id: r.1,
                author_did: r.2,
                rev: r.3,
                idx: r.4,
                action,
                collection: r.6,
                rkey: r.7,
                cid: r.8,
                prev: r.9,
                value: None,
                created_at: r.10,
            })
        })
        .collect()
}

/// As `list_ops`, but each op carries the record body it produced.
///
/// The join is on `cid`, so an op only carries a value while the record still
/// holds the exact version that op wrote. A superseded update therefore comes
/// back with `value: null` — the newer op in the same stream carries the
/// current body — and a delete never has one. Consumers collapse a page to the
/// last op per `(collection, rkey)` and read the value from that.
pub async fn list_ops_with_values(
    pool: &sqlx::AnyPool,
    backend: DatabaseBackend,
    space_id: &str,
    author_did: &str,
    cursor: Option<&OpCursor>,
    limit: i64,
) -> Result<Vec<OplogEntry>, AppError> {
    let mut sql = String::from(
        "SELECT o.id, o.space_id, o.author_did, o.rev, o.idx, o.action, o.collection, o.rkey, o.cid, o.prev, o.created_at, r.record FROM happyview_space_record_oplog o LEFT JOIN happyview_space_records r ON r.space_id = o.space_id AND r.author_did = o.author_did AND r.collection = o.collection AND r.rkey = o.rkey AND r.cid = o.cid WHERE o.space_id = ? AND o.author_did = ? ",
    );
    if cursor.is_some() {
        sql.push_str(&after_cursor_sql("o."));
    }
    sql.push_str("ORDER BY o.rev, o.idx LIMIT ?");
    let sql = adapt_sql(&sql, backend);

    type OplogWithValueRow = (
        String,
        String,
        String,
        String,
        i32,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
    );

    let mut query = crate::db::query_as::<OplogWithValueRow>(&sql)
        .bind(space_id)
        .bind(author_did);
    if let Some(c) = cursor {
        query = query.bind(&c.rev).bind(&c.rev).bind(c.idx);
    }
    query = query.bind(limit);

    let rows = query.fetch_all(pool).await.map_err(|e| {
        AppError::Internal(format!("failed to list oplog entries with values: {e}"))
    })?;

    rows.into_iter()
        .map(|r| {
            let action = OplogAction::parse(&r.5)
                .ok_or_else(|| AppError::Internal(format!("invalid oplog action: {}", r.5)))?;
            let value = r
                .11
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|e| AppError::Internal(format!("failed to parse record value: {e}")))?;
            Ok(OplogEntry {
                id: r.0,
                space_id: r.1,
                author_did: r.2,
                rev: r.3,
                idx: r.4,
                action,
                collection: r.6,
                rkey: r.7,
                cid: r.8,
                prev: r.9,
                value,
                created_at: r.10,
            })
        })
        .collect()
}

/// Drop ops older than `retention_days`, never taking a repo's newest op.
///
/// Keeping the last op matters: it is the anchor a caught-up client's cursor
/// points at. Prune it from an idle repo and every subsequent poll would see an
/// empty log, fail `can_serve`, and re-snapshot a repo that has not changed in
/// a month.
pub async fn prune(
    pool: &sqlx::AnyPool,
    backend: DatabaseBackend,
    retention_days: i64,
) -> Result<u64, AppError> {
    // The cutoff is computed here rather than in SQL so both backends compare
    // the same RFC3339 text `created_at` is written in.
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(retention_days)).to_rfc3339();
    let sql = adapt_sql(
        "DELETE FROM happyview_space_record_oplog WHERE created_at < ? AND rev < (SELECT MAX(o2.rev) FROM happyview_space_record_oplog o2 WHERE o2.space_id = happyview_space_record_oplog.space_id AND o2.author_did = happyview_space_record_oplog.author_did)",
        backend,
    );
    let result = crate::db::query(&sql)
        .bind(&cutoff)
        .execute(pool)
        .await
        .map_err(|e| AppError::Internal(format!("failed to prune oplog: {e}")))?;
    Ok(result.rows_affected())
}

/// Hourly oplog pruning, mirroring `event_log::spawn_retention_cleanup`.
pub async fn spawn_retention_cleanup(
    db: sqlx::AnyPool,
    retention_days: i64,
    backend: DatabaseBackend,
) {
    if retention_days <= 0 {
        tracing::info!("space oplog retention cleanup disabled");
        return;
    }

    tracing::info!(
        retention_days,
        "starting space oplog retention cleanup task"
    );
    let interval = tokio::time::Duration::from_secs(3600);

    loop {
        tokio::time::sleep(interval).await;
        match prune(&db, backend, retention_days).await {
            Ok(count) if count > 0 => tracing::info!(count, "pruned old space oplog entries"),
            Ok(_) => {}
            Err(e) => tracing::warn!("failed to prune space oplog: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_parses_the_wire_form() {
        let c = OpCursor::parse("3mslxeytuac2j:4").expect("parses");
        assert_eq!(c.rev, "3mslxeytuac2j");
        assert_eq!(c.idx, 4);
        assert_eq!(c.to_wire(), "3mslxeytuac2j:4");
    }

    #[test]
    fn cursor_treats_a_bare_rev_as_the_whole_revision() {
        let c = OpCursor::parse("3mslxeytuac2j").expect("parses");
        assert_eq!(c.idx, i32::MAX, "a rev-only cursor must skip the whole rev");
    }

    #[test]
    fn cursor_rejects_junk() {
        assert_eq!(OpCursor::parse(""), None);
        assert_eq!(OpCursor::parse("   "), None);
        assert_eq!(OpCursor::parse("rev:notanumber"), None);
    }
}

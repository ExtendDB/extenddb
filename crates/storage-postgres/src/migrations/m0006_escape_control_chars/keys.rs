// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Two rewrites of migration 6 that cannot be done by escaping a stored text
//! where it lies, because the text is derived from the key strings:
//!
//! - A partition key column of a key schema with more than one HASH attribute
//!   holds the netstring join of the parts (`<len>:<part>,` each), and the
//!   escape changes a part's length. [`rebuild_composite_rows`] splits each
//!   legacy value, escapes the parts and joins them again with the new
//!   lengths, through the function the write path uses
//!   (`key_text::join_escaped_parts`).
//! - `gsi_pending.worker_partition` is a hash of the stored base key text
//!   (`gsi_queue::partition_for`), so it changes with the key text.
//!   [`recompute_worker_partitions`] recomputes it with the function the
//!   enqueue uses (`gsi_queue::worker_partition`).
//!
//! Both run in the data body's transaction, after the table's other columns.

use extenddb_storage::migrations::MigrationError;
use sqlx::PgConnection;

use super::{db_error, rewrite_longest_first};
use crate::data::key_text::reescape_legacy_composite;
use crate::gsi_queue::{PendingApplyContext, worker_partition};

/// The `backup_items` rows whose `pk` holds a composite key: the rows of a
/// backup whose table's key schema has more than one HASH attribute. A boolean
/// SQL expression over a `backup_items` row; `backups` is in the same catalog
/// database.
pub(crate) const BACKUP_ITEMS_COMPOSITE_ROWS: &str = "backup_arn IN (\
     SELECT b.backup_arn FROM backups b WHERE (\
         SELECT COUNT(*) FROM jsonb_array_elements(b.key_schema) e \
         WHERE e->>'KeyType' = 'HASH') > 1)";

/// Whether a catalog key schema (`[{"AttributeName", "KeyType"}, ...]`) has
/// more than one HASH attribute, so its partition key column holds a netstring
/// join.
pub(crate) fn has_composite_partition_key(key_schema: &serde_json::Value) -> bool {
    key_schema.as_array().is_some_and(|elements| {
        elements
            .iter()
            .filter(|e| e.get("KeyType").and_then(serde_json::Value::as_str) == Some("HASH"))
            .count()
            > 1
    })
}

/// Rebuild the composite key text in `col` of the rows that `rows` (a boolean
/// SQL expression over the row) selects and that hold U+0001, one row at a
/// time, longest legacy text first and in batches ([`rewrite_longest_first`]).
/// A SQL `replace()` over the whole text would leave each length prefix
/// counting the raw part (`2:a\x01,1:b,` would become `2:a\x01\x02,1:b,`,
/// where this release writes and looks up `3:a\x01\x02,1:b,`). A value that is
/// not a netstring fails the body, naming the table and column (`what`), and
/// the store's transaction rolls back.
pub(crate) async fn rebuild_composite_rows(
    conn: &mut PgConnection,
    quoted_table: &str,
    col: &str,
    rows: &str,
    what: &str,
) -> Result<u64, MigrationError> {
    rewrite_longest_first(conn, quoted_table, col, rows, what, |legacy| {
        reescape_legacy_composite(legacy).ok_or_else(|| {
            MigrationError::Store(format!(
                "re-encode {what}: the composite key {legacy:?} is not a netstring"
            ))
        })
    })
    .await
}

/// The partition a row queued with these columns gets from this release's
/// enqueue, or `None` when a column does not decode (a worker handles such a
/// row as a poison row, whatever partition holds it).
fn current_partition(
    old_item: Option<serde_json::Value>,
    new_item: Option<serde_json::Value>,
    index_context: serde_json::Value,
) -> Option<i32> {
    let old_item = old_item.map(crate::data::json_to_item).transpose().ok()?;
    let new_item = new_item.map(crate::data::json_to_item).transpose().ok()?;
    let context: PendingApplyContext = crate::data::stored_json_to(index_context).ok()?;
    worker_partition(old_item.as_ref(), new_item.as_ref(), &context).ok()
}

/// Recompute `worker_partition` of the queued rows whose item holds U+0001,
/// after their jsonb columns were re-encoded in the same transaction.
///
/// A release without the escape hashed the raw base key; this release hashes
/// the escaped one, so for a key holding U+0001 a row queued before the
/// upgrade and the next write of the same item would be claimed by different
/// workers, in either order, which breaks the queue's one item, one worker
/// rule. Every row of one item moves to the same new partition and keeps its
/// `id` and `ready_at`, so the item's rows are still claimed in write order,
/// and a row enqueued after the upgrade gets a `ready_at` no earlier than
/// theirs from the partition clamp in `enqueue_gsi_pending`.
pub(crate) async fn recompute_worker_partitions(conn: &mut PgConnection) -> Result<u64, MigrationError> {
    type Row = (
        i64,
        i32,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
        serde_json::Value,
    );
    // '\u0001' is how jsonb::text renders U+0001; chr(92) spells the backslash
    // as in `escape_jsonb_rows`. The base key is in new_item, or in old_item for
    // a delete, as `worker_partition` reads it.
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT id, worker_partition, old_item, new_item, index_context FROM gsi_pending \
         WHERE position((chr(92) || 'u0001') IN COALESCE(new_item, old_item)::text) > 0 \
         ORDER BY id FOR UPDATE",
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| db_error("find the gsi_pending rows to repartition", &e))?;
    let mut changed: u64 = 0;
    for (id, stored, old_item, new_item, index_context) in rows {
        let Some(partition) = current_partition(old_item, new_item, index_context) else {
            continue;
        };
        if partition == stored {
            continue;
        }
        sqlx::query("UPDATE gsi_pending SET worker_partition = $1 WHERE id = $2")
            .bind(partition)
            .bind(id)
            .execute(&mut *conn)
            .await
            .map_err(|e| db_error(&format!("repartition gsi_pending row {id}"), &e))?;
        changed += 1;
    }
    Ok(changed)
}

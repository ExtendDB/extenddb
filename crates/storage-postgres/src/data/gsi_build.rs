// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Building a global secondary index on a table that is taking writes.
//!
//! Three pieces, and the order they run in is the design:
//!
//! 1. `UpdateTable` creates the index data table, then commits the catalog row as
//!    `CREATING`. From that commit on, every write that reads the catalog maintains
//!    the index the way it maintains an `ACTIVE` one, inline or through the queue.
//! 2. It waits [`GRACE_PUBLISH`], takes a [`write_barrier`], and runs
//!    [`backfill`], which writes the current state of every base item into the
//!    index under that item's row lock.
//! 3. It flips the row to `ACTIVE`. A build that fails or is cut short leaves the
//!    index `CREATING`, and [`recovery_worker`] finishes it.
//!
//! Every write to the index, from the backfill, the queue or a write's own inline
//! maintenance, leaves the rows for one base key equal to that item's state at the
//! time, and does so while holding the base row's lock. So the three can run in any
//! order against each other and the index still ends at the base table's state.
//!
//! What that does not cover is a write that read the catalog just before the
//! `CREATING` row committed. It does not know the index exists, so it maintains
//! nothing, and the catalog and the data tables are different databases, so no
//! transaction can tell it. [`WriteStamp`] and [`fix_up_stale_write`] close that
//! gap with a time bound:
//!
//! - A write whose base table statements had all returned within [`GRACE_WRITE`]
//!   of its catalog read holds the table's `ROW EXCLUSIVE` lock from the first of
//!   them until it commits. The barrier comes [`GRACE_PUBLISH`] (longer) after the
//!   catalog commit, so it finds that lock held, waits for the commit, and the
//!   backfill, which starts after it, sees the write. (A `SELECT ... FOR UPDATE`
//!   alone takes only `ROW SHARE`, which the barrier does not wait for; the check
//!   runs after the write's `INSERT`, `UPDATE` or `DELETE`, which is what counts.)
//! - A slower write re-reads the catalog before it commits and queues a repair for
//!   any index it did not know about. If the re-read does not show the index, the
//!   catalog commit came after it, so that write's lock was again taken before the
//!   barrier.
//!
//! Measurement error only ever makes a write look slower or the wait longer, which
//! is the safe direction: the clock is one that keeps counting while the host is
//! suspended, the writer measures to a point after its write, and `UpdateTable`
//! measures from after its commit returned.
//!
//! One gap is left. A write that needs no transaction (a plain put or delete on
//! a table it believed had no index) commits with its one statement, so if it
//! only becomes slow after that decision, its repair goes in a separate
//! transaction. A process that dies between the two, for a write that was over
//! [`GRACE_WRITE`] and raced an index creation, leaves that item out of the
//! index (or, for a delete, leaves its old row in) until the item is next
//! written. A write that is already slow when it decides takes the transaction
//! path instead, which closes most of this window.

use std::time::Duration;

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, Projection, ScalarAttributeType,
    TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, sk_column_n};
use sqlx::Row as _;

use super::index::{
    IndexMeta, db_error, delete_index_row_multi, fetch_indexes_for_table, insert_index_row_multi,
    project_item_for_index,
};
use super::{all_sort_key_info, data_table_name, index_table_name, json_to_item};
use crate::catalog_json::from_catalog_json;
use crate::gsi_queue::{GsiApplyContext, GsiIndexDef, PendingApplyContext, enqueue_gsi_pending};

/// How long a write may take, from just before its catalog read to just before
/// its commit, and still rely on the backfill to have seen it.
pub(crate) const GRACE_WRITE: Duration = Duration::from_millis(500);

/// How long `UpdateTable` waits after committing a `CREATING` index before the
/// barrier and backfill. Must exceed [`GRACE_WRITE`]; the margin covers the two
/// clocks (a writer's and this call's) disagreeing on how long half a second is,
/// which is the one error the monotonic clock does not remove.
pub(crate) const GRACE_PUBLISH: Duration = Duration::from_millis(1_000);

/// Base rows read and locked per backfill transaction.
///
/// A writer to any row in the batch waits for the batch to commit, so this bounds
/// that stall. Small next to the vector backfill's batch because the lock is the
/// point here.
const BACKFILL_BATCH: i64 = 256;

/// A point in time on a clock that keeps counting while the host is suspended.
///
/// `std::time::Instant` is `CLOCK_MONOTONIC` on Linux, which stops during suspend,
/// so a write could measure itself as fast when it was not.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WriteStamp(Option<Duration>);

impl WriteStamp {
    pub(crate) fn now() -> Self {
        Self(clock_now())
    }

    /// Time since [`WriteStamp::now`]. An unreadable clock reports as long ago,
    /// which sends the write down the repair path rather than trusting it.
    pub(crate) fn elapsed(self) -> Duration {
        match (self.0, clock_now()) {
            (Some(start), Some(now)) => now.saturating_sub(start),
            _ => Duration::MAX,
        }
    }
}

#[cfg(unix)]
fn clock_now() -> Option<Duration> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const CLOCK: libc::clockid_t = libc::CLOCK_BOOTTIME;
    // CLOCK_MONOTONIC counts across sleep on macOS and the BSDs.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    const CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;

    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable timespec for the duration of the call.
    let rc = unsafe { libc::clock_gettime(CLOCK, &mut ts) };
    if rc != 0 {
        return None;
    }
    Some(Duration::new(
        u64::try_from(ts.tv_sec).ok()?,
        u32::try_from(ts.tv_nsec).ok()?,
    ))
}

#[cfg(not(unix))]
fn clock_now() -> Option<Duration> {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    Some(START.get_or_init(std::time::Instant::now).elapsed())
}

/// Where a write pauses under [`pause_writes_for_tests`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub enum PauseWrites {
    /// Between the catalog read and the data work. A write paused here for longer
    /// than [`GRACE_WRITE`] takes the transaction path and repairs in-transaction
    /// (`fix_up_stale_write`).
    AfterIndexRead,
    /// Between the decision to write without a transaction and the statement. The
    /// write stays on the autocommit path and repairs afterwards
    /// (`fix_up_autocommit`), the one repair that is not atomic with the write.
    BeforeAutocommitStatement,
}

/// Test hooks: how long a write sleeps at each [`PauseWrites`] point. Zero
/// outside tests.
static PAUSE_AFTER_INDEX_READ_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static PAUSE_BEFORE_AUTOCOMMIT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Make every write on this process pause for `pause` at `at`, so a test can
/// deterministically produce a write that read the catalog before a GSI was
/// created and writes after its backfill finished: the case
/// [`fix_up_stale_write`] and `fix_up_autocommit` exist for, which no timing in a
/// test can otherwise hit reliably. Hidden because no deployment path calls it.
#[doc(hidden)]
pub fn pause_writes_for_tests(at: PauseWrites, pause: Duration) {
    let ms = u64::try_from(pause.as_millis()).unwrap_or(u64::MAX);
    let slot = match at {
        PauseWrites::AfterIndexRead => &PAUSE_AFTER_INDEX_READ_MS,
        PauseWrites::BeforeAutocommitStatement => &PAUSE_BEFORE_AUTOCOMMIT_MS,
    };
    slot.store(ms, std::sync::atomic::Ordering::Relaxed);
}

async fn pause(slot: &std::sync::atomic::AtomicU64) {
    let ms = slot.load(std::sync::atomic::Ordering::Relaxed);
    if ms > 0 {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

pub(crate) async fn pause_after_index_read() {
    pause(&PAUSE_AFTER_INDEX_READ_MS).await;
}

pub(crate) async fn pause_before_autocommit_statement() {
    pause(&PAUSE_BEFORE_AUTOCOMMIT_MS).await;
}

/// One typed key value, as stored in a base or index table's key column.
#[derive(Debug, Clone)]
enum KeyVal {
    S(String),
    N(sqlx::types::BigDecimal),
    B(Vec<u8>),
}

/// A base table primary key, in column order.
#[derive(Debug, Clone)]
struct BaseKey {
    pk: String,
    sks: Vec<KeyVal>,
}

/// The base table's key columns, `pk` then the typed sort key columns.
fn base_key_columns(base_sks: &[(&str, ScalarAttributeType)]) -> Vec<String> {
    let mut cols = vec!["pk".to_owned()];
    for (i, &(_, t)) in base_sks.iter().enumerate() {
        cols.push(sk_column_n(i, t));
    }
    cols
}

fn read_key(
    row: &sqlx::postgres::PgRow,
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<BaseKey, StorageError> {
    let pk: String = row.try_get("pk").map_err(db_error)?;
    let mut sks = Vec::with_capacity(base_sks.len());
    for (i, &(_, t)) in base_sks.iter().enumerate() {
        let col = sk_column_n(i, t);
        let v = match t {
            ScalarAttributeType::S => row.try_get::<String, _>(col.as_str()).map(KeyVal::S),
            ScalarAttributeType::N => row
                .try_get::<sqlx::types::BigDecimal, _>(col.as_str())
                .map(KeyVal::N),
            ScalarAttributeType::B => row.try_get::<Vec<u8>, _>(col.as_str()).map(KeyVal::B),
        }
        .map_err(db_error)?;
        sks.push(v);
    }
    Ok(BaseKey { pk, sks })
}

fn key_from_item(
    item: &Item,
    base_ks: &[KeySchemaElement],
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<BaseKey, StorageError> {
    let pk = composite_pk_to_text(item, base_ks)?;
    let mut sks = Vec::with_capacity(base_sks.len());
    for &(name, t) in base_sks {
        let v = item
            .get(name)
            .ok_or_else(|| StorageError::Internal(format!("missing sort key {name}")))?;
        sks.push(match parse_sk(v, t)? {
            extenddb_storage::util::SortKeyValue::S(s) => KeyVal::S(s),
            extenddb_storage::util::SortKeyValue::N(n) => KeyVal::N(n),
            extenddb_storage::util::SortKeyValue::B(b) => KeyVal::B(b),
        });
    }
    Ok(BaseKey { pk, sks })
}

fn bind_key<'q>(
    mut q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    key: &BaseKey,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    q = q.bind(key.pk.clone());
    for v in &key.sks {
        q = match v {
            KeyVal::S(s) => q.bind(s.clone()),
            KeyVal::N(n) => q.bind(n.clone()),
            KeyVal::B(b) => q.bind(b.clone()),
        };
    }
    q
}

/// `(c1, c2, ...)` over the given column names, with an optional prefix.
fn tuple(cols: &[String], prefix: &str) -> String {
    let parts: Vec<String> = cols.iter().map(|c| format!("{prefix}{c}")).collect();
    format!("({})", parts.join(", "))
}

/// `($start, $start+1, ...)` for `n` parameters.
fn params(start: usize, n: usize) -> String {
    let parts: Vec<String> = (start..start + n).map(|i| format!("${i}")).collect();
    format!("({})", parts.join(", "))
}

/// Whether an item belongs in the index: every key attribute present, of the
/// declared type, and not empty.
///
/// An item that fails is left out of the index rather than failing the build or
/// stalling a queue partition. It can only be an item written before the index
/// existed, or by a writer that had not yet seen the index's attribute
/// definitions; the service skips such items in a backfill too.
pub(crate) fn indexable(
    item: &Item,
    index_ks: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> bool {
    index_ks.iter().all(|ks| {
        let Some(value) = item.get(&ks.attribute_name) else {
            return false;
        };
        let declared = attr_defs
            .iter()
            .find(|ad| ad.attribute_name == ks.attribute_name)
            .map(|ad| ad.attribute_type);
        match (declared, value) {
            (Some(ScalarAttributeType::S), AttributeValue::S(s)) => !s.is_empty(),
            (Some(ScalarAttributeType::N), AttributeValue::N(n)) => {
                n.parse::<sqlx::types::BigDecimal>().is_ok()
            }
            (Some(ScalarAttributeType::B), AttributeValue::B(b)) => !b.is_empty(),
            _ => false,
        }
    })
}

/// Make one index's rows for one base item equal to `current`, the item's state
/// read under its row lock (or `None` if it has none).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_state(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    idx_table: &str,
    index_ks: &[KeySchemaElement],
    projection: &Projection,
    base_ks: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    base_sks: &[(&str, ScalarAttributeType)],
    current: &Item,
) -> Result<bool, StorageError> {
    // By base key, whatever index key the stale row carries.
    delete_index_row_multi(tx, idx_table, current, base_ks, base_sks).await?;
    if !indexable(current, index_ks, attr_defs) {
        return Ok(false);
    }
    let idx_sks = all_sort_key_info(index_ks, attr_defs);
    let projected = project_item_for_index(current, index_ks, base_ks, projection);
    insert_index_row_multi(
        tx, idx_table, current, &projected, index_ks, base_ks, &idx_sks, base_sks,
    )
    .await?;
    Ok(true)
}

/// Delete one base key's index rows, but only if the base row is absent.
///
/// Two statements, and the split is the point. A single `DELETE ... WHERE NOT
/// EXISTS (base row)` that has to wait on an index row a writer is updating
/// re-checks only that row once the writer commits, and evaluates `NOT EXISTS`
/// against its original snapshot: a writer that re-created the item and updated
/// the stale index row in place would have its fresh row deleted. So the rows are
/// locked first, which is where any wait happens, and the delete then runs with a
/// new snapshot over exactly the rows that are locked, by `ctid`. Nobody else can
/// change a locked row, so its `ctid` is stable and the delete waits on nothing;
/// a row some writer added after the lock is not among them and is left alone.
///
/// No writer in this crate updates an index row in place today: every GSI write
/// goes through `write_state`, which deletes by base key before it upserts, so
/// the single-statement form would not misbehave against them. The guard is for
/// the next writer that does not, which this function cannot see; it costs one
/// statement on a path taken only for an item that is gone.
async fn delete_if_absent(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    base_table: &str,
    idx_table: &str,
    base_cols: &[String],
    key: &BaseKey,
) -> Result<(), StorageError> {
    let idx_where: Vec<String> = base_cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("base_{c} = ${}", i + 1))
        .collect();
    let base_where: Vec<String> = base_cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("b.{c} = ${}", i + 1))
        .collect();
    let lock = format!(
        "SELECT ctid::text FROM {idx_table} WHERE {} FOR UPDATE",
        idx_where.join(" AND ")
    );
    let locked: Vec<String> = bind_key(sqlx::query(&lock), key)
        .fetch_all(&mut **tx)
        .await
        .map_err(db_error)?
        .iter()
        .map(|r| r.try_get::<String, _>(0))
        .collect::<Result<_, _>>()
        .map_err(db_error)?;
    if locked.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM {idx_table} WHERE ctid = ANY(${}::text[]::tid[]) \
         AND NOT EXISTS (SELECT 1 FROM {base_table} b WHERE {})",
        base_cols.len() + 1,
        base_where.join(" AND ")
    );
    bind_key(sqlx::query(&sql), key)
        .bind(locked)
        .execute(&mut **tx)
        .await
        .map_err(db_error)?;
    Ok(())
}

/// Bring one index's rows for one base key in line with the base table.
///
/// The queue's apply step: a queued row says which item changed, and this writes
/// the item's state now rather than the image the row carried. So a row applied
/// late, out of order, or twice cannot leave the index behind the base table, and
/// a repair row for a write that never told the index about itself needs no image
/// at all.
///
/// `key_item` only has to carry the base key attributes.
pub(crate) async fn resync_base_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table_id: &str,
    idx: &GsiIndexDef,
    base_ks: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    key_item: &Item,
) -> Result<(), StorageError> {
    let base_sks = all_sort_key_info(base_ks, attr_defs);
    let base_table = data_table_name(table_id);
    let idx_table = index_table_name(&idx.index_id);
    let key = key_from_item(key_item, base_ks, &base_sks)?;
    resync_key(
        tx,
        &base_table,
        &idx_table,
        idx,
        base_ks,
        attr_defs,
        &base_sks,
        &key,
    )
    .await
}

/// Wait out every write that already holds a lock on the base table.
///
/// `SHARE` conflicts with the `ROW EXCLUSIVE` lock every insert, update and
/// delete takes, so this returns once each of them has committed or rolled back.
/// New writes queue behind it while it waits, each holding a data pool
/// connection, so the wait is bounded: something that holds the table for long (a
/// long transaction, or a VACUUM, which takes a conflicting lock and need not
/// yield to this one) would otherwise stop every write to the table, and then
/// starve other tables of connections, for as long. On a timeout the lock request
/// is withdrawn, the queued writes proceed, and the barrier is tried again after
/// a growing pause. With the default `deadlock_timeout` the attempts add up to
/// about 12 s of stalls spread over 20 s before the build gives up.
pub(crate) async fn write_barrier(data: &sqlx::PgPool, table_id: &str) -> Result<(), StorageError> {
    // The timeout is a second longer than the server's `deadlock_timeout`, on
    // purpose: a waiter cancels a conflicting autovacuum only when it runs its
    // deadlock check, which happens after that timeout, so a shorter wait could
    // never get past one. An anti-wraparound or manual vacuum does not yield at
    // all; if one outlasts every attempt the build fails as transient, and the
    // index stays CREATING for the recovery worker.
    const ATTEMPTS: u32 = 6;
    let base_table = data_table_name(table_id);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let result = async {
            let mut tx = data.begin().await.map_err(db_error)?;
            sqlx::query(
                "SELECT set_config('lock_timeout', \
                 ((extract(epoch FROM current_setting('deadlock_timeout')::interval) * 1000)::int \
                  + 1000)::text || 'ms', true)",
            )
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            sqlx::query(&format!("LOCK TABLE {base_table} IN SHARE MODE"))
                .execute(&mut *tx)
                .await
                .map_err(db_error)?;
            tx.commit().await.map_err(db_error)
        }
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err(e) if attempt < ATTEMPTS && e.to_string().contains("SQLSTATE 55P03") => {
                // Writers queued behind the attempt run in the gap, and the gap
                // grows so a long holder is not met with a steady stream of stalls.
                tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Whether a build error is worth leaving to the recovery worker: a conflict,
/// a timeout, a lost or refused connection, a shutdown, a server-side I/O or
/// internal error, or a connection pool that had nothing to give. Anything else
/// (bad data, a row that does not decode, a key too large for the index) would
/// fail the same way every time, so the default is "not transient": a wrong
/// answer here is an index that stays CREATING for good, rebuilt every hour,
/// with nothing to say why.
pub(crate) fn is_transient(e: &StorageError) -> bool {
    let msg = e.to_string();
    if let Some(at) = msg.find("SQLSTATE ") {
        let code = &msg[at + "SQLSTATE ".len()..];
        return ["40", "08", "53", "55P03", "57", "58", "XX"]
            .iter()
            .any(|prefix| code.starts_with(prefix));
    }
    // sqlx's own wording for errors that never reached the server.
    [
        "pool timed out",
        "closed pool",
        "error communicating with database",
        "connection reset",
        "broken pipe",
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

/// What a backfill did, for the log line.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct BackfillCounts {
    pub(crate) indexed: usize,
    pub(crate) skipped: usize,
}

/// Write the current state of every base item into the index.
///
/// Keyset pages over the full base key, up to the last key present when the
/// backfill starts. A key added later was added by a write that saw the index (it
/// came after the barrier), and that write maintains the index itself; stopping
/// at the starting maximum is what lets the backfill end on a table whose keys
/// keep growing. Each page:
///
/// 1. Reads the page's keys without locking, which fixes the page's range.
/// 2. In one transaction, share-locks the rows in that range with `SKIP LOCKED`
///    and writes each one's state into the index. Skipping rather than waiting
///    keeps this out of deadlocks with a writer that holds one row of the page and
///    waits on another, because a page that waited would hold its earlier rows
///    while it did. A page that is still chosen as a deadlock victim (or hits a
///    serialization failure under a stricter isolation level) is retried.
/// 3. After that commits, takes each key from step 1 that step 2 skipped, one at
///    a time with a waiting lock. A transaction holding one row lock cannot be
///    part of a deadlock cycle.
///
/// Then one pass deletes index rows whose base item no longer exists. Only a
/// write that read the catalog before the index existed can leave one, by
/// deleting an item without touching its index row.
pub(crate) async fn backfill(
    data: &sqlx::PgPool,
    table_id: &str,
    idx: &GsiIndexDef,
    base_ks: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<BackfillCounts, StorageError> {
    let base_sks = all_sort_key_info(base_ks, attr_defs);
    let base_table = data_table_name(table_id);
    let idx_table = index_table_name(&idx.index_id);
    let cols = base_key_columns(&base_sks);
    let n = cols.len();
    let key_tuple = tuple(&cols, "");
    let selected = cols.join(", ");
    let descending: Vec<String> = cols.iter().map(|c| format!("{c} DESC")).collect();

    let mut counts = BackfillCounts::default();

    let last_sql = format!(
        "SELECT {selected} FROM {base_table} ORDER BY {} LIMIT 1",
        descending.join(", ")
    );
    let last = sqlx::query(&last_sql)
        .fetch_optional(data)
        .await
        .map_err(db_error)?
        .map(|r| read_key(&r, &base_sks))
        .transpose()?;

    let mut cursor: Option<BaseKey> = None;
    while let Some(end) = &last {
        // 1. The page's keys.
        let mut conds = Vec::new();
        let mut next = 1;
        if cursor.is_some() {
            conds.push(format!("{key_tuple} > {}", params(next, n)));
            next += n;
        }
        conds.push(format!("{key_tuple} <= {}", params(next, n)));
        let keys_sql = format!(
            "SELECT {selected} FROM {base_table} WHERE {} ORDER BY {selected} \
             LIMIT {BACKFILL_BATCH}",
            conds.join(" AND ")
        );
        let mut q = sqlx::query(&keys_sql);
        if let Some(c) = &cursor {
            q = bind_key(q, c);
        }
        q = bind_key(q, end);
        let key_rows = q.fetch_all(data).await.map_err(db_error)?;
        let page: Vec<BaseKey> = key_rows
            .iter()
            .map(|r| read_key(r, &base_sks))
            .collect::<Result<_, _>>()?;
        let Some(upper) = page.last().cloned() else {
            break;
        };

        // 2. Lock what is free in the range and write its state.
        let (done, page_counts) = with_retries(|| {
            write_page(
                data,
                &base_table,
                &idx_table,
                idx,
                base_ks,
                attr_defs,
                &base_sks,
                cursor.as_ref(),
                &upper,
            )
        })
        .await?;
        counts.indexed += page_counts.indexed;
        counts.skipped += page_counts.skipped;

        // 3. The keys someone else held.
        for key in &page {
            if done.iter().any(|d| same_key(d, key)) {
                continue;
            }
            with_retries(|| async {
                let mut tx = data.begin().await.map_err(db_error)?;
                resync_key(
                    &mut tx,
                    &base_table,
                    &idx_table,
                    idx,
                    base_ks,
                    attr_defs,
                    &base_sks,
                    key,
                )
                .await?;
                tx.commit().await.map_err(db_error)
            })
            .await?;
        }

        if same_key(&upper, end) {
            break;
        }
        cursor = Some(upper);
    }

    sweep_orphans(data, &base_table, &idx_table, &base_sks).await?;
    Ok(counts)
}

/// Attempts at one backfill transaction before its error is the build's error.
const PAGE_ATTEMPTS: u32 = 5;

/// Run one backfill transaction, again if it was a deadlock victim or hit a
/// serialization failure. Anything else is returned as it is.
async fn with_retries<T, F, Fut>(mut run: F) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StorageError>>,
{
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match run().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < PAGE_ATTEMPTS && is_retryable(&e) => {
                tracing::debug!(attempt, "GSI backfill transaction retried: {e}");
                tokio::time::sleep(Duration::from_millis(10 * u64::from(attempt))).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// A deadlock victim or a serialization failure: the page can simply run again.
fn is_retryable(e: &StorageError) -> bool {
    let msg = e.to_string();
    msg.contains("SQLSTATE 40P01") || msg.contains("SQLSTATE 40001")
}

/// Step 2 of [`backfill`]: one page's transaction. Returns the keys it wrote.
#[allow(clippy::too_many_arguments)]
async fn write_page(
    data: &sqlx::PgPool,
    base_table: &str,
    idx_table: &str,
    idx: &GsiIndexDef,
    base_ks: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    base_sks: &[(&str, ScalarAttributeType)],
    lower: Option<&BaseKey>,
    upper: &BaseKey,
) -> Result<(Vec<BaseKey>, BackfillCounts), StorageError> {
    let cols = base_key_columns(base_sks);
    let n = cols.len();
    let key_tuple = tuple(&cols, "");
    let selected = cols.join(", ");
    let (lower_clause, upper_first) = match lower {
        Some(_) => (format!("{key_tuple} > {} AND ", params(1, n)), n + 1),
        None => (String::new(), 1),
    };
    let lock_sql = format!(
        "SELECT {selected}, item_data FROM {base_table} \
         WHERE {lower_clause}{key_tuple} <= {} \
         ORDER BY {selected} FOR SHARE SKIP LOCKED",
        params(upper_first, n)
    );

    let mut tx = data.begin().await.map_err(db_error)?;
    let mut q = sqlx::query(&lock_sql);
    if let Some(c) = lower {
        q = bind_key(q, c);
    }
    q = bind_key(q, upper);
    let locked = q.fetch_all(&mut *tx).await.map_err(db_error)?;

    let mut counts = BackfillCounts::default();
    let mut done: Vec<BaseKey> = Vec::with_capacity(locked.len());
    for row in &locked {
        let key = read_key(row, base_sks)?;
        let json: serde_json::Value = row.try_get("item_data").map_err(db_error)?;
        let item = json_to_item(json)?;
        if write_state(
            &mut tx,
            idx_table,
            &idx.key_schema,
            &idx.projection,
            base_ks,
            attr_defs,
            base_sks,
            &item,
        )
        .await?
        {
            counts.indexed += 1;
        } else {
            counts.skipped += 1;
        }
        done.push(key);
    }
    tx.commit().await.map_err(db_error)?;
    Ok((done, counts))
}

fn same_key(a: &BaseKey, b: &BaseKey) -> bool {
    a.pk == b.pk
        && a.sks.len() == b.sks.len()
        && a.sks.iter().zip(&b.sks).all(|(x, y)| match (x, y) {
            (KeyVal::S(x), KeyVal::S(y)) => x == y,
            (KeyVal::N(x), KeyVal::N(y)) => x == y,
            (KeyVal::B(x), KeyVal::B(y)) => x == y,
            _ => false,
        })
}

/// [`resync_base_key`] for a key already in column form.
///
/// The row lock is taken before any index row is touched, which is the order a
/// writer takes them in, so the two cannot deadlock.
#[allow(clippy::too_many_arguments)]
async fn resync_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    base_table: &str,
    idx_table: &str,
    idx: &GsiIndexDef,
    base_ks: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    base_sks: &[(&str, ScalarAttributeType)],
    key: &BaseKey,
) -> Result<(), StorageError> {
    let cols = base_key_columns(base_sks);
    let where_parts: Vec<String> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{c} = ${}", i + 1))
        .collect();
    let sql = format!(
        "SELECT item_data FROM {base_table} WHERE {} FOR SHARE",
        where_parts.join(" AND ")
    );
    let current = bind_key(sqlx::query(&sql), key)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db_error)?;
    match current {
        Some(row) => {
            let json: serde_json::Value = row.try_get("item_data").map_err(db_error)?;
            let item = json_to_item(json)?;
            write_state(
                tx,
                idx_table,
                &idx.key_schema,
                &idx.projection,
                base_ks,
                attr_defs,
                base_sks,
                &item,
            )
            .await?;
        }
        None => delete_if_absent(tx, base_table, idx_table, &cols, key).await?,
    }
    Ok(())
}

/// Delete index rows whose base item no longer exists.
///
/// The candidates are found without locking, and each is then deleted through
/// [`delete_if_absent`] in its own transaction, which holds no base row lock: a
/// writer re-creating the item at the same moment holds that item's base row and
/// may be waiting on another, and an index row lock taken while holding base
/// locks could close a cycle with it.
async fn sweep_orphans(
    data: &sqlx::PgPool,
    base_table: &str,
    idx_table: &str,
    base_sks: &[(&str, ScalarAttributeType)],
) -> Result<(), StorageError> {
    let cols = base_key_columns(base_sks);
    let idx_cols: Vec<String> = cols.iter().map(|c| format!("i.base_{c} AS {c}")).collect();
    let join: Vec<String> = cols.iter().map(|c| format!("b.{c} = i.base_{c}")).collect();
    let sql = format!(
        "SELECT DISTINCT {} FROM {idx_table} i \
         WHERE NOT EXISTS (SELECT 1 FROM {base_table} b WHERE {})",
        idx_cols.join(", "),
        join.join(" AND ")
    );
    let rows = sqlx::query(&sql).fetch_all(data).await.map_err(db_error)?;
    for row in &rows {
        let key = read_key(row, base_sks)?;
        with_retries(|| async {
            let mut tx = data.begin().await.map_err(db_error)?;
            delete_if_absent(&mut tx, base_table, idx_table, &cols, &key).await?;
            tx.commit().await.map_err(db_error)
        })
        .await?;
    }
    Ok(())
}

/// The base key attributes of `item`, and nothing else.
fn key_only(item: &Item, base_ks: &[KeySchemaElement]) -> Item {
    let mut out = Item::new();
    for ks in base_ks {
        if let Some(v) = item.get(&ks.attribute_name) {
            out.insert(ks.attribute_name.clone(), v.clone());
        }
    }
    out
}

/// Queue a repair for any GSI this write did not know about, if the write was
/// slow enough that the backfill may have missed it.
///
/// Call inside the write's transaction, after its base table write and before its
/// commit, so the repair commits or rolls back with the write. For a write with no
/// transaction, call it in a new one right after the write.
///
/// `known` is the index set the write read at `stamp`. Returns the number of rows
/// queued, so the caller can wake the workers.
pub(crate) async fn fix_up_stale_write(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    catalog: &sqlx::PgPool,
    stamp: WriteStamp,
    key_info: &TableKeyInfo,
    known: &[IndexMeta],
    item: &Item,
) -> Result<usize, StorageError> {
    if stamp.elapsed() <= GRACE_WRITE {
        return Ok(0);
    }
    let missed = missed_gsis(catalog, &key_info.table_id, known).await?;
    queue_repairs(tx, key_info, &missed, item).await
}

/// The GSIs on a table now that are not in `known`.
pub(crate) async fn missed_gsis(
    catalog: &sqlx::PgPool,
    table_id: &str,
    known: &[IndexMeta],
) -> Result<Vec<IndexMeta>, StorageError> {
    let fresh = fetch_indexes_for_table(table_id, catalog).await?;
    Ok(fresh
        .into_iter()
        .filter(|idx| idx.index_type == "GSI" && !known.iter().any(|k| k.index_id == idx.index_id))
        .collect())
}

/// Queue one repair row per index in `missed` for the item's base key.
pub(crate) async fn queue_repairs(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_info: &TableKeyInfo,
    missed: &[IndexMeta],
    item: &Item,
) -> Result<usize, StorageError> {
    let base_ks = &key_info.key_schema;
    let key = key_only(item, base_ks);
    for idx in missed {
        let context = PendingApplyContext::Gsi(GsiApplyContext {
            base_key_schema: base_ks.clone(),
            attribute_definitions: idx.attribute_definitions.clone(),
            index: GsiIndexDef {
                index_id: idx.index_id.clone(),
                key_schema: idx.key_schema.clone(),
                projection: idx.projection.clone(),
            },
        });
        enqueue_gsi_pending(tx, &key_info.table_id, Some(&key), None, 0, &context).await?;
    }
    if !missed.is_empty() {
        tracing::debug!(
            table_id = %key_info.table_id,
            queued = missed.len(),
            "queued GSI repair for a write that predates the index"
        );
    }
    Ok(missed.len())
}

/// One GSI an `UpdateTable` (or a recovery sweep) is building.
pub(crate) struct GsiBuild {
    pub(crate) index_name: String,
    pub(crate) def: GsiIndexDef,
    /// Held for the whole build so a peer's sweep leaves it alone; released on drop.
    pub(crate) _owner: Option<crate::data::vector_index::BuildOwner>,
}

/// Drop the data tables of builds that are being abandoned. Logged, not
/// propagated: the caller is already returning an error.
pub(crate) async fn drop_gsi_tables(data: &sqlx::PgPool, builds: &[GsiBuild]) {
    for b in builds {
        let t = index_table_name(&b.def.index_id);
        if let Err(e) = sqlx::query(&format!("DROP TABLE IF EXISTS {t}"))
            .execute(data)
            .await
        {
            tracing::warn!(
                index_id = %b.def.index_id,
                "could not drop the data table of an abandoned GSI build: {e}"
            );
        }
    }
}

/// Remove a GSI whose build fails deterministically: the catalog row, then its
/// data table, but only if this call removed the row. The delete is conditional
/// on the row still being `CREATING`: two builds of one index can run at once (a
/// recovery pass next to an UpdateTable whose ownership session could not be
/// opened), and if the other one has published the index since, a failure here
/// is this build's problem and must not take an ACTIVE index away. With the row
/// gone or published, a table writes are maintaining must not vanish under them.
///
/// The other way round, a second build failing for good while the first is still
/// scanning, removes the row and the table under the first; the first then finds
/// no row to flip and reports the index as deleted while it was being built. The
/// failure would have met the first build too, so the outcome is the same, minus
/// the error to the caller.
///
/// The table is dropped [`GRACE_PUBLISH`] after the row, so a write that read the
/// catalog just before the delete has usually finished with the table. The
/// synchronous write path also treats a table that is gone as an index that is
/// gone, so the delay is for the common case, not the guarantee.
pub(crate) async fn remove_failed_build(
    catalog: &sqlx::PgPool,
    data: &sqlx::PgPool,
    table_id: &str,
    build: &GsiBuild,
) {
    // By id, not name: the name may already belong to a newer index.
    let deleted = sqlx::query(
        "DELETE FROM indexes WHERE table_id = $1 AND index_id = $2 \
         AND index_status = 'CREATING'",
    )
    .bind(table_id)
    .bind(&build.def.index_id)
    .execute(catalog)
    .await;
    match deleted {
        Ok(r) if r.rows_affected() == 1 => {
            tokio::time::sleep(GRACE_PUBLISH).await;
            drop_gsi_tables(data, std::slice::from_ref(build)).await;
        }
        Ok(_) => {}
        Err(e) => tracing::error!(
            index_id = %build.def.index_id,
            "could not remove the catalog row of a failed GSI build: {e}"
        ),
    }
}

/// Whether the catalog still has this index. A build that fails because the index
/// or its table was deleted under it has nothing left to clean up or report.
async fn index_row_exists(
    catalog: &sqlx::PgPool,
    table_id: &str,
    index_id: &str,
) -> Result<bool, StorageError> {
    let found: Option<(i32,)> =
        sqlx::query_as("SELECT 1 FROM indexes WHERE table_id = $1 AND index_id = $2")
            .bind(table_id)
            .bind(index_id)
            .fetch_optional(catalog)
            .await
            .map_err(db_error)?;
    Ok(found.is_some())
}

/// Wait, barrier, backfill and publish each build, in order.
///
/// Returns how many this call published, or the position of the first build
/// that failed with its error; builds before it are ACTIVE, and the caller
/// abandons it and the ones after it. A build whose index was deleted while it
/// ran, or that someone else published first, is not a failure.
pub(crate) async fn build_gsis(
    catalog: &sqlx::PgPool,
    data: &sqlx::PgPool,
    table_id: &str,
    base_ks: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
    builds: &[GsiBuild],
) -> Result<usize, (usize, StorageError)> {
    if builds.is_empty() {
        return Ok(0);
    }
    let mut published_here = 0usize;
    tokio::time::sleep(GRACE_PUBLISH).await;
    if let Err(e) = write_barrier(data, table_id).await {
        // A table deleted during the wait is not a failed build.
        let mut gone = true;
        for b in builds {
            match index_row_exists(catalog, table_id, &b.def.index_id).await {
                Ok(false) => {}
                _ => gone = false,
            }
        }
        if gone {
            drop_gsi_tables(data, builds).await;
            return Ok(0);
        }
        return Err((0, e));
    }
    for (i, b) in builds.iter().enumerate() {
        let filled = backfill(data, table_id, &b.def, base_ks, attr_defs).await;
        let published = match filled {
            Ok(counts) => {
                tracing::debug!(
                    index_id = %b.def.index_id,
                    indexed = counts.indexed,
                    skipped = counts.skipped,
                    "GSI backfill complete"
                );
                sqlx::query(
                    "UPDATE indexes SET index_status = 'ACTIVE' \
                     WHERE table_id = $1 AND index_id = $2 AND index_status = 'CREATING'",
                )
                .bind(table_id)
                .bind(&b.def.index_id)
                .execute(catalog)
                .await
                .map(|r| r.rows_affected() > 0)
                .map_err(db_error)
            }
            Err(e) => Err(e),
        };
        match published {
            Ok(true) => published_here += 1,
            outcome => {
                // Either the flip found no CREATING row or the build failed. If the
                // row is gone the index was deleted (or its table was) while this
                // ran: drop what the delete may have raced past, and move on.
                match index_row_exists(catalog, table_id, &b.def.index_id).await {
                    Ok(false) => {
                        drop_gsi_tables(data, std::slice::from_ref(b)).await;
                        tracing::info!(
                            index_id = %b.def.index_id,
                            "GSI was deleted while it was being built"
                        );
                    }
                    Ok(true) => {
                        let e = match outcome {
                            Err(e) => e,
                            // Present but not CREATING: someone else published it.
                            Ok(_) => continue,
                        };
                        return Err((i, e));
                    }
                    Err(check) => {
                        let e = match outcome {
                            Err(e) => e,
                            Ok(_) => check,
                        };
                        return Err((i, e));
                    }
                }
            }
        }
    }
    Ok(published_here)
}

/// Finish every GSI a crash left `CREATING`, once.
///
/// An index whose build is still running holds its ownership lock and is
/// skipped. The data table is created if the crash came before it, and the
/// backfill is re-run from the start: it writes each item's current state, so
/// what the dead build already wrote does not need clearing. Only indexes on
/// ACTIVE tables are taken; a table being deleted takes its indexes with it.
///
/// Returns the number of indexes this call published.
pub async fn reconcile_creating_gsis(
    engine: &crate::PostgresEngine,
) -> Result<usize, StorageError> {
    reconcile_pass(engine, None).await
}

/// When a recovery sweep may next try an index whose rebuild failed.
///
/// A rebuild that fails every time (a key too large for the index, say) would
/// otherwise rescan the table and take its write barrier every minute forever.
#[derive(Default)]
pub(crate) struct RecoveryBackoff {
    failures: std::collections::HashMap<String, (u32, std::time::Instant)>,
}

impl RecoveryBackoff {
    const FIRST: Duration = Duration::from_secs(60);
    const MAX: Duration = Duration::from_secs(3_600);

    fn due(&self, index_id: &str) -> bool {
        self.failures
            .get(index_id)
            .is_none_or(|(_, next)| std::time::Instant::now() >= *next)
    }

    fn failed(&mut self, index_id: &str) {
        let n = self.failures.get(index_id).map_or(0, |(n, _)| *n) + 1;
        let wait = Self::FIRST
            .saturating_mul(2u32.saturating_pow(n.saturating_sub(1)))
            .min(Self::MAX);
        self.failures
            .insert(index_id.to_owned(), (n, std::time::Instant::now() + wait));
    }

    fn succeeded(&mut self, index_id: &str) {
        self.failures.remove(index_id);
    }

    /// Forget indexes that are no longer CREATING (published or deleted).
    fn retain<'a>(&mut self, live: impl Iterator<Item = &'a str>) {
        let live: std::collections::HashSet<&str> = live.collect();
        self.failures.retain(|id, _| live.contains(id.as_str()));
    }
}

/// The periodic recovery sweep: runs once immediately, then every minute.
///
/// Its own task rather than a step at startup, so a large rebuild does not hold
/// the server from serving (writes keep a CREATING index current in the meantime)
/// and does not delay the vector index sweep.
pub(crate) async fn recovery_worker(
    engine: std::sync::Arc<crate::PostgresEngine>,
    token: extenddb_storage::CancellationToken,
) {
    const POLL_INTERVAL: Duration = Duration::from_secs(60);
    let mut backoff = RecoveryBackoff::default();
    loop {
        match reconcile_pass(&engine, Some(&mut backoff)).await {
            Ok(0) => {}
            Ok(n) => tracing::info!(finished = n, "finished GSI build(s) left incomplete"),
            Err(e) => tracing::error!("incomplete GSI build sweep failed: {e}"),
        }
        if !extenddb_storage::sleep_or_shutdown(&token, POLL_INTERVAL).await {
            return;
        }
    }
}

async fn reconcile_pass(
    engine: &crate::PostgresEngine,
    mut backoff: Option<&mut RecoveryBackoff>,
) -> Result<usize, StorageError> {
    type Row = (
        String,
        String,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        String,
        String,
    );
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT i.table_id, i.index_id, i.key_schema, i.projection, \
                t.key_schema, t.attribute_definitions, t.table_name, i.index_name \
         FROM indexes i JOIN tables t ON t.table_id = i.table_id \
         WHERE i.index_type = 'GSI' AND i.index_status = 'CREATING' \
           AND t.table_status = 'ACTIVE' \
         ORDER BY i.table_id, i.index_id",
    )
    .fetch_all(&engine.pool)
    .await
    .map_err(db_error)?;

    if let Some(b) = backoff.as_mut() {
        b.retain(rows.iter().map(|r| r.1.as_str()));
    }
    let mut finished = 0usize;
    for (table_id, index_id, idx_ks, idx_proj, base_ks, attr_defs, table_name, index_name) in rows {
        if backoff.as_ref().is_some_and(|b| !b.due(&index_id)) {
            continue;
        }
        let Some(owner) =
            crate::data::vector_index::build_ownership(&engine.data_pool, &index_id).await
        else {
            continue;
        };
        // Re-read under ownership: the list above is a snapshot, and the build
        // that held this index a moment ago may have published or abandoned it.
        let still_building: Result<Option<(i32,)>, _> = sqlx::query_as(
            "SELECT 1 FROM indexes i JOIN tables t ON t.table_id = i.table_id \
             WHERE i.index_id = $1 AND i.index_status = 'CREATING' AND t.table_status = 'ACTIVE'",
        )
        .bind(&index_id)
        .fetch_optional(&engine.pool)
        .await;
        match still_building {
            Ok(Some(_)) => {}
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(index_id = %index_id, "could not re-check a CREATING GSI: {e}");
                continue;
            }
        }
        let parse = || -> Result<_, StorageError> {
            Ok((
                from_catalog_json::<Vec<KeySchemaElement>>(idx_ks.clone())?,
                from_catalog_json::<extenddb_core::types::Projection>(idx_proj.clone())?,
                from_catalog_json::<Vec<KeySchemaElement>>(base_ks.clone())?,
                from_catalog_json::<Vec<AttributeDefinition>>(attr_defs.clone())?,
            ))
        };
        let (key_schema, projection, base_ks, attr_defs) = match parse() {
            Ok(parsed) => parsed,
            Err(e) => {
                tracing::error!(index_id = %index_id, "unreadable CREATING GSI definition: {e}");
                if let Some(b) = backoff.as_mut() {
                    b.failed(&index_id);
                }
                continue;
            }
        };

        let made = async {
            let mut tx = engine.data_pool.begin().await.map_err(db_error)?;
            let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(index_table_name(&index_id))
                .fetch_one(&mut *tx)
                .await
                .map_err(db_error)?;
            if !exists {
                crate::PostgresEngine::create_index_data_table(
                    &mut tx,
                    &index_id,
                    &key_schema,
                    &attr_defs,
                    &base_ks,
                    &attr_defs,
                )
                .await?;
            }
            tx.commit().await.map_err(db_error)
        }
        .await;
        if let Err(e) = made {
            tracing::error!(
                index_id = %index_id,
                "could not prepare a CREATING GSI for rebuild: {e}"
            );
            if let Some(b) = backoff.as_mut() {
                b.failed(&index_id);
            }
            continue;
        }

        let build = GsiBuild {
            index_name: index_name.clone(),
            def: GsiIndexDef {
                index_id: index_id.clone(),
                key_schema,
                projection,
            },
            _owner: Some(owner),
        };
        match build_gsis(
            &engine.pool,
            &engine.data_pool,
            &table_id,
            &base_ks,
            &attr_defs,
            std::slice::from_ref(&build),
        )
        .await
        {
            Ok(n) => {
                finished += n;
                if let Some(b) = backoff.as_mut() {
                    b.succeeded(&index_id);
                }
                if n > 0 {
                    tracing::info!(
                        table = %table_name,
                        index = %index_name,
                        "finished a GSI build left incomplete"
                    );
                }
            }
            Err((_, e)) if is_transient(&e) => {
                tracing::warn!(
                    table = %table_name,
                    index = %index_name,
                    "could not finish an incomplete GSI build; leaving it CREATING: {e}"
                );
                if let Some(b) = backoff.as_mut() {
                    b.failed(&index_id);
                }
            }
            Err((_, e)) => {
                // The same answer UpdateTable gives: an error that would recur on
                // every attempt removes the index rather than rebuilding it hourly.
                tracing::error!(
                    table = %table_name,
                    index = %index_name,
                    index_id = %index_id,
                    "GSI build fails on every attempt; removing the index: {e}"
                );
                remove_failed_build(&engine.pool, &engine.data_pool, &table_id, &build).await;
                if let Some(b) = backoff.as_mut() {
                    b.succeeded(&index_id);
                }
            }
        }
    }
    Ok(finished)
}

impl crate::PostgresEngine {
    /// [`fix_up_stale_write`] for a write that ran without a transaction.
    ///
    /// That path is only taken when the write's catalog read found no index at
    /// all, so every GSI the re-read finds is one it missed. The write has already
    /// committed, so the repair goes in its own transaction, and a failure is
    /// logged rather than returned: reporting an error for a write that happened
    /// would invite a retry of something that already took effect.
    pub(crate) async fn fix_up_autocommit(
        &self,
        stamp: WriteStamp,
        key_info: &TableKeyInfo,
        item: &Item,
    ) {
        if stamp.elapsed() <= GRACE_WRITE {
            return;
        }
        let repaired = async {
            // Asked before a transaction is opened, so a slow write on a table
            // that still has no index costs one catalog read and nothing more.
            let missed = missed_gsis(&self.pool, &key_info.table_id, &[]).await?;
            if missed.is_empty() {
                return Ok(0);
            }
            let mut tx = self.data_pool.begin().await.map_err(db_error)?;
            let n = queue_repairs(&mut tx, key_info, &missed, item).await?;
            tx.commit().await.map_err(db_error)?;
            Ok::<usize, StorageError>(n)
        }
        .await;
        match repaired {
            Ok(0) => {}
            Ok(_) => {
                if let Some(q) = &self.gsi_queue {
                    q.notify_workers();
                }
            }
            Err(e) => tracing::error!(
                table_id = %key_info.table_id,
                "could not queue a GSI repair for a slow write; the index may miss this item \
                 until it is written again: {e}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::KeyType;

    fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        }
    }

    fn ad(name: &str, t: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: t,
        }
    }

    #[test]
    fn indexable_requires_every_key_of_the_declared_type() {
        let index_ks = vec![ks("g", KeyType::Hash), ks("n", KeyType::Range)];
        let defs = vec![
            ad("g", ScalarAttributeType::S),
            ad("n", ScalarAttributeType::N),
        ];
        let mut item = Item::new();
        item.insert("g".into(), AttributeValue::S("a".into()));
        assert!(!indexable(&item, &index_ks, &defs), "missing range key");
        item.insert("n".into(), AttributeValue::S("1".into()));
        assert!(!indexable(&item, &index_ks, &defs), "wrong type");
        item.insert("n".into(), AttributeValue::N("1".into()));
        assert!(indexable(&item, &index_ks, &defs));
        item.insert("g".into(), AttributeValue::S(String::new()));
        assert!(!indexable(&item, &index_ks, &defs), "empty string key");
    }

    #[test]
    fn indexable_is_false_without_a_definition() {
        let index_ks = vec![ks("g", KeyType::Hash)];
        let mut item = Item::new();
        item.insert("g".into(), AttributeValue::S("a".into()));
        assert!(!indexable(&item, &index_ks, &[]));
    }

    #[test]
    fn stamp_elapsed_grows_and_is_not_max() {
        let s = WriteStamp::now();
        std::thread::sleep(Duration::from_millis(5));
        let e = s.elapsed();
        assert!(e >= Duration::from_millis(5), "{e:?}");
        assert!(e < Duration::from_secs(60), "{e:?}");
    }

    #[test]
    fn grace_publish_exceeds_grace_write() {
        assert!(GRACE_PUBLISH > GRACE_WRITE);
    }

    #[test]
    fn transient_errors_are_left_to_recovery() {
        let e = |m: &str| StorageError::Internal(m.to_owned());
        assert!(is_transient(&e("SQLSTATE 40P01: deadlock detected")));
        assert!(is_transient(&e("SQLSTATE 55P03: lock timeout")));
        assert!(is_transient(&e("SQLSTATE 08006: connection failure")));
        assert!(is_transient(&e("SQLSTATE 58030: could not read block")));
        assert!(is_transient(&e("SQLSTATE XX000: internal error")));
        assert!(is_transient(&e(
            "pool timed out while waiting for an open connection"
        )));
        assert!(!is_transient(&e(
            "SQLSTATE 54000: index row size exceeds maximum"
        )));
        assert!(!is_transient(&e("SQLSTATE 22P02: invalid input syntax")));
        assert!(is_transient(&e(
            "error communicating with database: broken pipe"
        )));
        assert!(!is_transient(&e("invalid type: null, expected a string")));
        assert!(!is_transient(&e("missing sort key sk")));
    }
}

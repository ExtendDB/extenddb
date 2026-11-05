// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend-independent consistency check for [`SnapshotEngine`], run by each
//! backend's own test suite against a real database.
//!
//! The check opens a snapshot, then has a writer delete pre-existing items,
//! insert new ones, and overwrite every remaining item several times while
//! the snapshot stream is still being read. A consistent snapshot shows the
//! table exactly as it was at open: every deleted item present, no inserted
//! item, every overwritten item at its pre-open version, no duplicates.
//!
//! For the check to mean anything the stream must still be open against the
//! backend while the writer runs. Every backend fetches a page at a time
//! through the shared [`paging`] knob; `run` sets the page size itself before
//! it opens anything, requires the table to span several pages
//! (`BASE_ITEMS + DOOMED_ITEMS > 3 * page_size`), reads one page plus one
//! item, and then asserts that the shared round-trip counter advanced by at
//! least two since just before the open, so a backend that ignored the knob (or a stream
//! that fetched the whole table at once) fails here rather than letting the
//! rest of the check pass vacuously. It then pauses until the writer has
//! committed, then drains. A paged reader that re-queries per page without a
//! snapshot fails the version or count assertion: such mutants, run at a page
//! of 50 on all three backends (SQLite: fresh `BEGIN` per page; PostgreSQL:
//! `OFFSET`/`LIMIT` in READ COMMITTED; MongoDB: a plain majority-read cursor
//! outside the session), each failed. The
//! counter is trusted, not verified: a backend must record one fetch per
//! round trip (PostgreSQL and SQLite record one beside each `FETCH` or page
//! query), and one that recorded per item would pass these assertions with
//! the whole table read in one trip. The MongoDB test fixture therefore
//! feeds the counter from what the server received, each `find` and
//! `getMore` on the data database (command monitoring), since the driver
//! hides `getMore` from the stream and the engine records nothing.
//!
//! Every drain here is under a deadline (`DRAIN_TIMEOUT`, 120 s): a stream that
//! never ends (a keyset page that re-queries an empty page forever) fails
//! the check naming the stage and the items read, instead of hanging it and,
//! through the hooks lock, every other snapshot test in the binary.
//! [`hooks_lock`] itself gives up when one holder has kept the lock for
//! `HOOKS_LOCK_WAIT` (600 s), naming the wait.
//!
//! Before the writer starts, the check compares the snapshot's `description`
//! with the storage layer's `describe_table` of the quiescent table, with the
//! running counts and `table_throughput_mode_summary` cleared on both sides
//! (`comparable_description`): the trait's promise that the description is
//! the table as described, which a backend building it from a second code
//! path could break without any other assertion noticing.
//! `restore_summary` is compared: storage sets it on a restored table, so a
//! backend's suite also runs [`assert_snapshot_describes_the_table`] on a
//! table that has one.
//!
//! The knob, the counter and the open pause are process-global, and the
//! pause goes to the next open that reaches it whatever its engine or table.
//! One lock covers all three: [`hooks_lock`]. Every test in a backend crate
//! that opens a snapshot or touches the [`paging`] statics holds it for its
//! whole body, and [`run`] and [`run_refusals`] take its guard as their first
//! argument, so a check cannot run without it and no other test in the
//! binary can take the pause, reset the knob, or feed the counter while a
//! check is running.
//!
//! Key order is checked with `KeyOrder::compare`, the comparator the
//! manifest parser and the restore use, so the backend's SQL order, the
//! format's rule and this oracle are one rule, not three.
//!
//! Reads after the commit: the counter is read when the writer's rounds
//! have committed, and must have advanced again by the end of the drain. A
//! stream that fetched every page during the open (each page in its own
//! transaction, say) passes every content assertion, because the writer has
//! not started; this assertion fails it.
//!
//! One instant: for each [`paging::OpenStage`] the check arms a pause in the
//! next open (every backend calls [`paging::open_pause`] at both stages) and,
//! while the open waits, commits a put W1, an `UpdateTable` U adding a GSI,
//! and a put W2 after U returns. With W1 < U < W2 in commit order, a
//! description and items of one instant satisfy: U's GSI in the description
//! implies W1 in the items, and W2 in the items implies U's GSI in the
//! description. Both are implications, and both hold when a backend reads
//! the definition and the items after W2, so the check also asserts that W2
//! is not in the items: W2 commits after U returns, and on every correct
//! backend U cannot commit inside the open's read window (PostgreSQL with a
//! separate data database makes U wait on the row until the data pin; every
//! other backend pinned before the pause). W2 in the items therefore means
//! the pause sat outside the read window and the scenario checked nothing;
//! mutants that moved both pauses to the top of the open (SQLite reading the
//! definition on the pool before `begin`, MongoDB reading the index
//! documents outside the session) passed without this
//! assertion and fail on it. That assertion is live only when W2 commits
//! while the open is held, so the check also asserts which branch resumed
//! the open against [`Expectations::update_table_waits_at`]: where U does
//! not wait on the open, W2's commit must resume it (within the 2 s
//! timeout), and where U waits (PostgreSQL, separate data database,
//! `AfterDefinition`), the timeout must. The sixth round's mutant, the
//! pauses before `begin` plus a 2.5 s delay in `UpdateTable`, passed the W2
//! assertion with W2 committed after the resume and fails on this one. `AfterDefinition` catches a definition read that is not
//! fenced against the data pin (PostgreSQL without `FOR SHARE`; MongoDB's
//! index documents read outside the session), `BeforeItems` an item read
//! that is not at the pinned snapshot (PostgreSQL at READ COMMITTED, where
//! the cursor takes a fresh snapshot; a MongoDB cursor outside the session;
//! a SQLite stream that reads its pages in fresh transactions).
//!
//! Key shapes: hash-only `S`, `N` and `B` keys and `S` hash with `S`, `N` or
//! `B` range key, at a page of three, with values whose bytewise order
//! differs from collation order, binary values whose base64 order differs
//! from their byte order, and range numbers (negative, fractional, 1E-130,
//! 9.9E125, 38-digit values a last digit apart) whose text order differs
//! from their numeric order. Each table is checked for completeness, duplicates
//! (a keyset page that repeats its last row) and, on a backend that reports
//! `key_ordered`, ascending `KeyOrder::compare` order.
//!
//! Liveness (lesson 5): a second snapshot taken after the writer stops must
//! show the writer's final state, which proves the writer wrote and the first
//! snapshot's view was a snapshot, not a frozen database.

use std::collections::BTreeSet;
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DescribeTableInput,
    GsiInput, Item, KeySchemaElement, KeyType, Projection, ProjectionType, ScalarAttributeType,
    TableKeyInfo, TableStatus,
};
use futures::StreamExt;

use crate::error::StorageError;
use crate::snapshot::paging;
use crate::{DataEngine, SnapshotEngine, TableEngine};

/// Number of items present before the snapshot opens and never deleted.
pub const BASE_ITEMS: usize = 600;
/// Items present before the snapshot opens that the writer deletes after.
pub const DOOMED_ITEMS: usize = 100;
/// Items the writer inserts after the snapshot opens.
pub const LATE_ITEMS: usize = 100;
/// Overwrite rounds the writer must complete while the stream is open.
pub const ROUNDS_DURING_STREAM: u32 = 2;

fn s(v: impl Into<String>) -> AttributeValue {
    AttributeValue::S(v.into())
}

fn n(v: impl std::fmt::Display) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn base_item(i: usize, version: u32) -> Item {
    [
        ("pk".to_owned(), s(format!("p{:03}", i % 20))),
        ("sk".to_owned(), n(i)),
        ("v".to_owned(), n(version)),
        ("g".to_owned(), s(format!("g{}", i % 7))),
    ]
    .into_iter()
    .collect()
}

fn doomed_item(i: usize) -> Item {
    [
        ("pk".to_owned(), s(format!("doomed{:02}", i % 5))),
        ("sk".to_owned(), n(i)),
        ("v".to_owned(), n(0)),
    ]
    .into_iter()
    .collect()
}

fn late_item(i: usize) -> Item {
    [
        ("pk".to_owned(), s(format!("late{:02}", i % 5))),
        ("sk".to_owned(), n(i)),
        ("v".to_owned(), n(0)),
    ]
    .into_iter()
    .collect()
}

fn key_of(item: &Item) -> Item {
    item.iter()
        .filter(|(k, _)| *k == "pk" || *k == "sk")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// `(pk, sk)` as a set key for identity checks.
fn sort_key(item: &Item) -> (String, i64) {
    let pk = match item.get("pk") {
        Some(AttributeValue::S(v)) => v.clone(),
        other => panic!("pk must be a string, got {other:?}"),
    };
    let sk = match item.get("sk") {
        Some(AttributeValue::N(v)) => v.parse::<i64>().expect("integer sort key"),
        other => panic!("sk must be a number, got {other:?}"),
    };
    (pk, sk)
}

/// The table the check creates: composite key (`pk` S, `sk` N) with one GSI.
#[must_use]
pub fn table_input(name: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: name.to_owned(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: ScalarAttributeType::N,
            },
            AttributeDefinition {
                attribute_name: "g".to_owned(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "gi".to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "g".to_owned(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        ..Default::default()
    }
}

async fn key_info<E: TableEngine>(engine: &E, account: &str, table: &str) -> TableKeyInfo {
    let desc = engine
        .describe_table(
            account,
            DescribeTableInput {
                table_name: table.to_owned(),
            },
        )
        .await
        .expect("describe the check table");
    TableKeyInfo {
        table_name: table.to_owned(),
        account_id: account.to_owned(),
        table_id: desc.table_id.clone(),
        key_schema: desc.key_schema.clone(),
        base_key_schema: desc.key_schema.clone(),
        attribute_definitions: desc.attribute_definitions.clone(),
        ..Default::default()
    }
}

/// Put `count` items of the check's shape (`pk`, `sk`, `v`) into an
/// existing table made with [`table_input`], for backend tests that need a
/// populated table outside [`run`].
pub async fn populate<E>(engine: &E, account: &str, table: &str, count: usize)
where
    E: TableEngine + DataEngine + Send + Sync,
{
    let ki = key_info(engine, account, table).await;
    for i in 0..count {
        put(engine, &ki, base_item(i, 0)).await;
    }
}

/// The lock behind [`hooks_lock`].
static HARNESS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// How long one holder may keep the hooks lock before a test waiting for it
/// panics naming the wait. A holder that hangs (a stream that never ends,
/// say) would otherwise block every other snapshot test in the binary
/// silently until the CI job's own timeout. Counted from when the current
/// holder took the lock, not from when the waiter queued: the suite runs
/// serially behind the lock, so with as many test threads as tests the last
/// one queues for nearly the whole suite while no holder is slow.
const HOOKS_LOCK_WAIT: Duration = Duration::from_secs(600);

/// When the current holder of [`HARNESS`] took it; `None` while it is free.
static HELD_SINCE: std::sync::Mutex<Option<std::time::Instant>> = std::sync::Mutex::new(None);

fn held_since() -> Option<std::time::Instant> {
    *HELD_SINCE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn set_held_since(since: Option<std::time::Instant>) {
    *HELD_SINCE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = since;
}

/// How long a drain in this module waits for the next item or the end of a
/// stream before it panics naming the stage and the items read so far. Well
/// above any drain here (the longest, the main scenario's, takes seconds)
/// and below [`HOOKS_LOCK_WAIT`], so the test that hangs is the one that
/// fails, not its neighbours.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(120);

/// Proof that the holder has the process-wide lock on the snapshot test
/// hooks (the page-size knob, the round-trip counter and the open pause).
/// [`run`] and [`run_refusals`] take it, so a check cannot run without it.
pub struct HooksGuard {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl Drop for HooksGuard {
    fn drop(&mut self) {
        set_held_since(None);
    }
}

/// Take the lock on the process-global snapshot test hooks. Hold it for the
/// whole body of any test that opens a snapshot or touches the [`paging`]
/// statics (module doc); it is a Tokio mutex, so a test that panicked while
/// holding it does not poison it for the next.
///
/// # Panics
///
/// When one holder has kept the lock for `HOOKS_LOCK_WAIT` (600 s) while this
/// test waits: that holder is the hung test to look at. A wait behind a
/// queue of holders that each finish in time does not panic, however long
/// the queue is.
pub async fn hooks_lock() -> HooksGuard {
    loop {
        let since = held_since();
        let deadline = since.unwrap_or_else(std::time::Instant::now) + HOOKS_LOCK_WAIT;
        if let Ok(guard) =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), HARNESS.lock()).await
        {
            set_held_since(Some(std::time::Instant::now()));
            return HooksGuard { _guard: guard };
        }
        // Panic only if the holder the deadline was counted from still holds
        // the lock; a newer holder gets its own `HOOKS_LOCK_WAIT`.
        let now = held_since();
        assert!(
            since.is_none() || now != since,
            "another test has held the snapshot hooks lock for {} s; it is the one that hangs",
            HOOKS_LOCK_WAIT.as_secs()
        );
    }
}

/// The next item of `stream`, or `None` at its end, within
/// [`DRAIN_TIMEOUT`]; panics naming `stage` and `read` (items read so far)
/// when neither arrives, so a stream that never ends fails the check instead
/// of hanging it and every test behind the hooks lock.
async fn next_within<S>(stream: &mut S, stage: &str, read: usize) -> Option<S::Item>
where
    S: futures::Stream + Unpin,
{
    tokio::time::timeout(DRAIN_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{stage}: the stream yielded neither an item nor its end within {} s after \
                 {read} item(s)",
                DRAIN_TIMEOUT.as_secs()
            )
        })
}

/// Drain `stream` into `into`, each item within [`DRAIN_TIMEOUT`] (see
/// [`next_within`]); `stage` names the drain in the panic.
async fn drain_within<S>(stream: &mut S, stage: &str, into: &mut Vec<Item>)
where
    S: futures::Stream<Item = Result<Item, StorageError>> + Unpin,
{
    while let Some(next) = next_within(stream, stage, into.len()).await {
        into.push(next.unwrap_or_else(|e| panic!("{stage}: snapshot stream item: {e}")));
    }
}

/// The members of a description that are not fixed at the snapshot instant,
/// cleared so two descriptions of one table compare equal: the table's and
/// each index's item count and size are the stats pass's last values,
/// `table_throughput_mode_summary` is set by the API layer's response
/// preparation and not by storage, and an index being deleted is dropped
/// from a snapshot's description by rule. Everything else (key schema,
/// attribute definitions, every index's definition and status, billing,
/// throughput, class, SSE, ARN, id, creation time, streams, TTL, deletion
/// protection, and `restore_summary`, which storage fills from its restore
/// record on a restored table) must agree.
fn comparable_description(mut description: extenddb_core::types::TableDescription) -> String {
    crate::snapshot::drop_deleting_indexes(&mut description);
    description.item_count = 0;
    description.table_size_bytes = 0;
    description.table_throughput_mode_summary = None;
    for gsi in description.global_secondary_indexes.iter_mut().flatten() {
        gsi.item_count = 0;
        gsi.index_size_bytes = 0;
    }
    for lsi in description.local_secondary_indexes.iter_mut().flatten() {
        lsi.item_count = 0;
        lsi.index_size_bytes = 0;
    }
    for vector in description.vector_indexes.iter_mut().flatten() {
        vector.item_count = 0;
        vector.index_size_bytes = 0;
    }
    serde_json::to_string_pretty(&description).expect("a description serializes")
}

/// Assert that `snapshot`'s description is `described`, compared by
/// [`comparable_description`].
fn assert_same_description(
    described: extenddb_core::types::TableDescription,
    snapshot: extenddb_core::types::TableDescription,
) {
    let want = comparable_description(described);
    let got = comparable_description(snapshot);
    assert!(
        want == got,
        "the snapshot's description is not DescribeTable's (counts and throughput mode summary \
         excluded); differing members:\n{}",
        description_diff(&want, &got)
    );
}

/// Describe `table` (quiescent, `ACTIVE`), open a snapshot of it, and
/// assert the two descriptions agree as [`run`] compares them. With
/// `restored`, the table must have a `restore_summary` (asserted on
/// `describe_table`'s side, so the comparison of it is not vacuous): a
/// backend's suite calls this on a table it has given a restore record.
///
/// # Panics
///
/// When the describe or the open fails, when `restored` and the table has
/// no `restore_summary`, or when the descriptions differ.
pub async fn assert_snapshot_describes_the_table<E>(
    hooks: &HooksGuard,
    engine: &E,
    account: &str,
    table: &str,
    restored: bool,
) where
    E: SnapshotEngine + TableEngine + Send + Sync,
{
    let _ = hooks;
    let described = engine
        .describe_table(
            account,
            DescribeTableInput {
                table_name: table.to_owned(),
            },
        )
        .await
        .expect("describe the table");
    assert_eq!(
        described.restore_summary.is_some(),
        restored,
        "precondition: restore_summary on DescribeTable's description: {:?}",
        described.restore_summary
    );
    let snapshot = engine
        .open_table_snapshot(account, table)
        .await
        .expect("open the snapshot");
    assert_same_description(described, snapshot.description);
}

/// The lines of `want` (`DescribeTable`'s description) and `got` (the
/// snapshot's) that the other does not have, for an assertion message. Both
/// are pretty-printed JSON of the same type, so a differing member shows as
/// one line on each side. Lines are compared without a trailing `,`: a member
/// present on one side only (`RestoreSummary`, say) changes whether the line
/// before it ends in one, and that line is not a difference.
fn description_diff(want: &str, got: &str) -> String {
    let only_in = |a: &str, b: &str| -> Vec<String> {
        let other: BTreeSet<&str> = b.lines().map(|l| l.trim().trim_end_matches(',')).collect();
        a.lines()
            .map(|l| l.trim().trim_end_matches(','))
            .filter(|l| !other.contains(l))
            .map(str::to_owned)
            .collect()
    };
    format!(
        "  only in DescribeTable's: {:?}\n  only in the snapshot's:  {:?}",
        only_in(want, got),
        only_in(got, want)
    )
}

async fn put<E: DataEngine>(engine: &E, key_info: &TableKeyInfo, item: Item) {
    engine
        .put_item(
            key_info,
            item,
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("put_item");
}

async fn delete<E: DataEngine>(engine: &E, key_info: &TableKeyInfo, key: &Item) {
    engine
        .delete_item(key_info, key, false, None, &ExpressionMaps::default(), None)
        .await
        .expect("delete_item");
}

/// Wait until the table is `ACTIVE`, calling `pump` between polls so a test
/// without background workers can drive the control plane itself.
pub async fn wait_active<E, F, Fut>(engine: &E, account: &str, table: &str, mut pump: F)
where
    E: TableEngine,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for _ in 0..600 {
        pump().await;
        let desc = engine
            .describe_table(
                account,
                DescribeTableInput {
                    table_name: table.to_owned(),
                },
            )
            .await
            .expect("describe");
        let indexes_active = desc
            .global_secondary_indexes
            .as_ref()
            .is_none_or(|g| g.iter().all(|g| g.index_status == "ACTIVE"));
        if desc.table_status == TableStatus::Active && indexes_active {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("table {table} did not become ACTIVE");
}

/// Sets the shared snapshot page size ([`paging::set_fetch_batch`]) and
/// restores the default when dropped, so a failed assertion cannot leave a
/// small page for the next test. The one helper for every backend's tests
/// and for [`run`]; hold [`hooks_lock`] while one is alive.
pub struct PageSize(());

impl PageSize {
    /// Set the page size to `rows` until the returned value is dropped.
    #[must_use]
    pub fn set(rows: usize) -> Self {
        paging::set_fetch_batch(rows);
        Self(())
    }
}

impl Drop for PageSize {
    fn drop(&mut self) {
        paging::set_fetch_batch(0);
    }
}

/// What the check expects of the backend under test.
#[derive(Debug, Clone, Copy)]
pub struct Expectations {
    /// Whether the backend reports `key_ordered` for hash-only and
    /// single-range-key tables (the SQL backends).
    pub key_ordered: bool,
    /// The most significant digits a number key may have: 38 (the service's
    /// limit) on the SQL backends, 34 on `MongoDB`, which stores number keys as
    /// Decimal128. Range numbers with more are expected to be refused with
    /// `StorageError::Validation` and are then not part of the table.
    pub numeric_key_digits: usize,
    /// The one-instant stages at which an `UpdateTable` issued while the open
    /// is held at the pause waits for the open to finish, so the pause is
    /// resumed by the 2 s timeout rather than by W2's commit: `PostgreSQL` with
    /// a separate data database at `AfterDefinition` (U takes the `tables`
    /// row `FOR UPDATE`, which the open holds `FOR SHARE` until the data pin);
    /// empty on `PostgreSQL` with one shared database, `SQLite` and `MongoDB`,
    /// where nothing the open holds makes U wait. The check asserts which
    /// branch ran (lesson 22): a backend whose U and W2 take longer than the
    /// timeout would otherwise pass the "W2 not in items" assertion with W2
    /// committed after the resume and the scenario checking nothing.
    pub update_table_waits_at: &'static [paging::OpenStage],
}

/// Significant digits of the longest `N` key attribute of `key` (0 when it
/// has none).
fn max_number_digits(key: &Item) -> usize {
    key.values()
        .filter_map(|v| match v {
            AttributeValue::N(n) => Some(n),
            _ => None,
        })
        .map(|n| {
            let mantissa = n
                .trim_start_matches('-')
                .split(['e', 'E'])
                .next()
                .unwrap_or_default();
            let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
            digits
                .trim_start_matches('0')
                .trim_end_matches('0')
                .len()
                .max(1)
        })
        .max()
        .unwrap_or(0)
}

/// Run the consistency check against `engine`. `hooks` is the caller's
/// [`hooks_lock`] guard. `table` must not exist, and
/// neither may tables named with it as a prefix (the check creates
/// `<table>_i0`, `<table>_i1` and `<table>_k<n>` too);
/// `pump` is called while waiting for a table to become `ACTIVE`;
/// `page_size` is the number of items every snapshot stream opened during
/// the main scenario fetches per round trip, set through
/// [`paging::set_fetch_batch`] here and restored to the default when the
/// check returns; `expect` states what the backend is known to do
/// ([`Expectations`]), so a backend that stops ordering, or that starts
/// refusing a number it accepted, fails instead of skipping an assertion.
///
/// After the main scenario (writes while a stream is read), the check runs
/// the one-instant scenario at each [`paging::OpenStage`] and the key-shape
/// scenario.
///
/// # Panics
///
/// Panics on any consistency violation, with the violation named, when
/// `page_size` is too large for the table to span several pages, and when the
/// backend did not page at `page_size` (fewer than two round trips after one
/// page plus one item).
#[allow(clippy::too_many_lines)]
pub async fn run<E, F, Fut>(
    hooks: &HooksGuard,
    engine: &E,
    account: &str,
    table: &str,
    page_size: usize,
    expect: Expectations,
    mut pump: F,
) where
    E: SnapshotEngine + TableEngine + DataEngine + Send + Sync,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    assert!(
        BASE_ITEMS + DOOMED_ITEMS > 3 * page_size,
        "page_size {page_size} must be well under the table size {} or the first fetch \
         holds the whole table and the check proves nothing",
        BASE_ITEMS + DOOMED_ITEMS
    );
    let _ = hooks;
    let _restore = PageSize::set(page_size);
    engine
        .create_table(account, table_input(table))
        .await
        .expect("create the check table");
    wait_active(engine, account, table, &mut pump).await;
    let ki = key_info(engine, account, table).await;

    for i in 0..BASE_ITEMS {
        put(engine, &ki, base_item(i, 0)).await;
    }
    for i in 0..DOOMED_ITEMS {
        put(engine, &ki, doomed_item(i)).await;
    }

    // The table is quiescent here (its puts are done, the writer has not
    // started), so storage's `describe_table` and the snapshot's
    // `description` must agree on everything that is not a running count:
    // the trait promises the table as DescribeTable would have reported it.
    let described = engine
        .describe_table(
            account,
            DescribeTableInput {
                table_name: table.to_owned(),
            },
        )
        .await
        .expect("describe the check table before the open");

    // Taken before the open: a backend may fetch its first page while
    // opening (MongoDB's `find` returns the first batch), and that fetch is
    // part of the paging the check relies on.
    let fetches_before_open = paging::fetches();
    let snapshot = engine
        .open_table_snapshot(account, table)
        .await
        .expect("open the snapshot");
    assert_eq!(snapshot.description.table_name, table);
    assert_eq!(
        snapshot.description.key_schema.len(),
        2,
        "composite key schema"
    );
    let gsis = snapshot
        .description
        .global_secondary_indexes
        .as_deref()
        .unwrap_or(&[]);
    assert_eq!(gsis.len(), 1, "the ACTIVE GSI is in the description");
    assert_eq!(gsis[0].index_name, "gi");
    assert_same_description(described, snapshot.description.clone());
    assert!(
        !snapshot.info.kind.is_empty(),
        "snapshot kind must name the mechanism"
    );
    let key_ordered = snapshot.key_ordered;
    assert_eq!(
        key_ordered, expect.key_ordered,
        "key_ordered for a single-range-key table"
    );
    let snapshot_description = snapshot.description;
    let mut items = snapshot.items;

    // Writer: runs only after the snapshot is open. Signals once the deletes,
    // inserts and ROUNDS_DURING_STREAM overwrite rounds have committed, then
    // keeps overwriting until told to stop. Reader and writer are joined in
    // this task; both make progress at their await points.
    let (committed_tx, committed_rx) = tokio::sync::oneshot::channel::<u32>();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let ki = ki.clone();
        let stop = stop.clone();
        async move {
            for i in 0..DOOMED_ITEMS {
                delete(engine, &ki, &key_of(&doomed_item(i))).await;
            }
            for i in 0..LATE_ITEMS {
                put(engine, &ki, late_item(i)).await;
            }
            let mut round = 0u32;
            let mut committed_tx = Some(committed_tx);
            loop {
                round += 1;
                for i in 0..BASE_ITEMS {
                    put(engine, &ki, base_item(i, round)).await;
                }
                if round == ROUNDS_DURING_STREAM
                    && let Some(tx) = committed_tx.take()
                {
                    let _ = tx.send(round);
                }
                if round >= ROUNDS_DURING_STREAM && stop.load(std::sync::atomic::Ordering::SeqCst) {
                    return round;
                }
            }
        }
    };

    // Reader: one page plus one item (so a second fetch has happened and the
    // backend transaction or cursor is provably still open), then hold until
    // the writer has committed everything it will commit during the stream,
    // then drain.
    let reader = {
        let stop = stop.clone();
        async move {
            let mut seen: Vec<Item> = Vec::new();
            let before_pause = page_size + 1;
            for _ in 0..before_pause {
                match next_within(&mut items, "main scenario, first page", seen.len()).await {
                    Some(Ok(item)) => seen.push(item),
                    Some(Err(e)) => panic!("snapshot stream failed: {e}"),
                    None => panic!("snapshot ended after fewer than {before_pause} items"),
                }
            }
            // The precondition the rest of the check rests on: the backend
            // paged at `page_size`, so item page_size + 1 came from a second
            // round trip with the snapshot still open.
            let fetches = paging::fetches() - fetches_before_open;
            assert!(
                fetches >= 2,
                "{fetches} round trip(s) after reading {before_pause} items: the backend is not \
                 paging at {page_size}, so the check would pass against anything"
            );
            tokio::time::timeout(Duration::from_secs(120), committed_rx)
                .await
                .expect("writer committed its rounds within the deadline")
                .expect("writer sent its round count");
            // Fetches recorded by the time the writer's rounds committed: the
            // drain below must fetch again, or the stream read everything
            // before the writes and the check above shows nothing.
            let fetches_at_commit = paging::fetches();
            drain_within(
                &mut items,
                "main scenario, drain after the writer committed",
                &mut seen,
            )
            .await;
            drop(items);
            let fetches_after_drain = paging::fetches();
            assert!(
                fetches_after_drain > fetches_at_commit,
                "no round trip after the writer committed ({fetches_at_commit} before, \
                 {fetches_after_drain} after the drain): the stream read the table before \
                 the writes, so the snapshot was not exercised"
            );
            stop.store(true, std::sync::atomic::Ordering::SeqCst);
            seen
        }
    };
    let (final_round, seen) = tokio::join!(writer, reader);

    // Safety: the stream is the table as of open.
    assert_eq!(
        seen.len(),
        BASE_ITEMS + DOOMED_ITEMS,
        "snapshot item count (base + doomed, no late)"
    );
    let mut keys = BTreeSet::new();
    for item in &seen {
        assert!(keys.insert(sort_key(item)), "duplicate item {item:?}");
        let pk = match item.get("pk") {
            Some(AttributeValue::S(v)) => v.as_str(),
            other => panic!("pk {other:?}"),
        };
        assert!(
            !pk.starts_with("late"),
            "item inserted after open is visible: {item:?}"
        );
        assert_eq!(
            item.get("v"),
            Some(&n(0)),
            "item overwritten after open shows a post-open version: {item:?}"
        );
    }
    for i in 0..DOOMED_ITEMS {
        assert!(
            keys.contains(&sort_key(&doomed_item(i))),
            "item deleted after open is missing: doomed {i}"
        );
    }
    if key_ordered {
        let order = extenddb_core::backup_format::KeyOrder::resolve(
            &snapshot_description.key_schema,
            &snapshot_description.attribute_definitions,
        )
        .expect("a key_ordered snapshot has an orderable key schema");
        for pair in seen.windows(2) {
            assert_eq!(
                order.compare(&pair[0], &pair[1]).expect("comparable keys"),
                std::cmp::Ordering::Less,
                "key_ordered snapshot must yield strictly ascending keys: {:?} then {:?}",
                key_of(&pair[0]),
                key_of(&pair[1])
            );
        }
    }

    // Liveness through the data: a fresh snapshot sees the writer's final
    // state, so the writes above were real and the first snapshot's view was
    // the snapshot's, not a frozen database.
    let mut after = engine
        .open_table_snapshot(account, table)
        .await
        .expect("open a second snapshot");
    let mut after_items: Vec<Item> = Vec::new();
    drain_within(
        &mut after.items,
        "liveness, second snapshot",
        &mut after_items,
    )
    .await;
    drop(after.items);
    assert_eq!(after_items.len(), BASE_ITEMS + LATE_ITEMS);
    assert!(
        after_items
            .iter()
            .filter(|i| !matches!(i.get("pk"), Some(AttributeValue::S(p)) if p.starts_with("late")))
            .all(|i| i.get("v") == Some(&n(final_round))),
        "second snapshot shows the final round {final_round}"
    );

    for (i, stage) in [
        paging::OpenStage::AfterDefinition,
        paging::OpenStage::BeforeItems,
    ]
    .into_iter()
    .enumerate()
    {
        one_instant(
            engine,
            account,
            &format!("{table}_i{i}"),
            stage,
            expect,
            &mut pump,
        )
        .await;
    }
    key_shapes(engine, account, table, expect, &mut pump).await;
}

/// Items in a one-instant or key-shape table before its snapshot opens, and
/// the page size those scenarios read at: several pages each.
const SMALL_PAGE: usize = 3;
const INSTANT_ITEMS: usize = 12;

fn named_item(pk: &str) -> Item {
    [
        ("pk".to_owned(), s(pk)),
        ("sk".to_owned(), n(0)),
        ("v".to_owned(), n(0)),
    ]
    .into_iter()
    .collect()
}

fn has_pk(items: &[Item], pk: &str) -> bool {
    items
        .iter()
        .any(|i| matches!(i.get("pk"), Some(AttributeValue::S(p)) if p == pk))
}

/// One instant: hold an open at `stage` and, while it waits, commit W1 (a new
/// item), then `UpdateTable` U (a GSI on a new attribute), then W2 (another
/// new item) once U has returned. Commit order is W1 < U < W2, so a snapshot
/// of one instant satisfies: the description has U's GSI only if the items
/// have W1, and the items have W2 only if the description has U's GSI. W2
/// must not be in the items at all: the pause is inside the open's read
/// window, so W2 commits after the instant (module doc).
///
/// The open resumes when W2 has committed or after 2 s, whichever is first.
/// On `PostgreSQL` with a separate data database at `AfterDefinition` U waits
/// on the `tables` row the open holds `FOR SHARE`, so the timeout resumes it
/// and U and W2 commit after the open; elsewhere W2 commits during the
/// pause. Which of the two happened is asserted against
/// `Expectations::update_table_waits_at`, since the "W2 not in items"
/// assertion is live only in the second case. The reader waits for W2
/// before it drains, so the drain's round trips are reads after the last
/// commit, and the check asserts the stream made at least one.
#[allow(clippy::too_many_lines)]
async fn one_instant<E, F, Fut>(
    engine: &E,
    account: &str,
    table: &str,
    stage: paging::OpenStage,
    expect: Expectations,
    pump: &mut F,
) where
    E: SnapshotEngine + TableEngine + DataEngine + Send + Sync,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    paging::set_fetch_batch(SMALL_PAGE);
    engine
        .create_table(account, table_input(table))
        .await
        .expect("create the one-instant table");
    wait_active(engine, account, table, &mut *pump).await;
    let ki = key_info(engine, account, table).await;
    for i in 0..INSTANT_ITEMS {
        put(engine, &ki, base_item(i, 0)).await;
    }
    let update: extenddb_core::types::UpdateTableInput =
        serde_json::from_value(serde_json::json!({
            "TableName": table,
            "AttributeDefinitions": [{"AttributeName": "u", "AttributeType": "S"}],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "gu",
                "KeySchema": [{"AttributeName": "u", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }}]
        }))
        .expect("UpdateTable input");

    let mut pause = paging::arm_open_pause(stage);
    let reached = pause.take_reached();
    let (w1_tx, w1_rx) = tokio::sync::oneshot::channel::<()>();
    // Some(fetches recorded when W2's put returned) once W2 has committed.
    let (w2_tx, w2_rx) = tokio::sync::watch::channel::<Option<u64>>(None);

    let controller = {
        let ki = ki.clone();
        let mut w2_seen = w2_rx.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(60), reached)
                .await
                .unwrap_or_else(|_| panic!("the open did not reach the {stage:?} pause"))
                .expect("pause dropped before the open reached it");
            put(engine, &ki, named_item("w1")).await;
            let _ = w1_tx.send(());
            let writes = async {
                engine
                    .update_table(account, update)
                    .await
                    .expect("UpdateTable adding a GSI");
                put(engine, &ki, named_item("w2")).await;
                let _ = w2_tx.send(Some(paging::fetches()));
            };
            let resume = async {
                let by_w2 =
                    tokio::time::timeout(Duration::from_secs(2), w2_seen.wait_for(Option::is_some))
                        .await
                        .is_ok();
                pause.resume();
                by_w2
            };
            let ((), by_w2) = tokio::join!(writes, resume);
            by_w2
        }
    };
    let reader = {
        let mut w2 = w2_rx.clone();
        async move {
            let snapshot = engine
                .open_table_snapshot(account, table)
                .await
                .expect("open the one-instant snapshot");
            assert!(w1_rx.await.is_ok(), "W1 committed during the pause");
            let fetches_at_w2 = {
                let committed =
                    tokio::time::timeout(Duration::from_secs(60), w2.wait_for(Option::is_some))
                        .await
                        .expect("W2 committed within the deadline")
                        .expect("W2 sender alive");
                (*committed).expect("W2 committed")
            };
            let mut items: Vec<Item> = Vec::new();
            let mut stream = snapshot.items;
            drain_within(&mut stream, &format!("one-instant {stage:?}"), &mut items).await;
            drop(stream);
            (snapshot.description, items, fetches_at_w2)
        }
    };
    let (by_w2, (description, items, fetches_at_w2)) = tokio::join!(controller, reader);
    let fetches_after_drain = paging::fetches();

    let has_gsi = description
        .global_secondary_indexes
        .iter()
        .flatten()
        .any(|g| g.index_name == "gu");
    let w1_in = has_pk(&items, "w1");
    let w2_in = has_pk(&items, "w2");
    eprintln!(
        "one-instant {stage:?} on {table}: resumed by {}, description has U's GSI: {has_gsi}, \
         W1 in items: {w1_in}, W2 in items: {w2_in}",
        if by_w2 {
            "W2's commit"
        } else {
            "the 2 s timeout"
        }
    );
    // Not vacuous: U and W2 completed (the controller expects both), and the
    // table as of either side of them is there.
    assert!(
        w2_rx.borrow().is_some(),
        "W2 committed by the end of the scenario"
    );
    // Which branch ran is known per backend and stage (`Expectations`), so
    // it is asserted: when U does not wait on the open, W2 must have
    // committed inside the pause (the "W2 not in items" assertion below is
    // live only then); when it does, the timeout must have resumed the open.
    // A slow U or W2 (over the 2 s timeout) on a backend where they should
    // not wait fails here instead of passing the scenario vacuously.
    let u_waits = expect.update_table_waits_at.contains(&stage);
    assert_eq!(
        by_w2,
        !u_waits,
        "{stage:?}: the open was resumed by {}, but on this backend UpdateTable {} at this \
         stage, so it should have been resumed by {}; with the wrong branch the W2 \
         assertion below checks nothing",
        if by_w2 {
            "W2's commit"
        } else {
            "the 2 s timeout"
        },
        if u_waits {
            "waits for the held open"
        } else {
            "does not wait for the held open"
        },
        if u_waits {
            "the 2 s timeout (U and W2 commit after the open)"
        } else {
            "W2's commit (U and W2 commit during the pause, within 2 s)"
        }
    );
    assert_eq!(
        items.len(),
        INSTANT_ITEMS + usize::from(w1_in) + usize::from(w2_in),
        "every pre-open item, once"
    );
    for i in 0..INSTANT_ITEMS {
        let (pk, sk) = sort_key(&base_item(i, 0));
        assert!(
            items
                .iter()
                .any(|it| it.get("pk") == Some(&s(pk.clone())) && it.get("sk") == Some(&n(sk))),
            "pre-open item {i} missing"
        );
    }
    assert!(
        !has_gsi || w1_in,
        "{stage:?}: the description has the GSI UpdateTable added after W1 committed, but \
         the items do not have W1: the definition is newer than the items"
    );
    assert!(
        !w2_in || has_gsi,
        "{stage:?}: the items have W2, committed after UpdateTable returned, but the \
         description does not have UpdateTable's GSI: the items are newer than the definition"
    );
    assert!(
        !w2_in,
        "{stage:?}: W2 committed while the open was held at the pause, yet the items have it: \
         the pause is outside the open's read window, so this scenario checked nothing"
    );
    assert!(
        fetches_after_drain > fetches_at_w2,
        "{stage:?}: no round trip after W2 committed ({fetches_at_w2} then, \
         {fetches_after_drain} after the drain): the stream read its items before the writes"
    );
}

/// Key shapes other than the main scenario's `S` hash with `N` range.
#[derive(Debug, Clone, Copy)]
enum Shape {
    HashS,
    HashN,
    HashB,
    HashSRangeS,
    HashSRangeN,
    HashSRangeB,
}

/// Strings whose bytewise order differs from collation order: case, `_`,
/// `-`, `~`, a non-ASCII letter, digits as text.
const TEXT_KEYS: [&str; 19] = [
    "a", "B", "_", "\u{e9}", "A", "b", "Z", "z", "-", "10", "9", "ab", "aB", "Ab", "~", "\u{e9}1",
    "a_", "a-", "0",
];
/// Normalized numbers whose text order differs from numeric order.
const NUMBER_KEYS: [&str; 14] = [
    "10", "9", "-1", "100", "2", "-20", "33", "1000", "7", "-3", "0", "15", "1", "22",
];
/// Range numbers whose text order differs from numeric order, inserted out
/// of order: negatives, fractions, the smallest and largest magnitudes a
/// number may have, and two 38-digit and two 34-digit values a last digit
/// apart (a lossy numeric type or an order-preserving text encoding that
/// drops digits would tie or swap them; `MongoDB` stores 34 digits and refuses
/// the 38-digit pair, see [`Expectations`]). Normalized before the put, as
/// the engine stores them.
const RANGE_NUMBER_KEYS: [&str; 14] = [
    "10",
    "-9.5",
    "12345678901234567890123456789012345679",
    "0",
    "1E-130",
    "-100",
    "9.9E125",
    "0.5",
    "-0.001",
    "12345678901234567890123456789012345678",
    "2",
    "-1",
    "1234567890123456789012345678901235",
    "1234567890123456789012345678901234",
];
/// Byte strings whose base64 order differs from their byte order (`[0xff]`
/// is `/w==`, `[0xfb]` is `+w==`, `[0x00]` is `AA==`).
const BINARY_KEYS: [&[u8]; 13] = [
    &[0x00],
    &[0xff],
    &[0xfb],
    &[0x3e],
    &[0x7f],
    &[0x80],
    &[0x01],
    &[0x00, 0x00],
    &[0xff, 0x00],
    &[0x10],
    &[0xf0],
    &[0x41],
    &[0x61],
];

impl Shape {
    const ALL: [Self; 6] = [
        Self::HashS,
        Self::HashN,
        Self::HashB,
        Self::HashSRangeS,
        Self::HashSRangeN,
        Self::HashSRangeB,
    ];

    fn types(self) -> (ScalarAttributeType, Option<ScalarAttributeType>) {
        match self {
            Self::HashS => (ScalarAttributeType::S, None),
            Self::HashN => (ScalarAttributeType::N, None),
            Self::HashB => (ScalarAttributeType::B, None),
            Self::HashSRangeS => (ScalarAttributeType::S, Some(ScalarAttributeType::S)),
            Self::HashSRangeN => (ScalarAttributeType::S, Some(ScalarAttributeType::N)),
            Self::HashSRangeB => (ScalarAttributeType::S, Some(ScalarAttributeType::B)),
        }
    }

    fn input(self, name: &str) -> CreateTableInput {
        let (hash, range) = self.types();
        let mut key_schema = vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }];
        let mut attribute_definitions = vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: hash,
        }];
        if let Some(range) = range {
            key_schema.push(KeySchemaElement {
                attribute_name: "sk".to_owned(),
                key_type: KeyType::Range,
            });
            attribute_definitions.push(AttributeDefinition {
                attribute_name: "sk".to_owned(),
                attribute_type: range,
            });
        }
        CreateTableInput {
            table_name: name.to_owned(),
            key_schema,
            attribute_definitions,
            billing_mode: Some(BillingMode::PayPerRequest),
            ..Default::default()
        }
    }

    /// The keys present before the open, in a fixed order that is not key
    /// order.
    fn keys(self) -> Vec<Item> {
        let key = |pk: AttributeValue, sk: Option<AttributeValue>| -> Item {
            std::iter::once(("pk".to_owned(), pk))
                .chain(sk.map(|sk| ("sk".to_owned(), sk)))
                .collect()
        };
        match self {
            Self::HashS => TEXT_KEYS.iter().map(|k| key(s(*k), None)).collect(),
            Self::HashN => NUMBER_KEYS
                .iter()
                .map(|k| key(AttributeValue::N((*k).to_owned()), None))
                .collect(),
            Self::HashB => BINARY_KEYS
                .iter()
                .map(|k| key(AttributeValue::B(k.to_vec()), None))
                .collect(),
            Self::HashSRangeS => ["a", "B", "\u{e9}"]
                .iter()
                .flat_map(|pk| {
                    ["a", "B", "_", "\u{e9}", "b", "Z", "-"]
                        .iter()
                        .map(|sk| key(s(*pk), Some(s(*sk))))
                })
                .collect(),
            Self::HashSRangeN => ["a", "B"]
                .iter()
                .flat_map(|pk| {
                    RANGE_NUMBER_KEYS.iter().map(|sk| {
                        let sk =
                            extenddb_core::validation::number::validate_and_normalize_number(sk)
                                .unwrap_or_else(|e| panic!("range number {sk}: {e:?}"));
                        key(s(*pk), Some(AttributeValue::N(sk)))
                    })
                })
                .collect(),
            Self::HashSRangeB => ["a", "B"]
                .iter()
                .flat_map(|pk| {
                    BINARY_KEYS
                        .iter()
                        .map(|sk| key(s(*pk), Some(AttributeValue::B(sk.to_vec()))))
                })
                .collect(),
        }
    }

    /// Keys inserted after the open, none of them in [`Shape::keys`].
    fn late_keys(self) -> Vec<Item> {
        let (hash, range) = self.types();
        (0..3u8)
            .map(|i| {
                let value = |ty: ScalarAttributeType| match ty {
                    ScalarAttributeType::S => s(format!("late{i}")),
                    ScalarAttributeType::N => AttributeValue::N(format!("5{i}5")),
                    ScalarAttributeType::B => AttributeValue::B(vec![0xee, i]),
                };
                std::iter::once(("pk".to_owned(), value(hash)))
                    .chain(range.map(|r| ("sk".to_owned(), value(r))))
                    .collect()
            })
            .collect()
    }
}

/// A key as a set member.
fn key_id(item: &Item) -> String {
    format!("{:?} {:?}", item.get("pk"), item.get("sk"))
}

fn with_version(key: &Item, version: u32) -> Item {
    let mut item = key.clone();
    item.insert("v".to_owned(), n(version));
    item
}

/// For each [`Shape`]: fill a table, open a snapshot at a page of three, read
/// one page plus one item, then delete three items, insert three and
/// overwrite the rest before draining. The stream must hold every pre-open
/// item once at its pre-open version and nothing inserted, and when
/// `key_ordered` (which must equal `expect.key_ordered`) ascend by
/// `KeyOrder::compare`. Duplicates catch a keyset page that repeats its last
/// row; the order catches a text key sorted by collation rather than
/// bytewise, a `B` range key sorted by base64 text where the format wants
/// bytes (the hash key compares as base64 text; the range key bytewise), and
/// an `N` key sorted as text.
#[allow(clippy::too_many_lines)]
async fn key_shapes<E, F, Fut>(
    engine: &E,
    account: &str,
    table: &str,
    expect: Expectations,
    pump: &mut F,
) where
    E: SnapshotEngine + TableEngine + DataEngine + Send + Sync,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    paging::set_fetch_batch(SMALL_PAGE);
    for (i, shape) in Shape::ALL.into_iter().enumerate() {
        let name = format!("{table}_k{i}");
        engine
            .create_table(account, shape.input(&name))
            .await
            .unwrap_or_else(|e| panic!("create the {shape:?} table: {e}"));
        wait_active(engine, account, &name, &mut *pump).await;
        let ki = key_info(engine, account, &name).await;
        // Numbers with more significant digits than the backend stores are
        // refused at the put (MongoDB's Decimal128), so they are not part of
        // the table; the refusal is asserted, not tolerated.
        let (keys, too_long): (Vec<Item>, Vec<Item>) = shape
            .keys()
            .into_iter()
            .partition(|key| max_number_digits(key) <= expect.numeric_key_digits);
        for key in &too_long {
            match engine
                .put_item(
                    &ki,
                    with_version(key, 0),
                    false,
                    None,
                    &ExpressionMaps::default(),
                    None,
                )
                .await
            {
                Err(StorageError::Validation(_)) => {}
                other => panic!(
                    "{shape:?}: a number key with more than {} significant digits is refused \
                     with a validation error, got {other:?} for {}",
                    expect.numeric_key_digits,
                    key_id(key)
                ),
            }
        }
        if !too_long.is_empty() {
            eprintln!(
                "{shape:?}: {} key(s) over {} significant digits refused at the put",
                too_long.len(),
                expect.numeric_key_digits
            );
        }
        assert!(keys.len() > 3 * SMALL_PAGE, "{shape:?}: several pages");
        for key in &keys {
            put(engine, &ki, with_version(key, 0)).await;
        }

        let fetches_before_open = paging::fetches();
        let snapshot = engine
            .open_table_snapshot(account, &name)
            .await
            .unwrap_or_else(|e| panic!("open the {shape:?} snapshot: {e}"));
        assert_eq!(
            snapshot.key_ordered, expect.key_ordered,
            "{shape:?}: key_ordered"
        );
        let description = snapshot.description;
        let mut stream = snapshot.items;
        let mut seen = Vec::new();
        for _ in 0..=SMALL_PAGE {
            seen.push(
                next_within(
                    &mut stream,
                    &format!("key shape {shape:?}, first page"),
                    seen.len(),
                )
                .await
                .unwrap_or_else(|| panic!("{shape:?}: stream ended early"))
                .expect("stream item"),
            );
        }
        assert!(
            paging::fetches() - fetches_before_open >= 2,
            "{shape:?}: the backend is not paging at {SMALL_PAGE}"
        );
        for key in &keys[..3] {
            delete(engine, &ki, key).await;
        }
        for key in shape.late_keys() {
            put(engine, &ki, with_version(&key, 0)).await;
        }
        for key in &keys[3..] {
            put(engine, &ki, with_version(key, 1)).await;
        }
        let fetches_at_commit = paging::fetches();
        drain_within(
            &mut stream,
            &format!("key shape {shape:?}, drain"),
            &mut seen,
        )
        .await;
        drop(stream);
        assert!(
            paging::fetches() > fetches_at_commit,
            "{shape:?}: no round trip after the writes committed"
        );

        let expected: BTreeSet<String> = keys.iter().map(key_id).collect();
        let mut got = BTreeSet::new();
        for item in &seen {
            let id = key_id(item);
            assert!(got.insert(id.clone()), "{shape:?}: duplicate item {id}");
            assert_eq!(
                item.get("v"),
                Some(&n(0)),
                "{shape:?}: item {id} shows a post-open version"
            );
        }
        assert_eq!(
            got, expected,
            "{shape:?}: the stream is the table as of open"
        );
        if expect.key_ordered {
            let order = extenddb_core::backup_format::KeyOrder::resolve(
                &description.key_schema,
                &description.attribute_definitions,
            )
            .expect("orderable key schema");
            for pair in seen.windows(2) {
                assert_eq!(
                    order.compare(&pair[0], &pair[1]).expect("comparable keys"),
                    std::cmp::Ordering::Less,
                    "{shape:?}: not in key order: {} then {}",
                    key_id(&pair[0]),
                    key_id(&pair[1])
                );
            }
        }
    }
}

/// Checks that a snapshot cannot be opened on a missing or non-`ACTIVE`
/// table. `hooks` is the caller's [`hooks_lock`] guard. `table` must not
/// exist; `set_updating` flips that table's catalog
/// status to `UPDATING` through the backend test's own catalog access, so
/// every backend covers the non-`ACTIVE` refusal with the same assertion.
pub async fn run_refusals<E, F, Fut, S, SFut>(
    hooks: &HooksGuard,
    engine: &E,
    account: &str,
    table: &str,
    pump: F,
    set_updating: S,
) where
    E: SnapshotEngine + TableEngine + Send + Sync,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
    S: FnOnce(String) -> SFut,
    SFut: std::future::Future<Output = ()>,
{
    let _ = hooks;
    let err = engine
        .open_table_snapshot(account, "does_not_exist_for_snapshot")
        .await
        .expect_err("missing table refused");
    assert!(matches!(err, StorageError::TableNotFound(_)), "{err}");

    engine
        .create_table(account, table_input(table))
        .await
        .expect("create the refusal table");
    wait_active(engine, account, table, pump).await;
    engine
        .open_table_snapshot(account, table)
        .await
        .expect("an ACTIVE table opens");
    set_updating(table.to_owned()).await;
    let err = engine
        .open_table_snapshot(account, table)
        .await
        .expect_err("a non-ACTIVE table is refused");
    assert!(matches!(err, StorageError::TableNotActive(_)), "{err}");
}

#[cfg(test)]
mod tests {
    use super::description_diff;

    /// A member on one side only is the one difference reported, not also
    /// the line before it whose trailing comma it changes.
    #[test]
    fn description_diff_ignores_a_trailing_comma() {
        let want = "{\n  \"A\": 1,\n  \"B\": false,\n  \"RestoreSummary\": {}\n}";
        let got = "{\n  \"A\": 1,\n  \"B\": false\n}";
        assert_eq!(
            description_diff(want, got),
            "  only in DescribeTable's: [\"\\\"RestoreSummary\\\": {}\"]\n  only in the snapshot's:  []"
        );
    }
}

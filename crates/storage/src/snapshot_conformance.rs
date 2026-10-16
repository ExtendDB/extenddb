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
//! backend while the writer runs. Every backend fetches a page at a time, so
//! the caller passes the page size and the setter that applies it to the
//! backend; `run` calls the setter before it opens anything and requires the
//! table to span several pages (`BASE_ITEMS + DOOMED_ITEMS > 3 * page_size`).
//! The reader reads one page plus one item, which forces a second fetch, then
//! pauses until the writer has committed, then drains. A paged reader that
//! re-queries per page without a snapshot fails the version or count
//! assertion: the second review round of #395 ran such mutants against this
//! check at a page of 50 on all three backends (SQLite: fresh `BEGIN` per
//! page; PostgreSQL: `OFFSET`/`LIMIT` in READ COMMITTED; MongoDB: a plain
//! majority-read cursor outside the session) and each failed. With the
//! production page of 1000 the first fetch would hold the whole table and
//! the check would pass against anything, which is why `run` owns the page
//! size rather than trusting the caller to have set it.
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

/// `(pk, sk)` as a sortable pair: bytewise string, then numeric.
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

/// Run the consistency check against `engine`. `table` must not exist;
/// `pump` is called while waiting for the table to become `ACTIVE`;
/// `page_size` is the number of items the backend's snapshot stream will
/// fetch per round trip and `set_page_size` is the backend's test hook that
/// applies it, called here before the first snapshot opens so the check
/// cannot run against a backend still at its production page size.
///
/// # Panics
///
/// Panics on any consistency violation, with the violation named, and when
/// `page_size` is too large for the table to span several pages.
#[allow(clippy::too_many_lines)]
pub async fn run<E, S, F, Fut>(
    engine: &E,
    account: &str,
    table: &str,
    page_size: usize,
    set_page_size: S,
    pump: F,
) where
    E: SnapshotEngine + TableEngine + DataEngine + Send + Sync,
    S: FnOnce(usize),
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    assert!(
        BASE_ITEMS + DOOMED_ITEMS > 3 * page_size,
        "page_size {page_size} must be well under the table size {} or the first fetch \
         holds the whole table and the check proves nothing",
        BASE_ITEMS + DOOMED_ITEMS
    );
    set_page_size(page_size);
    engine
        .create_table(account, table_input(table))
        .await
        .expect("create the check table");
    wait_active(engine, account, table, pump).await;
    let ki = key_info(engine, account, table).await;

    for i in 0..BASE_ITEMS {
        put(engine, &ki, base_item(i, 0)).await;
    }
    for i in 0..DOOMED_ITEMS {
        put(engine, &ki, doomed_item(i)).await;
    }

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
    assert!(
        !snapshot.info.kind.is_empty(),
        "snapshot kind must name the mechanism"
    );
    let key_ordered = snapshot.key_ordered;
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
                match items.next().await {
                    Some(Ok(item)) => seen.push(item),
                    Some(Err(e)) => panic!("snapshot stream failed: {e}"),
                    None => panic!("snapshot ended after fewer than {before_pause} items"),
                }
            }
            tokio::time::timeout(Duration::from_secs(120), committed_rx)
                .await
                .expect("writer committed its rounds within the deadline")
                .expect("writer sent its round count");
            while let Some(next) = items.next().await {
                seen.push(next.expect("snapshot stream item"));
            }
            drop(items);
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
        let mut sorted = seen.iter().map(sort_key).collect::<Vec<_>>();
        sorted.sort();
        assert_eq!(
            seen.iter().map(sort_key).collect::<Vec<_>>(),
            sorted,
            "key_ordered snapshot must yield ascending (pk, sk)"
        );
    }

    // Liveness through the data: a fresh snapshot sees the writer's final
    // state, so the writes above were real and the first snapshot's view was
    // the snapshot's, not a frozen database.
    let after = engine
        .open_table_snapshot(account, table)
        .await
        .expect("open a second snapshot");
    let after_items: Vec<Item> = after
        .items
        .map(|r| r.expect("second snapshot item"))
        .collect()
        .await;
    assert_eq!(after_items.len(), BASE_ITEMS + LATE_ITEMS);
    assert!(
        after_items
            .iter()
            .filter(|i| !matches!(i.get("pk"), Some(AttributeValue::S(p)) if p.starts_with("late")))
            .all(|i| i.get("v") == Some(&n(final_round))),
        "second snapshot shows the final round {final_round}"
    );
}

/// Checks that a snapshot cannot be opened on a missing or non-`ACTIVE`
/// table.
pub async fn run_refusals<E>(engine: &E, account: &str)
where
    E: SnapshotEngine + Send + Sync,
{
    let err = engine
        .open_table_snapshot(account, "does_not_exist_for_snapshot")
        .await
        .expect_err("missing table refused");
    assert!(matches!(err, StorageError::TableNotFound(_)), "{err}");
}

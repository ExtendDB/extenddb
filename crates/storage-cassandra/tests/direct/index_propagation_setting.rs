// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `index_propagation_delay_ms` setting applies to the next write (#407).
//!
//! SQLite and PostgreSQL read the setting live on each write to a table with
//! secondary indexes, so `extenddb settings set index_propagation_delay_ms 0`
//! makes the next write synchronous. These tests change the setting through
//! the catalog store, the way `settings set` does from another process, and
//! check that the next write follows it.
//!
//! Each test runs on its own keyspace prefix, so the catalog `settings` table
//! it changes is its own, and other tests running at the same time keep the
//! default.

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{AttributeValue, Item, TableKeyInfo};
use extenddb_storage::DataEngine as _;
use extenddb_storage::management_store::SettingsStore;
use extenddb_storage_cassandra::{CassandraCatalogStore, CassandraEngine};

const GSI: &str = "GsiIdx";
const GSI_PK: &str = "gpk";

/// An engine on a keyspace prefix of its own, with one account and a table
/// with one GSI that sets no delay of its own, so the GSI follows the system
/// setting.
struct Fixture {
    engine: std::sync::Arc<CassandraEngine>,
    settings: CassandraCatalogStore,
    key_info: TableKeyInfo,
    account_keyspace: String,
    isolated: crate::helpers::IsolatedEngine,
}

async fn fixture(tag: &str) -> Fixture {
    let isolated = crate::helpers::isolated_engine(&format!("ips{tag}")).await;
    let account =
        crate::helpers::isolated_gsi_account(&isolated, "PropagationSetting", GSI, GSI_PK).await;
    Fixture {
        engine: isolated.engine.clone(),
        settings: isolated.settings(),
        key_info: account.key_info,
        account_keyspace: account.keyspace,
        isolated,
    }
}

impl Fixture {
    async fn put(&self, id: &str, gsi_pk: &str) {
        let mut item = Item::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        item.insert(GSI_PK.to_string(), AttributeValue::S(gsi_pk.to_string()));
        self.engine
            .put_item(
                &self.key_info,
                item,
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put_item");
    }

    /// One GSI query, no polling: items the index holds right now.
    async fn gsi_count(&self, gsi_pk: &str) -> usize {
        let key_condition = KeyCondition {
            pk_path: vec![PathElement::Attribute(GSI_PK.to_string())],
            pk_value: Expr::Placeholder(":v".to_string()),
            sk_condition: None,
            extra_pk_conditions: vec![],
            extra_sk_conditions: vec![],
        };
        let mut maps = ExpressionMaps::default();
        maps.values
            .insert(":v".to_string(), AttributeValue::S(gsi_pk.to_string()));
        self.engine
            .query(
                &self.key_info,
                &key_condition,
                &maps,
                true,
                None,
                None,
                Some(GSI),
            )
            .await
            .map(|(items, _)| items.len())
            .expect("query GSI")
    }

    /// Queued `gsi_pending` entries, not counting a partition's static-only
    /// row (`last_ready_at` with no clustering row).
    async fn pending(&self) -> usize {
        use cdrs_tokio::types::IntoRustByName as _;
        let rows = self
            .engine
            .session()
            .query(&format!(
                "SELECT id FROM {}.gsi_pending",
                self.account_keyspace
            ))
            .await
            .unwrap()
            .response_body()
            .unwrap()
            .into_rows()
            .unwrap_or_default();
        rows.iter()
            .filter(|row| {
                let id: Result<uuid::Uuid, _> = row.get_r_by_name("id");
                id.is_ok()
            })
            .count()
    }
}

/// `index_propagation_delay_ms = 0`, set after the engine started, makes the
/// next write synchronous: the GSI holds the item at once and nothing is
/// queued.
#[tokio::test]
async fn setting_zero_makes_the_next_write_synchronous() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let f = fixture("zero").await;
    f.settings
        .set_setting("index_propagation_delay_ms", "0")
        .await
        .unwrap();

    f.put("a", "g-zero").await;

    assert_eq!(
        f.pending().await,
        0,
        "the write was queued for asynchronous propagation; the setting of 0 was not applied"
    );
    assert_eq!(
        f.gsi_count("g-zero").await,
        1,
        "the GSI does not hold the item"
    );
}

/// A change from 0 back to a delay applies to the next write too: the first
/// write is synchronous, the second is queued.
#[tokio::test]
async fn setting_change_applies_to_the_next_write() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let f = fixture("chg").await;
    f.settings
        .set_setting("index_propagation_delay_ms", "0")
        .await
        .unwrap();
    f.put("a", "g-sync").await;
    assert_eq!(
        f.pending().await,
        0,
        "the first write was queued; the setting of 0 was not applied"
    );

    f.settings
        .set_setting("index_propagation_delay_ms", "60000")
        .await
        .unwrap();
    f.put("b", "g-async").await;

    assert_eq!(
        f.pending().await,
        1,
        "the second write was not queued; the setting of 60000 was not applied"
    );
    assert_eq!(f.gsi_count("g-sync").await, 1);
    assert_eq!(f.gsi_count("g-async").await, 0);
}

/// A catalog that holds the value only under the pre-rename name
/// `gsi_propagation_delay_ms` is honoured, as on SQLite and PostgreSQL.
#[tokio::test]
async fn legacy_setting_name_is_honoured() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let f = fixture("old").await;
    f.settings
        .set_setting("gsi_propagation_delay_ms", "0")
        .await
        .unwrap();

    f.put("a", "g-old").await;

    assert_eq!(
        f.pending().await,
        0,
        "the write was queued; the legacy setting of 0 was not applied"
    );
    assert_eq!(f.gsi_count("g-old").await, 1);
}

/// With the GSI workers running, a write under a 50 ms delay reaches the GSI
/// within a small multiple of that delay, not after the workers' 1 s idle
/// sleep (#407). The bar, 500 ms, is the one
/// `tests/test_gsi_async.py::test_gsi_configured_delay_range` uses on a
/// served build.
///
/// The keyspace prefix is this test's own, so only this engine's workers
/// can apply the row: a worker of another test, polling once a second,
/// cannot make this pass by chance.
#[tokio::test]
async fn delayed_write_reaches_the_gsi_within_its_delay() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const DELAY_MS: &str = "50";
    const BAR: std::time::Duration = std::time::Duration::from_millis(500);
    const FLOOR: std::time::Duration = std::time::Duration::from_millis(25);
    const GIVE_UP: std::time::Duration = std::time::Duration::from_secs(5);

    let f = fixture("lat").await;
    f.settings
        .set_setting("index_propagation_delay_ms", DELAY_MS)
        .await
        .unwrap();
    let _workers = extenddb_storage_cassandra::workers::spawn_gsi_workers(f.engine.clone());
    // Let the workers reach their first idle sleep, as on a running server.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut observed = Vec::new();
    for i in 0..3 {
        let gsi_pk = format!("g-lat-{i}");
        let started = std::time::Instant::now();
        f.put(&format!("item-{i}"), &gsi_pk).await;
        while f.gsi_count(&gsi_pk).await == 0 {
            assert!(
                started.elapsed() < GIVE_UP,
                "write {i} did not reach the GSI within {GIVE_UP:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        observed.push(started.elapsed());
    }
    eprintln!("delay {DELAY_MS} ms; write-to-GSI times: {observed:?}");
    assert!(
        observed.iter().all(|t| *t < BAR),
        "with a {DELAY_MS} ms delay, writes reached the GSI after {observed:?}; \
         expected each under {BAR:?}"
    );
    // Half the delay: a write that skipped its delay lands in a few ms.
    assert!(
        observed.iter().all(|t| *t >= FLOOR),
        "with a {DELAY_MS} ms delay, writes reached the GSI after {observed:?}; \
         expected each at {FLOOR:?} or later"
    );
}

/// A queued write waits for its delay: under a 2,000 ms delay, the GSI does
/// not hold the item 300 ms after the put, with the workers running.
#[tokio::test]
async fn delayed_write_waits_for_its_delay() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const EARLY: std::time::Duration = std::time::Duration::from_millis(300);
    const GIVE_UP: std::time::Duration = std::time::Duration::from_secs(10);

    let f = fixture("wait").await;
    f.settings
        .set_setting("index_propagation_delay_ms", "2000")
        .await
        .unwrap();
    let _workers = extenddb_storage_cassandra::workers::spawn_gsi_workers(f.engine.clone());

    let started = std::time::Instant::now();
    f.put("a", "g-wait").await;
    tokio::time::sleep(EARLY.saturating_sub(started.elapsed())).await;
    assert_eq!(
        f.gsi_count("g-wait").await,
        0,
        "under a 2000 ms delay, the GSI held the item {:?} after the put",
        started.elapsed()
    );

    // It does arrive: the queued row was applied, not lost.
    while f.gsi_count("g-wait").await == 0 {
        assert!(
            started.elapsed() < GIVE_UP,
            "the write did not reach the GSI within {GIVE_UP:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A write that lands while its worker is in a pass still reaches the GSI
/// promptly: the wake it sends is kept until the pass ends, so the worker
/// does not sleep its full 1 s idle wait past the write (#407).
///
/// The setup makes the case happen on every round:
/// - Empty queue keyspaces make a pass slow (tens of ms).
/// - The test's own account is listed first, so a pass reads it before the
///   second write commits, and does not see that write.
/// - Write `first` goes to one worker and `second` to another. `first` wakes
///   every worker, so `second`'s worker is in a pass, finding nothing, when
///   `second` lands.
///
/// If the worker dropped that wake, it would find no next due time and sleep
/// 1 s, so `second` would take over 1 s.
#[tokio::test]
async fn write_during_a_pass_reaches_the_gsi_promptly() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const BAR: std::time::Duration = std::time::Duration::from_millis(500);
    const GIVE_UP: std::time::Duration = std::time::Duration::from_secs(5);
    const MIN_PASS: std::time::Duration = std::time::Duration::from_millis(40);
    const MAX_FILLERS: usize = 80;
    const ROUNDS: usize = 5;

    let f = fixture("wake").await;
    f.settings
        .set_setting("index_propagation_delay_ms", "50")
        .await
        .unwrap();

    // Add empty keyspaces listed after the test's account, until a pass takes
    // MIN_PASS. Ones listed before it are dropped.
    let (mut fillers, mut created) = (0, 0);
    loop {
        let started = std::time::Instant::now();
        extenddb_storage_cassandra::workers::run_gsi_pass(&f.engine, 0)
            .await
            .unwrap();
        let pass = started.elapsed();
        if pass >= MIN_PASS {
            eprintln!("{fillers} empty keyspaces; one pass takes {pass:?}");
            break;
        }
        assert!(
            fillers < MAX_FILLERS && created < 4 * MAX_FILLERS,
            "a pass over {fillers} keyspaces took only {pass:?}; need {MIN_PASS:?} \
             ({created} made, the ones listed before the test's account dropped)"
        );
        created += 10;
        for _ in 0..10 {
            crate::helpers::isolated_queue_keyspace(&f.isolated).await;
        }
        let listed = crate::helpers::isolated_account_keyspaces(&f.isolated).await;
        let ours = listed
            .iter()
            .position(|name| *name == f.account_keyspace)
            .expect("the test's account is listed");
        for early in &listed[..ours] {
            f.engine
                .session()
                .query(format!("DROP KEYSPACE IF EXISTS {early}"))
                .await
                .unwrap();
        }
        fillers = listed.len() - ours - 1;
    }

    let worker = |id: &str| {
        let mut item = Item::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        extenddb_storage_cassandra::gsi_queue::worker_for_item(&item, &f.key_info.key_schema)
            .unwrap()
    };
    let _workers = extenddb_storage_cassandra::workers::spawn_gsi_workers(f.engine.clone());
    // Let every worker reach its idle wait.
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;

    let mut observed = Vec::new();
    for round in 0..ROUNDS {
        // Keys for two different workers.
        let first = format!("first-{round}");
        let second = (0..)
            .map(|n| format!("second-{round}-{n}"))
            .find(|id| worker(id) != worker(&first))
            .unwrap();
        let gsi_pk = format!("g-wake-{round}");

        f.put(&first, &format!("{gsi_pk}-first")).await;
        let started = std::time::Instant::now();
        f.put(&second, &gsi_pk).await;
        while f.gsi_count(&gsi_pk).await == 0 {
            assert!(
                started.elapsed() < GIVE_UP,
                "round {round}: the second write did not reach the GSI within {GIVE_UP:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        observed.push(started.elapsed());
        // Let the workers go idle again before the next round.
        tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    }
    eprintln!("write-during-a-pass times: {observed:?}");
    assert!(
        observed.iter().all(|t| *t < BAR),
        "a write made during its worker's pass reached the GSI after {observed:?}; \
         expected each under {BAR:?}"
    );
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TTL micro-benchmarks against a live Cassandra.
//!
//! Ignored by default: numbers from a single-node container are only
//! meaningful *relative to each other* on the same machine, so these run
//! manually around a change, never in CI:
//!
//! ```text
//! cargo test -p extenddb-storage-cassandra --test ttl_bench --release -- \
//!     --ignored --test-threads=1 --nocapture
//! ```
//!
//! Reported: put latency on a TTL-enabled vs plain table (the write-path tax),
//! and expiration drain throughput (the sweep ceiling).

#[path = "common/mod.rs"]
mod helpers;

use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::{DataEngine, MetadataEngine};

use crate::helpers::setup_engine;

const PUT_SAMPLES: usize = 200;
const EXPIRED_ITEMS: usize = 500;

async fn activate_tables(engine: &extenddb_storage_cassandra::CassandraEngine) {
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
}

fn percentiles(mut samples: Vec<u128>) -> (u128, u128, u128) {
    samples.sort_unstable();
    let pick = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    (pick(0.50), pick(0.90), pick(0.99))
}

async fn bench_puts(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    key_info: &extenddb_core::types::TableKeyInfo,
    with_ttl_attribute: bool,
    label: &str,
) {
    // Each case writes its own key range (the label disambiguates), so a case
    // never measures overwriting another case's rows — a TTL-table put over a
    // row whose previous image carried a timestamp pays entry-retirement work
    // a fresh insert does not.
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 86_400;
    let mut samples = Vec::with_capacity(PUT_SAMPLES);
    for index in 0..PUT_SAMPLES {
        let mut item = Item::new();
        item.insert(
            "id".to_owned(),
            AttributeValue::S(format!("bench-{label}-{index}")),
        );
        item.insert(
            "value".to_owned(),
            AttributeValue::S("x".repeat(256).to_owned()),
        );
        if with_ttl_attribute {
            item.insert(
                "expires_at".to_owned(),
                AttributeValue::N(future.to_string()),
            );
        }
        let started = std::time::Instant::now();
        engine
            .put_item(key_info, item, false, None, &Default::default(), None)
            .await
            .expect("bench put");
        samples.push(started.elapsed().as_micros());
    }
    let (p50, p90, p99) = percentiles(samples);
    eprintln!("BENCH {label}: n={PUT_SAMPLES} p50={p50}us p90={p90}us p99={p99}us");
}

/// Put latency: plain table vs TTL-enabled table (with and without the item
/// actually carrying a timestamp — the claim is taken either way).
#[tokio::test]
#[ignore = "manual benchmark; requires live Cassandra and a quiet machine"]
async fn bench_put_latency_ttl_vs_plain() {
    let engine = setup_engine().await;

    let plain = crate::helpers::TestTable::new(&engine, "BenchPlain", false).await;
    let ttl = crate::helpers::TestTable::new(&engine, "BenchTtl", false).await;
    activate_tables(&engine).await;
    engine
        .update_ttl(
            &ttl.key_info.account_id,
            &ttl.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .unwrap();
    engine
        .create_ttl_index(
            &ttl.key_info.account_id,
            &ttl.key_info.table_name,
            "expires_at",
        )
        .await
        .unwrap();

    bench_puts(&engine, &plain.key_info, false, "put/plain-table").await;
    bench_puts(&engine, &ttl.key_info, true, "put/ttl-table+timestamp").await;
    bench_puts(&engine, &ttl.key_info, false, "put/ttl-table-no-timestamp").await;
}

/// Expiration drain throughput: how fast the sweep clears a backlog.
#[tokio::test]
#[ignore = "manual benchmark; requires live Cassandra and a quiet machine"]
async fn bench_expiration_drain_throughput() {
    use extenddb_core::metrics::MetricsCollector;

    let engine = setup_engine().await;
    let table = crate::helpers::TestTable::new(&engine, "BenchDrain", false).await;
    activate_tables(&engine).await;
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .unwrap();
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    for index in 0..EXPIRED_ITEMS {
        let mut item = Item::new();
        item.insert("id".to_owned(), AttributeValue::S(format!("drain-{index}")));
        item.insert(
            "expires_at".to_owned(),
            AttributeValue::N((now - 60).to_string()),
        );
        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                None,
            )
            .await
            .expect("seed put");
    }

    let metrics = MetricsCollector::new();
    async fn remaining(
        engine: &extenddb_storage_cassandra::CassandraEngine,
        key_info: &extenddb_core::types::TableKeyInfo,
    ) -> usize {
        let mut left = 0usize;
        for index in 0..EXPIRED_ITEMS {
            let mut key = Item::new();
            key.insert("id".to_owned(), AttributeValue::S(format!("drain-{index}")));
            if engine.get_item(key_info, &key).await.unwrap().is_some() {
                left += 1;
            }
        }
        left
    }
    let started = std::time::Instant::now();
    let mut sweeps = 0usize;
    loop {
        extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &metrics).await;
        sweeps += 1;
        // Full count, not a last-item probe: row failures are confined, so the
        // last item can be gone while others remain.
        if remaining(&engine, &table.key_info).await == 0 {
            break;
        }
        assert!(sweeps <= 50, "drain did not complete within 50 sweeps");
    }
    let elapsed = started.elapsed();
    eprintln!(
        "BENCH drain: {EXPIRED_ITEMS} items in {sweeps} sweep passes, {:.1}s total, {:.0} items/sec sweep-time (verified all {EXPIRED_ITEMS} gone)",
        elapsed.as_secs_f64(),
        EXPIRED_ITEMS as f64 / elapsed.as_secs_f64()
    );
}

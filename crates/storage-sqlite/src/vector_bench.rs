//! Measurement scaffolding for the vector scan, not part of the shipped surface.
//!
//! Ignored by default because it is a timing harness rather than an assertion:
//! it exists so a performance claim about the scan can be backed by a number
//! instead of by reading the code. Run it with
//!
//! ```text
//! VB_ITEMS=20000 VB_DIMS=384 VB_PAYLOAD=2000 \
//!   cargo test -p extenddb-storage-sqlite --lib vector_bench -- --ignored --nocapture
//! ```
//!
//! # What this harness established
//!
//! The scan is bound by per-row overhead in the row-streaming layer, not by any
//! vector-specific work. Measured at 10,000 items, 384 dimensions and a 2KB
//! non-indexed attribute, roughly 30ms per search, about 3us per row:
//!
//! - Removing `item_data` from the projection entirely: no change.
//! - Skipping the blob decode and the distance computation entirely: no change
//!   (34ms with no per-row work at all, against 25 to 28ms doing all of it).
//! - Replacing the indexed distance loops with iterator `zip`: no change.
//!
//! So the JSON parse of the stored item, which the scan performs for every row in
//! the partition and which looked like the obvious cost because the item carries
//! the vector as decimal strings, is free relative to streaming the row. Two
//! changes were tried and reverted: deferring the parse until a candidate can
//! enter the retained set, and scoring straight from the stored bytes into a
//! reused buffer. Interleaved against the unmodified code over six alternating
//! pairs, that version measured 48.6ms against 29.5ms with no overlap in range,
//! so it was reproducibly slower while doing strictly less work, for a reason
//! this harness did not isolate.
//!
//! # Measuring on a shared machine
//!
//! Take the minimum of many repetitions and interleave the two binaries under
//! comparison, alternating between them. A median of seven on an eight-core box
//! at load average 3 was not adequate: the unmodified code measured 59.1ms and
//! then 32.7ms on two consecutive runs of the identical binary, which is a wider
//! spread than any of the effects being investigated.
//!
//! `VB_PAYLOAD` is the width in bytes of a non-indexed attribute on each item.
//! It was expected to be the variable that matters, since the scan reads
//! `item_data` for every row in the partition. It is not: widening it from 200
//! bytes to 2KB did not slow the scan down, and the two measured in the opposite
//! order to the prediction.

#[cfg(test)]
mod tests {
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::{DataEngine, VectorSearch};
    use serde_json::json;
    use std::time::Instant;

    fn env_usize(key: &str, default: usize) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    /// Deterministic pseudo-random vector, so two runs of the harness measure the
    /// same work and a before/after comparison is meaningful.
    fn vector_for(seed: u64, dims: usize) -> Vec<f32> {
        let mut s = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let mut v = Vec::with_capacity(dims);
        for _ in 0..dims {
            s = s.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            #[allow(clippy::cast_precision_loss)]
            v.push(((s >> 33) as f32 / u32::MAX as f32) - 0.5);
        }
        v
    }

    #[tokio::test]
    #[ignore = "timing harness, run explicitly"]
    async fn vector_bench_scan() {
        let items = env_usize("VB_ITEMS", 5_000);
        let dims = env_usize("VB_DIMS", 384);
        let payload = env_usize("VB_PAYLOAD", 1_000);
        let top_k = env_usize("VB_TOPK", 10);
        let reps = env_usize("VB_REPS", 5);

        let engine = crate::SqliteEngine::new(":memory:", 1, "us-east-1", 4_096_000)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        let account = "000000000000";
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(account)
            .execute(&engine.pool)
            .await
            .expect("account");

        // Zero delay so index maintenance applies inline with each write. The
        // default is asynchronous, which would leave the index empty at search
        // time and measure a scan over nothing.
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '0')
             ON CONFLICT(key) DO UPDATE SET value = '0'",
        )
        .execute(&engine.pool)
        .await
        .expect("delay 0");

        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [{
                "IndexName": "vidx",
                "Dimensions": dims,
                "DistanceFunction": "COSINE",
                "VectorAttribute": {"AttributeName": "emb"},
                "Projection": {"ProjectionType": "ALL"}
            }]
        }))
        .expect("input");
        engine
            .create_table_impl(account, input)
            .await
            .expect("create table");

        // CreateTable returns with the table CREATING and the ACTIVE flip owned by
        // the control-plane worker, which this harness does not run. Flipped
        // directly because the transition is not what is being measured. The
        // vector index needs no flip: the inline create path sets it ACTIVE.
        sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE account_id = ?")
            .bind(account)
            .execute(&engine.pool)
            .await
            .expect("activate");

        let key_info = engine
            .fetch_table_key_info(account, "t")
            .await
            .expect("key info");
        let maps = ExpressionMaps::default();

        // Written through the real put path so the index rows are built exactly as
        // production builds them; a hand-rolled INSERT could encode differently
        // and measure something the server never does.
        let filler: String = "x".repeat(payload);
        for i in 0..items {
            let mut item = Item::new();
            item.insert("pk".to_owned(), AttributeValue::S(format!("k{i:07}")));
            item.insert(
                "emb".to_owned(),
                AttributeValue::L(
                    vector_for(i as u64, dims)
                        .into_iter()
                        .map(|f| AttributeValue::N(f.to_string()))
                        .collect(),
                ),
            );
            item.insert("filler".to_owned(), AttributeValue::S(filler.clone()));
            engine
                .put_item_impl(&key_info, item, false, None, &maps, None)
                .await
                .expect("put item");
        }

        let query = vector_for(u64::MAX, dims);
        let search = || VectorSearch {
            key_info: &key_info,
            index_name: "vidx",
            query_vector: &query,
            top_k: i64::try_from(top_k).expect("top_k fits"),
            hash_key: None,
            filters: &[],
        };
        let vector_engine = engine.as_vector_search().expect("vector capable");

        // Discarded: the first search warms SQLite's page cache, so including it
        // would report cache-miss cost as if it were steady state.
        let _ = vector_engine.search_vectors(search()).await.expect("warm");

        let mut times = Vec::with_capacity(reps);
        for _ in 0..reps {
            let started = Instant::now();
            let out = vector_engine
                .search_vectors(search())
                .await
                .expect("search");
            let elapsed = started.elapsed();
            assert_eq!(out.hits.len(), top_k.min(items), "unexpected hit count");
            times.push(elapsed.as_secs_f64() * 1000.0);
        }
        times.sort_by(|a, b| a.partial_cmp(b).expect("finite"));

        println!(
            "VBENCH items={items} dims={dims} payload={payload} top_k={top_k} \
             reps={reps} median_ms={:.2} min_ms={:.2} max_ms={:.2}",
            times[times.len() / 2],
            times[0],
            times[times.len() - 1],
        );
    }
}

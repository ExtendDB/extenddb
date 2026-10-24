// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Regression test for issue #403 (part 1): the connection pool size must
//! follow `max_connections`.
//!
//! cdrs-tokio defaults to ONE connection per node. ExtendDB's config carries
//! `max_connections` (default 10) and advertises it through
//! `ConnectionConfig::max_connections`, but `create_session` never applied it,
//! so every deployment ran a single connection regardless of config — the
//! precondition for the stream-id-exhaustion failure in #403, and a plain
//! false promise (config says 10, driver opens 1).
//!
//! This test builds two engines with different `max_connections` and checks,
//! via the node's `system_views.clients`, that the larger pool opens strictly
//! more client connections than the smaller one, and at least as many as its
//! configured size. It asserts a comparison rather than an exact count because
//! the driver also opens a control connection and other clients may be present;
//! the comparison isolates the effect of `max_connections` without depending on
//! that overhead.

#[cfg(test)]
mod tests {
    use crate::helpers::test_config;
    use extenddb_storage_cassandra::CassandraEngine;

    /// Count client connections the node currently sees from the loopback
    /// address, queried through the given engine's own session. Counts rows
    /// client-side rather than using CQL `COUNT(*)`, whose aggregate column type
    /// on a virtual table is awkward to deserialize.
    async fn client_connection_count(engine: &CassandraEngine) -> i64 {
        let body = engine
            .session()
            .query("SELECT address, port FROM system_views.clients")
            .await
            .expect("query system_views.clients")
            .response_body()
            .expect("response body");
        let rows = body.into_rows().unwrap_or_default();
        i64::try_from(rows.len()).unwrap_or(i64::MAX)
    }

    #[tokio::test]
    async fn pool_size_follows_max_connections() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }

        let small_size: u32 = 2;
        let large_size: u32 = 8;

        // Build the small-pool engine and measure how many connections the node
        // sees from us. Each engine opens its own pool, so we measure each
        // against its own session.
        let mut small_cfg = test_config();
        small_cfg.max_connections = small_size;
        small_cfg.ensure_cached_connection_string();
        let small_engine = CassandraEngine::new(&small_cfg, "us-east-1")
            .await
            .expect("build small-pool engine");
        // Give the pool a moment to finish opening every connection.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let small_count = client_connection_count(&small_engine).await;

        let mut large_cfg = test_config();
        large_cfg.max_connections = large_size;
        large_cfg.ensure_cached_connection_string();
        let large_engine = CassandraEngine::new(&large_cfg, "us-east-1")
            .await
            .expect("build large-pool engine");
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        // Count AFTER the large engine is up: this reflects both pools plus any
        // other clients. The delta attributable to the large engine is
        // large_total - small_count; asserting it carries large_size proves the
        // config was applied and scales.
        let both_count = client_connection_count(&large_engine).await;

        // Before the fix, create_session ignored max_connections and opened one
        // connection per engine, so both engines together added ~2 and this
        // assertion fails. With the fix, the large engine alone adds >= 8.
        let large_delta = both_count - small_count;
        assert!(
            large_delta >= i64::from(large_size),
            "large pool (max_connections={large_size}) added only {large_delta} connections \
             (small_count={small_count}, both_count={both_count}); expected at least \
             {large_size} — max_connections is not being applied to the pool (issue #403)"
        );
        assert!(
            large_delta > i64::from(small_size),
            "large pool did not open more connections than the small pool \
             (delta={large_delta}, small_size={small_size}) — pool size does not follow \
             max_connections (issue #403)"
        );
    }
}

// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Checks that `max_connections` reaches the driver's per-node pool (#403).
//!
//! The unit test on `pool_size_for` only covers the mapping. It still passes
//! if the engine stops handing the pool config to the session builder, in
//! which case the driver falls back to a pool of one. This test asks the
//! driver itself how many distinct connections the node's pool holds.
//!
//! The pool hands out connections in strict round-robin order, so 16 picks
//! from a pool of 4 visit every connection. The count is of this engine's own
//! pool, not server-side sockets, so other clients cannot change it.

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use extenddb_storage_cassandra::CassandraEngine;

    #[tokio::test]
    async fn max_connections_reaches_the_driver_pool() {
        if crate::helpers::skip_without_cassandra() {
            return;
        }

        const POOL: u32 = 4;

        let mut cfg = crate::helpers::test_config();
        cfg.max_connections = POOL;
        cfg.ensure_cached_connection_string();
        let engine = CassandraEngine::new(&cfg, "us-east-1").await.unwrap();

        // The driver creates a node's pool on first use.
        engine
            .session()
            .query("SELECT release_version FROM system.local")
            .await
            .unwrap();

        let nodes = engine.session().cluster_metadata().unignored_nodes();
        assert!(!nodes.is_empty(), "driver reports no nodes");

        let mut seen = HashSet::new();
        for _ in 0..(POOL * 4) {
            let conn = nodes[0].persistent_connection().await.unwrap();
            seen.insert(Arc::as_ptr(&conn) as usize);
        }

        assert_eq!(
            seen.len(),
            POOL as usize,
            "the node's pool should hold max_connections connections"
        );
    }
}

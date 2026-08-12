//! ServerRuntimeHooks implementation that spawns the TTL worker (and any
//! other background tasks added later) when the server starts.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};

use crate::BigtableEngine;
use crate::config::BigtableStorageConfig;
use crate::ttl_worker;

pub struct BigtableRuntimeHooks {
    pub engine: Arc<BigtableEngine>,
    pub config: BigtableStorageConfig,
}

#[async_trait]
impl ServerRuntimeHooks for BigtableRuntimeHooks {
    async fn spawn_workers(&self, _ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
        // Ensure the 2PC coordinator-log table exists. Idempotent — admin
        // create_table swallows AlreadyExists.
        if let Err(e) = crate::transact::ensure_txn_log_table(self.engine.client_ref()).await {
            tracing::warn!("could not ensure __extenddb_txn_log__ table: {e}");
        }
        let engine = self.engine.clone();
        let cadence = Duration::from_secs(self.config.ttl_scan_cadence_secs.max(1));
        let h1 = tokio::spawn(async move {
            ttl_worker::run(engine, cadence).await;
        });

        let engine_sweep = self.engine.clone();
        let sweep_cadence = Duration::from_secs(self.config.sweeper_cadence_secs.max(1));
        let h2 = tokio::spawn(async move {
            crate::sweeper::run(engine_sweep, sweep_cadence).await;
        });

        let engine = self.engine.clone();
        let gsi_cadence = Duration::from_secs(self.config.gsi_reconcile_cadence_secs.max(1));
        let h3 = tokio::spawn(async move {
            crate::gsi_worker::run(engine, gsi_cadence).await;
        });

        vec![h1, h2, h3]
    }

    fn backend_info(&self) -> Option<String> {
        Some(format!(
            "ttl_scan_cadence_secs={}, gsi_reconcile_cadence_secs={}, sweeper_cadence_secs={}",
            self.config.ttl_scan_cadence_secs,
            self.config.gsi_reconcile_cadence_secs,
            self.config.sweeper_cadence_secs,
        ))
    }
}

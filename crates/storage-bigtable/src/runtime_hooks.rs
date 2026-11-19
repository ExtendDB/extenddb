// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ServerRuntimeHooks` implementation that spawns the Bigtable TTL sweeper,
//! 2PC recovery sweeper, GSI reconciler, stream-record 24h retention cleanup,
//! idempotency-token 10m cleanup, and table-size refresh workers on server start.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use extenddb_core::metrics::{MetricsCollector, QuerySource};
use extenddb_storage::hooks::{
    CancellationToken, ServerRuntimeHooks, WorkerContext, sleep_or_shutdown,
};
use extenddb_storage::{DataEngine, MetadataEngine, StreamEngine};

use crate::BigtableEngine;
use crate::config::BigtableStorageConfig;
use crate::ttl_worker;

const STREAM_CLEANUP_INTERVAL: Duration = Duration::from_secs(3_600);
const STREAM_RETENTION_HOURS: i64 = 24;
const IDEMPOTENCY_CLEANUP_INTERVAL: Duration = Duration::from_secs(600);
const IDEMPOTENCY_MAX_AGE_SECS: i64 = 600;
const TABLE_SIZE_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

pub struct BigtableRuntimeHooks {
    pub engine: Arc<BigtableEngine>,
    pub config: BigtableStorageConfig,
}

#[async_trait]
impl ServerRuntimeHooks for BigtableRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
        if let Err(e) = crate::transact::ensure_txn_log_table(self.engine.client_ref()).await {
            tracing::warn!("could not ensure __extenddb_txn_log__ table: {e}");
        }
        if let Err(e) = ttl_worker::ensure_ttl_index_table(self.engine.client_ref()).await {
            tracing::warn!("could not ensure __extenddb_ttl_index__ table: {e}");
        }

        let engine = self.engine.clone();
        let cadence = Duration::from_secs(self.config.ttl_scan_cadence_secs.max(1));
        let metrics = Some(ctx.metrics.clone());
        let shutdown = ctx.shutdown.clone();
        let ttl_handle = tokio::spawn(async move {
            ttl_worker::run_with_metrics(engine, cadence, metrics, shutdown).await;
        });

        let engine_sweep = self.engine.clone();
        let sweep_cadence = Duration::from_secs(self.config.sweeper_cadence_secs.max(1));
        let shutdown_sweep = ctx.shutdown.clone();
        let sweeper_handle = tokio::spawn(async move {
            crate::sweeper::run(engine_sweep, sweep_cadence, shutdown_sweep).await;
        });

        let engine_gsi = self.engine.clone();
        let gsi_cadence = Duration::from_secs(self.config.gsi_reconcile_cadence_secs.max(1));
        let shutdown_gsi = ctx.shutdown.clone();
        let gsi_handle = tokio::spawn(async move {
            crate::gsi_worker::run(engine_gsi, gsi_cadence, shutdown_gsi).await;
        });

        let engine_stream_gc = self.engine.clone();
        let metrics_stream_gc = ctx.metrics.clone();
        let shutdown_stream_gc = ctx.shutdown.clone();
        let stream_cleanup_handle = tokio::spawn(async move {
            run_stream_record_cleanup(engine_stream_gc, metrics_stream_gc, shutdown_stream_gc)
                .await;
        });

        let engine_idem_gc = self.engine.clone();
        let metrics_idem_gc = ctx.metrics.clone();
        let shutdown_idem_gc = ctx.shutdown.clone();
        let idempotency_cleanup_handle = tokio::spawn(async move {
            run_idempotency_token_cleanup(engine_idem_gc, metrics_idem_gc, shutdown_idem_gc).await;
        });

        let engine_size = self.engine.clone();
        let shutdown_size = ctx.shutdown.clone();
        let table_size_handle = tokio::spawn(async move {
            run_table_size_refresh(engine_size, shutdown_size).await;
        });

        vec![
            ttl_handle,
            sweeper_handle,
            gsi_handle,
            stream_cleanup_handle,
            idempotency_cleanup_handle,
            table_size_handle,
        ]
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

async fn run_stream_record_cleanup(
    engine: Arc<BigtableEngine>,
    metrics: Arc<MetricsCollector>,
    shutdown: CancellationToken,
) {
    let lock_name = "stream_record_cleanup";
    let owner = engine.node_id();
    let lease = STREAM_CLEANUP_INTERVAL * 2;
    while sleep_or_shutdown(&shutdown, STREAM_CLEANUP_INTERVAL).await {
        let cat = engine.cat();
        if let Ok(true) = cat.try_lock(lock_name, owner, lease).await {
            let start = Instant::now();
            match StreamEngine::cleanup_expired_stream_records(&*engine, STREAM_RETENTION_HOURS)
                .await
            {
                Ok(n) => {
                    if n > 0 {
                        tracing::info!("bigtable stream cleanup deleted {n} expired record(s)");
                    }
                    #[allow(clippy::cast_precision_loss)]
                    metrics.record_worker_success(
                        QuerySource::StreamCleanup,
                        start.elapsed().as_micros() as f64,
                    );
                }
                Err(e) => {
                    tracing::error!("bigtable stream record cleanup failed: {e}");
                    metrics.record_worker_error(QuerySource::StreamCleanup);
                }
            }
            let _ = cat.release_lock(lock_name, owner).await;
        }
    }
}

async fn run_idempotency_token_cleanup(
    engine: Arc<BigtableEngine>,
    metrics: Arc<MetricsCollector>,
    shutdown: CancellationToken,
) {
    let lock_name = "idempotency_token_cleanup";
    let owner = engine.node_id();
    let lease = IDEMPOTENCY_CLEANUP_INTERVAL * 2;
    while sleep_or_shutdown(&shutdown, IDEMPOTENCY_CLEANUP_INTERVAL).await {
        let cat = engine.cat();
        if let Ok(true) = cat.try_lock(lock_name, owner, lease).await {
            let start = Instant::now();
            match DataEngine::cleanup_expired_idempotency_tokens(&*engine, IDEMPOTENCY_MAX_AGE_SECS)
                .await
            {
                Ok(n) => {
                    if n > 0 {
                        tracing::info!("bigtable idempotency cleanup deleted {n} expired token(s)");
                    }
                    #[allow(clippy::cast_precision_loss)]
                    metrics.record_worker_success(
                        QuerySource::IdempotencyCleanup,
                        start.elapsed().as_micros() as f64,
                    );
                }
                Err(e) => {
                    tracing::error!("bigtable idempotency token cleanup failed: {e}");
                    metrics.record_worker_error(QuerySource::IdempotencyCleanup);
                }
            }
            let _ = cat.release_lock(lock_name, owner).await;
        }
    }
}

async fn run_table_size_refresh(engine: Arc<BigtableEngine>, shutdown: CancellationToken) {
    let lock_name = "table_size_refresh";
    let owner = engine.node_id();
    let lease = TABLE_SIZE_REFRESH_INTERVAL * 2;
    while sleep_or_shutdown(&shutdown, TABLE_SIZE_REFRESH_INTERVAL).await {
        let cat = engine.cat();
        if let Ok(true) = cat.try_lock(lock_name, owner, lease).await {
            if let Ok(tables) = MetadataEngine::all_active_tables(&*engine).await {
                for (account_id, table_name) in &tables {
                    if let Err(e) =
                        MetadataEngine::refresh_table_size(&*engine, account_id, table_name).await
                    {
                        tracing::warn!("bigtable table_size_refresh ({table_name}): {e}");
                    }
                }
            }
            let _ = cat.release_lock(lock_name, owner).await;
        }
    }
}

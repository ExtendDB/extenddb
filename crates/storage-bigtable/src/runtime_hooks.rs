// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ServerRuntimeHooks` implementation for the Bigtable storage backend.
//!
//! Spawns the 5 background workers (`ttl_worker`, `sweeper`, `gsi_worker`,
//! `stream_record_cleanup`, and `idempotency_token_cleanup`), exposes a shared
//! distributed worker-lease helper with background lease renewal, and carries
//! the server's `ADR-0023` [`ServingRegistrar`].

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use extenddb_core::metrics::{MetricsCollector, QuerySource};
use extenddb_storage::hooks::{
    CancellationToken, ServerRuntimeHooks, ServingRegistrar, WorkerContext, sleep_or_shutdown,
};

use crate::BigtableEngine;
use crate::catalog::Catalog;
use crate::config::BigtableStorageConfig;
use crate::data::client::BigtableClient;
use crate::ttl_worker;

const STREAM_CLEANUP_INTERVAL: Duration = Duration::from_hours(1);
const STREAM_RETENTION_HOURS: i64 = 24;
const IDEMPOTENCY_CLEANUP_INTERVAL: Duration = Duration::from_mins(10);
const IDEMPOTENCY_MAX_AGE_SECS: i64 = 600;
const LEASE_RENEW_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Spawn a background renewal task for a held catalog lease `lock:<lock_name>`,
/// renewing every `lease_duration / 3` and cancelling `cancel_on_loss` if
/// ownership is lost (`Ok(false)`) OR if transient renewal errors / timeouts
/// prevent confirmed renewal across `80%` of `lease_duration`.
pub(crate) fn spawn_catalog_lease_renewal(
    catalog_client: Arc<BigtableClient>,
    lock_name: String,
    owner: String,
    lease_duration: Duration,
    cancel_on_loss: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let renew_interval = (lease_duration / 3).max(Duration::from_secs(1));
    let safety_ms = u64::try_from(lease_duration.as_millis())
        .unwrap_or(u64::MAX)
        .saturating_mul(4)
        / 5;
    let safety_window = Duration::from_millis(safety_ms.max(1_000));

    tokio::spawn(async move {
        let mut last_renewed = Instant::now();
        let mut wait = renew_interval;
        loop {
            let remaining = safety_window.saturating_sub(last_renewed.elapsed());
            if remaining.is_zero() {
                tracing::warn!(
                    lock_name,
                    "catalog lease safety window elapsed without renewal; cancelling fenced pass"
                );
                cancel_on_loss.cancel();
                return;
            }
            if !sleep_or_shutdown(&cancel_on_loss, wait.min(remaining)).await {
                return;
            }
            let remaining = safety_window.saturating_sub(last_renewed.elapsed());
            if remaining.is_zero() {
                tracing::warn!(
                    lock_name,
                    "catalog lease safety window elapsed without renewal; cancelling fenced pass"
                );
                cancel_on_loss.cancel();
                return;
            }
            let sent = Instant::now();
            let bound = LEASE_RENEW_RPC_TIMEOUT.min(renew_interval).min(remaining);
            let renew_cat = Catalog::new(&catalog_client);
            match tokio::time::timeout(
                bound,
                renew_cat.renew_lock(&lock_name, &owner, lease_duration),
            )
            .await
            {
                Ok(Ok(true)) => {
                    last_renewed = sent;
                    wait = renew_interval;
                }
                Ok(Ok(false)) => {
                    tracing::warn!(
                        lock_name,
                        "catalog lease ownership lost during pass; cancelling fenced pass"
                    );
                    cancel_on_loss.cancel();
                    return;
                }
                Ok(Err(e)) => {
                    if last_renewed.elapsed() >= safety_window {
                        tracing::warn!(
                            lock_name,
                            error = %e,
                            "catalog lease renewal failed past safety window; cancelling fenced pass"
                        );
                        cancel_on_loss.cancel();
                        return;
                    }
                    tracing::warn!(lock_name, error = %e, "catalog lease renewal failed; retrying");
                    wait = Duration::from_secs(1).min(renew_interval);
                }
                Err(_) => {
                    if last_renewed.elapsed() >= safety_window {
                        tracing::warn!(
                            lock_name,
                            "catalog lease renewal timed out past safety window; cancelling fenced pass"
                        );
                        cancel_on_loss.cancel();
                        return;
                    }
                    tracing::warn!(lock_name, "catalog lease renewal timed out; retrying");
                    wait = Duration::from_secs(1).min(renew_interval);
                }
            }
        }
    })
}

/// Run a periodic background worker guarded by a distributed catalog lease
/// `lock:<lock_name>`, renewing the lease every `lease_duration / 3` while the
/// pass runs and cancelling `pass_token` immediately if renewal loses the lock
/// or exceeds its safety window.
pub async fn run_leased_worker<F, Fut>(
    catalog_client: Arc<BigtableClient>,
    lock_name: &'static str,
    owner: String,
    cadence: Duration,
    lease_duration: Duration,
    shutdown: CancellationToken,
    mut work: F,
) where
    F: FnMut(CancellationToken) -> Fut,
    Fut: Future<Output = ()>,
{
    while sleep_or_shutdown(&shutdown, cadence).await {
        let cat = Catalog::new(&catalog_client);
        let acquired = match cat.try_lock(lock_name, &owner, lease_duration).await {
            Ok(acquired) => acquired,
            Err(e) => {
                tracing::debug!(lock_name, error = %e, "worker lease acquire skipped");
                continue;
            }
        };
        if !acquired {
            continue;
        }

        let pass_token = shutdown.child_token();
        let renewal_task = spawn_catalog_lease_renewal(
            catalog_client.clone(),
            lock_name.to_owned(),
            owner.clone(),
            lease_duration,
            pass_token.clone(),
        );

        work(pass_token.clone()).await;

        pass_token.cancel();
        renewal_task.abort();
        let _ = renewal_task.await;
        let _ = cat.release_lock(lock_name, &owner).await;
    }
}

pub struct BigtableRuntimeHooks {
    pub engine: Arc<BigtableEngine>,
    pub config: BigtableStorageConfig,
    pub registrar: Option<Arc<dyn ServingRegistrar>>,
}

#[async_trait]
impl ServerRuntimeHooks for BigtableRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
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

        vec![
            ttl_handle,
            sweeper_handle,
            gsi_handle,
            stream_cleanup_handle,
            idempotency_cleanup_handle,
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

    fn serving_registrar(&self) -> Option<Arc<dyn ServingRegistrar>> {
        self.registrar.clone()
    }
}

async fn run_stream_record_cleanup(
    engine: Arc<BigtableEngine>,
    metrics: Arc<MetricsCollector>,
    shutdown: CancellationToken,
) {
    let catalog_client = engine.catalog_client_arc();
    let owner = engine.node_id().to_owned();
    let lease = STREAM_CLEANUP_INTERVAL * 2;
    run_leased_worker(
        catalog_client,
        "stream_record_cleanup",
        owner,
        STREAM_CLEANUP_INTERVAL,
        lease,
        shutdown,
        |pass_token| {
            let engine = engine.clone();
            let metrics = metrics.clone();
            async move {
                let start = Instant::now();
                match engine
                    .cleanup_expired_stream_records_cancellable(STREAM_RETENTION_HOURS, &pass_token)
                    .await
                {
                    Ok(n) => {
                        if n > 0 {
                            tracing::info!("bigtable stream cleanup deleted {n} expired record(s)");
                        }
                        metrics.record_worker_success(
                            QuerySource::StreamCleanup,
                            start.elapsed().as_secs_f64() * 1_000_000.0,
                        );
                    }
                    Err(e) => {
                        tracing::error!("bigtable stream record cleanup failed: {e}");
                        metrics.record_worker_error(QuerySource::StreamCleanup);
                    }
                }
            }
        },
    )
    .await;
}

async fn run_idempotency_token_cleanup(
    engine: Arc<BigtableEngine>,
    metrics: Arc<MetricsCollector>,
    shutdown: CancellationToken,
) {
    let catalog_client = engine.catalog_client_arc();
    let owner = engine.node_id().to_owned();
    let lease = IDEMPOTENCY_CLEANUP_INTERVAL * 2;
    run_leased_worker(
        catalog_client,
        "idempotency_token_cleanup",
        owner,
        IDEMPOTENCY_CLEANUP_INTERVAL,
        lease,
        shutdown,
        |pass_token| {
            let engine = engine.clone();
            let metrics = metrics.clone();
            async move {
                let start = Instant::now();
                match engine
                    .cleanup_expired_idempotency_tokens_cancellable(
                        IDEMPOTENCY_MAX_AGE_SECS,
                        &pass_token,
                    )
                    .await
                {
                    Ok(n) => {
                        if n > 0 {
                            tracing::info!(
                                "bigtable idempotency cleanup deleted {n} expired token(s)"
                            );
                        }
                        metrics.record_worker_success(
                            QuerySource::IdempotencyCleanup,
                            start.elapsed().as_secs_f64() * 1_000_000.0,
                        );
                    }
                    Err(e) => {
                        tracing::error!("bigtable idempotency token cleanup failed: {e}");
                        metrics.record_worker_error(QuerySource::IdempotencyCleanup);
                    }
                }
            }
        },
    )
    .await;
}

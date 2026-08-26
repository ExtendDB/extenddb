// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ServerRuntimeHooks` for the DuckDB backend: spawns the background workers
//! (control-plane poller, table-size refresh, stream/idempotency cleanup, TTL
//! sweep).

use std::sync::Arc;

use async_trait::async_trait;
use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};

use crate::store::DuckDbEngine;
use crate::workers;

pub(crate) struct DuckDbRuntimeHooks {
    pub(crate) engine: Arc<DuckDbEngine>,
    pub(crate) control_plane_notify: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl ServerRuntimeHooks for DuckDbRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
        // Backend-specific workers. Each takes the shutdown token and returns
        // at its next tick after cancellation; the handles are returned so
        // `serve` can drain them.
        let engine = self.engine.clone();
        let notify = self.control_plane_notify.clone();
        let catalog_store = ctx.catalog_store.clone();
        let token = ctx.shutdown.clone();
        let control_plane = tokio::spawn(async move {
            workers::poll_control_plane_transitions(engine, notify, catalog_store, token).await;
        });

        let engine = self.engine.clone();
        let token = ctx.shutdown.clone();
        let table_size =
            tokio::spawn(async move { workers::table_size_refresh_worker(engine, token).await });

        let engine = self.engine.clone();
        let metrics = ctx.metrics.clone();
        let token = ctx.shutdown.clone();
        let stream_cleanup = tokio::spawn(async move {
            workers::stream_record_cleanup_worker(engine, metrics, token).await;
        });

        let engine = self.engine.clone();
        let metrics = ctx.metrics.clone();
        let token = ctx.shutdown.clone();
        let idempotency_cleanup = tokio::spawn(async move {
            workers::idempotency_token_cleanup_worker(engine, metrics, token).await;
        });

        let engine = self.engine.clone();
        let metrics = ctx.metrics.clone();
        let token = ctx.shutdown.clone();
        let ttl_cleanup =
            tokio::spawn(async move { workers::ttl_cleanup_worker(engine, metrics, token).await });

        // GSI propagation: drain gsi_pending after each write / on a sweep.
        let engine = self.engine.clone();
        let gsi_notify = engine.gsi_notify();
        let token = ctx.shutdown.clone();
        let gsi_propagation = tokio::spawn(async move {
            workers::gsi_propagation_worker(engine, gsi_notify, token).await;
        });

        // Keep the cached GSI propagation delay in sync with the setting.
        let index_delay_cache = self.engine.index_propagation_delay_cache.clone();
        let catalog_store = ctx.catalog_store.clone();
        let token = ctx.shutdown.clone();
        let gsi_delay = tokio::spawn(async move {
            workers::poll_index_propagation_delay(catalog_store, index_delay_cache, token).await;
        });

        vec![
            control_plane,
            table_size,
            stream_cleanup,
            idempotency_cleanup,
            ttl_cleanup,
            gsi_propagation,
            gsi_delay,
        ]
    }

    fn backend_info(&self) -> Option<String> {
        Some("storage=duckdb (single-file)".to_owned())
    }
}

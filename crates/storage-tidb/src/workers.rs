// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB-specific background workers.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::metrics::MetricsCollector;
use sqlx::MySqlPool;

use crate::TidbEngine;

const CONTROL_PLANE_ACTIVE_POLL_MAX: Duration = Duration::from_secs(1);
const CONTROL_PLANE_ACTIVE_WINDOW: Duration = Duration::from_secs(5);

pub(crate) async fn poll_control_plane_transitions(
    storage: Arc<TidbEngine>,
    notify: Arc<tokio::sync::Notify>,
) {
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

    loop {
        // Idle: wait for a wake signal or timeout (defensive sweep)
        let _ = tokio::time::timeout(IDLE_TIMEOUT, notify.notified()).await;

        // Active: process immediately, then sleep only until the next scheduled
        // transition or the bounded defensive poll interval.
        let deadline = tokio::time::Instant::now() + CONTROL_PLANE_ACTIVE_WINDOW;
        loop {
            match storage.process_control_plane_transitions().await {
                Ok(ref t) if t.is_empty() => {}
                Ok(transitions) => {
                    for (name, transition) in &transitions {
                        tracing::info!("Table '{name}': {transition}");
                    }
                }
                Err(e) => {
                    tracing::warn!("Control plane transition poll failed: {e}");
                    break;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }

            let sleep_for = match next_control_plane_poll_delay(&storage).await {
                Ok(delay) => delay,
                Err(e) => {
                    tracing::warn!("Control plane transition schedule probe failed: {e}");
                    CONTROL_PLANE_ACTIVE_POLL_MAX
                }
            };
            if sleep_for.is_zero() {
                continue;
            }

            tokio::select! {
                () = notify.notified() => {}
                () = tokio::time::sleep(sleep_for) => {}
            }
        }
    }
}

async fn next_control_plane_poll_delay(storage: &TidbEngine) -> Result<Duration, sqlx::Error> {
    let next_due_micros: Option<i64> = sqlx::query_scalar(
        "SELECT TIMESTAMPDIFF(MICROSECOND, CURRENT_TIMESTAMP(6), MIN(status_transition_at)) \
         FROM tables \
         WHERE table_status IN ('CREATING', 'UPDATING', 'DELETING') \
           AND status_transition_at IS NOT NULL",
    )
    .fetch_one(&storage.pool)
    .await?;

    Ok(transition_poll_delay(
        next_due_micros,
        CONTROL_PLANE_ACTIVE_POLL_MAX,
    ))
}

fn transition_poll_delay(next_due_micros: Option<i64>, max_poll: Duration) -> Duration {
    match next_due_micros {
        Some(micros) if micros <= 0 => Duration::ZERO,
        Some(micros) => std::cmp::min(Duration::from_micros(micros as u64), max_poll),
        None => max_poll,
    }
}

pub(crate) async fn pool_metrics_worker(pools: Vec<MySqlPool>, metrics: Arc<MetricsCollector>) {
    const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

    loop {
        tokio::time::sleep(SAMPLE_INTERVAL).await;

        let snapshots = pools
            .iter()
            .map(|pool| PoolSnapshot {
                size: pool.size() as usize,
                idle: pool.num_idle(),
            })
            .collect::<Vec<_>>();
        let (total_active, total_idle) = pool_metric_totals(&snapshots);

        #[allow(clippy::cast_possible_truncation)]
        metrics.record_pool_state(total_active as u32, total_idle as u32);
    }
}

#[derive(Clone, Copy)]
struct PoolSnapshot {
    size: usize,
    idle: usize,
}

fn pool_metric_totals(pools: &[PoolSnapshot]) -> (usize, usize) {
    pools.iter().fold((0, 0), |(active, idle), pool| {
        (
            active + pool.size.saturating_sub(pool.idle),
            idle + pool.idle,
        )
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{PoolSnapshot, pool_metric_totals, transition_poll_delay};

    #[test]
    fn transition_poll_delay_tracks_near_due_transitions() {
        let max_poll = Duration::from_secs(1);

        assert_eq!(transition_poll_delay(Some(-1), max_poll), Duration::ZERO);
        assert_eq!(transition_poll_delay(Some(0), max_poll), Duration::ZERO);
        assert_eq!(
            transition_poll_delay(Some(250_000), max_poll),
            Duration::from_millis(250)
        );
        assert_eq!(transition_poll_delay(Some(2_000_000), max_poll), max_poll);
        assert_eq!(transition_poll_delay(None, max_poll), max_poll);
    }

    #[test]
    fn pool_metric_totals_include_every_tidb_pool() {
        let pools = [
            PoolSnapshot { size: 10, idle: 7 },
            PoolSnapshot { size: 10, idle: 8 },
            PoolSnapshot { size: 10, idle: 10 },
            PoolSnapshot { size: 10, idle: 6 },
        ];

        assert_eq!(pool_metric_totals(&pools), (9, 31));
    }
}

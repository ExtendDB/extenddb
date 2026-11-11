// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for [`MetricsCollector`].

use super::*;
use crate::metrics::types::{Dimension, MetricName, MetricsQuery, TimeWindow};

const ACCOUNT: &str = "111111111111";

#[test]
fn record_and_query_basic() {
    let c = MetricsCollector::new();
    c.record_user_error(ACCOUNT, Some("MyTable"), "PutItem");
    c.record_user_error(ACCOUNT, Some("MyTable"), "PutItem");

    let results = c.query(&MetricsQuery {
        table_name: Some("MyTable".to_owned()),
        metric: Some(MetricName::UserErrors),
        window: Some(TimeWindow::AllTime),
        ..Default::default()
    });
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].count, 2);
    assert!((results[0].sum - 2.0).abs() < f64::EPSILON);
}

#[test]
fn latency_percentiles() {
    let c = MetricsCollector::new();
    for i in 1..=100 {
        c.record_latency(ACCOUNT, Some("T"), "GetItem", i as f64);
    }
    let results = c.query(&MetricsQuery {
        metric: Some(MetricName::SuccessfulRequestLatency),
        window: Some(TimeWindow::AllTime),
        ..Default::default()
    });
    assert_eq!(results.len(), 1);
    let p = results[0].percentiles.as_ref().expect("percentiles");
    assert!((p.p50 - 50.0).abs() < 1.5);
    assert!((p.p99 - 99.0).abs() < 1.5);
}

#[test]
fn filter_by_table() {
    let c = MetricsCollector::new();
    c.record_user_error(ACCOUNT, Some("A"), "PutItem");
    c.record_user_error(ACCOUNT, Some("B"), "PutItem");

    let results = c.query(&MetricsQuery {
        table_name: Some("A".to_owned()),
        ..Default::default()
    });
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .dimensions
            .contains(&Dimension::TableName("A".to_owned()))
    );
}

/// Two accounts' tables of the same name keep their own points: neither
/// adds into the other's, each carries its account as a dimension, and the
/// account filter returns only that account's. A metric of the whole
/// deployment has no account dimension and only an unfiltered query sees it.
#[test]
fn two_accounts_with_the_same_table_name_keep_separate_metrics() {
    const OTHER: &str = "222222222222";
    let c = MetricsCollector::new();
    c.record_user_error(ACCOUNT, Some("Orders"), "PutItem");
    c.record_user_error(OTHER, Some("Orders"), "PutItem");
    c.record_user_error(OTHER, Some("Orders"), "PutItem");
    c.record_pool_acquire_latency(5.0);

    let all = c.query(&MetricsQuery {
        metric: Some(MetricName::UserErrors),
        window: Some(TimeWindow::AllTime),
        ..Default::default()
    });
    let mut counts: Vec<(Vec<Dimension>, u64)> = all
        .iter()
        .map(|s| (s.dimensions.clone(), s.count))
        .collect();
    counts.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));
    let dims = |account: &str| {
        vec![
            Dimension::AccountId(account.to_owned()),
            Dimension::TableName("Orders".to_owned()),
            Dimension::Operation("PutItem".to_owned()),
        ]
    };
    assert_eq!(counts, [(dims(ACCOUNT), 1), (dims(OTHER), 2)]);

    let other = c.query(&MetricsQuery {
        account_id: Some(OTHER.to_owned()),
        window: Some(TimeWindow::AllTime),
        ..Default::default()
    });
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].dimensions, dims(OTHER));
    assert_eq!(other[0].count, 2);

    let pool = c.query(&MetricsQuery {
        metric: Some(MetricName::PoolAcquireLatency),
        window: Some(TimeWindow::AllTime),
        ..Default::default()
    });
    assert_eq!(pool.len(), 1);
    assert!(pool[0].dimensions.is_empty());
}

/// A drained bucket carries its account, and a metric of the whole
/// deployment drains with an empty one.
#[test]
fn a_drained_bucket_carries_its_account() {
    let c = MetricsCollector::new();
    c.record_ttl_deletion(ACCOUNT, "Orders");
    c.record_ttl_repair_markers(3.0);
    let mut buckets: Vec<(String, String, String)> = c
        .drain(Duration::ZERO)
        .into_iter()
        .map(|b| (b.metric.to_string(), b.account_id, b.table_name))
        .collect();
    buckets.sort();
    assert_eq!(
        buckets,
        [
            (
                "TimeToLiveDeletedItemCount".to_owned(),
                ACCOUNT.to_owned(),
                "Orders".to_owned()
            ),
            (
                "TtlRepairMarkerCount".to_owned(),
                String::new(),
                String::new()
            ),
        ]
    );
}

#[test]
fn prune_removes_old_data() {
    let c = MetricsCollector::new();
    c.record_user_error(ACCOUNT, Some("T"), "PutItem");

    // All data is recent, prune should keep it.
    c.prune();
    let results = c.query(&MetricsQuery {
        window: Some(TimeWindow::AllTime),
        ..Default::default()
    });
    assert_eq!(results.len(), 1);
}

#[test]
fn record_and_query_segments() {
    use crate::metrics::types::LatencySegments;

    let c = MetricsCollector::new();
    c.record_segments(
        ACCOUNT,
        "GetItem",
        Some("T"),
        LatencySegments {
            auth_us: 100.0,
            authz_us: 200.0,
            throttle_us: 50.0,
            dispatch_us: 400.0,
            response_us: 80.0,
            total_us: 830.0,
        },
    );
    c.record_segments(
        ACCOUNT,
        "GetItem",
        Some("T"),
        LatencySegments {
            auth_us: 200.0,
            authz_us: 300.0,
            throttle_us: 50.0,
            dispatch_us: 600.0,
            response_us: 120.0,
            total_us: 1270.0,
        },
    );

    let segs = c.query_segments(TimeWindow::AllTime);
    assert_eq!(segs.len(), 1);
    let s = &segs[0];
    assert_eq!(s.operation, "GetItem");
    assert_eq!(s.count, 2);
    assert!((s.avg.auth_us - 150.0).abs() < f64::EPSILON);
    assert!((s.avg.authz_us - 250.0).abs() < f64::EPSILON);
    assert!((s.avg.dispatch_us - 500.0).abs() < f64::EPSILON);
    assert!((s.avg.total_us - 1050.0).abs() < f64::EPSILON);
}

#[test]
fn prune_removes_old_segments() {
    use crate::metrics::types::LatencySegments;

    let c = MetricsCollector::new();
    c.record_segments(
        ACCOUNT,
        "PutItem",
        None,
        LatencySegments {
            auth_us: 10.0,
            authz_us: 20.0,
            throttle_us: 5.0,
            dispatch_us: 40.0,
            response_us: 8.0,
            total_us: 83.0,
        },
    );

    // All data is recent, prune should keep it.
    c.prune();
    let segs = c.query_segments(TimeWindow::AllTime);
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].count, 1);
}

#[test]
fn segments_multiple_operations() {
    use crate::metrics::types::LatencySegments;

    let c = MetricsCollector::new();
    c.record_segments(
        ACCOUNT,
        "GetItem",
        Some("T"),
        LatencySegments {
            auth_us: 100.0,
            authz_us: 200.0,
            throttle_us: 50.0,
            dispatch_us: 400.0,
            response_us: 80.0,
            total_us: 830.0,
        },
    );
    c.record_segments(
        ACCOUNT,
        "PutItem",
        Some("T"),
        LatencySegments {
            auth_us: 120.0,
            authz_us: 220.0,
            throttle_us: 60.0,
            dispatch_us: 500.0,
            response_us: 90.0,
            total_us: 990.0,
        },
    );

    let segs = c.query_segments(TimeWindow::AllTime);
    assert_eq!(segs.len(), 2);
    // Both operations should have count=1.
    for s in &segs {
        assert_eq!(s.count, 1);
    }
}

/// The backup job's recorders land under their names and dimensions: one
/// `BackupJobs` and one `BackupJobDuration` (microseconds) per job with the
/// outcome as the operation dimension, the bytes and the snapshot age
/// undimensioned, a store error under the call's name.
#[test]
fn backup_job_recorders_land_under_the_outcome_and_the_store_call() {
    use crate::metrics::types::BackupJobOutcome;

    let c = MetricsCollector::new();
    c.record_backup_job(BackupJobOutcome::Succeeded, 1_500.0);
    c.record_backup_job(BackupJobOutcome::Failed, 300.0);
    c.record_backup_bytes_uploaded(4096);
    c.record_backup_snapshot_age(12.5);
    c.record_backup_store_error("put");

    let by = |metric: MetricName| {
        let mut snaps = c.query(&MetricsQuery {
            metric: Some(metric),
            window: Some(TimeWindow::AllTime),
            ..Default::default()
        });
        snaps.sort_by(|a, b| format!("{:?}", a.dimensions).cmp(&format!("{:?}", b.dimensions)));
        snaps
    };
    let jobs = by(MetricName::BackupJobs);
    let outcomes: Vec<String> = jobs.iter().map(|s| format!("{:?}", s.dimensions)).collect();
    assert_eq!(
        outcomes,
        [
            "[Operation(\"failed\")]".to_owned(),
            "[Operation(\"succeeded\")]".to_owned()
        ]
    );
    assert!(jobs.iter().all(|s| s.count == 1 && s.sum == 1.0));
    let durations = by(MetricName::BackupJobDuration);
    assert_eq!(durations.len(), 2);
    assert_eq!(durations.iter().map(|s| s.sum).sum::<f64>(), 1_800.0);
    let bytes = by(MetricName::BackupBytesUploaded);
    assert_eq!(bytes.len(), 1);
    assert!(bytes[0].dimensions.is_empty());
    assert_eq!(bytes[0].sum, 4096.0);
    let age = by(MetricName::BackupSnapshotAge);
    assert_eq!(age[0].max, 12.5);
    let errors = by(MetricName::BackupStoreErrors);
    assert_eq!(
        format!("{:?}", errors[0].dimensions),
        "[Operation(\"put\")]"
    );
    for (outcome, text) in [
        (BackupJobOutcome::Succeeded, "succeeded"),
        (BackupJobOutcome::Failed, "failed"),
        (BackupJobOutcome::Cancelled, "cancelled"),
    ] {
        assert_eq!(outcome.to_string(), text);
    }
}

/// The restore job's recorders land as the backup job's do: one
/// `RestoreJobs` and one `RestoreJobDuration` per job under the outcome,
/// the bytes undimensioned, a `get` failure under `BackupStoreErrors`; and
/// the names render as the `/metrics` endpoint prints them.
#[test]
fn restore_job_recorders_land_under_the_outcome_and_render_their_names() {
    use crate::metrics::types::{BackupJobOutcome, QuerySource};

    let c = MetricsCollector::new();
    c.record_restore_job(BackupJobOutcome::Succeeded, 2_000.0);
    c.record_restore_job(BackupJobOutcome::Cancelled, 500.0);
    c.record_restore_bytes_downloaded(8192);
    c.record_backup_store_error("get");

    let by = |metric: MetricName| {
        let mut snaps = c.query(&MetricsQuery {
            metric: Some(metric),
            window: Some(TimeWindow::AllTime),
            ..Default::default()
        });
        snaps.sort_by(|a, b| format!("{:?}", a.dimensions).cmp(&format!("{:?}", b.dimensions)));
        snaps
    };
    let jobs = by(MetricName::RestoreJobs);
    let outcomes: Vec<String> = jobs.iter().map(|s| format!("{:?}", s.dimensions)).collect();
    assert_eq!(
        outcomes,
        [
            "[Operation(\"cancelled\")]".to_owned(),
            "[Operation(\"succeeded\")]".to_owned()
        ]
    );
    assert!(jobs.iter().all(|s| s.count == 1 && s.sum == 1.0));
    let durations = by(MetricName::RestoreJobDuration);
    assert_eq!(durations.len(), 2);
    assert_eq!(durations.iter().map(|s| s.sum).sum::<f64>(), 2_500.0);
    let bytes = by(MetricName::RestoreBytesDownloaded);
    assert_eq!(bytes.len(), 1);
    assert!(bytes[0].dimensions.is_empty());
    assert_eq!(bytes[0].sum, 8192.0);
    let errors = by(MetricName::BackupStoreErrors);
    assert_eq!(
        format!("{:?}", errors[0].dimensions),
        "[Operation(\"get\")]"
    );
    assert!(by(MetricName::BackupJobs).is_empty());
    assert_eq!(MetricName::RestoreJobs.to_string(), "RestoreJobs");
    assert_eq!(
        MetricName::RestoreJobDuration.to_string(),
        "RestoreJobDuration"
    );
    assert_eq!(
        MetricName::RestoreBytesDownloaded.to_string(),
        "RestoreBytesDownloaded"
    );
    assert_eq!(QuerySource::RestoreJob.to_string(), "RestoreJob");
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for [`MetricsCollector`].

use super::*;
use crate::metrics::types::{Dimension, MetricName, MetricsQuery, TimeWindow};

#[test]
fn record_and_query_basic() {
    let c = MetricsCollector::new();
    c.record_user_error(Some("MyTable"), "PutItem");
    c.record_user_error(Some("MyTable"), "PutItem");

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
        c.record_latency(Some("T"), "GetItem", i as f64);
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
    c.record_user_error(Some("A"), "PutItem");
    c.record_user_error(Some("B"), "PutItem");

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

#[test]
fn prune_removes_old_data() {
    let c = MetricsCollector::new();
    c.record_user_error(Some("T"), "PutItem");

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
        "GetItem",
        &LatencySegments {
            auth_us: 100.0,
            authz_us: 200.0,
            throttle_us: 50.0,
            dispatch_us: 400.0,
            response_us: 80.0,
            total_us: 830.0,
        },
    );
    c.record_segments(
        "GetItem",
        &LatencySegments {
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
fn prune_leaves_segments_alone_and_the_ring_ages_them_out() {
    use crate::metrics::types::LatencySegments;
    use std::time::Duration;

    let c = MetricsCollector::new();
    let Ok(epoch) = c.segments.read().map(|s| s.epoch()) else {
        panic!("segments lock poisoned");
    };
    c.record_segments(
        "PutItem",
        &LatencySegments {
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

    // A day later the bucket has left the ring without any prune call.
    let later = epoch + Duration::from_secs(1441 * 60);
    assert!(c.query_segments_at(TimeWindow::AllTime, later).is_empty());
    assert!(c.query_segments_at(TimeWindow::LastDay, later).is_empty());
}

#[test]
fn segments_multiple_operations() {
    use crate::metrics::types::LatencySegments;

    let c = MetricsCollector::new();
    c.record_segments(
        "GetItem",
        &LatencySegments {
            auth_us: 100.0,
            authz_us: 200.0,
            throttle_us: 50.0,
            dispatch_us: 400.0,
            response_us: 80.0,
            total_us: 830.0,
        },
    );
    c.record_segments(
        "PutItem",
        &LatencySegments {
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

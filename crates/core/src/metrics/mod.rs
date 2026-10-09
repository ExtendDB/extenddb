// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! In-memory metrics collection for extenddb.
//!
//! Collects `DynamoDB`-style `CloudWatch` metrics per table, per operation, and
//! per GSI. Metrics are accumulated in memory using an `RwLock`-protected map
//! of per-key accumulators. A background task prunes data points older than
//! 1 day every 5 minutes.
//!
//! REQ-OBS-005: Metrics endpoint.

mod accumulator;
mod collector;
mod collector_query;
mod types;

pub use collector::MetricsCollector;
pub use types::{
    Dimension, FlushBucket, LatencySegments, MetricName, MetricSnapshot, MetricsBucket,
    MetricsQuery, MetricsResponse, OperationSegments, Percentiles, QueryCategory, QuerySource,
    TimeWindow, auto_granularity, parse_granularity, window_duration_secs,
};

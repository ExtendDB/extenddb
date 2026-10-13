// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Per-key point storage for `MetricsCollector`.
//!
//! Points are kept in chunks of at most [`CHUNK_CAP`]. Taking the old points
//! out moves whole chunks and splits only the chunks that straddle the cutoff,
//! and a `record` call reallocates at most one chunk.

use std::collections::VecDeque;
use std::time::{Instant, SystemTime};

use super::collector::window_cutoff;
use super::types::TimeWindow;

/// Most points one chunk holds: the largest reallocation a `record` call can
/// cause, and the most points a drain splits one by one per straddling chunk.
pub(super) const CHUNK_CAP: usize = 1024;

/// Capacity of a key's first chunk; most keys never fill one.
const FIRST_CHUNK_CAP: usize = 64;

/// A single data point recorded for a metric.
#[derive(Debug, Clone)]
pub(super) struct DataPoint {
    pub(super) value: f64,
    pub(super) timestamp: Instant,
    /// Wall-clock time for DB persistence. Truncated to minute boundary on flush.
    pub(super) wall_time: SystemTime,
}

/// Points in record order, with the range of their timestamps.
#[derive(Debug)]
struct Chunk {
    points: Vec<DataPoint>,
    oldest: Instant,
    newest: Instant,
}

impl Chunk {
    fn new(point: DataPoint, capacity: usize) -> Self {
        let mut points = Vec::with_capacity(capacity);
        let (oldest, newest) = (point.timestamp, point.timestamp);
        points.push(point);
        Self {
            points,
            oldest,
            newest,
        }
    }

    fn push(&mut self, point: DataPoint) {
        self.oldest = self.oldest.min(point.timestamp);
        self.newest = self.newest.max(point.timestamp);
        self.points.push(point);
    }

    /// Moves the points that `is_old` selects out of the chunk, keeping order.
    fn split_off_old(&mut self, is_old: impl Fn(Instant) -> bool) -> Vec<DataPoint> {
        let (old, young): (Vec<_>, Vec<_>) =
            self.points.drain(..).partition(|p| is_old(p.timestamp));
        self.points = young;
        if let Some(first) = self.points.first() {
            let start = (first.timestamp, first.timestamp);
            (self.oldest, self.newest) = self.points.iter().fold(start, |(lo, hi), p| {
                (lo.min(p.timestamp), hi.max(p.timestamp))
            });
        }
        old
    }
}

/// Accumulator for a single metric+dimension combination.
#[derive(Debug)]
pub(super) struct Accumulator {
    chunks: VecDeque<Chunk>,
}

impl Accumulator {
    pub(super) fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
        }
    }

    pub(super) fn record(&mut self, value: f64, now: Instant, wall_time: SystemTime) {
        let point = DataPoint {
            value,
            timestamp: now,
            wall_time,
        };
        match self.chunks.back_mut() {
            Some(chunk) if chunk.points.len() < CHUNK_CAP => chunk.push(point),
            Some(_) => self.chunks.push_back(Chunk::new(point, CHUNK_CAP)),
            None => self.chunks.push_back(Chunk::new(point, FIRST_CHUNK_CAP)),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// All points, in record order.
    pub(super) fn points(&self) -> impl Iterator<Item = &DataPoint> {
        self.chunks.iter().flat_map(|c| c.points.iter())
    }

    /// Number of chunks held.
    pub(super) fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Appends the points of `newer`, recorded after every point held here.
    /// Moves chunk headers only: amortized O(chunks of `newer`).
    pub(super) fn append(&mut self, mut newer: Accumulator) {
        self.chunks.append(&mut newer.chunks);
    }

    /// Removes and returns the points at or before `cutoff`, in record order.
    pub(super) fn take_expired(&mut self, cutoff: Instant) -> Vec<Vec<DataPoint>> {
        self.take_old(|t| t <= cutoff)
    }

    /// Prune points older than the retention window (1 day).
    pub(super) fn prune(&mut self, cutoff: Instant) {
        self.take_old(|t| t < cutoff);
    }

    /// Moves out every point that `is_old` selects. Chunks whose newest point
    /// is old move whole; only chunks holding both old and young points are
    /// split. Points arrive almost in time order, so that is about one chunk.
    fn take_old(&mut self, is_old: impl Fn(Instant) -> bool) -> Vec<Vec<DataPoint>> {
        let mut out = Vec::new();
        while self.chunks.front().is_some_and(|c| is_old(c.newest)) {
            if let Some(chunk) = self.chunks.pop_front() {
                out.push(chunk.points);
            }
        }
        let mut emptied = false;
        for chunk in self.chunks.iter_mut().filter(|c| is_old(c.oldest)) {
            if is_old(chunk.newest) {
                out.push(std::mem::take(&mut chunk.points));
            } else {
                out.push(chunk.split_off_old(&is_old));
            }
            emptied |= chunk.points.is_empty();
        }
        if emptied {
            self.chunks.retain(|c| !c.points.is_empty());
        }
        out
    }

    pub(super) fn snapshot(&self, window: TimeWindow, now: Instant) -> Option<AccumulatorSnapshot> {
        let cutoff = window_cutoff(window, now);
        let values: Vec<f64> = match cutoff {
            Some(c) => self
                .points()
                .filter(|p| p.timestamp >= c)
                .map(|p| p.value)
                .collect(),
            None => self.points().map(|p| p.value).collect(),
        };

        if values.is_empty() {
            return None;
        }

        let sum: f64 = values.iter().sum();
        #[allow(clippy::cast_possible_truncation)]
        let count = values.len() as u64;
        let min = values.iter().copied().fold(f64::INFINITY, f64::min);
        let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);

        Some(AccumulatorSnapshot {
            sum,
            count,
            min,
            max,
            values,
        })
    }

    /// Chunk lengths and capacities, for tests of the storage bound.
    #[cfg(test)]
    pub(super) fn chunk_shapes(&self) -> Vec<(usize, usize)> {
        self.chunks
            .iter()
            .map(|c| (c.points.len(), c.points.capacity()))
            .collect()
    }
}

/// Snapshot of an accumulator's data for a given time window.
pub(super) struct AccumulatorSnapshot {
    pub(super) sum: f64,
    pub(super) count: u64,
    pub(super) min: f64,
    pub(super) max: f64,
    pub(super) values: Vec<f64>,
}

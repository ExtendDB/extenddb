// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Stale-while-revalidate (SWR) cache primitive used by the auth and storage layers.
//!
//! See `docs/design/12-auth-authz-cache.md` for the full design rationale.
//!
//! # Behavior
//!
//! Each cache entry has two timestamps relative to its insertion time:
//!
//! ```text
//! fetched_at ──── soft_ttl ──── hard_ttl ──── ∞
//!               fresh        stale-but-usable   miss
//! ```
//!
//! - `now − fetched_at < soft_ttl` → return cached value, no work.
//! - `soft_ttl ≤ now − fetched_at < hard_ttl` → return cached value AND spawn a
//!   tokio task to refetch and replace.
//! - `now − fetched_at ≥ hard_ttl` → cache miss. Caller awaits the load.
//!   Concurrent misses for the same key share one in-flight load (single-flight).
//!
//! Negative results (`Ok(None)` from the loader) are cached for `negative_ttl`,
//! which is typically much shorter than `hard_ttl`. Errors are never cached.
//!
//! # Thread / runtime safety
//!
//! `SwrCache` is `Clone + Send + Sync` and may be shared across tasks via direct
//! cloning (the underlying `moka` cache is internally `Arc`-shared). All
//! operations are async and contention-free under typical loads.
//!
//! # Shutdown
//!
//! Background refresh tasks are spawned via `tokio::spawn` without retaining
//! the `JoinHandle`. At runtime shutdown, tokio cancels in-flight tasks; an
//! in-progress loader query may produce a "connection closed" warning in
//! the logs from the database layer but cannot leak resources. Callers that
//! need deterministic shutdown should call `invalidate_all` and wait briefly
//! before dropping the cache; an explicit cancellation token is a future
//! enhancement.

#![forbid(unsafe_code)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod entry;
mod swr;

#[cfg(test)]
mod tests;

pub use swr::{ConfigError, Loader, SwrCache, SwrCacheConfig, SwrMetrics, SwrMetricsSnapshot};

/// Re-export of moka's `PredicateError`, returned by `invalidate_if` and
/// `invalidate_if_value`. Re-exported so callers don't need a direct moka
/// dependency.
pub use moka::PredicateError;

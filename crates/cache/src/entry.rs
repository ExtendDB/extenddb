// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cache entry payload.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

/// Internal cache entry. Stored as `Arc<Entry<V>>` inside the `moka` cache so
/// the value can be cheaply cloned out of the cache on hit and so the
/// `refresh_in_flight` flag can be CAS'd without removing the entry.
///
/// `value` is `Option<V>` to support negative caching: `None` means the
/// upstream loader returned `Ok(None)` (e.g. "key not found"). The two TTLs
/// (positive and negative) are applied at lookup time by `SwrCache`, not
/// stored on the entry.
pub(crate) struct Entry<V> {
    pub(crate) value: Option<V>,
    pub(crate) fetched_at: Instant,
    /// True when a background refresh task is currently running for this key.
    /// Used to single-flight refresh-ahead loads.
    pub(crate) refresh_in_flight: AtomicBool,
}

impl<V> Entry<V> {
    pub(crate) fn new(value: Option<V>) -> Arc<Self> {
        Arc::new(Self {
            value,
            fetched_at: Instant::now(),
            refresh_in_flight: AtomicBool::new(false),
        })
    }
}

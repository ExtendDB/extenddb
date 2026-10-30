// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! One pause machinery for the test hooks that hold an operation at a chosen
//! point (`snapshot::paging::arm_open_pause`, the `PostgreSQL` backend's
//! `arm_delete_table_pause`), so a test can act in the window the point
//! opens. Compiled only with `test-hooks`.
//!
//! A [`PausePoint`] is a process-global slot (a `static`) that the operation
//! calls [`PausePoint::reached`] on; a test arms it with [`PausePoint::arm`]
//! and gets a [`Pause`] handle. The next operation that reaches the point
//! with the armed key signals the handle and waits until the test resumes it
//! or drops the handle. Every test that arms a point holds
//! `snapshot_conformance::hooks_lock`, so no foreign operation can take it.

use std::sync::{Mutex, PoisonError};

use tokio::sync::oneshot;

struct Armed<K> {
    key: K,
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

/// A point an operation can be held at, keyed by `K` (`()` for a point with
/// one place, an enum for one with several).
pub struct PausePoint<K: 'static> {
    slot: Mutex<Option<Armed<K>>>,
}

impl<K: Copy + PartialEq + std::fmt::Debug + Send + 'static> PausePoint<K> {
    /// An unarmed point, for a `static`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            slot: Mutex::new(None),
        }
    }

    /// Arm the point at `key` for the next operation that reaches it (one
    /// operation, once).
    ///
    /// # Panics
    ///
    /// In debug builds, when the point is already armed: a second arm would
    /// replace the first silently, and the first handle's drop would then
    /// disarm the second.
    #[must_use]
    pub fn arm(&'static self, key: K) -> Pause<K> {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
        debug_assert!(
            slot.is_none(),
            "a pause is already armed at {:?}; drop its handle before arming another",
            slot.as_ref().map(|a| a.key)
        );
        *slot = Some(Armed {
            key,
            reached: reached_tx,
            resume: resume_rx,
        });
        drop(slot);
        Pause {
            point: self,
            reached: Some(reached_rx),
            resume: Some(resume_tx),
        }
    }

    /// Called by the operation at the point: when the point is armed at
    /// `key`, disarm it, signal the handle and wait until the test resumes or
    /// drops it. Otherwise returns at once.
    pub async fn reached(&self, key: K) {
        let armed = {
            let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
            if slot.as_ref().is_some_and(|a| a.key == key) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(armed) = armed {
            let _ = armed.reached.send(());
            let _ = armed.resume.await;
        }
    }

    fn disarm(&self) {
        *self.slot.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

impl<K: Copy + PartialEq + std::fmt::Debug + Send + 'static> Default for PausePoint<K> {
    fn default() -> Self {
        Self::new()
    }
}

/// A pause armed by [`PausePoint::arm`]. Dropping it disarms the point and
/// releases an operation waiting at it, so a failing test cannot leave a
/// pause armed for the next operation in the process.
pub struct Pause<K: Copy + PartialEq + std::fmt::Debug + Send + 'static> {
    point: &'static PausePoint<K>,
    reached: Option<oneshot::Receiver<()>>,
    resume: Option<oneshot::Sender<()>>,
}

impl<K: Copy + PartialEq + std::fmt::Debug + Send + 'static> Pause<K> {
    /// The receiver that resolves when the next operation reaches the point.
    ///
    /// # Panics
    ///
    /// When called twice.
    pub fn take_reached(&mut self) -> oneshot::Receiver<()> {
        self.reached.take().expect("take_reached called once")
    }

    /// Let the operation continue.
    pub fn resume(&mut self) {
        if let Some(tx) = self.resume.take() {
            let _ = tx.send(());
        }
    }
}

impl<K: Copy + PartialEq + std::fmt::Debug + Send + 'static> Drop for Pause<K> {
    fn drop(&mut self) {
        self.point.disarm();
        self.resume();
    }
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A time limit for tests of work that could run for minutes when a number
//! is not range checked before arithmetic or a comparison.

use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

/// Run `work` on another thread and return its answer, or fail the test after
/// 10 seconds with `what` in the message. A panic in `work` fails the test as
/// that panic.
pub(crate) fn within_10_s<T, F>(what: &str, work: F) -> T
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(work());
    });
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(answer) => answer,
        Err(RecvTimeoutError::Disconnected) => {
            std::panic::resume_unwind(worker.join().expect_err("no answer and no panic"))
        }
        Err(RecvTimeoutError::Timeout) => panic!("{what} ran for 10 s"),
    }
}

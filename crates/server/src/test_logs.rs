// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The log lines a future writes, for the tests that assert them.

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

/// Collects what a `tracing` subscriber writes.
#[derive(Clone, Default)]
struct Logged(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logged {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Run `future` on this thread and return its output with the log lines it
/// wrote at info and above.
pub(crate) fn logged<F: Future>(future: F) -> (F::Output, String) {
    let logged = Logged::default();
    let writer = logged.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || writer.clone())
        .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let output = tracing::subscriber::with_default(subscriber, || runtime.block_on(future));
    let lines = logged
        .0
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    (output, String::from_utf8(lines).unwrap())
}

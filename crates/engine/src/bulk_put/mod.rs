// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bulk put operations: the four request handlers plus the primitives they
//! and the runner consume: operation ARN handling, input row parsing, the
//! idempotency request fingerprint, input file enumeration and reading,
//! and report rendering. The worker in [`worker`] claims operations and
//! drives [`runner`]; the server spawns it next to its own workers.

use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) mod arn;
pub mod authorize;
pub(crate) mod fingerprint;
pub(crate) mod handlers;
pub(crate) mod pacer;
pub(crate) mod report;
pub(crate) mod rows;
pub(crate) mod run_context;
pub(crate) mod runner;
pub(crate) mod s3;
pub(crate) mod source;
pub mod store;
pub mod store_root;
pub(crate) mod usage;
pub(crate) mod worker;

#[cfg(test)]
mod tests;

/// The current time in epoch milliseconds, shared by the start handler
/// (`start_time_ms`) and the runner's terminal transition (`end_time_ms`).
pub(crate) fn epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

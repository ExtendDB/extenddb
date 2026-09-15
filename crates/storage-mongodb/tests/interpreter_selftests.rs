// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Runs the interpreter's self-tests as an integration test binary.
//! The interpreter itself lives in `tests/common/mod.rs` — this file
//! exists so cargo compiles the module and runs its `#[cfg(test)]` tests.

#[allow(dead_code, unused_imports)]
mod common;

// The interpreter's self-tests live inside `common::tests` (gated by
// `#[cfg(test)]`). Cargo runs them automatically when this binary is
// built for `cargo test`.

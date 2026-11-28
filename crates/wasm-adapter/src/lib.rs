// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! WebAssembly adapter for `ExtendDB`.
//!
//! Runs the `ExtendDB` engine (`extenddb-engine`, over `extenddb-core` and
//! `extenddb-auth`) inside a WebAssembly module in the browser. A browser
//! cannot run the HTTP server, which listens on a TCP socket, so in its place
//! JavaScript makes a direct in-process call: it hands a `DynamoDB` request in
//! and gets the response back. The calls skip the server's own checks of a
//! request; the `docs/webassembly.md` section "Checks the server makes that
//! this build does not" lists them.
//!
//! # Layout
//! - `runtime` — the portable engine wrapper: builds an `OperationContext`
//!   over the in-memory `SQLite` backend and calls [`extenddb_engine::dispatch`].
//!   The same code on both targets except the backup runtime, which it holds
//!   natively only, and its unit tests, which build natively only and
//!   exercise it against the native `ExtendDB` `SQLite` build.
//! - `wasm` — the `wasm-bindgen` entry points (`wasm32` only).
//!
//! # Scope and limits
//! Requests go to the engine's handlers over an in-memory `SQLite` database
//! (the synchronous `sqlx`-shaped shim over `sqlite-wasm-rs` on wasm, and
//! `sqlx` with C `SQLite` natively). The shared parity corpus
//! (`parity/corpus.json`) runs on both targets; its cases are the record of
//! which requests it covers and what each answers.
//! The build authenticates no request and authorizes none; every request runs
//! under a single fixed account.
//!
//! The module doc of `runtime` says what the adapter does with a request
//! before the engine's handlers see it, and the `docs/webassembly.md` section
//! "What it refuses" lists every request the build refuses, with its error.

mod runtime;

pub use runtime::Engine;

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::ExtendDb;

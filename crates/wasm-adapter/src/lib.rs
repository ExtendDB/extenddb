// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! WebAssembly adapter for `ExtendDB`.
//!
//! Runs the `ExtendDB` engine (`extenddb-engine`, over `extenddb-core` and
//! `extenddb-auth`) inside a WebAssembly module in the browser, and replaces
//! the one thing a browser cannot provide — the tokio TCP listener — with a
//! direct in-process call: JavaScript hands a `DynamoDB` request in and gets the
//! response back.
//!
//! # Layout
//! - [`runtime`] — the portable engine wrapper: builds an `OperationContext`
//!   over the in-memory `SQLite` backend and calls [`extenddb_engine::dispatch`].
//!   Target-agnostic; the native unit tests exercise it against the native
//!   `ExtendDB` `SQLite` build.
//! - `wasm` — the `wasm-bindgen` entry points (`wasm32` only).
//!
//! # Scope and limits
//! The data-plane operations (`CreateTable`, `PutItem`, `GetItem`, Query, Scan,
//! `UpdateItem`, `DeleteItem`, and the batch, transaction, stream, TTL and tagging
//! operations) answer end-to-end against an in-memory `SQLite` database (the
//! synchronous `sqlx`-shaped shim over `sqlite-wasm-rs` on wasm, and `sqlx`
//! with C `SQLite` natively). The build authenticates no request and authorizes
//! none; every request runs under a single fixed account.
//!
//! The operations a browser cannot serve are answered by [`runtime`] itself,
//! identically on both targets, so the native and wasm builds stay in step:
//! the backup, restore and point-in-time operations, table import and export,
//! `PartiQL`, and creating a vector index through `UpdateTable` each return the
//! answer documented on [`runtime::Engine::submit`].

mod runtime;

pub use runtime::Engine;

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::ExtendDb;

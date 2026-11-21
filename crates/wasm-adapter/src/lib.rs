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
//! Requests go to the engine's handlers over an in-memory `SQLite` database
//! (the synchronous `sqlx`-shaped shim over `sqlite-wasm-rs` on wasm, and
//! `sqlx` with C `SQLite` natively). The shared parity corpus
//! (`parity/corpus.json`) runs on both targets. Its successful requests are
//! `CreateTable`, `DescribeTable`, `DeleteTable`, `PutItem`, `GetItem`,
//! `UpdateItem`, `DeleteItem`, Query, Scan, `BatchWriteItem`, `BatchGetItem`,
//! `TransactWriteItems`, `TransactGetItems`, `ListStreams`, `DescribeStream`,
//! `GetShardIterator`, `GetRecords`, `UpdateTimeToLive`, `DescribeTimeToLive`,
//! `TagResource`, `ListTagsOfResource`, `UntagResource`, `ListBackups`,
//! `DescribeContinuousBackups` and `UpdateContinuousBackups`, and natively
//! `UpdateTable`, whose vector-index create the wasm build refuses. Its cases
//! for `ImportTable`, `ExportTableToPointInTime`, the refused operations below
//! and an unknown operation answer errors. `ListTables`, `DescribeLimits` and
//! `SearchVectors` reach the engine's handlers too but have no corpus case.
//! The build authenticates no request and authorizes none; every request runs
//! under a single fixed account.
//!
//! [`runtime`] refuses, the same way on both targets, the operations a browser
//! cannot serve: the store-backed backup operations, `PartiQL` and
//! `DescribeEndpoints`, and, as the server does, the point-in-time recovery
//! requests. On wasm, the `SQLite` backend also refuses an `UpdateTable` that
//! creates a vector index, after the engine has validated the request. The
//! module doc of [`runtime`] lists each answer.

mod runtime;

pub use runtime::Engine;

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
pub use wasm::ExtendDb;

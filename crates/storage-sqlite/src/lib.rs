// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite storage backend for ExtendDB.
//!
//! Implements the full `StorageEngine` + `CatalogStore` trait surface using
//! SQLite via `sqlx`, persisting to a single file (or `:memory:` for testing).
//! WAL mode enables concurrent readers; writers are serialized by the engine
//! (see `store`).
//!
//! # Design decisions vs the PostgreSQL backend
//!
//! - Placeholders: `?` positional (not `$N`).
//! - Concurrency: single WAL-mode pool; an engine-level write lock plus
//!   `BEGIN IMMEDIATE` replaces `SERIALIZABLE` / `FOR UPDATE`.
//! - Numbers: `N` sort keys stored as order-preserving TEXT (never `REAL`),
//!   full precision retained in the item JSON. See `docs/design-decisions.md`.
//! - One file holds catalog and data (no separate catalog/data databases).
//!
//! # The WebAssembly build
//!
//! On `wasm32` the crate runs over the in-crate `sqlx` shim (`wasm_sqlx`),
//! which opens only an in-memory database (`sqlite::memory:`); it runs no
//! server, no command and no background worker. Code that build does not run
//! is marked by one rule per concern:
//!
//! 1. **Does not compile for wasm32** (it calls `libsqlite3-sys`, `sqlx` API
//!    the shim does not provide, or an item that rule 1 or 2 removes):
//!    `#[cfg(not(target_arch = "wasm32"))]` on the smallest item that fails,
//!    the `mod` line where a whole module does, with a one-line reason.
//! 2. **Compiles for wasm32, and the dead-code lint reports it there** (no
//!    wasm32 code calls it, and it is not part of the crate's public API):
//!    removed the same way, together with its callers, with
//!    a one-line reason. Only where removing it would put a `cfg` into code
//!    both builds run, such as a field that a constructor both builds run
//!    sets, does it carry
//!    `#[cfg_attr(target_arch = "wasm32", expect(dead_code, reason = "..."))]`
//!    instead, the reason naming that code.
//! 3. **Compiled and called on wasm32, but reached by no wasm32 request**, and
//!    calling a method the WebAssembly clippy step disallows: the allow of
//!    `clippy::disallowed_methods` that the header of
//!    `devtools/wasm-clippy/clippy.toml`, in the repository this crate's
//!    manifest names, describes.
//!
//! NOTE: this is the catalog-layer build slice. The data-plane engine modules
//! and the `ServerComponents` registration are added in the engine slice.

// On wasm the backend cannot use `sqlx` + the C `libsqlite3-sys` (a dedicated
// OS thread per connection, a C sysroot), so every `sqlx::` path in this crate
// resolves instead to the in-crate `wasm_sqlx` module: aliasing this crate to
// `sqlx` and re-exporting the shim at the crate root, crate-privately, makes
// bare `sqlx::` paths resolve to it from every module without a per-module
// import. Native keeps the real `sqlx` crate and is the behaviour this backend
// is defined against.
#[cfg(target_arch = "wasm32")]
extern crate self as sqlx;
#[cfg(target_arch = "wasm32")]
pub(crate) use wasm_sqlx::*;

/// The shim items `tests/wasm_shim.rs` drives on wasm, under the paths `sqlx`
/// gives them. Not part of this crate's API.
#[cfg(target_arch = "wasm32")]
#[doc(hidden)]
pub mod wasm_sqlx_for_tests {
    pub use crate::wasm_sqlx::{
        Error, Executor, FromRow, Row, SqlitePool, query, query_as, query_scalar, raw_sql, sqlite,
    };

    /// A fresh pool builder from the shim's crate-private
    /// `SqlitePoolOptions::new()`. The tests call this where they call
    /// `sqlx::sqlite::SqlitePoolOptions::new()` natively.
    pub fn pool_options() -> sqlite::SqlitePoolOptions {
        sqlite::SqlitePoolOptions::new()
    }
}

/// Declare a catalog-row struct with a single field list that decodes the same
/// way on both targets.
///
/// On native the struct derives `sqlx::FromRow`. On wasm, where `sqlx` is the
/// in-crate shim and a derive macro is unavailable, the same field list
/// generates a `FromRow` impl that reads each column through the shim's
/// `try_get`, so a new or renamed column changes one place.
///
/// Every field decodes from the column with the field's own name, which is the
/// derive's default mapping; as the derive does, the `r#` of a raw identifier
/// is not part of the name, so `r#type` reads the column `type`. A
/// `#[sqlx(...)]` attribute, on the struct (such as `rename_all`) or on a field
/// (such as `rename`, `default` or `flatten`), would change that on native
/// only, so the macro refuses one with a compile error, including one inside a
/// `#[cfg_attr(...)]` at any depth.
///
/// Exported, and hidden from the documentation, only so that the crate's
/// `tests/wasm_shim.rs` declares its row structs with this macro on both
/// targets; it is not part of this crate's API. The expansion names `sqlx`,
/// which resolves where the macro is invoked.
#[doc(hidden)]
#[macro_export]
macro_rules! sqlite_row_struct {
    (
        $(#[$($meta:tt)*])*
        $vis:vis struct $name:ident {
            $( $(#[$($fattr:tt)*])* $fvis:vis $field:ident : $ty:ty ),* $(,)?
        }
    ) => {
        $( $crate::sqlite_row_attr!($($meta)*); )*
        $( $( $crate::sqlite_row_attr!($($fattr)*); )* )*

        $(#[$($meta)*])*
        #[cfg_attr(not(target_arch = "wasm32"), derive(sqlx::FromRow))]
        $vis struct $name {
            $( $(#[$($fattr)*])* $fvis $field : $ty ),*
        }

        #[cfg(target_arch = "wasm32")]
        impl sqlx::FromRow for $name {
            fn from_row(row: &sqlx::Row) -> ::core::result::Result<Self, sqlx::Error> {
                ::core::result::Result::Ok(Self {
                    $( $field: sqlx::Row::try_get(
                        row,
                        stringify!($field).trim_start_matches("r#"),
                    )? ),*
                })
            }
        }
    };
}

/// Refuses a `#[sqlx(...)]` attribute of a `sqlite_row_struct!` struct or
/// field, or one among the attributes of a `#[cfg_attr(...)]`, and passes
/// every other attribute (doc comments, `allow`s). Exported with
/// `sqlite_row_struct!`, which expands to it.
#[doc(hidden)]
#[macro_export]
macro_rules! sqlite_row_attr {
    (sqlx $($rest:tt)*) => {
        compile_error!(
            "sqlite_row_struct! decodes every field from the column of the field's name on \
             both targets; a #[sqlx(...)] attribute would change that on native only"
        );
    };
    (cfg_attr ( $predicate:meta $(, $($attrs:tt)*)? )) => {
        $( $crate::sqlite_row_attr!(@list [] $($attrs)*); )?
    };
    // The comma-separated attributes of a `cfg_attr`: each one is checked
    // as an attribute of its own. The bracket holds the tokens of the
    // attribute read so far.
    (@list [$($attr:tt)*]) => {
        $crate::sqlite_row_attr!($($attr)*);
    };
    (@list [$($attr:tt)*] , $($rest:tt)*) => {
        $crate::sqlite_row_attr!($($attr)*);
        $crate::sqlite_row_attr!(@list [] $($rest)*);
    };
    (@list [$($attr:tt)*] $next:tt $($rest:tt)*) => {
        $crate::sqlite_row_attr!(@list [$($attr)* $next] $($rest)*);
    };
    ($($other:tt)*) => {};
}

mod admin_store;
mod authorization_store;
mod backup;
mod backup_catalog;
// `Bootstrapper`; its `destroy` takes the serve lock, compiled off wasm32.
#[cfg(not(target_arch = "wasm32"))]
mod bootstrapper;
mod catalog_store;
pub mod config;
mod create_table;
mod credential_store;
mod data;
mod delete_table;
// `ServerRuntimeHooks`: the server's background workers, not run on wasm32.
#[cfg(not(target_arch = "wasm32"))]
mod hooks;
mod management_store;
mod metadata;
pub mod migrations;
mod number_key;
// `OperationsEngine`, which only `server_backend` installs.
#[cfg(not(target_arch = "wasm32"))]
mod operations;
mod password_hash_store;
mod restore_engine;
#[cfg(test)]
mod restore_job_tests;
mod schema;
// wasm32 calls only `database_file`; the module compiles its lock-taking
// items off wasm32.
mod serve_lock;
// The server's registration and factories, which use the modules gated here.
#[cfg(not(target_arch = "wasm32"))]
mod server_backend;
// Uses `sqlx` API the shim does not provide; on wasm32 `SnapshotEngine` answers `Unsupported`.
#[cfg(not(target_arch = "wasm32"))]
mod snapshot_engine;
mod sqlite_util;
mod store;
mod stream;
mod stream_at_timestamp;
#[cfg(test)]
mod stream_cleanup_tests;
mod stream_lifecycle;
#[cfg(test)]
mod stream_tests;
mod table_engine;
mod table_helpers;
mod update_table;
mod vector_bench;
mod vector_search;
mod warm_throughput;
mod worker;
// The server's background worker loops, spawned only by `hooks`.
#[cfg(not(target_arch = "wasm32"))]
mod workers;

// On wasm, the synchronous reimplementation of the slice of the `sqlx` API the
// backend uses, over the SQLite that `sqlite-wasm-rs` bundles (in-memory VFS).
// The crate root aliases it to `sqlx` (above), so the modules' `sqlx::` paths
// resolve to it.
#[cfg(target_arch = "wasm32")]
mod wasm_sqlx;

/// Default secondary-index propagation delay (milliseconds) when the
/// `index_propagation_delay_ms` setting is absent, as it is on a catalog the
/// migrations created (releases before 0.1.15 seeded the same value). The single
/// definition used by both the live read on the write path and the background
/// refresh worker.
pub(crate) const DEFAULT_INDEX_PROPAGATION_DELAY_MS: u64 = 10;

/// Default control-plane delay (seconds) when the `control_plane_delay_seconds`
/// setting is absent or unparseable: CreateTable, DeleteTable, the
/// control-plane worker and the console's default all read this one value.
pub(crate) const DEFAULT_CONTROL_PLANE_DELAY_SECONDS: f64 = 0.25;

/// Read the propagation-delay setting, preferring the canonical key and falling back
/// to the pre-rename one.
///
/// A catalog created before the rename holds the operator's value under the old name,
/// and no catalog migration rewrites that row yet, though one could. Reading past it
/// would silently reset a
/// configured delay to the default; since 0 means synchronous, the silent change would
/// be from strict to eventually consistent. `ORDER BY ... DESC` makes the preference
/// deterministic when both rows exist.
pub(crate) const INDEX_PROPAGATION_DELAY_QUERY: &str = "SELECT value FROM settings \
     WHERE key IN ('index_propagation_delay_ms', 'gsi_propagation_delay_ms') \
     ORDER BY key = 'index_propagation_delay_ms' DESC LIMIT 1";

pub use catalog_store::SqliteCatalogStore;
pub use config::SqliteConfig;
pub use credential_store::SqliteCredentialStore;
pub use store::SqliteEngine;
// From `bootstrapper` and `server_backend`, which wasm32 does not compile.
#[cfg(not(target_arch = "wasm32"))]
pub use {bootstrapper::SqliteBootstrapper, server_backend::backend};

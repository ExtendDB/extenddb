// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! S3 Object Annotations storage backend for ExtendDB.
//!
//! A second satirical-but-functional backend, modeled on the Route 53 backend
//! from PR #54. It stores items as named annotations on a sentinel S3 object,
//! chunking large item bodies across sibling annotations (see [`encoding`]).
//!
//! This crate currently ships:
//!
//! - [`encoding`] — the real, round-trip-tested item↔annotation mapping.
//! - [`S3AnnotationsBootstrapper`] — a registered but stubbed [`Bootstrapper`],
//!   whose every method returns an error naming the S3 Object Annotations API
//!   call a real implementation would issue.
//!
//! The backend registers itself with the `extenddb-storage` inventory registry
//! under the name `"s3annotations"`, so `extenddb init --backend s3annotations`
//! reaches the bootstrapper (when the binary is built with the `s3annotations`
//! feature) rather than failing with "unknown backend".
//!
//! [`Bootstrapper`]: extenddb_storage::bootstrapper::Bootstrapper

pub mod bootstrapper;
pub mod encoding;

pub use bootstrapper::S3AnnotationsBootstrapper;

// Auto-register the S3 Annotations backend at compile time. Mirrors the
// postgres registration in `extenddb-storage-postgres`.
inventory::submit! {
    extenddb_storage::bootstrapper::BackendRegistration {
        name: "s3annotations",
        factory: |config_path, cli_args| {
            Box::pin(async move {
                let store = S3AnnotationsBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        }
    }
}

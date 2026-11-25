// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Core types, validation, error definitions, and limits for extenddb.
//!
//! This crate is synchronous Rust with no async runtime dependency.
//! It defines the Virtual `DynamoDB` type system, request/response envelopes,
//! validation logic, and error types shared by the crates that depend on it.

pub mod arn;
pub mod backup_format;
/// Shared cryptographic primitives with a single target-specific backend
/// choice. Internal to the workspace; no API-stability promise.
#[doc(hidden)]
pub mod crypto;
pub mod error;
pub mod expression;
pub mod limits;
pub mod metrics;
#[cfg(feature = "partiql")]
pub mod partiql;
pub mod serde_helpers;
pub mod settings_keys;
pub mod throttle;
pub mod types;
pub mod validation;
pub mod vector_capacity;
pub mod version;

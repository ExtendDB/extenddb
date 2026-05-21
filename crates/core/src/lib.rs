// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Core types, validation, error definitions, and limits for extenddb.
//!
//! This crate is pure synchronous Rust with no async runtime dependency.
//! It defines the Virtual `DynamoDB` type system, request/response envelopes,
//! validation logic, and error types shared across all other crates.

pub mod error;
pub mod expression;
pub mod limits;
pub mod metrics;
pub mod serde_helpers;
pub mod throttle;
pub mod types;
pub mod validation;
pub mod version;

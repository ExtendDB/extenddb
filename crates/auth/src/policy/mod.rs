// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM policy evaluation engine.
//!
//! Implements the full IAM policy evaluation algorithm: explicit deny →
//! permissions boundary → session policy → identity allow → implicit deny.
//! Supports IBAC, RBAC, and ABAC/FGAC patterns through a unified evaluator.

pub mod condition;
pub mod context;
pub mod document;
pub mod evaluator;
pub mod matcher;
pub mod principal_id;

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! AWS Signature Version 4 verification.
//!
//! Implements server-side `SigV4` verification: parsing the `Authorization` header,
//! reconstructing the canonical request, deriving the signing key, and performing
//! constant-time signature comparison.

pub mod canonical;
pub mod parse;
pub mod signing_key;
pub mod verify;

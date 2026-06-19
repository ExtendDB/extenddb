// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! "DynamoDB at home" storage backend for ExtendDB.
//!
//! The third entry in the satirical-but-functional backend series, and the only
//! one that actually works the way the marketing implies: ExtendDB speaks the
//! DynamoDB wire protocol, and this backend stores its data in *actual*
//! DynamoDB. The point is not the encoding — there is barely any, because
//! DynamoDB is already a key/value database — it is the deployment posture. Run
//! ExtendDB yourself, pointed at DynamoDB, and you are technically "self-hosted."
//! The execs stop asking.
//!
//! Data plane forwards to DynamoDB; the catalog/IAM/auth plane is delegated to
//! the Postgres backend (`extenddb-storage-postgres`), because DynamoDB has
//! opinions about what a database is and "relational IAM catalog" is not one.

pub mod config;
pub mod client;
pub mod encoding;
pub mod naming;
pub(crate) mod errors;
pub(crate) mod expression;

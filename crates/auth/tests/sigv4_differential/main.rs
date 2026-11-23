// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SigV4 decisions of the server's authentication path, checked against the
//! AWS SDK for Rust signer, `aws-sigv4`.
//!
//! Each request is signed by `aws-sigv4` and passed to
//! `BuiltinAuthProvider::authenticate`, the call the HTTP layer makes for a
//! DynamoDB request, over an in-memory `CredentialStore` that holds one
//! long-term key and one session key with its token, for a server deployed
//! as one region. A request as the signer produced it, scoped to that region,
//! must verify, and the canonical request the server builds for it must equal
//! the one `aws-sigv4` signed. A request changed after signing in a part the
//! signature covers, or scoped to another region, must be refused.
//!
//! Requests are signed with the current time, so the timestamp check runs
//! against the real clock with a margin of a minute on each side. Generated
//! requests come from a fixed seed, so every run checks the same shapes. Each
//! test prints how many requests it checked per category; `--nocapture`
//! shows the counts.

mod accept;
mod capture;
mod corpus;
mod generated;
mod harness;
mod mutate;
mod reject;

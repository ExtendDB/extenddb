// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Principal unique identifiers for the `aws:userid` condition key.
//!
//! The live service populates `aws:userid` with the principal's unique id: for
//! an IAM user the user's id (an `AIDA...` string), and for a role session the
//! pair `<role-id>:<session-name>` where the role id is an `AROA...` string.
//! Measured live on this account: an IAM user resolves to `AIDA` followed by a
//! base32 body, and an assumed-role session to `AROA...:<session-name>` (for
//! example `AROA6A7B6LB4GWNTMCATH:r3probe`).
//!
//! ExtendDB does not store a unique id for users or roles (the catalog keys a
//! user by `(account_id, user_name)` and a role by `(account_id, role_name)`),
//! and it exposes no surface, such as an STS `GetCallerIdentity`, that returns
//! one. The id therefore has no external value a customer could already have
//! read, so the only contract `aws:userid` must honour is the service's: an
//! `AIDA`/`AROA` string that is stable for a principal, unique across
//! principals, and the same body for a user and for a role session built from
//! that role. This module derives it deterministically from the principal's
//! stable catalog identity, so every existing user and role has one with no
//! stored column and no catalog migration; the derivation is the source of
//! truth.

use sha2::{Digest, Sha256};

/// RFC 4648 base32 alphabet (uppercase, no padding), the character set the
/// service's principal ids use.
const BASE32: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// The body length (characters after the four-character type prefix). The
/// service's ids are 21 characters (for example `AROA` plus a 17-character
/// body), which this matches.
const BODY_LEN: usize = 17;

/// Base32-encode the first `n` characters of `data`.
fn base32_prefix(data: &[u8], n: usize) -> String {
    let mut buffer: u32 = 0;
    let mut bits_left: u32 = 0;
    let mut out = String::with_capacity(n);
    for &byte in data {
        buffer = (buffer << 8) | u32::from(byte);
        bits_left += 8;
        while bits_left >= 5 {
            bits_left -= 5;
            let idx = ((buffer >> bits_left) & 0x1f) as usize;
            out.push(BASE32[idx] as char);
            if out.len() == n {
                return out;
            }
        }
        buffer &= (1 << bits_left) - 1;
    }
    out
}

/// Derive an id body from a domain-separated set of stable parts.
fn id_body(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        // A length-prefixed separator keeps ("ab","c") distinct from ("a","bc").
        hasher.update([0u8]);
    }
    base32_prefix(&hasher.finalize(), BODY_LEN)
}

/// The `aws:userid` value for an IAM user: its unique id, an `AIDA...` string.
///
/// `created_at` is the user's creation time (any stable representation, in
/// practice RFC 3339). Including it means a user deleted and recreated with the
/// same name gets a different id, as IAM issues a new unique id each time.
#[must_use]
pub fn user_unique_id(account_id: &str, user_name: &str, created_at: &str) -> String {
    format!("AIDA{}", id_body(&["user", account_id, user_name, created_at]))
}

/// A role's unique id, an `AROA...` string. Used as the first half of a role
/// session's `aws:userid`. `created_at` is the role's creation time, so a
/// recreated role gets a new id.
#[must_use]
pub fn role_unique_id(account_id: &str, role_name: &str, created_at: &str) -> String {
    format!("AROA{}", id_body(&["role", account_id, role_name, created_at]))
}

/// The `aws:userid` value for a role session: `<role-id>:<session-name>`.
///
/// `role_created_at` is the role's creation time (not the session's): the role
/// id half changes when the role is recreated, matching IAM.
#[must_use]
pub fn role_session_userid(
    account_id: &str,
    role_name: &str,
    session_name: &str,
    role_created_at: &str,
) -> String {
    format!(
        "{}:{session_name}",
        role_unique_id(account_id, role_name, role_created_at)
    )
}

/// The `aws:userid` value for a caller identified by ARN, for the trust path.
///
/// An IAM user ARN (`arn:aws:iam::<account>:user/<name>`) resolves to the
/// user's `AIDA...` id; an assumed-role ARN
/// (`arn:aws:sts::<account>:assumed-role/<role>/<session>`) to
/// `<role-id>:<session>`. `created_at` is the caller principal's creation time
/// (the user's for a user ARN, the role's for an assumed-role ARN), looked up
/// by the caller; when the caller is not a known local principal it is `None`
/// and the id is derived without it. Any other ARN form returns `None`, so a
/// trust policy conditioning on `aws:userid` for such a caller fails to match
/// rather than resolving to a fabricated value.
#[must_use]
pub fn userid_for_caller_arn(caller_arn: &str, created_at: Option<&str>) -> Option<String> {
    let parts: Vec<&str> = caller_arn.splitn(6, ':').collect();
    if parts.len() < 6 {
        return None;
    }
    let account = parts[4];
    let resource = parts[5];
    let created_at = created_at.unwrap_or("");
    if let Some(user_name) = resource.strip_prefix("user/") {
        Some(user_unique_id(account, user_name, created_at))
    } else if let Some(rest) = resource.strip_prefix("assumed-role/") {
        let (role, session) = rest.split_once('/')?;
        Some(role_session_userid(account, role, session, created_at))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T1: &str = "2026-10-21T00:00:00Z";
    const T2: &str = "2026-10-21T01:00:00Z";

    #[test]
    fn user_id_has_the_service_prefix_and_length() {
        let id = user_unique_id("123456789012", "alice", T1);
        assert!(id.starts_with("AIDA"), "{id}");
        assert_eq!(id.len(), 4 + BODY_LEN);
        assert!(
            id.bytes()
                .all(|b| BASE32.contains(&b) || b"AIDA".contains(&b))
        );
    }

    #[test]
    fn role_id_has_the_service_prefix_and_length() {
        let id = role_unique_id("123456789012", "app-role", T1);
        assert!(id.starts_with("AROA"), "{id}");
        assert_eq!(id.len(), 4 + BODY_LEN);
    }

    #[test]
    fn ids_are_stable_and_unique() {
        assert_eq!(
            user_unique_id("123456789012", "alice", T1),
            user_unique_id("123456789012", "alice", T1)
        );
        assert_ne!(
            user_unique_id("123456789012", "alice", T1),
            user_unique_id("123456789012", "bob", T1)
        );
        assert_ne!(
            user_unique_id("123456789012", "alice", T1),
            user_unique_id("999999999999", "alice", T1)
        );
        // A user and a role with the same name get different ids (different
        // type prefix and domain separation).
        assert_ne!(
            user_unique_id("123456789012", "shared", T1),
            role_unique_id("123456789012", "shared", T1)
        );
    }

    #[test]
    fn recreating_a_principal_changes_its_id() {
        // Same account and name, different creation time: a new id, as IAM
        // issues on delete and recreate.
        assert_ne!(
            user_unique_id("123456789012", "alice", T1),
            user_unique_id("123456789012", "alice", T2)
        );
        assert_ne!(
            role_unique_id("123456789012", "app-role", T1),
            role_unique_id("123456789012", "app-role", T2)
        );
    }

    #[test]
    fn role_session_userid_is_role_id_colon_session() {
        let rid = role_unique_id("123456789012", "app-role", T1);
        assert_eq!(
            role_session_userid("123456789012", "app-role", "sess-1", T1),
            format!("{rid}:sess-1")
        );
    }

    #[test]
    fn userid_for_caller_arn_covers_user_and_role_session() {
        assert_eq!(
            userid_for_caller_arn("arn:aws:iam::123456789012:user/alice", Some(T1)),
            Some(user_unique_id("123456789012", "alice", T1))
        );
        assert_eq!(
            userid_for_caller_arn(
                "arn:aws:sts::123456789012:assumed-role/app-role/sess-1",
                Some(T1)
            ),
            Some(role_session_userid("123456789012", "app-role", "sess-1", T1))
        );
        assert_eq!(
            userid_for_caller_arn("arn:aws:iam::123456789012:root", Some(T1)),
            None
        );
        assert_eq!(userid_for_caller_arn("not-an-arn", None), None);
    }
}

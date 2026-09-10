// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Key and prefix validation shared by both store implementations.
//!
//! A key is a `/`-separated component path. The rules exist so a key can be
//! mapped onto a filesystem path without ever escaping the store root, and so
//! the two stores accept exactly the same key space:
//!
//! - every component is non-empty (no leading, trailing, or doubled `/`)
//! - no component is `.` or `..`
//! - no component contains a backslash (a path separator on other platforms)
//! - no component contains a control character
//! - the whole key is at most [`MAX_KEY_LEN`] bytes

use crate::StoreError;

/// Maximum total key length in bytes, matching the S3 key length limit.
pub const MAX_KEY_LEN: usize = 1024;

/// Validate an object key.
///
/// # Errors
///
/// Returns [`StoreError::InvalidKey`] naming the rule the key violated.
pub fn validate_key(key: &str) -> Result<(), StoreError> {
    if key.is_empty() {
        return Err(StoreError::InvalidKey("key is empty".to_owned()));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(StoreError::InvalidKey(format!(
            "key is {} bytes; the maximum is {MAX_KEY_LEN}",
            key.len()
        )));
    }
    for component in key.split('/') {
        validate_component(component)?;
    }
    Ok(())
}

/// Validate a listing or deletion prefix.
///
/// A prefix follows the key rules with two relaxations: it may be empty
/// (matching every object) and it may carry one trailing `/`.
///
/// # Errors
///
/// Returns [`StoreError::InvalidKey`] naming the rule the prefix violated.
pub fn validate_prefix(prefix: &str) -> Result<(), StoreError> {
    if prefix.is_empty() {
        return Ok(());
    }
    let normalized = prefix.strip_suffix('/').unwrap_or(prefix);
    if normalized.is_empty() {
        return Err(StoreError::InvalidKey(
            "prefix consists only of a separator".to_owned(),
        ));
    }
    validate_key(normalized)
}

/// Strip at most one trailing `/` from a validated prefix, yielding the form
/// the matching rule operates on.
pub(crate) fn normalize_prefix(prefix: &str) -> &str {
    prefix.strip_suffix('/').unwrap_or(prefix)
}

/// Whether `key` is matched by the normalized prefix `prefix`: the empty
/// prefix matches everything, otherwise the key must equal the prefix or sit
/// under `prefix/`. Component-aligned, so prefix `a/b` never matches `a/bc`.
pub(crate) fn key_matches_prefix(key: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    match key.strip_prefix(prefix) {
        Some("") => true,
        Some(rest) => rest.starts_with('/'),
        None => false,
    }
}

fn validate_component(component: &str) -> Result<(), StoreError> {
    if component.is_empty() {
        return Err(StoreError::InvalidKey(
            "key has an empty component (leading, trailing, or doubled '/')".to_owned(),
        ));
    }
    if component == "." || component == ".." {
        return Err(StoreError::InvalidKey(format!(
            "key component '{component}' is not allowed"
        )));
    }
    if component.contains('\\') {
        return Err(StoreError::InvalidKey(
            "key component contains a backslash".to_owned(),
        ));
    }
    if component.chars().any(char::is_control) {
        return Err(StoreError::InvalidKey(
            "key component contains a control character".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_well_formed_keys() {
        for key in [
            "a",
            "a/b",
            "123456789012/orders/backup-01J5/extenddb-manifest.json",
            "data/000001.json.gz",
            "with space/and.dots.json",
            "unicode/ключ/键",
        ] {
            assert!(validate_key(key).is_ok(), "{key} should be valid");
        }
    }

    #[test]
    fn rejects_malformed_keys() {
        let cases: &[&str] = &[
            "", "/", "/a", "a/", "a//b", ".", "..", "a/./b", "a/../b", "../a", "a/..", "a\\b",
            "a/b\\c", "a\x00b", "a/\x1fb", "a\x7f",
        ];
        for key in cases {
            assert!(
                matches!(validate_key(key), Err(StoreError::InvalidKey(_))),
                "{key:?} should be rejected"
            );
        }
    }

    #[test]
    fn rejects_overlong_keys() {
        let key = "a/".repeat(512) + "b";
        assert!(key.len() > MAX_KEY_LEN);
        assert!(matches!(validate_key(&key), Err(StoreError::InvalidKey(_))));
        let max = "a".repeat(MAX_KEY_LEN);
        assert!(validate_key(&max).is_ok());
    }

    #[test]
    fn prefix_rules() {
        assert!(validate_prefix("").is_ok());
        assert!(validate_prefix("a").is_ok());
        assert!(validate_prefix("a/b/").is_ok());
        assert!(matches!(
            validate_prefix("/"),
            Err(StoreError::InvalidKey(_))
        ));
        assert!(matches!(
            validate_prefix("a//b"),
            Err(StoreError::InvalidKey(_))
        ));
        assert!(matches!(
            validate_prefix("a/../b"),
            Err(StoreError::InvalidKey(_))
        ));
        assert!(matches!(
            validate_prefix("a\x01"),
            Err(StoreError::InvalidKey(_))
        ));
    }

    #[test]
    fn prefix_matching_is_component_aligned() {
        assert!(key_matches_prefix("a/b/c", ""));
        assert!(key_matches_prefix("a/b/c", "a"));
        assert!(key_matches_prefix("a/b/c", "a/b"));
        assert!(key_matches_prefix("a/b", "a/b"));
        assert!(!key_matches_prefix("a/bc", "a/b"));
        assert!(!key_matches_prefix("ab/c", "a"));
        assert!(!key_matches_prefix("a", "a/b"));
    }
}

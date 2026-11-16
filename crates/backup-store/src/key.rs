// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Key and prefix validation shared by both store implementations.
//!
//! A key is a `/`-separated component path. The rules exist so a key can be
//! mapped onto a filesystem path without ever escaping the store root, and so
//! that no key one store accepts is refused by the rule the other applies.
//! The rule is the upper bound of what both stores accept:
//!
//! - every component is non-empty (no leading, trailing, or doubled `/`)
//! - no component is `.` or `..`
//! - no component contains a backslash (a path separator on other platforms)
//! - no component contains a control character
//! - no component contains U+FFFE or U+FFFF: S3 stores such a key, but its
//!   XML listing cannot carry the character, so `list` and `delete_prefix`
//!   over any prefix covering it fail
//! - every component is at most [`MAX_COMPONENT_LEN`] bytes, the common
//!   filesystem name limit (`NAME_MAX`)
//! - the whole key is at most [`MAX_KEY_LEN`] bytes
//!
//! Each store narrows that bound; this is the one list of the narrowings.
//! A backup written through one store reads through the other only if its
//! keys avoid all of them:
//!
//! - S3: the configured bucket prefix counts towards S3's 1024-byte key
//!   limit, so under a prefix of `n` bytes a key may have at most
//!   `1024 - n` bytes (refused as `InvalidKey`).
//! - Filesystem: a file name of the store's temporary form (`.put-*.tmp`)
//!   can be written and read but is hidden from `list`.
//! - Filesystem: an object at `a` and another at `a/b` cannot both exist (a
//!   file and a directory cannot share a path); S3 holds both.
//! - Filesystem: a file system with a shorter name limit than 255 bytes
//!   (eCryptfs allows 143) refuses longer components at `put`, and one that
//!   folds case or Unicode normalization (APFS by default) maps `A` and `a`,
//!   or the NFC and NFD spellings of a name, to one object.

use crate::StoreError;

/// Maximum total key length in bytes, matching the S3 key length limit.
pub const MAX_KEY_LEN: usize = 1024;

/// Maximum length of one key component in bytes, matching the filesystem
/// name limit (`NAME_MAX`, 255 bytes on ext4 and xfs).
///
/// Another copy of this bound: `MAX_TABLE_NAME_LEN` (255) in
/// `extenddb-core`'s `backup_format/layout.rs` (#394), which caps the table
/// name, one component of a backup key, for the same reason. This crate does
/// not depend on core, so only this comment ties the two together; the
/// assertion beside `s3::MAX_STORE_PREFIX_LEN` uses this constant as the
/// layout's table-name length.
pub const MAX_COMPONENT_LEN: usize = 255;

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
    if component.contains(['\u{fffe}', '\u{ffff}']) {
        return Err(StoreError::InvalidKey(
            "key component contains U+FFFE or U+FFFF, which S3 listings cannot carry".to_owned(),
        ));
    }
    if component.len() > MAX_COMPONENT_LEN {
        return Err(StoreError::InvalidKey(format!(
            "key component is {} bytes; the maximum is {MAX_COMPONENT_LEN}",
            component.len()
        )));
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
        // A key of exactly the limit made of components within theirs.
        let component = "a".repeat(MAX_COMPONENT_LEN);
        let max = format!("{component}/{component}/{component}/{}/x", "b".repeat(254));
        assert_eq!(max.len(), MAX_KEY_LEN);
        assert!(validate_key(&max).is_ok());
        let over = format!("{max}y");
        assert_eq!(over.len(), MAX_KEY_LEN + 1);
        assert!(matches!(
            validate_key(&over),
            Err(StoreError::InvalidKey(msg)) if msg.contains("1025 bytes")
        ));
    }

    #[test]
    fn caps_each_component_at_the_filesystem_name_limit() {
        let at_limit = "c".repeat(MAX_COMPONENT_LEN);
        assert!(validate_key(&at_limit).is_ok());
        assert!(validate_key(&format!("a/{at_limit}/b")).is_ok());
        let over = "c".repeat(MAX_COMPONENT_LEN + 1);
        for key in [over.clone(), format!("a/{over}"), format!("{over}/b")] {
            let err = validate_key(&key).expect_err("over-long component");
            assert!(
                matches!(&err, StoreError::InvalidKey(msg) if msg.contains("256 bytes")),
                "{err}"
            );
        }
        // Multi-byte characters count in bytes: 85 three-byte characters are
        // 255 bytes, 86 are 258.
        assert!(validate_key(&"\u{4e2d}".repeat(85)).is_ok());
        assert!(matches!(
            validate_key(&"\u{4e2d}".repeat(86)),
            Err(StoreError::InvalidKey(_))
        ));
        assert!(matches!(
            validate_prefix(&format!("{over}/")),
            Err(StoreError::InvalidKey(_))
        ));
    }

    #[test]
    fn rejects_the_noncharacters_s3_listings_cannot_carry() {
        for key in [
            "a\u{fffe}",
            "a\u{ffff}",
            "a/\u{fffe}b",
            "\u{ffff}/b",
            "x/y/z\u{ffff}",
        ] {
            let err = validate_key(key).expect_err("noncharacter");
            assert!(
                matches!(&err, StoreError::InvalidKey(msg) if msg.contains("U+FFFE or U+FFFF")),
                "{key:?}: {err}"
            );
        }
        assert!(matches!(
            validate_prefix("p\u{fffe}/"),
            Err(StoreError::InvalidKey(_))
        ));
        // Neighbouring code points stay valid.
        for key in ["a\u{fffd}", "a\u{fffc}", "a\u{10000}", "a\u{10ffff}"] {
            assert!(validate_key(key).is_ok(), "{key:?}");
        }
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

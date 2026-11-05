// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The key a store writes, or lists, for a key as the request spells it.
//!
//! The filesystem store walks a key segment by segment, and an empty
//! segment or a `.` segment names the directory it is already in, so
//! `./protected/x`, `.//protected/x` and `protected/x` are one file. An
//! S3-compatible service with a file backend does the same and also
//! resolves `..` against the segment before it. AWS S3 stores every key as
//! given. The S3 permission checks evaluate the key as spelled and, where
//! it differs, the key the store uses, so a policy on one spelling holds
//! for every spelling of the same object.

/// The segments of `key` the filesystem store walks: every `/`-separated
/// segment except an empty one and `.`. The report writer, the manifest
/// writer and [`stored_form`] all take their segments from here.
pub(crate) fn walked_segments(key: &str) -> impl Iterator<Item = &str> {
    key.split('/')
        .filter(|part| !part.is_empty() && *part != ".")
}

/// `key` as the store holds it: the [`walked_segments`] joined with `/`,
/// and with `resolve_parent` each `..` segment removing the segment kept
/// before it (at the start it removes nothing). A trailing `/` is kept
/// when any segment remains, so a prefix keeps the boundary it names.
pub(crate) fn stored_form(key: &str, resolve_parent: bool) -> String {
    let mut kept: Vec<&str> = Vec::new();
    for part in walked_segments(key) {
        if resolve_parent && part == ".." {
            kept.pop();
        } else {
            kept.push(part);
        }
    }
    let mut stored = kept.join("/");
    if key.ends_with('/') && !stored.is_empty() {
        stored.push('/');
    }
    stored
}

/// Whether resolving the `..` segments of `key` against the segments before
/// them goes above the start of the key, as in `../x` and `a/../../x`.
/// [`stored_form`] drops such a climb, so the key it gives is inside the
/// bucket while a service that resolves the URL path may name another one.
pub(crate) fn climbs_above_start(key: &str) -> bool {
    let mut depth: usize = 0;
    for part in walked_segments(key) {
        if part == ".." {
            let Some(up) = depth.checked_sub(1) else {
                return true;
            };
            depth = up;
        } else {
            depth += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_dot_segments_are_dropped_and_a_parent_kept_without_resolution() {
        for (key, stored) in [
            ("protected/x", "protected/x"),
            ("./protected/x", "protected/x"),
            (".//protected/x", "protected/x"),
            ("x/./protected/x", "x/protected/x"),
            ("protected/.", "protected"),
            ("a//b/", "a/b/"),
            ("/lead/", "lead/"),
            ("x/../protected/x", "x/../protected/x"),
            ("", ""),
            ("./", ""),
        ] {
            assert_eq!(stored_form(key, false), stored, "{key}");
        }
    }

    #[test]
    fn a_parent_segment_removes_the_segment_before_it_when_resolved() {
        for (key, stored) in [
            ("x/../protected/x", "protected/x"),
            ("x/./../protected/", "protected/"),
            ("../protected/x", "protected/x"),
            ("a/b/../../c", "c"),
            ("a/..", ""),
            ("a/../", ""),
        ] {
            assert_eq!(stored_form(key, true), stored, "{key}");
        }
    }

    #[test]
    fn a_climb_above_the_start_is_found_after_dot_and_empty_segments_drop() {
        for (key, climbs) in [
            ("../victim/x", true),
            ("a/../../victim/x", true),
            ("x/../../victim", true),
            ("./../victim/", true),
            ("a//../..", true),
            ("x/../protected/x", false),
            ("a/b/../../c", false),
            ("a/..", false),
            ("...", false),
            ("a/.../b", false),
            ("", false),
        ] {
            assert_eq!(climbs_above_start(key), climbs, "{key}");
        }
    }
}

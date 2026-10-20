// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wildcard and ARN matching for IAM policy evaluation.
//!
//! `wildcard_match` supports `*` (zero or more characters) and `?` (exactly one
//! character). `arn_match` splits an ARN into its first five fields plus the
//! resource field (everything after the fifth `:`) as one field, and matches
//! each field with `wildcard_match`, so `*` and `?` in the resource field span
//! `:` and `/`.

/// Match a pattern against a value. Supports `*` and `?` wildcards.
/// Comparison is case-sensitive.
///
/// Uses a greedy algorithm with O(1) heap allocation.
///
/// Used for `StringLike` condition evaluation (case-sensitive per AWS IAM).
///
/// # Examples
///
/// ```
/// # use extenddb_auth::policy::matcher::wildcard_match;
/// assert!(wildcard_match("dynamodb:*", "dynamodb:PutItem"));
/// assert!(wildcard_match("dynamodb:Get*", "dynamodb:GetItem"));
/// assert!(!wildcard_match("dynamodb:Get*", "dynamodb:PutItem"));
/// assert!(wildcard_match("s?s", "sis"));
/// ```
#[must_use]
pub fn wildcard_match(pattern: &str, value: &str) -> bool {
    wildcard_match_impl(pattern.as_bytes(), value.as_bytes(), false)
}

/// Match a pattern against a value with case-insensitive comparison.
/// Supports `*` and `?` wildcards.
///
/// Used for Action matching (case-insensitive per AWS IAM).
///
/// # Examples
///
/// ```
/// # use extenddb_auth::policy::matcher::wildcard_match_ignore_case;
/// assert!(wildcard_match_ignore_case("dynamodb:getitem", "dynamodb:GetItem"));
/// assert!(wildcard_match_ignore_case("dynamodb:Get*", "dynamodb:getitem"));
/// assert!(!wildcard_match_ignore_case("dynamodb:Get*", "dynamodb:PutItem"));
/// ```
#[must_use]
pub fn wildcard_match_ignore_case(pattern: &str, value: &str) -> bool {
    wildcard_match_impl(pattern.as_bytes(), value.as_bytes(), true)
}

fn wildcard_match_impl(p: &[u8], v: &[u8], ignore_case: bool) -> bool {
    let (plen, vlen) = (p.len(), v.len());

    let mut pi = 0; // pattern index
    let mut vi = 0; // value index
    let mut last_star = usize::MAX; // pattern index after last '*'
    let mut match_from = 0; // value index when last '*' was hit

    while vi < vlen {
        if pi < plen && p[pi] == b'*' {
            last_star = pi + 1;
            match_from = vi;
            pi += 1;
        } else if pi < plen
            && (p[pi] == b'?'
                || if ignore_case {
                    p[pi].eq_ignore_ascii_case(&v[vi])
                } else {
                    p[pi] == v[vi]
                })
        {
            pi += 1;
            vi += 1;
        } else if last_star != usize::MAX {
            // Backtrack: let the last '*' consume one more character.
            match_from += 1;
            vi = match_from;
            pi = last_star;
        } else {
            return false;
        }
    }

    // Consume trailing '*' in pattern.
    while pi < plen && p[pi] == b'*' {
        pi += 1;
    }

    pi == plen
}

/// Match an ARN pattern against an ARN value, the way IAM does.
///
/// The ARN is split into its first five fields (`arn`, partition, service,
/// region, account) plus the resource field (everything after the fifth `:`)
/// as a single field. Each field is matched with `wildcard_match`, so `*` and
/// `?` in the resource field span `:` and `/`. This matters for stream ARNs,
/// whose label contains `:` (for example
/// `.../stream/2026-04-08T08:40:22.123`), and for sub-resources separated by
/// `/` such as `.../index/<name>` and `.../stream/<label>`. Both sides must
/// have the same number of fields.
///
/// # Examples
///
/// ```
/// # use extenddb_auth::policy::matcher::arn_match;
/// assert!(arn_match(
///     "arn:aws:dynamodb:*:*:table/Users",
///     "arn:aws:dynamodb:us-east-1:123456789012:table/Users"
/// ));
/// assert!(arn_match(
///     "arn:aws:dynamodb:*:*:table/T/stream/*",
///     "arn:aws:dynamodb:us-east-1:123456789012:table/T/stream/2026-04-08T08:40:22.123"
/// ));
/// ```
#[must_use]
pub fn arn_match(pattern: &str, value: &str) -> bool {
    // "*" as a pattern matches any ARN
    if pattern == "*" {
        return true;
    }

    // Split into at most six fields so the resource field (index 5) keeps its
    // own `:` and `/`; wildcards in it then span those characters.
    let p_fields: Vec<&str> = pattern.splitn(6, ':').collect();
    let v_fields: Vec<&str> = value.splitn(6, ':').collect();

    if p_fields.len() != v_fields.len() {
        return false;
    }

    p_fields
        .iter()
        .zip(v_fields.iter())
        .all(|(p, v)| wildcard_match(p, v))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- wildcard_match ---

    #[test]
    fn exact_match() {
        assert!(wildcard_match("dynamodb:PutItem", "dynamodb:PutItem"));
    }

    #[test]
    fn star_matches_all() {
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("*", ""));
    }

    #[test]
    fn star_suffix() {
        assert!(wildcard_match("dynamodb:*", "dynamodb:PutItem"));
        assert!(wildcard_match("dynamodb:*", "dynamodb:"));
    }

    #[test]
    fn star_prefix() {
        assert!(wildcard_match("*Item", "dynamodb:PutItem"));
    }

    #[test]
    fn star_middle() {
        assert!(wildcard_match("dyn*Item", "dynamodb:PutItem"));
    }

    #[test]
    fn question_mark() {
        assert!(wildcard_match("s?s", "sis"));
        assert!(!wildcard_match("s?s", "sass"));
    }

    #[test]
    fn no_match() {
        assert!(!wildcard_match("dynamodb:Get*", "dynamodb:PutItem"));
    }

    #[test]
    fn case_sensitive() {
        assert!(!wildcard_match("dynamodb:getitem", "dynamodb:GetItem"));
    }

    // --- wildcard_match_ignore_case ---

    #[test]
    fn ignore_case_exact() {
        assert!(wildcard_match_ignore_case(
            "dynamodb:getitem",
            "dynamodb:GetItem"
        ));
        assert!(wildcard_match_ignore_case(
            "dynamodb:PUTITEM",
            "dynamodb:PutItem"
        ));
    }

    #[test]
    fn ignore_case_wildcard() {
        assert!(wildcard_match_ignore_case(
            "dynamodb:get*",
            "dynamodb:GetItem"
        ));
        assert!(wildcard_match_ignore_case("DYNAMODB:*", "dynamodb:PutItem"));
    }

    #[test]
    fn ignore_case_no_match() {
        assert!(!wildcard_match_ignore_case(
            "dynamodb:get*",
            "dynamodb:PutItem"
        ));
    }

    #[test]
    fn empty_pattern_empty_value() {
        assert!(wildcard_match("", ""));
    }

    #[test]
    fn empty_pattern_nonempty_value() {
        assert!(!wildcard_match("", "x"));
    }

    #[test]
    fn multiple_stars() {
        assert!(wildcard_match("*a*b*", "xaybz"));
        assert!(!wildcard_match("*a*b*", "xyz"));
    }

    // --- arn_match ---

    #[test]
    fn arn_exact() {
        assert!(arn_match(
            "arn:aws:dynamodb:us-east-1:123:table/T",
            "arn:aws:dynamodb:us-east-1:123:table/T"
        ));
    }

    #[test]
    fn arn_wildcard_segments() {
        assert!(arn_match(
            "arn:aws:dynamodb:*:*:table/Users",
            "arn:aws:dynamodb:us-east-1:123456789012:table/Users"
        ));
    }

    #[test]
    fn arn_wildcard_in_resource() {
        assert!(arn_match(
            "arn:aws:dynamodb:*:*:table/User*",
            "arn:aws:dynamodb:us-east-1:123:table/Users"
        ));
    }

    #[test]
    fn arn_star_matches_any() {
        assert!(arn_match("*", "arn:aws:dynamodb:us-east-1:123:table/T"));
    }

    #[test]
    fn arn_segment_count_mismatch() {
        assert!(!arn_match(
            "arn:aws:dynamodb",
            "arn:aws:dynamodb:us-east-1:123:table/T"
        ));
    }

    #[test]
    fn arn_no_match() {
        assert!(!arn_match(
            "arn:aws:dynamodb:*:*:table/Orders",
            "arn:aws:dynamodb:us-east-1:123:table/Users"
        ));
    }

    #[test]
    fn arn_wildcard_stream_label_spans_colons() {
        // A stream label contains ':'. A trailing '*' on the label must match
        // across those colons.
        let value = "arn:aws:dynamodb:us-east-1:123:table/T/stream/2026-10-20T16:17:40.188";
        assert!(arn_match(
            "arn:aws:dynamodb:us-east-1:123:table/T/stream/*",
            value
        ));
        assert!(arn_match("arn:aws:dynamodb:*:*:table/T/stream/*", value));
    }

    #[test]
    fn arn_exact_stream_label_matches() {
        let value = "arn:aws:dynamodb:us-east-1:123:table/T/stream/2026-10-20T16:17:40.188";
        assert!(arn_match(value, value));
    }

    #[test]
    fn arn_resource_wildcard_spans_slash() {
        // 'table/T/*' matches any sub-resource of T (index or stream).
        assert!(arn_match(
            "arn:aws:dynamodb:*:*:table/T/*",
            "arn:aws:dynamodb:us-east-1:123:table/T/index/gsi1"
        ));
        assert!(arn_match(
            "arn:aws:dynamodb:*:*:table/T/*",
            "arn:aws:dynamodb:us-east-1:123:table/T/stream/2026-10-20T16:17:40.188"
        ));
    }

    #[test]
    fn arn_stream_wildcard_does_not_match_other_table() {
        assert!(!arn_match(
            "arn:aws:dynamodb:*:*:table/T/stream/*",
            "arn:aws:dynamodb:us-east-1:123:table/Other/stream/2026-10-20T16:17:40.188"
        ));
    }

    #[test]
    fn arn_resource_field_count_is_fixed_at_six() {
        // A value with extra ':' only in the resource still has six fields and
        // matches an exact pattern field for field.
        let value = "arn:aws:dynamodb:us-east-1:123:table/T/stream/a:b:c";
        assert!(arn_match(value, value));
        assert!(arn_match(
            "arn:aws:dynamodb:us-east-1:123:table/T/stream/*",
            value
        ));
    }
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Wildcard and ARN matching for IAM policy evaluation.
//!
//! `wildcard_match` supports `*` (zero or more characters) and `?` (exactly one
//! character). `arn_match` splits on `:` and matches each segment independently,
//! with `*` in a segment matching any value for that segment.

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

/// Match an ARN pattern against an ARN value.
///
/// ARN matching is segment-aware: each colon-separated segment is matched
/// independently using `wildcard_match`. Both pattern and value must have
/// the same number of segments (6 for standard ARNs).
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
///     "arn:aws:dynamodb:*:*:table/User*",
///     "arn:aws:dynamodb:us-east-1:123456789012:table/Users"
/// ));
/// ```
#[must_use]
pub fn arn_match(pattern: &str, value: &str) -> bool {
    // "*" as a pattern matches any ARN
    if pattern == "*" {
        return true;
    }

    let p_segments: Vec<&str> = pattern.split(':').collect();
    let v_segments: Vec<&str> = value.split(':').collect();

    if p_segments.len() != v_segments.len() {
        return false;
    }

    p_segments
        .iter()
        .zip(v_segments.iter())
        .all(|(p, v)| wildcard_match(p, v))
}

/// Whether `addr` falls inside the CIDR `cidr`, for the `IpAddress` and
/// `NotIpAddress` condition operators.
///
/// `cidr` is an IPv4 or IPv6 network in `address/prefix` form, or a bare
/// address (treated as a full-length prefix). Returns:
/// - `Some(true)` when `addr` and `cidr` are the same family and `addr` is in
///   the network,
/// - `Some(false)` when they are the same family and `addr` is not in it,
/// - `None` when either value does not parse or the families differ.
///
/// The family-mismatch `None` matters: the live service, given an IPv4 policy
/// range and an IPv6 caller (or the reverse), matches neither `IpAddress` nor
/// `NotIpAddress` (both evaluate false), rather than letting `NotIpAddress`
/// pass by negation. Measured live on this account: an IPv4 `IpAddress` range
/// and an IPv4 `NotIpAddress` range both fail to apply for an IPv6 caller.
/// Whether `cidr` is a well-formed IPv4 or IPv6 network in `address/prefix`
/// form, or a bare address.
///
/// The live service validates `IpAddress` and `NotIpAddress` values in an
/// identity or managed policy at write ("legacy parsing"), rejecting a bad
/// address, a bad or empty prefix, or a prefix out of range for the family
/// (`/33` on IPv4, `/129` on IPv6) with `MalformedPolicyDocument`. This mirrors
/// the parse rules `ip_in_cidr` applies, so a value that passes here is one it
/// can evaluate.
#[must_use]
pub fn is_valid_cidr(cidr: &str) -> bool {
    let (net_str, prefix_str) = match cidr.split_once('/') {
        Some((n, p)) => (n, Some(p)),
        None => (cidr, None),
    };
    let net = match parse_network(net_str) {
        Some(n) => n,
        None => return false,
    };
    match prefix_str {
        None => true,
        Some(p) => match (net, p.parse::<u32>()) {
            (std::net::IpAddr::V4(_), Ok(prefix)) => prefix <= 32,
            (std::net::IpAddr::V6(_), Ok(prefix)) => prefix <= 128,
            _ => false,
        },
    }
}

/// Parse an IPv4 address/network literal reading each octet as DECIMAL, so a
/// leading-zero octet is the same value as without it (`072` is 72, not octal
/// 58, and `0110` is 110). Measured: the live service reads a leading-zero octet
/// in an `IpAddress` value as decimal. Rejects a wrong octet count, an empty or
/// non-digit octet, or an octet greater than 255.
fn parse_ipv4_decimal(s: &str) -> Option<std::net::Ipv4Addr> {
    let mut octets = [0u8; 4];
    let mut parts = s.split('.');
    for octet in &mut octets {
        let part = parts.next()?;
        if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let val: u32 = part.parse().ok()?;
        if val > 255 {
            return None;
        }
        *octet = u8::try_from(val).ok()?;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(std::net::Ipv4Addr::from(octets))
}

/// Parse a CIDR network literal, matching the live service. An IPv4 literal is
/// read with decimal octets (`parse_ipv4_decimal`); an IPv6 literal is parsed by
/// the standard library (upper-case hex accepted), but an IPv4-mapped IPv6
/// address (`::ffff:a.b.c.d`) is rejected, because the service rejects it with
/// `MalformedPolicyDocument` and accepting it would store an `IpAddress` Deny
/// that silently never matches (the caller source IP is canonicalized to a bare
/// IPv4 of a different family).
fn parse_network(net_str: &str) -> Option<std::net::IpAddr> {
    use std::net::{IpAddr, Ipv6Addr};
    if net_str.contains(':') {
        let v6: Ipv6Addr = net_str.parse().ok()?;
        if v6.to_ipv4_mapped().is_some() {
            return None;
        }
        Some(IpAddr::V6(v6))
    } else {
        parse_ipv4_decimal(net_str).map(IpAddr::V4)
    }
}

#[must_use]
pub fn ip_in_cidr(cidr: &str, addr: &str) -> Option<bool> {
    use std::net::IpAddr;

    let ip: IpAddr = addr.parse().ok()?;
    let (net_str, prefix_str) = match cidr.split_once('/') {
        Some((n, p)) => (n, Some(p)),
        None => (cidr, None),
    };
    let net: IpAddr = parse_network(net_str)?;
    match (ip, net) {
        (IpAddr::V4(ip), IpAddr::V4(net)) => {
            let prefix: u32 = match prefix_str {
                Some(p) => p.parse().ok()?,
                None => 32,
            };
            if prefix > 32 {
                return None;
            }
            let mask: u32 = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            Some((u32::from(ip) & mask) == (u32::from(net) & mask))
        }
        (IpAddr::V6(ip), IpAddr::V6(net)) => {
            let prefix: u32 = match prefix_str {
                Some(p) => p.parse().ok()?,
                None => 128,
            };
            if prefix > 128 {
                return None;
            }
            let mask: u128 = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            Some((u128::from(ip) & mask) == (u128::from(net) & mask))
        }
        _ => None,
    }
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

    // --- ip_in_cidr ---

    #[test]
    fn ipv4_membership() {
        assert_eq!(ip_in_cidr("203.0.113.0/24", "203.0.113.9"), Some(true));
        assert_eq!(ip_in_cidr("203.0.113.0/24", "203.0.114.9"), Some(false));
        assert_eq!(ip_in_cidr("0.0.0.0/0", "52.94.133.133"), Some(true));
        // Bare address is a full-length (/32) prefix.
        assert_eq!(ip_in_cidr("52.94.133.133", "52.94.133.133"), Some(true));
        assert_eq!(ip_in_cidr("52.94.133.133/32", "52.94.133.134"), Some(false));
    }

    #[test]
    fn ipv6_membership() {
        assert_eq!(ip_in_cidr("2001:db8::/32", "2001:db8::1"), Some(true));
        assert_eq!(ip_in_cidr("2001:db8::/32", "2001:dead::1"), Some(false));
        assert_eq!(ip_in_cidr("::/0", "2001:db8::1"), Some(true));
    }

    #[test]
    fn family_mismatch_is_none() {
        // An IPv4 range and an IPv6 caller (and the reverse) are incomparable:
        // neither IpAddress nor NotIpAddress applies.
        assert_eq!(ip_in_cidr("203.0.113.0/24", "2001:db8::1"), None);
        assert_eq!(ip_in_cidr("2001:db8::/32", "203.0.113.9"), None);
    }

    #[test]
    fn malformed_is_none() {
        assert_eq!(ip_in_cidr("not-a-cidr", "203.0.113.9"), None);
        assert_eq!(ip_in_cidr("203.0.113.0/24", "not-an-ip"), None);
        assert_eq!(ip_in_cidr("203.0.113.0/33", "203.0.113.9"), None);
    }

    #[test]
    fn valid_cidr_rejects_mapped_ipv6_and_accepts_upper_and_padded() {
        // The service rejects an IPv4-mapped IPv6 literal in a policy CIDR with
        // MalformedPolicyDocument (measured).
        assert!(!is_valid_cidr("::ffff:10.0.0.0/120"));
        assert!(!is_valid_cidr("::ffff:72.21.196.65/128"));
        assert!(!is_valid_cidr("::ffff:10.0.0.0"));
        // Upper-case IPv6 hex and a zero-padded prefix are accepted (match service).
        assert!(is_valid_cidr("2001:DB8::/32"));
        assert!(is_valid_cidr("10.0.0.0/08"));
        // Genuine IPv6 networks are still accepted.
        assert!(is_valid_cidr("2001:db8::/32"));
        assert!(is_valid_cidr("::/0"));
        // A leading-zero octet is accepted and read as decimal (measured: the
        // service reads 072 as 72, 0110 as 110).
        assert!(is_valid_cidr("010.0.0.0/8"));
        assert!(is_valid_cidr("072.021.196.065/32"));
        assert!(is_valid_cidr("0110.0.0.0/8"));
        // An octet over 255 is still rejected.
        assert!(!is_valid_cidr("10.0.0.256/8"));
        assert!(!is_valid_cidr("0256.0.0.0/8"));
        assert!(!is_valid_cidr("10.0.0.0/33"));
    }

    #[test]
    fn ip_in_cidr_reads_leading_zero_octet_as_decimal() {
        // 072.0.0.0/8 is the 72.0.0.0/8 network (decimal), not octal 58.
        assert_eq!(ip_in_cidr("072.0.0.0/8", "72.21.196.65"), Some(true));
        assert_eq!(ip_in_cidr("072.0.0.0/8", "58.0.0.1"), Some(false));
        // 0110.0.0.0/8 is decimal 110, not octal 72.
        assert_eq!(ip_in_cidr("0110.0.0.0/8", "110.1.2.3"), Some(true));
        assert_eq!(ip_in_cidr("0110.0.0.0/8", "72.1.2.3"), Some(false));
        // A mapped IPv6 network does not parse, so neither operator applies.
        assert_eq!(ip_in_cidr("::ffff:10.0.0.0/120", "10.0.0.1"), None);
    }

    #[test]
    fn valid_cidr_accepts_well_formed_and_rejects_malformed() {
        // Well-formed (accepted at write).
        assert!(is_valid_cidr("10.0.0.0/8"));
        assert!(is_valid_cidr("0.0.0.0/0"));
        assert!(is_valid_cidr("203.0.113.9"));
        assert!(is_valid_cidr("203.0.113.9/32"));
        assert!(is_valid_cidr("2001:db8::/32"));
        assert!(is_valid_cidr("::/0"));
        // Malformed, the exact set the live service rejects with
        // MalformedPolicyDocument.
        assert!(!is_valid_cidr("10.0.0.0/33"));
        assert!(!is_valid_cidr("999.0.0.0/8"));
        assert!(!is_valid_cidr("not-a-cidr"));
        assert!(!is_valid_cidr("10.0.0.0/"));
        assert!(!is_valid_cidr("2001:db8::/129"));
    }
}

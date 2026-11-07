// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Range bounds for `begins_with` on a string sort key.

/// Exclusive upper bound for `begins_with(sk, prefix)` on a string sort key.
///
/// Sort keys are ordered by their UTF-8 bytes, which is code point order, so
/// the smallest string above every string that starts with `prefix` is the
/// prefix with its last character replaced by the next scalar value. A
/// trailing U+10FFFF has no next value and is dropped first. Returns `None`
/// when the prefix is empty or all U+10FFFF: every string at or above the
/// prefix then starts with it, and the range has no upper bound.
///
/// Appending U+10FFFF to the prefix, as an upper bound, leaves out the keys
/// that continue with U+10FFFF.
#[must_use]
pub fn string_prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        if last == char::MAX {
            continue;
        }
        // The only value in range without a successor is U+D7FF: the scalar
        // values skip the surrogates U+D800 to U+DFFF.
        chars.push(char::from_u32(u32::from(last) + 1).unwrap_or('\u{E000}'));
        return Some(chars.into_iter().collect());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::string_prefix_upper_bound;

    #[test]
    fn the_bound_is_the_next_prefix_in_code_point_order() {
        let cases: [(&str, Option<&str>); 10] = [
            ("ab", Some("ac")),
            ("a\u{7F}", Some("a\u{80}")),
            ("a\u{D7FF}", Some("a\u{E000}")),
            ("a\u{FFFF}", Some("a\u{10000}")),
            ("a\u{10FFFF}", Some("b")),
            ("a\u{10FFFF}\u{10FFFF}", Some("b")),
            ("\u{10FFFF}a", Some("\u{10FFFF}b")),
            ("\u{10FFFF}", None),
            ("\u{10FFFF}\u{10FFFF}", None),
            ("", None),
        ];
        for (prefix, want) in cases {
            assert_eq!(
                string_prefix_upper_bound(prefix).as_deref(),
                want,
                "{}",
                prefix.escape_unicode()
            );
        }
    }

    #[test]
    fn the_range_holds_every_key_with_the_prefix_and_no_other() {
        // Sort keys measured on the service on 2026-10-25: begins_with(sk, p)
        // returns exactly the keys that start with p, in UTF-8 byte order.
        let keys = [
            "a",
            "ab",
            "a\u{D7FF}",
            "a\u{E000}",
            "a\u{FFFF}",
            "a\u{10000}",
            "a\u{10FFFF}",
            "a\u{10FFFF}b",
            "a\u{10FFFF}\u{10FFFF}",
            "b",
            "\u{10FFFF}",
            "\u{10FFFF}x",
            "\u{10FFFF}\u{10FFFF}",
            "z",
        ];
        for prefix in [
            "a",
            "a\u{10FFFF}",
            "a\u{10FFFF}\u{10FFFF}",
            "a\u{D7FF}",
            "a\u{FFFF}",
            "\u{10FFFF}",
        ] {
            let upper = string_prefix_upper_bound(prefix);
            for key in keys {
                let in_range = key.as_bytes() >= prefix.as_bytes()
                    && upper.as_ref().is_none_or(|u| key.as_bytes() < u.as_bytes());
                assert_eq!(
                    in_range,
                    key.starts_with(prefix),
                    "prefix {} key {}",
                    prefix.escape_unicode(),
                    key.escape_unicode()
                );
            }
        }
    }
}

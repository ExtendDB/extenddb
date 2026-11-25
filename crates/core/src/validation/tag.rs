// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tag key and value validation, shared by `TagResource`, `UntagResource` and
//! `CreateTable.Tags`.

use unicode_general_category::{GeneralCategory, get_general_category};

use crate::error::DynamoDbError;
use crate::types::Tag;

/// Longest tag key the service accepts, in UTF-16 code units. A key of 64
/// U+20000 (two units each) is accepted and one of 65 is refused, and a key of
/// 127 ASCII letters and one U+20000 is refused (measured 2026-11-20). A key
/// of 128 two-byte UTF-8 characters such as `é` is one unit each and is
/// accepted.
const MAX_TAG_KEY_UNITS: usize = 128;

/// Longest tag value the service accepts, in UTF-16 code units: 128 U+20000
/// and 256 `é` are accepted, 129 U+20000 and 257 `é` are refused (measured
/// 2026-11-20).
const MAX_TAG_VALUE_UNITS: usize = 256;

/// Whether `c` is admissible in a tag key or value.
///
/// The grammar is `[\p{L}\p{Z}\p{N}_.:/=+\-@]`: any Unicode Letter (Lu, Ll,
/// Lt, Lm, Lo), Number (Nd, Nl, No) or Separator (Zs, Zl, Zp), plus the eight
/// listed characters. Measured against the service: `é`, `ß`, a CJK ideograph
/// and a Cyrillic letter are accepted, as are U+01C5 (Lt), U+02B0 (Lm), an
/// Arabic-Indic digit (Nd), a Roman numeral (Nl), U+00B2 (No), U+00A0 (Zs),
/// U+2028 (Zl) and U+2029 (Zp); U+0301 and U+093E (combining marks), U+200B
/// (a format character), an emoji, a tab, a newline and the other ASCII
/// punctuation characters are rejected.
///
/// The categories are those of Unicode 13.0, from `unicode-general-category`
/// 0.4.0. The service refuses letters and digits that later versions assign
/// (U+0870 and U+16AC0 from 14.0, U+31350 from 15.0, U+2EBF0 from 15.1,
/// U+1C89 from 16.0, U+323B0 from 17.0) and accepts the 13.0 additions U+08BE,
/// U+10E80 and U+30000 (measured 2026-11-20). `char::is_alphabetic` is not
/// used: it is the Alphabetic property, a superset of `\p{L}` that admits
/// Other_Alphabetic combining marks such as U+093E.
fn is_tag_char(c: char) -> bool {
    matches!(c, '_' | '.' | ':' | '/' | '=' | '+' | '-' | '@')
        || matches!(
            get_general_category(c),
            GeneralCategory::UppercaseLetter
                | GeneralCategory::LowercaseLetter
                | GeneralCategory::TitlecaseLetter
                | GeneralCategory::ModifierLetter
                | GeneralCategory::OtherLetter
                | GeneralCategory::DecimalNumber
                | GeneralCategory::LetterNumber
                | GeneralCategory::OtherNumber
                | GeneralCategory::SpaceSeparator
                | GeneralCategory::LineSeparator
                | GeneralCategory::ParagraphSeparator
        )
}

/// Validate one tag key.
///
/// An empty key, a key longer than 128 UTF-16 code units and a key holding a
/// character outside the tag grammar are all reported as an invalid key. The
/// length and character rules are applied before the reserved-prefix rule, so
/// `aws:k!` reports the invalid key rather than the prefix (measured).
///
/// # Errors
///
/// Returns `ValidationException`.
fn validate_tag_key(key: &str) -> Result<(), DynamoDbError> {
    if key.is_empty()
        || key.encode_utf16().count() > MAX_TAG_KEY_UNITS
        || !key.chars().all(is_tag_char)
    {
        return Err(DynamoDbError::ValidationException(format!(
            "The Tag Key provided is invalid, Key: {key}"
        )));
    }
    // `get` keeps a character that spans byte 4 from splitting: it yields None
    // when 4 is not a character boundary.
    if key
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("aws:"))
    {
        return Err(DynamoDbError::ValidationException(format!(
            "Tag Key cannot be prefixed with aws:, Key: {key}"
        )));
    }
    Ok(())
}

/// Validate one tag value. An empty value is accepted (measured).
///
/// # Errors
///
/// Returns `ValidationException`.
fn validate_tag_value(value: &str) -> Result<(), DynamoDbError> {
    if value.encode_utf16().count() > MAX_TAG_VALUE_UNITS || !value.chars().all(is_tag_char) {
        return Err(DynamoDbError::ValidationException(format!(
            "The Tag Value provided is invalid, Value: {value}"
        )));
    }
    Ok(())
}

/// Validate the tags of a `TagResource` or `CreateTable` request. Tags are
/// checked in request order, and the key of a tag before its value (measured).
///
/// # Errors
///
/// Returns `ValidationException` for the first key or value that is invalid.
pub fn validate_tags(tags: &[Tag]) -> Result<(), DynamoDbError> {
    for tag in tags {
        validate_tag_key(&tag.key)?;
        validate_tag_value(&tag.value)?;
    }
    Ok(())
}

/// Refuse a tag key that starts with `aws:` once U+017F (`ſ`) is read as `s`,
/// such as `awſ:foo`, which `validate_tag_key` accepts. The service refuses
/// it on `TagResource` and `CreateTable` with this message, in a later check
/// than [`validate_tags`]: on `TagResource` an absent table and a key outside
/// the grammar later in the request are reported first. `UntagResource`
/// accepts such a key (all measured 2026-11-20).
///
/// # Errors
///
/// Returns `ValidationException`.
pub fn validate_folded_tag_prefix(tags: &[Tag]) -> Result<(), DynamoDbError> {
    let folds_to_prefix = |key: &str| {
        let mut folded = key.chars().map(|c| {
            if c == '\u{17f}' {
                's'
            } else {
                c.to_ascii_lowercase()
            }
        });
        "aws:".chars().all(|want| folded.next() == Some(want))
    };
    if tags.iter().any(|tag| folds_to_prefix(&tag.key)) {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid".to_owned(),
        ));
    }
    Ok(())
}

/// Validate the `Tags` member of a raw `TagResource` or `CreateTable` request
/// body. A member that does not parse as a tag list is left to the request's
/// deserialization, which reports it.
///
/// # Errors
///
/// Returns `ValidationException` for the first key or value that is invalid.
pub fn validate_tags_member(input: &serde_json::Value) -> Result<(), DynamoDbError> {
    use serde::Deserialize;
    match input.get("Tags").map(Vec::<Tag>::deserialize) {
        Some(Ok(tags)) => validate_tags(&tags),
        _ => Ok(()),
    }
}

/// Validate the tag keys of an `UntagResource` request. The service applies the
/// same key rules there as on `TagResource` (measured).
///
/// # Errors
///
/// Returns `ValidationException` for the first key that is invalid.
pub fn validate_tag_keys(keys: &[String]) -> Result<(), DynamoDbError> {
    for key in keys {
        validate_tag_key(key)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        validate_folded_tag_prefix, validate_tag_key, validate_tag_keys, validate_tag_value,
        validate_tags, validate_tags_member,
    };
    use crate::error::DynamoDbError;
    use crate::types::Tag;

    fn message(err: DynamoDbError) -> String {
        match err {
            DynamoDbError::ValidationException(m) => m,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    fn tag(key: &str, value: &str) -> Tag {
        Tag {
            key: key.to_owned(),
            value: value.to_owned(),
        }
    }

    #[test]
    fn accepts_the_characters_the_service_accepts() {
        for key in [
            "env",
            "Abc123 _.:/=+-@",
            "k\u{e9}y",
            "k\u{df}y",
            "k\u{4e2d}y",
            "k\u{434}y",
            "k\u{1c5}y",
            "k\u{2b0}y",
            "k\u{a0}y",
            "k\u{2028}y",
            "k\u{2029}y",
            "k\u{663}y",
            "k\u{2167}y",
            "k\u{b2}y",
            " lead",
            "trail ",
            " ",
            "aws",
            "xaws:foo",
            "k\u{8be}y",
            "k\u{10e80}y",
            "k\u{30000}y",
            &"k".repeat(128),
            &"\u{e9}".repeat(128),
            &"\u{20000}".repeat(64),
            &format!("{}\u{20000}", "a".repeat(126)),
        ] {
            assert!(validate_tag_key(key).is_ok(), "key {key:?}");
        }
        for value in [
            "",
            "Abc123 _.:/=+-@",
            "v\u{e9}x",
            "v\u{1c5}x",
            "v\u{2b0}x",
            "v\u{b2}x",
            "v\u{2029}x",
            "aws:foo",
            &"v".repeat(256),
            &"\u{e9}".repeat(256),
            &"\u{20000}".repeat(128),
        ] {
            assert!(validate_tag_value(value).is_ok(), "value {value:?}");
        }
    }

    #[test]
    fn rejects_the_characters_the_service_rejects() {
        for bad in [
            '!',
            '#',
            '$',
            '%',
            '&',
            '*',
            '(',
            ',',
            ';',
            '<',
            '\'',
            '"',
            '\\',
            '`',
            '~',
            '^',
            '|',
            '{',
            '[',
            '?',
            '\t',
            '\n',
            '\u{1f600}',
            '\u{301}',
            '\u{93e}',
            '\u{200b}',
            // Letters and digits assigned after Unicode 13.0.
            '\u{870}',
            '\u{16ac0}',
            '\u{1e290}',
            '\u{31350}',
            '\u{2ebf0}',
            '\u{1c89}',
            '\u{323b0}',
        ] {
            let key = format!("k{bad}y");
            assert_eq!(
                message(validate_tag_key(&key).unwrap_err()),
                format!("The Tag Key provided is invalid, Key: {key}"),
                "key {key:?}"
            );
            let value = format!("v{bad}x");
            assert_eq!(
                message(validate_tag_value(&value).unwrap_err()),
                format!("The Tag Value provided is invalid, Value: {value}"),
                "value {value:?}"
            );
        }
    }

    #[test]
    fn rejects_an_empty_key_and_an_over_length_key_or_value() {
        assert_eq!(
            message(validate_tag_key("").unwrap_err()),
            "The Tag Key provided is invalid, Key: "
        );
        // The limits count UTF-16 code units: U+20000 is two of them, `é` one.
        for long_key in [
            "k".repeat(129),
            "\u{e9}".repeat(129),
            "\u{20000}".repeat(65),
            format!("{}\u{20000}", "a".repeat(127)),
        ] {
            assert_eq!(
                message(validate_tag_key(&long_key).unwrap_err()),
                format!("The Tag Key provided is invalid, Key: {long_key}")
            );
        }
        for long_value in [
            "v".repeat(257),
            "\u{e9}".repeat(257),
            "\u{20000}".repeat(129),
        ] {
            assert_eq!(
                message(validate_tag_value(&long_value).unwrap_err()),
                format!("The Tag Value provided is invalid, Value: {long_value}")
            );
        }
    }

    #[test]
    fn rejects_the_reserved_prefix_in_any_case() {
        for key in ["aws:foo", "AWS:foo", "Aws:foo", "aws:"] {
            assert_eq!(
                message(validate_tag_key(key).unwrap_err()),
                format!("Tag Key cannot be prefixed with aws:, Key: {key}"),
                "key {key:?}"
            );
        }
    }

    #[test]
    fn checks_characters_before_the_reserved_prefix() {
        assert_eq!(
            message(validate_tag_key("aws:k!").unwrap_err()),
            "The Tag Key provided is invalid, Key: aws:k!"
        );
        let long = format!("aws:{}", "k".repeat(130));
        assert_eq!(
            message(validate_tag_key(&long).unwrap_err()),
            format!("The Tag Key provided is invalid, Key: {long}")
        );
    }

    #[test]
    fn a_character_that_spans_byte_4_does_not_split_the_prefix_check() {
        // In the last three keys byte 4 falls inside a character, so slicing
        // the key at byte 4 would panic.
        for key in [
            "\u{4e2d}aws:",
            "\u{e9}aw",
            "ab\u{4e2d}",
            "\u{e9}\u{4e2d}",
            "abc\u{e9}",
        ] {
            assert!(validate_tag_key(key).is_ok(), "key {key:?}");
        }
    }

    #[test]
    fn a_long_s_in_the_reserved_prefix_is_refused_by_the_later_check() {
        for key in ["aw\u{17f}:foo", "AW\u{17f}:foo", "Aw\u{17f}:"] {
            assert!(validate_tag_key(key).is_ok(), "key {key:?}");
            assert_eq!(
                message(validate_folded_tag_prefix(&[tag("ok", "v"), tag(key, "v")]).unwrap_err()),
                "One or more parameter values were invalid",
                "key {key:?}"
            );
        }
        for key in [
            "aw\u{17f}",
            "aw\u{17f}x:",
            "xaw\u{17f}:",
            "a\u{17f}w:",
            "ok",
        ] {
            assert!(
                validate_folded_tag_prefix(&[tag(key, "aw\u{17f}:foo")]).is_ok(),
                "key {key:?}"
            );
        }
    }

    #[test]
    fn checks_the_key_before_the_value_and_tags_in_order() {
        assert_eq!(
            message(validate_tags(&[tag("k!", "v!")]).unwrap_err()),
            "The Tag Key provided is invalid, Key: k!"
        );
        assert_eq!(
            message(validate_tags(&[tag("ok1", "v!"), tag("k!", "ok2")]).unwrap_err()),
            "The Tag Value provided is invalid, Value: v!"
        );
        assert_eq!(
            message(validate_tags(&[tag("zk!", "v"), tag("ak#", "v")]).unwrap_err()),
            "The Tag Key provided is invalid, Key: zk!"
        );
    }

    #[test]
    fn untag_keys_follow_the_same_key_rules() {
        assert!(validate_tag_keys(&["env".to_owned(), "team".to_owned()]).is_ok());
        assert_eq!(
            message(validate_tag_keys(&["ok".to_owned(), "k!".to_owned()]).unwrap_err()),
            "The Tag Key provided is invalid, Key: k!"
        );
        assert_eq!(
            message(validate_tag_keys(&["aws:foo".to_owned()]).unwrap_err()),
            "Tag Key cannot be prefixed with aws:, Key: aws:foo"
        );
    }

    #[test]
    fn an_empty_tag_list_passes() {
        // The service refuses an empty Tags list with its own message; that
        // check is not part of this validator.
        assert!(validate_tags(&[]).is_ok());
        assert!(validate_tag_keys(&[]).is_ok());
    }

    #[test]
    fn the_tags_member_is_checked_only_when_it_parses_as_a_tag_list() {
        let bad = serde_json::json!({"Tags": [{"Key": "k!", "Value": "v"}]});
        assert_eq!(
            message(validate_tags_member(&bad).expect_err("k!")),
            "The Tag Key provided is invalid, Key: k!"
        );
        let good = serde_json::json!({"Tags": [{"Key": "k", "Value": "v"}]});
        assert!(validate_tags_member(&good).is_ok());
        // A member that is not a tag list is left to the request's deserialization.
        assert!(validate_tags_member(&serde_json::json!({"Tags": "x"})).is_ok());
        assert!(validate_tags_member(&serde_json::json!({})).is_ok());
    }
}

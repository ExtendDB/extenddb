// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tag key and value validation, shared by `TagResource`, `UntagResource` and
//! `CreateTable.Tags`.

use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};

use crate::error::DynamoDbError;
use crate::types::Tag;

/// Longest tag key the service accepts. Counted in characters rather than
/// bytes: a key of 128 two-byte characters is accepted and a key of 129
/// one-byte characters is rejected (both measured).
const MAX_TAG_KEY_CHARS: usize = 128;

/// Longest tag value the service accepts, counted in characters.
const MAX_TAG_VALUE_CHARS: usize = 256;

/// Whether `c` is admissible in a tag key or value.
///
/// The grammar is `[\p{L}\p{Z}\p{N}_.:/=+\-@]`: any Unicode Letter (Lu, Ll,
/// Lt, Lm, Lo), Number (Nd, Nl, No) or Separator (Zs, Zl, Zp), plus the eight
/// listed characters. Measured against the service: `é`, `ß`, a CJK ideograph
/// and a Cyrillic letter are accepted, as are an Arabic-Indic digit (Nd), a
/// Roman numeral (Nl), U+00A0 (Zs) and U+2028 (Zl); U+0301 and U+093E
/// (combining marks), an emoji, a tab, a newline and the other ASCII
/// punctuation characters are rejected.
///
/// The categories are read through `general_category_group` rather than
/// `char::is_alphabetic` and `char::is_numeric`, because `is_alphabetic` is the
/// Alphabetic property, a superset of `\p{L}` that admits Other_Alphabetic
/// combining marks such as U+093E, and an ASCII-space-only separator check
/// would reject the `\p{Z}` separators the service accepts.
/// `crates/server/src/management/mod.rs` applies the same grammar to IAM user
/// and role tags.
fn is_tag_char(c: char) -> bool {
    matches!(c, '_' | '.' | ':' | '/' | '=' | '+' | '-' | '@')
        || matches!(
            c.general_category_group(),
            GeneralCategoryGroup::Letter
                | GeneralCategoryGroup::Number
                | GeneralCategoryGroup::Separator
        )
}

/// Validate one tag key.
///
/// An empty key, a key longer than 128 characters and a key holding a
/// character outside the tag grammar are all reported as an invalid key. The
/// length and character rules are applied before the reserved-prefix rule, so
/// `aws:k!` reports the invalid key rather than the prefix (measured).
///
/// # Errors
///
/// Returns `ValidationException`.
pub fn validate_tag_key(key: &str) -> Result<(), DynamoDbError> {
    if key.is_empty() || key.chars().count() > MAX_TAG_KEY_CHARS || !key.chars().all(is_tag_char) {
        return Err(DynamoDbError::ValidationException(format!(
            "The Tag Key provided is invalid, Key: {key}"
        )));
    }
    // `get` keeps a multi-byte first character from splitting: it yields None
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
pub fn validate_tag_value(value: &str) -> Result<(), DynamoDbError> {
    if value.chars().count() > MAX_TAG_VALUE_CHARS || !value.chars().all(is_tag_char) {
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
    use super::{validate_tag_key, validate_tag_keys, validate_tag_value, validate_tags};
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
            "k\u{a0}y",
            "k\u{2028}y",
            "k\u{663}y",
            "k\u{2167}y",
            " lead",
            "trail ",
            " ",
            "aws",
            "xaws:foo",
            &"k".repeat(128),
            &"\u{e9}".repeat(128),
        ] {
            assert!(validate_tag_key(key).is_ok(), "key {key:?}");
        }
        for value in [
            "",
            "Abc123 _.:/=+-@",
            "v\u{e9}x",
            "aws:foo",
            &"v".repeat(256),
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
        let long_key = "k".repeat(129);
        assert_eq!(
            message(validate_tag_key(&long_key).unwrap_err()),
            format!("The Tag Key provided is invalid, Key: {long_key}")
        );
        // The limit counts characters, so 129 two-byte characters is over it
        // while 128 of them is not.
        assert!(validate_tag_key(&"\u{e9}".repeat(129)).is_err());
        let long_value = "v".repeat(257);
        assert_eq!(
            message(validate_tag_value(&long_value).unwrap_err()),
            format!("The Tag Value provided is invalid, Value: {long_value}")
        );
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
    fn a_multi_byte_first_character_does_not_split_the_prefix_check() {
        assert!(validate_tag_key("\u{4e2d}aws:").is_ok());
        assert!(validate_tag_key("\u{e9}aw").is_ok());
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
}

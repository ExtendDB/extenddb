// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for `extenddb_storage::config::string_coerce`.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Sample {
    #[serde(deserialize_with = "super::string_coerce::u32")]
    count: u32,
    #[serde(default, deserialize_with = "super::string_coerce::opt_u32")]
    optional: Option<u32>,
    #[serde(deserialize_with = "super::string_coerce::bool")]
    flag: bool,
    #[serde(deserialize_with = "super::string_coerce::u16")]
    port: u16,
    // Deliberately NOT coerced: a numeric-looking string field must stay a string.
    password: String,
}

#[test]
fn native_types_deserialize_unchanged() {
    let s: Sample = toml::from_str(
        r#"count = 7
optional = 9
flag = true
port = 8443
password = "12345""#,
    )
    .expect("native types must deserialize");
    assert_eq!(s.count, 7);
    assert_eq!(s.optional, Some(9));
    assert!(s.flag);
    assert_eq!(s.port, 8443);
    assert_eq!(s.password, "12345");
}

/// Issue #222: env-var overrides arrive as strings.
#[test]
fn string_forms_coerce_to_the_native_type() {
    let s: Sample = toml::from_str(
        r#"count = "7"
optional = "9"
flag = "true"
port = " 8443 "
password = "12345""#,
    )
    .expect("string forms must coerce");
    assert_eq!(s.count, 7);
    assert_eq!(s.optional, Some(9));
    assert!(s.flag);
    assert_eq!(s.port, 8443);
    // The plain String field is never reinterpreted.
    assert_eq!(s.password, "12345");
}

#[test]
fn absent_optional_stays_none() {
    let s: Sample = toml::from_str(
        r#"count = 1
flag = false
port = 1
password = "x""#,
    )
    .expect("absent optional must deserialize");
    assert_eq!(s.optional, None);
}

#[test]
fn garbage_strings_are_rejected_with_the_value_in_the_error() {
    let err = toml::from_str::<Sample>(
        r#"count = "seven"
flag = false
port = 1
password = "x""#,
    )
    .expect_err("non-numeric string must be rejected");
    assert!(
        err.to_string().contains("seven"),
        "error should quote the bad value, got: {err}"
    );
}

#[test]
fn negative_and_overflow_values_are_rejected() {
    assert!(
        toml::from_str::<Sample>(
            r#"count = "-1"
flag = false
port = 1
password = "x""#
        )
        .is_err()
    );
    assert!(
        toml::from_str::<Sample>(
            r#"count = 1
flag = false
port = "70000"
password = "x""#
        )
        .is_err()
    );
}

/// The messages for one `count` line of the wrong type or range, as
/// `toml::de::Error::message` gives them (the text without the source
/// snippet). A native value that does not fit fails with serde's own value
/// or type error, as it did before the string form was accepted; a bad
/// string fails with the value quoted.
const WRONG_TYPE_ROWS: [(&str, &str); 6] = [
    (
        "count = -1",
        "invalid value: integer `-1`, expected a u32 or its string form",
    ),
    (
        "count = 4294967296",
        "invalid value: integer `4294967296`, expected a u32 or its string form",
    ),
    (
        "count = 4.0",
        "invalid type: floating point `4.0`, expected a u32 or its string form",
    ),
    (
        "count = true",
        "invalid type: boolean `true`, expected a u32 or its string form",
    ),
    (
        "count = [4]",
        "invalid type: sequence, expected a u32 or its string form",
    ),
    (
        "count = \"-1\"",
        "invalid u32 value \"-1\": invalid digit found in string",
    ),
];

#[test]
fn a_wrong_type_or_range_fails_with_serdes_wording_for_a_native_value() {
    for (line, expected) in WRONG_TYPE_ROWS {
        let doc = format!("{line}\nflag = false\nport = 1\npassword = \"x\"");
        let err = toml::from_str::<Sample>(&doc).expect_err(line);
        assert_eq!(err.message(), expected, "{line}");
    }
}

/// The backends hand their `[storage.<backend>]` table to `parse_table`,
/// whose deserializer has no source text to point at, so the message carries
/// the key itself; an unknown or missing field names its key already.
#[test]
fn parse_table_names_the_key_of_a_wrong_value() {
    for (line, expected) in WRONG_TYPE_ROWS {
        let doc = format!("{line}\nflag = false\nport = 1\npassword = \"x\"");
        let table: toml::Table = toml::from_str(&doc).expect(line);
        let err = super::parse_table::<Sample>(&table, "sample").expect_err(line);
        assert_eq!(
            err,
            format!("Failed to parse sample config: count: {expected}"),
            "{line}"
        );
    }
    let table: toml::Table = toml::from_str("count = 1\nflag = false\nport = 1").expect("toml");
    assert_eq!(
        super::parse_table::<Sample>(&table, "sample").expect_err("missing field"),
        "Failed to parse sample config: missing field `password`"
    );
}

#[derive(Debug, Deserialize)]
struct ListSample {
    #[serde(default, deserialize_with = "super::string_coerce::string_list")]
    items: Vec<String>,
}

#[test]
fn a_list_key_takes_an_array_as_written() {
    let s: ListSample = toml::from_str(r#"items = ["a:9042", " b:9042 ", ""]"#).expect("array");
    assert_eq!(s.items, vec!["a:9042", " b:9042 ", ""]);
}

/// The environment form: one string, split on commas, each item trimmed,
/// empty items dropped.
#[test]
fn a_list_key_takes_one_comma_delimited_string() {
    let s: ListSample = toml::from_str(r#"items = "a:9042, b:9042,,c:9042 ""#).expect("string");
    assert_eq!(s.items, vec!["a:9042", "b:9042", "c:9042"]);
    let one: ListSample = toml::from_str(r#"items = "/data/in""#).expect("single item");
    assert_eq!(one.items, vec!["/data/in"]);
    let none: ListSample = toml::from_str(r#"items = """#).expect("empty string");
    assert!(none.items.is_empty());
}

/// A wrong type is refused with both accepted shapes named, so a startup
/// error tells the operator what the key takes; an array item that is not
/// a string fails with serde's own type error.
#[test]
fn a_list_key_rejects_other_types() {
    let rows = [
        (
            "items = 7",
            "invalid type: integer `7`, expected an array of strings or one comma-delimited string",
        ),
        (
            "items = [1, 2]",
            "invalid type: integer `1`, expected a string",
        ),
        (
            "items = true",
            "invalid type: boolean `true`, expected an array of strings or one comma-delimited string",
        ),
    ];
    for (line, expected) in rows {
        let err = toml::from_str::<ListSample>(line).expect_err(line);
        assert_eq!(err.message(), expected, "{line}");
    }
}

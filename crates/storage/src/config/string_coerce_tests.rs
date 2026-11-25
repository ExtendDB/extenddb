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

/// A native value of the wrong type or range is refused with the value,
/// the accepted type and the key in the message, so `pool_size = -1` in a
/// file reads the same as it did before the string form was accepted.
#[test]
fn native_wrong_type_errors_name_the_value_type_and_key() {
    let cases: [(&str, &[&str]); 5] = [
        ("count = -1", &["-1", "u32", "count"]),
        ("count = 4294967296", &["4294967296", "u32", "count"]),
        ("count = 4.0", &["floating point", "u32", "count"]),
        ("count = true", &["true", "u32", "count"]),
        ("count = [4]", &["sequence", "u32", "count"]),
    ];
    for (line, expected) in cases {
        let doc = format!("{line}\nflag = false\nport = 1\npassword = \"x\"");
        let err = toml::from_str::<Sample>(&doc).expect_err(line);
        let text = err.to_string();
        for needle in expected {
            assert!(
                text.contains(needle),
                "{line}: missing {needle:?} in: {text}"
            );
        }
    }
}

/// Native integers are accepted as before: a float that happens to be
/// whole is not.
#[test]
fn native_integers_are_accepted_and_whole_floats_are_not() {
    let s: Sample =
        toml::from_str("count = 7\nflag = false\nport = 1\npassword = \"x\"").expect("int");
    assert_eq!(s.count, 7);
    assert!(
        toml::from_str::<Sample>("count = 7.0\nflag = false\nport = 1\npassword = \"x\"").is_err()
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
/// error tells the operator what the key takes.
#[test]
fn a_list_key_rejects_other_types() {
    let err = toml::from_str::<ListSample>("items = 7").expect_err("integer");
    assert!(
        err.to_string()
            .contains("an array of strings or one comma-delimited string"),
        "got: {err}"
    );
    assert!(toml::from_str::<ListSample>("items = [1, 2]").is_err());
    assert!(toml::from_str::<ListSample>("items = true").is_err());
}

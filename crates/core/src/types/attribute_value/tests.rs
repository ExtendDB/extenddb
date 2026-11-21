// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn string_roundtrip() {
    let val = AttributeValue::S("hello".into());
    let json = serde_json::to_string(&val).unwrap();
    assert_eq!(json, r#"{"S":"hello"}"#);
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn number_roundtrip() {
    let val = AttributeValue::N("42".into());
    let json = serde_json::to_string(&val).unwrap();
    assert_eq!(json, r#"{"N":"42"}"#);
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn binary_roundtrip() {
    let val = AttributeValue::B(vec![1, 2, 3]);
    let json = serde_json::to_string(&val).unwrap();
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn bool_true_roundtrip() {
    let val = AttributeValue::Bool(true);
    let json = serde_json::to_string(&val).unwrap();
    assert_eq!(json, r#"{"BOOL":true}"#);
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn bool_false_roundtrip() {
    let val = AttributeValue::Bool(false);
    let json = serde_json::to_string(&val).unwrap();
    assert_eq!(json, r#"{"BOOL":false}"#);
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn null_roundtrip() {
    let val = AttributeValue::Null;
    let json = serde_json::to_string(&val).unwrap();
    assert_eq!(json, r#"{"NULL":true}"#);
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn list_roundtrip() {
    let val = AttributeValue::L(vec![
        AttributeValue::S("a".into()),
        AttributeValue::N("1".into()),
    ]);
    let json = serde_json::to_string(&val).unwrap();
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn map_roundtrip() {
    let mut m = BTreeMap::new();
    m.insert("key".into(), AttributeValue::S("value".into()));
    let val = AttributeValue::M(m);
    let json = serde_json::to_string(&val).unwrap();
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn string_set_roundtrip() {
    let mut set = BTreeSet::new();
    set.insert("a".into());
    set.insert("b".into());
    let val = AttributeValue::SS(set);
    let json = serde_json::to_string(&val).unwrap();
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn number_set_roundtrip() {
    let mut set = BTreeSet::new();
    set.insert("1".into());
    set.insert("2".into());
    let val = AttributeValue::NS(set);
    let json = serde_json::to_string(&val).unwrap();
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn binary_set_roundtrip() {
    let mut set = BTreeSet::new();
    set.insert(vec![1, 2]);
    set.insert(vec![3, 4]);
    let val = AttributeValue::BS(set);
    let json = serde_json::to_string(&val).unwrap();
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn empty_string_set_rejected() {
    let json = r#"{"SS":[]}"#;
    let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
    assert!(err.to_string().contains("may not be empty"));
}

#[test]
fn empty_number_set_rejected() {
    let json = r#"{"NS":[]}"#;
    let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
    assert!(err.to_string().contains("may not be empty"));
}

#[test]
fn empty_binary_set_rejected() {
    let json = r#"{"BS":[]}"#;
    let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
    assert!(err.to_string().contains("should not be empty"));
}

#[test]
fn null_false_rejected() {
    let json = r#"{"NULL":false}"#;
    let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
    assert!(
        err.to_string()
            .contains("Null attribute value types must have the value of true")
    );
}

#[test]
fn multiple_type_descriptors_rejected() {
    let json = r#"{"S":"hello","N":"42"}"#;
    let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
    assert!(err.to_string().contains("more than one datatypes"));
}

#[test]
fn empty_map_rejected() {
    let json = r#"{}"#;
    let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
    assert!(err.to_string().contains("empty"));
}

#[test]
fn unknown_type_descriptor_rejected() {
    let json = r#"{"X":"hello"}"#;
    let err = serde_json::from_str::<AttributeValue>(json).unwrap_err();
    assert!(err.to_string().contains("unknown"));
}

#[test]
fn invalid_number_accepted_at_deserialization() {
    // Invalid numbers are accepted by the deserializer (stored raw)
    // and rejected later by the validation layer as ValidationException.
    let json = r#"{"N":"abc"}"#;
    let val: AttributeValue = serde_json::from_str(json).unwrap();
    assert_eq!(val, AttributeValue::N("abc".to_owned()));

    let json = r#"{"N":"1E999"}"#;
    let val: AttributeValue = serde_json::from_str(json).unwrap();
    assert_eq!(val, AttributeValue::N("1E999".to_owned()));
}

#[test]
fn invalid_number_in_ns_accepted() {
    let json = r#"{"NS":["1","abc"]}"#;
    let val: AttributeValue = serde_json::from_str(json).unwrap();
    match val {
        AttributeValue::NS(set) => assert!(set.contains("abc")),
        _ => panic!("expected NS"),
    }
}

#[test]
fn nested_map_roundtrip() {
    let mut inner = BTreeMap::new();
    inner.insert("nested".into(), AttributeValue::N("99".into()));
    let mut outer = BTreeMap::new();
    outer.insert("inner".into(), AttributeValue::M(inner));
    let val = AttributeValue::M(outer);
    let json = serde_json::to_string(&val).unwrap();
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn empty_list_roundtrip() {
    let val = AttributeValue::L(vec![]);
    let json = serde_json::to_string(&val).unwrap();
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

#[test]
fn empty_map_value_roundtrip() {
    let val = AttributeValue::M(BTreeMap::new());
    let json = serde_json::to_string(&val).unwrap();
    let parsed: AttributeValue = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, val);
}

// ─── Empty AttributeValue map and multiple-keys (DDB error parity) ─────
//
// The AWS Java SDK omits null-valued fields from JSON, so calls like
// `attrVal.setBOOL(null)` serialize to a literal `{}` AttributeValue.
// Real DynamoDB rejects these with a `ValidationException` whose
// message starts with "Supplied AttributeValue is empty". Match that
// wording so the engine-level error classifier promotes it to
// `ValidationException` instead of `SerializationException`.

#[test]
fn empty_map_rejected_with_ddb_message() {
    let err = serde_json::from_str::<AttributeValue>("{}").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Supplied AttributeValue is empty"),
        "expected DDB EMPTY_ATTRIBUTE_VALUE wording, got: {msg}"
    );
    assert!(
        msg.contains("must contain exactly one of the supported datatypes"),
        "expected DDB suffix, got: {msg}"
    );
}

#[test]
fn multi_key_rejected_with_ddb_message() {
    let err = serde_json::from_str::<AttributeValue>(r#"{"S":"a","N":"1"}"#).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Supplied AttributeValue has more than one datatypes set"),
        "expected DDB MULTI_ATTRIBUTE_VALUE wording, got: {msg}"
    );
    assert!(
        msg.contains("must contain exactly one of the supported datatypes"),
        "expected DDB suffix, got: {msg}"
    );
}

#[test]
fn ss_with_null_element_rejected_with_ddb_message() {
    // Real DynamoDB returns INVALID_PARAMETER_VALUE with a SS-specific
    // suffix (note the "An string set" wording is preserved from the
    // upstream service — it's grammatically awkward but verbatim).
    let err = serde_json::from_str::<AttributeValue>(r#"{"SS":["a",null]}"#).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("One or more parameter values were invalid"),
        "expected INVALID_PARAMETER_VALUE wording, got: {msg}"
    );
    assert!(
        msg.contains("An string set may not have a null string as a member"),
        "expected SS-specific suffix, got: {msg}"
    );
}

#[test]
fn ns_with_null_element_rejected_with_ddb_message() {
    let err = serde_json::from_str::<AttributeValue>(r#"{"NS":["1",null]}"#).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Supplied AttributeValue is empty"),
        "expected DDB EMPTY_ATTRIBUTE_VALUE wording for null NS element, got: {msg}"
    );
}

#[test]
fn bs_with_null_element_rejected_with_ddb_message() {
    let err = serde_json::from_str::<AttributeValue>(r#"{"BS":["QQ==",null]}"#).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Supplied AttributeValue is empty"),
        "expected DDB EMPTY_ATTRIBUTE_VALUE wording for null BS element, got: {msg}"
    );
}

#[test]
fn empty_map_inside_list_rejected_with_ddb_message() {
    // Common path: PutItem with a List whose element is `attrVal.setBOOL(null)`.
    let err = serde_json::from_str::<AttributeValue>(r#"{"L":[{}]}"#).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Supplied AttributeValue is empty"),
        "expected DDB EMPTY_ATTRIBUTE_VALUE wording for empty inside L, got: {msg}"
    );
}

/// The text of an error inside an `L` or `M` is what
/// `serde_json::from_value` gave when each level re-deserialized a clone
/// of its subtree; the levels now consume the value instead, and these
/// strings (the API's error text) did not change. Checked from text and
/// from a `serde_json::Value`, the engine's path.
#[test]
fn nested_error_text_is_unchanged() {
    const EXPECTING: &str =
        "expected a DynamoDB AttributeValue map with exactly one type descriptor";
    let cases = [
        (
            r#"{"L":["x"]}"#,
            format!("invalid type: string \"x\", {EXPECTING}"),
            " at line 1 column 11",
        ),
        (
            r#"{"L":[{"L":[{"Q":1}]}]}"#,
            "unknown AttributeValue type descriptor: Q".to_owned(),
            " at line 1 column 23",
        ),
        (
            r#"{"M":{"k":{"S":1}}}"#,
            "S value must be a string".to_owned(),
            " at line 1 column 19",
        ),
        (
            r#"{"M":{"k":{"S":"a","N":"1"}}}"#,
            MULTI_ATTRIBUTE_VALUE.to_owned(),
            " at line 1 column 29",
        ),
        (
            r#"{"L":[{"L":[{}]}]}"#,
            EMPTY_ATTRIBUTE_VALUE.to_owned(),
            " at line 1 column 18",
        ),
        (
            r#"{"M":{"a":{"L":[null]}}}"#,
            format!("invalid type: null, {EXPECTING}"),
            " at line 1 column 24",
        ),
    ];
    for (json, message, position) in cases {
        let from_text = serde_json::from_str::<AttributeValue>(json).unwrap_err();
        assert_eq!(
            from_text.to_string(),
            format!("{message}{position}"),
            "{json}"
        );
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        let from_value = serde_json::from_value::<AttributeValue>(value).unwrap_err();
        assert_eq!(from_value.to_string(), message, "{json}");
    }
    // A message ending in ` at line 0 column 0` loses one such suffix
    // per nesting level, as `serde_json::Error::custom` parses it out at
    // each level; a nonzero position is kept. The texts are those the
    // per-level `from_value` deserializer produced.
    let at0 = " at line 0 column 0";
    let unknown = "unknown AttributeValue type descriptor: X";
    let cases = [
        (
            format!(r#"{{"M":{{"k":{{"X{at0}{at0}":1}}}}}}"#),
            unknown.to_owned(),
            " at line 1 column 57",
        ),
        (
            format!(r#"{{"L":[{{"X{at0}{at0}":1}}]}}"#),
            unknown.to_owned(),
            " at line 1 column 53",
        ),
        (
            format!(r#"{{"L":[{{"M":{{"k":{{"X{at0}{at0}{at0}":1}}}}}}]}}"#),
            unknown.to_owned(),
            " at line 1 column 84",
        ),
        (
            format!(r#"{{"M":{{"k":{{"X{at0}":1}}}}}}"#),
            unknown.to_owned(),
            " at line 1 column 38",
        ),
        (
            format!(r#"{{"X{at0}{at0}":1}}"#),
            format!("{unknown}{at0}"),
            " at line 1 column 45",
        ),
        // One suffix more than the levels strip: the nested level and
        // the outer one take one each, and the third stays in the text.
        (
            format!(r#"{{"L":[{{"X{at0}{at0}{at0}":1}}]}}"#),
            format!("{unknown}{at0}"),
            " at line 1 column 72",
        ),
        (
            r#"{"M":{"k":{"X at line 3 column 4":1}}}"#.to_owned(),
            format!("{unknown} at line 3 column 4"),
            "",
        ),
    ];
    for (json, message, position) in cases {
        let from_text = serde_json::from_str::<AttributeValue>(&json).unwrap_err();
        assert_eq!(
            from_text.to_string(),
            format!("{message}{position}"),
            "{json}"
        );
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let from_value = serde_json::from_value::<AttributeValue>(value).unwrap_err();
        assert_eq!(from_value.to_string(), message, "{json}");
    }
}

#[test]
fn empty_map_inside_map_rejected_with_ddb_message() {
    // Common path: PutItem with a Map whose value is `attrVal.setM(null)`.
    let err = serde_json::from_str::<AttributeValue>(r#"{"M":{"k":{}}}"#).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Supplied AttributeValue is empty"),
        "expected DDB EMPTY_ATTRIBUTE_VALUE wording for empty inside M, got: {msg}"
    );
}

/// The service's answer to each `B` value (measured 2026-11-20, and
/// 2026-11-21 for the values with their padding left out or cut short
/// and for the values with `=` or another character inside): the decoded
/// bytes, or the text of the refusal, empty for no message. A `BS`
/// member, and a value inside an `M` or an `L`, get the same text.
#[test]
fn a_value_that_is_not_base64_gets_the_service_text() {
    let short = || Err("Buffer is too short for base64 encoding".to_owned());
    let length = |n: usize| {
        Err(format!(
            "Base64 encoded length is expected a multiple of 4 bytes but found: {n}"
        ))
    };
    let last = || Err("Invalid last non-pad Base64 character dectected".to_owned());
    let none = || Err(String::new());
    let rows: [(&str, Result<&[u8], String>); 154] = [
        ("YWJj", Ok(b"abc")),
        ("YQ==", Ok(b"a")),
        ("YWE=", Ok(b"aa")),
        ("AA==", Ok(b"\0")),
        ("AAA=", Ok(b"\0\0")),
        ("YQ", Ok(b"a")),
        ("YQ=", Ok(b"a")),
        ("YWJjZA", Ok(b"abcd")),
        ("YWJjZA=", Ok(b"abcd")),
        ("YWI", Ok(b"ab")),
        ("a", short()),
        ("Y", short()),
        ("!", short()),
        ("=", short()),
        ("!!", length(2)),
        ("YR", length(2)),
        ("Y=", length(2)),
        ("!=", length(2)),
        ("\u{e9}", length(2)),
        ("===", length(3)),
        ("A==", length(3)),
        ("YR=", length(3)),
        ("YWJ", length(3)),
        ("YQ==\n", length(5)),
        ("Y Q==", length(5)),
        ("abcde", length(5)),
        ("YWJjZ", length(5)),
        ("YQ===", length(5)),
        ("=====", length(5)),
        ("YQ==YQ", length(6)),
        ("======", length(6)),
        ("Y===", last()),
        ("YR==", last()),
        ("YWF=", last()),
        ("YWJjYR==", last()),
        ("YWJj====", last()),
        ("====", last()),
        ("A===", last()),
        ("AB==", last()),
        ("AAB=", last()),
        ("==", none()),
        ("!!!!", none()),
        ("!!==", none()),
        ("YQ!=", none()),
        ("YQ=A", none()),
        ("YWJ!", none()),
        ("YW!j", none()),
        ("!WJj", none()),
        ("-_-_", none()),
        ("YQ==YQ==", none()),
        ("YWJjYWJj!!!!", none()),
        ("\u{e9}\u{e9}", none()),
        ("\u{1f600}", none()),
        ("YQ\u{e9}", none()),
        // Measured 2026-11-21: `=` or another byte outside the alphabet
        // inside a value, with the bytes GetItem returns.
        ("=YQ=", Ok(b"\xfd\x84")),
        (",YQ=", Ok(b"\xfd\x84")),
        ("@YQ=", Ok(b"\xfd\x84")),
        ("`YQ=", Ok(b"\xfd\x84")),
        ("=A==", Ok(b"\xfc")),
        ("=AA=", Ok(b"\xfc\0")),
        ("Y=Q=", Ok(b"\xff\xf4")),
        ("A=Q=", Ok(b"\xff\xf4")),
        ("==Q=", Ok(b"\xff\xf4")),
        ("Y(Q=", Ok(b"\xff\xf4")),
        ("Y)Q=", Ok(b"\xff\xf4")),
        ("Y*Q=", Ok(b"\xff\xf4")),
        ("Y,Q=", Ok(b"\xff\xf4")),
        ("Y-Q=", Ok(b"\xff\xf4")),
        ("Y.Q=", Ok(b"\xff\xf4")),
        ("Y:Q=", Ok(b"\xff\xf4")),
        ("Y;Q=", Ok(b"\xff\xf4")),
        ("Y<Q=", Ok(b"\xff\xf4")),
        ("Y>Q=", Ok(b"\xff\xf4")),
        ("Y?Q=", Ok(b"\xff\xf4")),
        ("Y@Q=", Ok(b"\xff\xf4")),
        ("Y[Q=", Ok(b"\xff\xf4")),
        ("Y\\Q=", Ok(b"\xff\xf4")),
        ("Y]Q=", Ok(b"\xff\xf4")),
        ("Y^Q=", Ok(b"\xff\xf4")),
        ("Y_Q=", Ok(b"\xff\xf4")),
        ("Y`Q=", Ok(b"\xff\xf4")),
        ("Y{Q=", Ok(b"\xff\xf4")),
        ("Y|Q=", Ok(b"\xff\xf4")),
        ("Y}Q=", Ok(b"\xff\xf4")),
        ("Y~Q=", Ok(b"\xff\xf4")),
        ("Y\u{7f}Q=", Ok(b"\xff\xf4")),
        ("A=A=", Ok(b"\xff\xf0")),
        ("/=A=", Ok(b"\xff\xf0")),
        ("==A=", Ok(b"\xff\xf0")),
        ("YWJj=YQ=", Ok(b"abc\xfd\x84")),
        ("YWJj,YQ=", Ok(b"abc\xfd\x84")),
        ("YWJjY=Q=", Ok(b"abc\xff\xf4")),
        ("YWJjY-Q=", Ok(b"abc\xff\xf4")),
        ("YWJjYWJjY=Q=", Ok(b"abcabc\xff\xf4")),
        ("=Y", length(2)),
        ("Y-", length(2)),
        ("=YQ", length(3)),
        ("Y=Q", length(3)),
        ("Y-Q", length(3)),
        ("YW=Jj", length(5)),
        ("Y=Q=\n", length(5)),
        ("\u{e9}YQ=", length(5)),
        ("YWJjY=", length(6)),
        ("YWJj=YQ", length(7)),
        ("YWJjY=Q", length(7)),
        ("=Y==", last()),
        ("YW==", last()),
        ("A=B=", last()),
        ("Y=R=", last()),
        ("Y,==", last()),
        ("A,==", last()),
        ("A@==", last()),
        ("YQ,=", last()),
        ("YQ@=", last()),
        ("=YQA", none()),
        ("=AAA", none()),
        ("=///", none()),
        ("Y=QA", none()),
        ("A=AA", none()),
        ("A=//", none()),
        ("==QA", none()),
        ("==AA", none()),
        ("YQ=Q", none()),
        ("AA=A", none()),
        ("Y,QA", none()),
        ("YW-j", none()),
        ("Y=QAYWJj", none()),
        ("=YQAYWJj", none()),
        ("Y=QA=YQ=", none()),
        ("YWJjY=QA", none()),
        ("YWJj=YQA", none()),
        ("Y=Q=YWJj", none()),
        ("Y-QAYWJj", none()),
        ("YWJjY,QA", none()),
        ("Y Q=", none()),
        ("Y!Q=", none()),
        ("Y\"Q=", none()),
        ("Y#Q=", none()),
        ("Y$Q=", none()),
        ("Y%Q=", none()),
        ("Y&Q=", none()),
        ("Y'Q=", none()),
        ("!YQ=", none()),
        ("Y\0Q=", none()),
        ("Y\tQ=", none()),
        ("Y\nQ=", none()),
        ("Y\u{e9}=", none()),
        // Measured 2026-11-21: more `=` at the end than the padding.
        ("YQ======", none()),
        ("========", none()),
        ("YWJjYQ======", none()),
        ("Y=======", none()),
        ("AAAA====", last()),
        ("YWJjYWJj====", last()),
        ("YQ=====", length(7)),
    ];
    for (text, want) in rows {
        let want = want.map(<[u8]>::to_vec);
        assert_eq!(decode_base64(text), want, "{text:?}");
        for json in [
            serde_json::json!({"B": text}),
            serde_json::json!({"BS": [text]}),
            serde_json::json!({"M": {"x": {"B": text}}}),
            serde_json::json!({"L": [{"B": text}]}),
        ] {
            let got = serde_json::from_value::<AttributeValue>(json.clone());
            assert_eq!(
                got.err().map(|e| e.to_string()),
                want.clone().err(),
                "{json}"
            );
        }
    }
    // A set: a member that does not decode gets its text, and members
    // with the same bytes are duplicates, each printed as the base64 of
    // its bytes (measured 2026-11-20 and 2026-11-21).
    let duplicates = |members: &str| {
        Err(format!(
            "One or more parameter values were invalid: Input collection [{members}]of type \
             BS contains duplicates."
        ))
    };
    for (members, want) in [
        (serde_json::json!(["YQ==", "!!!!"]), Err(String::new())),
        (
            serde_json::json!(["Y=Q=", "//Q="]),
            duplicates("//Q=, //Q="),
        ),
        (
            serde_json::json!(["=YQ=", "/YQ="]),
            duplicates("/YQ=, /YQ="),
        ),
        (
            serde_json::json!(["Y=Q=", "=YQ="]),
            Ok(serde_json::json!({"BS": ["/YQ=", "//Q="]})),
        ),
    ] {
        let set = serde_json::json!({ "BS": members });
        let got = serde_json::from_value::<AttributeValue>(set.clone())
            .map(|value| serde_json::to_value(value).unwrap())
            .map_err(|e| e.to_string());
        assert_eq!(got, want, "{set}");
    }
}

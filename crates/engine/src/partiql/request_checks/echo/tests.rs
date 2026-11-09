// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the echo of a statement list.

use serde_json::json;

use super::*;

/// What `write` writes for `value` into a fresh echo.
fn written(write: fn(&mut Echo, &Value), value: &Value) -> String {
    let mut echo = Echo::new();
    write(&mut echo, value);
    echo.0
}

fn binary(value: Option<&Value>) -> String {
    let mut echo = Echo::new();
    write_binary(&mut echo, value);
    echo.0
}

/// A parameter as echoed, `set` giving the members that are not null.
fn av(set: &[(&str, &str)]) -> String {
    const FIELDS: [&str; 16] = [
        "s",
        "n",
        "b",
        "iNT",
        "dECIMAL",
        "dOUBLE",
        "sS",
        "nS",
        "bS",
        "iNTSET",
        "dECIMALSET",
        "dOUBLESET",
        "m",
        "l",
        "nULL",
        "bOOL",
    ];
    let fields: Vec<String> = FIELDS
        .iter()
        .map(|name| {
            let value = set
                .iter()
                .find(|(field, _)| field == name)
                .map_or("null", |(_, value)| value);
            format!("{name}={value}")
        })
        .collect();
    format!("AttributeValue({})", fields.join(", "))
}

#[test]
fn a_parameter_is_echoed_with_every_member_of_its_class() {
    const HEAD: &str = "SELECT * FROM \"t\" WHERE pk = 'a'";
    // The measured text of a string parameter.
    assert_eq!(
        av(&[("s", "a")]),
        "AttributeValue(s=a, n=null, b=null, iNT=null, dECIMAL=null, dOUBLE=null, \
         sS=null, nS=null, bS=null, iNTSET=null, dECIMALSET=null, dOUBLESET=null, m=null, \
         l=null, nULL=null, bOOL=null)"
    );
    let buffer = "java.nio.HeapByteBuffer[pos=0 lim=1 cap=1]";
    let member = json!({
        "Statement": "SELECT * FROM \"t\" WHERE pk = ?",
        "Parameters": [
            {"S": "a"}, {"N": "1"}, {"B": "eA=="}, {"BOOL": true}, {"NULL": true},
            {"L": [{"S": "x"}]}, {"M": {"k": {"S": "v"}}}, {"SS": ["a", "b"]},
            {"NS": ["1"]}, {"BS": ["eA=="]}
        ],
        "ConsistentRead": true,
        "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
    });
    let parameters = [
        av(&[("s", "a")]),
        av(&[("n", "1")]),
        av(&[("b", buffer)]),
        av(&[("bOOL", "true")]),
        av(&[("nULL", "true")]),
        av(&[("l", &format!("[{}]", av(&[("s", "x")])))]),
        av(&[("m", &format!("{{k={}}}", av(&[("s", "v")])))]),
        av(&[("sS", "[a, b]")]),
        av(&[("nS", "[1]")]),
        av(&[("bS", &format!("[{buffer}]"))]),
    ];
    assert_eq!(
        written(write_batch_member, &member),
        format!(
            "BatchStatementRequest(statement=SELECT * FROM \"t\" WHERE pk = ?, \
             parameters=[{}], consistentRead=true, \
             returnValuesOnConditionCheckFailure=ALL_OLD)",
            parameters.join(", ")
        )
    );
    assert_eq!(
        written(write_transaction_member, &member),
        format!(
            "ParameterizedStatement(statement=SELECT * FROM \"t\" WHERE pk = ?, \
             parameters=[{}], returnValuesOnConditionCheckFailure=ALL_OLD)",
            parameters.join(", ")
        )
    );
    let member = json!({ "Statement": HEAD, "ConsistentRead": false });
    assert_eq!(
        written(write_batch_member, &member),
        format!(
            "BatchStatementRequest(statement={HEAD}, parameters=null, consistentRead=false, \
             returnValuesOnConditionCheckFailure=null)"
        )
    );
    let list = json!([member, member]);
    let one = written(write_batch_member, &member);
    assert_eq!(
        echo_list(Some(&list), write_batch_member),
        format!("[{one}, {one}]")
    );
    assert_eq!(echo_list(None, write_batch_member), "null");
}

#[test]
fn an_echo_stops_at_its_size_limit() {
    // A parameter list of NULLs echoes to about 13 times its size on the
    // wire; the echo stops at the limit instead.
    let nulls: Vec<Value> = (0..20_000).map(|_| json!({ "NULL": true })).collect();
    let mut members = vec![json!({ "Statement": "SELECT * FROM \"t\"" }); 25];
    members.push(json!({ "Statement": "SELECT * FROM \"t\" WHERE pk = ?", "Parameters": nulls }));
    let list = Value::Array(members);
    let echo = echo_list(Some(&list), write_batch_member);
    // The echo is the start of the whole text.
    let small = written(
        write_batch_member,
        &json!({ "Statement": "SELECT * FROM \"t\"" }),
    );
    let null = av(&[("nULL", "true")]);
    let whole = format!(
        "[{}, BatchStatementRequest(statement=SELECT * FROM \"t\" WHERE pk = ?, parameters=[{}], \
         consistentRead=null, returnValuesOnConditionCheckFailure=null)]",
        vec![small; 25].join(", "),
        vec![null; 20_000].join(", ")
    );
    assert!(whole.len() > 3 * MAX_ECHO_BYTES);
    assert_eq!(echo, whole[..MAX_ECHO_BYTES]);
    // A map parameter stops the same way.
    let members: Map<String, Value> = (0..20_000)
        .map(|i| (format!("k{i}"), json!({ "NULL": true })))
        .collect();
    let list = json!([{ "Statement": "s", "Parameters": [{ "M": members }] }]);
    assert_eq!(
        echo_list(Some(&list), write_transaction_member).len(),
        MAX_ECHO_BYTES
    );
}

#[test]
fn an_echo_is_cut_at_a_character_boundary() {
    let mut echo = Echo::new();
    echo.push(&"a".repeat(MAX_ECHO_BYTES - 1));
    // One byte is left, and the two-byte character does not fit in it.
    echo.push("\u{e9}\u{e9}");
    assert_eq!(echo.0.len(), MAX_ECHO_BYTES - 1);
    assert!(!echo.full());
    echo.push("b");
    assert_eq!(echo.0.len(), MAX_ECHO_BYTES);
    assert!(echo.full());
    echo.push("c");
    assert_eq!(echo.0.len(), MAX_ECHO_BYTES);
}

fn printed(map: &Value) -> Vec<&str> {
    print_order(map.as_object().unwrap())
        .into_iter()
        .map(|(name, _)| name.as_str())
        .collect()
}

#[test]
fn map_members_print_in_the_measured_order() {
    let map = json!({ "c": {"S": "1"}, "a": {"S": "2"}, "p": {"S": "3"} });
    assert_eq!(printed(&map), ["p", "a", "c"]);
    let map = json!({ "zeta": {}, "alpha": {}, "mid": {}, "beta": {} });
    assert_eq!(printed(&map), ["zeta", "mid", "beta", "alpha"]);
    let members: Map<String, Value> = (0..=12)
        .rev()
        .map(|i| (format!("k{i}"), json!({ "N": i.to_string() })))
        .collect();
    assert_eq!(
        printed(&Value::Object(members)),
        [
            "k0", "k1", "k2", "k3", "k4", "k11", "k5", "k10", "k6", "k7", "k12", "k8", "k9"
        ]
    );
}

#[test]
fn the_table_holds_size_over_three_quarters_rounded_up() {
    // 3 members take 4 buckets and 6 take 8, not 8 and 16.
    let map = json!({ "m0": {}, "m1": {}, "m2": {} });
    assert_eq!(printed(&map), ["m1", "m2", "m0"]);
    let members: Map<String, Value> = (0..6).map(|i| (format!("k{i}"), json!({}))).collect();
    assert_eq!(
        printed(&Value::Object(members)),
        ["k3", "k4", "k5", "k0", "k1", "k2"]
    );
}

#[test]
fn keys_that_share_a_bucket_keep_the_order_the_map_holds_them_in() {
    // q and a share bucket 1 of 4.
    assert_eq!(bucket("q", 4), bucket("a", 4));
    let map = json!({ "q": {"S": "1"}, "a": {"S": "2"} });
    let held: Vec<&str> = map
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(printed(&map), held);
}

#[test]
fn a_key_hashes_over_its_utf16_units() {
    // 0xD83D * 31 + 0xDE00 = 1772899, whose folded low five bits are 24.
    assert_eq!(bucket("\u{1F600}", 32), 24);
    assert_eq!(bucket("a", 1), 0);
}

#[test]
fn a_nested_map_is_echoed_in_the_same_order() {
    let value = json!({ "L": [{ "M": { "c": {"S": "1"}, "a": {"S": "2"}, "p": {"S": "3"} } }] });
    let rendered = written(write_attribute_value, &value);
    let p = rendered.find("p=").unwrap();
    let a = rendered.find("a=").unwrap();
    let c = rendered.find("c=").unwrap();
    assert!(p < a && a < c, "{rendered}");
}

#[test]
fn a_binary_is_echoed_as_the_buffer_of_its_bytes() {
    assert_eq!(
        binary(Some(&json!("AAECAwQ="))),
        "java.nio.HeapByteBuffer[pos=0 lim=5 cap=5]"
    );
    assert_eq!(binary(Some(&json!("not base64!"))), "null");
    assert_eq!(binary(None), "null");
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use super::*;
use crate::expression::CompareOp;

const HASH_MSG_2048: &str = "One or more parameter values were invalid: \
    Size of hashkey has exceeded the maximum size limit of2048 bytes";
const HASH_TOTAL_MSG_2048: &str = "One or more parameter values were invalid: \
    Aggregated size of all hash keys has exceeded the size limit of 2048 bytes";
const RANGE_MSG_1024: &str = "One or more parameter values were invalid: \
    Aggregated size of all range keys has exceeded the size limit of 1024 bytes";

fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn path(name: &str) -> Vec<PathElement> {
    vec![PathElement::Attribute(name.to_owned())]
}

fn placeholder(name: &str) -> Expr {
    Expr::Placeholder(name.to_owned())
}

fn eq(name: &str, value: &str) -> SortKeyCondition {
    SortKeyCondition::Compare {
        path: path(name),
        op: CompareOp::Eq,
        value: placeholder(value),
    }
}

fn compare(name: &str, op: CompareOp, value: &str) -> SortKeyCondition {
    SortKeyCondition::Compare {
        path: path(name),
        op,
        value: placeholder(value),
    }
}

fn maps(values: &[(&str, usize)]) -> ExpressionMaps {
    let values: HashMap<String, AttributeValue> = values
        .iter()
        .map(|(name, len)| ((*name).to_owned(), AttributeValue::S("x".repeat(*len))))
        .collect();
    ExpressionMaps::new(HashMap::new(), values)
}

/// Placeholder values with their content, for tests where the bytes matter.
fn maps_with(values: Vec<(&str, AttributeValue)>) -> ExpressionMaps {
    let values: HashMap<String, AttributeValue> = values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    ExpressionMaps::new(HashMap::new(), values)
}

fn s_of(fill: &str, len: usize) -> AttributeValue {
    AttributeValue::S(fill.repeat(len))
}

fn base_schema() -> Vec<KeySchemaElement> {
    vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)]
}

/// `h` plus RANGE `a`, `b`, `c`, `d`: the index `idx` of the measured rows
/// (service, 2026-10-21 and 2026-10-23).
fn multi_range_schema() -> Vec<KeySchemaElement> {
    vec![
        ks("h", KeyType::Hash),
        ks("a", KeyType::Range),
        ks("b", KeyType::Range),
        ks("c", KeyType::Range),
        ks("d", KeyType::Range),
    ]
}

/// HASH `h1`, `h2` plus RANGE `r`: the index `two-hash` of the measured rows
/// (service, 2026-10-23).
fn two_hash_schema() -> Vec<KeySchemaElement> {
    vec![
        ks("h1", KeyType::Hash),
        ks("h2", KeyType::Hash),
        ks("r", KeyType::Range),
    ]
}

fn hash_only(name: &str, value: &str) -> KeyCondition {
    KeyCondition {
        pk_path: path(name),
        pk_value: placeholder(value),
        extra_pk_conditions: Vec::new(),
        sk_condition: None,
        extra_sk_conditions: Vec::new(),
    }
}

fn check(
    kc: &KeyCondition,
    schema: &[KeySchemaElement],
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    validate_query_key_sizes(kc, schema, maps, &LimitsConfig::default())
}

fn message(result: Result<(), DynamoDbError>) -> String {
    match result {
        Err(DynamoDbError::ValidationException(msg)) => msg,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

/// A key condition of `attribute = :vN` clauses, each `:vN` an S value of the
/// given length. The first clause is in `pk_path` and the others in
/// `extra_pk_conditions`: the check assigns a clause by its attribute name,
/// not by its field.
fn equalities(clauses: &[(&str, usize)]) -> (KeyCondition, ExpressionMaps) {
    let names: Vec<String> = (0..clauses.len()).map(|i| format!("v{i}")).collect();
    let mut kc = hash_only(clauses[0].0, &names[0]);
    kc.extra_pk_conditions = clauses[1..]
        .iter()
        .zip(&names[1..])
        .map(|((attribute, _), name)| (path(attribute), placeholder(name)))
        .collect();
    let values: Vec<(&str, usize)> = names
        .iter()
        .zip(clauses)
        .map(|(name, (_, len))| (name.as_str(), *len))
        .collect();
    (kc, maps(&values))
}

#[test]
fn equality_clauses_are_checked_against_the_limits() {
    // The key schema, the `attribute = :v` clauses with the bytes of each
    // value, and the expected sentence (`None` for `Ok`).
    type Row<'a> = (
        &'a [KeySchemaElement],
        &'a [(&'a str, usize)],
        Option<&'a str>,
    );
    let (base, idx, two) = (base_schema(), multi_range_schema(), two_hash_schema());
    let hash = Some(HASH_MSG_2048);
    let total = Some(HASH_TOTAL_MSG_2048);
    let range = Some(RANGE_MSG_1024);
    let cases: [Row<'_>; 18] = [
        // Measured 2026-10-23 on a base table with HASH `pk` and RANGE `sk`:
        // the limits, and the hash sentence when both limits are exceeded.
        (&base, &[("pk", 2048)], None),
        (&base, &[("pk", 2049)], hash),
        (&base, &[("pk", 1), ("sk", 1024)], None),
        (&base, &[("pk", 1), ("sk", 1025)], range),
        (&base, &[("pk", 2049), ("sk", 1025)], hash),
        // Measured 2026-10-23 on `idx`: `a` and `c` of 600 bytes with `b` of 1
        // byte is refused, `a` of 600 bytes alone passes. Each value is under
        // the limit alone; together they are not. 512 and 512 pass, 512 and
        // 513 are refused.
        (&idx, &[("h", 1), ("a", 600), ("b", 1), ("c", 600)], range),
        (&idx, &[("h", 1), ("a", 600)], None),
        (&idx, &[("h", 1), ("a", 512), ("b", 512)], None),
        (&idx, &[("h", 1), ("a", 512), ("b", 513)], range),
        // Measured 2026-10-23 on `two-hash`: `h1` of 2049 bytes and `h2` of 1
        // byte get the single sentence. 1025 and 1024 bytes (sum 2049, neither
        // over) and 2048 and 2048 bytes get the aggregated sentence.
        (&two, &[("h1", 2049), ("h2", 1)], hash),
        (&two, &[("h1", 1), ("h2", 2049)], hash),
        (&two, &[("h1", 1025), ("h2", 1024)], total),
        (&two, &[("h1", 2048), ("h2", 2048)], total),
        // Measured 2026-11-20 on `two-hash`: a sum of exactly 2048 bytes
        // passes, and with the HASH sum and a 1025-byte RANGE value both over,
        // the aggregated hash sentence comes back.
        (&two, &[("h1", 1024), ("h2", 1024)], None),
        (&two, &[("h1", 1025), ("h2", 1024), ("r", 1025)], total),
        // Not measured: the single hash check runs before the sum.
        (&two, &[("h1", 2049), ("h2", 2048)], hash),
        // Two clauses on one attribute: the attribute counts its larger value
        // once. The service refuses both with `Invalid KeyConditionExpression:
        // KeyConditionExpressions must only contain one condition per key`
        // (measured 2026-11-24 with 1025 bytes in each `pk` value; #489
        // records the `sk` shape), a refusal this check does not make.
        (&base, &[("pk", 1025), ("pk", 1025)], None),
        (&base, &[("pk", 1), ("sk", 600), ("sk", 600)], None),
    ];
    for (schema, clauses, expected) in cases {
        let (kc, m) = equalities(clauses);
        let result = check(&kc, schema, &m);
        match expected {
            Some(sentence) => assert_eq!(message(result), sentence, "{clauses:?}"),
            None => assert!(result.is_ok(), "{clauses:?}: {result:?}"),
        }
    }
}

#[test]
fn a_non_equality_on_a_later_range_attribute_is_added_to_the_total() {
    // Measured 2026-10-23 on `idx`: `a` of 1 byte with `b > :v` of 1025 bytes
    // is refused.
    let mut kc = hash_only("h", "h");
    kc.extra_sk_conditions = vec![(path("a"), placeholder("x"))];
    kc.sk_condition = Some(compare("b", CompareOp::Gt, "v"));
    let m = maps(&[("h", 1), ("x", 1), ("v", 1025)]);
    assert_eq!(
        message(check(&kc, &multi_range_schema(), &m)),
        RANGE_MSG_1024
    );
}

#[test]
fn clauses_are_assigned_by_attribute_name_not_by_field() {
    // The parser leaves RANGE equalities in extra_pk_conditions until the
    // schema classifier moves them. The sizes still add up by name, so the
    // measured `idx` rows hold whether or not the classifier has run.
    let mut kc = hash_only("h", "h");
    kc.extra_pk_conditions = vec![(path("a"), placeholder("x"))];
    kc.sk_condition = Some(eq("b", "y"));
    let m = maps(&[("h", 1), ("x", 600), ("y", 600)]);
    assert_eq!(
        message(check(&kc, &multi_range_schema(), &m)),
        RANGE_MSG_1024
    );
}

fn between(name: &str, low: &str, high: &str) -> SortKeyCondition {
    SortKeyCondition::Between {
        path: path(name),
        low: placeholder(low),
        high: placeholder(high),
    }
}

#[test]
fn between_contributes_its_larger_bound_not_the_sum() {
    // Measured 2026-10-23: 600 and 600 (sum 1200) pass.
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(between("sk", "l", "u"));
    let m = maps(&[("p", 1), ("l", 600), ("u", 600)]);
    assert!(check(&kc, &base_schema(), &m).is_ok());
}

#[test]
fn between_with_a_1025_byte_upper_bound_is_refused() {
    // Measured 2026-10-23: bounds of 1 and 1025 bytes.
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(between("sk", "l", "u"));
    let m = maps(&[("p", 1), ("l", 1), ("u", 1025)]);
    assert_eq!(message(check(&kc, &base_schema(), &m)), RANGE_MSG_1024);
}

#[test]
fn between_with_a_1025_byte_lower_bound_is_refused() {
    // Measured 2026-10-23: lower bound `a` x 1025, upper bound `z` (an ordered
    // pair) is refused with the range sentence, and `a` x 1024 passes. The
    // reversed pair, a 1025-byte lower bound above a 1-byte upper bound, is
    // refused by the service with a bound-order sentence ahead of the size
    // check; ExtendDB has no bound-order check yet, and this test pins only
    // the sizing of the ordered pair.
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(between("sk", "l", "u"));
    let m = maps_with(vec![
        ("p", s_of("p", 1)),
        ("l", s_of("a", 1025)),
        ("u", s_of("z", 1)),
    ]);
    assert_eq!(message(check(&kc, &base_schema(), &m)), RANGE_MSG_1024);
    let m = maps_with(vec![
        ("p", s_of("p", 1)),
        ("l", s_of("a", 1024)),
        ("u", s_of("z", 1)),
    ]);
    assert!(check(&kc, &base_schema(), &m).is_ok());
}

#[test]
fn a_between_adds_its_larger_bound_to_the_range_total() {
    // Measured 2026-10-23 on `idx`: `a` of 600 bytes with `b BETWEEN` 1 and
    // 424 bytes (total 1024) passes, with 1 and 600 bytes (total 1200) is
    // refused.
    let mut kc = hash_only("h", "h");
    kc.extra_sk_conditions = vec![(path("a"), placeholder("x"))];
    kc.sk_condition = Some(between("b", "l", "u"));
    let m = maps(&[("h", 1), ("x", 600), ("l", 1), ("u", 424)]);
    assert!(check(&kc, &multi_range_schema(), &m).is_ok());
    let m = maps(&[("h", 1), ("x", 600), ("l", 1), ("u", 600)]);
    assert_eq!(
        message(check(&kc, &multi_range_schema(), &m)),
        RANGE_MSG_1024
    );
}

#[test]
fn every_comparison_operator_and_begins_with_are_sized() {
    // Measured 2026-10-23: a 1025-byte value on each form is refused and a
    // 1024-byte value passes.
    let forms: Vec<fn(&str) -> SortKeyCondition> = vec![
        |v| compare("sk", CompareOp::Gt, v),
        |v| compare("sk", CompareOp::Ge, v),
        |v| compare("sk", CompareOp::Lt, v),
        |v| compare("sk", CompareOp::Le, v),
        |v| SortKeyCondition::BeginsWith {
            path: path("sk"),
            prefix: placeholder(v),
        },
    ];
    for form in forms {
        let mut kc = hash_only("pk", "p");
        kc.sk_condition = Some(form("v"));
        let m = maps(&[("p", 1), ("v", 1025)]);
        assert_eq!(message(check(&kc, &base_schema(), &m)), RANGE_MSG_1024);
        let m = maps(&[("p", 1), ("v", 1024)]);
        assert!(check(&kc, &base_schema(), &m).is_ok());
    }
}

#[test]
fn binary_values_count_their_bytes() {
    let kc = hash_only("pk", "v");
    let values = HashMap::from([("v".to_owned(), AttributeValue::B(vec![0u8; 2049]))]);
    let m = ExpressionMaps::new(HashMap::new(), values);
    assert_eq!(message(check(&kc, &base_schema(), &m)), HASH_MSG_2048);
}

#[test]
fn multibyte_strings_count_utf8_bytes_not_characters() {
    // Measured 2026-10-23: 683 U+20AC (3 bytes each, 2049 bytes) in the hash
    // value is refused; 682 of them plus `xx` (2048 bytes) passes. In a range
    // value, 342 of them (1026 bytes) is refused and 341 plus `x` (1024 bytes)
    // passes.
    let euro = "\u{20ac}";
    let hash = |value: String| {
        let kc = hash_only("pk", "v");
        let m = maps_with(vec![("v", AttributeValue::S(value))]);
        check(&kc, &base_schema(), &m)
    };
    assert_eq!(message(hash(euro.repeat(683))), HASH_MSG_2048);
    assert!(hash(format!("{}xx", euro.repeat(682))).is_ok());
    let range = |value: String| {
        let mut kc = hash_only("pk", "p");
        kc.sk_condition = Some(eq("sk", "v"));
        let m = maps_with(vec![
            ("p", AttributeValue::S("p".to_owned())),
            ("v", AttributeValue::S(value)),
        ]);
        check(&kc, &base_schema(), &m)
    };
    assert_eq!(message(range(euro.repeat(342))), RANGE_MSG_1024);
    assert!(range(format!("{}x", euro.repeat(341))).is_ok());
}

#[test]
fn name_references_resolve_through_expression_attribute_names() {
    let kc = hash_only("#p", "v");
    let m = ExpressionMaps::new(
        HashMap::from([("p".to_owned(), "pk".to_owned())]),
        HashMap::from([("v".to_owned(), AttributeValue::S("x".repeat(2049)))]),
    );
    assert_eq!(message(check(&kc, &base_schema(), &m)), HASH_MSG_2048);
}

#[test]
fn limits_come_from_the_configuration_and_the_sentence_names_them() {
    let limits = LimitsConfig {
        max_partition_key_size_bytes: 10,
        max_sort_key_size_bytes: 20,
        ..LimitsConfig::default()
    };
    let mut kc = hash_only("pk", "p");
    kc.sk_condition = Some(eq("sk", "v"));
    let hash =
        validate_query_key_sizes(&kc, &base_schema(), &maps(&[("p", 11), ("v", 1)]), &limits);
    assert!(message(hash).ends_with("limit of10 bytes"));
    let range =
        validate_query_key_sizes(&kc, &base_schema(), &maps(&[("p", 10), ("v", 21)]), &limits);
    assert!(message(range).ends_with("limit of 20 bytes"));
}

#[test]
fn a_clause_on_an_attribute_outside_the_key_schema_adds_nothing() {
    let kc = hash_only("other", "v");
    assert!(check(&kc, &base_schema(), &maps(&[("v", 5000)])).is_ok());
}

#[test]
fn an_undefined_reference_returns_ok_whatever_the_sizes() {
    // A label, the key condition, and the bytes of each defined value.
    type Row<'a> = (&'a str, KeyCondition, &'a [(&'a str, usize)]);
    let with_sort = |pk_value: &str, sk: SortKeyCondition| KeyCondition {
        sk_condition: Some(sk),
        ..hash_only("pk", pk_value)
    };
    let cases: [Row<'_>; 6] = [
        (
            "undefined value on the clause",
            hash_only("pk", "missing"),
            &[("v", 5000)],
        ),
        (
            "undefined name on the clause",
            hash_only("#nope", "v"),
            &[("v", 5000)],
        ),
        // Measured 2026-10-23: `pk = :v AND sk = :missing` with `:v` of 2049
        // bytes is refused as an undefined attribute value, not as a size.
        (
            "undefined value in another clause",
            with_sort("v", eq("sk", "missing")),
            &[("v", 2049)],
        ),
        // Measured 2026-10-23: `pk = :v AND #n = :w` with `#n` undefined and
        // `:v` of 2049 bytes is refused as an undefined attribute name, not as
        // a size.
        (
            "undefined name in another clause",
            with_sort("v", eq("#n", "w")),
            &[("v", 2049), ("w", 1)],
        ),
        // The oversized clause may come before or after the undefined one.
        (
            "oversized clause after the undefined one",
            with_sort("missing", eq("sk", "v")),
            &[("v", 5000)],
        ),
        (
            "undefined bound of a BETWEEN",
            with_sort("p", between("sk", "l", "missing")),
            &[("p", 1), ("l", 5000)],
        ),
    ];
    for (label, kc, values) in cases {
        let result = check(&kc, &base_schema(), &maps(values));
        assert!(result.is_ok(), "{label}: {result:?}");
    }
}

#[test]
fn non_scalar_values_are_skipped() {
    let kc = hash_only("pk", "v");
    let values = HashMap::from([(
        "v".to_owned(),
        AttributeValue::L(vec![AttributeValue::S("x".repeat(5000))]),
    )]);
    let m = ExpressionMaps::new(HashMap::new(), values);
    assert!(check(&kc, &base_schema(), &m).is_ok());
}

#[test]
fn number_values_get_the_number_rules_before_any_size() {
    // Measured 2026-11-20 in us-east-1 on a GSI with HASH `n` (N). A number
    // with more than 38 significant digits or out of range gets the number's
    // sentence, never the size sentence. A valid one is sized by its
    // normalized digits. A number in other Unicode decimal digits is served.
    // The RANGE rows apply the same rule to a sort key value. The 2049 `x`
    // and 2049 U+FF11 rows are values the validator does not read as a
    // number: they add nothing, so the size check passes them. The service
    // refuses them with a number sentence (measured 2026-11-24: `The parameter
    // cannot be converted to a numeric value: xxx...` for 2049 `x`, and the
    // digit sentence for 39 U+FF11).
    const DIGITS: &str = "Attempting to store more than 38 significant digits in a Number";
    const OVERFLOW: &str =
        "Number overflow. Attempting to store a number with magnitude larger than supported range";
    let zeros = "0".repeat(2100);
    let cases: [(&str, &str, String, Option<&str>); 10] = [
        ("hash, 2049 digits", "pk", "1".repeat(2049), Some(DIGITS)),
        ("hash, 39 digits", "pk", "1".repeat(39), Some(DIGITS)),
        (
            "hash, 1 and 2100 zeros",
            "pk",
            format!("1{zeros}"),
            Some(OVERFLOW),
        ),
        ("hash, 2100 zeros and 1", "pk", format!("{zeros}1"), None),
        ("hash, U+FF11", "pk", "\u{ff11}".to_owned(), None),
        (
            "hash, U+0967 U+0968",
            "pk",
            "\u{967}\u{968}".to_owned(),
            None,
        ),
        ("range, 1025 digits", "sk", "1".repeat(1025), Some(DIGITS)),
        ("range, 2100 zeros and 1", "sk", format!("{zeros}1"), None),
        ("hash, 2049 x", "pk", "x".repeat(2049), None),
        ("hash, 2049 U+FF11", "pk", "\u{ff11}".repeat(2049), None),
    ];
    for (label, attribute, n, expected) in cases {
        let (kc, values) = if attribute == "sk" {
            let mut kc = hash_only("pk", "p");
            kc.sk_condition = Some(eq("sk", "v"));
            (kc, vec![("p", s_of("p", 1)), ("v", AttributeValue::N(n))])
        } else {
            (hash_only("pk", "p"), vec![("p", AttributeValue::N(n))])
        };
        let result = check(&kc, &base_schema(), &maps_with(values));
        match expected {
            Some(sentence) => assert_eq!(message(result), sentence, "{label}"),
            None => assert!(result.is_ok(), "{label}: {result:?}"),
        }
    }
}

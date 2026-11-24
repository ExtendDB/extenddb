// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The search condition against the index search schema.
//!
//! Every expected answer was measured against the live service in
//! eu-north-1 on 2026-11-23, each request twice with the same answer,
//! against a vector table with the search schema of the case; `Ok` stands
//! for a search the service answered with 200, the accepted shapes of the
//! two partition tests included. The partition a condition selects is
//! internal; the wire tests pin that the HASH value scopes the search.

use std::collections::HashMap;

use super::parse_search_filter;
use super::tests_support::{OK, message, n, p, s, used_values};
use crate::types::{
    AttributeDefinition, AttributeValue, ScalarAttributeType, SearchSchemaElement,
    SearchSchemaElementType,
};

const NOT_MUST_MATCH: &str = "SearchConditionExpression must use = on the HASH attribute, not \
                              inside OR or NOT; HASH attribute: Category";
const MORE_THAN_ONCE: &str = "SearchConditionExpression must contain only one condition on \
                              the HASH attribute; to search more than one HASH value, make one \
                              SearchVectors call for each value";
const HASH_MISSING: &str =
    "SearchConditionExpression must have all HASH attributes in configured SearchSchema";

fn not_in_schema(attribute: &str) -> String {
    format!(
        "SearchConditionExpression must not contain any attributes that is not in SearchSchema. \
         Invalid attribute: {attribute}"
    )
}

fn number_only(operator: &str, attribute: &str, attribute_type: &str) -> String {
    format!(
        "SearchConditionExpression allows this operator only on Number (N) attributes; \
         operator: {operator}, attribute: {attribute}, attribute type: {attribute_type}"
    )
}

fn wrong_type(attribute: &str, given: &str, declared: &str) -> String {
    format!(
        "Type of '{attribute}' attribute in SearchConditionExpression ({given}) does not match \
         type in search schema ({declared})"
    )
}

struct Index {
    schema: Vec<SearchSchemaElement>,
    definitions: Vec<AttributeDefinition>,
}

/// `(name, type, is_hash)` for each search schema element.
fn index(elements: &[(&str, ScalarAttributeType, bool)]) -> Index {
    Index {
        schema: elements
            .iter()
            .map(|(name, _, hash)| SearchSchemaElement {
                attribute_name: (*name).to_owned(),
                element_type: if *hash {
                    SearchSchemaElementType::Hash
                } else {
                    SearchSchemaElementType::InlineFilter
                },
            })
            .collect(),
        definitions: elements
            .iter()
            .map(|(name, t, _)| AttributeDefinition {
                attribute_name: (*name).to_owned(),
                attribute_type: *t,
            })
            .collect(),
    }
}

use ScalarAttributeType::{B, N, S};

/// HASH `Category` (S); `Country` (S), `ProductCount` (N), `Thumbnail` (B).
fn with_hash() -> Index {
    index(&[
        ("Category", S, true),
        ("Country", S, false),
        ("ProductCount", N, false),
        ("Thumbnail", B, false),
    ])
}

/// `Country` (S) and `ProductCount` (N), no HASH.
fn without_hash() -> Index {
    index(&[("Country", S, false), ("ProductCount", N, false)])
}

fn values() -> HashMap<String, AttributeValue> {
    [
        (":s", s("Electronics")),
        (":other", s("Books")),
        (":n", n("5")),
        (":hi", n("10")),
        (":b", AttributeValue::B(b"hi".to_vec())),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

fn outcome(index: &Index, expr: &str, all: &HashMap<String, AttributeValue>) -> String {
    let names: HashMap<String, String> = [
        ("#country", "Country"),
        ("#category", "Category"),
        ("#c", "Category"),
        ("#u", "Color"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v.to_owned()))
    .collect();
    let used = used_values(expr, all);
    match parse_search_filter(expr, Some(&names), Some(&used), 50).and_then(|filter| {
        filter
            .validate_against_schema(Some(&index.schema), &index.definitions)
            .map(|_| ())
    }) {
        Ok(()) => OK.to_owned(),
        Err(e) => message(e),
    }
}

fn assert_all(index: &Index, all: &HashMap<String, AttributeValue>, cases: &[(&str, &str)]) {
    let mismatches: Vec<String> = cases
        .iter()
        .filter_map(|(expr, expected)| {
            let actual = outcome(index, expr, all);
            (actual != *expected)
                .then(|| format!("{expr:?}: expected {expected:?}, got {actual:?}"))
        })
        .collect();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[test]
fn the_hash_must_be_one_must_match_equals() {
    assert_all(
        &with_hash(),
        &values(),
        &[
            ("Category <> :s", NOT_MUST_MATCH),
            ("Category > :s", NOT_MUST_MATCH),
            (":s <= Category", NOT_MUST_MATCH),
            ("Category BETWEEN :other AND :s", NOT_MUST_MATCH),
            ("Category = :s AND Category > :other", NOT_MUST_MATCH),
            ("Category > :other AND Category = :s", NOT_MUST_MATCH),
            ("Category = :s AND Category IN (:s, :other)", NOT_MUST_MATCH),
            (
                "Category = :s AND attribute_exists(Category)",
                NOT_MUST_MATCH,
            ),
            (
                "Category = :s AND Category BETWEEN :other AND :s",
                NOT_MUST_MATCH,
            ),
            ("Category = :s AND Category = :other", MORE_THAN_ONCE),
            ("Category = :s AND Category = :s", MORE_THAN_ONCE),
            ("ProductCount > :n", HASH_MISSING),
            ("Country = :s AND ProductCount > :n", HASH_MISSING),
            // the HASH checks run before the per-condition checks
            ("Color = :s AND Category > :s", NOT_MUST_MATCH),
            (
                "Color = :s AND Category = :s AND Category = :other",
                MORE_THAN_ONCE,
            ),
            ("Color > :n", HASH_MISSING),
            ("Category = :s OR Country = :s", NOT_MUST_MATCH),
            ("ProductCount > :n OR Category = :s", NOT_MUST_MATCH),
            ("NOT (Category = :s)", NOT_MUST_MATCH),
            ("NOT NOT (Category = :s)", NOT_MUST_MATCH),
            ("Category IN (:s, :other)", NOT_MUST_MATCH),
            ("Category IN (:s)", NOT_MUST_MATCH),
            ("NOT Category IN (:s, :other)", NOT_MUST_MATCH),
            ("attribute_exists(Category)", NOT_MUST_MATCH),
            ("attribute_not_exists(Category)", NOT_MUST_MATCH),
            (
                "(Category = :s AND ProductCount > :n) OR (Category = :other AND ProductCount < :hi)",
                NOT_MUST_MATCH,
            ),
            (
                "(Category = :s AND Country = :s) OR (Category = :s AND ProductCount > :n)",
                NOT_MUST_MATCH,
            ),
            (
                "Category = :s AND Country = :s OR ProductCount > :n",
                NOT_MUST_MATCH,
            ),
            (
                "Category = :s AND (Category = :s OR ProductCount > :n)",
                NOT_MUST_MATCH,
            ),
            // a #name reports the attribute it names
            ("#c <> :s", NOT_MUST_MATCH),
        ],
    );
}

#[test]
fn ordering_needs_a_number_attribute() {
    let mut cases: Vec<(String, String)> = Vec::new();
    for op in ["<", "<=", ">", ">="] {
        cases.push((
            format!("Category = :s AND Country {op} :other"),
            number_only(op, "Country", "S"),
        ));
        cases.push((
            format!("Category = :s AND :other {op} Country"),
            number_only(op, "Country", "S"),
        ));
        cases.push((
            format!("Category = :s AND Thumbnail {op} :b"),
            number_only(op, "Thumbnail", "B"),
        ));
    }
    for (expr, expected) in [
        (
            "Category = :s AND Country BETWEEN :other AND :s",
            number_only("BETWEEN", "Country", "S"),
        ),
        (
            "Category = :s AND Thumbnail BETWEEN :b AND :b",
            number_only("BETWEEN", "Thumbnail", "B"),
        ),
        // the operator is checked before the value type
        (
            "Category = :s AND Country > :n",
            number_only(">", "Country", "S"),
        ),
        (
            "Category = :s AND Country BETWEEN :n AND :hi",
            number_only("BETWEEN", "Country", "S"),
        ),
        ("Category = :s AND Color > :n", not_in_schema("Color")),
        (
            "Category = :s AND ProductCount > :s",
            wrong_type("ProductCount", "S", "N"),
        ),
        (
            "Category = :s AND ProductCount BETWEEN :other AND :s",
            wrong_type("ProductCount", "S", "N"),
        ),
        // written order decides between two conditions
        (
            "Category = :s AND Country > :s AND Color = :s",
            number_only(">", "Country", "S"),
        ),
        (
            "Category = :s AND Color = :s AND Country > :s",
            not_in_schema("Color"),
        ),
        (
            "Category = :s AND (ProductCount > :n OR Country > :other)",
            number_only(">", "Country", "S"),
        ),
        (
            "Category = :s AND NOT (Thumbnail > :b)",
            number_only(">", "Thumbnail", "B"),
        ),
        // <> is not an ordering operator
        ("Category = :s AND Country <> :other", OK.to_owned()),
        ("Category = :s AND Thumbnail <> :b", OK.to_owned()),
    ] {
        cases.push((expr.to_owned(), expected));
    }
    let refs: Vec<(&str, &str)> = cases
        .iter()
        .map(|(e, x)| (e.as_str(), x.as_str()))
        .collect();
    assert_all(&with_hash(), &values(), &refs);
}

#[test]
fn in_lists_and_functions_are_checked_against_the_schema() {
    let country_n = wrong_type("Country", "N", "S");
    let count_s = wrong_type("ProductCount", "S", "N");
    let color = not_in_schema("Color");
    assert_all(
        &with_hash(),
        &values(),
        &[
            ("Category = :s AND attribute_exists(Color)", &color),
            ("Category = :s AND Color IN (:s)", &color),
            ("Category = :s AND Country IN (:other, :n)", &country_n),
            ("Category = :s AND NOT ProductCount IN (:n, :s)", &count_s),
            ("Category = :s AND attribute_exists(ProductCount)", OK),
            ("Category = :s AND attribute_not_exists(Thumbnail)", OK),
        ],
    );
}

#[test]
fn the_first_failure_in_written_order_answers() {
    assert_all(
        &with_hash(),
        &values(),
        &[
            (
                "Category = :s AND Color = :s AND ProductCount = :s",
                &not_in_schema("Color"),
            ),
            (
                "Category = :s AND ProductCount = :s AND Color = :s",
                &wrong_type("ProductCount", "S", "N"),
            ),
            ("Color = :s AND ProductCount = :n", HASH_MISSING),
            (
                "Category = :n AND Color = :s",
                &wrong_type("Category", "N", "S"),
            ),
            ("Color = :s AND Category = :n", &not_in_schema("Color")),
        ],
    );
}

#[test]
fn accepted_shapes_select_the_hash_partition() {
    let index = with_hash();
    for expr in [
        "Category = :s",
        "Category = :s AND ProductCount > :n",
        "Category = :s AND ProductCount > :n AND ProductCount < :hi",
        "ProductCount >= :n AND :hi >= ProductCount AND Category = :s",
        "Category = :s AND :n <= ProductCount",
        "Category = :s AND Country <> :other",
        "Category = :s AND Thumbnail <> :b",
        "Category = :s AND Country = :s AND Country = :other",
        "Category = :s AND (Country = :s OR ProductCount > :n)",
        ":s = Category AND NOT (Country = :other)",
        "Category = :s AND attribute_exists(Country)",
        "Category = :s AND attribute_not_exists(Thumbnail)",
        "Category = :s AND Country IN (:s, :other)",
        "Category = :s AND NOT Country IN (:s, :other)",
        "Category = :s AND ProductCount BETWEEN :n AND :hi",
        "Category = :s AND ((Country = :s AND ProductCount > :n) OR (Country = :other AND ProductCount < :hi))",
    ] {
        let used = used_values(expr, &values());
        let filter = parse_search_filter(expr, None, Some(&used), 50).unwrap();
        let selected = filter
            .validate_against_schema(Some(&index.schema), &index.definitions)
            .unwrap_or_else(|e| panic!("{expr}: {e:?}"));
        assert_eq!(selected, Some(("Category", &s("Electronics"))), "{expr}");
    }
}

#[test]
fn an_index_without_hash_searches_the_whole_index() {
    let index = without_hash();
    for expr in [
        "ProductCount > :n",
        "ProductCount > :n AND ProductCount < :hi",
        "ProductCount BETWEEN :n AND :hi",
        "Country = :s AND Country = :other",
        "Country <> :s AND :n < ProductCount",
        "Country = :s OR Country = :other",
        "NOT Country IN (:s, :other) AND attribute_exists(ProductCount)",
    ] {
        let used = used_values(expr, &values());
        let filter = parse_search_filter(expr, None, Some(&used), 50).unwrap();
        let selected = filter
            .validate_against_schema(Some(&index.schema), &index.definitions)
            .unwrap_or_else(|e| panic!("{expr}: {e:?}"));
        assert_eq!(selected, None, "{expr}");
    }
}

/// HASH `Country` (S); `Category` and `Brand` (S); every value is `S` "USA".
#[test]
fn rules_with_a_string_only_schema() {
    let index = index(&[
        ("Country", S, true),
        ("Category", S, false),
        ("Brand", S, false),
    ]);
    let all: HashMap<String, AttributeValue> = [":a", ":b", ":c"]
        .into_iter()
        .map(|k| (k.to_owned(), s("USA")))
        .collect();
    let country = "SearchConditionExpression must use = on the HASH attribute, not inside OR \
                   or NOT; HASH attribute: Country";
    assert_all(
        &index,
        &all,
        &[
            ("#country <> :c AND #category = :a", country),
            (
                "#country = :a AND #country = :b AND #category = :c",
                MORE_THAN_ONCE,
            ),
            (
                "#country = :c AND #category > :a AND #category < :b",
                &number_only(">", "Category", "S"),
            ),
            (
                "#country = :c AND #category BETWEEN :a AND :b",
                &number_only("BETWEEN", "Category", "S"),
            ),
            ("#country BETWEEN :a AND :b AND #category = :c", country),
            ("#country = :c OR #category = :a OR #category = :b", country),
            ("NOT (#country = :c) AND #category IN (:a, :b)", country),
            ("#country IN (:c, :a) AND #category = :b", country),
            (
                "attribute_exists(#country) AND #category IN (:c, :a, :b)",
                country,
            ),
            (
                "#country = :c AND #category IN (:a, :b, #country)",
                &p(
                    "The IN operator list must contain only expression attribute values (:value), \
                    not attribute names",
                ),
            ),
            (
                "#country = :c AND #category BETWEEN :a AND #country",
                &p(
                    "The BETWEEN operator bounds must be expression attribute values (:value), \
                    not attribute names",
                ),
            ),
            (
                "#country = :c AND begins_with(#category, :a) AND #category = :b",
                &p("The function is not allowed in a search condition; function: begins_with"),
            ),
            // a #name is reported as the attribute it names
            ("#country = :c AND #u = :a", &not_in_schema("Color")),
        ],
    );
}

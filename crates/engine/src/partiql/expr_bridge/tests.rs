// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the operand and condition bridge.

use super::*;
use extenddb_core::partiql::ast::Statement;
use extenddb_core::partiql::parse;

fn where_of(sql: &str) -> WhereExpr {
    match parse(sql).unwrap() {
        Statement::Select(s) => s.where_clause.unwrap(),
        _ => panic!("not a select"),
    }
}

#[test]
fn renders_equality_with_name_and_value_placeholders() {
    let w = where_of("SELECT * FROM \"t\" WHERE pk = 'user1'");
    let mut b = ExprBridge::new(&[]);
    let s = b.render_condition(&w).unwrap();
    assert_eq!(s, "#n0 = :v0");
    assert_eq!(b.names_value().unwrap()["#n0"], "pk");
    assert_eq!(b.values_value().unwrap().unwrap()[":v0"]["S"], "user1");
}

#[test]
fn renders_nested_path_on_leaf_name_placeholders() {
    let w = where_of("SELECT * FROM \"t\" WHERE addr.city = 'NYC'");
    let mut b = ExprBridge::new(&[]);
    let s = b.render_condition(&w).unwrap();
    assert_eq!(s, "#n0.#n1 = :v0");
}

#[test]
fn renders_begins_with_and_between_and_in() {
    let mut b = ExprBridge::new(&[]);
    let w = where_of("SELECT * FROM \"t\" WHERE begins_with(\"sk\", 'p')");
    assert_eq!(b.render_condition(&w).unwrap(), "begins_with(#n0, :v0)");

    let mut b = ExprBridge::new(&[]);
    let w = where_of("SELECT * FROM \"t\" WHERE sk BETWEEN 'a' AND 'z'");
    assert_eq!(b.render_condition(&w).unwrap(), "#n0 BETWEEN :v0 AND :v1");

    let mut b = ExprBridge::new(&[]);
    let w = where_of("SELECT * FROM \"t\" WHERE sk IN ['a','b']");
    assert_eq!(b.render_condition(&w).unwrap(), "#n0 IN (:v0, :v1)");
}

#[test]
fn renders_is_missing_as_attribute_not_exists() {
    let mut b = ExprBridge::new(&[]);
    let w = where_of("SELECT * FROM \"t\" WHERE addr IS MISSING");
    assert_eq!(b.render_condition(&w).unwrap(), "attribute_not_exists(#n0)");

    let mut b = ExprBridge::new(&[]);
    let w = where_of("SELECT * FROM \"t\" WHERE addr IS NOT MISSING");
    assert_eq!(b.render_condition(&w).unwrap(), "attribute_exists(#n0)");
}

#[test]
fn renders_is_null_as_an_equality_with_null() {
    let mut b = ExprBridge::new(&[]);
    let w = where_of("SELECT * FROM \"t\" WHERE nul IS NULL");
    assert_eq!(b.render_condition(&w).unwrap(), "#n0 = :v0");
    assert_eq!(b.values_value().unwrap().unwrap()[":v0"]["NULL"], true);

    let mut b = ExprBridge::new(&[]);
    let w = where_of("SELECT * FROM \"t\" WHERE s IS NOT NULL");
    assert_eq!(b.render_condition(&w).unwrap(), "NOT #n0 = :v0");
}

#[test]
fn renders_is_type_as_attribute_type() {
    for (sql, code) in [
        ("SELECT * FROM \"t\" WHERE flag IS BOOL", "BOOL"),
        ("SELECT * FROM \"t\" WHERE s IS STRING", "S"),
        ("SELECT * FROM \"t\" WHERE n IS NUMERIC", "N"),
        ("SELECT * FROM \"t\" WHERE m IS STRUCT", "M"),
        ("SELECT * FROM \"t\" WHERE l IS LIST", "L"),
    ] {
        let mut b = ExprBridge::new(&[]);
        let w = where_of(sql);
        assert_eq!(
            b.render_condition(&w).unwrap(),
            "attribute_type(#n0, :v0)",
            "{sql}"
        );
        assert_eq!(
            b.values_value().unwrap().unwrap()[":v0"]["S"],
            code,
            "{sql}"
        );
    }
}

#[test]
fn a_run_of_nots_renders_as_one_not_or_none() {
    for (nots, rendered) in [
        (1, "NOT #n0 = :v0"),
        (2, "#n0 = :v0"),
        (3, "NOT #n0 = :v0"),
        (299, "NOT #n0 = :v0"),
    ] {
        let mut b = ExprBridge::new(&[]);
        let sql = format!("SELECT * FROM \"t\" WHERE {}a = 1", "NOT ".repeat(nots));
        assert_eq!(
            b.render_condition(&where_of(&sql)).unwrap(),
            rendered,
            "{nots}"
        );
    }
    let mut b = ExprBridge::new(&[]);
    let w = where_of("SELECT * FROM \"t\" WHERE NOT NOT (a = 1 OR b = 2)");
    assert_eq!(b.render_condition(&w).unwrap(), "(#n0 = :v0 OR #n1 = :v1)");
}

fn rendered(condition: &str) -> String {
    let sql = format!("SELECT * FROM \"t\" WHERE {condition}");
    ExprBridge::new(&[])
        .render_condition(&where_of(&sql))
        .unwrap()
}

#[test]
fn parentheses_go_only_around_an_or_inside_an_and() {
    for (condition, text) in [
        (
            "a = 1 AND (b = 1 OR c = 1)",
            "#n0 = :v0 AND (#n1 = :v1 OR #n2 = :v2)",
        ),
        (
            "(a = 1 OR b = 1) AND c = 1",
            "(#n0 = :v0 OR #n1 = :v1) AND #n2 = :v2",
        ),
        (
            "a = 1 OR b = 1 AND c = 1",
            "(#n0 = :v0 OR #n1 = :v1 AND #n2 = :v2)",
        ),
        (
            "a = 1 AND (b = 1 AND c = 1)",
            "#n0 = :v0 AND #n1 = :v1 AND #n2 = :v2",
        ),
        (
            "a = 1 OR (b = 1 OR c = 1)",
            "(#n0 = :v0 OR #n1 = :v1 OR #n2 = :v2)",
        ),
        ("((a = 1))", "#n0 = :v0"),
    ] {
        assert_eq!(rendered(condition), text, "{condition}");
    }
}

#[test]
fn a_not_is_moved_onto_the_predicates_under_it() {
    for (condition, text) in [
        ("NOT (a = 1 AND b = 1)", "(NOT #n0 = :v0 OR NOT #n1 = :v1)"),
        ("NOT (a = 1 OR b = 1)", "NOT #n0 = :v0 AND NOT #n1 = :v1"),
        (
            "NOT (a = 1 AND NOT (b = 1 OR c = 1))",
            "(NOT #n0 = :v0 OR #n1 = :v1 OR #n2 = :v2)",
        ),
        (
            "c = 1 AND NOT (a = 1 AND b = 1)",
            "#n0 = :v0 AND (NOT #n1 = :v1 OR NOT #n2 = :v2)",
        ),
        ("NOT a IS MISSING", "attribute_exists(#n0)"),
        ("NOT a IS NOT MISSING", "attribute_not_exists(#n0)"),
        ("NOT a IS NOT NULL", "#n0 = :v0"),
        ("NOT attribute_exists(a) = false", "attribute_exists(#n0)"),
    ] {
        assert_eq!(rendered(condition), text, "{condition}");
    }
}

#[test]
fn the_not_forms_render_as_negated_predicates() {
    assert_eq!(rendered("a NOT IN [1, 2]"), "NOT #n0 IN (:v0, :v1)");
    assert_eq!(rendered("a NOT IN (1, 2)"), "NOT #n0 IN (:v0, :v1)");
    assert_eq!(rendered("a IN (1, 2)"), "#n0 IN (:v0, :v1)");
    assert_eq!(
        rendered("a NOT BETWEEN 1 AND 2"),
        "NOT #n0 BETWEEN :v0 AND :v1"
    );
    assert_eq!(rendered("NOT a NOT IN [1]"), "#n0 IN (:v0)");
}

/// Conditions within the 300 operators the service takes, nested as
/// deep as that allows.
fn deep_conditions() -> Vec<String> {
    let alternated = |op: &str, depth: usize| {
        format!("NOT (a = 1 {op} ").repeat(depth) + "a = 1" + &")".repeat(depth)
    };
    let nested = |innermost: &str| "(a = 1 AND ".repeat(149) + innermost + &")".repeat(149);
    vec![
        alternated("AND", 99),
        alternated("OR", 99),
        nested("a = 1"),
        nested("NOT a = 1"),
        format!("NOT ({})", vec!["a = 1"; 150].join(" AND ")),
        "a = 1 AND (a = 1 OR ".repeat(74) + "a = 1" + &")".repeat(74),
        "(a = 1 OR (a = 1 AND ".repeat(74) + "a = 1" + &"))".repeat(74),
    ]
}

#[test]
fn a_deep_condition_within_the_operator_limit_passes_the_classic_parser() {
    let limits = extenddb_core::limits::LimitsConfig::default();
    for condition in deep_conditions() {
        let sql = format!("SELECT * FROM \"t\" WHERE {condition}");
        let text = ExprBridge::new(&[])
            .render_condition(&where_of(&sql))
            .unwrap();
        let parsed = crate::expression_helpers::parse_optional_filter(Some(&text), &limits);
        assert!(parsed.is_ok(), "{condition}: {parsed:?}");
    }
}

#[test]
fn a_deep_condition_is_written_on_a_small_stack() {
    let conditions: Vec<WhereExpr> = deep_conditions()
        .iter()
        .map(|c| where_of(&format!("SELECT * FROM \"t\" WHERE {c}")))
        .collect();
    // A recursive walk took about 9 KB a level on a debug build.
    std::thread::Builder::new()
        .stack_size(128 << 10)
        .spawn(move || {
            for condition in &conditions {
                ExprBridge::new(&[]).render_condition(condition).unwrap();
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

/// The condition written the way the bridge wrote it before
/// `condition_text`: every AND, OR and NOT in parentheses where it is.
fn nested_text(expr: &WhereExpr, bridge: &mut ExprBridge<'_>) -> String {
    match expr {
        WhereExpr::And(a, b) => format!(
            "({} AND {})",
            nested_text(a, bridge),
            nested_text(b, bridge)
        ),
        WhereExpr::Or(a, b) => {
            format!("({} OR {})", nested_text(a, bridge), nested_text(b, bridge))
        }
        WhereExpr::Not(inner) => format!("(NOT {})", nested_text(inner, bridge)),
        WhereExpr::ComparedWithBoolean(inner, comparison) if comparison.negates() => {
            format!("(NOT {})", nested_text(inner, bridge))
        }
        WhereExpr::ComparedWithBoolean(inner, _) => nested_text(inner, bridge),
        leaf => bridge.predicate(leaf, false).unwrap(),
    }
}

#[test]
fn the_condition_text_means_what_the_condition_says() {
    use extenddb_core::expression::evaluate_condition;
    let limits = extenddb_core::limits::LimitsConfig::default();
    let evaluate =
        |text: &str, bridge: &ExprBridge<'_>, item: &BTreeMap<String, AttributeValue>| {
            let filter = crate::expression_helpers::parse_optional_filter(Some(text), &limits)
                .unwrap()
                .unwrap();
            let maps = crate::expression_helpers::build_expression_maps(
                Some(&bridge.names_map()),
                Some(&bridge.values_map()),
            );
            evaluate_condition(&filter, item, &maps).unwrap()
        };
    let values = [None, Some("1"), Some("2")];
    let items: Vec<BTreeMap<String, AttributeValue>> = values
        .iter()
        .flat_map(|a| values.iter().map(move |b| (a, b)))
        .flat_map(|(a, b)| values.iter().map(move |c| (a, b, c)))
        .map(|(a, b, c)| {
            [("a", a), ("b", b), ("c", c)]
                .into_iter()
                .filter_map(|(name, v)| {
                    v.map(|n| (name.to_owned(), AttributeValue::N(n.to_owned())))
                })
                .collect()
        })
        .collect();
    for condition in [
        "NOT (a = 1 AND b = 1)",
        "NOT (a = 1 OR NOT (b = 2 AND c = 1))",
        "a = 1 OR b = 1 AND NOT (c = 1 OR a = 2)",
        "NOT ((a = 1 OR b = 2) AND (c = 1 OR NOT a IN [2]))",
        "a NOT IN [1, 2] OR b NOT BETWEEN 1 AND 1 AND c <> 2",
        "NOT (a IS MISSING OR NOT b IS NOT MISSING) OR NOT c IS NULL",
        "NOT NOT (a = 1 AND NOT NOT NOT b = 1)",
        "(a = 1 OR b = 2) = false AND NOT c = 1 = false",
    ] {
        let expr = where_of(&format!("SELECT * FROM \"t\" WHERE {condition}"));
        let mut bridge = ExprBridge::new(&[]);
        let text = bridge.render_condition(&expr).unwrap();
        let mut reference = ExprBridge::new(&[]);
        let nested = nested_text(&expr, &mut reference);
        for item in &items {
            assert_eq!(
                evaluate(&text, &bridge, item),
                evaluate(&nested, &reference, item),
                "{condition} on {item:?}"
            );
        }
    }
}

/// A name of one or two letters for the `n`th attribute, for `n` below
/// 182. The first letters of the two-letter names start no reserved word.
fn short_name(n: usize) -> String {
    let letter = |i: usize| char::from(b'a' + u8::try_from(i % 26).unwrap());
    if n < 26 {
        letter(n).to_string()
    } else {
        format!(
            "{}{}",
            ["j", "k", "q", "w", "x", "z"][n / 26 - 1],
            letter(n)
        )
    }
}

#[test]
fn the_text_written_for_a_condition_is_at_most_five_bytes_a_character() {
    // The densest conditions the operator limit allows: the shortest
    // names and values, the fewest characters between them.
    let comparisons: Vec<String> = (0..150).map(|i| format!("{}=1", short_name(i))).collect();
    let members = vec!["1"; 100].join(",");
    for condition in [
        comparisons.join(" AND "),
        comparisons.join(" OR "),
        format!("a IN [{members}]"),
        format!("a NOT IN [{members}] OR b IN ({members})"),
        format!("NOT ({})", comparisons[..140].join(" AND ")),
    ] {
        let text = rendered(&condition);
        assert!(
            text.len() <= 5 * condition.len(),
            "{} bytes for {} characters: {condition}",
            text.len(),
            condition.len()
        );
    }
}

#[test]
fn binds_positional_parameter() {
    let params = [AttributeValue::S("p1".to_owned())];
    let mut b = ExprBridge::new(&params);
    let w = where_of("SELECT * FROM \"t\" WHERE pk = ?");
    assert_eq!(b.render_condition(&w).unwrap(), "#n0 = :v0");
    assert_eq!(b.values_value().unwrap().unwrap()[":v0"]["S"], "p1");
}

#[test]
fn too_few_parameters_is_count_mismatch() {
    let mut b = ExprBridge::new(&[]);
    let w = where_of("SELECT * FROM \"t\" WHERE pk = ?");
    let err = b.render_condition(&w).unwrap_err();
    match err {
        DynamoDbError::ValidationException(m) => {
            assert_eq!(
                m,
                "Number of parameters in request and statement don't match."
            );
        }
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

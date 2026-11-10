// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the predicates that follow an operand in a condition.

use crate::partiql::ast::{CompareOp, Operand, Path, Statement, WhereExpr};
use crate::partiql::parse;
use crate::partiql::parser::{IN_OPERANDS, limits};
use crate::types::AttributeValue;

const WF: &str = "Statement wasn't well formed, can't be processed: ";

fn err(sql: &str) -> String {
    parse(sql).unwrap_err().message
}

fn wf(reason: &str) -> String {
    format!("{WF}{reason}")
}

fn where_of(sql: &str) -> WhereExpr {
    match parse(sql).unwrap() {
        Statement::Select(s) => s.where_clause.unwrap(),
        other => panic!("expected select, got {other:?}"),
    }
}

fn path(name: &str) -> Operand {
    Operand::Path(Path {
        root: name.to_owned(),
        steps: Vec::new(),
    })
}

fn number(n: &str) -> Operand {
    Operand::Literal(AttributeValue::N(n.to_owned()))
}

#[test]
fn not_in_and_not_between_negate_the_predicate_after_the_not() {
    let not_in = WhereExpr::Not(Box::new(WhereExpr::In(
        path("a"),
        vec![number("1"), number("2")],
    )));
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a NOT IN [1, 2]"),
        not_in
    );
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a not in (1, 2)"),
        not_in
    );
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a NOT BETWEEN 1 AND 2"),
        WhereExpr::Not(Box::new(WhereExpr::Between(
            path("a"),
            number("1"),
            number("2")
        )))
    );
    // A NOT before the operand applies to the whole predicate.
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE NOT a NOT IN [1, 2]"),
        WhereExpr::Not(Box::new(not_in))
    );
}

#[test]
fn in_takes_its_members_in_parentheses() {
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a IN (1, 2)"),
        WhereExpr::In(path("a"), vec![number("1"), number("2")])
    );
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a IN (?, ?)"),
        WhereExpr::In(
            path("a"),
            vec![Operand::Parameter(0), Operand::Parameter(1)]
        )
    );
}

#[test]
fn the_not_forms_parse_wherever_a_condition_does() {
    for sql in [
        "SELECT * FROM \"t\" WHERE pk = 'a' AND a NOT IN [1]",
        "SELECT * FROM \"t\" WHERE a NOT IN [?, ?] OR a NOT BETWEEN ? AND ?",
        "SELECT * FROM \"t\".\"gsi\" WHERE g = 1 AND a NOT BETWEEN 1 AND 2",
        "UPDATE \"t\" SET q = 1 WHERE pk = 'a' AND a NOT IN (1, 2)",
        "UPDATE \"t\" SET q = 1 WHERE pk = 'a' AND a NOT BETWEEN 5 AND 6 RETURNING ALL NEW *",
        "DELETE FROM \"t\" WHERE pk = 'a' AND a NOT IN [1]",
        "EXISTS(SELECT * FROM \"t\" WHERE pk = 'a' AND a NOT IN [1])",
        "FROM \"t\" WHERE pk = 'a' AND a NOT BETWEEN 1 AND 2 SET q = 1",
    ] {
        assert!(parse(sql).is_ok(), "{sql}");
    }
}

#[test]
fn a_not_form_counts_two_operators() {
    let nots =
        |n: usize, term: &str| format!("SELECT * FROM \"t\" WHERE {}{term}", "NOT ".repeat(n));
    for term in [
        "a NOT IN [1]",
        "a NOT IN (1, 2, 3)",
        "a NOT BETWEEN 1 AND 2",
    ] {
        assert!(parse(&nots(298, term)).is_ok(), "{term}");
        assert_eq!(
            err(&nots(299, term)),
            limits::too_many_operators(),
            "{term}"
        );
    }
    // IN in parentheses counts one, however many members it has.
    assert!(parse(&nots(299, "a IN (1, 2, 3)")).is_ok());
}

#[test]
fn malformed_not_forms_get_the_messages_of_their_predicates() {
    for (condition, expected) in [
        ("a NOT IN 1", IN_OPERANDS.to_owned()),
        ("a NOT IN []", IN_OPERANDS.to_owned()),
        ("a IN ()", wf("Unexpected term")),
        ("a NOT IN ()", wf("Unexpected term")),
        ("a IN (1,)", wf("Unexpected term")),
        ("(a NOT = 1)", wf("Expected RIGHT_PAREN")),
        ("a NOT", wf("Unexpected token after expression")),
        (
            "a NOT AND pk = 'a'",
            wf("Unexpected token after expression"),
        ),
        ("a NOT NOT IN [1]", wf("Unexpected token after expression")),
        ("a NOT BETWEEN 1", wf("Expected AND keyword")),
        ("a NOT = 1", wf("Unexpected token after expression")),
        ("a NOT IS MISSING", wf("Unexpected token after expression")),
        ("a IS NOT IN [1]", wf("Expected type name")),
        (
            "a NOT IN",
            wf("Missing right-hand side expression of infix operator"),
        ),
        (
            "a NOT BETWEEN",
            wf("Missing right-hand side expression of infix operator"),
        ),
        ("a NOT BETWEEN 1 AND", wf("Expected expression after AND")),
        ("a IN (1, 2", wf("Expected RIGHT_PAREN")),
    ] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            expected,
            "{condition}"
        );
    }
}

fn list(items: &[&str]) -> Operand {
    Operand::Literal(AttributeValue::L(
        items
            .iter()
            .map(|n| AttributeValue::N((*n).to_owned()))
            .collect(),
    ))
}

#[test]
fn a_member_in_parentheses_of_its_own_is_a_list_or_what_they_hold() {
    for (condition, members) in [
        ("a IN ((1, 2))", vec![list(&["1", "2"])]),
        ("a IN ((5))", vec![number("5")]),
        ("a IN ([5])", vec![list(&["5"])]),
        ("a IN ([1], 2)", vec![list(&["1"]), number("2")]),
        ("a IN (1)", vec![number("1")]),
        ("a IN (pk, 1)", vec![path("pk"), number("1")]),
    ] {
        assert_eq!(
            where_of(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            WhereExpr::In(path("a"), members),
            "{condition}"
        );
    }
}

fn string(s: &str) -> Operand {
    Operand::Literal(AttributeValue::S(s.to_owned()))
}

#[test]
fn in_takes_its_members_in_a_bag() {
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a IN <<1, 5>>"),
        WhereExpr::In(path("a"), vec![number("1"), number("5")])
    );
    // The bag is not checked as a set literal.
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a IN <<1, 1>>"),
        WhereExpr::In(path("a"), vec![number("1"), number("1")])
    );
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a IN <<'x', 1>>"),
        WhereExpr::In(path("a"), vec![string("x"), number("1")])
    );
    assert_eq!(err("SELECT * FROM \"t\" WHERE a IN <<>>"), IN_OPERANDS);
}

#[test]
fn a_name_inside_a_list_in_parentheses_is_refused_at_its_position() {
    let sql = "SELECT * FROM \"t\" WHERE a IN ((pk, 1))";
    let column = sql.find("pk").unwrap() + 1;
    assert_eq!(
        err(sql),
        format!("Unsupported data type: Id under key root[0] at 1:{column}:2")
    );
}

#[test]
fn a_predicate_may_be_compared_with_a_boolean() {
    let not_in = WhereExpr::Not(Box::new(WhereExpr::In(path("a"), vec![number("1")])));
    for condition in ["a NOT IN [1] = true", "(a NOT IN [1]) = true"] {
        assert_eq!(
            where_of(&format!("SELECT * FROM \"t\" WHERE {condition}")).without_kept_comparisons(),
            &not_in,
            "{condition}"
        );
    }
    let pk = WhereExpr::Compare(path("pk"), CompareOp::Eq, string("a"));
    let compared = |equal, literal| {
        WhereExpr::ComparedWithBoolean(
            Box::new(pk.clone()),
            crate::partiql::ast::BooleanComparison { equal, literal },
        )
    };
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a IN [1] AND pk = 'a' = true"),
        WhereExpr::And(
            Box::new(WhereExpr::In(path("a"), vec![number("1")])),
            Box::new(compared(true, true))
        )
    );
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE (pk = 'a') = false"),
        compared(true, false)
    );
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE pk = 'a' <> true"),
        compared(false, true)
    );
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE pk = 'a' <> FALSE"),
        compared(false, false)
    );
}

#[test]
fn an_is_test_after_a_predicate_is_refused_for_its_left_side() {
    for condition in [
        "a IN (1, 2) IS MISSING",
        "a NOT IN (1, 2) IS MISSING",
        "a IN [1] IS NOT MISSING",
        "a BETWEEN 1 AND 2 IS MISSING",
        "a = 1 IS MISSING",
    ] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            super::super::IS_LEFT_SIDE,
            "{condition}"
        );
    }
}

#[test]
fn an_in_list_of_more_than_100_members_is_refused() {
    let select = |n: usize| {
        let members: Vec<String> = (0..n).map(|i| i.to_string()).collect();
        format!("SELECT * FROM \"t\" WHERE a IN [{}]", members.join(", "))
    };
    for n in [101, 1000] {
        assert_eq!(err(&select(n)), limits::too_many_in_operands(n), "{n}");
    }
    assert!(parse(&select(100)).is_ok());
}

#[test]
fn like_is_refused_as_an_operator_a_condition_does_not_take() {
    let like = "Unsupported operator in Condition Expression. Operator: like";
    for condition in ["a LIKE 'x'", "a NOT LIKE 'x'", "a LIKE 'x' ESCAPE 'y'"] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            like,
            "{condition}"
        );
    }
    let missing_rhs = wf("Missing right-hand side expression of infix operator");
    assert_eq!(err("SELECT * FROM \"t\" WHERE a NOT LIKE"), missing_rhs);
    // A syntax error after it wins.
    assert_eq!(err("SELECT * FROM \"t\" WHERE a LIKE 'x' AND"), missing_rhs);
}

#[test]
fn like_inside_the_argument_of_a_call_is_refused_as_like() {
    let like = "Unsupported operator in Condition Expression. Operator: like";
    for condition in [
        "foo(a LIKE 'x')",
        "foo(b, a LIKE 'x' ESCAPE 'y')",
        "b = foo(a LIKE 'x')",
    ] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            like,
            "{condition}"
        );
    }
    // An argument before it is met first.
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE foo(1e126, a LIKE 'x')"),
        "Number overflow. Attempting to store a number with magnitude larger than \
         supported range under root"
    );
    // NOT LIKE there, a condition function that starts a predicate and
    // LIKE in a SET value are not measured.
    for condition in ["foo(a NOT LIKE 'x')", "size(a LIKE 'x') = 1"] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            wf("Expected RIGHT_PAREN"),
            "{condition}"
        );
    }
    assert_eq!(
        err("UPDATE \"t\" SET q = foo(a LIKE 'x') WHERE pk = 'a'"),
        wf("Expected RIGHT_PAREN")
    );
}

#[test]
fn escape_with_nothing_after_it() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE a LIKE 'x' ESCAPE"),
        wf("Expected expression after ESCAPE")
    );
}

#[test]
fn is_missing_on_a_number_or_a_string_needs_a_document_path() {
    let path = "Operator or function requires a document path; operator or function: \
                attribute_not_exists";
    for condition in ["1 IS MISSING", "'x' IS NOT MISSING"] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            path,
            "{condition}"
        );
    }
    // Other literals are not measured.
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE [1] IS MISSING"),
        "IS MISSING requires an attribute path"
    );
}

#[test]
fn an_is_after_an_is_test_is_refused_for_its_left_side() {
    for condition in ["a IS NULL IS NULL", "a IS MISSING IS NOT NULL"] {
        assert_eq!(
            err(&format!("SELECT * FROM \"t\" WHERE {condition}")),
            super::super::IS_LEFT_SIDE,
            "{condition}"
        );
    }
    // A comparison with a boolean after it is not measured.
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE a IS NULL = true"),
        wf("Unexpected token after expression")
    );
}

#[test]
fn a_name_in_a_bag_after_in_is_an_operand() {
    assert_eq!(
        where_of("SELECT * FROM \"t\" WHERE a IN <<pk, 1>>"),
        WhereExpr::In(path("a"), vec![path("pk"), number("1")])
    );
}

#[test]
fn a_quoted_name_a_path_or_a_call_in_a_list_in_parentheses_is_refused_at_its_position() {
    for (members, token, kind, length) in [
        ("((\"pk\", 1))", "\"pk\"", "Id", 4),
        ("((m.b, 1))", "m.b", "Path", 1),
        ("((foo(b), 1))", "foo", "Call", 3),
        ("((1, foo(1 + 1)))", "foo", "Call", 3),
    ] {
        let sql = format!("SELECT * FROM \"t\" WHERE a IN {members}");
        let column = sql.find(token).unwrap() + 1;
        let index = usize::from(members.starts_with("((1"));
        assert_eq!(
            err(&sql),
            format!("Unsupported data type: {kind} under key root[{index}] at 1:{column}:{length}"),
            "{members}"
        );
    }
    // In a list in brackets they are not measured.
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE a = [\"pk\"]"),
        wf("Expected a literal value")
    );
}

#[test]
fn positions_after_a_not_form_are_unchanged() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE a NOT IN [1] AND foo(b)"),
        "Unrecognized function: foo at 1:42:3"
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE a NOT BETWEEN 1 AND 2 AND\nfoo(b) = 1"),
        "Unrecognized function: foo at 2:1:3"
    );
}

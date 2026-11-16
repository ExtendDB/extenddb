// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the statement forms and functions beyond the base productions.

use crate::partiql::ast::{Operand, SetMembersOp, Statement, WhereExpr};
use crate::partiql::parse;
use crate::types::AttributeValue;

fn err(sql: &str) -> String {
    parse(sql).unwrap_err().message
}

fn update(sql: &str) -> crate::partiql::ast::UpdateStatement {
    match parse(sql).unwrap() {
        Statement::Update(u) => u,
        other => panic!("expected an update, got {other:?}"),
    }
}

// `UPDATE "t" SET ` is 15 characters, so a value expression starts at 16.
const SET_AT: &str = "UPDATE \"t\" SET ";

#[test]
fn set_add_becomes_a_set_members_action() {
    let u = update("UPDATE \"t\" SET ss = set_add(ss, <<'a', 'b'>>) WHERE pk = 'p'");
    assert!(u.sets.is_empty());
    assert_eq!(u.set_members.len(), 1);
    let action = &u.set_members[0];
    assert_eq!(action.op, SetMembersOp::Add);
    assert_eq!(action.path.root, "ss");
    assert!(matches!(
        &action.members,
        Operand::Literal(AttributeValue::SS(s)) if s.len() == 2
    ));
    assert_eq!(action.first_argument_at, "1:29:2");
}

#[test]
fn set_functions_are_case_insensitive_and_take_a_parameter() {
    let u = update("UPDATE \"t\" SET ns = SET_DELETE(ns, ?) WHERE pk = ?");
    assert_eq!(u.set_members[0].op, SetMembersOp::Delete);
    assert!(matches!(u.set_members[0].members, Operand::Parameter(0)));
    assert_eq!(
        u.where_clause,
        Some(WhereExpr::Compare(
            Operand::Path(crate::partiql::ast::Path {
                root: "pk".to_owned(),
                steps: Vec::new()
            }),
            crate::partiql::ast::CompareOp::Eq,
            Operand::Parameter(1)
        ))
    );
}

#[test]
fn two_set_clauses_and_a_remove_parse_in_any_order() {
    let u = update(
        "UPDATE \"t\" SET a = 1 REMOVE b SET ss = set_add(ss, <<'x'>>) SET c = 2 WHERE pk = 'p'",
    );
    assert_eq!(u.sets.len(), 2);
    assert_eq!(u.removes.len(), 1);
    assert_eq!(u.set_members.len(), 1);
}

#[test]
fn a_remove_takes_one_path_and_a_set_a_list() {
    let leftover =
        "Statement wasn't well formed, can't be processed: Unexpected token after expression";
    for sql in [
        "UPDATE \"t\" REMOVE m.a, m WHERE pk = 'p'",
        "UPDATE \"t\" SET a = 1, b = 2 REMOVE c, d WHERE pk = 'p'",
        "UPDATE \"t\" REMOVE c, d SET a = 1 WHERE pk = 'p'",
        "UPDATE \"t\" REMOVE c, WHERE pk = 'p'",
        "UPDATE \"t\" REMOVE c, d",
        "FROM \"t\" WHERE pk = 'p' REMOVE c, d",
        // The comma is reported ahead of a refusal deferred to the end.
        "UPDATE \"t\" REMOVE \"\", d WHERE pk = 'p'",
    ] {
        assert_eq!(err(sql), leftover, "{sql}");
    }
    let u = update("UPDATE \"t\" SET a = 1, b = 2 REMOVE c REMOVE d WHERE pk = 'p'");
    assert_eq!(u.sets.len(), 2);
    assert_eq!(u.removes.len(), 2);
}

#[test]
fn set_add_assigned_to_another_attribute_names_the_function() {
    let sql = format!("{SET_AT}t = set_add(ss, <<'x'>>) WHERE pk = 'p'");
    assert_eq!(
        err(&sql),
        "The first argument to SET_ADD must equal the assignment value at 1:20:7"
    );
}

#[test]
fn set_add_whose_first_argument_is_not_a_path_names_the_function() {
    let sql = format!("{SET_AT}ss = set_add(<<'a'>>, <<'b'>>) WHERE pk = 'p'");
    assert_eq!(
        err(&sql),
        "The first argument to SET_ADD must equal the assignment value at 1:21:7"
    );
}

#[test]
fn set_add_of_a_value_that_is_not_a_set_names_the_first_argument() {
    for value in ["'x'", "['x']", "1", "{'a': 1}", "true", "null", "other"] {
        let sql = format!("{SET_AT}ss = set_add(ss, {value}) WHERE pk = 'p'");
        assert_eq!(
            err(&sql),
            "The second argument to SET_ADD must be a value with type SET at 1:29:2",
            "{value}"
        );
    }
    let sql = format!("{SET_AT}ss = set_delete(ss, 'a') WHERE pk = 'p'");
    assert_eq!(
        err(&sql),
        "The second argument to SET_DELETE must be a value with type SET at 1:32:2"
    );
}

#[test]
fn set_add_needs_exactly_two_arguments() {
    for args in ["ss", "ss, <<'x'>>, <<'y'>>", ""] {
        let sql = format!("{SET_AT}ss = set_add({args}) WHERE pk = 'p'");
        assert_eq!(
            err(&sql),
            "SET_ADD must have exactly two arguments at 1:21:7",
            "{args}"
        );
    }
}

#[test]
fn set_delete_assigned_to_another_attribute_names_the_function() {
    let sql = format!("{SET_AT}t = set_delete(ss, <<'a'>>) WHERE pk = 'p'");
    assert_eq!(
        err(&sql),
        "The first argument to SET_DELETE must equal the assignment value at 1:20:10"
    );
}

#[test]
fn set_add_of_an_empty_set_literal() {
    let sql = format!("{SET_AT}ss = set_add(ss, <<>>) WHERE pk = 'p'");
    assert_eq!(err(&sql), "Empty bags are not supported");
}

#[test]
fn set_add_of_a_set_with_a_repeated_member_reports_the_duplicate() {
    let sql = format!("{SET_AT}ss = set_add(ss, <<'x', 'x'>>) WHERE pk = 'p'");
    assert_eq!(
        err(&sql),
        "One or more parameter values were invalid: Input collection [x, x] contains duplicates."
    );
}

#[test]
fn set_add_in_a_where_is_an_unrecognized_function() {
    let sql = "SELECT * FROM \"t\" WHERE pk = 'p' AND set_add(ss, <<'x'>>) = ss";
    assert_eq!(err(sql), "Unrecognized function: set_add at 1:38:7");
}

#[test]
fn exists_in_a_where_is_an_unrecognized_function() {
    let sql = "SELECT * FROM \"t\" WHERE pk = 'p' AND EXISTS(ss)";
    assert_eq!(err(sql), "Unrecognized function: exists at 1:38:6");
    let sql = "SELECT * FROM \"t\" WHERE pk = 'p' AND a = foo(b)";
    assert_eq!(err(sql), "Unrecognized function: foo at 1:42:3");
}

#[test]
fn condition_functions_stay_recognized_in_a_where() {
    assert!(parse("SELECT * FROM \"t\" WHERE pk = 'p' AND size(s) = 3").is_ok());
    assert!(parse("SELECT * FROM \"t\" WHERE pk = 'p' AND 3 = size(s)").is_ok());
    assert!(parse("UPDATE \"t\" SET l = list_append(l, [1]) WHERE pk = 'p'").is_ok());
}

#[test]
fn set_add_as_an_insert_value_is_an_unsupported_data_type() {
    let sql = "INSERT INTO \"t\" VALUE {'pk': 'p', 'ss': set_add(<<'a'>>, <<'b'>>)}";
    assert_eq!(
        err(sql),
        "Unsupported data type: set_add under key root at 1:41:7"
    );
}

#[test]
fn attribute_exists_needs_a_document_path() {
    for operand in ["'ss'", "?", "1"] {
        let sql = format!("SELECT * FROM \"t\" WHERE pk = 'p' AND attribute_exists({operand})");
        assert_eq!(
            err(&sql),
            "Operator or function requires a document path; operator or function: attribute_exists",
            "{operand}"
        );
    }
    assert_eq!(
        err("UPDATE \"t\" SET a = 1 WHERE pk = 'p' AND attribute_not_exists('a')"),
        "Operator or function requires a document path; operator or function: attribute_not_exists"
    );
}

#[test]
fn attribute_exists_needs_exactly_one_operand() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'p' AND attribute_exists()"),
        "Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 0"
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'p' AND attribute_exists(ss, ns)"),
        "Incorrect number of operands for operator or function; operator or function: attribute_exists, number of operands: 2"
    );
}

#[test]
fn a_condition_function_compared_with_a_boolean() {
    let condition = |sql: &str| match parse(sql).unwrap() {
        Statement::Select(s) => match s.where_clause.unwrap() {
            WhereExpr::And(_, right) => *right,
            other => panic!("expected AND, got {other:?}"),
        },
        other => panic!("expected a select, got {other:?}"),
    };
    let base = "SELECT * FROM \"t\" WHERE pk = 'p' AND ";
    // As a condition, `= true` and `<> false` keep the function's test and
    // `= false` and `<> true` negate it.
    for (tail, negated) in [
        ("attribute_exists(ss) = true", false),
        ("attribute_exists(ss) <> FALSE", false),
        ("attribute_exists(ss) = false", true),
        ("begins_with(s, 'a') <> true", true),
    ] {
        let condition = condition(&format!("{base}{tail}"));
        assert!(
            matches!(condition, WhereExpr::ComparedWithBoolean(..)),
            "{tail}"
        );
        let (negates, inner) = condition.without_nots();
        assert_eq!(negates, negated, "{tail}");
        assert!(
            matches!(inner, WhereExpr::Function(..) | WhereExpr::BeginsWith(..)),
            "{tail}"
        );
    }
}

#[test]
fn a_comparison_compared_with_booleans_and_with_a_number() {
    use crate::partiql::ast::{CompareOp, Path};
    let condition = |sql: &str| match parse(sql).unwrap() {
        Statement::Select(s) => s.where_clause.unwrap(),
        other => panic!("expected a select, got {other:?}"),
    };
    let compare = WhereExpr::Compare(
        Operand::Path(Path {
            root: "a".to_owned(),
            steps: Vec::new(),
        }),
        CompareOp::Eq,
        Operand::Literal(AttributeValue::N("1".to_owned())),
    );
    let base = "SELECT * FROM \"t\" WHERE ";
    let negation = |sql: &str| {
        let condition = condition(sql);
        let (negated, inner) = condition.without_nots();
        (negated, inner.clone())
    };
    assert_eq!(
        negation(&format!("{base}a = 1 = true = true")),
        (false, compare.clone())
    );
    assert_eq!(
        negation(&format!("{base}a = 1 = true <> true")),
        (true, compare.clone())
    );
    // With a number it holds for no item.
    assert_eq!(
        condition(&format!("{base}a = 1 = 1")),
        WhereExpr::And(
            Box::new(compare.clone()),
            Box::new(WhereExpr::Not(Box::new(compare.clone())))
        )
    );
    // A comparison with true that keeps the condition still takes a number.
    match condition(&format!("{base}a = 1 = true = 1")) {
        WhereExpr::And(kept, never) => {
            assert_eq!(kept.without_nots(), (false, &compare));
            assert_eq!(never.without_nots(), (true, &compare));
        }
        other => panic!("expected AND, got {other:?}"),
    }
    assert_eq!(
        err(&format!("{base}a = 1 = 1e126")),
        "Number overflow. Attempting to store a number with magnitude larger than \
         supported range under root"
    );
    // `<>` with a number, and a number after any other predicate, are not
    // measured.
    let leftover =
        "Statement wasn't well formed, can't be processed: Unexpected token after expression";
    for tail in ["a = 1 <> 1", "a IN [1] = 1"] {
        assert_eq!(err(&format!("{base}{tail}")), leftover, "{tail}");
    }
}

#[test]
fn from_first_update_parses_where_then_clauses() {
    let u = update("from \"t\" where pk = 'p' AND a = 1 set c = 2 remove b returning all new *");
    assert_eq!(u.table, "t");
    assert!(u.where_clause.is_some());
    assert_eq!(u.sets.len(), 1);
    assert_eq!(u.removes.len(), 1);
    assert_eq!(u.returning, Some(crate::partiql::ast::Returning::AllNew));
}

#[test]
fn from_first_update_without_where_parses_for_execution_to_refuse() {
    assert!(update("FROM \"t\" SET c = 1").where_clause.is_none());
    assert!(update("FROM \"t\" REMOVE a").where_clause.is_none());
}

#[test]
fn from_first_update_with_where_after_set_is_a_leftover_token() {
    let leftover =
        "Statement wasn't well formed, can't be processed: Unexpected token after expression";
    assert_eq!(err("FROM \"t\" SET c = 1 WHERE pk = 'p'"), leftover);
    assert_eq!(
        err("FROM \"t\" SET c = 1 WHERE pk = 'p' SET d = 2"),
        leftover
    );
}

#[test]
fn from_first_update_paths_and_aliases_get_the_update_refusals() {
    for (sql, expected) in [
        (
            "FROM \"t\".\"idx\" WHERE pk = 'p' SET c = 1",
            "This operation is not supported on an index",
        ),
        (
            "FROM \"t\".\"idx\" WHERE pk = 'p' REMOVE c",
            "This operation is not supported on an index",
        ),
        (
            "FROM \"t\".\"i\" WHERE pk = 'p' SET c = 1",
            crate::partiql::from_path::NAME_RULE,
        ),
        (
            "FROM \"t\".\"a\".\"b\" WHERE pk = 'p' SET c = 1",
            "A path may contain at most 2 components in the FROM clause",
        ),
        (
            "FROM \"t\".\"\" WHERE pk = 'p' SET c = 1",
            "Path component cannot be an empty string",
        ),
        (
            "FROM \"t\" t WHERE pk = 'p' SET c = 1",
            "Aliasing is not supported",
        ),
        (
            "FROM \"t\" AS t WHERE pk = 'p' SET c = 1",
            "Aliasing is not supported",
        ),
    ] {
        assert_eq!(err(sql), expected, "{sql}");
    }
}

#[test]
fn from_first_update_without_a_clause_is_not_data_manipulation() {
    assert_eq!(
        err("FROM \"t\" WHERE pk = 'p'"),
        "Statement wasn't well formed, can't be processed: Expected data manipulation"
    );
}

#[test]
fn exists_statement_parses_its_select() {
    match parse("EXISTS(SELECT * FROM \"t\" WHERE pk = 'p' AND a = 1)").unwrap() {
        Statement::Exists(select) => {
            assert_eq!(select.from.table, "t");
            assert!(select.where_clause.is_some());
        }
        other => panic!("expected EXISTS, got {other:?}"),
    }
    assert!(matches!(
        parse("exists (select * from \"t\" where pk = 'p' and a = 1)").unwrap(),
        Statement::Exists(_)
    ));
}

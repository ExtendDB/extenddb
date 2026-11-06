// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::partiql::parse;

const NOT_ALLOWED: &str = "The function is not allowed in an update expression; function: ";

fn written(sql: &str) -> Result<(), String> {
    check_written_values(&parse(sql).expect("the statement parses"), sql)
}

fn condition_values(sql: &str) -> Result<(), String> {
    check_condition_values(&parse(sql).expect("the statement parses"))
}

/// An UPDATE that sets `a` to `value`, which starts at column 22.
fn update(value: &str) -> String {
    format!("UPDATE \"tbl\" SET a = {value} WHERE pk = 'h' AND sk = 's'")
}

#[test]
fn a_condition_function_in_a_set_value_is_refused_by_name() {
    for (call, name) in [
        ("attribute_exists(m)", "attribute_exists"),
        ("attribute_not_exists(m)", "attribute_not_exists"),
        ("attribute_type(m, 'M')", "attribute_type"),
        ("begins_with(m, 'x')", "begins_with"),
        ("contains(m, 'x')", "contains"),
        ("size(m)", "size"),
        ("SIZE(m)", "size"),
        ("size(m, 1)", "size"),
        ("size()", "size"),
        ("attribute_exists('x')", "attribute_exists"),
        ("size(m) + 1", "size"),
        ("1 + size(m)", "size"),
        ("if_not_exists(a, size(m))", "size"),
        ("(size(m))", "size"),
    ] {
        assert_eq!(
            written(&update(call)),
            Err(format!("{NOT_ALLOWED}{name}")),
            "{call}"
        );
    }
}

#[test]
fn any_other_call_is_unrecognized_at_its_name() {
    for (call, message) in [
        ("upper(m)", "Unrecognized function: upper at 1:22:5"),
        ("UPPER(m)", "Unrecognized function: upper at 1:22:5"),
        ("lower(m)", "Unrecognized function: lower at 1:22:5"),
        ("exists(m)", "Unrecognized function: exists at 1:22:6"),
        (
            "char_length(m)",
            "Unrecognized function: char_length at 1:22:11",
        ),
        (
            "substring(m, 1)",
            "Unrecognized function: substring at 1:22:9",
        ),
        ("trim(m)", "Unrecognized function: trim at 1:22:4"),
        ("foo(m)", "Unrecognized function: foo at 1:22:3"),
        (
            "if_not_exists(a, set_add(ss, <<'b'>>))",
            "Unrecognized function: set_add at 1:39:7",
        ),
        ("(upper(m))", "Unrecognized function: upper at 1:23:5"),
        ("upper()", "Unrecognized function: upper at 1:22:5"),
    ] {
        assert_eq!(written(&update(call)), Err(message.to_owned()), "{call}");
    }
}

#[test]
fn the_keyword_calls_are_unsupported_tokens() {
    for (call, message) in [
        (
            "count(m)",
            "Unsupported token in expression: count at 1:22:5",
        ),
        (
            "COUNT(m)",
            "Unsupported token in expression: count at 1:22:5",
        ),
        (
            "count(m, 1)",
            "Unsupported token in expression: count at 1:22:5",
        ),
        ("sum(m)", "Unsupported token in expression: sum at 1:22:3"),
        ("avg(m)", "Unsupported token in expression: avg at 1:22:3"),
        ("min(m)", "Unsupported token in expression: min at 1:22:3"),
        ("max(m)", "Unsupported token in expression: max at 1:22:3"),
        (
            "nullif(1, m)",
            "Unsupported token in expression: UNKNOWN at 1:22:6",
        ),
        (
            "coalesce(m, 1)",
            "Unsupported token in expression: UNKNOWN at 1:30:1",
        ),
        (
            "coalesce(1, m)",
            "Unsupported token in expression: UNKNOWN at 1:30:1",
        ),
    ] {
        assert_eq!(written(&update(call)), Err(message.to_owned()), "{call}");
    }
}

#[test]
fn the_update_functions_take_two_operands_and_a_first_path() {
    for value in [
        "if_not_exists(a, 7)",
        "IF_NOT_EXISTS(a, 7)",
        "if_not_exists(m.k, 7)",
        "if_not_exists(l[0], 7)",
        "list_append(l, [3])",
        "list_append([1], l)",
        "list_append([1], [2])",
        "if_not_exists(a, if_not_exists(b, 1))",
        "list_append(if_not_exists(l, []), [3])",
        "if_not_exists(q, list_append(l, [3]))",
        "if_not_exists(a, 0) + if_not_exists(b, 0)",
        "if_not_exists(a, a + 1)",
        "(a + 1) + 1",
    ] {
        assert_eq!(written(&update(value)), Ok(()), "{value}");
    }
    let operands = |name: &str, count: usize| {
        format!(
            "Incorrect number of operands for operator or function; operator or function: \
             {name}, number of operands: {count}"
        )
    };
    let path = "Operator or function requires a document path; operator or function: \
                if_not_exists"
        .to_owned();
    for (value, message) in [
        ("if_not_exists(m)", operands("if_not_exists", 1)),
        ("if_not_exists(1)", operands("if_not_exists", 1)),
        ("if_not_exists(a, 1, 2)", operands("if_not_exists", 3)),
        ("if_not_exists(1, 2, 3)", operands("if_not_exists", 3)),
        ("if_not_exists()", operands("if_not_exists", 0)),
        ("list_append(l)", operands("list_append", 1)),
        ("list_append(l, [1], [2])", operands("list_append", 3)),
        ("list_append()", operands("list_append", 0)),
        ("if_not_exists(1, a)", path.clone()),
        ("if_not_exists(1, 2)", path),
    ] {
        assert_eq!(written(&update(value)), Err(message), "{value}");
    }
}

#[test]
fn a_call_is_met_after_its_arguments() {
    assert_eq!(
        written(&update("size(upper(m))")),
        Err("Unrecognized function: upper at 1:27:5".to_owned())
    );
    assert_eq!(
        written(&update("upper(size(m))")),
        Err(format!("{NOT_ALLOWED}size"))
    );
    assert_eq!(
        written(&update("if_not_exists(upper(m))")),
        Err("Unrecognized function: upper at 1:36:5".to_owned())
    );
    assert_eq!(
        written(&update("if_not_exists(size(m))")),
        Err(format!("{NOT_ALLOWED}size"))
    );
    assert_eq!(
        written(&update("if_not_exists(a, upper(m))")),
        Err("Unrecognized function: upper at 1:39:5".to_owned())
    );
    for value in ["size(1e126)", "if_not_exists(a, 1e126)"] {
        assert_eq!(
            written(&update(value)),
            Err(
                "Number overflow. Attempting to store a number with magnitude larger than \
                 supported range under root"
                    .to_owned()
            ),
            "{value}"
        );
    }
}

#[test]
fn the_values_are_met_in_the_order_written() {
    for (value, first) in [
        (
            "upper(m), b = size(m)",
            "Unrecognized function: upper at 1:22:5",
        ),
        ("size(m), b = upper(m)", NOT_ALLOWED),
        (
            "upper(m), b = lower(m)",
            "Unrecognized function: upper at 1:22:5",
        ),
        ("size(m), b = 1e126", NOT_ALLOWED),
        ("upper(m), b = 1e126", "Unrecognized function"),
        ("1e126, b = size(m)", "Number overflow"),
    ] {
        let refusal = written(&update(value)).unwrap_err();
        assert!(refusal.starts_with(first), "{value}: {refusal}");
    }
}

#[test]
fn a_set_add_assignment_keeps_its_place() {
    let nested_after_whole = "UPDATE \"tbl\" SET ss = set_add(ss, <<'b'>>), a = \
                              if_not_exists(a, set_add(ss, <<'c'>>)) WHERE pk = 'h'";
    assert_eq!(
        written(nested_after_whole),
        Err("Unrecognized function: set_add at 1:66:7".to_owned())
    );
}

#[test]
fn a_call_is_named_where_it_is_written() {
    for (sql, message) in [
        (
            "UPDATE \"tbl\"\n  SET a = upper(m)\n  WHERE pk = 'h' AND sk = 's'",
            "Unrecognized function: upper at 2:11:5",
        ),
        (
            "FROM \"tbl\" WHERE pk = 'h' AND sk = 's' SET a = upper(m)",
            "Unrecognized function: upper at 1:48:5",
        ),
        (
            "UPDATE \"tbl\" SET x = 1 SET a = upper(m) WHERE pk = 'h'",
            "Unrecognized function: upper at 1:32:5",
        ),
        (
            "UPDATE \"tbl\" SET a = upper(m) REMOVE x WHERE pk = 'h'",
            "Unrecognized function: upper at 1:22:5",
        ),
        (
            "UPDATE \"tbl\" SET pk = 'z', a = upper(m) WHERE pk = 'h'",
            "Unrecognized function: upper at 1:32:5",
        ),
        (
            "UPDATE \"tbl\" SET a = upper(upper(m)) WHERE pk = 'h'",
            "Unrecognized function: upper at 1:28:5",
        ),
    ] {
        assert_eq!(written(sql), Err(message.to_owned()), "{sql}");
    }
}

#[test]
fn the_condition_and_the_written_values_are_checked_apart() {
    // A number in a condition that breaks a rule is refused by the parser.
    let sql = "UPDATE \"tbl\" SET a = size(m) WHERE pk = 'h' AND w = 1";
    assert_eq!(condition_values(sql), Ok(()));
    assert_eq!(written(sql), Err(format!("{NOT_ALLOWED}size")));
    let sql = "UPDATE \"tbl\" SET v = 1e126 WHERE pk = 'h' AND w = 1";
    assert_eq!(condition_values(sql), Ok(()));
    assert!(written(sql).unwrap_err().starts_with("Number overflow"));
}

#[test]
fn the_set_tokens_list_the_assignments_and_the_calls() {
    let tokens = SetTokens::read(
        "UPDATE \"t\" SET a = upper(m), ss = set_add(ss, <<'b'>>) REMOVE r SET b = \
         coalesce(m, 1) WHERE pk = size(x)",
    );
    assert_eq!(tokens.members, vec![false, true, false]);
    let calls: Vec<(&str, &str, &str)> = tokens
        .calls
        .iter()
        .map(|c| (c.name.as_str(), c.at.as_str(), c.paren_at.as_str()))
        .collect();
    assert_eq!(
        calls,
        vec![
            ("upper", "1:20:5", "1:25:1"),
            ("coalesce", "1:73:8", "1:81:1")
        ]
    );
}

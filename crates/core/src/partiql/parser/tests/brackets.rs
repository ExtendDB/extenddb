// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for a path in the brackets of a path: where it is refused,
//! what its own brackets may hold, and how a SET or REMOVE target and an
//! ORDER BY term answer it.

use super::*;
use crate::partiql::number::OVERFLOW_MESSAGE;
use crate::partiql::parser::limits::MAX_STATEMENT_LENGTH;

/// The start of a condition on `m`, which puts what `m[` holds at column 40.
const AND: &str = "SELECT * FROM \"t\" WHERE pk = 'p' AND ";

fn condition(c: &str) -> String {
    err(&format!("{AND}{c}"))
}

/// The refusal of a path in brackets whose first component starts at
/// `column` and is `length` characters long.
fn refused_at(column: usize, length: usize) -> String {
    format!("Unexpected path component at 1:{column}:{length}")
}

/// `m[`, `levels` times `step`, `inner`, as many `]`, then `] = 1`.
fn nested(step: &str, inner: &str, levels: usize) -> String {
    format!(
        "m[{}{inner}{}] = 1",
        step.repeat(levels),
        "]".repeat(levels)
    )
}

/// `head`, as many `open` as fit in the length limit, `inner`, as many
/// `close`, then `tail`.
fn fill(head: &str, open: &str, inner: &str, close: &str, tail: &str) -> String {
    let times =
        (MAX_STATEMENT_LENGTH - head.len() - inner.len() - tail.len()) / (open.len() + close.len());
    format!(
        "{head}{}{inner}{}{tail}",
        open.repeat(times),
        close.repeat(times)
    )
}

#[test]
fn a_path_in_brackets_is_refused_at_its_first_component_however_deep() {
    for (step, inner, length) in [
        ("k[", "k", 1),
        ("'k'[", "'k'", 3),
        ("\"k\"[", "\"k\"", 3),
        ("k.j[", "k", 1),
    ] {
        for levels in [1, 2, 10, 1000, 2700] {
            assert_eq!(
                condition(&nested(step, inner, levels)),
                refused_at(40, length),
                "{step} {levels}"
            );
        }
        let longest = fill(&format!("{AND}m["), step, inner, "]", "] = 1");
        assert!(longest.len() <= MAX_STATEMENT_LENGTH);
        assert_eq!(err(&longest), refused_at(40, length), "{step} at the limit");
    }
    let dots = fill(&format!("{AND}m[k"), ".j", "", "", "] = 1");
    assert_eq!(err(&dots), refused_at(40, 1));
    // The first bracket that holds a path is named.
    for (path, first) in [
        ("m[k][k[0]] = 1", "k[0]"),
        ("m[k[0]][j[0]] = 1", "k[0]"),
        ("m[j].x[k[0]] = 1", "k[0]"),
        ("m[''[k]] = 1", "''["),
        ("m[''.k] = 1", "''."),
    ] {
        let sql = format!("{AND}{path}");
        let column = sql.find(first).unwrap() + 1;
        let length = if first.starts_with("''") { 2 } else { 1 };
        assert_eq!(err(&sql), refused_at(column, length), "{path}");
    }
}

#[test]
fn what_follows_the_first_component_of_a_path_in_brackets_is_not_refused() {
    for path in [
        "m[k['']]",
        "m[k[-1]]",
        "m[k[1.5]]",
        "m[k[true]]",
        "m[k[MISSING]]",
        "m[k[null]]",
        "m[k['k'.x]]",
        "m[k[\"j\".x]]",
        "m[k[(1)]]",
        "m[k[(j)]]",
        "m[k[(j[0])]]",
        "m[k.*]",
        "m[k.\"j\"]",
        "m[k[*]]",
        "m[k[0]].x",
        "m[k[0]][1]",
        "m[k[0].j]",
        "m[k[`1`]]",
        "m[k[?]]",
        "m[k[f(x)]]",
        "m[k[size(x)]]",
        "m[k[EXISTS(a)]]",
        "m[k[[1]]]",
        "m[k[<<1>>]]",
        "m[k[{'a': 1}]]",
        "m[k[a.b.c]]",
        "m[k[a[b[c]]]]",
        "m[k[NOT a]]",
        "m[k[a = 1]]",
        "m[k[a AND b]]",
        "m[k[a BETWEEN 1 AND 2]]",
        "m[k[a IN [1]]]",
        "m[k[a IS NULL]]",
        "m[k[a LIKE 'x']]",
        "m[k[- a]]",
        "m[k[+1]]",
        "m[k[-1.5]]",
        "m[k[1e3]]",
        "m[k[1.2.e3]]",
        "m[k[1 + 1]]",
        "m[k[2 * 3]]",
        "m[k['a' || 'b']]",
        "m[k[j.x = 1]]",
        "m[k[j[0] + 1]]",
        "m[k[j.x AND j.y IS NULL]]",
        "m[k[j.x NOT IN [1]]]",
        "m[k[f(j[x + 1])]]",
        "m[k[i[h = 1] = 1]]",
        "m[k[j.x IS MISSING]]",
        "m[k[j.x BETWEEN 1 AND 2]]",
        "m[k[j.x LIKE 'a']]",
        "m[k[j.x OR j.y]]",
        "m[k[j.x[0] = 1]]",
        "m[k[j.x = 1 AND j.y = 2 OR j.z = 3]]",
        "m[k[j.x * j.y]]",
        "m[k[j.x || 'a']]",
        "m[k[j.x - 1 = 2]]",
        "m[k[j.x = 1 = true]]",
        "m[k[j.x IS NOT NULL]]",
        "m[k[j.x IN (1, 2)]]",
        "m[k[j.x IN <<1>>]]",
        "m[k[j.x NOT LIKE 'a']]",
        "m[k[j.x NOT BETWEEN 1 AND 2]]",
        "m[k[j.x + j[0]]]",
        "m[k[j.x AND NOT j.y]]",
        "m[k[j.x = ?]]",
        "m[k[a NOT IN [1]]]",
        "m[k[a IN 1]]",
        "m[k[a IN x]]",
        "m[k[{a: 1}]]",
    ] {
        assert_eq!(
            condition(&format!("{path} = 1")),
            refused_at(40, 1),
            "{path}"
        );
    }
}

#[test]
fn a_syntax_error_in_an_inner_bracket_comes_first() {
    for (path, reason) in [
        ("m[k[select]] = 1", "Unexpected term"),
        ("m[k[and]] = 1", "Unexpected operator"),
        ("m[k[]] = 1", "Unexpected term"),
        ("m[k[(]] = 1", "Unexpected term"),
        ("m[k[)]] = 1", "Unexpected term"),
        ("m[k[,]] = 1", "Unexpected term"),
        ("m[k[;]] = 1", "Unexpected term"),
        ("m[k[CASE]] = 1", "Unexpected term"),
        ("m[k[FROM]] = 1", "Unexpected term"),
        ("m[k[j k]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k['a' 'b']] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[1 2]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[1, 2]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k.1] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[k[k] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j.x = 1", "Expected RIGHT_BRACKET"),
        ("m[k[f(x) = 1", "Expected RIGHT_BRACKET"),
        ("m[k.] = 1", "Invalid path dot component"),
        ("m[k.select] = 1", "Invalid path dot component"),
        ("m[k[j]]] = 1", "Unexpected token after expression"),
        // What may follow an operand, a path with steps included.
        ("m[k[j.x NOT = 1]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j.x NOT NOT IN [1]]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j.x NOT]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j.x NOT a]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j[0] NOT = 1]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[a NOT = 1]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[a NOT a]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[a NOT IS NULL]] = 1", "Expected RIGHT_BRACKET"),
        ("(m[k[a NOT = 1]] = 1)", "Expected RIGHT_BRACKET"),
        ("m[k[j.x (y)]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j.x size(y)]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j.x begins_with(a, b)]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j.x y]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[j.x 'y']] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[a,]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[EXEC(x)]] = 1", "Expected RIGHT_BRACKET"),
        ("m[k[(a NOT = 1)]] = 1", "Expected RIGHT_PAREN"),
        ("m[k[(a]] = 1", "Expected RIGHT_PAREN"),
        ("m[k[f(a]] = 1", "Expected RIGHT_PAREN"),
        ("m[k[j.x = ]] = 1", "Unexpected term"),
        ("m[k[j.x AND]] = 1", "Unexpected term"),
        ("m[k[j.x IN]] = 1", "Unexpected term"),
        ("m[k[j.x +]] = 1", "Unexpected term"),
        ("m[k[a IN ]] = 1", "Unexpected term"),
        ("m[k[a IN ,]] = 1", "Unexpected term"),
        ("m[k[(a IN )]] = 1", "Unexpected term"),
        ("m[k[a = 1 AND]] = 1", "Unexpected term"),
        ("m[k[a LIKE]] = 1", "Unexpected term"),
        ("m[k[a LIKE 'x' ESCAPE]] = 1", "Unexpected term"),
        ("m[k[NOT]] = 1", "Unexpected term"),
        ("m[k[EXEC x]] = 1", "Unexpected term"),
        ("m[k[EXEC]] = 1", "Unexpected term"),
        ("m[k[a = EXEC x]] = 1", "Unexpected term"),
        ("m[k[a = CASE]] = 1", "Unexpected term"),
        ("m[k[{]] = 1", "Unexpected term"),
        ("m[k[{)]] = 1", "Unexpected term"),
        ("m[k[j.x IS]] = 1", "Expected type name"),
        ("m[k[a IS NOT]] = 1", "Expected type name"),
        ("m[k[a BETWEEN 1]] = 1", "Expected AND keyword"),
        (
            "m[k[VALUES]] = 1",
            "Expected LEFT_PAREN for row value constructor",
        ),
    ] {
        assert_eq!(condition(path), wf(reason), "{path}");
    }
    // A bracket left open at the length limit.
    let open = fill(&format!("{AND}m["), "k[", "k", "]", " = 1");
    assert_eq!(err(&open), wf("Expected RIGHT_BRACKET"));
}

#[test]
fn a_path_in_brackets_keeps_its_place_among_the_refusals() {
    let path_refused = refused_at(40, 1);
    for (c, want) in [
        (
            "a = 1e126 AND m[k[0]] = 1",
            format!("{OVERFLOW_MESSAGE} under root"),
        ),
        ("m[k[0]] = 1 AND a = 1e126", path_refused.clone()),
        ("m[k[k[0]]] = 1e126", path_refused.clone()),
        ("m[k[0]] = ?", path_refused.clone()),
        ("m[k[0]] = 1 AND a = 1 + 1", path_refused.clone()),
        (
            "a = 1 + 1 AND m[k[0]] = 1",
            "Unsupported operator in Condition Expression. Operator: +".to_owned(),
        ),
        ("m[k[0]] = 1 AND m[''] = 1", path_refused.clone()),
        (
            "m[''] = 1 AND m[k[0]] = 1",
            "Path component cannot be an empty string".to_owned(),
        ),
    ] {
        assert_eq!(condition(c), want, "{c}");
    }
}

#[test]
fn a_path_in_brackets_is_refused_wherever_a_path_is_read() {
    for (sql, first) in [
        ("UPDATE \"t\" SET x = m[k[0]] WHERE pk = 'p'", "k[0]"),
        (
            "UPDATE \"t\" SET x = list_append(m[k.j], [1]) WHERE pk = 'p'",
            "k.j",
        ),
        ("SELECT m[k[0]] FROM \"t\" WHERE pk = 'p'", "k[0]"),
        (
            "SELECT * FROM \"t\" WHERE pk = 'p' AND size(m[k[0]]) = 1",
            "k[0]",
        ),
        (
            "SELECT * FROM \"t\" WHERE pk = 'p' AND attribute_exists(m[k[0]])",
            "k[0]",
        ),
        (
            "SELECT * FROM \"t\" WHERE pk = 'p' AND (m)[k[0]] = 1",
            "k[0]",
        ),
        (
            "SELECT * FROM \"t\" WHERE pk = 'p' AND m[k[0]] IS MISSING",
            "k[0]",
        ),
        (
            "SELECT * FROM \"t\" WHERE pk = 'p' AND m[k[0]] IN [1]",
            "k[0]",
        ),
        (
            "SELECT * FROM \"t\" WHERE pk = 'p' AND a BETWEEN m[k[0]] AND 2",
            "k[0]",
        ),
        ("SELECT * FROM \"t\" WHERE pk[k[0]] = 'p'", "k[0]"),
        ("DELETE FROM \"t\" WHERE pk = 'p' AND m[k[0]] = 1", "k[0]"),
    ] {
        let column = sql.find(first).unwrap() + 1;
        assert_eq!(err(sql), refused_at(column, 1), "{sql}");
    }
    // In a list in parentheses the path is refused as a path first.
    let sql = "SELECT * FROM \"t\" WHERE pk = 'p' AND a IN ((m[k[0]], 1))";
    let column = sql.find("m[k").unwrap() + 1;
    assert_eq!(
        err(sql),
        format!("Unsupported data type: Path under key root[0] at 1:{column}:1")
    );
}

#[test]
fn a_target_whose_brackets_hold_a_path_or_a_parameter_is_a_syntax_error() {
    let not_simple = wf("Invalid path component for simple path");
    for path in [
        "m[k.j]",
        "m['k'.x]",
        "m[k[0]]",
        "m[?]",
        "m[k][k.j]",
        "m.x[k.j]",
        "m[[1]]",
        "m[{'a': 1}]",
        "m[<<1>>]",
        "m[*]",
        "m[- k]",
        "m[NOT a]",
        "m[f(x)]",
        "m[a = 1]",
        "m[k + 1]",
        "m[k.j + 1]",
        "m['k' + 1]",
        "m[1 + 1]",
        "m[true + 1]",
        "m[? + 1]",
    ] {
        for clause in [format!("SET {path} = 1"), format!("REMOVE {path}")] {
            let sql = format!("UPDATE \"t\" {clause} WHERE pk = 'p'");
            assert_eq!(err(&sql), not_simple, "{sql}");
        }
    }
    for sql in [
        // Ahead of what follows the target, and of an earlier refusal.
        "UPDATE \"t\" SET m[k.j] = WHERE pk = 'p'".to_owned(),
        "UPDATE \"t\" SET m[k.j] 1 WHERE pk = 'p'".to_owned(),
        "UPDATE \"t\" SET m[k.j] = 1 WHERE pk = 'p' AND a =".to_owned(),
        "UPDATE \"t\" SET m[k.j] = 1, n[0 = 2 WHERE pk = 'p'".to_owned(),
        "UPDATE \"t\" REMOVE m[k.j] x WHERE pk = 'p'".to_owned(),
        "UPDATE \"t\" SET x = 1e126, m[k.j] = 1 WHERE pk = 'p'".to_owned(),
        "UPDATE \"t\" AS z SET m[k.j] = 1 WHERE pk = 'p'".to_owned(),
        "FROM \"t\" WHERE pk = 'p' SET m[k[0]] = 1".to_owned(),
        fill("UPDATE \"t\" REMOVE m[", "k[", "k", "]", "] WHERE pk = 'p'"),
        fill(
            "UPDATE \"t\" SET m[",
            "k[",
            "k",
            "]",
            "] = 1 WHERE pk = 'p'",
        ),
    ] {
        assert_eq!(err(&sql), not_simple, "{sql}");
    }
    // Other contents keep their refusals, and a bracket left open its
    // syntax error.
    let at = |sql: &str, token: &str| sql.find(token).unwrap() + 1;
    let sql = "UPDATE \"t\" SET m[1.5] = 1 WHERE pk = 'p'";
    assert_eq!(err(sql), refused_at(at(sql, "1.5"), 3));
    let sql = "UPDATE \"t\" REMOVE m[true] WHERE pk = 'p'";
    assert_eq!(err(sql), refused_at(at(sql, "true"), 4));
    assert_eq!(
        err("UPDATE \"t\" SET m[''] = 1 WHERE pk = 'p'"),
        "Path component cannot be an empty string"
    );
    for (sql, reason) in [
        (
            "UPDATE \"t\" SET m[k.j = 1 WHERE pk = 'p'",
            "Expected RIGHT_BRACKET",
        ),
        (
            "UPDATE \"t\" SET m[k WHERE pk = 'p'",
            "Expected RIGHT_BRACKET",
        ),
        ("UPDATE \"t\" SET m[] = 1 WHERE pk = 'p'", "Unexpected term"),
        (
            "UPDATE \"t\" SET m[select] = 1 WHERE pk = 'p'",
            "Unexpected term",
        ),
        (
            "UPDATE \"t\" SET m[k.j NOT = 1] = 1 WHERE pk = 'p'",
            "Expected RIGHT_BRACKET",
        ),
        (
            "UPDATE \"t\" SET m[a NOT = 1] = 1 WHERE pk = 'p'",
            "Expected RIGHT_BRACKET",
        ),
        (
            "UPDATE \"t\" SET m[k.j AND] = 1 WHERE pk = 'p'",
            "Unexpected term",
        ),
        (
            "UPDATE \"t\" SET m[k.j = ] = 1 WHERE pk = 'p'",
            "Unexpected term",
        ),
        (
            "UPDATE \"t\" SET m[a IN ] = 1 WHERE pk = 'p'",
            "Unexpected term",
        ),
    ] {
        assert_eq!(err(sql), wf(reason), "{sql}");
    }
    let Ok(Statement::Update(update)) =
        parse("UPDATE \"t\" SET m[k] = 1 REMOVE l[0] WHERE pk = 'p'")
    else {
        panic!("not an update");
    };
    assert_eq!(update.sets.len(), 1);
    assert_eq!(update.removes.len(), 1);
}

#[test]
fn nothing_in_the_brackets_of_an_order_by_term_is_refused() {
    let head = "SELECT * FROM \"t\" WHERE pk = 'p' ORDER BY ";
    for path in [
        "m[?]",
        "m[1.5]",
        "m['']",
        "m[-1]",
        "m[true]",
        "m[MISSING]",
        "m[k.j]",
        "m[k[0]]",
        "m['k'.x]",
        "m[(1)]",
        "m[*]",
        "m[f(x)]",
        "m[k + 1]",
        "m[k.j + 1]",
        "m[[1]]",
        "m[<<1>>]",
        "m[`1`]",
        "m[{'a': 1}]",
        "m[0]",
        "m[k]",
    ] {
        let sql = format!("{head}{path}");
        let Ok(Statement::Select(select)) = parse(&sql) else {
            panic!("{sql}: {:?}", parse(&sql));
        };
        let order = select.order_by.unwrap();
        assert_eq!(order.terms[0].path.root, "m", "{path}");
        assert_eq!(order.terms[0].path.steps.len(), 1, "{path}");
        assert_eq!(order.terms[0].at, "1:43:1", "{path}");
    }
    let deep = fill(&format!("{head}m["), "k[", "k", "]", "]");
    assert!(parse(&deep).is_ok());
    for (path, reason) in [
        ("m[k[0]", "Expected RIGHT_BRACKET"),
        ("m[select]", "Unexpected term"),
        ("m[]", "Unexpected term"),
        ("m[k.j NOT = 1]", "Expected RIGHT_BRACKET"),
        ("m[a NOT = 1]", "Expected RIGHT_BRACKET"),
        ("m[a NOT IS NULL]", "Expected RIGHT_BRACKET"),
        ("m[k.j AND]", "Unexpected term"),
        ("m[a IN ]", "Unexpected term"),
    ] {
        assert_eq!(err(&format!("{head}{path}")), wf(reason), "{path}");
    }
    assert_eq!(condition("m[] = 1"), wf("Unexpected term"));
    // A refusal in the condition comes first.
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'p' AND a = 1e126 ORDER BY m[k[0]]"),
        format!("{OVERFLOW_MESSAGE} under root")
    );
}

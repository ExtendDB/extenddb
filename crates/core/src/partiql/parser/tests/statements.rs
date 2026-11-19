// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the statements, their targets and their clauses.

use super::*;

#[test]
fn select_missing_from() {
    assert_eq!(
        err("SELECT * WHERE pk='a'"),
        "Statement wasn't well formed, can't be processed: Expected FROM after SELECT list"
    );
}

#[test]
fn non_dml_leading_token() {
    assert_eq!(
        err("FOO * FROM \"t\""),
        "Statement wasn't well formed, can't be processed: Expected data manipulation"
    );
}

#[test]
fn an_unquoted_table_name_is_a_table() {
    let Statement::Select(s) = parse("SELECT * FROM t WHERE pk='a'").unwrap() else {
        panic!("expected select");
    };
    assert_eq!(s.from.table, "t");
    for sql in [
        "UPDATE t SET a = 1 WHERE pk = 'a'",
        "DELETE FROM t WHERE pk = 'a'",
        "INSERT INTO t VALUE {'pk': 'a'}",
    ] {
        assert!(parse(sql).is_ok(), "{sql}");
    }
    // A literal is not a table.
    assert_eq!(
        err("SELECT * FROM null WHERE pk='a'"),
        "Unexpected from source"
    );
    assert_eq!(
        err("SELECT * FROM 't' WHERE pk='a'"),
        "Unexpected from source"
    );
}

#[test]
fn delete_missing_from() {
    assert_eq!(
        err("DELETE \"t\" WHERE pk='a' AND sk='b'"),
        "Statement wasn't well formed, can't be processed: Expected FROM after DELETE"
    );
}

#[test]
fn update_without_set_or_remove() {
    assert_eq!(
        err("UPDATE \"t\" WHERE pk='a' AND sk='b'"),
        "Statement wasn't well formed, can't be processed: Expected data manipulation"
    );
}

#[test]
fn insert_values_plural_is_rejected() {
    assert_eq!(
        err("INSERT INTO \"t\" VALUES {'pk':'a'}"),
        "Statement wasn't well formed, can't be processed: Expected LEFT_PAREN for row value constructor"
    );
}

#[test]
fn insert_double_quoted_key_is_rejected() {
    let m = err("INSERT INTO \"t\" VALUE {\"pk\":'a'}");
    assert!(
        m.starts_with("Tuple keys must be of type String. Actual type: Id under root at "),
        "{m}"
    );
}

#[test]
fn a_closing_token_where_an_item_key_starts_is_an_unexpected_term() {
    // (measured 2026-11-06, the same answer as for a nested map's key)
    for item in [
        "{]",
        "{)",
        "{>>",
        "{,}",
        "{'pk': 'a', ]",
        "{'pk': 'a', 'sk': 's',}",
        "{'pk': 'a', 'sk': 's', }",
    ] {
        assert_eq!(
            err(&format!("INSERT INTO \"t\" VALUE {item}")),
            "Statement wasn't well formed, can't be processed: Unexpected term",
            "{item}"
        );
    }
    // A name as a key keeps its refusal, at the name.
    assert_eq!(
        err("INSERT INTO \"t\" VALUE {'pk': 'a', sk: 's'}"),
        "Tuple keys must be of type String. Actual type: Id under root at 1:35:2"
    );
}

#[test]
fn two_statements_separated_by_semicolon() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk='a';SELECT * FROM \"t\" WHERE pk='b'"),
        "Statement wasn't well formed, can't be processed: Unexpected token after semicolon. (Only one query is allowed.)"
    );
}

#[test]
fn aliasing_is_rejected() {
    assert_eq!(
        err("SELECT name AS n FROM \"t\" WHERE pk='a'"),
        "Aliasing is not supported"
    );
}

#[test]
fn limit_clause_is_an_unsupported_clause() {
    // The column is that of the token after LIMIT, with length 1.
    let sql = "SELECT * FROM \"t\" WHERE pk='a' LIMIT 2";
    let column = sql.find('2').unwrap() + 1;
    assert_eq!(
        err(sql),
        format!("Unsupported clause: LIMIT at 1:{column}:1")
    );
}

#[test]
fn a_projected_path_may_have_any_number_of_steps() {
    assert!(parse("SELECT a.b.c FROM \"t\" WHERE pk='a'").is_ok());
    // A reserved word after a dot is still refused.
    assert_eq!(
        err("SELECT a.inner.c FROM \"t\" WHERE pk='a'"),
        "Statement wasn't well formed, can't be processed: Invalid path dot component"
    );
}

#[test]
fn single_dot_projection_is_allowed() {
    assert!(parse("SELECT a.b FROM \"t\" WHERE pk='a'").is_ok());
}

#[test]
fn count_star_is_an_unexpected_path_component() {
    // COUNT starts at column 8 (after "SELECT ") and is 5 chars. The
    // statement parses and carries the refusal for the executor.
    let Statement::Select(s) = parse("SELECT COUNT(*) FROM \"t\" WHERE pk='a'").unwrap() else {
        panic!("expected select");
    };
    assert_eq!(
        s.projection_error.as_deref(),
        Some("Unexpected path component at 1:8:5")
    );
    assert_eq!(s.from.table, "t");
    assert!(s.where_clause.is_some());
}

#[test]
fn a_projected_call_keeps_the_first_refusal_and_parses_on() {
    let Statement::Select(s) = parse("SELECT a, size(b(c)), COUNT(*) FROM \"t\"").unwrap() else {
        panic!("expected select");
    };
    assert_eq!(
        s.projection_error.as_deref(),
        Some("Unexpected path component at 1:11:4")
    );
}

#[test]
fn a_syntax_error_after_a_projected_call_wins() {
    let m = err("SELECT COUNT(*) FROM \"t\" WHERE");
    assert!(
        m.starts_with("Statement wasn't well formed, can't be processed: "),
        "{m}"
    );
    assert_eq!(
        err("SELECT COUNT(* FROM \"t\""),
        "Unexpected path component at 1:8:5"
    );
}

#[test]
fn select_happy_path_parses() {
    let s =
        parse("SELECT a, b.c, d[0] FROM \"t\".\"gsi1\" WHERE pk='a' AND sk > 'b' ORDER BY sk DESC")
            .unwrap();
    match s {
        Statement::Select(sel) => {
            assert_eq!(sel.from.table, "t");
            assert_eq!(sel.from.index.as_deref(), Some("gsi1"));
            let order = sel.order_by.unwrap();
            assert_eq!(order.terms.len(), 1);
            assert!(order.terms[0].descending);
        }
        _ => panic!("expected select"),
    }
}

#[test]
fn order_by_takes_a_list_of_names_each_with_a_direction() {
    let order = |sql: &str| match parse(sql).unwrap() {
        Statement::Select(s) => s
            .order_by
            .unwrap()
            .terms
            .iter()
            .map(|term| (term.path.as_simple().unwrap().to_owned(), term.descending))
            .collect::<Vec<_>>(),
        other => panic!("expected select, got {other:?}"),
    };
    let base = "SELECT * FROM \"t\" WHERE pk IN ['a', 'b'] ORDER BY ";
    let term = |name: &str, descending| (name.to_owned(), descending);
    for (tail, terms) in [
        ("pk", vec![term("pk", false)]),
        ("pk DESC", vec![term("pk", true)]),
        ("pk ASC", vec![term("pk", false)]),
        ("pk, sk", vec![term("pk", false), term("sk", false)]),
        ("pk DESC, sk", vec![term("pk", true), term("sk", false)]),
        ("pk ASC, sk", vec![term("pk", false), term("sk", false)]),
        ("pk, sk DESC", vec![term("pk", false), term("sk", true)]),
        ("sk DESC, pk", vec![term("sk", true), term("pk", false)]),
        ("pk DESC, sk DESC", vec![term("pk", true), term("sk", true)]),
        (
            "pk, \"sk\", n",
            vec![term("pk", false), term("sk", false), term("n", false)],
        ),
    ] {
        assert_eq!(order(&format!("{base}{tail}")), terms, "{tail}");
    }
    // A comma that ends the statement is an unexpected term (measured
    // 2026-11-03). A comma before anything else but a name stays a token
    // left over; the service answers it with an internal error.
    assert_eq!(err(&format!("{base}pk,")), wf("Unexpected term"));
    assert_eq!(
        err(&format!("{base}pk, 1")),
        wf("Unexpected token after expression")
    );
}

#[test]
fn insert_rich_literal_parses() {
    let s = parse(
        "INSERT INTO \"t\" VALUE {'pk':'p','n':3.5,'b':true,'z':null,'m':{'x':1},'l':[1,'two'],'ss':<<'a','b'>>}",
    )
    .unwrap();
    match s {
        Statement::Insert(i) => assert_eq!(i.value.len(), 7),
        _ => panic!("expected insert"),
    }
}

#[test]
fn update_with_params_assigns_positional_indices() {
    let s = parse("UPDATE \"t\" SET a = ? WHERE pk = ? AND sk = ?").unwrap();
    match s {
        Statement::Update(u) => {
            assert!(matches!(u.sets[0].value, Operand::Parameter(0)));
        }
        _ => panic!("expected update"),
    }
}

#[test]
fn a_bare_is_type_test_is_an_unsupported_operation() {
    for sql in [
        "x IS INT4",
        "x IS INT8",
        "x IS DOUBLE PRECISION",
        "x IS CHARACTER VARYING",
        "x IS NULL",
    ] {
        assert_eq!(err(sql), "Unsupported operation: IsType", "{sql}");
    }
    assert_eq!(err("x IS FOO"), wf("Expected type name"));
    assert_eq!(err("x IS INT4 y"), wf("Unexpected token after expression"));
}

#[test]
fn a_bare_expression_is_refused_by_what_it_is() {
    const CHECK: &str = "EXISTS can only be used in ExecuteTransaction write requests.";
    for (sql, expected) in [
        ("x", "Unsupported operation: Id"),
        ("1", "Unsupported operation: Lit"),
        ("x = 1", CHECK),
        ("NOT x", CHECK),
        ("x IS NOT INT4", CHECK),
        ("x IS VARCHAR(10)", "Unsupported operation: IsType"),
        ("x IS TIME WITH TIME ZONE", "Unsupported operation: IsType"),
        ("'a' IS STRING", "Unsupported operation: IsType"),
        ("? IS NULL", "Unsupported operation: IsType"),
        ("x IS INT4;", "Unsupported operation: IsType"),
    ] {
        assert_eq!(err(sql), expected, "{sql}");
    }
    assert_eq!(err("x IS ES_ANY"), wf("Expected type name"));
    // `*` then a FROM-led form that has no data manipulation.
    assert_eq!(err("FOO * FROM \"t\""), wf("Expected data manipulation"));
    // A call is a condition check like an operator expression.
    for sql in ["size(x)", "attribute_exists(x)", "foo(x)"] {
        assert_eq!(err(sql), CHECK, "{sql}");
    }
}

#[test]
fn a_statement_may_end_with_a_semicolon() {
    assert!(parse("SELECT * FROM \"t\" WHERE pk = 'p';").is_ok());
    assert!(parse("UPDATE \"t\" SET a = 1 WHERE pk = 'p';").is_ok());
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'p'; x"),
        wf("Unexpected token after semicolon. (Only one query is allowed.)")
    );
}

#[test]
fn an_insert_without_value_reads_the_items_to_insert() {
    const MANY: &str = "Unsupported operation: Inserting multiple items in a single \
                        statement is not supported, use \"INSERT INTO tableName VALUE \
                        item\" instead";
    for sql in [
        "INSERT INTO \"t\" {'pk': 'x'}",
        "INSERT INTO \"t\" t",
        "INSERT INTO \"t\" 'x'",
    ] {
        assert_eq!(err(sql), MANY, "{sql}");
    }
    assert_eq!(err("INSERT INTO \"t\""), wf("Unexpected term"));
    assert_eq!(
        err("INSERT INTO \"t\" t VALUE {'pk': 'x'}"),
        wf("Unexpected token after expression")
    );
}

#[test]
fn as_with_no_name_after_it_is_a_syntax_error() {
    for sql in [
        "SELECT pk AS FROM \"t\" WHERE pk = 'p'",
        "SELECT * FROM \"t\" AS WHERE pk = 'p'",
    ] {
        assert_eq!(err(sql), wf("Expected identifier for AS-alias"), "{sql}");
    }
}

#[test]
fn at_and_by_after_a_table_are_aliases() {
    for sql in [
        "SELECT * FROM \"t\" AT x WHERE pk = 'p'",
        "SELECT * FROM \"t\" BY x WHERE pk = 'p'",
    ] {
        assert_eq!(err(sql), "Aliasing is not supported", "{sql}");
    }
}

#[test]
fn an_alias_is_refused_ahead_of_an_empty_index_name() {
    assert_eq!(
        err("SELECT * FROM \"t\".\"\" x WHERE pk = 'p'"),
        "Aliasing is not supported"
    );
}

#[test]
fn a_from_path_step_that_is_not_a_name() {
    for sql in [
        "SELECT * FROM \"t\".index WHERE pk = 'p'",
        "SELECT * FROM \"t\". WHERE pk = 'p'",
    ] {
        assert_eq!(err(sql), wf("Invalid path dot component"), "{sql}");
    }
}

#[test]
fn an_update_without_where_parses_for_execution_to_refuse() {
    let Statement::Update(u) = parse("UPDATE \"t\" SET a = 1").unwrap() else {
        panic!("expected update");
    };
    assert!(u.where_clause.is_none());
    assert_eq!(err("UPDATE \"t\" t SET a = 1"), "Aliasing is not supported");
    assert_eq!(
        err("UPDATE \"t\" SET a = 1 FOO"),
        wf("Unexpected token after expression")
    );
}

#[test]
fn a_dml_statement_with_no_table() {
    for sql in [
        "DELETE FROM",
        "DELETE FROM   ",
        "DELETE FROM;",
        "DELETE FROM WHERE pk = 'a'",
        "UPDATE SET a = 1 WHERE pk = 'a'",
        "INSERT INTO VALUE {'pk': 'a'}",
    ] {
        assert_eq!(err(sql), wf("Expected identifier for simple path"), "{sql}");
    }
}

#[test]
fn select_from_with_no_table() {
    assert_eq!(err("SELECT * FROM"), wf("Unexpected term"));
    assert_eq!(
        err("SELECT * FROM WHERE pk = 'a'"),
        wf("Unexpected keyword")
    );
}

#[test]
fn a_table_alias_is_refused() {
    for sql in [
        "SELECT * FROM \"t\" t WHERE pk = 'x'",
        "SELECT * FROM \"t\" AS t WHERE pk = 'x'",
        "UPDATE \"t\" t SET a = 3 WHERE pk = 'x'",
        "DELETE FROM \"t\" t WHERE pk = 'x'",
        "SELECT a b FROM \"t\" WHERE pk = 'x'",
    ] {
        assert_eq!(err(sql), "Aliasing is not supported", "{sql}");
    }
    // An INSERT target takes no alias: the name is a leftover token.
    assert_eq!(
        err("INSERT INTO \"t\" t VALUE {'pk': 'x'}"),
        wf("Unexpected token after expression")
    );
}

#[test]
fn a_syntax_error_is_reported_ahead_of_an_alias() {
    assert_eq!(err("SELECT * FROM \"t\" t WHERE"), wf("Unexpected term"));
}

#[test]
fn a_lexing_failure_has_nothing_after_the_prefix() {
    assert_eq!(err("SELECT * FROM \"t\" WHERE pk = 1 # note"), WF);
    assert_eq!(err("SELECT * FROM \"t\" WHERE pk = 1 /* note"), WF);
}

#[test]
fn comments_read_as_whitespace() {
    for sql in [
        "SELECT * FROM \"t\" WHERE pk = 'a' -- a note",
        "SELECT * FROM \"t\" -- a note\nWHERE pk = 'a'",
        "-- a note\nSELECT * FROM \"t\" WHERE pk = 'a'",
        "SELECT * FROM \"t\" /* a note */ WHERE pk = 'a'",
        "SELECT * FROM \"t\" WHERE pk = 'a' /* a note */",
    ] {
        let w = where_of(sql);
        assert!(matches!(w, WhereExpr::Compare(..)), "{sql}");
    }
}

#[test]
fn an_empty_from_component_is_refused_after_the_parse() {
    assert_eq!(
        err("SELECT * FROM \"\" WHERE pk = 'x'"),
        "Path component cannot be an empty string"
    );
    assert_eq!(
        err("SELECT * FROM \"t\".\"\" WHERE pk = 'x'"),
        "Path component cannot be an empty string"
    );
    assert_eq!(err("SELECT * FROM \"\" WHERE"), wf("Unexpected term"));
}

#[test]
fn delete_returning_all_old_parses() {
    let s = parse("DELETE FROM \"t\" WHERE pk='a' AND sk='b' RETURNING ALL OLD *").unwrap();
    match s {
        Statement::Delete(d) => assert_eq!(d.returning, Some(super::Returning::AllOld)),
        _ => panic!("expected delete"),
    }
}

#[test]
fn a_delete_returning_all_with_nothing_after_it_is_a_syntax_error() {
    let element = wf("Expected ( MODIFIED | ALL ) ( NEW | OLD ) in each returning element.");
    for sql in [
        "DELETE FROM \"t\" WHERE pk = 'a' RETURNING ALL",
        "DELETE FROM \"t\" WHERE pk = 'a' AND c = 1e126 RETURNING ALL",
    ] {
        assert_eq!(err(sql), element, "{sql}");
    }
    // An UPDATE's is not measured.
    assert_eq!(
        err("UPDATE \"t\" SET a = 1 WHERE pk = 'a' RETURNING ALL"),
        "Invalid returning clause"
    );
}

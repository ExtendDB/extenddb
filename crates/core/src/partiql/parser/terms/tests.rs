// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the answers for a reserved word or an `@` where a term or a
//! name starts.

use crate::partiql::parser::{
    EXISTS_OUTSIDE_TRANSACTION, UNSUPPORTED_IN_TRANSACTION, parse_for_execute_statement,
    parse_for_transaction, unsupported_operation,
};
use crate::partiql::{Statement, parse};

const WF: &str = "Statement wasn't well formed, can't be processed: ";

fn wf(reason: &str) -> String {
    format!("{WF}{reason}")
}

/// The parse failure, or the projection refusal a SELECT carries, or
/// "parsed".
fn answer(sql: &str) -> String {
    match parse(sql) {
        Err(e) => e.message,
        Ok(Statement::Select(s)) => s.projection_error.unwrap_or_else(|| "parsed".to_owned()),
        Ok(_) => "parsed".to_owned(),
    }
}

/// A word's answers in the four measured positions: a condition, the
/// argument of attribute_exists, a SET target and a projection.
fn positions(word: &str) -> [String; 4] {
    [
        answer(&format!("SELECT * FROM \"tbl\" WHERE {word} = 1")),
        answer(&format!(
            "SELECT * FROM \"tbl\" WHERE attribute_exists({word})"
        )),
        answer(&format!("UPDATE \"tbl\" SET {word} = 1 WHERE pk = 'a'")),
        answer(&format!("SELECT {word} FROM \"tbl\"")),
    ]
}

fn in_select_list(word: &str) -> String {
    wf(&format!(
        "Expected identifier or an expression but found unexpected keyword '{word}' in a select list."
    ))
}

#[test]
fn a_reserved_word_in_the_four_positions() {
    let keyword = wf("Unexpected keyword");
    let operator = wf("Unexpected operator");
    let term = wf("Unexpected term");
    let simple = wf("Expected identifier for simple path");
    let manipulation = wf("Expected data manipulation");
    let date = wf("Expected date string followed by the keyword DATE, found SYMBOL");
    let time = wf(
        "Expected time string followed by the keyword TIME OR TIME WITH TIME ZONE, found SYMBOL",
    );
    let cases: Vec<(&str, [String; 4])> = vec![
        ("order", [keyword.clone(), keyword.clone(), simple.clone(), keyword.clone()]),
        ("INNER", [keyword.clone(), keyword.clone(), simple.clone(), keyword.clone()]),
        ("count", [keyword.clone(), keyword.clone(), simple.clone(), keyword.clone()]),
        ("insert", [keyword.clone(), keyword.clone(), simple.clone(), keyword.clone()]),
        ("and", [operator.clone(), operator.clone(), simple.clone(), operator.clone()]),
        ("between", [operator.clone(), operator.clone(), simple.clone(), operator.clone()]),
        ("as", [term.clone(), term.clone(), simple.clone(), term.clone()]),
        ("desc", [term.clone(), term.clone(), simple.clone(), term.clone()]),
        ("not", [operator.clone(), term.clone(), simple.clone(), manipulation.clone()]),
        ("case", [operator.clone(), term.clone(), simple.clone(), manipulation.clone()]),
        ("pivot", [operator.clone(), term.clone(), simple.clone(), manipulation.clone()]),
        ("select", [operator.clone(), term.clone(), simple.clone(), in_select_list("select")]),
        ("from", [operator.clone(), term.clone(), simple.clone(), in_select_list("from")]),
        ("all", [keyword.clone(), keyword.clone(), simple.clone(), in_select_list("from")]),
        ("distinct", [keyword.clone(), keyword.clone(), simple.clone(), in_select_list("from")]),
        ("value", [keyword.clone(), keyword.clone(), simple.clone(), manipulation.clone()]),
        ("where", [keyword.clone(), keyword.clone(), simple.clone(), in_select_list("where")]),
        (
            "delete",
            [
                wf("Expected FROM after DELETE"),
                wf("Expected FROM after DELETE"),
                simple.clone(),
                in_select_list("delete"),
            ],
        ),
        ("set", [simple.clone(), simple.clone(), simple.clone(), in_select_list("set")]),
        ("update", [simple.clone(), simple.clone(), simple.clone(), in_select_list("update")]),
        ("remove", [simple.clone(), simple.clone(), simple.clone(), in_select_list("remove")]),
        (
            "exec",
            [
                wf("Keyword = only expected at the top level in the query"),
                wf("Expected RIGHT_PAREN"),
                simple.clone(),
                wf("Expected FROM after SELECT list"),
            ],
        ),
        ("date", [date.clone(), date.clone(), simple.clone(), date.clone()]),
        ("time", [time.clone(), time.clone(), simple.clone(), wf("Expected WITH keyword")]),
        (
            "cast",
            [
                wf("Missing left parenthesis after cast"),
                wf("Missing left parenthesis after cast"),
                simple.clone(),
                wf("Missing left parenthesis after cast"),
            ],
        ),
        (
            "nullif",
            [
                wf("Missing left parenthesis after nullif"),
                wf("Missing left parenthesis after nullif"),
                simple.clone(),
                wf("Missing left parenthesis after nullif"),
            ],
        ),
        (
            "trim",
            [
                wf("Expected LEFT_PAREN"),
                wf("Expected LEFT_PAREN"),
                simple.clone(),
                wf("Expected LEFT_PAREN"),
            ],
        ),
        (
            "values",
            [
                wf("Expected LEFT_PAREN for row value constructor"),
                wf("Expected LEFT_PAREN for row value constructor"),
                simple.clone(),
                wf("Expected LEFT_PAREN for row value constructor"),
            ],
        ),
        (
            "create",
            [
                wf("Unexpected token following CREATE"),
                wf("Unexpected token following CREATE"),
                simple.clone(),
                wf("Unexpected token following CREATE"),
            ],
        ),
        (
            "drop",
            [
                wf("Unexpected token following DROP"),
                wf("Unexpected token following DROP"),
                simple.clone(),
                wf("Unexpected token following DROP"),
            ],
        ),
        (
            "missing",
            [
                "MISSING must be compared using IS operator".to_owned(),
                "MISSING must be compared using IS operator".to_owned(),
                simple.clone(),
                "Unexpected path component at 1:8:7".to_owned(),
            ],
        ),
        (
            "null",
            [
                "parsed".to_owned(),
                "Operator or function requires a document path; operator or function: attribute_exists"
                    .to_owned(),
                simple.clone(),
                "Unexpected path component at 1:8:4".to_owned(),
            ],
        ),
        (
            "false",
            [
                "parsed".to_owned(),
                "Operator or function requires a document path; operator or function: attribute_exists"
                    .to_owned(),
                simple.clone(),
                "Unexpected path component at 1:8:5".to_owned(),
            ],
        ),
    ];
    for (word, expected) in cases {
        assert_eq!(positions(word), expected, "{word}");
    }
}

#[test]
fn a_word_the_service_does_not_reserve_is_a_name_everywhere() {
    for word in ["year", "partition", "leading", "upsert", "timezone_hour"] {
        assert_eq!(
            positions(word),
            ["parsed", "parsed", "parsed", "parsed"],
            "{word}"
        );
    }
}

#[test]
fn a_reserved_word_where_a_from_source_starts() {
    for (word, expected) in [
        ("where", wf("Unexpected keyword")),
        ("count", wf("Unexpected keyword")),
        ("as", wf("Unexpected term")),
        ("and", wf("Unexpected operator")),
        ("set", wf("Expected identifier for simple path")),
        ("select", in_select_list("where")),
        ("null", "Unexpected from source".to_owned()),
    ] {
        assert_eq!(
            answer(&format!("SELECT * FROM {word} WHERE pk = 'p'")),
            expected,
            "{word}"
        );
    }
    for word in [
        "where", "value", "select", "inner", "as", "and", "null", "set",
    ] {
        assert_eq!(
            answer(&format!("DELETE FROM {word} WHERE pk = 'p'")),
            wf("Expected identifier for simple path"),
            "{word}"
        );
    }
}

#[test]
fn the_forms_that_take_an_expression_report_the_next_token() {
    // FROM-led form with a WHERE and no data manipulation.
    assert_eq!(
        answer("SELECT case FROM \"tbl\" WHERE pk = 'p'"),
        wf("Expected data manipulation")
    );
    // EXEC reads to the end of its arguments; the next clause is checked.
    assert_eq!(
        answer("SELECT exec FROM \"tbl\" WHERE pk = 'p'"),
        wf("Expected FROM after SELECT list")
    );
    assert_eq!(
        answer("SELECT * FROM \"tbl\" WHERE pk = 'p' AND exec = 1"),
        wf("Keyword = only expected at the top level in the query")
    );
}

#[test]
fn ion_type_names() {
    use super::ion_type_name;
    assert_eq!(ion_type_name("1"), "INT");
    assert_eq!(ion_type_name("-12"), "INT");
    assert_eq!(ion_type_name("1.5"), "DECIMAL");
    assert_eq!(ion_type_name("1e0"), "FLOAT");
    assert_eq!(ion_type_name("\"a\""), "STRING");
    assert_eq!(ion_type_name("{a: 1}"), "STRUCT");
    assert_eq!(ion_type_name("[1]"), "LIST");
    assert_eq!(ion_type_name("abc"), "SYMBOL");
}

/// The statements that are a call: ExecuteStatement takes each for a
/// condition check, and a transaction answers each as an operation it does
/// not run.
const CALL_STATEMENTS: [&str; 14] = [
    "foo(1)",
    "foo()",
    "FOO (1)",
    "foo(?)",
    "foo(<<>>)",
    "foo(1e999999)",
    "foo(foo(1), a.b[0])",
    "size(1)",
    "attribute_exists(a)",
    "upper('a')",
    "EXISTS()",
    "EXISTS(1, 2)",
    "exists(foo(1))",
    "\"foo\"(1)",
];

#[test]
fn a_statement_that_is_a_call_is_refused_by_the_request() {
    for statement in CALL_STATEMENTS {
        let parameters = usize::from(statement.contains('?'));
        assert_eq!(
            parse_for_execute_statement(statement, parameters)
                .unwrap_err()
                .message,
            EXISTS_OUTSIDE_TRANSACTION,
            "{statement}"
        );
        assert_eq!(
            parse_for_transaction(statement, parameters)
                .unwrap_err()
                .message,
            UNSUPPORTED_IN_TRANSACTION,
            "{statement}"
        );
    }
    // A query with more arguments after it is a call like any other.
    let exists = "EXISTS(SELECT * FROM \"tbl\" WHERE pk = 1, 1)";
    assert_eq!(
        parse_for_transaction(exists, 0).unwrap_err().message,
        UNSUPPORTED_IN_TRANSACTION
    );
    assert!(parse_for_transaction("EXISTS(SELECT * FROM \"tbl\" WHERE pk = 1)", 0).is_ok());
}

#[test]
fn a_statement_the_service_reads_and_does_not_run_is_named_by_its_form() {
    for (statement, expected) in [
        ("count(1)", unsupported_operation("CallAgg")),
        ("sum(1)", unsupported_operation("CallAgg")),
        ("foo(1).a", unsupported_operation("Path")),
        ("foo(1)[0].a", unsupported_operation("Path")),
        (
            "foo(DELETE FROM \"tbl\" WHERE pk = 1)",
            wf("Keyword delete only expected at the top level in the query"),
        ),
    ] {
        assert_eq!(answer(statement), expected, "{statement}");
    }
}

#[test]
fn a_query_where_a_term_starts_is_refused_at_its_select() {
    for statement in [
        "SELECT * FROM \"tbl\" WHERE pk = (SELECT * FROM \"tbl\")",
        "SELECT * FROM \"tbl\" WHERE a IN [SELECT a FROM \"tbl\"]",
        "UPDATE \"tbl\" SET a = (SELECT * FROM \"tbl\") + 1 WHERE pk = 'a'",
        "SELECT * FROM \"tbl\" WHERE pk = (SELECT * FROM \"other\" WHERE pk = 'p' ORDER BY pk)",
    ] {
        // The nested SELECT is the last one in each statement.
        let column = statement.rfind("SELECT").unwrap() + 1;
        assert_eq!(
            answer(statement),
            format!("Unsupported token in expression: SELECT at 1:{column}:6"),
            "{statement}"
        );
    }
    // A fault in the query itself is a syntax error.
    assert_eq!(
        answer("SELECT * FROM \"tbl\" WHERE pk = (SELECT a)"),
        wf("Expected FROM after SELECT list")
    );
}

/// Stands for the answer a statement gets with each `@` replaced by a space:
/// the name after an `@` reads as the name, at the name's position.
const AS_THE_NAME: &str = "as the name";

#[test]
fn an_at_sign_must_have_a_name_after_it() {
    let at: &str = &wf("Identifier must follow @-operator");
    let from: &str = &wf("Expected FROM after SELECT list");
    let after: &str = &wf("Unexpected token after expression");
    let simple: &str = &wf("Expected identifier for simple path");
    let not_simple: &str = &wf("Invalid path component for simple path");
    let dot: &str = &wf("Invalid path dot component");
    let paren: &str = &wf("Expected RIGHT_PAREN");
    let alias: &str = &wf("Expected identifier for AS-alias");
    let exec: &str = &wf("Keyword foo only expected at the top level in the query");
    let exec_at: &str = &wf("Keyword @ only expected at the top level in the query");
    let curly: &str = &wf("Expected } to close item value");
    let lexing: &str = &wf("");
    let (parsed, name) = ("parsed", AS_THE_NAME);
    // Measured 2026-11-23 on tables that do not exist, each statement twice:
    // "parsed" where the service refuses the missing table. `<T>` is
    // `"ets-parityat-missing"`, which the positions count.
    let rows = [
        ("SELECT @a FROM <T> WHERE pk = 'q'", parsed),
        ("SELECT @ a FROM <T> WHERE pk = 'q'", parsed),
        ("SELECT @\"a\" FROM <T> WHERE pk = 'q'", parsed),
        ("SELECT @ \"a\" FROM <T> WHERE pk = 'q'", parsed),
        ("SELECT @a.b FROM <T> WHERE pk = 'q'", parsed),
        ("SELECT @a[0] FROM <T> WHERE pk = 'q'", parsed),
        (
            "SELECT @a AS x FROM <T> WHERE pk = 'q'",
            "Aliasing is not supported",
        ),
        ("SELECT @size(a) FROM <T> WHERE pk = 'q'", at),
        ("SELECT @SIZE(a) FROM <T> WHERE pk = 'q'", at),
        ("SELECT @ size(a) FROM <T> WHERE pk = 'q'", at),
        ("SELECT @size FROM <T> WHERE pk = 'q'", at),
        ("SELECT @select FROM <T> WHERE pk = 'q'", at),
        ("SELECT @missing FROM <T> WHERE pk = 'q'", at),
        ("SELECT @null FROM <T> WHERE pk = 'q'", at),
        ("SELECT @and FROM <T> WHERE pk = 'q'", at),
        ("SELECT @as FROM <T> WHERE pk = 'q'", at),
        ("SELECT @1 FROM <T> WHERE pk = 'q'", at),
        ("SELECT @'s' FROM <T> WHERE pk = 'q'", at),
        ("SELECT @@a FROM <T> WHERE pk = 'q'", at),
        ("SELECT @(a) FROM <T> WHERE pk = 'q'", at),
        ("SELECT @ FROM <T> WHERE pk = 'q'", at),
        ("SELECT @foo(a) FROM <T> WHERE pk = 'q'", from),
        ("SELECT @begins_with(a, 'x') FROM <T> WHERE pk = 'q'", from),
        ("SELECT @\"size\"(a) FROM <T> WHERE pk = 'q'", from),
        ("SELECT a.@b FROM <T> WHERE pk = 'q'", dot),
        ("SELECT @size FROM <T> WHERE pk = 'q' #", lexing),
        ("SELECT * FROM <T> WHERE @pk = 'q'", parsed),
        ("SELECT * FROM <T> WHERE size(@a) = 1", parsed),
        ("SELECT * FROM <T> WHERE begins_with(@pk, 'q')", parsed),
        ("SELECT * FROM <T> WHERE attribute_exists(@a)", parsed),
        ("SELECT * FROM <T> WHERE @a IS MISSING", parsed),
        ("SELECT * FROM <T> WHERE @a IN [1, 2]", parsed),
        ("SELECT * FROM <T> WHERE @a BETWEEN 1 AND 2", parsed),
        ("SELECT * FROM <T> WHERE pk = 'q' AND @\"a\" = 1", parsed),
        ("SELECT * FROM <T> WHERE pk = 'q' ORDER BY @a", parsed),
        ("SELECT * FROM <T> WHERE @size(a) = 1", at),
        ("SELECT * FROM <T> WHERE pk = @", at),
        ("SELECT * FROM <T> WHERE @foo(a) = 1", after),
        ("SELECT * FROM <T> WHERE @begins_with(pk, 'q')", after),
        ("SELECT * FROM <T> WHERE pk = 'q' @", after),
        ("SELECT * FROM @<T> WHERE pk = 'q'", parsed),
        ("SELECT * FROM @etsparityatmissing WHERE pk = 'q'", parsed),
        ("UPDATE <T> SET a = @b WHERE pk = 'q'", parsed),
        ("UPDATE <T> SET a = 1 WHERE @pk = 'q'", parsed),
        (
            "UPDATE <T> SET a = list_append(@b, [1]) WHERE pk = 'q'",
            parsed,
        ),
        ("UPDATE <T> SET a = @size(b) WHERE pk = 'q'", at),
        ("UPDATE <T> SET a = 1 WHERE pk = @", at),
        ("UPDATE <T> SET a = @foo(b) WHERE pk = 'q'", after),
        ("UPDATE <T> SET @a = 1 WHERE pk = 'q'", simple),
        ("UPDATE <T> SET @size = 1 WHERE pk = 'q'", simple),
        ("UPDATE <T> REMOVE @a WHERE pk = 'q'", simple),
        ("UPDATE @<T> SET a = 1 WHERE pk = 'q'", simple),
        ("DELETE FROM <T> WHERE @pk = 'q'", parsed),
        ("DELETE FROM <T> WHERE pk = @size(a)", at),
        ("DELETE FROM @<T> WHERE pk = 'q'", simple),
        ("INSERT INTO @<T> VALUE {'pk': 'q'}", simple),
        ("INSERT INTO <T> VALUE {'pk': 'q', 'a': @size(b)}", at),
        (
            "INSERT INTO <T> VALUE {'pk': @a}",
            "Unsupported data type: a under key root at 1:50:1",
        ),
        // With an `@` before them or none, the service refuses `"b"` and `a.b`
        // as item values with texts ExtendDB does not give.
        ("INSERT INTO <T> VALUE {'pk': 'q', 'a': @\"b\"}", name),
        ("INSERT INTO <T> VALUE {'pk': 'q', 'a': @a.b}", name),
        // The other places where an operand or an alias starts.
        ("SELECT * FROM <T> WHERE pk = 'q' ORDER BY pk, @a", parsed),
        ("SELECT * FROM <T> WHERE pk = 'q' ORDER BY @m.x", parsed),
        ("SELECT * FROM <T> WHERE a IN <<@b>>", parsed),
        ("SELECT * FROM <T> WHERE m[@k] = 1", parsed),
        ("UPDATE <T> SET m[@k] = 1 WHERE pk = 'q'", not_simple),
        (
            "UPDATE <T> SET a = [@b] WHERE pk = 'q'",
            "Unsupported data type: Id under key root[0] at 1:41:1",
        ),
        (
            "INSERT INTO <T> VALUE {'pk': 'q', 'm': {'x': @y}}",
            "Unsupported data type: Id under key root.x at 1:66:1",
        ),
        (
            "SELECT * FROM <T> WHERE @a IS INT4",
            "Unsupported data type for IS operator: Integer4Type. Supported data types are \
             [MISSING, NULL, BOOLEAN, BLOB, STRING, NUMERIC, STRUCT, TUPLE, LIST] at 1:45:1",
        ),
        (
            "UPDATE <T> SET s = set_add(@s, 5) WHERE pk = 'q'",
            "The second argument to SET_ADD must be a value with type SET at 1:48:1",
        ),
        (
            "INSERT INTO <T> VALUE {@a: 1}",
            "Tuple keys must be of type String. Actual type: Id under root at 1:44:1",
        ),
        (
            "SELECT * FROM <T> WHERE a IN ((@m.b, 1))",
            "Unsupported data type: Path under key root[0] at 1:52:1",
        ),
        (
            "SELECT * FROM <T> WHERE a IN ((@\"pk\", 1))",
            "Unsupported data type: Id under key root[0] at 1:52:4",
        ),
        ("SELECT * FROM <T> WHERE a IN ((@foo(b), 1))", paren),
        ("SELECT pk AS @x FROM <T> WHERE pk = 'q'", alias),
        ("SELECT * FROM <T> @x WHERE pk = 'q'", after),
        ("SELECT * FROM <T> WHERE exec foo @a", exec),
        (
            "UPDATE <T> SET a = @set_add(a, <<'x'>>) WHERE pk = 'q'",
            after,
        ),
        ("@foo(1)", after),
        // The service names the missing `}` "Expected RIGHT_CURLY", a text
        // ExtendDB does not give for an item value (#887).
        ("INSERT INTO <T> VALUE {'pk': 'q', 'a': @foo(b)}", curly),
        ("INSERT INTO <T> VALUE {@1: 1}", at),
        ("INSERT INTO <T> VALUE {'pk': 'q', 'm': {@1: 1}}", at),
        ("INSERT INTO <T> VALUE {'pk': 'q', 'm': {@'x': 1}}", at),
        ("SELECT * FROM <T> WHERE a IN @", at),
        (
            "SELECT * FROM <T> WHERE a IN @b",
            "IN operator must have a left hand argument of type Variable Reference and \
             right hand argument of type Seq with at least one member",
        ),
        ("SELECT * FROM <T> WHERE exec @a", exec_at),
        ("UPDATE <T> REMOVE m[@k] WHERE pk = 'q'", not_simple),
        (
            "INSERT INTO <T> VALUE {'pk': 'q', 'm': {@a: 1}}",
            "Tuple keys must be of type String. Actual type: Id under root at 1:61:1",
        ),
        // The service gives `<<a>>` and `<<@a>>` the Bag text, at the name;
        // ExtendDB gives a name in a bag the name's own text (#1383).
        ("INSERT INTO <T> VALUE {'pk': 'q', 's': <<@a>>}", name),
        (
            "INSERT INTO <T> VALUE @a",
            "Unsupported format in item expression. Please use struct format {...} to \
             express item values for insert statement. at 1:43:1",
        ),
    ];
    for (statement, expected) in rows {
        let statement = statement.replace("<T>", "\"ets-parityat-missing\"");
        let expected = if expected == AS_THE_NAME {
            answer(&statement.replace('@', " "))
        } else {
            expected.to_owned()
        };
        assert_eq!(answer(&statement), expected, "{statement}");
    }
}

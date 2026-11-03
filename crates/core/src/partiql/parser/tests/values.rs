// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for literal values, parameters, numbers and paths.

use super::*;

#[test]
fn duplicate_bag_members_are_named() {
    assert_eq!(
        err("INSERT INTO \"t\" VALUE {'pk':'a','s':<<'a','a'>>}"),
        "One or more parameter values were invalid: Input collection [a, a] contains duplicates."
    );
}

/// The refusal of the first `?` inside a literal, at its column.
fn parameter_in_literal(sql: &str, path: &str) -> String {
    let column = sql.find('?').unwrap() + 1;
    format!("Unsupported data type: Parameter under key {path} at 1:{column}:1")
}

#[test]
fn a_parameter_inside_a_literal_is_refused_at_its_position() {
    for (sql, path) in [
        ("INSERT INTO \"t\" VALUE {'pk':'m1','w':{'k':?}}", "root.k"),
        ("INSERT INTO \"t\" VALUE {'pk':'m2','w':[?]}", "root[0]"),
        ("INSERT INTO \"t\" VALUE {'pk':'m3','w':[1,?]}", "root[1]"),
        (
            "INSERT INTO \"t\" VALUE {'pk':'m4','w':{'a':{'b':?}}}",
            "root.a.b",
        ),
        (
            "INSERT INTO \"t\" VALUE {'pk':'m7','w':{'k':?, 'j':?}}",
            "root.k",
        ),
        (
            "INSERT INTO \"t\" VALUE {'pk':'m8','w':{'a':[1, {'b':?}]}}",
            "root.a[1].b",
        ),
        ("UPDATE \"t\" SET w = {'k':?} WHERE pk = 'p'", "root.k"),
        ("UPDATE \"t\" SET w = [?] WHERE pk = 'p'", "root[0]"),
        (
            "SELECT * FROM \"t\" WHERE pk = 'p' AND w = {'k':?}",
            "root.k",
        ),
        ("SELECT * FROM \"t\" WHERE pk = 'p' AND w = [?]", "root[0]"),
    ] {
        assert_eq!(err(sql), parameter_in_literal(sql, path), "{sql}");
    }
    // A parameter as the whole value is a parameter, and a syntax error
    // after the literal wins.
    assert!(parse("INSERT INTO \"t\" VALUE {'pk':'m6','w':?}").is_ok());
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'p' AND w = {'k':?} AND"),
        wf(MISSING_RHS)
    );
}

#[test]
fn a_value_that_names_an_attribute_twice_is_refused_at_the_second_name() {
    // Measured 2026-11-03 with table names of these lengths: the line,
    // column and length of the second occurrence, the quotes counted.
    for (sql, key, at) in [
        (
            "INSERT INTO \"pqa-abcdef-c\" VALUE {'pk': 'd1', 'pk': 'd2', 'sk': 's'}",
            "pk",
            "1:47:4",
        ),
        (
            "INSERT INTO \"pqa-abcdef-c\" VALUE {'pk': 'a', 'pk': 'b', 'sk': 'dup'}",
            "pk",
            "1:46:4",
        ),
        (
            "INSERT INTO \"pqa-abcdef-c\" VALUE {'pk': 'd3', 'sk': 's', 'v': '1', 'v': '2'}",
            "v",
            "1:68:3",
        ),
        (
            "INSERT INTO \"pqa-abcdef-c\" VALUE {'pk': 'd4', 'sk': 's1', 'sk': 's2'}",
            "sk",
            "1:59:4",
        ),
        (
            "INSERT INTO \"rv-0123456789\" VALUE {'pk': 'd1', 'pk': 'd2', 'sk': 's'}",
            "pk",
            "1:48:4",
        ),
        (
            "INSERT INTO \"rv-0123456789\" VALUE {'pk': 'd3', 'sk': 's', 'v': '1', 'v': '2'}",
            "v",
            "1:69:3",
        ),
    ] {
        assert_eq!(
            err(sql),
            format!("Duplicate keys in tuple. Key: {key} under root at {at}"),
            "{sql}"
        );
    }
}

#[test]
fn any_other_map_that_names_a_key_twice_is_refused_at_its_opening_brace() {
    // Measured 2026-11-03 with a table name of this length: the position of
    // the `{` of the map that names the key twice, length 1, and the path
    // of that map inside the value.
    for (sql, path, at) in [
        (
            "INSERT INTO \"rv-0123456789\" VALUE {'pk': 'n1', 'sk': 's', 'm': {'x': 1, 'x': 2}}",
            "root",
            "1:64:1",
        ),
        (
            "INSERT INTO \"rv-0123456789\" VALUE {'pk': 'n2', 'sk': 's', 'l': [{'x': 1, 'x': 2}]}",
            "root[0]",
            "1:65:1",
        ),
        (
            "INSERT INTO \"rv-0123456789\" VALUE {'pk': 'n4', 'sk': 's', 'm': {'y': {'x': 1, 'x': \
             2}}}",
            "root.y",
            "1:70:1",
        ),
        (
            "UPDATE \"rv-0123456789\" SET m = {'x': 1, 'x': 2} WHERE pk = 'a' AND sk = 'x'",
            "root",
            "1:32:1",
        ),
        (
            "SELECT * FROM \"rv-0123456789\" WHERE pk = 'a' AND m = {'x': 1, 'x': 2}",
            "root",
            "1:54:1",
        ),
    ] {
        assert_eq!(
            err(sql),
            format!("Duplicate keys in tuple. Key: x under {path} at {at}"),
            "{sql}"
        );
    }
    // The same rule elsewhere in a value, and for a name given three times.
    for (sql, path) in [
        (
            "INSERT INTO \"t\" VALUE {'pk':'a','m':{'n':{'x':1,'x':2}}}",
            "root.n",
        ),
        (
            "INSERT INTO \"t\" VALUE {'pk':'a','l':[1,{'x':1,'x':2}]}",
            "root[1]",
        ),
        (
            "INSERT INTO \"t\" VALUE {'pk':'a','m':{'x':1,'x':2,'x':3}}",
            "root",
        ),
        (
            "UPDATE \"t\" SET m = [{'x':1,'x':2}] WHERE pk = 'a'",
            "root[0]",
        ),
    ] {
        let column = sql.find("{'x'").unwrap() + 1;
        assert_eq!(
            err(sql),
            format!("Duplicate keys in tuple. Key: x under {path} at 1:{column}:1"),
            "{sql}"
        );
    }
}

#[test]
fn a_repeated_key_is_one_refusal_among_the_others_of_a_value() {
    // The first refusal in the value is reported, and a syntax error after
    // the literal wins.
    let sql = "INSERT INTO \"t\" VALUE {'pk':'a','w':{'k':?},'pk':'b'}";
    assert_eq!(err(sql), parameter_in_literal(sql, "root.k"));
    let sql = "INSERT INTO \"t\" VALUE {'pk':'a','pk':'b','w':{'k':?}}";
    let column = sql.rfind("'pk'").unwrap() + 1;
    assert_eq!(
        err(sql),
        format!("Duplicate keys in tuple. Key: pk under root at 1:{column}:4")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'p' AND w = {'k':1,'k':2} AND"),
        wf(MISSING_RHS)
    );
    // Measured 2026-11-03.
    assert_eq!(
        err("INSERT INTO \"t\" VALUE {'pk': 'n7', 'pk': 'n8', 'sk': 's'} WHERE"),
        wf("Unexpected token after expression")
    );
    // Names are compared as written: a name in another case is another
    // attribute (measured 2026-11-03 for 'v' and 'V').
    assert!(parse("INSERT INTO \"t\" VALUE {'pk':'a','PK':'b'}").is_ok());
    assert!(parse("INSERT INTO \"t\" VALUE {'pk': 'n3', 'sk': 's', 'v': 1, 'V': 2}").is_ok());
    assert!(parse("UPDATE \"t\" SET m = {'x':1,'X':2} WHERE pk = 'a'").is_ok());
}

#[test]
fn a_parameter_in_a_bag_is_refused_at_its_position() {
    let sql = "INSERT INTO \"t\" VALUE {'pk':'m5','w':<<?>>}";
    let column = sql.find('?').unwrap() + 1;
    assert_eq!(
        err(sql),
        format!(
            "Unsupported data type in Bag. DynamoDB only supports either numbers or strings \
             in bags at 1:{column}:1"
        )
    );
}

#[test]
fn a_sign_before_a_set_value_that_is_not_a_number() {
    assert_eq!(
        err("UPDATE \"t\" SET a = -b WHERE pk = 'p'"),
        "Incorrect number of operands for operator or function; operator or function: -, \
         number of operands: 1"
    );
    assert!(parse("UPDATE \"t\" SET a = b - 1 WHERE pk = 'p'").is_ok());
}

#[test]
fn a_backtick_literal_is_refused_by_its_ion_type() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE s = `1`"),
        "Ion Literals are not supported: INT under key root at 1:29:3"
    );
}

#[test]
fn at_and_dollar_words_are_names() {
    let Statement::Select(s) = parse("SELECT * FROM \"t\" WHERE @n = 1 AND $x = 2").unwrap() else {
        panic!("expected select");
    };
    let Some(WhereExpr::And(a, b)) = s.where_clause else {
        panic!("expected AND");
    };
    assert!(matches!(*a, WhereExpr::Compare(Operand::Path(ref p), _, _) if p.root == "n"));
    assert!(matches!(*b, WhereExpr::Compare(Operand::Path(ref p), _, _) if p.root == "$x"));
}

#[test]
fn number_text_in_a_condition() {
    let where_ = |n: &str| err(&format!("SELECT * FROM \"t\" WHERE pk = {n}"));
    for n in ["1.2.3", ".5.5", "1e5.5", "1e5e5", "1_000", "1a", "1.2a"] {
        assert_eq!(where_(n), wf("Unexpected token after expression"), "{n}");
    }
    for n in ["1..2", "5..", "1.5e", "1e+", "5.e", "1ee5", "1e.5"] {
        assert_eq!(where_(n), wf(""), "{n}");
    }
    for n in ["1.2.", "1.2..3"] {
        assert_eq!(where_(n), wf("Invalid path dot component"), "{n}");
    }
    // The path is refused at the number it starts from.
    assert_eq!(where_("1.2.e3"), "Unexpected path component at 1:30:3");
    for n in ["..5", ".e5"] {
        assert_eq!(where_(n), wf("Unexpected term"), "{n}");
    }
    for n in ["1.e3", "1.", ".5", "00.5", "1e05", "1E5"] {
        assert!(
            parse(&format!("SELECT * FROM \"t\" WHERE pk = {n}")).is_ok(),
            "{n}"
        );
    }
}

#[test]
fn an_empty_name_in_a_condition_is_refused_after_the_parse() {
    for condition in ["\"\" = 1", "m.\"\" = 1"] {
        assert_eq!(
            err(&format!(
                "SELECT * FROM \"t\" WHERE pk = 'p' AND {condition}"
            )),
            "Path component cannot be an empty string",
            "{condition}"
        );
    }
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'p' AND \"\" = 1 AND"),
        wf(MISSING_RHS)
    );
}

#[test]
fn a_bag_whose_member_runs_on_is_missing_its_closing_brackets() {
    for function in ["set_add", "set_delete"] {
        assert_eq!(
            err(&format!(
                "UPDATE \"t\" SET ns = {function}(ns, <<1.2.3>>) WHERE pk = 'p'"
            )),
            wf("Expected RIGHT_DOUBLE_ANGLE_BRACKET"),
            "{function}"
        );
    }
}

#[test]
fn a_reserved_word_after_a_dot_is_an_invalid_path_dot_component() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'a' AND attribute_exists(m.inner)"),
        wf("Invalid path dot component")
    );
    assert_eq!(
        err("UPDATE \"t\" SET m.inner = 1 WHERE pk = 'a'"),
        wf("Invalid path dot component")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 'a' AND m. = 1"),
        wf("Invalid path dot component")
    );
    // A quoted name after a dot is a name.
    assert!(parse("SELECT * FROM \"t\" WHERE pk = 'a' AND m.\"inner\" = 1").is_ok());
}

#[test]
fn a_number_with_no_leading_digit() {
    let literal = |sql: &str| match where_of(sql) {
        WhereExpr::Compare(_, _, Operand::Literal(AttributeValue::N(n))) => n,
        other => panic!("{other:?}"),
    };
    assert_eq!(literal("SELECT * FROM \"t\" WHERE pk = .73e1"), ".73e1");
    assert_eq!(literal("SELECT * FROM \"t\" WHERE pk = -.73e1"), "-.73e1");
    assert_eq!(literal("SELECT * FROM \"t\" WHERE pk = - 7.3"), "-7.3");
    assert_eq!(literal("SELECT * FROM \"t\" WHERE pk = - -7"), "7");
    match parse("INSERT INTO \"t\" VALUE {'pk': .7325e1, 'n': -5}").unwrap() {
        Statement::Insert(i) => {
            assert_eq!(
                i.value[0].1,
                Operand::Literal(AttributeValue::N(".7325e1".to_owned()))
            );
            assert_eq!(
                i.value[1].1,
                Operand::Literal(AttributeValue::N("-5".to_owned()))
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_name_as_an_insert_value_is_refused_naming_itself() {
    let sql = "INSERT INTO \"t\" VALUE {'pk': pk}";
    let column = sql.find("pk}").unwrap() + 1;
    assert_eq!(
        err(sql),
        format!("Unsupported data type: pk under key root at 1:{column}:2")
    );
    // Inside a value it is an Id.
    let sql = "INSERT INTO \"t\" VALUE {'pk': 'a', 'x': [pk]}";
    let column = sql.find("pk]").unwrap() + 1;
    assert_eq!(
        err(sql),
        format!("Unsupported data type: Id under key root[0] at 1:{column}:2")
    );
}

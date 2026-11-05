// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the number rules of a statement's literals and parameters.

use super::*;
use crate::partiql::parse;

fn problem(text: &str) -> Option<NumberProblem> {
    check_number(text).err()
}

#[test]
fn an_exponent_outside_32_bits_cannot_be_converted() {
    for text in [
        "1e2147483648",
        "1e99999999999",
        "-1e99999999999",
        "1e-99999999999",
        "1E+2147483648",
    ] {
        assert_eq!(problem(text), Some(NumberProblem::Unconvertible), "{text}");
    }
}

#[test]
fn a_scale_outside_32_bits_cannot_be_converted() {
    // The scale is the digits after the point less the exponent.
    assert_eq!(problem("1e-2147483648"), Some(NumberProblem::Unconvertible));
    assert_eq!(
        problem("1.5e-2147483647"),
        Some(NumberProblem::Unconvertible)
    );
}

#[test]
fn the_largest_exponent_wraps_to_an_underflow() {
    assert_eq!(problem("1e2147483647"), Some(NumberProblem::Underflow));
    assert_eq!(problem("-1e2147483647"), Some(NumberProblem::Underflow));
    // 1e2147483646 has precision minus scale 2147483647: no wrap.
    assert_eq!(problem("1e2147483646"), Some(NumberProblem::Overflow));
}

#[test]
fn the_magnitude_range_matches_the_item_rules() {
    assert_eq!(problem("1e126"), Some(NumberProblem::Overflow));
    assert_eq!(problem("1e-131"), Some(NumberProblem::Underflow));
    assert_eq!(
        problem("9.9999999999999999999999999999999999999E+125"),
        None
    );
    assert_eq!(problem("1E-130"), None);
    assert_eq!(problem("0.1E-129"), None);
    assert_eq!(problem("0.01E-129"), Some(NumberProblem::Underflow));
}

#[test]
fn significant_digits_ignore_leading_and_trailing_zeros() {
    let digits39 = "123456789012345678901234567890123456789";
    assert_eq!(problem(digits39), Some(NumberProblem::TooManyDigits));
    assert_eq!(problem(&digits39[..38]), None);
    assert_eq!(problem(&format!("000{}", &digits39[..38])), None);
    assert_eq!(problem(&format!("1{}", "0".repeat(60))), None);
    assert_eq!(problem(&format!("0.{}", &digits39[..38])), None);
}

#[test]
fn zero_is_accepted_at_any_scale() {
    for text in ["0", "-0", "0.000", "0e-500", "000e2147483647"] {
        assert_eq!(problem(text), None, "{text}");
    }
}

#[test]
fn forms_the_decimal_reader_accepts() {
    // Leading exponent zeros are skipped while more than ten digits remain.
    for text in [
        "+1",
        ".5",
        "5.",
        "-.5e1",
        "1.e5",
        "1e+5",
        "1E05",
        "1e00000000005",
        "1e0000000000005",
    ] {
        assert_eq!(problem(text), None, "{text}");
    }
}

#[test]
fn forms_the_decimal_reader_refuses() {
    for text in [
        "",
        "abc",
        " 1",
        "1 ",
        ".",
        "+",
        "-",
        "1e",
        "1e+",
        "1e-",
        "1.2.3",
        "1e5e5",
        "1e5.5",
        "0x10",
        "+-1",
        "Infinity",
        "NaN",
        "1_000",
        "e5",
        "1e12345678901",
    ] {
        assert_eq!(
            problem(text),
            Some(NumberProblem::Unconvertible),
            "{text:?}"
        );
    }
}

#[test]
fn parameter_messages_echo_the_text_unless_empty() {
    assert_eq!(
        parameter_message("1e99999999999", NumberProblem::Unconvertible),
        "The parameter cannot be converted to a numeric value: 1e99999999999"
    );
    assert_eq!(
        parameter_message("", NumberProblem::Unconvertible),
        "The parameter cannot be converted to a numeric value"
    );
    assert_eq!(
        parameter_message("1e2147483647", NumberProblem::Underflow),
        UNDERFLOW_MESSAGE
    );
    assert_eq!(
        parameter_message("1e126", NumberProblem::Overflow),
        OVERFLOW_MESSAGE
    );
}

fn err(sql: &str) -> String {
    match parse(sql) {
        Err(e) => e.message,
        Ok(stmt) => check_literal_numbers(&stmt).unwrap_err().message,
    }
}

#[test]
fn an_unconvertible_literal_is_a_parse_error_with_nothing_after_the_colon() {
    for sql in [
        "SELECT * FROM \"t\" WHERE pk = 1e2147483648",
        "SELECT * FROM \"t\" WHERE pk = 1e99999999999",
        "SELECT * FROM \"t\" WHERE pk = 1.5e-2147483647",
        "INSERT INTO \"t\" VALUE {'pk': 1e99999999999}",
    ] {
        assert_eq!(
            err(sql),
            "Statement wasn't well formed, can't be processed: ",
            "{sql}"
        );
    }
}

#[test]
fn a_literal_that_breaks_a_rule_names_its_position() {
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 1e126"),
        format!("{OVERFLOW_MESSAGE} under root")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 1e-131"),
        format!("{UNDERFLOW_MESSAGE} under root")
    );
    assert_eq!(
        err("SELECT * FROM \"t\" WHERE pk = 123456789012345678901234567890123456789"),
        format!("{TOO_MANY_DIGITS_MESSAGE} under root")
    );
    assert_eq!(
        err("INSERT INTO \"t\" VALUE {'pk': 1, 'm': {'x': 1e126}}"),
        format!("{OVERFLOW_MESSAGE} under root.x")
    );
    assert_eq!(
        err("INSERT INTO \"t\" VALUE {'pk': 1, 'l': [1, 1e126]}"),
        format!("{OVERFLOW_MESSAGE} under root[1]")
    );
    assert_eq!(
        err("UPDATE \"t\" SET a = 1e-131 WHERE pk = 1"),
        format!("{UNDERFLOW_MESSAGE} under root")
    );
    assert_eq!(
        err("INSERT INTO \"t\" VALUE {'pk': 1, 'm': {'x': [{'y': 1e126}]}}"),
        format!("{OVERFLOW_MESSAGE} under root.x[0].y")
    );
}

#[test]
fn a_literal_in_a_set_add_or_set_delete_bag_names_root() {
    for function in ["set_add", "set_delete"] {
        for (literal, message) in [
            ("1e126", OVERFLOW_MESSAGE),
            ("1e-131", UNDERFLOW_MESSAGE),
            (
                "123456789012345678901234567890123456789",
                TOO_MANY_DIGITS_MESSAGE,
            ),
        ] {
            let sql =
                format!("UPDATE \"t\" SET ns = {function}(ns, <<1, {literal}>>) WHERE pk = 1");
            assert_eq!(err(&sql), format!("{message} under root"), "{sql}");
        }
        let sql = format!("UPDATE \"t\" SET ns = {function}(ns, <<1e99999999999>>) WHERE pk = 1");
        assert_eq!(
            err(&sql),
            "Statement wasn't well formed, can't be processed: ",
            "{sql}"
        );
    }
    let stmt = parse("UPDATE \"t\" SET ns = set_add(ns, <<1, 9.9E125>>) WHERE pk = 1").unwrap();
    assert!(check_literal_numbers(&stmt).is_ok());
}

#[test]
fn literals_in_range_pass() {
    let stmt = parse(
        "UPDATE \"t\" SET a = 9.9E125, b = [1e-130, {'c': 5}] WHERE pk = 1 AND n BETWEEN 1 AND 2",
    )
    .unwrap();
    assert!(check_literal_numbers(&stmt).is_ok());
}

#[test]
fn a_zero_with_a_point_or_an_exponent_is_a_zero_decimal() {
    for text in [
        "0.0", ".0", "0.", "00.0", "0.00", "0e0", "0E-3", "0.0e0", "-0.0", "+0e5",
    ] {
        assert!(is_zero_decimal(text), "{text}");
    }
    for text in ["0", "00", "-0", "0.5", "1.0", "1e0", "10e-1", ".", ""] {
        assert!(!is_zero_decimal(text), "{text}");
    }
}

/// The refusal of a negative zero decimal at `path`, the number `number`
/// being its first occurrence in `sql` from byte `from` on.
fn negative_zero(sql: &str, number: &str, path: &str, from: usize) -> String {
    let column = sql[from..].find(number).unwrap() + from + 1;
    negative_zero_refusal(path, &format!("1:{column}:{}", number.len()))
}

#[test]
fn a_negative_zero_decimal_is_refused_at_the_number_after_its_signs() {
    let insert = |value: &str| format!("INSERT INTO \"t\" VALUE {{'pk':'a','x':{value}}}");
    for (value, number, path) in [
        ("-0.0", "0.0", "root"),
        ("- 0.0", "0.0", "root"),
        ("- -0.0", "0.0", "root"),
        ("- - -0.0", "0.0", "root"),
        ("+-0.0", "0.0", "root"),
        ("-+0.0", "0.0", "root"),
        ("-00.0", "00.0", "root"),
        ("-0.00", "0.00", "root"),
        ("-.0", ".0", "root"),
        ("-0.", "0.", "root"),
        ("-0E-3", "0E-3", "root"),
        ("-0.0e0", "0.0e0", "root"),
        ("[-0.0]", "0.0", "root[0]"),
        ("{'y':-0.0}", "0.0", "root.y"),
        ("<<-0.0, 1>>", "0.0", "root"),
    ] {
        let sql = insert(value);
        let from = sql.find("'x'").unwrap();
        assert_eq!(err(&sql), negative_zero(&sql, number, path, from), "{sql}");
    }
    // A key attribute too: the parse refuses it ahead of the key type check.
    let sql = "INSERT INTO \"t\" VALUE {'pk':-0.0,'sk':'b'}";
    assert_eq!(err(sql), negative_zero(sql, "0.0", "root", 0));
    let update = |value: &str| format!("UPDATE \"t\" SET x = {value} WHERE pk = 'a'");
    for (value, number, path) in [
        ("-0e0", "0e0", "root"),
        ("-(0.0)", "0.0", "root"),
        ("(-0.0)", "0.0", "root"),
        ("-(-0.0)", "0.0", "root"),
        ("+(-0.0)", "0.0", "root"),
        ("-(+0.0)", "0.0", "root"),
        ("{'y': -0.0}", "0.0", "root.y"),
        ("[1, -0.0]", "0.0", "root[1]"),
        ("set_add(x, <<-0.0>>)", "0.0", "root"),
    ] {
        let sql = update(value);
        assert_eq!(err(&sql), negative_zero(&sql, number, path, 0), "{sql}");
    }
    let select = |condition: &str| format!("SELECT * FROM \"t\" WHERE pk = 'a' AND {condition}");
    for (condition, number, path) in [
        ("x = -0.0", "0.0", "root"),
        ("x = -0e0", "0e0", "root"),
        ("x = - -0.0", "0.0", "root"),
        ("x = -+0.0", "0.0", "root"),
        ("x IN [-0.0]", "0.0", "root"),
        ("x = [-0.0]", "0.0", "root[0]"),
    ] {
        let sql = select(condition);
        assert_eq!(err(&sql), negative_zero(&sql, number, path, 0), "{sql}");
    }
}

#[test]
fn a_zero_with_no_minus_sign_or_written_as_an_integer_is_taken() {
    for sql in [
        "INSERT INTO \"t\" VALUE {'pk':'a','x':-0,'y':+0.0}",
        "SELECT * FROM \"t\" WHERE pk = 'a' AND x = -0",
    ] {
        let stmt = parse(sql).unwrap();
        assert!(check_literal_numbers(&stmt).is_ok(), "{sql}");
    }
}

#[test]
fn a_negative_zero_decimal_comes_in_statement_order_with_the_number_rules() {
    let overflow = |path: &str| format!("{OVERFLOW_MESSAGE} under {path}");
    let insert = |value: &str| format!("INSERT INTO \"t\" VALUE {{'pk':'a',{value}}}");
    // In a value, the number written first is reported.
    assert_eq!(err(&insert("'x':1e126,'y':-0.0")), overflow("root"));
    let sql = insert("'x':-0.0,'y':1e126");
    assert_eq!(err(&sql), negative_zero(&sql, "0.0", "root", 0));
    // The members of a map in the order they are written, not by name.
    let sql = insert("'x':{'b':-0.0,'a':1e126}");
    assert_eq!(err(&sql), negative_zero(&sql, "0.0", "root.b", 0));
    assert_eq!(err(&insert("'x':{'y':1e126},'z':-0.0")), overflow("root.y"));
    // Of two zeros, the first.
    let sql = insert("'x':-0.0,'y':-0.0");
    assert_eq!(err(&sql), negative_zero(&sql, "0.0", "root", 0));
    let update =
        |sets: &str, condition: &str| format!("UPDATE \"t\" SET {sets} WHERE pk = 'a'{condition}");
    let sql = update("x = -0.0, y = 1e126", "");
    assert_eq!(err(&sql), negative_zero(&sql, "0.0", "root", 0));
    assert_eq!(err(&update("x = 1e126, y = -0.0", "")), overflow("root"));
    // The condition's numbers come first.
    assert_eq!(err(&update("x = -0.0", " AND a = 1e126")), overflow("root"));
    let select = |condition: &str| format!("SELECT * FROM \"t\" WHERE pk = 'a' AND {condition}");
    assert_eq!(err(&select("a = 1e126 AND b = -0.0")), overflow("root"));
    let sql = select("a = -0.0 AND b = 1e126");
    assert_eq!(err(&sql), negative_zero(&sql, "0.0", "root", 0));
}

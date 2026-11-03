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

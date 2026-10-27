// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use crate::error::DynamoDbError;

const MAX_SIGNIFICANT_DIGITS: usize = 38;
const MAX_EXPONENT: i32 = 125;
const MIN_EXPONENT: i32 = -130;

/// Validate and normalize a `DynamoDB` number string.
pub fn validate_and_normalize_number(s: &str) -> Result<String, DynamoDbError> {
    // DynamoDB does not trim: leading or trailing whitespace makes the number
    // malformed rather than acceptable (e.g. " 5" and "5 " are rejected). A
    // genuinely empty string is reported with a distinct message from a
    // malformed-but-non-empty one.
    if s.is_empty() {
        return Err(empty_numeric_err());
    }

    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };

    // DynamoDB reads the exponent as a signed 64-bit integer: one outside that
    // range is malformed (`1E9223372036854775808`), one inside it is range
    // checked below (`1E2147483648` is an overflow).
    let (mantissa_str, explicit_exp) = match rest.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i64>().map_err(|_| numeric_value_err(s))?),
        None => (rest, 0),
    };

    if mantissa_str.is_empty() || !mantissa_str.chars().all(|c| c.is_ascii_digit() || c == '.') {
        return Err(numeric_value_err(s));
    }

    // The mantissa must contain at least one digit. A bare "." (also "+.",
    // ".e5") passes the character check above since '.' is allowed, but has no
    // digits and is not a valid number.
    if !mantissa_str.bytes().any(|b| b.is_ascii_digit()) {
        return Err(numeric_value_err(s));
    }

    // Split into integer and fractional parts
    let (int_part, frac_part) = match mantissa_str.split_once('.') {
        Some((i, f)) => (if i.is_empty() { "0" } else { i }, f),
        None => (mantissa_str, ""),
    };

    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(numeric_value_err(s));
    }

    // Combine into a single digit string and track where the decimal point is.
    // The value is: <int_part>.<frac_part> * 10^explicit_exp
    // Which equals: <all_digits> * 10^(explicit_exp - frac_part.len())
    // The exponent arithmetic is done in i128: an i64 exponent plus or minus a
    // digit count cannot leave that range.
    let all_digits = format!("{int_part}{frac_part}");
    let point_offset = i128::from(explicit_exp) - digit_count(frac_part);

    // Strip leading zeros to get significant digits
    let sig_start = all_digits
        .find(|c: char| c != '0')
        .unwrap_or(all_digits.len());
    let significant = &all_digits[sig_start..];

    if significant.is_empty() {
        // DynamoDB range checks a zero on its exponent less its fraction
        // digits (`0E126` is an overflow, `0.0E-130` an underflow). Only a zero
        // whose exponent does not fit in an i32 is checked here, since those
        // were already refused: a zero refused here keeps its raw spelling in
        // `AttributeValue`, and only the PutItem item and UpdateItem's update
        // values are validated afterwards (#618), so the other operations, and
        // UpdateItem's key, would store and look up `0E126` as a key of its own.
        if i32::try_from(explicit_exp).is_err() {
            check_magnitude(point_offset)?;
        }
        return Ok("0".to_owned());
    }

    // Strip trailing zeros from significant digits
    let sig_end = significant.rfind(|c: char| c != '0').map_or(0, |i| i + 1);
    let sig_trimmed = &significant[..sig_end];

    if sig_trimmed.is_empty() {
        return Ok("0".to_owned());
    }

    if sig_trimmed.len() > MAX_SIGNIFICANT_DIGITS {
        return Err(too_many_digits_err());
    }

    // The exponent for the normalized form:
    // value = all_digits_as_integer * 10^point_offset
    //       = significant_as_integer * 10^point_offset  (leading zeros don't change value)
    //       = sig_trimmed_as_integer * 10^(point_offset + the trailing zeros of significant)
    let exp = point_offset + digit_count(&significant[sig_end..]);

    // Magnitude: the number is sig_trimmed * 10^exp, so its order of magnitude
    // is sig_trimmed.len() - 1 + exp
    check_magnitude(digit_count(sig_trimmed) - 1 + exp)?;

    // The magnitude is in range and `sig_trimmed` has at most 38 digits, so
    // `exp` is between MIN_EXPONENT - 37 and MAX_EXPONENT and fits in an i32.
    let exp = i32::try_from(exp).map_err(|_| overflow_err())?;

    // Format the normalized number
    // sig_trimmed represents an integer, and we multiply by 10^exp
    let result = format_plain(negative, sig_trimmed, exp);

    // Handle -0 case
    if negative && result == "-0" {
        return Ok("0".to_owned());
    }

    Ok(result)
}

/// The length of an ASCII digit string, as an i128 for the exponent arithmetic.
fn digit_count(digits: &str) -> i128 {
    // A `usize` always fits in an `i128`.
    i128::try_from(digits.len()).unwrap_or(i128::MAX)
}

/// Refuse an order of magnitude outside `DynamoDB`'s supported range.
fn check_magnitude(magnitude_exp: i128) -> Result<(), DynamoDbError> {
    if magnitude_exp > i128::from(MAX_EXPONENT) {
        return Err(overflow_err());
    }
    if magnitude_exp < i128::from(MIN_EXPONENT) {
        return Err(underflow_err());
    }
    Ok(())
}

/// Format digits * 10^exp as a plain decimal string (no scientific notation).
fn format_plain(negative: bool, digits: &str, exp: i32) -> String {
    let mut result = String::new();
    if negative {
        result.push('-');
    }

    let num_digits = digits.len() as i32;
    // decimal_pos: how many digits are to the left of the decimal point
    // value = digits * 10^exp, so decimal point is at position num_digits + exp from left
    let decimal_pos = num_digits + exp;

    if decimal_pos <= 0 {
        // 0.000...digits
        result.push_str("0.");
        for _ in 0..(-decimal_pos) {
            result.push('0');
        }
        result.push_str(digits);
    } else if decimal_pos >= num_digits {
        // Integer with trailing zeros
        result.push_str(digits);
        for _ in 0..(decimal_pos - num_digits) {
            result.push('0');
        }
    } else {
        let (left, right) = digits.split_at(decimal_pos as usize);
        result.push_str(left);
        result.push('.');
        result.push_str(right);
    }

    result
}

/// Empty number string, e.g. `{"N":""}`. Real `DynamoDB` reports the generic
/// numeric-conversion failure with no offending value appended (distinct from
/// the malformed-but-non-empty message, which appends the input).
fn empty_numeric_err() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "The parameter cannot be converted to a numeric value".to_owned(),
    )
}

/// More than 38 significant digits. Real `DynamoDB`:
/// "Attempting to store more than 38 significant digits in a Number".
fn too_many_digits_err() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "Attempting to store more than 38 significant digits in a Number".to_owned(),
    )
}

/// Magnitude above the supported exponent ceiling. Real `DynamoDB`:
/// "Number overflow. Attempting to store a number with magnitude larger than
/// supported range".
fn overflow_err() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "Number overflow. Attempting to store a number with magnitude larger than supported range"
            .to_owned(),
    )
}

/// Magnitude below the supported exponent floor. Real `DynamoDB`:
/// "Number underflow. Attempting to store a number with magnitude smaller than
/// supported range".
fn underflow_err() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "Number underflow. Attempting to store a number with magnitude smaller than supported range"
            .to_owned(),
    )
}

/// Error for a non-empty but malformed number string. Mirrors real DynamoDB,
/// which reports `The parameter cannot be converted to a numeric value: <input>`
/// for syntactically invalid numbers, distinct from the empty-value message.
fn numeric_value_err(s: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!(
        "The parameter cannot be converted to a numeric value: {s}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_leading_zeros() {
        assert_eq!(validate_and_normalize_number("00042").unwrap(), "42");
    }

    #[test]
    fn normalizes_trailing_decimal_zeros() {
        assert_eq!(validate_and_normalize_number("1.0").unwrap(), "1");
        assert_eq!(validate_and_normalize_number("1.50").unwrap(), "1.5");
    }

    #[test]
    fn normalizes_negative_zero() {
        assert_eq!(validate_and_normalize_number("-0").unwrap(), "0");
        assert_eq!(validate_and_normalize_number("-0.0").unwrap(), "0");
    }

    #[test]
    fn normalizes_scientific_notation() {
        assert_eq!(validate_and_normalize_number("1.5E2").unwrap(), "150");
        assert_eq!(validate_and_normalize_number("1.5e2").unwrap(), "150");
        assert_eq!(validate_and_normalize_number("42E0").unwrap(), "42");
    }

    #[test]
    fn rejects_39_significant_digits() {
        let n = "1".repeat(39);
        let err = validate_and_normalize_number(&n).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Attempting to store more than 38 significant digits in a Number"
        );
    }

    #[test]
    fn accepts_38_significant_digits() {
        let n = "1".repeat(38);
        assert!(validate_and_normalize_number(&n).is_ok());
    }

    #[test]
    fn rejects_over_max_positive() {
        let err = validate_and_normalize_number("1E126").unwrap_err();
        assert_eq!(
            err.to_string(),
            "Number overflow. Attempting to store a number with magnitude larger than supported range"
        );
    }

    #[test]
    fn accepts_max_positive() {
        assert!(validate_and_normalize_number("9.9E125").is_ok());
    }

    #[test]
    fn rejects_below_min_positive() {
        let err = validate_and_normalize_number("1E-131").unwrap_err();
        assert_eq!(
            err.to_string(),
            "Number underflow. Attempting to store a number with magnitude smaller than supported range"
        );
    }

    #[test]
    fn accepts_min_positive() {
        assert!(validate_and_normalize_number("1E-130").is_ok());
    }

    #[test]
    fn zero_is_valid() {
        assert_eq!(validate_and_normalize_number("0").unwrap(), "0");
    }

    #[test]
    fn simple_integers() {
        assert_eq!(validate_and_normalize_number("42").unwrap(), "42");
        assert_eq!(validate_and_normalize_number("-7").unwrap(), "-7");
    }

    #[test]
    fn simple_decimals() {
        assert_eq!(validate_and_normalize_number("3.14").unwrap(), "3.14");
        assert_eq!(validate_and_normalize_number("0.5").unwrap(), "0.5");
    }

    #[test]
    fn small_decimals() {
        assert_eq!(validate_and_normalize_number("0.001").unwrap(), "0.001");
    }

    #[test]
    fn large_integer() {
        assert_eq!(validate_and_normalize_number("1000").unwrap(), "1000");
    }

    #[test]
    fn negative_decimal() {
        assert_eq!(validate_and_normalize_number("-0.5").unwrap(), "-0.5");
    }

    #[test]
    fn rejects_surrounding_whitespace() {
        // DynamoDB does not trim; whitespace makes the value malformed.
        assert!(validate_and_normalize_number(" 5").is_err());
        assert!(validate_and_normalize_number("5 ").is_err());
        assert!(validate_and_normalize_number("1 5").is_err());
    }

    #[test]
    fn rejects_bare_dot() {
        assert!(validate_and_normalize_number(".").is_err());
        assert!(validate_and_normalize_number("+.").is_err());
    }

    #[test]
    fn malformed_uses_numeric_value_message() {
        for bad in [
            "+e2", "1+2", "1.2.3", "0x5", "NaN", "Infinity", "1_000", "1e",
        ] {
            let err = validate_and_normalize_number(bad).unwrap_err();
            assert!(
                err.to_string().contains("numeric value"),
                "expected numeric-value message for {bad:?}, got: {err}"
            );
        }
    }

    #[test]
    fn empty_uses_numeric_value_message() {
        // Real DynamoDB reports an empty number string ({"N":""}) as a
        // numeric-conversion failure with no value appended, distinct from the
        // empty-AttributeValue message.
        let err = validate_and_normalize_number("").unwrap_err();
        assert_eq!(
            err.to_string(),
            "The parameter cannot be converted to a numeric value"
        );
    }

    // Every value in these tests was sent to DynamoDB's PutItem twice on
    // 2026-10-27, with the same answer both times. Each test expects
    // DynamoDB's answer, except the last, which says why.
    const OVERFLOW: &str =
        "Number overflow. Attempting to store a number with magnitude larger than supported range";
    const UNDERFLOW: &str = "Number underflow. Attempting to store a number with magnitude smaller than supported range";

    fn assert_refused(input: &str, want: &str) {
        match validate_and_normalize_number(input) {
            Ok(n) => panic!("{input:?} accepted as {n:?}, want {want:?}"),
            Err(e) => assert_eq!(e.to_string(), want, "{input:?}"),
        }
    }

    #[test]
    fn magnitude_counts_every_significant_digit() {
        for n in [
            "12E124", "99E124", "10E124", "100E123", "1.2E-130", "10E-131",
        ] {
            assert!(validate_and_normalize_number(n).is_ok(), "{n:?}");
        }
        for n in ["12E125", "10E125"] {
            assert_refused(n, OVERFLOW);
        }
        for n in ["1.2E-131", "10E-132"] {
            assert_refused(n, UNDERFLOW);
        }
    }

    #[test]
    fn exponent_near_i32_limits_is_an_overflow_or_underflow() {
        for n in [
            "10E2147483647",
            "100E2147483647",
            "12E2147483647",
            "-10E2147483647",
            "1E+2147483647",
            "0.1E2147483647",
            "1E2147483647",
        ] {
            assert_refused(n, OVERFLOW);
        }
        for n in [
            "1.5E-2147483648",
            "-1.5E-2147483648",
            "0.01E-2147483648",
            "1E-2147483648",
        ] {
            assert_refused(n, UNDERFLOW);
        }
    }

    #[test]
    fn exponent_is_read_as_i64() {
        for n in [
            "1E2147483648",
            "1E4294967296",
            "1E9223372036854775807",
            "10E9223372036854775807",
        ] {
            assert_refused(n, OVERFLOW);
        }
        for n in [
            "1E-2147483649",
            "1E-9223372036854775808",
            "0.1E-9223372036854775808",
        ] {
            assert_refused(n, UNDERFLOW);
        }
        for n in [
            "1E9223372036854775808",
            "1E-9223372036854775809",
            "1E99999999999999999999",
        ] {
            assert_refused(
                n,
                &format!("The parameter cannot be converted to a numeric value: {n}"),
            );
        }
    }

    #[test]
    fn zero_with_exponent_outside_i32_is_range_checked() {
        // DynamoDB accepts each of these as 0.
        for n in [
            "0E125",
            "0.0E126",
            "000.0E126",
            "-0.0E126",
            "0E-130",
            "0.0E-129",
            "0E0",
            "0.0",
        ] {
            assert_eq!(validate_and_normalize_number(n).unwrap(), "0", "{n:?}");
        }
        // A zero whose exponent does not fit in an i32. DynamoDB also refuses a
        // zero such as `0E126` whose exponent fits; see the comment in
        // `validate_and_normalize_number` for why those are not checked.
        for n in ["0E2147483648", "0E9223372036854775806"] {
            assert_refused(n, OVERFLOW);
        }
        for n in ["0E-9223372036854775807", "0E-9223372036854775808"] {
            assert_refused(n, UNDERFLOW);
        }
    }

    // DynamoDB refuses these zeros (`0E126` overflow, `0.0E-130` underflow).
    // They stay accepted as 0 until every operation validates numbers (#618):
    // refusing them here would make BatchWriteItem and the other operations
    // that skip validation store and look up `0E126` as a key of its own.
    // Remove this test and add the rows to the one above when #618 is fixed.
    #[test]
    fn zero_with_i32_exponent_stays_accepted_until_618() {
        for n in [
            "0E126",
            "00E126",
            "-0E126",
            "0E2147483647",
            "00.000E2147483647",
            "0.0E-130",
            "0E-131",
            "-0E-131",
            "0.00E-131",
            "0E-2147483648",
            "0.0E-2147483648",
        ] {
            assert_eq!(validate_and_normalize_number(n).unwrap(), "0", "{n:?}");
        }
    }
}

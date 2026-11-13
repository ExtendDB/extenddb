// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use crate::error::DynamoDbError;

pub mod operations;

const MAX_SIGNIFICANT_DIGITS: usize = 38;
const MAX_EXPONENT: i32 = 125;
const MIN_EXPONENT: i32 = -130;

/// The largest and smallest magnitude, the precision less the scale, that
/// `DynamoDB` accepts: one more than the largest and smallest order of
/// magnitude, so 126 for 9.99E125 and -129 for 1E-130.
pub(super) const MAX_MAGNITUDE: i64 = MAX_EXPONENT as i64 + 1;
pub(super) const MIN_MAGNITUDE: i64 = MIN_EXPONENT as i64 + 1;

/// The service's text for a value that is not a number. A malformed value
/// is named after a colon; an empty one is not.
const NOT_A_NUMBER: &str = "The parameter cannot be converted to a numeric value";

/// Why a string is not a number `DynamoDB` can store, as
/// [`normalize_number`] reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberFault {
    /// An empty string, e.g. `{"N":""}`.
    Empty,
    /// A string that is not a number.
    Malformed,
    /// A magnitude above the supported range.
    Overflow,
    /// A magnitude below the supported range.
    Underflow,
    /// More than 38 significant digits.
    TooManyDigits,
}

impl NumberFault {
    /// Whether the string is not a number at all, where the other faults are
    /// numbers out of range or with too many digits.
    #[must_use]
    pub fn is_not_a_number(self) -> bool {
        matches!(self, Self::Empty | Self::Malformed)
    }

    /// The service's message for this fault, without the offending value.
    /// `DynamoDB` names the value only for a malformed one; see
    /// [`NumberFault::message`].
    #[must_use]
    pub fn text(self) -> &'static str {
        match self {
            Self::Empty | Self::Malformed => NOT_A_NUMBER,
            Self::Overflow => {
                "Number overflow. Attempting to store a number with magnitude larger than supported range"
            }
            Self::Underflow => {
                "Number underflow. Attempting to store a number with magnitude smaller than supported range"
            }
            Self::TooManyDigits => {
                "Attempting to store more than 38 significant digits in a Number"
            }
        }
    }

    /// The service's message for this fault in the number `s`: a malformed
    /// value is appended after a colon, as `DynamoDB` reports it, and the
    /// other faults carry no value.
    #[must_use]
    pub fn message(self, s: &str) -> String {
        match self {
            Self::Malformed => format!("{NOT_A_NUMBER}: {s}"),
            _ => self.text().to_owned(),
        }
    }

    /// The fault in the number `s` as the `ValidationException` the service
    /// answers.
    #[must_use]
    pub fn error(self, s: &str) -> DynamoDbError {
        DynamoDbError::ValidationException(self.message(s))
    }
}

/// Validate and normalize a `DynamoDB` number string.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` with the service's message
/// for the number's [`NumberFault`].
pub fn validate_and_normalize_number(s: &str) -> Result<String, DynamoDbError> {
    normalize_number(s).map_err(|fault| fault.error(s))
}

/// Validate and normalize a `DynamoDB` number string, with the fault as a
/// [`NumberFault`] for a caller that acts on its kind.
///
/// # Errors
///
/// Returns the [`NumberFault`] of the first rule `s` breaks.
pub fn normalize_number(s: &str) -> Result<String, NumberFault> {
    // DynamoDB does not trim: leading or trailing whitespace makes the number
    // malformed rather than acceptable (e.g. " 5" and "5 " are rejected). A
    // genuinely empty string is reported with a distinct message from a
    // malformed-but-non-empty one.
    if s.is_empty() {
        return Err(NumberFault::Empty);
    }

    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };

    // DynamoDB reads the exponent as a signed 64-bit integer: one outside that
    // range is malformed (`1E9223372036854775808`), one inside it is range
    // checked below (`1E2147483648` is an overflow).
    let (mantissa_str, explicit_exp) = match rest.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i64>().map_err(|_| NumberFault::Malformed)?),
        None => (rest, 0),
    };

    if mantissa_str.is_empty() || !mantissa_str.chars().all(|c| c.is_ascii_digit() || c == '.') {
        return Err(NumberFault::Malformed);
    }

    // The mantissa must contain at least one digit. A bare "." (also "+.",
    // ".e5") passes the character check above since '.' is allowed, but has no
    // digits and is not a valid number.
    if !mantissa_str.bytes().any(|b| b.is_ascii_digit()) {
        return Err(NumberFault::Malformed);
    }

    // Split into integer and fractional parts
    let (int_part, frac_part) = match mantissa_str.split_once('.') {
        Some((i, f)) => (if i.is_empty() { "0" } else { i }, f),
        None => (mantissa_str, ""),
    };

    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(NumberFault::Malformed);
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
        // whose exponent does not fit in an i32 is checked here. A refused
        // zero keeps its raw spelling in `AttributeValue`, as every refused
        // number does, and Query and Scan (#544, #562) do not validate
        // numbers yet: refusing `0E126` here would make them look it up as a
        // key of its own instead of `0` (#618).
        if i32::try_from(explicit_exp).is_err() {
            // The exponent of the normalized form `0.d * 10^n` is the point
            // offset plus one, and DynamoDB computes it in 64 bits, where
            // i64::MAX + 1 wraps to a negative exponent: `0E9223372036854775807`
            // is an underflow, while `0E9223372036854775806` and
            // `0.0E9223372036854775807` are overflows.
            if normalized_exponent_exceeds_i64(point_offset) {
                return Err(NumberFault::Underflow);
            }
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

    // The exponent for the normalized form:
    // value = all_digits_as_integer * 10^point_offset
    //       = significant_as_integer * 10^point_offset  (leading zeros don't change value)
    //       = sig_trimmed_as_integer * 10^(point_offset + the trailing zeros of significant)
    let exp = point_offset + digit_count(&significant[sig_end..]);

    // Magnitude: the number is sig_trimmed * 10^exp, so its order of magnitude
    // is sig_trimmed.len() - 1 + exp
    let magnitude_exp = digit_count(sig_trimmed) - 1 + exp;

    // DynamoDB reports an overflow before it counts the digits when the
    // exponent of the normalized form, the magnitude plus one, is above
    // i64::MAX. Measured with `1234567890123456789012345678901234567890` as
    // the digits: `E9223372036854775768` (an order of magnitude of i64::MAX),
    // `E9223372036854775769` (i64::MAX + 1) and `E9223372036854775807`
    // (i64::MAX + 39) are overflows, and `E9223372036854775767` is too many
    // digits. The one-digit `1E9223372036854775807` (i64::MAX) and
    // `10E9223372036854775807` (i64::MAX + 1) are overflows as well.
    if normalized_exponent_exceeds_i64(magnitude_exp) {
        return Err(NumberFault::Overflow);
    }

    // The small side mirrors it: an underflow is reported before the digits
    // are counted when the exponent of the normalized form is below i64::MIN.
    // Measured with the forty digits `1234567890123456789012345678901234567891`
    // after the point, where each fraction zero before them lowers that
    // exponent by one: `.0<digits>E-9223372036854775808` and
    // `.00<digits>E-9223372036854775807` (i64::MIN - 1),
    // `.00<digits>E-9223372036854775808` (i64::MIN - 2) and the same with
    // forty zeros (i64::MIN - 40) are underflows, while
    // `.<digits>E-9223372036854775808`, `.0<digits>E-9223372036854775807` and
    // `.00<digits>E-9223372036854775806` (i64::MIN) are too many digits, as is
    // `<digits>E-9223372036854775808` (i64::MIN + 40). The answers are the
    // same with 39 digits and with a minus sign.
    if normalized_exponent_is_below_i64(magnitude_exp) {
        return Err(NumberFault::Underflow);
    }

    if sig_trimmed.len() > MAX_SIGNIFICANT_DIGITS {
        return Err(NumberFault::TooManyDigits);
    }

    check_magnitude(magnitude_exp)?;

    // The magnitude is in range and `sig_trimmed` has at most 38 digits, so
    // `exp` is between MIN_EXPONENT - 37 and MAX_EXPONENT and fits in an i32.
    let exp = i32::try_from(exp).map_err(|_| NumberFault::Overflow)?;

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

/// Whether the exponent of the normalized form `0.d * 10^n`, which is the
/// given order of magnitude plus one, is above `i64::MAX`. `DynamoDB` answers
/// such a nonzero value with an overflow before it counts the digits, and such
/// a zero with an underflow. Measured with forty digits at orders of magnitude
/// of `i64::MAX`, `i64::MAX + 1` and `i64::MAX + 39`, with one and two digits
/// at `i64::MAX` and `i64::MAX + 1`, and with the zero `0E9223372036854775807`.
fn normalized_exponent_exceeds_i64(magnitude_exp: i128) -> bool {
    magnitude_exp + 1 > i128::from(i64::MAX)
}

/// Whether the exponent of the normalized form `0.d * 10^n`, which is the
/// given order of magnitude plus one, is below `i64::MIN`. `DynamoDB` answers
/// such a nonzero value with an underflow before it counts the digits.
/// Measured with forty digits, with 39 digits and with a minus sign at
/// normalized exponents of `i64::MIN - 1`, `i64::MIN - 2` and `i64::MIN - 40`,
/// against `i64::MIN` and `i64::MIN + 40`, which get the digit count. A zero
/// takes the other path in the caller: its exponent is range checked, and a
/// zero this far down is an underflow there as well.
fn normalized_exponent_is_below_i64(magnitude_exp: i128) -> bool {
    magnitude_exp + 1 < i128::from(i64::MIN)
}

/// Refuse an order of magnitude outside `DynamoDB`'s supported range.
fn check_magnitude(magnitude_exp: i128) -> Result<(), NumberFault> {
    if magnitude_exp > i128::from(MAX_EXPONENT) {
        return Err(NumberFault::Overflow);
    }
    if magnitude_exp < i128::from(MIN_EXPONENT) {
        return Err(NumberFault::Underflow);
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

    /// An exponent at either end of `i32` combined with the digits: the
    /// magnitude is computed without overflow (a debug build panicked on
    /// each of these, and a release build wrapped and reported overflow as
    /// underflow and the reverse).
    #[test]
    fn exponents_at_the_ends_of_i32_classify_the_magnitude() {
        let overflow = "Number overflow. Attempting to store a number with magnitude larger than supported range";
        let underflow = "Number underflow. Attempting to store a number with magnitude smaller than supported range";
        for (input, expected) in [
            ("10E2147483647", overflow),
            ("49E2147483647", overflow),
            ("1E2147483647", overflow),
            ("-10E2147483647", overflow),
            ("0.1E-2147483648", underflow),
            ("-.80e-2147483648", underflow),
            ("1E-2147483648", underflow),
            ("1.5E-2147483648", underflow),
        ] {
            let err = validate_and_normalize_number(input).unwrap_err();
            assert_eq!(err.to_string(), expected, "{input}");
        }
        for zero in [".00E-2147483647", "0.0E-2147483648", "00.000E2147483647"] {
            assert_eq!(validate_and_normalize_number(zero).unwrap(), "0", "{zero}");
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

    #[test]
    fn normalize_number_names_the_fault() {
        // The kind of fault, which callers act on, and the message each kind
        // gives, which is the text `validate_and_normalize_number` reports.
        for (input, fault, message) in [
            (
                "",
                NumberFault::Empty,
                "The parameter cannot be converted to a numeric value",
            ),
            (
                "abc",
                NumberFault::Malformed,
                "The parameter cannot be converted to a numeric value: abc",
            ),
            (
                "1E126",
                NumberFault::Overflow,
                "Number overflow. Attempting to store a number with magnitude larger than supported range",
            ),
            (
                "1E-131",
                NumberFault::Underflow,
                "Number underflow. Attempting to store a number with magnitude smaller than supported range",
            ),
            (
                "1234567890123456789012345678901234567890",
                NumberFault::TooManyDigits,
                "Attempting to store more than 38 significant digits in a Number",
            ),
        ] {
            assert_eq!(normalize_number(input), Err(fault), "{input:?}");
            assert_eq!(fault.message(input), message, "{input:?}");
            assert_eq!(
                validate_and_normalize_number(input)
                    .unwrap_err()
                    .to_string(),
                message,
                "{input:?}"
            );
            assert_eq!(
                fault.is_not_a_number(),
                message.starts_with("The parameter")
            );
        }
        assert_eq!(normalize_number("1.50E2").unwrap(), "150");
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
        for n in [
            "0E-9223372036854775807",
            "0E-9223372036854775808",
            // Fraction digits lower the exponent further below the 64-bit
            // minimum, and the zero stays an underflow (sent to GetItem on
            // 2026-10-27, twice each).
            "0.00E-9223372036854775808",
            "0.000E-9223372036854775808",
        ] {
            assert_refused(n, UNDERFLOW);
        }
    }

    // DynamoDB refuses each of these zeros: `0E126` and the next four are a
    // `Number overflow`, `0.0E-130` and the rest a `Number underflow`. They
    // stay accepted as 0 until Query and Scan (#544, #562) validate numbers,
    // which #618 still tracks: a zero refused by this function keeps its raw
    // spelling, and those operations would then look up `0E126` as a key of
    // its own. Move these rows to the refused lists above when they do.
    #[test]
    fn zero_with_i32_exponent_stays_accepted_until_query_and_scan_validate() {
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

    // DynamoDB's answers to the values in the next three tests were measured
    // with PutItem in us-east-1 on 2026-11-09, each value sent twice with the
    // same answer both times, except the values marked with another date.
    const TOO_MANY_DIGITS: &str = "Attempting to store more than 38 significant digits in a Number";

    #[test]
    fn zero_with_exponent_at_i64_max_is_an_underflow() {
        // The exponent of a zero's normalized form is its point offset plus
        // one, computed in 64 bits: at i64::MAX the sum wraps to a negative
        // exponent. A fraction digit lowers the point offset before the sum.
        assert_refused("0E9223372036854775807", UNDERFLOW);
        // `0E9223372036854775806` was measured on 2026-10-27.
        for n in ["0E9223372036854775806", "0.0E9223372036854775807"] {
            assert_refused(n, OVERFLOW);
        }
    }

    #[test]
    fn magnitude_at_i64_max_is_an_overflow_before_the_digit_count() {
        // Forty digits with the order of magnitude at i64::MAX: the overflow
        // is reported before the digits are counted. With the exponent one
        // lower, the digit count is reported.
        assert_refused(
            "1234567890123456789012345678901234567890E9223372036854775768",
            OVERFLOW,
        );
        assert_refused(
            "1234567890123456789012345678901234567890E9223372036854775767",
            TOO_MANY_DIGITS,
        );
        // The same digits with the order of magnitude at i64::MAX + 1 and at
        // i64::MAX + 39. Measured on 2026-11-10, each value sent twice with
        // the same answer both times.
        for n in [
            "1234567890123456789012345678901234567890E9223372036854775769",
            "1234567890123456789012345678901234567890E9223372036854775807",
        ] {
            assert_refused(n, OVERFLOW);
        }
    }

    #[test]
    fn magnitude_below_i64_min_is_an_underflow_before_the_digit_count() {
        // More than 38 digits with the exponent of the normalized form
        // `0.d * 10^n` below i64::MIN: the underflow is reported before the
        // digits are counted. Each fraction zero before the digits lowers
        // that exponent by one. Under the smallest exponent, one zero puts
        // forty digits at i64::MIN - 1, two at i64::MIN - 2 and forty at
        // i64::MIN - 40; under the next exponent, two zeros reach i64::MIN - 1
        // as well. The answer is the same with 39 digits and with a minus
        // sign. Measured on 2026-11-10, each value sent twice with the same
        // answer both times.
        let forty_zeros = "0".repeat(40);
        let forty_below =
            format!(".{forty_zeros}1234567890123456789012345678901234567891E-9223372036854775808");
        for n in [
            ".01234567890123456789012345678901234567891E-9223372036854775808",
            ".001234567890123456789012345678901234567891E-9223372036854775807",
            ".001234567890123456789012345678901234567891E-9223372036854775808",
            forty_below.as_str(),
            ".0123456789012345678901234567890123456789E-9223372036854775808",
            "-.01234567890123456789012345678901234567891E-9223372036854775808",
        ] {
            assert_refused(n, UNDERFLOW);
        }
        // At i64::MIN, reached with no fraction zero under the smallest
        // exponent, one under the next and two under the one after, and at
        // i64::MIN + 40, with the digits before the point, the digit count is
        // reported. The same holds with 39 digits and with a minus sign.
        for n in [
            ".1234567890123456789012345678901234567891E-9223372036854775808",
            ".01234567890123456789012345678901234567891E-9223372036854775807",
            ".001234567890123456789012345678901234567891E-9223372036854775806",
            "1234567890123456789012345678901234567891E-9223372036854775808",
            ".123456789012345678901234567890123456789E-9223372036854775808",
            "-.1234567890123456789012345678901234567891E-9223372036854775808",
        ] {
            assert_refused(n, TOO_MANY_DIGITS);
        }
    }
}

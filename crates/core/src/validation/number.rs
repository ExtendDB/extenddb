// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use crate::error::DynamoDbError;

const MAX_SIGNIFICANT_DIGITS: usize = 38;
const MAX_EXPONENT: i32 = 125;
const MIN_EXPONENT: i32 = -130;

/// Validate and normalize a `DynamoDB` number string.
pub fn validate_and_normalize_number(s: &str) -> Result<String, DynamoDbError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(number_err());
    }

    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };

    let (mantissa_str, explicit_exp) = match rest.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i32>().map_err(|_| number_err())?),
        None => (rest, 0),
    };

    if mantissa_str.is_empty() || !mantissa_str.chars().all(|c| c.is_ascii_digit() || c == '.') {
        return Err(number_err());
    }

    // Split into integer and fractional parts
    let (int_part, frac_part) = match mantissa_str.split_once('.') {
        Some((i, f)) => (if i.is_empty() { "0" } else { i }, f),
        None => (mantissa_str, ""),
    };

    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(number_err());
    }

    // Combine into a single digit string and track where the decimal point is.
    // The value is: <int_part>.<frac_part> * 10^explicit_exp
    // Which equals: <all_digits> * 10^(explicit_exp - frac_part.len())
    let all_digits = format!("{int_part}{frac_part}");
    let point_offset = explicit_exp - frac_part.len() as i32;

    // Strip leading zeros to get significant digits
    let sig_start = all_digits
        .find(|c: char| c != '0')
        .unwrap_or(all_digits.len());
    let significant = &all_digits[sig_start..];

    if significant.is_empty() {
        return Ok("0".to_owned());
    }

    // Strip trailing zeros from significant digits
    let sig_end = significant.rfind(|c: char| c != '0').map_or(0, |i| i + 1);
    let sig_trimmed = &significant[..sig_end];

    if sig_trimmed.is_empty() {
        return Ok("0".to_owned());
    }

    if sig_trimmed.len() > MAX_SIGNIFICANT_DIGITS {
        return Err(number_err());
    }

    // The exponent for the normalized form:
    // value = all_digits_as_integer * 10^point_offset
    //       = significant_as_integer * 10^point_offset  (leading zeros don't change value)
    //       = sig_trimmed_as_integer * 10^(point_offset + trailing_zeros)
    let trailing_zeros = significant.len() - sig_end;
    let exp = point_offset + trailing_zeros as i32;

    // Magnitude: the number is sig_trimmed * 10^exp, so its order of magnitude
    // is sig_trimmed.len() - 1 + exp
    let magnitude_exp = sig_trimmed.len() as i32 - 1 + point_offset + trailing_zeros as i32;

    if magnitude_exp > MAX_EXPONENT {
        return Err(number_err());
    }
    if magnitude_exp < MIN_EXPONENT {
        return Err(number_err());
    }

    // Format the normalized number
    // sig_trimmed represents an integer, and we multiply by 10^exp
    let result = format_plain(negative, sig_trimmed, exp);

    // Handle -0 case
    if negative && result == "-0" {
        return Ok("0".to_owned());
    }

    Ok(result)
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

fn number_err() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "Supplied AttributeValue is empty, must contain exactly one of the supported datatypes"
            .to_owned(),
    )
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
        assert!(validate_and_normalize_number(&n).is_err());
    }

    #[test]
    fn accepts_38_significant_digits() {
        let n = "1".repeat(38);
        assert!(validate_and_normalize_number(&n).is_ok());
    }

    #[test]
    fn rejects_over_max_positive() {
        assert!(validate_and_normalize_number("1E126").is_err());
    }

    #[test]
    fn accepts_max_positive() {
        assert!(validate_and_normalize_number("9.9E125").is_ok());
    }

    #[test]
    fn rejects_below_min_positive() {
        assert!(validate_and_normalize_number("1E-131").is_err());
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
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Order-preserving encoding of DynamoDB numbers for use as SQLite sort keys.
//!
//! SQLite has no arbitrary-precision numeric type, and storing a `N` sort key
//! as `REAL` would truncate beyond ~15-17 significant digits, corrupting both
//! ordering and equality. Instead we store `N` sort keys in a `TEXT` column
//! holding a canonical, byte-lexicographically order-preserving encoding of the
//! value, so SQLite's default BINARY collation yields exact DynamoDB numeric
//! ordering. The full-precision value is always retained in the item JSON, so
//! reads lose nothing — this column exists only for ordering/range/equality.
//!
//! # Encoding
//!
//! A value is written in canonical normalized form `± 0.d₁d₂… × 10^E` (leading
//! digit non-zero, no trailing zeros), then encoded as:
//!
//! - **class byte** — `'0'` negative, `'1'` zero, `'2'` positive — so the three
//!   classes order negative < zero < positive.
//! - **exponent** — `E` biased into a fixed-width 6-digit field. Positive
//!   numbers use `E + BIAS` (increasing in `E`); negative numbers use
//!   `BIAS - E` (decreasing in `E`) so larger magnitudes sort earlier.
//! - **mantissa digits** — written verbatim for positives. For negatives each
//!   digit `d` is complemented to `9 - d`, and a high terminator (`':'`, which
//!   sorts above any digit) is appended so a longer (more negative) value sorts
//!   before its prefix.
//!
//! Canonical normalization makes numerically-equal inputs (`5`, `5.0`, `+5`)
//! encode identically, matching DynamoDB's number normalization for key
//! equality.

use bigdecimal::{BigDecimal, Zero};

/// Exponent bias. Comfortably covers DynamoDB's exponent range (≈ -130..=126)
/// within a fixed 6-digit field with wide margin.
const EXP_BIAS: i64 = 100_000;
const EXP_WIDTH: usize = 6;
/// Terminator that sorts above any decimal digit (`'9'` = 0x39 < `':'` = 0x3A).
const NEG_TERMINATOR: char = ':';

/// Encode a `BigDecimal` into an order-preserving ASCII string such that, for
/// all `a`, `b`: `a.cmp(b) == encode(a).cmp(&encode(b))`.
///
/// Precondition: `value` is a finite decimal within DynamoDB's supported numeric
/// range. `BigDecimal` cannot represent NaN or ±Infinity, and the protocol /
/// validation layer rejects non-numeric, NaN, and Infinity inputs before they
/// reach this encoder, so those cases are not handled here.
pub(crate) fn encode_orderable_number(value: &BigDecimal) -> String {
    if value.is_zero() {
        return "1".to_owned();
    }

    let negative = value < &BigDecimal::zero();
    let magnitude = value.abs().normalized();

    // value = mantissa_int * 10^(-scale); with no trailing zeros (normalized)
    // and no leading zeros (decimal string), the normalized form is
    // 0.<digits> × 10^E where E = digit_count - scale.
    let (mantissa_int, scale) = magnitude.as_bigint_and_exponent();
    let digits = mantissa_int.to_string();
    let exponent = digits.len() as i64 - scale;

    if negative {
        let exp_code = format!("{:0width$}", EXP_BIAS - exponent, width = EXP_WIDTH);
        let complemented: String = digits
            .bytes()
            .map(|b| char::from(b'9' - (b - b'0')))
            .collect();
        format!("0{exp_code}{complemented}{NEG_TERMINATOR}")
    } else {
        let exp_code = format!("{:0width$}", EXP_BIAS + exponent, width = EXP_WIDTH);
        format!("2{exp_code}{digits}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use std::str::FromStr;

    fn bd(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).expect("valid decimal")
    }

    fn enc(s: &str) -> String {
        encode_orderable_number(&bd(s))
    }

    /// Core property: encoded byte order equals numeric order, for every pair.
    fn assert_order_preserving(values: &[&str]) {
        for a in values {
            for b in values {
                let numeric = bd(a).cmp(&bd(b));
                let encoded = enc(a).cmp(&enc(b));
                assert_eq!(
                    numeric,
                    encoded,
                    "order mismatch for {a} vs {b}: numeric={numeric:?} encoded={encoded:?} \
                     (enc(a)={:?}, enc(b)={:?})",
                    enc(a),
                    enc(b),
                );
            }
        }
    }

    #[test]
    fn ascending_sequence_spanning_sign_and_scale() {
        // Deliberately spans negatives, zero, fractions, integers, and large
        // and small magnitudes, listed in true numeric ascending order.
        let ordered = [
            "-1000000000000000000000000000000",
            "-100",
            "-10.5",
            "-10",
            "-1.0001",
            "-1",
            "-0.5",
            "-0.05",
            "-0.0000001",
            "0",
            "0.0000001",
            "0.05",
            "0.5",
            "1",
            "1.0001",
            "10",
            "10.5",
            "100",
            "1000000000000000000000000000000",
        ];
        // Each strictly less than the next.
        for w in ordered.windows(2) {
            assert!(
                enc(w[0]) < enc(w[1]),
                "expected enc({}) < enc({}) but got {:?} >= {:?}",
                w[0],
                w[1],
                enc(w[0]),
                enc(w[1]),
            );
        }
        assert_order_preserving(&ordered);
    }

    #[test]
    fn canonical_equality_for_equal_values() {
        // Numerically equal values must encode identically (DynamoDB number
        // normalization), so they compare Equal as sort keys.
        for (a, b) in [
            ("5", "5.0"),
            ("5", "5.00"),
            ("5", "+5"),
            ("0", "0.0"),
            ("0", "-0"),
            ("-7.5", "-7.50"),
            ("100", "1E2"),
            ("0.1", "1E-1"),
            ("12300", "1.23E4"),
        ] {
            assert_eq!(enc(a), enc(b), "{a} and {b} should encode identically");
            assert_eq!(bd(a).cmp(&bd(b)), Ordering::Equal);
        }
    }

    #[test]
    fn full_precision_38_digit_extremes() {
        // 38 significant digits differing only in the last digit must order
        // correctly — the case that REAL/f64 silently corrupts.
        let lo = "1.2345678901234567890123456789012345678";
        let hi = "1.2345678901234567890123456789012345679";
        assert!(bd(lo) < bd(hi));
        assert!(enc(lo) < enc(hi));

        let neg_lo = "-1.2345678901234567890123456789012345679";
        let neg_hi = "-1.2345678901234567890123456789012345678";
        assert!(bd(neg_lo) < bd(neg_hi));
        assert!(enc(neg_lo) < enc(neg_hi));

        assert_order_preserving(&[lo, hi, neg_lo, neg_hi, "0"]);
    }

    #[test]
    fn dynamodb_magnitude_bounds() {
        // Near DynamoDB's documented positive/negative magnitude limits.
        let vals = [
            "-9.9999999999999999999999999999999999999E+125",
            "-1E-130",
            "0",
            "1E-130",
            "9.9999999999999999999999999999999999999E+125",
        ];
        for w in vals.windows(2) {
            assert!(bd(w[0]) < bd(w[1]));
            assert!(
                enc(w[0]) < enc(w[1]),
                "enc order failed at {} vs {}",
                w[0],
                w[1]
            );
        }
        assert_order_preserving(&vals);
    }

    #[test]
    fn same_exponent_varying_mantissa_length() {
        // Prefix relationships within one decade, both signs.
        assert_order_preserving(&[
            "0.12", "0.123", "0.1234", "0.2", "-0.12", "-0.123", "-0.1234", "-0.2",
        ]);
        // Positive: shorter prefix is smaller (0.12 < 0.123).
        assert!(enc("0.12") < enc("0.123"));
        // Negative: more-negative (longer magnitude) is smaller (-0.123 < -0.12).
        assert!(enc("-0.123") < enc("-0.12"));
    }

    #[test]
    fn zero_sits_between_smallest_negative_and_smallest_positive() {
        assert!(enc("-0.0000000001") < enc("0"));
        assert!(enc("0") < enc("0.0000000001"));
        assert_eq!(enc("0"), "1");
    }

    /// Binds this encoding to the backup format's key comparator: a
    /// `key_ordered` backup of a table with an `N` sort key promises the
    /// order `KeyOrder::compare` defines, and this backend produces it with
    /// `ORDER BY pk, sk_n` only while the byte order of `sk_n` equals that
    /// comparison. A change to either side fails here. The corpus is a fixed
    /// pseudo-random set of `DynamoDB`-valid numbers (1 to 38 significant
    /// digits, both signs, magnitudes from 10^-130 up to 10^120) plus the
    /// boundary values the tests above use; magnitudes from 10^120 to the
    /// 10^126 limit appear only through the two `9.99...E+125` boundary
    /// values.
    #[test]
    fn number_key_order_matches_backup_format() {
        use extenddb_core::backup_format::KeyOrder;
        use extenddb_core::types::{
            AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType,
            ScalarAttributeType,
        };
        use extenddb_core::validation::number::validate_and_normalize_number;

        let order = KeyOrder::resolve(
            &[
                KeySchemaElement {
                    attribute_name: "pk".to_owned(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_owned(),
                    key_type: KeyType::Range,
                },
            ],
            &[
                AttributeDefinition {
                    attribute_name: "pk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_owned(),
                    attribute_type: ScalarAttributeType::N,
                },
            ],
        )
        .unwrap();
        let item = |n: &str| {
            let mut m = Item::new();
            m.insert("pk".to_owned(), AttributeValue::S("p".to_owned()));
            m.insert("sk".to_owned(), AttributeValue::N(n.to_owned()));
            m
        };

        let mut corpus: Vec<String> = [
            "0",
            "1",
            "-1",
            "1E-130",
            "-1E-130",
            "9.9999999999999999999999999999999999999E+125",
            "-9.9999999999999999999999999999999999999E+125",
            "1.2345678901234567890123456789012345678",
            "1.2345678901234567890123456789012345679",
            "0.12",
            "0.123",
            "-0.123",
            "10",
            "9",
        ]
        .iter()
        .map(|s| validate_and_normalize_number(s).unwrap())
        .collect();
        let mut x: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        while corpus.len() < 200 {
            let digits = usize::try_from(next() % 38).unwrap() + 1;
            let mut mantissa = String::new();
            for i in 0..digits {
                let d = next() % 10;
                // No leading zero so the digit count is the significant count.
                let d = if i == 0 && d == 0 { 1 } else { d };
                mantissa.push(char::from(b'0' + u8::try_from(d).unwrap()));
            }
            // Exponent of the leading digit, -130 to 119, so
            // 10^-130 <= |v| < 10^120, inside DynamoDB's range (below
            // 10^126); the mantissa's `digits` digits follow it.
            let exp = i64::try_from(next() % 250).unwrap() - 130;
            let sign = if next() % 2 == 0 { "" } else { "-" };
            let candidate = format!(
                "{sign}{mantissa}E{}",
                exp - i64::try_from(digits).unwrap() + 1
            );
            if let Ok(normalized) = validate_and_normalize_number(&candidate) {
                corpus.push(normalized);
            }
        }
        corpus.sort();
        corpus.dedup();

        for a in &corpus {
            for b in &corpus {
                let encoded = enc(a).cmp(&enc(b));
                let format = order.compare(&item(a), &item(b)).unwrap();
                assert_eq!(
                    encoded, format,
                    "{a} vs {b}: sk_n byte order {encoded:?}, backup format order {format:?}"
                );
            }
        }
    }
}

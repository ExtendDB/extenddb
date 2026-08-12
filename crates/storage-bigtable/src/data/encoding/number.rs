//! Lex-preserving encoding for DDB `N` attribute values used as row-key parts.
//!
//! DDB `N` is an arbitrary-precision decimal (up to 38 significant digits;
//! magnitude in `[1e-130, 1e+126)` or zero, with either sign). To use it as a
//! row-key in BigTable's lex-sorted row-key space, the encoded bytes must
//! compare byte-wise the same way the underlying numbers compare arithmetically.
//!
//! ## Format
//!
//! ```text
//!   value = 0   → 0x80                          (1 byte)
//!   value > 0   → 0xFF | exp_biased | digits[38]   (41 bytes)
//!   value < 0   → 0x00 | ~exp_biased | ~digits[38] (41 bytes)
//! ```
//!
//! - `exp_biased = scientific_exponent + 130` (range `[0, 255]`, fits in 1 byte).
//! - Mantissa digits stored as one byte per decimal digit, normalized to remove
//!   leading and trailing zeros so identical values share the same encoding.
//!   Padded to 38 bytes with `0x00` (positive) so shorter mantissas compare
//!   less. Negatives invert byte-wise (`9 - digit`) and pad with `0xFF` so
//!   shorter negative mantissas compare greater (closer to zero).
//! - The single-byte `0x80` for zero sits between the negative band (leading
//!   `0x00`) and the positive band (leading `0xFF`).

use extenddb_storage::error::StorageError;

const EXP_BIAS: i32 = 130;
const MANTISSA_DIGITS: usize = 38;

const SIGN_NEG: u8 = 0x00;
const SIGN_ZERO: u8 = 0x80;
const SIGN_POS: u8 = 0xFF;

pub fn encode(s: &str) -> Result<Vec<u8>, StorageError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(StorageError::Validation("empty N value".into()));
    }
    let bytes = s.as_bytes();
    let (sign, rest) = match bytes[0] {
        b'-' => (-1i8, &s[1..]),
        b'+' => (1i8, &s[1..]),
        _ => (1i8, s),
    };
    if rest.is_empty() {
        return Err(StorageError::Validation(format!("bad N value: {s}")));
    }

    let (mantissa_part, exp_suffix) = match rest.find(['e', 'E']) {
        Some(i) => {
            let e_str = &rest[i + 1..];
            let e: i32 = e_str
                .parse()
                .map_err(|_| StorageError::Validation(format!("bad N exponent: {e_str}")))?;
            (&rest[..i], e)
        }
        None => (rest, 0i32),
    };

    let (int_part, frac_part) = match mantissa_part.find('.') {
        Some(i) => (&mantissa_part[..i], &mantissa_part[i + 1..]),
        None => (mantissa_part, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(StorageError::Validation(format!("N has no digits: {s}")));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(StorageError::Validation(format!("bad N mantissa: {s}")));
    }

    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);
    let raw_exp = exp_suffix - (frac_part.len() as i32);

    let leading_zeros = digits.bytes().take_while(|&b| b == b'0').count();
    let digits = &digits[leading_zeros..];
    if digits.is_empty() {
        return Ok(vec![SIGN_ZERO]);
    }
    let trailing_zeros = digits.bytes().rev().take_while(|&b| b == b'0').count();
    let raw_exp = raw_exp + (trailing_zeros as i32);
    let digits = &digits[..digits.len() - trailing_zeros];

    if digits.len() > MANTISSA_DIGITS {
        return Err(StorageError::Validation(format!(
            "N has more than {MANTISSA_DIGITS} significant digits: {s}"
        )));
    }

    let sci_exp = raw_exp + (digits.len() as i32) - 1;
    if !(-EXP_BIAS..=125).contains(&sci_exp) {
        return Err(StorageError::Validation(format!(
            "N scientific exponent out of DDB range [-130, 125]: {s} (sci_exp={sci_exp})"
        )));
    }
    let biased_exp = (sci_exp + EXP_BIAS) as u8;

    let mut out = Vec::with_capacity(1 + 1 + MANTISSA_DIGITS);
    if sign > 0 {
        out.push(SIGN_POS);
        out.push(biased_exp);
        for d in digits.bytes() {
            out.push(d - b'0');
        }
        out.resize(1 + 1 + MANTISSA_DIGITS, 0x00);
    } else {
        out.push(SIGN_NEG);
        out.push(0xFF - biased_exp);
        for d in digits.bytes() {
            out.push(9 - (d - b'0'));
        }
        out.resize(1 + 1 + MANTISSA_DIGITS, 0x09);
    }
    Ok(out)
}

pub fn decode(bytes: &[u8]) -> Result<String, StorageError> {
    if bytes.is_empty() {
        return Err(StorageError::Internal("empty number bytes".into()));
    }
    let sign = bytes[0];
    if sign == SIGN_ZERO {
        return Ok("0".to_string());
    }
    if bytes.len() != 2 + MANTISSA_DIGITS {
        return Err(StorageError::Internal(format!("invalid encoded number length: {}", bytes.len())));
    }
    let biased_exp = bytes[1];
    
    let (is_pos, exp, raw_digits) = if sign == SIGN_POS {
        let exp = (biased_exp as i32) - EXP_BIAS;
        let mut digs = Vec::with_capacity(MANTISSA_DIGITS);
        for &b in &bytes[2..] {
            digs.push(b + b'0');
        }
        (true, exp, digs)
    } else if sign == SIGN_NEG {
        let exp = ((0xFF - biased_exp) as i32) - EXP_BIAS;
        let mut digs = Vec::with_capacity(MANTISSA_DIGITS);
        for &b in &bytes[2..] {
            let digit = if b <= 9 { 9 - b } else { 0 };
            digs.push(digit + b'0');
        }
        (false, exp, digs)
    } else {
        return Err(StorageError::Internal(format!("invalid sign byte: {sign}")));
    };

    let mut sig_len = MANTISSA_DIGITS;
    while sig_len > 0 && raw_digits[sig_len - 1] == b'0' {
        sig_len -= 1;
    }
    if sig_len == 0 {
        return Ok("0".to_string());
    }
    let sig_digits = &raw_digits[..sig_len];

    let mut out_str = String::new();
    if !is_pos {
        out_str.push('-');
    }
    let sig_digits_str = std::str::from_utf8(sig_digits)
        .map_err(|e| StorageError::Internal(format!("invalid UTF-8 in numeric digits: {e}")))?;
    let final_exp = exp - (sig_len as i32) + 1;
    if final_exp == 0 {
        out_str.push_str(&sig_digits_str);
    } else {
        out_str.push_str(&format!("{sig_digits_str}e{final_exp}"));
    }
    Ok(out_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(s: &str) -> Vec<u8> {
        encode(s).unwrap_or_else(|e| panic!("encode {s:?}: {e:?}"))
    }

    #[test]
    fn zero_forms_collapse() {
        let z = enc("0");
        assert_eq!(z, vec![SIGN_ZERO]);
        assert_eq!(enc("0.0"), z);
        assert_eq!(enc("-0"), z);
        assert_eq!(enc("0e10"), z);
        assert_eq!(enc("0.000"), z);
    }

    #[test]
    fn equivalent_forms_match() {
        assert_eq!(enc("10"), enc("10.0"));
        assert_eq!(enc("10"), enc("1e1"));
        assert_eq!(enc("100"), enc("1E2"));
        assert_eq!(enc("0.5"), enc("5e-1"));
        assert_eq!(enc("3.14"), enc("314e-2"));
    }

    #[test]
    fn sign_partitioning() {
        let neg = enc("-1");
        let zero = enc("0");
        let pos = enc("1");
        assert!(neg < zero);
        assert!(zero < pos);
    }

    fn check_order(values: &[&str]) {
        for w in values.windows(2) {
            let a = enc(w[0]);
            let b = enc(w[1]);
            assert!(a < b, "expected {} < {} (encoded {:?} < {:?})", w[0], w[1], a, b);
        }
    }

    #[test]
    fn lex_matches_numeric_full_spectrum() {
        check_order(&[
            "-1e125", "-1e10", "-100", "-99.99", "-10", "-1.51", "-1.5", "-1.05",
            "-1", "-0.5", "-0.1", "-1e-10", "-1e-130",
            "0",
            "1e-130", "1e-10", "0.1", "0.5", "1", "1.05", "1.5", "1.51", "10",
            "99.99", "100", "1e10", "1e125",
        ]);
    }

    #[test]
    fn lex_matches_numeric_near_zero_dense() {
        check_order(&[
            "-0.0011", "-0.001", "-0.0009", "-0.0001",
            "0",
            "0.0001", "0.0009", "0.001", "0.0011",
        ]);
    }

    #[test]
    fn lex_matches_numeric_38_digit_precision() {
        let a = "1.0000000000000000000000000000000000001"; // 38 sig digits
        let b = "1.0000000000000000000000000000000000002";
        assert!(enc(a) < enc(b));
        let c = "-1.0000000000000000000000000000000000001";
        let d = "-1.0000000000000000000000000000000000002";
        assert!(enc(d) < enc(c)); // d is more negative
    }

    #[test]
    fn rejects_too_many_digits() {
        // 39 sig digits
        assert!(encode("1234567890123456789012345678901234567890").is_err());
    }

    #[test]
    fn rejects_out_of_range_exp() {
        assert!(encode("1e126").is_err());
        assert!(encode("1e-131").is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(encode("abc").is_err());
        assert!(encode("1.2.3").is_err());
        assert!(encode("--1").is_err());
        assert!(encode("").is_err());
        assert!(encode(".").is_err());
    }

    #[test]
    fn handles_leading_zeros() {
        assert_eq!(enc("007"), enc("7"));
        assert_eq!(enc("000.5"), enc("0.5"));
        assert_eq!(enc("-0007"), enc("-7"));
    }

    #[test]
    fn round_trip_decode() {
        let cases = [
            "0", "1", "-1", "10", "100", "0.5", "-0.5", "3.14", "-0.045",
            "1.23e5", "1.23e-5", "-1.23e5", "-1.23e-5",
            "12345678901234567890123456789012345678",
        ];
        for c in cases {
            let encoded = encode(c).unwrap();
            let decoded = decode(&encoded).unwrap();
            let re_encoded = encode(&decoded).unwrap();
            assert_eq!(encoded, re_encoded, "roundtrip fail for {c} -> decoded {decoded}");
        }
    }
}

// SPDX-License-Identifier: Apache-2.0

//! Unicode decimal digits (general category `Nd`) folded to ASCII for the
//! request-time number check.
//!
//! Measured 2026-10-25 (us-east-1, each request twice): the service reads a
//! fullwidth, Arabic-Indic or Devanagari digit in an `N` value as the digit it
//! stands for (`{"N": "\u{ff11}"}` in a `FilterExpression` answers OK; thirty
//! nine Devanagari ones answer `Attempting to store more than 38 significant
//! digits in a Number`), and refuses a superscript (`\u{b2}`), a vulgar
//! fraction (`\u{bd}`), a fullwidth minus (`\u{ff0d}`) and a fullwidth full
//! stop (`\u{ff0e}`) with `The parameter cannot be converted to a numeric
//! value: <value>`. The accepted characters are exactly those of category `Nd`;
//! the refused ones are `No` or `Pd` and `Po`.
//!
//! Every `Nd` run in Unicode 15.0 is ten consecutive code points starting at
//! its zero, so the table below holds the 68 zeros (generated with Python's
//! `unicodedata`, Unicode 15.0.0) and a digit's value is its offset from the
//! run's zero.

use std::borrow::Cow;

/// The zero of every `Nd` run in Unicode 15.0, ascending.
const ND_ZEROS: [u32; 68] = [
    0x0030, 0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6, 0x0B66, 0x0BE6, 0x0C66, 0x0CE6,
    0x0D66, 0x0DE6, 0x0E50, 0x0ED0, 0x0F20, 0x1040, 0x1090, 0x17E0, 0x1810, 0x1946, 0x19D0, 0x1A80,
    0x1A90, 0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620, 0xA8D0, 0xA900, 0xA9D0, 0xA9F0, 0xAA50, 0xABF0,
    0xFF10, 0x104A0, 0x10D30, 0x11066, 0x110F0, 0x11136, 0x111D0, 0x112F0, 0x11450, 0x114D0,
    0x11650, 0x116C0, 0x11730, 0x118E0, 0x11950, 0x11C50, 0x11D50, 0x11DA0, 0x11F50, 0x16A60,
    0x16AC0, 0x16B50, 0x1D7CE, 0x1D7D8, 0x1D7E2, 0x1D7EC, 0x1D7F6, 0x1E140, 0x1E2F0, 0x1E4F0,
    0x1E950, 0x1FBF0,
];

/// The ASCII digit `c` stands for when `c` is a Unicode decimal digit
/// (general category `Nd`), or `None` for every other character.
pub(crate) fn ascii_digit(c: char) -> Option<char> {
    let cp = u32::from(c);
    // The zero of the run that could hold `cp`: the last zero at or below it.
    let run = ND_ZEROS
        .partition_point(|&zero| zero <= cp)
        .checked_sub(1)?;
    let offset = cp - ND_ZEROS[run];
    u8::try_from(offset)
        .ok()
        .filter(|offset| *offset < 10)
        .map(|offset| char::from(b'0' + offset))
}

/// `s` with every Unicode decimal digit replaced by the ASCII digit it stands
/// for. Borrows `s` when it has no such digit outside ASCII.
pub(crate) fn fold_decimal_digits(s: &str) -> Cow<'_, str> {
    if s.is_ascii() || !s.chars().any(|c| !c.is_ascii() && ascii_digit(c).is_some()) {
        return Cow::Borrowed(s);
    }
    Cow::Owned(s.chars().map(|c| ascii_digit(c).unwrap_or(c)).collect())
}

#[cfg(test)]
mod tests {
    use super::{ND_ZEROS, ascii_digit, fold_decimal_digits};

    #[test]
    fn measured_digits_fold_to_their_ascii_value() {
        // Fullwidth one, Arabic-Indic one, Devanagari one (X545-1 K1 to K3),
        // fullwidth seven (H545-2 C11).
        assert_eq!(ascii_digit('\u{ff11}'), Some('1'));
        assert_eq!(ascii_digit('\u{661}'), Some('1'));
        assert_eq!(ascii_digit('\u{967}'), Some('1'));
        assert_eq!(ascii_digit('\u{ff17}'), Some('7'));
        assert_eq!(ascii_digit('0'), Some('0'));
        assert_eq!(ascii_digit('9'), Some('9'));
    }

    #[test]
    fn measured_controls_are_not_digits() {
        // Superscript two, vulgar half, fullwidth minus, fullwidth full stop
        // (H545-2 C13 to C16): refused by the service.
        for c in ['\u{b2}', '\u{bd}', '\u{ff0d}', '\u{ff0e}'] {
            assert_eq!(ascii_digit(c), None, "{c:?}");
        }
        // Neighbours of a run and non-digit characters.
        assert_eq!(ascii_digit('\u{ff10}'), Some('0'));
        assert_eq!(ascii_digit('\u{ff19}'), Some('9'));
        assert_eq!(ascii_digit('\u{ff1a}'), None);
        assert_eq!(ascii_digit('\u{ff0f}'), None);
        assert_eq!(ascii_digit('a'), None);
        assert_eq!(ascii_digit('.'), None);
        assert_eq!(ascii_digit('e'), None);
        assert_eq!(ascii_digit('\u{1FBF9}'), Some('9'));
        assert_eq!(ascii_digit('\u{1FBFA}'), None);
        assert_eq!(ascii_digit(char::MAX), None);
        assert_eq!(ascii_digit('\u{0}'), None);
    }

    #[test]
    fn zeros_are_ascending_and_distinct_runs() {
        for pair in ND_ZEROS.windows(2) {
            assert!(pair[0] + 10 <= pair[1], "{pair:?}");
        }
    }

    #[test]
    fn folding_keeps_every_other_character() {
        assert_eq!(fold_decimal_digits("\u{ff11}"), "1");
        assert_eq!(fold_decimal_digits("1\u{ff12}"), "12");
        assert_eq!(fold_decimal_digits("1e\u{ff12}"), "1e2");
        assert_eq!(fold_decimal_digits(&"\u{967}".repeat(39)), "1".repeat(39));
        assert_eq!(fold_decimal_digits("\u{ff0d}1"), "\u{ff0d}1");
        assert_eq!(fold_decimal_digits("1\u{ff0e}5"), "1\u{ff0e}5");
        assert_eq!(fold_decimal_digits("\u{b2}"), "\u{b2}");
        assert_eq!(fold_decimal_digits("-1.5e3"), "-1.5e3");
        assert_eq!(fold_decimal_digits(""), "");
        assert!(matches!(
            fold_decimal_digits("bob"),
            std::borrow::Cow::Borrowed(_)
        ));
        assert!(matches!(
            fold_decimal_digits("\u{ff0d}"),
            std::borrow::Cow::Borrowed(_)
        ));
    }
}

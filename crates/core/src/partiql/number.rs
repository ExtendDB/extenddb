// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL number conversion: number literals in a statement and `N` values in
//! the request `Parameters`.
//!
//! The service reads a PartiQL number the way a Java `BigDecimal` reads a
//! string, then applies the item number rules to the result. Two consequences
//! differ from the classic data-plane number check (`validation::number`),
//! which is why this module exists instead of reusing it (measured 2026-11-02):
//!
//! - A number whose exponent does not fit a 32-bit integer, or whose scale
//!   (digits after the point less the exponent) does not, cannot be converted
//!   at all: `1e2147483648`, `1e99999999999`, `1e-2147483648` and
//!   `1.5e-2147483647`. A parameter answers "The parameter cannot be converted
//!   to a numeric value: <text>" and a literal answers the parse-error prefix
//!   with nothing after it.
//! - The magnitude check works on precision minus scale in 32-bit arithmetic,
//!   which wraps: `1e2147483647` converts and is then reported as an
//!   underflow, not an overflow.
//!
//! A literal that converts but breaks a number rule gets the rule's message
//! with the position inside the value appended (`under root`, `under root.x`,
//! `under root[1]`); a parameter gets the bare message.

use crate::types::AttributeValue;

use super::ast::{Operand, Statement, WhereExpr};
use super::lexer::{Token, TokenKind};
use super::parse_error;
use super::parser::ParseError;

/// The item number rule messages, in the service's words.
pub const OVERFLOW_MESSAGE: &str =
    "Number overflow. Attempting to store a number with magnitude larger than supported range";
pub const UNDERFLOW_MESSAGE: &str =
    "Number underflow. Attempting to store a number with magnitude smaller than supported range";
pub const TOO_MANY_DIGITS_MESSAGE: &str =
    "Attempting to store more than 38 significant digits in a Number";
/// A parameter whose `N` text cannot be read as a number. The text is
/// appended after `": "` unless it is empty.
pub const UNCONVERTIBLE_PARAMETER_MESSAGE: &str =
    "The parameter cannot be converted to a numeric value";

const MAX_SIGNIFICANT_DIGITS: usize = 38;
/// Largest precision minus scale a number may have (`9.99...E+125`).
const MAX_MAGNITUDE: i64 = 126;
/// Smallest precision minus scale a non-zero number may have (`1E-130`).
const MIN_MAGNITUDE: i64 = -129;
/// At most this many exponent digits remain once leading zeros are skipped.
const MAX_EXPONENT_DIGITS: usize = 10;

/// Why a number text was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberProblem {
    /// The text cannot be read as a number at all.
    Unconvertible,
    /// More than 38 significant digits.
    TooManyDigits,
    /// Magnitude above `9.99...E+125`.
    Overflow,
    /// Magnitude below `1E-130`, including the 32-bit wrap of a huge exponent.
    Underflow,
}

impl NumberProblem {
    /// The bare message for the problem. A literal that cannot be converted is
    /// reported as a parse failure instead, and a parameter's text is appended
    /// to the unconvertible message by [`parameter_message`].
    #[must_use]
    pub fn rule_message(self) -> &'static str {
        match self {
            NumberProblem::Unconvertible => UNCONVERTIBLE_PARAMETER_MESSAGE,
            NumberProblem::TooManyDigits => TOO_MANY_DIGITS_MESSAGE,
            NumberProblem::Overflow => OVERFLOW_MESSAGE,
            NumberProblem::Underflow => UNDERFLOW_MESSAGE,
        }
    }
}

/// A number as the decimal reader holds it: the digits as written (sign and
/// point removed) and a 32-bit scale, so the value is `digits * 10^-scale`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Decimal {
    digits: String,
    scale: i32,
}

/// Read `text` with the decimal reader's rules: an optional sign, digits with at
/// most one point (at least one digit), then an optional `e`/`E`, optional
/// sign and exponent digits running to the end of the text. The exponent and
/// the resulting scale must each fit a 32-bit integer. `None` when the text
/// cannot be converted.
fn read_decimal(text: &str) -> Option<Decimal> {
    let bytes = text.as_bytes();
    let mut i = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let mut digits = String::new();
    let mut fraction_digits: i64 = 0;
    let mut seen_point = false;
    let mut exponent: i64 = 0;
    while i < bytes.len() {
        match bytes[i] {
            c @ b'0'..=b'9' => {
                digits.push(char::from(c));
                if seen_point {
                    fraction_digits += 1;
                }
            }
            b'.' if !seen_point => seen_point = true,
            b'e' | b'E' => {
                exponent = read_exponent(&bytes[i + 1..])?;
                break;
            }
            _ => return None,
        }
        i += 1;
    }
    if digits.is_empty() {
        return None;
    }
    let scale = i32::try_from(fraction_digits - exponent).ok()?;
    Some(Decimal { digits, scale })
}

/// The exponent digits after `e`: an optional sign, then digits to the end of
/// the text. Leading zeros are skipped while more than ten digits remain, and
/// at most ten may be left; the value must fit a 32-bit integer.
fn read_exponent(rest: &[u8]) -> Option<i64> {
    let (negative, mut digits) = match rest.first() {
        Some(b'-') => (true, &rest[1..]),
        Some(b'+') => (false, &rest[1..]),
        _ => (false, rest),
    };
    if digits.is_empty() {
        return None;
    }
    while digits.len() > MAX_EXPONENT_DIGITS && digits[0] == b'0' {
        digits = &digits[1..];
    }
    if digits.len() > MAX_EXPONENT_DIGITS {
        return None;
    }
    let mut value: i64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        value = value * 10 + i64::from(c - b'0');
    }
    let value = if negative { -value } else { value };
    i32::try_from(value).ok()?;
    Some(value)
}

/// Check a number text against the conversion and the item number rules.
///
/// # Errors
///
/// Returns the first rule the text breaks: it cannot be converted, then more
/// than 38 significant digits, then the magnitude range.
pub fn check_number(text: &str) -> Result<(), NumberProblem> {
    let decimal = read_decimal(text).ok_or(NumberProblem::Unconvertible)?;
    let significant = decimal.digits.trim_start_matches('0');
    if significant.is_empty() {
        // Zero, whatever its scale.
        return Ok(());
    }
    if significant.trim_end_matches('0').len() > MAX_SIGNIFICANT_DIGITS {
        return Err(NumberProblem::TooManyDigits);
    }
    // Precision (digits from the first non-zero one) minus scale, which the
    // service computes in 32-bit arithmetic. Past `i32::MAX` it wraps to a
    // large negative value, which the floor check reports as an underflow.
    let precision = i64::try_from(significant.len()).unwrap_or(i64::MAX);
    let magnitude = precision.saturating_sub(i64::from(decimal.scale));
    if magnitude > i64::from(i32::MAX) {
        return Err(NumberProblem::Underflow);
    }
    if magnitude > MAX_MAGNITUDE {
        return Err(NumberProblem::Overflow);
    }
    if magnitude < MIN_MAGNITUDE {
        return Err(NumberProblem::Underflow);
    }
    Ok(())
}

/// The message for a parameter's `N` text that fails [`check_number`].
#[must_use]
pub fn parameter_message(text: &str, problem: NumberProblem) -> String {
    match problem {
        NumberProblem::Unconvertible if text.is_empty() => {
            UNCONVERTIBLE_PARAMETER_MESSAGE.to_owned()
        }
        NumberProblem::Unconvertible => format!("{UNCONVERTIBLE_PARAMETER_MESSAGE}: {text}"),
        other => other.rule_message().to_owned(),
    }
}

/// Refuse a statement whose text holds a number literal that cannot be
/// converted. The service reports it as a parse failure with nothing after the
/// prefix, ahead of any other check on the statement (measured 2026-11-02).
///
/// # Errors
///
/// Returns the parse error for the first such literal.
pub(crate) fn check_number_tokens(tokens: &[Token]) -> Result<(), ParseError> {
    for token in tokens {
        if let TokenKind::Number(text) = &token.kind
            && read_decimal(text).is_none()
        {
            return Err(unconvertible_literal());
        }
    }
    Ok(())
}

fn unconvertible_literal() -> ParseError {
    ParseError {
        message: parse_error(""),
    }
}

/// Check every number literal in a parsed statement against the item number
/// rules. The message names the position inside the literal value: `root` for
/// the value itself, `.name` for a map member and `[i]` for a list element
/// (measured 2026-11-02 for a top-level value, a map member and a list
/// element). Literals are visited in statement order; members of one map
/// literal are visited in name order.
///
/// # Errors
///
/// Returns the message for the first literal that breaks a rule.
pub fn check_literal_numbers(stmt: &Statement) -> Result<(), ParseError> {
    match stmt {
        Statement::Select(select) | Statement::Exists(select) => match &select.where_clause {
            Some(where_clause) => where_literals(where_clause),
            None => Ok(()),
        },
        Statement::Insert(insert) => {
            for (_, value) in &insert.value {
                operand_literals(value)?;
            }
            Ok(())
        }
        Statement::Update(update) => {
            for action in &update.sets {
                operand_literals(&action.value)?;
            }
            // The bag of a set_add or set_delete is a value like any other
            // (measured 2026-11-02).
            for action in &update.set_members {
                operand_literals(&action.members)?;
            }
            match &update.where_clause {
                Some(where_clause) => where_literals(where_clause),
                None => Ok(()),
            }
        }
        Statement::Delete(delete) => where_literals(&delete.where_clause),
    }
}

/// The refusal [`check_literal_numbers`] gives a number literal `text` at
/// `path` inside its value, or `None` when the number passes. The parser uses
/// it to report such a number ahead of the operator count.
pub(crate) fn literal_number_refusal(text: &str, path: &str) -> Option<String> {
    literal_number(text, path).err().map(|e| e.message)
}

/// The refusal [`check_literal_numbers`] gives the first number literal in
/// `operand` that breaks a rule, or `None` when they all pass. The parser
/// uses it to report a SET value's number ahead of a sign in the clause.
pub(crate) fn operand_number_refusal(operand: &Operand) -> Option<String> {
    operand_literals(operand).err().map(|e| e.message)
}

fn where_literals(expr: &WhereExpr) -> Result<(), ParseError> {
    match expr {
        WhereExpr::Compare(left, _, right)
        | WhereExpr::BeginsWith(left, right)
        | WhereExpr::Contains(left, right)
        | WhereExpr::AttributeType(left, right) => {
            operand_literals(left)?;
            operand_literals(right)
        }
        WhereExpr::Between(value, low, high) => {
            operand_literals(value)?;
            operand_literals(low)?;
            operand_literals(high)
        }
        WhereExpr::In(value, items) => {
            operand_literals(value)?;
            items.iter().try_for_each(operand_literals)
        }
        WhereExpr::Function(_, args) => args.iter().try_for_each(operand_literals),
        WhereExpr::IsMissing(..) => Ok(()),
        WhereExpr::IsType(operand, ..) => operand_literals(operand),
        WhereExpr::And(left, right) | WhereExpr::Or(left, right) => {
            where_literals(left)?;
            where_literals(right)
        }
        WhereExpr::Not(inner) | WhereExpr::ComparedWithBoolean(inner, _) => where_literals(inner),
    }
}

fn operand_literals(operand: &Operand) -> Result<(), ParseError> {
    match operand {
        Operand::Literal(value) => value_numbers(value, &mut "root".to_owned()),
        Operand::Arith(left, _, right) => {
            operand_literals(left)?;
            operand_literals(right)
        }
        Operand::Function(_, args) => args.iter().try_for_each(operand_literals),
        Operand::Parameter(_) | Operand::Path(_) => Ok(()),
    }
}

fn value_numbers(value: &AttributeValue, path: &mut String) -> Result<(), ParseError> {
    match value {
        AttributeValue::N(text) => literal_number(text, path),
        AttributeValue::NS(members) => members.iter().try_for_each(|n| literal_number(n, path)),
        AttributeValue::L(items) => {
            for (index, item) in items.iter().enumerate() {
                let len = path.len();
                path.push_str(&format!("[{index}]"));
                value_numbers(item, path)?;
                path.truncate(len);
            }
            Ok(())
        }
        AttributeValue::M(members) => {
            for (name, member) in members {
                let len = path.len();
                path.push('.');
                path.push_str(name);
                value_numbers(member, path)?;
                path.truncate(len);
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn literal_number(text: &str, path: &str) -> Result<(), ParseError> {
    match check_number(text) {
        Ok(()) => Ok(()),
        Err(NumberProblem::Unconvertible) => Err(unconvertible_literal()),
        Err(problem) => Err(ParseError {
            message: format!("{} under {path}", problem.rule_message()),
        }),
    }
}

#[cfg(test)]
mod tests;

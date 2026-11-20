// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checks of a statement's literal values and of the calls in its SET
//! values, made once the statement parses.
//!
//! [`check_condition_values`] reads the literals of the WHERE condition and
//! [`check_written_values`] the values an INSERT or an UPDATE writes. Each
//! walk reports the first problem it meets in the order the statement is
//! written: a number literal that breaks an item number rule, an empty bag,
//! a number set two of whose members have one value, and, in a SET value, a
//! call an update cannot make. The arguments of a call are met before the
//! call (measured 2026-11-06 in us-east-1: `SET a = size(upper(m))` is
//! refused for `upper`, `SET a = upper(size(m))` for `size`, `SET b = 1e126,
//! a = size(m)` for the number and `SET a = size(m), b = 1e126` for the
//! call).
//!
//! The service reads the condition's literals ahead of the operand types of
//! its ordering comparisons, and the written values after them: `SET a = 1
//! WHERE ... AND v = <<>> AND x < true` and `... AND x < true AND v = <<>>`
//! are refused for the bag, and `SET b = 1e126 WHERE ... AND x < true`, with
//! a literal or a parameter, for the operand type (measured 2026-11-06). The
//! written values come ahead of overlapping SET paths, of a `?` without a
//! parameter and of every check against the table (measured 2026-11-06).
//!
//! The operand type of an arithmetic literal in a SET value (`'x' + 1`) is
//! not checked here: the service reports it after every number rule, bag and
//! call refusal in the SET values, whatever order they are written in
//! (measured 2026-11-06: `SET a = 'x' + 1, b = size(m)`, `SET b = size(m), a
//! = 'x' + 1`, `SET a = 'x' + size(m)` and `SET a = size('x' + 1)` are
//! refused for `size`, `SET a = 'x' + 1, b = <<>>` and `SET a = <<>> + 1` for
//! the bag, `SET a = 'x' + 1e126` and `SET a = 'x' + 1, b = 1e126` for the
//! number). The translation of the statement checks it.
//!
//! A map literal holds its members in name order, not in the order they are
//! written, and a number set its members in text order, so where two
//! members of one map both have a problem the one reported can differ from
//! the service's, and the members a number set refusal lists can come in
//! another order.

use std::collections::{BTreeSet, HashMap, HashSet};

use super::ast::{Operand, SetAction, SetMembersAction, Statement, UpdateStatement, WhereExpr};
use super::lexer::{Token, TokenKind, lex};
use super::number::literal_number_refusal;
use crate::types::AttributeValue;
use crate::validation::number::validate_and_normalize_number;

/// The refusal of a bag literal with no members (measured 2026-11-06 in an
/// INSERT value, a SET value and a condition, at the top of the value and
/// inside a map, a list and a call).
pub const EMPTY_BAG: &str = "Empty bags are not supported";

/// The condition functions. A SET value that calls one is refused naming it
/// (measured 2026-11-06 for each, with any number of arguments).
const CONDITION_FUNCTIONS: [&str; 6] = [
    "attribute_exists",
    "attribute_not_exists",
    "attribute_type",
    "begins_with",
    "contains",
    "size",
];

/// The aggregate calls, refused in a SET value as a token the expression
/// does not take, naming the call (measured 2026-11-06 for each).
const AGGREGATES: [&str; 5] = ["avg", "count", "max", "min", "sum"];

/// The path a value's own refusals name for the value itself.
const ROOT: &str = "root";

/// Check the literals of the statement's WHERE condition (see the module
/// documentation).
///
/// # Errors
///
/// Returns the service's message for the first problem.
pub fn check_condition_values(stmt: &Statement) -> Result<(), String> {
    let where_clause = match stmt {
        Statement::Select(select) | Statement::Exists(select) => select.where_clause.as_ref(),
        Statement::Update(update) => update.where_clause.as_ref(),
        Statement::Delete(delete) => Some(&delete.where_clause),
        Statement::Insert(_) => None,
    };
    match where_clause {
        Some(expr) => condition(expr),
        None => Ok(()),
    }
}

/// Check the values an INSERT or an UPDATE writes (see the module
/// documentation). `source` is the statement's text, which gives the
/// position a refusal of a call names.
///
/// # Errors
///
/// Returns the service's message for the first problem.
pub fn check_written_values(stmt: &Statement, source: &str) -> Result<(), String> {
    match stmt {
        // A call as an INSERT value is refused by the parser.
        Statement::Insert(insert) => insert
            .value
            .iter()
            .try_for_each(|(_, value)| literals(value)),
        Statement::Update(update) => {
            let tokens = SetTokens::read(source);
            let mut walk = SetWalk {
                tokens: &tokens,
                seen: HashMap::new(),
            };
            for assignment in assignments(update, &tokens.members) {
                match assignment {
                    Assignment::Set(action) => walk.operand(&action.value)?,
                    Assignment::Members(action) => literals(&action.members)?,
                }
            }
            Ok(())
        }
        Statement::Select(_) | Statement::Exists(_) | Statement::Delete(_) => Ok(()),
    }
}

fn condition(expr: &WhereExpr) -> Result<(), String> {
    match expr {
        WhereExpr::Compare(left, _, right)
        | WhereExpr::BeginsWith(left, right)
        | WhereExpr::Contains(left, right)
        | WhereExpr::AttributeType(left, right) => {
            literals(left)?;
            literals(right)
        }
        WhereExpr::Between(value, low, high) => {
            literals(value)?;
            literals(low)?;
            literals(high)
        }
        WhereExpr::In(value, items) => {
            literals(value)?;
            items.iter().try_for_each(literals)
        }
        WhereExpr::Function(_, args) => args.iter().try_for_each(literals),
        WhereExpr::IsMissing(..) => Ok(()),
        WhereExpr::IsType(operand, ..) => literals(operand),
        WhereExpr::And(left, right) | WhereExpr::Or(left, right) => {
            condition(left)?;
            condition(right)
        }
        WhereExpr::Not(inner) | WhereExpr::ComparedWithBoolean(inner, _) => condition(inner),
    }
}

/// The literals of an operand outside a SET value, where the parser has
/// checked every call.
fn literals(operand: &Operand) -> Result<(), String> {
    match operand {
        Operand::Literal(value) => literal(value, &mut ROOT.to_owned()),
        Operand::Arith(left, _, right) => {
            literals(left)?;
            literals(right)
        }
        Operand::Function(_, args) => args.iter().try_for_each(literals),
        Operand::Parameter(_) | Operand::Path(_) => Ok(()),
    }
}

/// One literal value at `path` inside the value it is part of (`root`,
/// `root.k`, `root[0]`): its numbers, its bags, and the values inside it.
fn literal(value: &AttributeValue, path: &mut String) -> Result<(), String> {
    match value {
        AttributeValue::N(text) => number(text, path),
        AttributeValue::NS(members) => {
            members.iter().try_for_each(|text| number(text, path))?;
            number_set(members, path)
        }
        AttributeValue::SS(members) if members.is_empty() => Err(EMPTY_BAG.to_owned()),
        AttributeValue::L(items) => {
            for (index, item) in items.iter().enumerate() {
                let len = path.len();
                path.push_str(&format!("[{index}]"));
                literal(item, path)?;
                path.truncate(len);
            }
            Ok(())
        }
        AttributeValue::M(members) => {
            for (name, member) in members {
                let len = path.len();
                path.push('.');
                path.push_str(name);
                literal(member, path)?;
                path.truncate(len);
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// A number literal against the item number rules (see the `number`
/// module), named by its `path` inside the value.
fn number(text: &str, path: &str) -> Result<(), String> {
    match literal_number_refusal(text, path) {
        Some(refusal) => Err(refusal),
        None => Ok(()),
    }
}

/// Refuse a number set two of whose members have one value, listing the
/// members as decimals (see [`decimal_text`]) and naming the set's path
/// (measured 2026-11-06: `<<1, 1.0>>` is "One or more parameter values were
/// invalid: Input collection [1, 1.0] contains duplicates! under root", in
/// a map member `root.x`, in a list `root[0]`, also in a set_add, in an IN
/// list and in a call's argument).
fn number_set(members: &BTreeSet<String>, path: &str) -> Result<(), String> {
    let mut values = HashSet::with_capacity(members.len());
    let repeated = members.iter().any(|text| {
        !values.insert(validate_and_normalize_number(text).unwrap_or_else(|_| text.clone()))
    });
    if !repeated {
        return Ok(());
    }
    let listed: Vec<String> = members.iter().map(|text| decimal_text(text)).collect();
    Err(format!(
        "One or more parameter values were invalid: Input collection [{}] contains duplicates! \
         under {path}",
        listed.join(", ")
    ))
}

/// A number literal written as the decimal it reads as: the digits without
/// leading zeros and the number of digits after the point the literal gives,
/// in plain notation when the exponent of that decimal is not positive and
/// its value is at least 1e-6, and otherwise as one digit, the rest of the
/// digits after a point, and `E` with a signed exponent (measured
/// 2026-11-06: `1e2` is `1E+2`, `12e3` is `1.2E+4`, `1.0e1` is `10`,
/// `0.001` and `1e-3` are `0.001`, `0.0000001` and `1e-7` are `1E-7`, `007`
/// is `7`, `1.50` is `1.50`, `-1e2` is `-1E+2`).
fn decimal_text(text: &str) -> String {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(at) => (
            &unsigned[..at],
            unsigned[at + 1..].parse::<i64>().unwrap_or(0),
        ),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    let digits = match digits.trim_start_matches('0') {
        "" => "0",
        significant => significant,
    };
    let count = i64::try_from(digits.len()).unwrap_or(i64::MAX);
    let scale = i64::try_from(fraction.len()).unwrap_or(i64::MAX) - exponent;
    let adjusted = count - 1 - scale;
    let body = if scale >= 0 && adjusted >= -6 {
        let point = usize::try_from(scale).unwrap_or(usize::MAX);
        if point == 0 {
            digits.to_owned()
        } else if digits.len() > point {
            let (int, frac) = digits.split_at(digits.len() - point);
            format!("{int}.{frac}")
        } else {
            format!("0.{}{digits}", "0".repeat(point - digits.len()))
        }
    } else {
        let (first, rest) = digits.split_at(1);
        let point = if rest.is_empty() { "" } else { "." };
        let sign = if adjusted >= 0 { "+" } else { "" };
        format!("{first}{point}{rest}E{sign}{adjusted}")
    };
    if negative && digits != "0" {
        format!("-{body}")
    } else {
        body
    }
}

/// An assignment of an UPDATE's SET clauses.
enum Assignment<'a> {
    Set(&'a SetAction),
    Members(&'a SetMembersAction),
}

/// The SET assignments in the order they are written. The statement keeps
/// its `set_add` and `set_delete` assignments apart from the others, and
/// `members` says, for each assignment in the order written, whether it is
/// one of them. When `members` does not match the statement, the others
/// come first.
fn assignments<'a>(update: &'a UpdateStatement, members: &[bool]) -> Vec<Assignment<'a>> {
    let mut sets = update.sets.iter();
    let mut set_members = update.set_members.iter();
    if members.len() != update.sets.len() + update.set_members.len()
        || members.iter().filter(|m| **m).count() != update.set_members.len()
    {
        return sets
            .map(Assignment::Set)
            .chain(set_members.map(Assignment::Members))
            .collect();
    }
    members
        .iter()
        .filter_map(|&is_members| {
            if is_members {
                set_members.next().map(Assignment::Members)
            } else {
                sets.next().map(Assignment::Set)
            }
        })
        .collect()
}

/// What the tokens of an UPDATE's SET clauses say about its assignments and
/// calls.
#[derive(Debug, Default, PartialEq)]
struct SetTokens {
    /// For each assignment in the order written, whether its value is a
    /// `set_add` or `set_delete` call.
    members: Vec<bool>,
    /// Each call in a SET value in the order written, but the `set_add` and
    /// `set_delete` calls that are the whole value.
    calls: Vec<Call>,
}

/// A call in a SET value.
#[derive(Debug, PartialEq)]
struct Call {
    /// The name, in lower case.
    name: String,
    /// The `line:column:length` of the name.
    at: String,
    /// The `line:column:length` of the `(` after the name.
    paren_at: String,
}

impl SetTokens {
    /// Read the SET clauses of the statement `source`. A SET clause runs from
    /// a SET keyword outside brackets to the next SET, REMOVE, WHERE or
    /// RETURNING keyword outside brackets; an assignment's value starts after
    /// its `=` and ends at the next comma outside brackets.
    fn read(source: &str) -> Self {
        let Ok(tokens) = lex(source) else {
            return Self::default();
        };
        let mut read = Self::default();
        let mut depth = 0usize;
        let mut in_set = false;
        let mut whole_value_call = None;
        for (i, token) in tokens.iter().enumerate() {
            let next = tokens.get(i + 1).map(|t| &t.kind);
            match &token.kind {
                TokenKind::LParen
                | TokenKind::LBracket
                | TokenKind::LBrace
                | TokenKind::LDoubleAngle => depth += 1,
                TokenKind::RParen
                | TokenKind::RBracket
                | TokenKind::RBrace
                | TokenKind::RDoubleAngle => depth = depth.saturating_sub(1),
                TokenKind::Ident(word) if depth == 0 && is_clause_keyword(word) => {
                    in_set = word.eq_ignore_ascii_case("set");
                }
                TokenKind::Eq if depth == 0 && in_set => {
                    let members = is_set_members_call(&tokens, i + 1);
                    read.members.push(members);
                    whole_value_call = members.then_some(i + 1);
                }
                TokenKind::Ident(word)
                    if in_set
                        && next == Some(&TokenKind::LParen)
                        && whole_value_call != Some(i) =>
                {
                    read.calls.push(Call {
                        name: word.to_ascii_lowercase(),
                        at: span(token),
                        paren_at: tokens.get(i + 1).map(span).unwrap_or_default(),
                    });
                }
                _ => {}
            }
        }
        read
    }

    /// The `occurrence`-th call named `name` in the SET values.
    fn call(&self, name: &str, occurrence: usize) -> Option<&Call> {
        self.calls
            .iter()
            .filter(|call| call.name == name)
            .nth(occurrence)
    }
}

/// Whether the token at `i` and the one after it are `set_add(` or
/// `set_delete(`, in any case.
fn is_set_members_call(tokens: &[Token], i: usize) -> bool {
    matches!(
        tokens.get(i).map(|t| &t.kind),
        Some(TokenKind::Ident(word))
            if word.eq_ignore_ascii_case("set_add") || word.eq_ignore_ascii_case("set_delete")
    ) && matches!(tokens.get(i + 1).map(|t| &t.kind), Some(TokenKind::LParen))
}

fn is_clause_keyword(word: &str) -> bool {
    ["set", "remove", "where", "returning"]
        .iter()
        .any(|keyword| word.eq_ignore_ascii_case(keyword))
}

fn span(token: &Token) -> String {
    format!("{}:{}:{}", token.line, token.column, token.length)
}

/// The walk of an UPDATE's SET values.
struct SetWalk<'a> {
    tokens: &'a SetTokens,
    /// How many calls of each name the walk has entered.
    seen: HashMap<String, usize>,
}

impl SetWalk<'_> {
    fn operand(&mut self, operand: &Operand) -> Result<(), String> {
        match operand {
            Operand::Literal(value) => literal(value, &mut ROOT.to_owned()),
            Operand::Arith(left, _, right) => {
                self.operand(left)?;
                self.operand(right)
            }
            Operand::Function(name, args) => {
                // The calls are entered in the order their names are
                // written, which is how the tokens list them.
                let seen = self.seen.entry(name.clone()).or_default();
                let occurrence = *seen;
                *seen += 1;
                args.iter().try_for_each(|arg| self.operand(arg))?;
                self.call(name, args, occurrence)
            }
            Operand::Parameter(_) | Operand::Path(_) => Ok(()),
        }
    }

    /// The refusal of the `occurrence`-th call named `name` in the SET
    /// values, once its arguments pass (measured 2026-11-06 for each name
    /// here and for `exists`, `upper`, `lower`, `char_length`, `substring`,
    /// `trim` and two names that are no function, in ExecuteStatement,
    /// BatchExecuteStatement and ExecuteTransaction). `if_not_exists` and
    /// `list_append` take two operands, and the first operand of
    /// `if_not_exists` is a path; `set_add` and `set_delete` are the whole
    /// value or not a function.
    fn call(&self, name: &str, args: &[Operand], occurrence: usize) -> Result<(), String> {
        let at = |paren: bool| {
            self.tokens
                .call(name, occurrence)
                .map(|call| format!(" at {}", if paren { &call.paren_at } else { &call.at }))
                .unwrap_or_default()
        };
        match name {
            "if_not_exists" | "list_append" => {
                if args.len() != 2 {
                    return Err(format!(
                        "Incorrect number of operands for operator or function; operator or \
                         function: {name}, number of operands: {}",
                        args.len()
                    ));
                }
                if name == "if_not_exists" && !matches!(args[0], Operand::Path(_)) {
                    return Err(format!(
                        "Operator or function requires a document path; operator or function: \
                         {name}"
                    ));
                }
                Ok(())
            }
            _ if CONDITION_FUNCTIONS.contains(&name) => Err(format!(
                "The function is not allowed in an update expression; function: {name}"
            )),
            _ if AGGREGATES.contains(&name) => Err(format!(
                "Unsupported token in expression: {name}{}",
                at(false)
            )),
            // (measured 2026-11-06: `nullif(1, m)` names the call,
            // `coalesce(m, 1)` the parenthesis after it)
            "nullif" => Err(format!(
                "Unsupported token in expression: UNKNOWN{}",
                at(false)
            )),
            "coalesce" => Err(format!(
                "Unsupported token in expression: UNKNOWN{}",
                at(true)
            )),
            _ => Err(format!("Unrecognized function: {name}{}", at(false))),
        }
    }
}

#[cfg(test)]
mod tests;

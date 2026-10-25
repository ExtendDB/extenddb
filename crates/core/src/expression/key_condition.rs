// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `KeyConditionExpression` parser.
//!
//! Parses a `KeyConditionExpression` into a partition key equality condition
//! and an optional sort key condition. The grammar is a restricted subset of
//! `ConditionExpression`:
//!
//! - Partition key: `pk = :val` (equality only, required)
//! - Sort key (optional): `sk op :val`, `:val op sk`, `sk BETWEEN :lo AND :hi`,
//!   `begins_with(sk, :prefix)`
//! - Only AND is allowed to combine PK and SK conditions
//! - No OR, NOT, nested conditions, or other functions

use super::ast::{CompareOp, Expr, PathElement};
use super::kind::ExpressionKind;
use super::parser_common;
use super::syntax_error::{is_operator_or_keyword, syntax_error};
use super::tokenizer::Token;
use crate::error::DynamoDbError;

/// Parsed key condition: partition key value expression(s) and optional sort key condition.
///
/// For standard (single-attribute) keys, `pk_path`/`pk_value` hold the single PK condition
/// and `extra_pk_conditions` is empty.
///
/// For multi-part keys, `pk_path`/`pk_value` hold the first PK condition and
/// `extra_pk_conditions` holds additional PK equality conditions.
#[derive(Debug, Clone)]
pub struct KeyCondition {
    /// First partition key attribute path (resolved name).
    pub pk_path: Vec<PathElement>,
    /// First partition key value expression (placeholder or literal).
    pub pk_value: Expr,
    /// Additional partition key equality conditions for multi-part keys.
    pub extra_pk_conditions: Vec<(Vec<PathElement>, Expr)>,
    /// Optional sort key condition (first RANGE key).
    pub sk_condition: Option<SortKeyCondition>,
    /// Additional RANGE key equality conditions for multi-RANGE key schemas.
    pub extra_sk_conditions: Vec<(Vec<PathElement>, Expr)>,
}

/// Sort key condition variants.
#[derive(Debug, Clone)]
pub enum SortKeyCondition {
    /// `sk op :val`
    Compare {
        path: Vec<PathElement>,
        op: CompareOp,
        value: Expr,
    },
    /// `sk BETWEEN :lo AND :hi`
    Between {
        path: Vec<PathElement>,
        low: Expr,
        high: Expr,
    },
    /// `begins_with(sk, :prefix)`
    BeginsWith {
        path: Vec<PathElement>,
        prefix: Expr,
    },
}

/// Parse a `KeyConditionExpression` token stream.
///
/// Extracts the partition key equality and optional sort key condition.
/// Supports multi-part keys: up to 8 AND-ed clauses (4 HASH + 4 RANGE).
/// Rejects OR, NOT, and unsupported functions.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors or unsupported constructs.
pub fn parse_key_condition(tokens: &[Token]) -> Result<KeyCondition, DynamoDbError> {
    parse_key_condition_from(tokens, "")
}

/// Parse a `KeyConditionExpression` whose tokens came from `source`, so a
/// syntax error can quote the source window around the offending token as
/// Amazon `DynamoDB` does (`pk = = :v` answers `Syntax error; token: "=",
/// near: "= = :v"`; `pk = :v AND` answers `token: "<EOF>", near: "AND"`,
/// measured 2026-10-25). An empty `source` joins the token texts instead.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors or unsupported constructs.
pub fn parse_key_condition_from(
    tokens: &[Token],
    source: &str,
) -> Result<KeyCondition, DynamoDbError> {
    parser_common::check_redundant_parens(tokens)
        .map_err(|body| validation_err(&format!("Invalid KeyConditionExpression: {body}")))?;

    // Strip outer parentheses: "(pk = :pk AND sk > :sk)" → "pk = :pk AND sk > :sk"
    let all = tokens;
    let (tokens, offset) = if tokens.len() >= 2
        && tokens[0] == Token::LParen
        && tokens[tokens.len() - 1] == Token::RParen
        && outer_parens_match(tokens)
    {
        (&tokens[1..tokens.len() - 1], 1)
    } else {
        (tokens, 0)
    };
    // Syntax errors quote the window from the full token list, so a position
    // inside the stripped slice is shifted back by the stripped parenthesis.
    let src = Source {
        text: source,
        all,
        offset,
    };

    let mut pos = 0;
    let mut clauses = Vec::new();

    clauses.push(parse_key_clause(tokens, &src, &mut pos)?);

    while pos < tokens.len() {
        if tokens[pos] != Token::And {
            return Err(validation_err(
                "Invalid KeyConditionExpression: only AND is supported between key conditions",
            ));
        }
        pos += 1;
        clauses.push(parse_key_clause(tokens, &src, &mut pos)?);
    }

    if pos < tokens.len() {
        return Err(validation_err(
            "Invalid KeyConditionExpression: unexpected token after key conditions",
        ));
    }

    // A key condition may only reference a top-level key attribute. Any document
    // path (`attr.sub` or `attr[i]`) on a key is rejected, matching Amazon
    // DynamoDB. Runs after the reserved-word check and before key-schema checks.
    for clause in &clauses {
        reject_nested_key_path(raw_clause_path(clause))?;
    }

    match clauses.len() {
        1 => classify_single(clauses.remove(0)),
        2 => classify_pair(clauses.remove(0), clauses.remove(0)),
        _ => classify_multi(clauses),
    }
}

/// The expression text and full token list a clause parser reports syntax
/// errors against; `offset` is the number of tokens stripped before the slice
/// being parsed.
struct Source<'a> {
    text: &'a str,
    all: &'a [Token],
    offset: usize,
}

impl Source<'_> {
    /// The service's syntax error for the token at `pos` of the parsed slice.
    fn syntax_error(&self, pos: usize) -> DynamoDbError {
        syntax_error(
            ExpressionKind::KeyCondition,
            self.text,
            self.all,
            pos + self.offset,
        )
    }
}

/// The attribute-side path of a raw clause.
fn raw_clause_path(clause: &RawClause) -> &[PathElement] {
    match clause {
        RawClause::Eq(path, _)
        | RawClause::Compare(path, _, _)
        | RawClause::Between(path, _, _)
        | RawClause::BeginsWith(path, _) => path,
    }
}

/// Reject any key path that is not a single top-level attribute.
///
/// `parse_path` always emits an `Attribute` (or `#name`) as element 0, so a path
/// can never start with an `Index` or be empty: the length is the real
/// discriminator. The element check is defensive against future AST changes.
fn reject_nested_key_path(path: &[PathElement]) -> Result<(), DynamoDbError> {
    if path.len() == 1 && matches!(path.first(), Some(PathElement::Attribute(_))) {
        return Ok(());
    }
    Err(validation_err(
        "Invalid KeyConditionExpression: KeyConditionExpressions cannot have conditions on nested attributes",
    ))
}

/// A raw parsed clause before we know if it's PK or SK.
enum RawClause {
    Eq(Vec<PathElement>, Expr),
    Compare(Vec<PathElement>, CompareOp, Expr),
    Between(Vec<PathElement>, Expr, Expr),
    BeginsWith(Vec<PathElement>, Expr),
}

enum KeyOperand {
    Path(Vec<PathElement>),
    Value(Expr),
}

fn classify_single(clause: RawClause) -> Result<KeyCondition, DynamoDbError> {
    match clause {
        RawClause::Eq(path, value) => Ok(KeyCondition {
            pk_path: path,
            pk_value: value,
            extra_pk_conditions: Vec::new(),
            sk_condition: None,
            extra_sk_conditions: Vec::new(),
        }),
        _ => Err(validation_err(
            "Invalid KeyConditionExpression: partition key condition must use equality (=)",
        )),
    }
}

fn classify_pair(a: RawClause, b: RawClause) -> Result<KeyCondition, DynamoDbError> {
    // When both clauses are Eq, we assign the first as PK and second as SK.
    // The caller must call `resolve_pk_sk()` after parsing to correct the
    // assignment using the table's key schema (REQ-QUERY-002).
    // One must be EQ (the PK), the other is the SK condition
    match (a, b) {
        (RawClause::Eq(pk_path, pk_value), sk) | (sk, RawClause::Eq(pk_path, pk_value)) => {
            Ok(KeyCondition {
                pk_path,
                pk_value,
                extra_pk_conditions: Vec::new(),
                sk_condition: Some(raw_to_sk(sk)),
                extra_sk_conditions: Vec::new(),
            })
        }
        _ => Err(validation_err(
            "Invalid KeyConditionExpression: one condition must be an equality on the partition key",
        )),
    }
}

/// Classify 3+ AND-ed clauses for multi-part key conditions.
///
/// All clauses must be equality conditions except at most one which may be
/// a range condition (the sort key condition). The caller must later call
/// `resolve_multipart()` to assign HASH vs RANGE based on the key schema.
fn classify_multi(clauses: Vec<RawClause>) -> Result<KeyCondition, DynamoDbError> {
    let mut eq_clauses: Vec<(Vec<PathElement>, Expr)> = Vec::new();
    let mut non_eq: Option<SortKeyCondition> = None;

    for clause in clauses {
        match clause {
            RawClause::Eq(path, value) => {
                eq_clauses.push((path, value));
            }
            other => {
                if non_eq.is_some() {
                    return Err(validation_err(
                        "Invalid KeyConditionExpression: at most one non-equality condition is allowed",
                    ));
                }
                non_eq = Some(raw_to_sk(other));
            }
        }
    }

    if eq_clauses.is_empty() {
        return Err(validation_err(
            "Invalid KeyConditionExpression: at least one equality condition on a partition key attribute is required",
        ));
    }

    let (pk_path, pk_value) = eq_clauses.remove(0);
    // Remaining Eq clauses go to extra_pk_conditions initially.
    // resolve_multipart() will reclassify them as PK or SK.
    let sk_condition = if let Some(sk) = non_eq {
        Some(sk)
    } else if let Some((path, value)) = eq_clauses.pop() {
        // Last Eq clause becomes the SK condition (may be reclassified later)
        Some(SortKeyCondition::Compare {
            path,
            op: CompareOp::Eq,
            value,
        })
    } else {
        None
    };

    Ok(KeyCondition {
        pk_path,
        pk_value,
        extra_pk_conditions: eq_clauses,
        sk_condition,
        extra_sk_conditions: Vec::new(),
    })
}

fn raw_to_sk(clause: RawClause) -> SortKeyCondition {
    match clause {
        RawClause::Eq(path, value) => SortKeyCondition::Compare {
            path,
            op: CompareOp::Eq,
            value,
        },
        RawClause::Compare(path, op, value) => SortKeyCondition::Compare { path, op, value },
        RawClause::Between(path, low, high) => SortKeyCondition::Between { path, low, high },
        RawClause::BeginsWith(path, prefix) => SortKeyCondition::BeginsWith { path, prefix },
    }
}

fn parse_key_clause(
    tokens: &[Token],
    src: &Source<'_>,
    pos: &mut usize,
) -> Result<RawClause, DynamoDbError> {
    if *pos >= tokens.len() {
        if tokens.is_empty() {
            return Err(validation_err(
                "Invalid KeyConditionExpression: unexpected end of expression",
            ));
        }
        // Input ends where a clause is required (`pk = :v AND`).
        return Err(src.syntax_error(*pos));
    }

    // Allow one level of parentheses around a single clause
    if tokens[*pos] == Token::LParen {
        *pos += 1;
        let clause = parse_key_clause_inner(tokens, src, pos)?;
        if *pos < tokens.len() && tokens[*pos] == Token::RParen {
            *pos += 1;
            return Ok(clause);
        }
        return Err(validation_err(
            "Invalid KeyConditionExpression: expected ')'",
        ));
    }

    parse_key_clause_inner(tokens, src, pos)
}

fn parse_key_clause_inner(
    tokens: &[Token],
    src: &Source<'_>,
    pos: &mut usize,
) -> Result<RawClause, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(src.syntax_error(*pos));
    }

    // begins_with(path, :val)
    if let Token::Ident(name) = &tokens[*pos]
        && name.eq_ignore_ascii_case("begins_with")
        && *pos + 1 < tokens.len()
        && tokens[*pos + 1] == Token::LParen
    {
        *pos += 1; // skip function name
        parser_common::expect_token(tokens, pos, &Token::LParen, "(", "KeyConditionExpression")?;
        let path = parse_key_operand_path(tokens, src, pos)?;
        parser_common::expect_token(tokens, pos, &Token::Comma, ",", "KeyConditionExpression")?;
        let prefix = parse_key_value(tokens, src, pos)?;
        parser_common::expect_token(tokens, pos, &Token::RParen, ")", "KeyConditionExpression")?;
        return Ok(RawClause::BeginsWith(path, prefix));
    }

    // path op :value | :value op path | path BETWEEN :lo AND :hi
    let left = parse_key_operand(tokens, src, pos)?;

    if *pos >= tokens.len() {
        return Err(validation_err(
            "Invalid KeyConditionExpression: expected operator after operand",
        ));
    }

    let path = match left {
        KeyOperand::Path(path) => path,
        KeyOperand::Value(value) => {
            let op = try_comparator(&tokens[*pos]).ok_or_else(|| {
                validation_err("Invalid KeyConditionExpression: expected comparison operator")
            })?;
            // DynamoDB rejects <> in KeyConditionExpression — only =, <, <=, >, >=
            // are valid key condition operators.
            if op == CompareOp::Ne {
                return Err(validation_err(
                    "Invalid KeyConditionExpression: Unsupported operator on KeyCondition: NE",
                ));
            }
            *pos += 1;

            let path = parse_key_operand_path(tokens, src, pos)?;
            // Key conditions are stored path-first, so reverse the operator when
            // the source expression puts the value placeholder on the left.
            let op = reverse_comparator(op);

            return if op == CompareOp::Eq {
                Ok(RawClause::Eq(path, value))
            } else {
                Ok(RawClause::Compare(path, op, value))
            };
        }
    };

    // BETWEEN
    if tokens[*pos] == Token::Between {
        *pos += 1;
        let low = parse_key_value(tokens, src, pos)?;
        parser_common::expect_token(tokens, pos, &Token::And, "AND", "KeyConditionExpression")?;
        let high = parse_key_value(tokens, src, pos)?;
        return Ok(RawClause::Between(path, low, high));
    }

    // Comparison operator
    let op = try_comparator(&tokens[*pos]).ok_or_else(|| {
        validation_err("Invalid KeyConditionExpression: expected comparison operator")
    })?;
    // DynamoDB rejects <> in KeyConditionExpression — only =, <, <=, >, >=
    // are valid key condition operators.
    if op == CompareOp::Ne {
        return Err(validation_err(
            "Invalid KeyConditionExpression: Unsupported operator on KeyCondition: NE",
        ));
    }
    *pos += 1;

    let value = parse_key_value(tokens, src, pos)?;

    if op == CompareOp::Eq {
        Ok(RawClause::Eq(path, value))
    } else {
        Ok(RawClause::Compare(path, op, value))
    }
}

fn parse_key_operand(
    tokens: &[Token],
    src: &Source<'_>,
    pos: &mut usize,
) -> Result<KeyOperand, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(src.syntax_error(*pos));
    }

    match &tokens[*pos] {
        Token::Ident(_) | Token::NameRef(_) => {
            let path = parser_common::parse_path(tokens, pos)?;
            Ok(KeyOperand::Path(path))
        }
        Token::Placeholder(_) => {
            let value = parse_key_value(tokens, src, pos)?;
            Ok(KeyOperand::Value(value))
        }
        // An operator or keyword where an operand is required (`AND pk = :v`).
        token if is_operator_or_keyword(token) => Err(src.syntax_error(*pos)),
        _ => Err(validation_err(
            "Invalid KeyConditionExpression: expected attribute path or value placeholder",
        )),
    }
}

fn parse_key_operand_path(
    tokens: &[Token],
    src: &Source<'_>,
    pos: &mut usize,
) -> Result<Vec<PathElement>, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(src.syntax_error(*pos));
    }

    match &tokens[*pos] {
        Token::Ident(_) | Token::NameRef(_) => parser_common::parse_path(tokens, pos),
        token if is_operator_or_keyword(token) => Err(src.syntax_error(*pos)),
        _ => Err(validation_err(
            "Invalid KeyConditionExpression: expected attribute path",
        )),
    }
}

fn parse_key_value(
    tokens: &[Token],
    src: &Source<'_>,
    pos: &mut usize,
) -> Result<Expr, DynamoDbError> {
    if *pos >= tokens.len() {
        // Input ends where a value is required (`pk =`).
        return Err(src.syntax_error(*pos));
    }
    match &tokens[*pos] {
        Token::Placeholder(name) => {
            let expr = Expr::Placeholder(name.clone());
            *pos += 1;
            Ok(expr)
        }
        // An operator or keyword where a value is required (`pk = = :v`,
        // measured 2026-10-25: token "=", near "= = :v").
        token if is_operator_or_keyword(token) => Err(src.syntax_error(*pos)),
        _ => Err(validation_err(
            "Invalid KeyConditionExpression: key condition values must be expression attribute value placeholders",
        )),
    }
}

fn reverse_comparator(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Eq => CompareOp::Eq,
        CompareOp::Ne => CompareOp::Ne,
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
    }
}

fn try_comparator(token: &Token) -> Option<CompareOp> {
    match token {
        Token::Eq => Some(CompareOp::Eq),
        Token::Ne => Some(CompareOp::Ne),
        Token::Lt => Some(CompareOp::Lt),
        Token::Le => Some(CompareOp::Le),
        Token::Gt => Some(CompareOp::Gt),
        Token::Ge => Some(CompareOp::Ge),
        _ => None,
    }
}

/// Check if the outermost parens in a token slice are a matching pair that
/// wraps the entire expression (not just a prefix).
fn outer_parens_match(tokens: &[Token]) -> bool {
    let mut depth = 0;
    for (i, token) in tokens.iter().enumerate() {
        match token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 && i < tokens.len() - 1 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

fn validation_err(msg: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(msg.to_owned())
}

#[cfg(test)]
#[path = "key_condition_tests.rs"]
mod tests;

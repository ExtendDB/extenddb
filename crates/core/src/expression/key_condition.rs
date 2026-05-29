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
use super::parser_common;
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

impl KeyCondition {
    /// Correct PK/SK assignment using the table's key schema.
    ///
    /// For single-attribute keys: when both the PK and SK conditions are equality
    /// comparisons, the parser cannot distinguish which is which without knowing
    /// the key schema. This method resolves `#name` references and swaps PK/SK
    /// if the current PK path actually refers to the sort key attribute.
    ///
    /// For multi-part keys: all equality conditions are initially parsed as
    /// PK + SK pairs. This method reclassifies them based on the key schema,
    /// moving HASH attribute conditions to `extra_pk_conditions` and keeping
    /// the RANGE condition as `sk_condition`.
    ///
    /// Call this after parsing and before passing to the storage layer.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` if a `#name` reference cannot be resolved.
    pub fn resolve_pk_sk(
        &mut self,
        pk_attr: &str,
        names: &std::collections::HashMap<String, String>,
    ) -> Result<(), DynamoDbError> {
        // Only ambiguous when SK is an Eq comparison (both clauses were Eq).
        let sk_is_eq = matches!(
            self.sk_condition,
            Some(SortKeyCondition::Compare {
                op: CompareOp::Eq,
                ..
            })
        );
        if !sk_is_eq {
            return Ok(());
        }

        // Resolve the current PK path's top-level attribute name.
        let pk_name = match self.pk_path.first() {
            Some(PathElement::Attribute(name)) => {
                if let Some(ref_name) = name.strip_prefix('#') {
                    names.get(ref_name).map(String::as_str).ok_or_else(|| {
                        DynamoDbError::ValidationException(format!(
                            "An expression attribute name used in the document path \
                             is not defined; attribute name: #{ref_name}"
                        ))
                    })?
                } else {
                    name.as_str()
                }
            }
            _ => return Ok(()),
        };

        // If the current PK path already matches the actual PK attribute, no swap needed.
        if pk_name == pk_attr {
            return Ok(());
        }

        // Swap: current PK is actually the SK, and the SK condition holds the real PK.
        if let Some(SortKeyCondition::Compare {
            path: sk_path,
            op: CompareOp::Eq,
            value: sk_value,
        }) = self.sk_condition.take()
        {
            let old_pk_path = std::mem::replace(&mut self.pk_path, sk_path);
            let old_pk_value = std::mem::replace(&mut self.pk_value, sk_value);
            self.sk_condition = Some(SortKeyCondition::Compare {
                path: old_pk_path,
                op: CompareOp::Eq,
                value: old_pk_value,
            });
        }

        Ok(())
    }

    /// Reclassify conditions for multi-part key schemas.
    ///
    /// Given the full set of HASH attribute names, moves any equality conditions
    /// on HASH attributes from `sk_condition` into `extra_pk_conditions`.
    /// The remaining non-HASH equality or range condition stays as `sk_condition`.
    ///
    /// Call this after `resolve_pk_sk()` for multi-part key queries.
    pub fn resolve_multipart(
        &mut self,
        hash_attrs: &[&str],
        names: &std::collections::HashMap<String, String>,
    ) -> Result<(), DynamoDbError> {
        if hash_attrs.len() <= 1 {
            return Ok(());
        }

        // Collect all equality conditions (pk + extra_pk + sk if Eq)
        let mut all_eq: Vec<(Vec<PathElement>, Expr)> = Vec::new();
        all_eq.push((self.pk_path.clone(), self.pk_value.clone()));
        all_eq.append(&mut self.extra_pk_conditions);

        let mut sk_non_eq: Option<SortKeyCondition> = None;

        if let Some(sk) = self.sk_condition.take() {
            match sk {
                SortKeyCondition::Compare {
                    path,
                    op: CompareOp::Eq,
                    value,
                } => {
                    all_eq.push((path, value));
                }
                other => {
                    sk_non_eq = Some(other);
                }
            }
        }

        // Classify each equality condition as HASH or RANGE.
        // Index by position in hash_attrs so we can reorder to key schema order.
        let mut pk_by_position: Vec<(usize, Vec<PathElement>, Expr)> = Vec::new();
        let mut sk_eqs: Vec<(Vec<PathElement>, Expr)> = Vec::new();

        for (path, value) in all_eq {
            let attr_name = resolve_path_attr(&path, names)?;
            if let Some(pos) = hash_attrs.iter().position(|a| *a == attr_name.as_str()) {
                pk_by_position.push((pos, path, value));
            } else {
                sk_eqs.push((path, value));
            }
        }

        // Sort PK conditions by key schema order to ensure composite PK
        // concatenation matches the write path regardless of expression order.
        pk_by_position.sort_by_key(|(pos, _, _)| *pos);

        // Assign first PK condition as primary, rest as extra
        let mut pk_iter = pk_by_position
            .into_iter()
            .map(|(_, path, value)| (path, value));
        if let Some((first_path, first_value)) = pk_iter.next() {
            self.pk_path = first_path;
            self.pk_value = first_value;
            self.extra_pk_conditions = pk_iter.collect();
        }

        // Assign SK conditions: non-eq condition takes priority as primary SK,
        // otherwise first equality RANGE condition is primary, rest are extra.
        if let Some(non_eq) = sk_non_eq {
            self.sk_condition = Some(non_eq);
            self.extra_sk_conditions = sk_eqs;
        } else if let Some((path, value)) = sk_eqs.first().cloned() {
            self.sk_condition = Some(SortKeyCondition::Compare {
                path,
                op: CompareOp::Eq,
                value,
            });
            self.extra_sk_conditions = sk_eqs.into_iter().skip(1).collect();
        } else {
            self.sk_condition = None;
            self.extra_sk_conditions = Vec::new();
        };

        Ok(())
    }
}

/// Resolve a path's top-level attribute name, handling `#name` references.
fn resolve_path_attr(
    path: &[PathElement],
    names: &std::collections::HashMap<String, String>,
) -> Result<String, DynamoDbError> {
    match path.first() {
        Some(PathElement::Attribute(name)) => {
            if let Some(ref_name) = name.strip_prefix('#') {
                names.get(ref_name).cloned().ok_or_else(|| {
                    DynamoDbError::ValidationException(format!(
                        "An expression attribute name used in the document path \
                         is not defined; attribute name: #{ref_name}"
                    ))
                })
            } else {
                Ok(name.clone())
            }
        }
        _ => Err(DynamoDbError::ValidationException(
            "Invalid key condition path".to_owned(),
        )),
    }
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
    parser_common::check_redundant_parens(tokens)
        .map_err(|body| validation_err(&format!("Invalid KeyConditionExpression: {body}")))?;

    // Strip outer parentheses: "(pk = :pk AND sk > :sk)" → "pk = :pk AND sk > :sk"
    let tokens = if tokens.len() >= 2
        && tokens[0] == Token::LParen
        && tokens[tokens.len() - 1] == Token::RParen
        && outer_parens_match(tokens)
    {
        &tokens[1..tokens.len() - 1]
    } else {
        tokens
    };

    let mut pos = 0;
    let mut clauses = Vec::new();

    clauses.push(parse_key_clause(tokens, &mut pos)?);

    while pos < tokens.len() {
        if tokens[pos] != Token::And {
            return Err(validation_err(
                "Invalid KeyConditionExpression: only AND is supported between key conditions",
            ));
        }
        pos += 1;
        clauses.push(parse_key_clause(tokens, &mut pos)?);
    }

    if pos < tokens.len() {
        return Err(validation_err(
            "Invalid KeyConditionExpression: unexpected token after key conditions",
        ));
    }

    match clauses.len() {
        1 => classify_single(clauses.remove(0)),
        2 => classify_pair(clauses.remove(0), clauses.remove(0)),
        _ => classify_multi(clauses),
    }
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

fn parse_key_clause(tokens: &[Token], pos: &mut usize) -> Result<RawClause, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(validation_err(
            "Invalid KeyConditionExpression: unexpected end of expression",
        ));
    }

    // Allow one level of parentheses around a single clause
    if tokens[*pos] == Token::LParen {
        *pos += 1;
        let clause = parse_key_clause_inner(tokens, pos)?;
        if *pos < tokens.len() && tokens[*pos] == Token::RParen {
            *pos += 1;
            return Ok(clause);
        }
        return Err(validation_err(
            "Invalid KeyConditionExpression: expected ')'",
        ));
    }

    parse_key_clause_inner(tokens, pos)
}

fn parse_key_clause_inner(tokens: &[Token], pos: &mut usize) -> Result<RawClause, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(validation_err(
            "Invalid KeyConditionExpression: unexpected end of expression",
        ));
    }

    // begins_with(path, :val)
    if let Token::Ident(name) = &tokens[*pos] {
        if name.eq_ignore_ascii_case("begins_with")
            && *pos + 1 < tokens.len()
            && tokens[*pos + 1] == Token::LParen
        {
            *pos += 1; // skip function name
            parser_common::expect_token(
                tokens,
                pos,
                &Token::LParen,
                "(",
                "KeyConditionExpression",
            )?;
            let path = parse_key_operand_path(tokens, pos)?;
            parser_common::expect_token(tokens, pos, &Token::Comma, ",", "KeyConditionExpression")?;
            let prefix = parse_key_value(tokens, pos)?;
            parser_common::expect_token(
                tokens,
                pos,
                &Token::RParen,
                ")",
                "KeyConditionExpression",
            )?;
            return Ok(RawClause::BeginsWith(path, prefix));
        }
    }

    // path op :value | :value op path | path BETWEEN :lo AND :hi
    let left = parse_key_operand(tokens, pos)?;

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

            let path = parse_key_operand_path(tokens, pos)?;
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
        let low = parse_key_value(tokens, pos)?;
        parser_common::expect_token(tokens, pos, &Token::And, "AND", "KeyConditionExpression")?;
        let high = parse_key_value(tokens, pos)?;
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

    let value = parse_key_value(tokens, pos)?;

    if op == CompareOp::Eq {
        Ok(RawClause::Eq(path, value))
    } else {
        Ok(RawClause::Compare(path, op, value))
    }
}

fn parse_key_operand(tokens: &[Token], pos: &mut usize) -> Result<KeyOperand, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(validation_err(
            "Invalid KeyConditionExpression: unexpected end of expression",
        ));
    }

    match &tokens[*pos] {
        Token::Ident(_) | Token::NameRef(_) => {
            let path = parser_common::parse_path(tokens, pos)?;
            Ok(KeyOperand::Path(path))
        }
        Token::Placeholder(_) => {
            let value = parse_key_value(tokens, pos)?;
            Ok(KeyOperand::Value(value))
        }
        _ => Err(validation_err(
            "Invalid KeyConditionExpression: expected attribute path or value placeholder",
        )),
    }
}

fn parse_key_operand_path(
    tokens: &[Token],
    pos: &mut usize,
) -> Result<Vec<PathElement>, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(validation_err(
            "Invalid KeyConditionExpression: expected attribute path",
        ));
    }

    match &tokens[*pos] {
        Token::Ident(_) | Token::NameRef(_) => parser_common::parse_path(tokens, pos),
        _ => Err(validation_err(
            "Invalid KeyConditionExpression: expected attribute path",
        )),
    }
}

fn parse_key_value(tokens: &[Token], pos: &mut usize) -> Result<Expr, DynamoDbError> {
    if *pos >= tokens.len() {
        return Err(validation_err(
            "Invalid KeyConditionExpression: expected value",
        ));
    }
    match &tokens[*pos] {
        Token::Placeholder(name) => {
            let expr = Expr::Placeholder(name.clone());
            *pos += 1;
            Ok(expr)
        }
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
mod tests {
    use super::*;
    use crate::expression::tokenize;
    use std::collections::HashMap;

    #[test]
    fn key_condition_redundant_parens_rejected_with_canonical_message() {
        for expr in ["((pk = :v))", "((#pk = :pk)) AND (#sk = :sk)"] {
            let tokens = tokenize(expr).unwrap();
            let err = parse_key_condition(&tokens).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg)
                    if msg == "Invalid KeyConditionExpression: The expression has redundant parentheses;"),
                "expr {expr}: got {err:?}"
            );
        }
    }

    #[test]
    fn key_condition_valid_parens_accepted() {
        for expr in [
            "(pk = :v)",
            "(#pk = :pk) AND (#sk = :sk)",
            "(#pk = :pk AND #sk = :sk)",
            "(#pk = :pk AND (#sk = :sk))",
        ] {
            let tokens = tokenize(expr).unwrap();
            assert!(
                parse_key_condition(&tokens).is_ok(),
                "expr {expr} should parse"
            );
        }
    }

    #[test]
    fn resolve_pk_sk_swaps_when_reversed() {
        // "sk = :sk AND pk = :pk" — parser assigns first Eq as PK
        let tokens = tokenize("sk = :sk AND pk = :pk").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("sk".to_owned())]);

        kc.resolve_pk_sk("pk", &HashMap::new()).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
        match &kc.sk_condition {
            Some(SortKeyCondition::Compare {
                path,
                op: CompareOp::Eq,
                ..
            }) => {
                assert_eq!(path, &vec![PathElement::Attribute("sk".to_owned())]);
            }
            other => panic!("expected Eq SK condition, got {other:?}"),
        }
    }

    #[test]
    fn resolve_pk_sk_no_swap_when_correct() {
        let tokens = tokenize("pk = :pk AND sk = :sk").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);

        kc.resolve_pk_sk("pk", &HashMap::new()).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
    }

    #[test]
    fn resolve_pk_sk_with_name_refs() {
        let tokens = tokenize("#s = :sk AND #p = :pk").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();

        let mut names = HashMap::new();
        names.insert("s".to_owned(), "sortkey".to_owned());
        names.insert("p".to_owned(), "partkey".to_owned());

        kc.resolve_pk_sk("partkey", &names).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("#p".to_owned())]);
    }

    #[test]
    fn resolve_pk_sk_noop_for_non_eq_sk() {
        // "pk = :pk AND sk > :val" — SK is not Eq, no ambiguity
        let tokens = tokenize("pk = :pk AND sk > :val").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        kc.resolve_pk_sk("pk", &HashMap::new()).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
    }

    #[test]
    fn resolve_pk_sk_noop_for_single_clause() {
        let tokens = tokenize("pk = :pk").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        kc.resolve_pk_sk("pk", &HashMap::new()).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
    }

    #[test]
    fn parse_three_clause_multipart() {
        // pk1 = :v1 AND pk2 = :v2 AND sk = :v3
        let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk = :v3").unwrap();
        let kc = parse_key_condition(&tokens).unwrap();
        // First clause is pk_path, one extra_pk, one sk
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk1".to_owned())]);
        assert_eq!(kc.extra_pk_conditions.len(), 1);
        assert!(kc.sk_condition.is_some());
    }

    #[test]
    fn parse_four_clause_multipart() {
        let tokens = tokenize("a = :a AND b = :b AND c = :c AND d = :d").unwrap();
        let kc = parse_key_condition(&tokens).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("a".to_owned())]);
        assert_eq!(kc.extra_pk_conditions.len(), 2);
        assert!(kc.sk_condition.is_some());
    }

    #[test]
    fn parse_multipart_with_range_condition() {
        // pk1 = :v1 AND pk2 = :v2 AND sk > :v3
        let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk > :v3").unwrap();
        let kc = parse_key_condition(&tokens).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk1".to_owned())]);
        // pk2 is in extra_pk_conditions (will be reclassified by resolve_multipart)
        assert_eq!(kc.extra_pk_conditions.len(), 1);
        match &kc.sk_condition {
            Some(SortKeyCondition::Compare {
                op: CompareOp::Gt, ..
            }) => {}
            other => panic!("expected Gt SK condition, got {other:?}"),
        }
    }

    #[test]
    fn parse_reversed_sort_key_comparison() {
        let tokens = tokenize("pk = :pk AND :lo <= sk").unwrap();
        let kc = parse_key_condition(&tokens).unwrap();

        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
        match &kc.sk_condition {
            Some(SortKeyCondition::Compare {
                path,
                op: CompareOp::Ge,
                value: Expr::Placeholder(name),
            }) => {
                assert_eq!(path, &vec![PathElement::Attribute("sk".to_owned())]);
                assert_eq!(name, "lo");
            }
            other => panic!("expected reversed Ge SK condition, got {other:?}"),
        }
    }

    #[test]
    fn parse_reversed_partition_key_equality() {
        let tokens = tokenize(":pk = pk").unwrap();
        let kc = parse_key_condition(&tokens).unwrap();

        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk".to_owned())]);
        assert_eq!(kc.pk_value, Expr::Placeholder("pk".to_owned()));
    }

    #[test]
    fn reverse_comparator_maps_each_operator_and_is_involutive() {
        use CompareOp::{Eq, Ge, Gt, Le, Lt, Ne};

        let cases = [(Eq, Eq), (Ne, Ne), (Lt, Gt), (Le, Ge), (Gt, Lt), (Ge, Le)];

        for (op, reversed) in cases {
            assert_eq!(reverse_comparator(op), reversed);
            assert_eq!(reverse_comparator(reverse_comparator(op)), op);
        }
    }

    #[test]
    fn resolve_multipart_reclassifies() {
        let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk1 = :v3").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        let names = HashMap::new();
        kc.resolve_multipart(&["pk1", "pk2"], &names).unwrap();

        // After resolve_multipart, pk_path + extra_pk should have pk1 and pk2
        let pk_count = 1 + kc.extra_pk_conditions.len();
        assert_eq!(pk_count, 2);
        // sk_condition should be the sk1 equality
        assert!(kc.sk_condition.is_some());
    }

    #[test]
    fn resolve_multipart_with_range() {
        let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk1 > :v3").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        let names = HashMap::new();
        kc.resolve_multipart(&["pk1", "pk2"], &names).unwrap();

        assert_eq!(1 + kc.extra_pk_conditions.len(), 2);
        match &kc.sk_condition {
            Some(SortKeyCondition::Compare {
                op: CompareOp::Gt, ..
            }) => {}
            other => panic!("expected Gt SK condition, got {other:?}"),
        }
    }

    #[test]
    fn resolve_multipart_reorders_to_schema_order() {
        // Expression order: pk2, pk1, sk — reversed from key schema order [pk1, pk2]
        let tokens = tokenize("pk2 = :v2 AND pk1 = :v1 AND sk = :v3").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        let names = HashMap::new();
        kc.resolve_multipart(&["pk1", "pk2"], &names).unwrap();

        // After resolve_multipart, pk_path should be pk1 (first in schema)
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk1".to_owned())]);
        // extra_pk_conditions should have pk2
        assert_eq!(kc.extra_pk_conditions.len(), 1);
        assert_eq!(
            kc.extra_pk_conditions[0].0,
            vec![PathElement::Attribute("pk2".to_owned())]
        );
    }

    #[test]
    fn resolve_multipart_multi_range() {
        // 2 HASH + 2 RANGE, all equality conditions
        let tokens = tokenize("pk1 = :v1 AND pk2 = :v2 AND sk1 = :v3 AND sk2 = :v4").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        let names = HashMap::new();
        kc.resolve_multipart(&["pk1", "pk2"], &names).unwrap();

        // 2 PK conditions
        assert_eq!(1 + kc.extra_pk_conditions.len(), 2);
        // Primary SK condition is sk1
        match &kc.sk_condition {
            Some(SortKeyCondition::Compare {
                path,
                op: CompareOp::Eq,
                ..
            }) => {
                assert_eq!(path, &vec![PathElement::Attribute("sk1".to_owned())]);
            }
            other => panic!("expected Eq SK condition on sk1, got {other:?}"),
        }
        // Extra SK condition is sk2
        assert_eq!(kc.extra_sk_conditions.len(), 1);
        assert_eq!(
            kc.extra_sk_conditions[0].0,
            vec![PathElement::Attribute("sk2".to_owned())]
        );
    }
}

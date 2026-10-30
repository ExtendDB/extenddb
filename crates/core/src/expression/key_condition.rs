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
                            "Invalid KeyConditionExpression: An expression attribute name used in \
                             the document path is not defined; attribute name: #{ref_name}"
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

    /// Classify and validate the conditions of a multi-attribute key schema.
    ///
    /// A multi-attribute key schema has more than one HASH attribute, more
    /// than one RANGE attribute, or both. `hash_attrs` and `range_attrs` are
    /// the index's HASH and RANGE attribute names in key-schema order. The
    /// parser cannot tell HASH from RANGE equalities, and clause order in the
    /// expression is free, so every clause is assigned here by name.
    ///
    /// The rules the service applies:
    ///
    /// - Each key attribute may carry one condition; a second one is refused
    ///   as `KeyConditionExpressions must only contain one condition per key`.
    /// - Every HASH attribute needs an equality condition. Every one without
    ///   it is named, in key-schema order, in `HASH key attributes ... must
    ///   have equality conditions specified in the query`.
    /// - Conditions on RANGE attributes must cover a leading run of them in
    ///   key-schema order. Every RANGE attribute before the last one that has
    ///   a condition needs an equality; only that last one may use `<`, `<=`,
    ///   `>`, `>=`, `BETWEEN` or `begins_with`. `h = :h AND b = :b` on a schema
    ///   whose RANGE attributes are `a, b` is refused, and so is
    ///   `h = :h AND a = :a AND b > :b AND c = :c`.
    ///
    /// On return the HASH conditions are in key-schema order (`pk_path` and
    /// `extra_pk_conditions`, matching the composite partition key the write
    /// path builds). When a RANGE attribute carries a non-equality condition,
    /// that condition is `sk_condition` and the equalities before it are
    /// `extra_sk_conditions`; otherwise `sk_condition` is the equality on the
    /// first RANGE attribute and the rest follow in `extra_sk_conditions`.
    /// Either way `extra_sk_conditions` is in key-schema order. A storage
    /// backend maps each condition to its column by attribute name.
    ///
    /// Call this instead of [`KeyCondition::resolve_pk_sk`] when the key
    /// schema is multi-attribute. A schema with one HASH and at most one
    /// RANGE attribute is left unchanged.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` when a `#name` reference cannot be
    /// resolved, a HASH attribute has no equality condition, the RANGE
    /// conditions skip an attribute or put a non-equality before the last
    /// one, or a non-equality names a HASH attribute, or a non-key attribute
    /// while a RANGE attribute also has a condition.
    pub fn resolve_multipart(
        &mut self,
        hash_attrs: &[&str],
        range_attrs: &[&str],
        names: &std::collections::HashMap<String, String>,
    ) -> Result<(), DynamoDbError> {
        self.resolve_multipart_with(hash_attrs, range_attrs, names, |_, _| Ok(()))
    }

    /// [`Self::resolve_multipart`] with a check on every clause's value,
    /// called with the clause's path and value after the structural rules
    /// (one condition per key, condition count, HASH coverage, RANGE gap) and
    /// before the non-key rule. That is where the service reports a value of
    /// the wrong type (measured 2026-10-21: a wrong-typed value loses to a
    /// duplicate, a count overflow, a missing HASH attribute and a RANGE gap,
    /// and wins over a condition on a non-key attribute).
    ///
    /// # Errors
    ///
    /// As [`Self::resolve_multipart`], plus whatever `value_check` returns.
    pub fn resolve_multipart_with<F>(
        &mut self,
        hash_attrs: &[&str],
        range_attrs: &[&str],
        names: &std::collections::HashMap<String, String>,
        mut value_check: F,
    ) -> Result<(), DynamoDbError>
    where
        F: FnMut(&[PathElement], &Expr) -> Result<(), DynamoDbError>,
    {
        if hash_attrs.len() <= 1 && range_attrs.len() <= 1 {
            return Ok(());
        }

        // Every equality clause, whichever field the parser put it in.
        let mut all_eq: Vec<(Vec<PathElement>, Expr)> = Vec::new();
        all_eq.push((self.pk_path.clone(), self.pk_value.clone()));
        all_eq.append(&mut self.extra_pk_conditions);
        all_eq.append(&mut self.extra_sk_conditions);

        // The parser allows at most one non-equality clause.
        let mut non_eq: Option<SortKeyCondition> = None;
        if let Some(sk) = self.sk_condition.take() {
            match sk {
                SortKeyCondition::Compare {
                    path,
                    op: CompareOp::Eq,
                    value,
                } => all_eq.push((path, value)),
                other => non_eq = Some(other),
            }
        }

        let mut hash_eqs: Vec<(usize, Vec<PathElement>, Expr)> = Vec::new();
        let mut range_eqs: Vec<(usize, Vec<PathElement>, Expr)> = Vec::new();
        let mut other_eqs: Vec<(Vec<PathElement>, Expr)> = Vec::new();
        for (path, value) in all_eq {
            let attr_name = resolve_path_attr(&path, names)?;
            if let Some(pos) = hash_attrs.iter().position(|a| *a == attr_name) {
                hash_eqs.push((pos, path, value));
            } else if let Some(pos) = range_attrs.iter().position(|a| *a == attr_name) {
                range_eqs.push((pos, path, value));
            } else {
                other_eqs.push((path, value));
            }
        }

        // The non-equality clause's attribute: its RANGE position when it
        // names a RANGE attribute, its HASH position when it names a HASH
        // attribute, neither when it names a non-key attribute.
        let non_eq_attr = match &non_eq {
            Some(cond) => Some(resolve_path_attr(cond.path(), names)?),
            None => None,
        };
        let non_eq_pos = non_eq_attr
            .as_deref()
            .and_then(|a| range_attrs.iter().position(|r| *r == a));
        let non_eq_hash_pos = non_eq_attr
            .as_deref()
            .and_then(|a| hash_attrs.iter().position(|h| *h == a));

        // One condition per key attribute. A second condition on one HASH or
        // RANGE attribute, equality or not, is refused before any coverage
        // check (measured on the service, 2026-10-21).
        hash_eqs.sort_by_key(|(pos, _, _)| *pos);
        range_eqs.sort_by_key(|(pos, _, _)| *pos);
        let duplicated = hash_eqs.windows(2).any(|w| w[0].0 == w[1].0)
            || range_eqs.windows(2).any(|w| w[0].0 == w[1].0)
            || non_eq_pos.is_some_and(|p| range_eqs.iter().any(|(q, _, _)| *q == p))
            || non_eq_hash_pos.is_some_and(|p| hash_eqs.iter().any(|(q, _, _)| *q == p));
        if duplicated {
            return Err(DynamoDbError::ValidationException(
                "Invalid KeyConditionExpression: KeyConditionExpressions must only contain \
                 one condition per key"
                    .to_string(),
            ));
        }

        // More conditions than key attributes (measured 2026-10-21: the count
        // is reported ahead of which attribute is missing).
        let condition_count =
            hash_eqs.len() + range_eqs.len() + other_eqs.len() + usize::from(non_eq.is_some());
        let key_attribute_count = hash_attrs.len() + range_attrs.len();
        if condition_count > key_attribute_count {
            return Err(DynamoDbError::ValidationException(format!(
                "The number of query conditions ({condition_count}) exceeds the number of key \
                 attributes defined in the schema ({key_attribute_count})"
            )));
        }

        // Every HASH attribute needs an equality condition. On a
        // multi-attribute key schema the service names every HASH attribute
        // without one, in key-schema order (measured 2026-10-21); the
        // single-attribute `Query condition missed key schema element`
        // wording belongs to the single-attribute path in the engine.
        let missing_hash: Vec<&str> = hash_attrs
            .iter()
            .enumerate()
            .filter(|(pos, _)| !hash_eqs.iter().any(|(p, _, _)| p == pos))
            .map(|(_, attr)| *attr)
            .collect();
        if !missing_hash.is_empty() {
            return Err(DynamoDbError::ValidationException(format!(
                "HASH key attributes {} must have equality conditions specified in the query",
                missing_hash.join(", ")
            )));
        }

        // RANGE conditions must cover a leading run of the RANGE attributes,
        // with an equality on every one before the last.
        let last_pos = range_eqs.iter().map(|(p, _, _)| *p).chain(non_eq_pos).max();
        if let Some(last_pos) = last_pos {
            let without_eq: Vec<&str> = range_attrs[..last_pos]
                .iter()
                .enumerate()
                .filter(|(pos, _)| !range_eqs.iter().any(|(p, _, _)| p == pos))
                .map(|(_, attr)| *attr)
                .collect();
            if !without_eq.is_empty() {
                return Err(DynamoDbError::ValidationException(format!(
                    "RANGE key attributes {} must have equality conditions specified in the \
                     query because a condition is present on key attribute {}",
                    without_eq.join(", "),
                    range_attrs[last_pos]
                )));
            }
        }

        for (_, path, value) in hash_eqs.iter().chain(range_eqs.iter()) {
            value_check(path, value)?;
        }
        for (path, value) in &other_eqs {
            value_check(path, value)?;
        }
        match &non_eq {
            Some(SortKeyCondition::Compare { path, value, .. }) => value_check(path, value)?,
            Some(SortKeyCondition::Between { path, low, high }) => {
                value_check(path, low)?;
                value_check(path, high)?;
            }
            Some(SortKeyCondition::BeginsWith { path, prefix }) => value_check(path, prefix)?,
            None => {}
        }

        // A condition on an attribute outside the index key schema. The
        // service refuses it by naming the first RANGE attribute that has no
        // condition (measured 2026-10-21: `h = :h AND z = :z` on RANGE
        // `a, b, c, d` is `Query condition missed key schema element: a`;
        // with `a` given, `b`). The count check above has already refused
        // the case where every key attribute has a condition as well.
        let non_key_non_eq = non_eq.is_some() && non_eq_pos.is_none() && non_eq_hash_pos.is_none();
        if !other_eqs.is_empty() || non_key_non_eq {
            let missing_range = range_attrs
                .iter()
                .enumerate()
                .find(|(pos, _)| {
                    !range_eqs.iter().any(|(p, _, _)| p == pos) && non_eq_pos != Some(*pos)
                })
                .map(|(_, attr)| *attr)
                .unwrap_or(hash_attrs[0]);
            return Err(DynamoDbError::ValidationException(format!(
                "Query condition missed key schema element: {missing_range}"
            )));
        }

        let mut hash_iter = hash_eqs.into_iter().map(|(_, path, value)| (path, value));
        if let Some((first_path, first_value)) = hash_iter.next() {
            self.pk_path = first_path;
            self.pk_value = first_value;
            self.extra_pk_conditions = hash_iter.collect();
        }

        let mut range_eqs: Vec<(Vec<PathElement>, Expr)> = range_eqs
            .into_iter()
            .map(|(_, path, value)| (path, value))
            .collect();

        if let Some(cond) = non_eq {
            self.sk_condition = Some(cond);
        } else if range_eqs.is_empty() {
            self.sk_condition = None;
        } else {
            let (path, value) = range_eqs.remove(0);
            self.sk_condition = Some(SortKeyCondition::Compare {
                path,
                op: CompareOp::Eq,
                value,
            });
        }
        self.extra_sk_conditions = range_eqs;

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
                        "Invalid KeyConditionExpression: An expression attribute name used in \
                         the document path is not defined; attribute name: #{ref_name}"
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

impl SortKeyCondition {
    /// The attribute path the condition applies to.
    #[must_use]
    pub fn path(&self) -> &[PathElement] {
        match self {
            Self::Compare { path, .. }
            | Self::Between { path, .. }
            | Self::BeginsWith { path, .. } => path,
        }
    }
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
    let mut clauses = parse_clauses(tokens)?;

    match clauses.len() {
        1 => classify_single(clauses.remove(0)),
        2 => classify_pair(clauses.remove(0), clauses.remove(0)),
        _ => classify_multi(clauses),
    }
}

/// The attribute path and single value operand of each clause of a
/// `KeyConditionExpression`, and whether the clause compares by equality.
///
/// Authorization reads this to find the value a key condition names for the
/// partition key when the operator makes the shape one the engine rejects, and
/// to tell that case apart from an equality on the partition key whose value
/// it cannot read. A `BETWEEN` clause carries two operands and is left out,
/// since it names no single value for the attribute.
///
/// # Errors
///
/// Returns `ValidationException` for the same syntax errors and unsupported
/// constructs [`parse_key_condition`] rejects before it classifies clauses.
pub fn key_condition_clause_operands(
    tokens: &[Token],
) -> Result<Vec<KeyClauseOperand>, DynamoDbError> {
    Ok(parse_clauses(tokens)?
        .into_iter()
        .filter_map(|clause| match clause {
            RawClause::Eq(path, value) => Some(KeyClauseOperand {
                path,
                value,
                equality: true,
            }),
            RawClause::Compare(path, _, value) | RawClause::BeginsWith(path, value) => {
                Some(KeyClauseOperand {
                    path,
                    value,
                    equality: false,
                })
            }
            RawClause::Between(..) => None,
        })
        .collect())
}

/// One clause of a `KeyConditionExpression`, as [`key_condition_clause_operands`]
/// returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyClauseOperand {
    /// The attribute side of the clause, as written.
    pub path: Vec<PathElement>,
    /// The single value operand.
    pub value: Expr,
    /// Whether the clause is `attribute = value`.
    pub equality: bool,
}

/// Parse a `KeyConditionExpression` token stream into its AND-ed clauses,
/// before any of them is read as the partition or the sort key condition.
fn parse_clauses(tokens: &[Token]) -> Result<Vec<RawClause>, DynamoDbError> {
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

    // A key condition may only reference a top-level key attribute. Any document
    // path (`attr.sub` or `attr[i]`) on a key is rejected, matching Amazon
    // DynamoDB. Runs after the reserved-word check and before key-schema checks.
    for clause in &clauses {
        reject_nested_key_path(raw_clause_path(clause))?;
    }

    Ok(clauses)
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
    if let Token::Ident(name) = &tokens[*pos]
        && name.eq_ignore_ascii_case("begins_with")
        && *pos + 1 < tokens.len()
        && tokens[*pos + 1] == Token::LParen
    {
        *pos += 1; // skip function name
        parser_common::expect_token(tokens, pos, &Token::LParen, "(", "KeyConditionExpression")?;
        let path = parse_key_operand_path(tokens, pos)?;
        parser_common::expect_token(tokens, pos, &Token::Comma, ",", "KeyConditionExpression")?;
        let prefix = parse_key_value(tokens, pos)?;
        parser_common::expect_token(tokens, pos, &Token::RParen, ")", "KeyConditionExpression")?;
        return Ok(RawClause::BeginsWith(path, prefix));
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
        kc.resolve_multipart(&["pk1", "pk2"], &["sk1"], &names)
            .unwrap();

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
        kc.resolve_multipart(&["pk1", "pk2"], &["sk1"], &names)
            .unwrap();

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
        kc.resolve_multipart(&["pk1", "pk2"], &["sk"], &names)
            .unwrap();

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
        kc.resolve_multipart(&["pk1", "pk2"], &["sk1", "sk2"], &names)
            .unwrap();

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

    const NESTED_KEY_ERR: &str = "Invalid KeyConditionExpression: KeyConditionExpressions cannot have conditions on nested attributes";

    #[test]
    fn nested_dot_path_on_key_rejected() {
        for expr in [
            "pk.foo = :v",
            "pk = :p AND sk.foo = :v",
            "begins_with(sk.foo, :v)",
            "foo.bar = :v",
        ] {
            let tokens = tokenize(expr).unwrap();
            let err = parse_key_condition(&tokens).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if msg == NESTED_KEY_ERR),
                "expr {expr}: got {err:?}"
            );
        }
    }

    #[test]
    fn nested_path_in_all_clause_shapes_rejected() {
        // Exercises every `raw_clause_path` arm (Compare/Between/BeginsWith) and a
        // 3-clause multi-part shape with a nested middle clause. Multi-part keys
        // are an ExtendDB extension real DynamoDB cannot express, hence a
        // parser-level test. The reject loop runs before classification, so a
        // nested clause in any position is rejected up front.
        for expr in [
            "pk.foo > :v",                            // single non-eq (Compare arm)
            "pk = :p AND sk.foo BETWEEN :a AND :b",   // Between arm
            "pk = :p AND begins_with(sk[0], :v)",     // BeginsWith arm, index
            "pk = :p AND :v < sk.foo",                // reversed value-first
            "pk1 = :v1 AND pk2.x = :v2 AND sk = :v3", // multi-part, nested middle
        ] {
            let tokens = tokenize(expr).unwrap();
            let err = parse_key_condition(&tokens).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if msg == NESTED_KEY_ERR),
                "expr {expr}: got {err:?}"
            );
        }
    }

    #[test]
    fn index_path_on_key_rejected() {
        for expr in ["pk[0] = :v", "pk = :p AND sk[0] = :v"] {
            let tokens = tokenize(expr).unwrap();
            let err = parse_key_condition(&tokens).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if msg == NESTED_KEY_ERR),
                "expr {expr}: got {err:?}"
            );
        }
    }

    #[test]
    fn nested_path_via_name_placeholder_rejected() {
        // The check is structural (path length), so it fires before #name
        // resolution.
        let tokens = tokenize("#d.foo = :v").unwrap();
        let err = parse_key_condition(&tokens).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if msg == NESTED_KEY_ERR),
            "got {err:?}"
        );
    }

    #[test]
    fn top_level_key_paths_accepted() {
        for expr in ["pk = :v", "pk = :p AND sk = :s", "#pk = :p AND #sk = :s"] {
            let tokens = tokenize(expr).unwrap();
            assert!(
                parse_key_condition(&tokens).is_ok(),
                "expr {expr} should parse"
            );
        }
    }

    /// One expected clause: attribute, operand placeholder, equality.
    type ExpectedClause = (&'static str, &'static str, bool);

    /// The attribute and operand of each clause, and whether it is an
    /// equality, which is what authorization reads to find the partition value
    /// of a key condition the engine rejects for its operator.
    #[test]
    fn clause_operands_name_every_single_operand_clause() {
        let cases: &[(&str, &[ExpectedClause])] = &[
            ("pk = :v", &[("pk", "v", true)]),
            (
                "pk = :v AND sk = :s",
                &[("pk", "v", true), ("sk", "s", true)],
            ),
            ("pk > :v", &[("pk", "v", false)]),
            (":v > pk", &[("pk", "v", false)]),
            ("begins_with(pk, :v)", &[("pk", "v", false)]),
            ("(pk >= :v)", &[("pk", "v", false)]),
            ("#p <= :v", &[("#p", "v", false)]),
            (
                "pk = :v AND sk < :s",
                &[("pk", "v", true), ("sk", "s", false)],
            ),
            (
                "pk > :v AND sk = :s",
                &[("pk", "v", false), ("sk", "s", true)],
            ),
            (
                "pk < :v AND sk > :s",
                &[("pk", "v", false), ("sk", "s", false)],
            ),
            (
                "pk = :u AND pk > :v",
                &[("pk", "u", true), ("pk", "v", false)],
            ),
            // BETWEEN names two operands, so it contributes none.
            ("pk BETWEEN :lo AND :hi", &[]),
            ("pk = :v AND sk BETWEEN :lo AND :hi", &[("pk", "v", true)]),
        ];
        for (expr, want) in cases {
            let tokens = tokenize(expr).unwrap();
            let got = key_condition_clause_operands(&tokens).unwrap();
            let got: Vec<(String, String, bool)> = got
                .into_iter()
                .map(|clause| {
                    let name = match &clause.path[0] {
                        PathElement::Attribute(n) => n.clone(),
                        PathElement::Index(i) => format!("[{i}]"),
                    };
                    let value = match clause.value {
                        Expr::Placeholder(n) => n,
                        other => format!("{other:?}"),
                    };
                    (name, value, clause.equality)
                })
                .collect();
            let want: Vec<(String, String, bool)> = want
                .iter()
                .map(|(a, v, e)| ((*a).to_owned(), (*v).to_owned(), *e))
                .collect();
            assert_eq!(got, want, "expr {expr}");
        }
    }

    /// The same syntax errors and unsupported constructs `parse_key_condition`
    /// rejects before classification.
    #[test]
    fn clause_operands_reject_what_the_parser_rejects() {
        for expr in [
            "pk :: :v",
            "pk > :v OR sk = :s",
            "((pk = :v))",
            "pk.sub > :v",
            "pk <> :v",
            "pk >",
        ] {
            let Ok(tokens) = tokenize(expr) else {
                continue;
            };
            assert!(
                key_condition_clause_operands(&tokens).is_err(),
                "expr {expr} should be rejected"
            );
        }
    }

    /// The index key schema the single-HASH cases use: HASH `h`, then RANGE
    /// `a`, `b`, `c`, `d`.
    const ONE_HASH: [&str; 1] = ["h"];
    const FOUR_RANGE: [&str; 4] = ["a", "b", "c", "d"];

    /// Parse `expr` and classify it against `h` + `a, b, c, d`.
    fn classify_one_hash(expr: &str) -> Result<KeyCondition, DynamoDbError> {
        classify_with_names(expr, &HashMap::new())
    }

    fn classify_with_names(
        expr: &str,
        names: &HashMap<String, String>,
    ) -> Result<KeyCondition, DynamoDbError> {
        let tokens = tokenize(expr).unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        kc.resolve_multipart(&ONE_HASH, &FOUR_RANGE, names)?;
        Ok(kc)
    }

    /// The attribute names of `extra_sk_conditions`, in order.
    fn extra_sk_names(kc: &KeyCondition) -> Vec<String> {
        kc.extra_sk_conditions
            .iter()
            .map(|(path, _)| match path.first() {
                Some(PathElement::Attribute(name)) => name.clone(),
                other => panic!("unexpected path {other:?}"),
            })
            .collect()
    }

    /// The sort-key condition's attribute and a label for its operator.
    fn sk_shape(kc: &KeyCondition) -> Option<(String, &'static str)> {
        let cond = kc.sk_condition.as_ref()?;
        let name = match cond.path().first() {
            Some(PathElement::Attribute(name)) => name.clone(),
            other => panic!("unexpected path {other:?}"),
        };
        let op = match cond {
            SortKeyCondition::Compare { op, .. } => match op {
                CompareOp::Eq => "=",
                CompareOp::Ne => "<>",
                CompareOp::Lt => "<",
                CompareOp::Le => "<=",
                CompareOp::Gt => ">",
                CompareOp::Ge => ">=",
            },
            SortKeyCondition::Between { .. } => "BETWEEN",
            SortKeyCondition::BeginsWith { .. } => "begins_with",
        };
        Some((name, op))
    }

    fn assert_range_gap(expr: &str, without_eq: &str, present: &str) {
        let err = classify_one_hash(expr).unwrap_err();
        let want = format!(
            "RANGE key attributes {without_eq} must have equality conditions specified in the \
             query because a condition is present on key attribute {present}"
        );
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
            "expr {expr}: got {err:?}, want {want}"
        );
    }

    #[test]
    fn one_hash_multi_range_hash_only_condition_is_accepted() {
        let kc = classify_one_hash("h = :h").unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("h".to_owned())]);
        assert!(kc.extra_pk_conditions.is_empty());
        assert!(kc.sk_condition.is_none());
        assert!(kc.extra_sk_conditions.is_empty());
    }

    #[test]
    fn one_hash_multi_range_equality_prefixes_are_accepted() {
        // Equalities on a leading run of the RANGE attributes, up to all four.
        for (expr, extra) in [
            ("h = :h AND a = :a", vec![]),
            ("h = :h AND a = :a AND b = :b", vec!["b"]),
            ("h = :h AND a = :a AND b = :b AND c = :c", vec!["b", "c"]),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND d = :d",
                vec!["b", "c", "d"],
            ),
        ] {
            let kc = classify_one_hash(expr).unwrap_or_else(|e| panic!("{expr}: {e:?}"));
            assert_eq!(
                kc.pk_path,
                vec![PathElement::Attribute("h".to_owned())],
                "{expr}"
            );
            assert!(kc.extra_pk_conditions.is_empty(), "{expr}");
            assert_eq!(sk_shape(&kc), Some(("a".to_owned(), "=")), "{expr}");
            assert_eq!(extra_sk_names(&kc), extra, "{expr}");
        }
    }

    #[test]
    fn one_hash_multi_range_clause_order_is_free() {
        // The defect: the first clause was taken as the partition key, so
        // `b = :b AND h = :h AND a = :a` was refused as missing `h`.
        for expr in [
            "b = :b AND h = :h AND a = :a",
            "a = :a AND b = :b AND h = :h",
            "b = :b AND a = :a AND h = :h",
        ] {
            let kc = classify_one_hash(expr).unwrap_or_else(|e| panic!("{expr}: {e:?}"));
            assert_eq!(
                kc.pk_path,
                vec![PathElement::Attribute("h".to_owned())],
                "{expr}"
            );
            assert_eq!(sk_shape(&kc), Some(("a".to_owned(), "=")), "{expr}");
            assert_eq!(extra_sk_names(&kc), vec!["b"], "{expr}");
        }
    }

    #[test]
    fn one_hash_multi_range_operator_on_the_last_given_attribute_is_accepted() {
        for (expr, sk, extra) in [
            ("h = :h AND a > :a", ("a", ">"), vec![]),
            ("h = :h AND a = :a AND b < :b", ("b", "<"), vec!["a"]),
            (
                "h = :h AND a = :a AND b = :b AND c > :c",
                ("c", ">"),
                vec!["a", "b"],
            ),
            (
                "h = :h AND a = :a AND begins_with(b, :b)",
                ("b", "begins_with"),
                vec!["a"],
            ),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND begins_with(d, :d)",
                ("d", "begins_with"),
                vec!["a", "b", "c"],
            ),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND d BETWEEN :x AND :y",
                ("d", "BETWEEN"),
                vec!["a", "b", "c"],
            ),
            (
                "c > :c AND b = :b AND h = :h AND a = :a",
                ("c", ">"),
                vec!["a", "b"],
            ),
        ] {
            let kc = classify_one_hash(expr).unwrap_or_else(|e| panic!("{expr}: {e:?}"));
            assert_eq!(
                kc.pk_path,
                vec![PathElement::Attribute("h".to_owned())],
                "{expr}"
            );
            assert_eq!(sk_shape(&kc), Some((sk.0.to_owned(), sk.1)), "{expr}");
            assert_eq!(extra_sk_names(&kc), extra, "{expr}");
        }
    }

    #[test]
    fn one_hash_multi_range_gap_is_refused() {
        // A condition on a RANGE attribute needs an equality on every RANGE
        // attribute before it. The defect accepted `h = :h AND b = :b` and
        // returned no items.
        assert_range_gap("h = :h AND b = :b", "a", "b");
        assert_range_gap("h = :h AND a = :a AND c = :c", "b", "c");
        assert_range_gap("h = :h AND a = :a AND b = :b AND d > :d", "c", "d");
    }

    #[test]
    fn one_hash_multi_range_operator_before_a_later_condition_is_refused() {
        assert_range_gap("h = :h AND a = :a AND b > :b AND c = :c", "b", "c");
        assert_range_gap("h = :h AND begins_with(a, :a) AND b = :b", "a", "b");
    }

    #[test]
    fn one_hash_multi_range_missing_hash_is_refused() {
        for expr in [
            "a = :a AND b = :b AND c = :c",
            "a = :a AND b = :b",
            "a = :a",
        ] {
            let err = classify_one_hash(expr).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg)
                    if msg == "HASH key attributes h must have equality conditions specified in the query"),
                "expr {expr}: got {err:?}"
            );
        }
    }

    /// Classify against HASH `h1`, `h2` and RANGE `r`.
    fn classify_two_hash(expr: &str) -> Result<KeyCondition, DynamoDbError> {
        let tokens = tokenize(expr).unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        kc.resolve_multipart(&["h1", "h2"], &["r"], &HashMap::new())?;
        Ok(kc)
    }

    #[test]
    fn two_hash_missing_hash_names_every_missing_attribute() {
        for (expr, missing) in [
            ("h1 = :x", "h2"),
            ("h2 = :x", "h1"),
            ("h1 = :x AND r = :y", "h2"),
            ("r = :y", "h1, h2"),
            ("h1 = :x AND h2 > :y", "h2"),
        ] {
            let err = classify_two_hash(expr).unwrap_err();
            let want = format!(
                "HASH key attributes {missing} must have equality conditions specified in the query"
            );
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
                "expr {expr}: got {err:?}"
            );
        }
        assert!(classify_two_hash("h1 = :x AND h2 = :y").is_ok());
        assert!(classify_two_hash("h1 = :x AND h2 = :y AND r > :z").is_ok());
    }

    #[test]
    fn a_second_condition_on_one_key_attribute_is_refused() {
        const WANT: &str = "Invalid KeyConditionExpression: KeyConditionExpressions must only \
                            contain one condition per key";
        for expr in [
            "h = :x AND h = :y",
            "h = :x AND h = :y AND a = :z",
            "h = :x AND a = :y AND a = :z",
            "h = :h AND a = :a AND a > :x",
            "h = :h AND a > :x AND a = :a",
            "h = :h AND a = :a AND b = :n AND b > :lo",
            "h = :h AND h > :x",
            "h = :h AND h = :y AND c = :c",
            "a = :a AND a = :y",
        ] {
            let err = classify_one_hash(expr).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if msg == WANT),
                "expr {expr}: got {err:?}"
            );
        }
        for expr in [
            "h1 = :x AND h1 = :y AND h2 = :z",
            "h1 = :x AND h1 = :y",
            "h1 = :x AND h2 = :n AND r = :r AND r > :y",
            "h1 = :x AND r = :r AND r = :y",
        ] {
            let err = classify_two_hash(expr).unwrap_err();
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if msg == WANT),
                "expr {expr}: got {err:?}"
            );
        }
    }

    #[test]
    fn a_condition_on_a_non_key_attribute_names_the_first_uncovered_range_attribute() {
        for (expr, missing) in [
            ("h = :h AND z = :z", "a"),
            ("h = :h AND z = :z AND y = :y", "a"),
            ("h = :h AND z > :z", "a"),
            ("h = :h AND a = :a AND z = :z", "b"),
            ("h = :h AND a = :a AND z > :z", "b"),
            ("h = :h AND a > :a AND z = :z", "b"),
            ("h = :h AND a = :a AND b = :b AND z > :z", "c"),
            ("h = :h AND a = :a AND b = :b AND c = :c AND z = :z", "d"),
        ] {
            let err = classify_one_hash(expr).unwrap_err();
            let want = format!("Query condition missed key schema element: {missing}");
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
                "expr {expr}: got {err:?}"
            );
        }
        let err = classify_two_hash("h1 = :x AND h2 = :y AND z = :z").unwrap_err();
        assert!(matches!(&err, DynamoDbError::ValidationException(msg)
            if msg == "Query condition missed key schema element: r"));
        // A gap is reported before the non-key attribute.
        let err = classify_one_hash("h = :h AND b = :b AND z = :z").unwrap_err();
        assert!(matches!(&err, DynamoDbError::ValidationException(msg)
            if msg.starts_with("RANGE key attributes a must have equality conditions")));
        // A missing HASH attribute is reported before the non-key attribute.
        let err = classify_two_hash("h1 = :x AND z = :z").unwrap_err();
        assert!(matches!(&err, DynamoDbError::ValidationException(msg)
            if msg == "HASH key attributes h2 must have equality conditions specified in the query"));
    }

    #[test]
    fn more_conditions_than_key_attributes_are_refused_by_count() {
        for (expr, n) in [
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND d = :d AND z = :z",
                6,
            ),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND d = :d AND z > :z",
                6,
            ),
            (
                "h = :h AND a = :a AND b = :b AND c = :c AND d = :d AND PK = :p",
                6,
            ),
        ] {
            let err = classify_one_hash(expr).unwrap_err();
            let want = format!(
                "The number of query conditions ({n}) exceeds the number of key attributes \
                 defined in the schema (5)"
            );
            assert!(
                matches!(&err, DynamoDbError::ValidationException(msg) if *msg == want),
                "expr {expr}: got {err:?}"
            );
        }
        let err = classify_two_hash("h1 = :x AND h2 = :y AND r = :r AND z = :z").unwrap_err();
        assert!(matches!(&err, DynamoDbError::ValidationException(msg)
            if msg == "The number of query conditions (4) exceeds the number of key attributes defined in the schema (3)"));
    }

    #[test]
    fn one_hash_multi_range_resolves_name_references() {
        let names: HashMap<String, String> = [("hh", "h"), ("aa", "a"), ("bb", "b")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        let kc = classify_with_names("#bb > :b AND #hh = :h AND #aa = :a", &names).unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("#hh".to_owned())]);
        assert_eq!(sk_shape(&kc), Some(("#bb".to_owned(), ">")));
        assert_eq!(extra_sk_names(&kc), vec!["#aa"]);

        let err = classify_with_names("#hh = :h AND #bb = :b", &names).unwrap_err();
        assert!(
            matches!(&err, DynamoDbError::ValidationException(msg)
                if msg.starts_with("RANGE key attributes a must have")),
            "got {err:?}"
        );
    }

    #[test]
    fn one_hash_one_range_schema_is_left_unchanged() {
        let tokens = tokenize("sk = :s AND pk = :p").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        kc.resolve_multipart(&["pk"], &["sk"], &HashMap::new())
            .unwrap();
        // Still the parser's assignment; resolve_pk_sk handles this schema.
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("sk".to_owned())]);
    }

    #[test]
    fn multi_hash_partition_key_need_not_come_first() {
        // With two HASH attributes the first clause used to be checked as the
        // first HASH attribute before reclassification, so this order was
        // refused as missing `pk1`.
        let tokens = tokenize("sk = :s AND pk1 = :a AND pk2 = :b").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        kc.resolve_multipart(&["pk1", "pk2"], &["sk"], &HashMap::new())
            .unwrap();
        assert_eq!(kc.pk_path, vec![PathElement::Attribute("pk1".to_owned())]);
        assert_eq!(
            kc.extra_pk_conditions[0].0,
            vec![PathElement::Attribute("pk2".to_owned())]
        );
        assert_eq!(sk_shape(&kc), Some(("sk".to_owned(), "=")));
        assert!(kc.extra_sk_conditions.is_empty());
    }

    #[test]
    fn multi_hash_multi_range_operator_after_equality_keeps_both() {
        // The non-equality on the second RANGE attribute is the sort-key
        // condition, and the equality on the first is carried with it.
        let tokens = tokenize("pk1 = :a AND pk2 = :b AND s1 = :x AND s2 > :y").unwrap();
        let mut kc = parse_key_condition(&tokens).unwrap();
        kc.resolve_multipart(&["pk1", "pk2"], &["s1", "s2"], &HashMap::new())
            .unwrap();
        assert_eq!(sk_shape(&kc), Some(("s2".to_owned(), ">")));
        assert_eq!(extra_sk_names(&kc), vec!["s1"]);
    }
}

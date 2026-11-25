// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The extended `SearchConditionExpression` grammar of `SearchVectors`.
//!
//! With the grammar enabled for a request, a search condition takes the
//! comparators `=`, `<>`, `<`, `<=`, `>` and `>=`, BETWEEN, IN, AND, OR, NOT,
//! parentheses, `attribute_exists` and `attribute_not_exists`, and more than
//! one condition on one attribute. The checks run in this order, and the
//! first failure is the answer:
//!
//! 1. the expression is not empty and is at most 4096 bytes;
//! 2. the syntax, then redundant parentheses;
//! 3. the checks of the condition grammar, post-order (see [`walk`]);
//! 4. at most 300 operators, then the combined size;
//! 5. the search rules, in written order (see [`rules`]): attribute operands,
//!    nested paths, unsupported functions, and the condition limit;
//! 6. every `ExpressionAttributeValues` key is used.
//!
//! Every failure in 1 to 5 carries the prefix
//! `Invalid SearchConditionExpression: `; the failure in 6 does not. The schema
//! check ([`SearchFilter::validate_against_schema`]) runs once the index is
//! known, and its messages carry no prefix either.
//!
//! Evaluation uses the condition evaluator that FilterExpression uses, so a
//! missing attribute and a type mismatch behave as they do in a Query or Scan
//! filter. Numbers compare as the nearest binary64 value of each side, as the
//! service compares inline filters: `99999999999999999999999999999999999999 >
//! 99999999999999999999999999999999999998` is false.
//!
//! Pure synchronous Rust: no async, no I/O.

mod gate;
mod grammar;
mod lexer;
mod rules;
mod schema;
mod walk;

use std::collections::{BTreeSet, HashMap};

pub use gate::EnabledAccounts;
pub use rules::{Condition, Operator};

use crate::error::DynamoDbError;
use crate::expression::search_params::check_unused_keys;
use crate::expression::{Expr, ExpressionMaps};
use crate::types::{AttributeDefinition, AttributeValue, Item, SearchSchemaElement};

/// The condition limit the service applies by default.
pub const DEFAULT_MAX_CONDITIONS: usize = 50;

/// The most bytes an expression takes, counted before substitution.
const MAX_EXPRESSION_BYTES: usize = 4096;

const PREFIX: &str = "Invalid SearchConditionExpression: ";

fn invalid(body: &str) -> DynamoDbError {
    DynamoDbError::ValidationException(format!("{PREFIX}{body}"))
}

/// A search condition that passed every check that needs no index metadata.
#[derive(Debug, Clone)]
pub struct SearchFilter {
    expr: Expr,
    maps: ExpressionMaps,
    conditions: Vec<Condition>,
}

/// Parse and check a search condition with the extended grammar.
///
/// # Errors
///
/// Returns `ValidationException` with the message of the first failed check,
/// in the order the module documentation gives.
pub fn parse_search_filter(
    expr: &str,
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
    max_conditions: usize,
) -> Result<SearchFilter, DynamoDbError> {
    if expr.is_empty() {
        return Err(invalid("The expression can not be empty;"));
    }
    if expr.len() > MAX_EXPRESSION_BYTES {
        return Err(invalid(
            "Expression size has exceeded the maximum allowed size;",
        ));
    }
    let toks = lexer::lex(expr);
    let tree = grammar::parse(expr, &toks).map_err(|body| invalid(&body))?;
    let maps = walk::Maps { names, values };
    walk::check(&tree, &maps).map_err(|body| invalid(&body))?;
    let operators = walk::operators(&tree);
    if operators > walk::MAX_OPERATORS {
        return Err(invalid(&format!(
            "The expression contains too many operators; operator count: {operators}"
        )));
    }
    if walk::combined_bytes(expr, &tree, &maps) > walk::MAX_COMBINED_BYTES {
        return Err(invalid(
            "Expression size has exceeded the maximum allowed size;",
        ));
    }
    let conditions = rules::collect(&tree, &maps, max_conditions).map_err(|body| invalid(&body))?;
    unused_values(&toks, expr, values)?;

    Ok(SearchFilter {
        expr: rules::lower(&tree),
        maps: evaluation_maps(names, values),
        conditions,
    })
}

/// Every supplied value must be used. The keys are listed in sorted order.
fn unused_values(
    toks: &[lexer::Tok],
    src: &str,
    values: Option<&HashMap<String, AttributeValue>>,
) -> Result<(), DynamoDbError> {
    let used = refs(toks, src, lexer::Kind::ValueRef);
    check_unused_keys("ExpressionAttributeValues", values, &used)
}

/// The text of every token of `kind`.
fn refs<'a>(toks: &[lexer::Tok], src: &'a str, kind: lexer::Kind) -> BTreeSet<&'a str> {
    toks.iter()
        .filter(|t| t.kind == kind)
        .map(|t| &src[t.start..t.end])
        .collect()
}

/// The `#name` tokens of `expr`, for the unused-names check.
#[must_use]
pub fn name_refs(expr: &str) -> BTreeSet<&str> {
    refs(&lexer::lex(expr), expr, lexer::Kind::NameRef)
}

/// The maps the evaluator reads: names keyed without `#`, values without `:`.
fn evaluation_maps(
    names: Option<&HashMap<String, String>>,
    values: Option<&HashMap<String, AttributeValue>>,
) -> ExpressionMaps {
    let names = names
        .into_iter()
        .flatten()
        .filter_map(|(k, v)| Some((k.strip_prefix('#')?.to_owned(), v.clone())))
        .collect();
    let values = values
        .into_iter()
        .flatten()
        .filter_map(|(k, v)| Some((k.strip_prefix(':')?.to_owned(), v.clone())))
        .collect();
    ExpressionMaps::new(names, values)
}

impl SearchFilter {
    /// The conditions in written order.
    #[must_use]
    pub fn conditions(&self) -> &[Condition] {
        &self.conditions
    }

    /// Check the conditions against the index search schema, and return the
    /// HASH attribute and the value that selects its partition when the
    /// schema declares a HASH element.
    ///
    /// # Errors
    ///
    /// Returns `ValidationException` with the message of the first failed
    /// check, without the expression prefix.
    pub fn validate_against_schema(
        &self,
        schema: Option<&[SearchSchemaElement]>,
        definitions: &[AttributeDefinition],
    ) -> Result<Option<(&str, &AttributeValue)>, DynamoDbError> {
        schema::validate(&self.conditions, schema.unwrap_or_default(), definitions)
            .map_err(DynamoDbError::ValidationException)
    }

    /// Whether `item` satisfies the condition.
    ///
    /// # Errors
    ///
    /// Returns the evaluator's error. The checks above refuse every
    /// condition the evaluator would refuse, so this does not happen for a
    /// parsed filter.
    pub fn matches(&self, item: &Item) -> Result<bool, DynamoDbError> {
        crate::expression::evaluate_condition_at(
            &self.expr,
            item,
            &self.maps,
            crate::expression::NumberPrecision::Binary64,
        )
    }
}

#[cfg(test)]
mod tests_support;

#[cfg(test)]
mod tests_grammar;

#[cfg(test)]
mod tests_rules;

#[cfg(test)]
mod tests_schema;

#[cfg(test)]
mod tests_eval;

#[cfg(test)]
mod tests_depth;

#[cfg(test)]
mod tests_operands;

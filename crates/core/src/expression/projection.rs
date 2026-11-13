// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `ProjectionExpression` parser and evaluator.
//!
//! Parses a comma-separated list of document paths and applies them to an item,
//! returning only the requested attributes.

use std::collections::BTreeMap;

use super::ast::PathElement;
use super::kind::ExpressionKind;
use super::resolver::{ExpressionMaps, resolve_name_ref};
use super::syntax_error::{is_operator_or_keyword, syntax_error};
use super::tokenizer::Token;
use crate::error::DynamoDbError;
use crate::types::{AttributeValue, Item};

/// Parse a `ProjectionExpression` token stream into a list of document paths.
///
/// Grammar: `path ( ',' path )*`
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors.
pub fn parse_projection(tokens: &[Token]) -> Result<Vec<Vec<PathElement>>, DynamoDbError> {
    parse_projection_from(tokens, "")
}

/// Parse a `ProjectionExpression` whose tokens came from `source`, so a syntax
/// error can quote the source window around the offending token as Amazon
/// `DynamoDB` does. An empty `source` joins the token texts instead.
///
/// # Errors
///
/// Returns `ValidationException` for syntax errors.
pub fn parse_projection_from(
    tokens: &[Token],
    source: &str,
) -> Result<Vec<Vec<PathElement>>, DynamoDbError> {
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    let mut pos = 0;
    let mut paths = vec![parse_projection_path(tokens, source, &mut pos)?];
    while pos < tokens.len() {
        if tokens[pos] != Token::Comma {
            return Err(DynamoDbError::ValidationException(format!(
                "Invalid ProjectionExpression: unexpected token at position {pos}"
            )));
        }
        pos += 1;
        paths.push(parse_projection_path(tokens, source, &mut pos)?);
    }
    Ok(paths)
}

/// Parse one projected path. Input that ends where a path is required (`#v,`)
/// and a comma, operator or keyword there (`#v, , #v`) answer the service's
/// syntax error (measured 2026-10-25: token ",", near ", , #v"; token "<EOF>",
/// near ","). Any other token keeps the previous text.
fn parse_projection_path(
    tokens: &[Token],
    source: &str,
    pos: &mut usize,
) -> Result<Vec<PathElement>, DynamoDbError> {
    match tokens.get(*pos) {
        None | Some(Token::Comma) => Err(syntax_error(
            ExpressionKind::Projection,
            source,
            tokens,
            *pos,
        )),
        Some(token) if is_operator_or_keyword(token) => Err(syntax_error(
            ExpressionKind::Projection,
            source,
            tokens,
            *pos,
        )),
        Some(_) => super::parser_common::parse_path(tokens, pos),
    }
}

/// A node in the projection trie.
///
/// Each node selects part of the item. `terminal` means the whole value at
/// this position is projected. Otherwise the node descends via `attrs` (for a
/// map) or `indices` (for a list). A value is either a map or a list, so only
/// one of the two child maps is populated on a given node in practice.
///
/// `indices` is a `BTreeMap` so selected list elements come out in ascending
/// original-index order, which is how DynamoDB compacts list projections.
#[derive(Default)]
struct ProjNode {
    terminal: bool,
    attrs: BTreeMap<String, ProjNode>,
    indices: BTreeMap<usize, ProjNode>,
}

/// A compiled projection: `#name` references resolved and paths merged into a
/// trie. Compile once per request, then apply to any number of items.
pub struct Projection {
    root: ProjNode,
}

impl Projection {
    /// Compile parsed paths into a trie, resolving `#name` references once.
    /// With `reject_overlap`, rejects paths where one is a prefix of another
    /// (DynamoDB's rule); pass `false` for desugared `AttributesToGet`.
    pub fn compile(
        paths: &[Vec<PathElement>],
        maps: &ExpressionMaps,
        reject_overlap: bool,
    ) -> Result<Self, DynamoDbError> {
        let resolved = resolve_paths(paths, maps).map_err(prefix_projection_error)?;
        if reject_overlap {
            check_overlap(&resolved)?;
        }
        Ok(Self {
            root: build_trie(resolved),
        })
    }

    /// Apply the projection to an item, returning only the requested attributes.
    ///
    /// List elements selected by index are returned in a new list compacted in
    /// ascending original-index order (matching Amazon DynamoDB), not in the
    /// order the indices appear in the expression. Map keys not on a projected
    /// path are dropped. The structure of the original item is otherwise
    /// preserved.
    #[must_use]
    pub fn apply(&self, item: &Item) -> Item {
        let mut result = BTreeMap::new();
        for (name, child) in &self.root.attrs {
            if let Some(val) = item.get(name)
                && let Some(projected) = project_value(val, child)
            {
                result.insert(name.clone(), projected);
            }
        }
        result
    }
}

/// Compile (no overlap rejection) and apply to one item. Test-only helper;
/// production callers compile once via `Projection::compile` and apply per item.
#[cfg(test)]
fn apply_projection(
    item: &Item,
    paths: &[Vec<PathElement>],
    maps: &ExpressionMaps,
) -> Result<Item, DynamoDbError> {
    Ok(Projection::compile(paths, maps, false)?.apply(item))
}

/// Prefix a projection resolve error with `Invalid ProjectionExpression:`,
/// matching Amazon DynamoDB. Messages already carrying an `Invalid ` prefix
/// (for example a path that starts with an index) are left unchanged.
fn prefix_projection_error(err: DynamoDbError) -> DynamoDbError {
    match err {
        DynamoDbError::ValidationException(msg) if !msg.starts_with("Invalid ") => {
            DynamoDbError::ValidationException(format!("Invalid ProjectionExpression: {msg}"))
        }
        other => other,
    }
}

/// A path element after `#name` resolution.
#[derive(PartialEq, Eq)]
enum ResolvedElement {
    Attr(String),
    Index(usize),
}

/// Resolve `#name` references in parsed paths, rejecting index-start paths.
fn resolve_paths(
    paths: &[Vec<PathElement>],
    maps: &ExpressionMaps,
) -> Result<Vec<Vec<ResolvedElement>>, DynamoDbError> {
    let mut resolved = Vec::with_capacity(paths.len());
    for path in paths {
        if path.is_empty() {
            continue;
        }
        let mut elems = Vec::with_capacity(path.len());
        for (i, element) in path.iter().enumerate() {
            match element {
                PathElement::Attribute(raw) => {
                    elems.push(ResolvedElement::Attr(
                        resolve_name_ref(raw, maps)?.into_owned(),
                    ));
                }
                PathElement::Index(idx) => {
                    if i == 0 {
                        return Err(DynamoDbError::ValidationException(
                            "Invalid expression: path cannot start with an index".to_owned(),
                        ));
                    }
                    elems.push(ResolvedElement::Index(*idx));
                }
            }
        }
        resolved.push(elems);
    }
    Ok(resolved)
}

/// Reject paths where one is a prefix of another (DynamoDB's overlap rule),
/// reporting the first offending pair in expression order. Siblings are fine.
fn check_overlap(resolved: &[Vec<ResolvedElement>]) -> Result<(), DynamoDbError> {
    for i in 0..resolved.len() {
        for j in (i + 1)..resolved.len() {
            if is_prefix(&resolved[i], &resolved[j]) || is_prefix(&resolved[j], &resolved[i]) {
                return Err(DynamoDbError::ValidationException(format!(
                    "Invalid ProjectionExpression: Two document paths overlap with each other; \
                     must remove or rewrite one of these paths; path one: {}, path two: {}",
                    render_path(&resolved[i]),
                    render_path(&resolved[j]),
                )));
            }
        }
    }
    Ok(())
}

/// Return true when `a` is a prefix of `b` (equal paths included).
fn is_prefix(a: &[ResolvedElement], b: &[ResolvedElement]) -> bool {
    a.len() <= b.len() && a == &b[..a.len()]
}

/// Render a resolved path as DynamoDB does: `[a, [0], b]`.
fn render_path(path: &[ResolvedElement]) -> String {
    let parts: Vec<String> = path
        .iter()
        .map(|el| match el {
            ResolvedElement::Attr(name) => name.clone(),
            ResolvedElement::Index(idx) => format!("[{idx}]"),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}

/// Build the projection trie from resolved paths.
fn build_trie(resolved: Vec<Vec<ResolvedElement>>) -> ProjNode {
    let mut root = ProjNode::default();
    for path in resolved {
        let mut node = &mut root;
        for element in path {
            node = match element {
                ResolvedElement::Attr(name) => node.attrs.entry(name).or_default(),
                ResolvedElement::Index(idx) => node.indices.entry(idx).or_default(),
            };
        }
        node.terminal = true;
    }
    root
}

/// Project a single value against a trie node.
///
/// Returns `None` when nothing is selected (missing key, out-of-bounds index,
/// or a path that does not match the value's type), so the caller omits the
/// attribute entirely.
fn project_value(value: &AttributeValue, node: &ProjNode) -> Option<AttributeValue> {
    if node.terminal {
        return Some(value.clone());
    }

    if !node.attrs.is_empty() {
        let AttributeValue::M(map) = value else {
            return None;
        };
        let mut out = BTreeMap::new();
        for (name, child) in &node.attrs {
            if let Some(child_val) = map.get(name)
                && let Some(projected) = project_value(child_val, child)
            {
                out.insert(name.clone(), projected);
            }
        }
        return (!out.is_empty()).then_some(AttributeValue::M(out));
    }

    if !node.indices.is_empty() {
        let AttributeValue::L(list) = value else {
            return None;
        };
        let mut out = Vec::new();
        // BTreeMap iterates indices ascending, compacting the projected list.
        for (idx, child) in &node.indices {
            if let Some(element) = list.get(*idx)
                && let Some(projected) = project_value(element, child)
            {
                out.push(projected);
            }
        }
        return (!out.is_empty()).then_some(AttributeValue::L(out));
    }

    None
}

#[cfg(test)]
#[path = "projection_tests.rs"]
mod tests;

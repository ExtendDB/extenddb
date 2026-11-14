// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What a statement is authorized for: the action, the resource and the
//! condition keys of its [`ItemAccess`].

use extenddb_core::partiql::Statement;
use extenddb_core::partiql::ast::{
    Operand, Projection, Returning, SelectStatement, UpdateStatement, WhereExpr,
};
use extenddb_core::partiql::plan::{KeyEquality, KeyNames, WhereAnalysis, key_equality};
use extenddb_core::types::AttributeValue;

use super::Surface;
use crate::ItemAccess;
use crate::partiql::SingleRequest;
use crate::partiql::common;
use crate::partiql::index_read::{operand_roots, where_roots};

/// What a statement reads or writes, as far as the catalog knows it.
pub(in crate::partiql) struct Target<'k> {
    /// The table's key attribute names, or `None` for a missing table.
    pub(in crate::partiql) table_keys: Option<Vec<&'k str>>,
    /// The key names of what the statement reads or writes: the index's for
    /// a read of an index. `None` when they are not known: a missing table
    /// or index, a vector index, a key of several attributes.
    pub(in crate::partiql) keys: Option<KeyNames<'k>>,
}

/// The value `operand` names as the request wrote it: a literal as the
/// statement writes it, a parameter as the request sends it. A number keeps
/// the text it was written with, which `SingleRequest::parameters` do not.
pub(in crate::partiql) fn written_value(
    operand: &Operand,
    req: &SingleRequest,
) -> Option<AttributeValue> {
    match operand {
        Operand::Literal(value) => Some(value.clone()),
        Operand::Parameter(index) => match req.parameter_numbers.get(*index) {
            Some(Some(text)) => Some(AttributeValue::N(text.clone())),
            _ => req.parameters.get(*index).cloned(),
        },
        Operand::Path(_) | Operand::Arith(..) | Operand::Function(..) => None,
    }
}

/// What `stmt`, a member of a request of `surface`, is authorized for, on
/// `target` and with `analysis`, its WHERE read against `target.keys`. For
/// an `ExecuteStatement` SELECT read by key, each disjunct is authorized
/// with its own partition value in place of the ones here (see
/// `each_disjunct`).
///
/// Measured 2026-11-03:
///
/// * the resource is the table, or for a read of an index the index; an
///   index read of a missing table is authorized against the table;
/// * `dynamodb:LeadingKeys` holds the partition key values the statement
///   reads or writes, as the request writes them: a number keeps its scale
///   (`10.0` is not `10`, `010` is `10` once the server writes it);
/// * `dynamodb:Attributes` holds the table's key attributes and the
///   top-level names the statement uses;
/// * `dynamodb:ReturnValues` is "NONE" for a write without `RETURNING`;
/// * `dynamodb:FullTableScan` is set for an `ExecuteStatement` read only,
///   and absent when the key of what it reads is not known.
pub(in crate::partiql) fn access(
    stmt: &Statement,
    target: &Target<'_>,
    analysis: Option<&WhereAnalysis<'_>>,
    req: &SingleRequest,
    surface: Surface,
) -> ItemAccess {
    let written = |op: &Operand| written_value(op, req);
    let keys = target.keys;
    let table_keys = target.table_keys.as_deref().unwrap_or_default();
    let base = ItemAccess {
        enclosing_operation: surface.enclosing_operation(),
        ..ItemAccess::default()
    };
    match stmt {
        Statement::Select(select) | Statement::Exists(select) => {
            let keyed = analysis.filter(|analysis| analysis.is_keyed());
            ItemAccess {
                action: "PartiQLSelect",
                table: select.from.table.clone(),
                index: target.table_keys.as_ref().and(select.from.index.clone()),
                partition_keys: keyed.and_then(|analysis| {
                    distinct(analysis.reads().iter().map(|read| written(read.partition)))
                }),
                attributes: names(table_keys, select_names(select)),
                select: Some(select_value(select)),
                full_table_scan: match surface {
                    Surface::Statement => keys.map(|_| keyed.is_none()),
                    Surface::Batch | Surface::Transaction => None,
                },
                ..base
            }
        }
        Statement::Insert(insert) => ItemAccess {
            action: "PartiQLInsert",
            table: insert.table.clone(),
            // The value the item stores, the one the write takes its key from.
            partition_keys: keys
                .and_then(|keys| common::insert_values(insert).get(keys.partition).copied())
                .and_then(written)
                .map(|value| vec![value]),
            attributes: names(
                table_keys,
                insert.value.iter().map(|(name, _)| name.as_str()).collect(),
            ),
            return_values: Some(return_values(None)),
            ..base
        },
        Statement::Update(update) => ItemAccess {
            action: "PartiQLUpdate",
            table: update.table.clone(),
            partition_keys: keys.zip(update.where_clause.as_ref()).and_then(
                |(keys, where_clause)| partition_equalities(where_clause, keys, &written),
            ),
            attributes: names(table_keys, update_names(update)),
            return_values: Some(return_values(update.returning)),
            ..base
        },
        Statement::Delete(delete) => ItemAccess {
            action: "PartiQLDelete",
            table: delete.table.clone(),
            partition_keys: keys
                .and_then(|keys| partition_equalities(&delete.where_clause, keys, &written)),
            attributes: names(table_keys, where_names(&delete.where_clause)),
            return_values: Some(return_values(delete.returning)),
            ..base
        },
    }
}

/// The values of `values` once each, in order, or `None` when one of them
/// does not resolve (a parameter the request does not supply), as a key the
/// classic operations cannot read is left absent.
fn distinct(values: impl Iterator<Item = Option<AttributeValue>>) -> Option<Vec<AttributeValue>> {
    let mut out: Vec<AttributeValue> = Vec::new();
    for value in values {
        let value = value?;
        if !out.contains(&value) {
            out.push(value);
        }
    }
    (!out.is_empty()).then_some(out)
}

/// The values a write's WHERE gives the partition key by equality among its
/// top-level `AND` conditions, written either way round, or `None` when it
/// gives none (measured 2026-11-03: UPDATE and DELETE of an item of
/// partition a are allowed under `ForAllValues` `["a"]` and of b refused).
fn partition_equalities(
    where_clause: &WhereExpr,
    keys: KeyNames<'_>,
    written: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> Option<Vec<AttributeValue>> {
    let mut terms = Vec::new();
    common::flatten_and_lossy(where_clause, &mut terms);
    distinct(
        terms
            .into_iter()
            .filter_map(|term| match key_equality(term, keys) {
                Some(KeyEquality::Partition(value)) => Some(written(value)),
                Some(KeyEquality::Sort(_)) | None => None,
            }),
    )
}

/// `dynamodb:Select` for a SELECT: `SPECIFIC_ATTRIBUTES` for a projection
/// list, `ALL_ATTRIBUTES` for `*` on a table and `ALL_PROJECTED_ATTRIBUTES`
/// for `*` on an index, of any projection, in all three operations
/// (measured 2026-11-03).
fn select_value(select: &SelectStatement) -> &'static str {
    match (&select.projection, &select.from.index) {
        (Projection::Paths(_), _) => "SPECIFIC_ATTRIBUTES",
        (Projection::All, None) => "ALL_ATTRIBUTES",
        (Projection::All, Some(_)) => "ALL_PROJECTED_ATTRIBUTES",
    }
}

/// `dynamodb:ReturnValues` for a write's `RETURNING` clause: "NONE" without
/// one, in all three operations, and the classic name of what each mode
/// returns (measured 2026-11-03).
fn return_values(returning: Option<Returning>) -> &'static str {
    match returning {
        None => "NONE",
        Some(Returning::AllOld) => "ALL_OLD",
        Some(Returning::ModifiedOld) => "UPDATED_OLD",
        Some(Returning::AllNew) => "ALL_NEW",
        Some(Returning::ModifiedNew) => "UPDATED_NEW",
    }
}

/// `dynamodb:Attributes`: the table's key attributes `table_keys`, then the
/// names the statement uses, each once, in the order first named, or absent
/// when there are none. For a read of an index the index's key attributes
/// are in the set only when the statement names them (not measured).
fn names(table_keys: &[&str], used: Vec<&str>) -> Option<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for name in table_keys.iter().copied().chain(used) {
        if !out.iter().any(|seen| seen == name) {
            out.push(name.to_owned());
        }
    }
    (!out.is_empty()).then_some(out)
}

/// The top-level names a SELECT names: its projection list (`*` names none),
/// its WHERE and its ORDER BY (measured 2026-11-03: a name in the WHERE
/// counts as one in the projection does).
fn select_names(select: &SelectStatement) -> Vec<&str> {
    let mut out = Vec::new();
    if let Projection::Paths(paths) = &select.projection {
        out.extend(paths.iter().map(|path| path.root.as_str()));
    }
    if let Some(where_clause) = &select.where_clause {
        where_roots(where_clause, &mut out);
    }
    if let Some(order_by) = &select.order_by {
        out.extend(order_by.terms.iter().map(|term| term.path.root.as_str()));
    }
    out
}

/// The top-level names an UPDATE names: what it sets, the values it sets
/// them to, what it removes, and its WHERE (measured 2026-11-03: `SET g` is
/// refused under `ForAllValues` `[pk, sk, a, q]`, and so are `SET q` with `g`
/// in the WHERE, `SET q = g` and `REMOVE q` without q in the set).
fn update_names(update: &UpdateStatement) -> Vec<&str> {
    let mut out = Vec::new();
    for action in &update.sets {
        out.push(action.path.root.as_str());
        operand_roots(&action.value, &mut out);
    }
    for action in &update.set_members {
        out.push(action.path.root.as_str());
        operand_roots(&action.members, &mut out);
    }
    out.extend(update.removes.iter().map(|path| path.root.as_str()));
    if let Some(where_clause) = &update.where_clause {
        where_roots(where_clause, &mut out);
    }
    out
}

/// The top-level names a WHERE names.
fn where_names(where_clause: &WhereExpr) -> Vec<&str> {
    let mut out = Vec::new();
    where_roots(where_clause, &mut out);
    out
}

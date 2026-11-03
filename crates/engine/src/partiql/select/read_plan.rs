// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The WHERE checks a SELECT gets before anything is read, and the read plan they lead to.
//!
//! The disjuncts held back for a refusal made when they are read, the
//! caller's authorization refusal among them, share one refusal (see
//! [`check_reads`]): the last refusal of a check in read order, else an
//! authorization refusal. A request whose page visits only held-back
//! disjuncts gets it (measured 2026-11-03 when no disjunct is read, and
//! when every held-back disjunct is refused for authorization). A page that
//! visits only a disjunct the caller may not read, while another held-back
//! disjunct fails a check, gets that check's refusal; that is not measured.

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::partiql::ast::{CompareOp, Operand, OrderBy, WhereExpr};
use extenddb_core::partiql::plan::{
    Direction, KeyNames, KeyRead, SortCondition, WhereAnalysis, filter_key_refusal, read_order,
    read_order_in, scalar_key_refusal,
};
use extenddb_core::types::{AttributeValue, Item, KeySchemaElement, KeyType, TableKeyInfo};
use extenddb_core::validation::validate_key_size_limits;

use super::bound_value;
use super::order::{check_order_by_reads, check_order_by_terms};
use crate::partiql::common;

/// How a `SELECT` reaches its rows.
pub(super) enum ReadPlan<'a, 'b> {
    /// One disjunct read by key: a Query with its key condition and filter.
    Key(&'b KeyRead<'a>),
    /// Disjuncts read one after another in key order (see `read_steps`).
    Steps(Vec<Step<'a, 'b>>),
    /// Everything else: a Scan with the WHERE as its filter.
    Scan,
}

impl<'a, 'b> ReadPlan<'a, 'b> {
    /// The read of a checked WHERE on a target with key names `keys`, a
    /// table when `table` is set.
    ///
    /// An index is read by one Query for each disjunct, also when several
    /// name one partition of it (measured 2026-11-03: on a GSI keyed on g
    /// and gs, `g = 'a' AND gs IN ['w000', 'w001', 'w002']` reports 1.5
    /// capacity units, 0.5 for each Query, and `g = 'a' AND gs IN [...]`
    /// with 11 values gives 10 items and then 1). A disjunct of an index
    /// never names one item, since an index key does not.
    pub(super) fn of(
        checked: Option<&Checked<'a, 'b>>,
        keys: KeyNames<'_>,
        table: bool,
        direction: Direction,
        value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
    ) -> Self {
        let Some(checked) = checked.filter(|c| c.keyed) else {
            return Self::Scan;
        };
        let points = table.then_some(keys);
        match checked.reads.as_slice() {
            [read]
                if checked.refused.is_empty()
                    && points.is_none_or(|keys| Step::wire_key(read, keys, value_of).is_none()) =>
            {
                Self::Key(read)
            }
            reads => Self::Steps(Step::all(
                reads,
                &checked.refused,
                points,
                direction,
                value_of,
            )),
        }
    }
}

/// One disjunct of a read of several, in the order it is read.
pub(super) struct Step<'a, 'b> {
    pub(super) read: &'b KeyRead<'a>,
    /// The item's key in wire form when the disjunct names one item of the
    /// table (an equality on every key attribute).
    pub(super) point: Option<Value>,
    /// Whether the disjunct is held back for the refusal it gets when it is
    /// read: it takes its place in a page and reads nothing (see
    /// `read_steps`).
    pub(super) held_back: bool,
}

impl<'a, 'b> Step<'a, 'b> {
    /// The disjuncts `reads`, and those `held_back` for a refusal, in the
    /// order they are read: key order (see [`read_order`]), its partitions
    /// and the ranges of a partition each in the direction an ORDER BY gives
    /// (measured 2026-11-02: `pk IN ['c', 'a', 'b']` reads a, b then c, and
    /// `pk = 'a' AND sk IN ['x', 'y'] ORDER BY sk DESC` reads a/y first;
    /// measured 2026-11-03: `pk IN ['a', 'b'] ORDER BY pk DESC, sk` reads
    /// b/x, a/x then a/y). A disjunct held back takes its place among the
    /// others (measured 2026-11-03: the first page of 10 disjuncts of `pk
    /// IN [v000..v010, <2049 bytes that start with v005>]` holds v000 to
    /// v008).
    ///
    /// `points` are the key names of a table, whose disjuncts may name one
    /// item each; a read of an index has none.
    fn all(
        reads: &[&'b KeyRead<'a>],
        held_back: &[&'b KeyRead<'a>],
        points: Option<KeyNames<'_>>,
        direction: Direction,
        value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
    ) -> Vec<Self> {
        let mut steps: Vec<Self> = reads
            .iter()
            .map(|&read| Self {
                read,
                point: points.and_then(|keys| Self::wire_key(read, keys, value_of)),
                held_back: false,
            })
            .chain(held_back.iter().map(|&read| Self {
                read,
                point: None,
                held_back: true,
            }))
            .collect();
        steps.sort_by(|a, b| read_order_in(a.read, b.read, direction, value_of));
        steps
    }

    /// The key of the one item a disjunct names, in wire form, or `None`
    /// when it does not name one.
    fn wire_key(
        read: &KeyRead<'_>,
        keys: KeyNames<'_>,
        value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
    ) -> Option<Value> {
        let mut wire = Map::new();
        let partition = value_of(read.partition)?;
        wire.insert(
            keys.partition.to_owned(),
            serde_json::to_value(&partition).ok()?,
        );
        match (keys.sort, &read.sort) {
            (None, _) => {}
            (Some(name), Some(SortCondition::Compare(CompareOp::Eq, value))) => {
                wire.insert(
                    name.to_owned(),
                    serde_json::to_value(value_of(value)?).ok()?,
                );
            }
            (Some(_), _) => return None,
        }
        Some(Value::Object(wire))
    }
}

/// A WHERE as the service checks it before anything is read.
pub(super) struct Checked<'a, 'b> {
    /// Whether every disjunct reads by key, so the statement is not a scan.
    pub(super) keyed: bool,
    /// The disjuncts read by key that are read, in statement order: every
    /// one, unless some fail a check made when they are read.
    pub(super) reads: Vec<&'b KeyRead<'a>>,
    /// The conditions every disjunct shares, when the statement reads by key.
    pub(super) shared_filter: Option<Vec<&'a WhereExpr>>,
    /// The refusal of the disjuncts that fail a check made when they are
    /// read, which a request whose page visits only them gets (see
    /// `read_steps`).
    pub(super) deferred: Option<DynamoDbError>,
    /// The disjuncts that fail a check made when they are read, in statement
    /// order.
    pub(super) refused: Vec<&'b KeyRead<'a>>,
}

/// The most disjuncts a SELECT read by key may split into (measured
/// 2026-11-03: `pk IN [...]` with 50 values runs and with 51 values is
/// refused, on a table and on an index). It is also the most partition
/// values the authorization of a read by key asks about
/// (`authorize::each_disjunct`), which is safe because a statement with
/// more disjuncts is refused before it is authorized.
pub(in crate::partiql) const MAX_DECOMPOSED_READS: usize = 50;

/// Check the keys of a WHERE and the ORDER BY of a SELECT the way the
/// service does, in its order, before anything is read (measured 2026-11-03
/// for a permitted caller):
///
/// 1. a path into a key attribute ("Key attributes must be scalars"): on a
///    table anywhere in the WHERE, on an index in the filter of a disjunct
///    read by key;
/// 2. an ORDER BY term that is not a key attribute (see
///    `order::check_order_by_terms`);
/// 3. the key values of each disjunct read by key, in statement order;
/// 4. the WHERE an ORDER BY reads (see `order::check_order_by_reads`),
///    with the refusal of more than one partition key value without the
///    partition key in the list;
/// 5. overlapping disjuncts;
/// 6. more than [`MAX_DECOMPOSED_READS`] disjuncts (see
///    [`check_decomposed`]).
///
/// Measured: with `pk.x = 1` the scalar refusal whatever the ORDER BY;
/// `pk = 1`, `pk IN ['a', 1]` and `pk = 'a' AND sk = 1` with `ORDER BY n`
/// the term refusal, with `ORDER BY sk` the key type refusal; `(pk = 'a' OR
/// pk = 'a')` with `ORDER BY n` the term refusal, with `ORDER BY sk` the
/// overlap refusal; `(pk = 'a' OR pk = 'b' OR pk = 'a') ORDER BY sk` the
/// refusal of more than one partition key value; `pk IN [51 values]` the
/// ORDER BY refusals. Whether a WHERE that does not read by key is refused
/// ahead of a key value of the wrong type is not measured.
///
/// A path into a key attribute is refused at once, naming the first such
/// disjunct in statement order, ahead of the other refusals and when a later
/// disjunct makes the statement a scan (measured 2026-11-02: `(pk = 'a' AND
/// sk.x = 1) OR pk = 'b'` gets the scalar refusal on the first request, as
/// do `(pk = 'a' AND sk.x = 1) OR (pk = 'a' AND sk = 'y')`, `pk = 1 OR (pk =
/// 'a' AND sk.x = 1)` and `(pk = 'a' AND sk.x = 1) OR x = 1`). The
/// statement is authorized after these checks: 1, 5 and 6 are made before
/// authorization and 2, 3 and 4 after it (see `authorize::gate`).
pub(super) fn check_keys(
    analysis: Option<&WhereAnalysis<'_>>,
    order: Option<&OrderBy>,
    keys: KeyNames<'_>,
    key_info: &TableKeyInfo,
    table: bool,
    params: &[AttributeValue],
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> Result<(), DynamoDbError> {
    let path_into_key = analysis.and_then(|analysis| {
        if table {
            analysis.path_into_key_refusal(keys)
        } else {
            analysis
                .reads()
                .iter()
                .find_map(|read| scalar_key_refusal(&read.filter, keys))
        }
    });
    if let Some(message) = path_into_key {
        return Err(DynamoDbError::ValidationException(message));
    }
    check_order_by_terms(order, keys)?;
    for read in analysis.map(WhereAnalysis::reads).unwrap_or_default() {
        check_key_read(read, keys, key_info, params)?;
    }
    check_order_by_reads(order, analysis, keys, table, value_of)?;
    let Some(analysis) = analysis else {
        return Ok(());
    };
    analysis
        .check_overlap(value_of)
        .map_err(|m| DynamoDbError::ValidationException(m.to_owned()))?;
    check_decomposed(analysis)
}

/// Refuse a statement read by key whose WHERE splits into more than
/// [`MAX_DECOMPOSED_READS`] disjuncts (see
/// `WhereAnalysis::disjunct_count`). Measured 2026-11-03, on tables and on
/// indexes: the service counts each value of an IN on a key attribute, each
/// side of an OR of key conditions, the product of ANDed lists (`pk IN [25
/// values] AND sk IN [2 values]` runs, `pk IN [17 values] AND sk IN [3
/// values]` is refused), parameters, and the disjuncts held back for a
/// refusal made when they are read; it does not count the conditions of a
/// filter (`pk IN [50 values]` beside 99 comparisons runs), and a statement
/// with a disjunct that is a scan runs as a scan (`pk IN [51 values] OR x =
/// 1`). The refusal comes last of those of [`check_keys`], and before the
/// checks a disjunct gets when it is read (see [`check_reads`]): `pk IN [50
/// values, <2049 bytes>]` and `pk IN [51 values] AND sk = a` get it.
fn check_decomposed(analysis: &WhereAnalysis<'_>) -> Result<(), DynamoDbError> {
    if analysis.is_keyed() && analysis.disjunct_count() > MAX_DECOMPOSED_READS as u64 {
        return Err(DynamoDbError::ValidationException(
            TOO_MANY_DECOMPOSED_READS.to_owned(),
        ));
    }
    Ok(())
}

/// The refusal of a statement read by key with more than
/// [`MAX_DECOMPOSED_READS`] disjuncts, which comes before the statement is
/// authorized (measured 2026-11-03).
pub(in crate::partiql) const TOO_MANY_DECOMPOSED_READS: &str =
    "Too many decomposed read operations for a given query.";

/// The checks each disjunct of a statement read by key gets when it is read
/// (see [`read_refusal`]), made once the WHERE has passed [`check_keys`] and
/// [`check_decomposed`].
///
/// A disjunct that fails them does not fail the statement while another
/// disjunct passes: it is held back, and the request whose page visits only
/// disjuncts held back gets the refusal (see `read_steps`; measured
/// 2026-11-02: `(pk = 'a' AND sk = a) OR pk = 'b'` returns partition 'b'
/// with a NextToken, and the request with that token gets the filter
/// refusal; `pk IN ['a', <2049 bytes>]` the same with the key size
/// refusal). When every disjunct fails, the statement is refused at once.
/// Of several failing disjuncts, the one read last in key order is reported
/// (measured 2026-11-02: `pk = <2049 bytes of z> OR (pk = 'a' AND sk = a)
/// OR pk = 'b'` gets the key size refusal and `(pk = 'b' AND sk = a) OR (pk
/// = 'a' AND pk = b) OR pk = 'c'` the filter refusal naming sk).
///
/// `denied` gives the authorization refusal of a disjunct the caller may not
/// read (see `authorize`), which is held back the same way, its refusal
/// taking the place of the checks here (measured 2026-11-03: a disjunct the
/// policy refuses that is past the key size limit, or has a filter on a key
/// attribute, gets AccessDenied). Across disjuncts it ranks below the
/// refusals of the checks.
#[allow(clippy::too_many_arguments)]
pub(super) fn check_reads<'a, 'b>(
    analysis: &'b WhereAnalysis<'a>,
    keys: KeyNames<'_>,
    target_schema: &[KeySchemaElement],
    limits: &LimitsConfig,
    params: &[AttributeValue],
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
    denied: &dyn Fn(&KeyRead<'a>) -> Option<DynamoDbError>,
) -> Result<Checked<'a, 'b>, DynamoDbError> {
    let checked = Checked {
        keyed: analysis.is_keyed(),
        reads: analysis.reads().iter().collect(),
        shared_filter: analysis.shared_filter(),
        deferred: None,
        refused: Vec::new(),
    };
    if !checked.keyed {
        return Ok(checked);
    }
    // Each failing disjunct, its refusal, and whether the refusal is the
    // authorization's.
    let mut failing: Vec<(&'b KeyRead<'a>, DynamoDbError, bool)> = Vec::new();
    let mut reads = Vec::with_capacity(checked.reads.len());
    for &read in &checked.reads {
        if let Some(error) = denied(read) {
            failing.push((read, error, true));
        } else if let Some(error) = read_refusal(read, keys, target_schema, limits, params) {
            failing.push((read, error, false));
        } else {
            reads.push(read);
        }
    }
    let refused: Vec<&'b KeyRead<'a>> = failing.iter().map(|(read, ..)| *read).collect();
    // An authorization refusal ranks below the others: the one reported is
    // the last in read order of the refusals made by the checks, and an
    // authorization refusal only when every failing disjunct has one
    // (measured 2026-11-03: under `ForAllValues` LeadingKeys ["a"], `(pk =
    // 'a' AND sk = a) OR pk = 'b'`, either way round, is refused at once
    // with the filter refusal of the disjunct the policy allows).
    let by_checks = failing.iter().any(|(_, _, denied)| !denied);
    // `max_by` keeps the last of equal elements, so disjuncts whose order is
    // unknown are taken in statement order.
    let error = failing
        .into_iter()
        .filter(|(_, _, denied)| !by_checks || !denied)
        .max_by(|a, b| read_order(a.0, b.0, value_of))
        .map(|(_, error, _)| error);
    match error {
        None => Ok(checked),
        Some(error) if reads.is_empty() => Err(error),
        Some(error) => Ok(Checked {
            reads,
            deferred: Some(error),
            refused,
            ..checked
        }),
    }
}

/// The refusal a disjunct read by key gets when it is read: a key value
/// longer than the key may be (see [`check_key_sizes`]), else a key
/// attribute compared with another path in its filter.
fn read_refusal(
    read: &KeyRead<'_>,
    keys: KeyNames<'_>,
    schema: &[KeySchemaElement],
    limits: &LimitsConfig,
    params: &[AttributeValue],
) -> Option<DynamoDbError> {
    if let Err(error) = check_key_sizes(read, schema, limits, params) {
        return Some(error);
    }
    filter_key_refusal(&read.filter, keys).map(DynamoDbError::ValidationException)
}

/// Refuse a key read whose values do not fit the key: on the sort key a
/// `begins_with` prefix that is not a string or binary (the operand-type
/// message a filter gets for it) or a value of another type than the key's,
/// then a partition value of another type than the key's. A parameter the
/// request does not supply is left to the parameter count check.
///
/// The sort key is checked first: for `pk = 1 AND sk = 1` the service names
/// `sk`, in either order, and it does so for the names `PK`/`SK`, `id`/`sk`
/// and `id`/`ts` too (measured 2026-11-02). For some other pairs of names it
/// names the partition key; no rule was found.
fn check_key_read(
    read: &KeyRead<'_>,
    keys: KeyNames<'_>,
    key_info: &TableKeyInfo,
    params: &[AttributeValue],
) -> Result<(), DynamoDbError> {
    if let (Some(sort), Some(condition)) = (keys.sort, &read.sort) {
        if let SortCondition::BeginsWith(prefix) = condition
            && let Some(value) = bound_value(prefix, params)
            && !matches!(value, AttributeValue::S(_) | AttributeValue::B(_))
        {
            return Err(DynamoDbError::ValidationException(format!(
                "Incorrect operand type for operator or function; operator or function: \
                 begins_with, operand type: {}",
                crate::partiql::write::av_type_label(value)
            )));
        }
        for operand in condition.values() {
            if let Some(value) = bound_value(operand, params) {
                common::check_key_value_type(key_info, sort, value)?;
            }
        }
    }
    match bound_value(read.partition, params) {
        Some(value) => common::check_key_value_type(key_info, keys.partition, value),
        None => Ok(()),
    }
}

/// Refuse a disjunct whose partition value is longer than a partition key may
/// be, then one whose sort-key condition names a value longer than a sort key
/// may be, with the messages a GetItem of such a key gets (measured
/// 2026-11-02 for an equality, `>`, the upper bound of `BETWEEN` and a
/// `begins_with` prefix, as literals and parameters).
fn check_key_sizes(
    read: &KeyRead<'_>,
    schema: &[KeySchemaElement],
    limits: &LimitsConfig,
    params: &[AttributeValue],
) -> Result<(), DynamoDbError> {
    let check = |key_type: KeyType, operand: &Operand| {
        let (Some(element), Some(value)) = (
            schema.iter().find(|k| k.key_type == key_type),
            bound_value(operand, params),
        ) else {
            return Ok(());
        };
        let key: Item = [(element.attribute_name.clone(), value.clone())]
            .into_iter()
            .collect();
        validate_key_size_limits(&key, std::slice::from_ref(element), limits)
    };
    check(KeyType::Hash, read.partition)?;
    if let Some(condition) = &read.sort {
        for operand in condition.values() {
            check(KeyType::Range, operand)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

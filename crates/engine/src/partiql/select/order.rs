// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The ORDER BY of a SELECT: the refusals the service gives for it, and the
//! order a read takes under it.

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{Operand, OrderBy};
use extenddb_core::partiql::plan::{Direction, KeyNames, KeyRead, WhereAnalysis, same_value};
use extenddb_core::types::AttributeValue;

/// The directions a read takes under `order` (measured 2026-11-03): the
/// partitions ascend unless the term that names the partition key says
/// DESC, and the items of a partition follow the term that names the sort
/// key, descending when no term names it. `ORDER BY pk`, `pk ASC`, `pk
/// DESC` and `pk, pk` read the items of a partition in descending order;
/// `pk, sk`, `sk` and `sk, pk` in ascending order; `sk DESC` and `pk, sk
/// DESC` in descending order; `pk IN ['a', 'b'] ORDER BY pk DESC, sk` reads
/// partition b first. On an index the index's keys take those parts
/// (measured 2026-11-03: `ORDER BY g` reads the index sort key in descending
/// order). Without an ORDER BY a read takes key order.
pub(super) fn direction(order: Option<&OrderBy>, keys: KeyNames<'_>) -> Direction {
    let Some(order) = order else {
        return Direction::default();
    };
    Direction {
        partitions_descending: order
            .term_for(keys.partition)
            .is_some_and(|term| term.descending),
        items_descending: keys
            .sort
            .and_then(|sort| order.term_for(sort))
            .is_none_or(|term| term.descending),
    }
}

/// Refuse an ORDER BY term that is not a key attribute of what is read, the
/// table's key or the index's (measured 2026-11-03: "Attribute n in ORDER BY
/// clause must be part of the primary key", for `n`, for `PK` on a key named
/// `pk`, and for `pk` on an index keyed on other attributes). The service
/// makes this check after the one of a path into a key attribute and ahead
/// of the key values (see `read_plan::check_keys`). A term with a path into
/// an attribute is not measured and is not checked.
pub(super) fn check_order_by_terms(
    order: Option<&OrderBy>,
    keys: KeyNames<'_>,
) -> Result<(), DynamoDbError> {
    let Some(name) = order.and_then(|order| {
        order
            .terms
            .iter()
            .filter_map(|term| term.path.as_simple())
            .find(|name| *name != keys.partition && Some(*name) != keys.sort)
    }) else {
        return Ok(());
    };
    Err(DynamoDbError::ValidationException(format!(
        "Attribute {name} in ORDER BY clause must be part of the primary key"
    )))
}

/// Refuse the WHERE an ORDER BY reads, in the order the service checks
/// (measured 2026-11-03), after the key values and ahead of overlapping
/// disjuncts (see `read_plan::check_keys`):
///
/// * no WHERE;
/// * a WHERE that does not read by key: `n = 1`, `pk > 'a'`, `pk = 'a' OR n
///   = 1`, and a sort key predicate that is not one key condition, such as
///   `sk <> 'x'`, `contains(sk, 'x')` or `sk > 'a' AND sk < 'z'`;
/// * a read of more than one partition key value whose list does not name
///   the partition key (`pk IN ['a', 'b'] ORDER BY sk`; `ORDER BY sk, pk`
///   runs).
///
/// Measured on tables; an index read keeps the term rule of
/// [`check_order_by_terms`] alone.
pub(super) fn check_order_by_reads(
    order: Option<&OrderBy>,
    analysis: Option<&WhereAnalysis<'_>>,
    keys: KeyNames<'_>,
    table: bool,
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> Result<(), DynamoDbError> {
    let Some(order) = order.filter(|_| table) else {
        return Ok(());
    };
    let refuse = |message: &str| Err(DynamoDbError::ValidationException(message.to_owned()));
    let Some(analysis) = analysis else {
        return refuse("Must have WHERE clause in the statement when using ORDER BY clause.");
    };
    if !analysis.is_keyed() {
        return refuse(
            "Must have at least one non-optional hash key condition in WHERE clause when using \
             ORDER BY clause.",
        );
    }
    if order.term_for(keys.partition).is_none() && partition_count(analysis.reads(), value_of) > 1 {
        return refuse(
            "Must have hash key in ORDER BY clause when more than one hash key condition \
             specified in WHERE clause.",
        );
    }
    Ok(())
}

/// How many distinct partition key values the disjuncts `reads` read.
fn partition_count(
    reads: &[KeyRead<'_>],
    value_of: &dyn Fn(&Operand) -> Option<AttributeValue>,
) -> usize {
    let mut seen: Vec<&Operand> = Vec::new();
    for read in reads {
        if !seen
            .iter()
            .any(|value| same_value(value, read.partition, value_of))
        {
            seen.push(read.partition);
        }
    }
    seen.len()
}

#[cfg(test)]
mod tests;

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The classic Query and Scan requests a SELECT is translated into.

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{Operand, SelectStatement, WhereExpr};
use extenddb_core::partiql::plan::{KeyNames, SortCondition};

use crate::partiql::SingleRequest;
use crate::partiql::expr_bridge::{self, ExprBridge};
use crate::{DispatchResult, OperationContext};

/// The `KeyConditionExpression` of a key read: the partition equality, and
/// the sort-key condition when there is one, written with the key on the left.
pub(super) fn key_condition(
    bridge: &mut ExprBridge<'_>,
    keys: KeyNames<'_>,
    partition: &Operand,
    sort: Option<&SortCondition<'_>>,
) -> Result<String, DynamoDbError> {
    let name = bridge.name_ref(keys.partition);
    let value = bridge.operand(partition)?;
    let mut kce = format!("{name} = {value}");
    if let (Some(sort_name), Some(condition)) = (keys.sort, sort) {
        let name = bridge.name_ref(sort_name);
        let condition = match *condition {
            SortCondition::Compare(op, value) => {
                format!(
                    "{name} {} {}",
                    expr_bridge::compare_symbol(op),
                    bridge.operand(value)?
                )
            }
            SortCondition::Between(low, high) => {
                let low = bridge.operand(low)?;
                let high = bridge.operand(high)?;
                format!("{name} BETWEEN {low} AND {high}")
            }
            SortCondition::BeginsWith(prefix) => {
                format!("begins_with({name}, {})", bridge.operand(prefix)?)
            }
        };
        kce.push_str(" AND ");
        kce.push_str(&condition);
    }
    Ok(kce)
}

/// Where a Query or Scan starts, how many items it may read, and whether a
/// Query reads the items of its partition in descending order (see
/// `order::direction`).
#[derive(Clone, Copy, Default)]
pub(super) struct Paging<'v> {
    pub(super) start: Option<&'v Value>,
    pub(super) limit: Option<i64>,
    pub(super) descending: bool,
}

/// Compose and dispatch a `Query` with a rendered key condition and filter.
pub(super) async fn query_request(
    ctx: &OperationContext,
    stmt: &SelectStatement,
    req: &SingleRequest,
    paging: Paging<'_>,
    bridge: &ExprBridge<'_>,
    kce: String,
    filter: Option<String>,
) -> Result<DispatchResult, DynamoDbError> {
    let mut body = Map::new();
    body.insert(
        "TableName".to_owned(),
        Value::String(stmt.from.table.clone()),
    );
    body.insert("KeyConditionExpression".to_owned(), Value::String(kce));
    if let Some(f) = filter {
        body.insert("FilterExpression".to_owned(), Value::String(f));
    }
    if let Some(index) = &stmt.from.index {
        body.insert("IndexName".to_owned(), Value::String(index.clone()));
    }
    body.insert(
        "ScanIndexForward".to_owned(),
        Value::Bool(!paging.descending),
    );
    insert_names_values(&mut body, bridge)?;
    insert_common(&mut body, req, paging);

    crate::handle_query(Value::Object(body), ctx)
        .await
        .map_err(crate::partiql::bare_expression_error)
}

/// Compose and dispatch a `Scan` for a Scan plan, with `filter` as its
/// filter.
pub(super) async fn scan_request(
    ctx: &OperationContext,
    stmt: &SelectStatement,
    req: &SingleRequest,
    paging: Paging<'_>,
    filter: Option<&WhereExpr>,
) -> Result<DispatchResult, DynamoDbError> {
    let mut bridge = ExprBridge::new(&req.parameters);
    let filter = filter.map(|w| bridge.render_condition(w)).transpose()?;

    let mut body = Map::new();
    body.insert(
        "TableName".to_owned(),
        Value::String(stmt.from.table.clone()),
    );
    if let Some(f) = filter {
        body.insert("FilterExpression".to_owned(), Value::String(f));
    }
    if let Some(index) = &stmt.from.index {
        body.insert("IndexName".to_owned(), Value::String(index.clone()));
    }
    insert_names_values(&mut body, &bridge)?;
    insert_common(&mut body, req, paging);

    crate::handle_scan(Value::Object(body), &[], ctx)
        .await
        .map_err(crate::partiql::bare_expression_error)
}

/// Join residual filter conjuncts into one `FilterExpression`, or `None`.
pub(super) fn render_filter(
    bridge: &mut ExprBridge<'_>,
    filter_terms: &[&WhereExpr],
) -> Result<Option<String>, DynamoDbError> {
    if filter_terms.is_empty() {
        return Ok(None);
    }
    let parts: Vec<String> = filter_terms
        .iter()
        .map(|t| bridge.render_condition(t))
        .collect::<Result<_, _>>()?;
    Ok(Some(parts.join(" AND ")))
}

/// Attach the accumulated name/value maps to a composed request body.
fn insert_names_values(
    body: &mut Map<String, Value>,
    bridge: &ExprBridge<'_>,
) -> Result<(), DynamoDbError> {
    if let Some(names) = bridge.names_value() {
        body.insert("ExpressionAttributeNames".to_owned(), names);
    }
    if let Some(values) = bridge.values_value()? {
        body.insert("ExpressionAttributeValues".to_owned(), values);
    }
    Ok(())
}

/// Attach the request parameters common to Query and Scan: Limit, an opaque
/// pagination cursor, ConsistentRead, and ReturnConsumedCapacity.
fn insert_common(body: &mut Map<String, Value>, req: &SingleRequest, paging: Paging<'_>) {
    if let Some(limit) = paging.limit {
        body.insert("Limit".to_owned(), Value::from(limit));
    }
    if let Some(key) = paging.start {
        body.insert("ExclusiveStartKey".to_owned(), key.clone());
    }
    if req.consistent_read {
        body.insert("ConsistentRead".to_owned(), Value::Bool(true));
    }
    if let Some(rcc) = &req.return_consumed_capacity {
        body.insert(
            "ReturnConsumedCapacity".to_owned(),
            Value::String(rcc.clone()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::bound_value;
    use super::super::read_plan::{ReadPlan, check_reads};
    use super::super::tests::{KEYS, select};
    use super::*;
    use crate::partiql::TRANSLATED_EXPRESSION_LIMIT;
    use extenddb_core::limits::LimitsConfig;
    use extenddb_core::partiql::plan::{Direction, WhereAnalysis, keyed_condition};
    use extenddb_core::types::{KeySchemaElement, KeyType};

    #[test]
    fn the_key_condition_is_written_with_the_key_on_the_left() {
        let s = select("SELECT * FROM \"t\" WHERE 'a' = pk AND 1 < sk AND x = 2");
        let value_of = |op: &Operand| bound_value(op, &[]).cloned();
        let analysis = WhereAnalysis::new(s.where_clause.as_ref().unwrap(), KEYS, &value_of);
        let read = analysis.single().unwrap();
        let mut bridge = ExprBridge::new(&[]);
        let kce = key_condition(&mut bridge, KEYS, read.partition, read.sort.as_ref()).unwrap();
        assert_eq!(kce, "#n0 = :v0 AND #n1 > :v1");
        assert_eq!(read.filter.len(), 1);
    }

    #[test]
    fn each_query_of_an_index_read_that_holds_a_disjunct_back_stays_within_the_bound() {
        // The measured shape (2026-11-03): an index keyed on g read by 49
        // values and one past the key size limit, beside 99 comparisons.
        let mut values: Vec<String> = (0..47).map(|i| format!("'a{i:02}'")).collect();
        values.extend(["'a50'".to_owned(), "'a98'".to_owned()]);
        let filter: Vec<String> = (1..=99).map(|i| format!("x{i:02} = 1")).collect();
        let sql = format!(
            "SELECT pk FROM \"t\".\"gix\" WHERE g IN [{}, '{}'] AND {}",
            values.join(", "),
            "z".repeat(2049),
            filter.join(" AND ")
        );
        let s = select(&sql);
        let keys = KeyNames {
            partition: "g",
            sort: None,
        };
        let schema = [KeySchemaElement {
            attribute_name: "g".to_owned(),
            key_type: KeyType::Hash,
        }];
        let value_of = |op: &Operand| bound_value(op, &[]).cloned();
        let where_clause = s.where_clause.as_ref().unwrap();
        let analysis = WhereAnalysis::new(where_clause, keys, &value_of);
        assert_eq!(analysis.disjunct_count(), 50);
        let limits = LimitsConfig::default();
        let checked =
            check_reads(&analysis, keys, &schema, &limits, &[], &value_of, &|_| None).unwrap();
        let ReadPlan::Steps(steps) =
            ReadPlan::of(Some(&checked), keys, false, Direction::default(), &value_of)
        else {
            panic!("an index read of several disjuncts reads them one after another");
        };
        assert_eq!(steps.iter().filter(|step| step.held_back).count(), 1);
        assert_eq!(steps.len(), 50);
        // Each Query carries its own disjunct's filter: the 99 comparisons
        // once.
        for step in steps.iter().filter(|step| !step.held_back) {
            let mut bridge = ExprBridge::new(&[]);
            let kce = key_condition(
                &mut bridge,
                keys,
                step.read.partition,
                step.read.sort.as_ref(),
            )
            .unwrap();
            let text = render_filter(&mut bridge, &step.read.filter)
                .unwrap()
                .unwrap();
            assert_eq!(text.matches(" AND ").count(), 98, "{text}");
            assert!(kce.len() + text.len() < TRANSLATED_EXPRESSION_LIMIT);
        }
        // A read that reaches back filters the base-table items on the WHERE
        // once.
        let joined = ExprBridge::new(&[])
            .render_condition(&keyed_condition(where_clause, keys))
            .unwrap();
        assert!(
            joined.len() < 5 * sql.len(),
            "{} for {}",
            joined.len(),
            sql.len()
        );
    }
}

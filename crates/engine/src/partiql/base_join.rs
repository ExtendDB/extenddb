// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The base-table read under an LSI read whose projection names an attribute
//! the index does not project (the rule is described in `index_read`).
//!
//! The index is read without a filter, so every row it walks comes back. Each
//! row's item is then read from the base table and charged on the table arm,
//! one read unit per 4 KB of item (half that when eventually consistent),
//! before the filter is evaluated against the item (measured 2026-11-02: a
//! filter that keeps no row costs what one that keeps every row costs, and a
//! `Limit` that walks fewer rows costs less).

use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    Expr, ExpressionKind, ExpressionMaps, evaluate_condition, validate_begins_with_operands,
    validate_ordering_operand_types,
};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::partiql::ast::{SelectStatement, WhereExpr};
use extenddb_core::types::{
    AttributeValue, IndexInfo, Item, KeySchemaElement, TableKeyInfo, extract_key, item_size_bytes,
};

use super::SingleRequest;
use super::expr_bridge::ExprBridge;
use super::index_read;
use crate::capacity_helpers::read_capacity_units;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::{
    build_expression_maps, parse_optional_filter, prefix_expression_error,
};
use crate::{DispatchResult, OperationContext};

/// The filter of a read that reaches back, evaluated against each base-table
/// item rather than the index row.
pub(super) struct BaseTableJoin {
    filter: Option<Expr>,
    maps: ExpressionMaps,
}

impl BaseTableJoin {
    /// The join a read of `info` needs, or `None` when the index serves the
    /// read alone. `filter_terms` are a keyed read's filter terms; a scan
    /// (`None`) filters on the whole WHERE. The filter is prepared before the
    /// index is read, so a parameter or operand error is reported ahead of the
    /// read as it is on any other read.
    ///
    /// # Errors
    ///
    /// Returns the parameter count mismatch or the filter validation error the
    /// same filter gets on a read that does not reach back.
    pub(super) fn for_read(
        ctx: &OperationContext,
        req: &SingleRequest,
        stmt: &SelectStatement,
        info: &IndexInfo,
        base_key_schema: &[KeySchemaElement],
        filter_terms: Option<&[&WhereExpr]>,
    ) -> Result<Option<Self>, DynamoDbError> {
        if !index_read::reaches_back(&stmt.projection, info, base_key_schema) {
            return Ok(None);
        }
        let terms: Vec<&WhereExpr> = match filter_terms {
            Some(terms) => terms.to_vec(),
            None => stmt.where_clause.iter().collect(),
        };
        Self::prepare(&ctx.limits, &req.parameters, &terms).map(Some)
    }

    fn prepare(
        limits: &LimitsConfig,
        params: &[AttributeValue],
        terms: &[&WhereExpr],
    ) -> Result<Self, DynamoDbError> {
        if terms.is_empty() {
            return Ok(Self {
                filter: None,
                maps: ExpressionMaps::default(),
            });
        }
        let mut bridge = ExprBridge::new(params);
        let parts: Vec<String> = terms
            .iter()
            .map(|t| bridge.render_condition(t))
            .collect::<Result<_, _>>()?;
        let maps = build_expression_maps(Some(&bridge.names_map()), Some(&bridge.values_map()));
        let filter = parse_optional_filter(Some(&parts.join(" AND ")), limits)?;
        if let Some(f) = &filter {
            validate_begins_with_operands(f, &maps)
                .map_err(|e| prefix_expression_error(e, ExpressionKind::Filter))?;
            validate_ordering_operand_types(f, &maps)
                .map_err(|e| prefix_expression_error(e, ExpressionKind::Filter))?;
        }
        Ok(Self { filter, maps })
    }

    /// Replace each row of an unfiltered index read with its base-table item,
    /// keep the items the filter passes, and add the base-table reads to the
    /// consumed capacity, on the table arm and in the total.
    ///
    /// # Errors
    ///
    /// Returns a storage error from a base-table read, or a filter evaluation
    /// error.
    pub(super) async fn apply(
        &self,
        ctx: &OperationContext,
        key_info: &TableKeyInfo,
        strongly_consistent: bool,
        result: DispatchResult,
    ) -> Result<DispatchResult, DynamoDbError> {
        let DispatchResult { body, mut metrics } = result;
        let mut body = match body {
            Value::Object(m) => m,
            _ => Map::new(),
        };
        let rows = match body.remove("Items") {
            Some(Value::Array(rows)) => rows,
            _ => Vec::new(),
        };
        let mut kept = Vec::new();
        let mut table_units = 0.0;
        for row in rows {
            let index_row: Item = serde_json::from_value(row).map_err(internal)?;
            let key = extract_key(&index_row, &key_info.key_schema);
            let base = ctx
                .storage
                .get_item(key_info, &key)
                .await
                .map_err(storage_err_to_dynamo)?;
            table_units += read_capacity_units(
                base.as_ref().map_or(0, item_size_bytes),
                strongly_consistent,
            );
            // An item deleted since its index row was read is served from the
            // row it left behind.
            let item = base.unwrap_or(index_row);
            if self.passes(&item)? {
                kept.push(serde_json::to_value(&item).map_err(internal)?);
            }
        }
        metrics.returned_item_count = kept.len() as u64;
        metrics.read_capacity_units += table_units;
        body.insert("Items".to_owned(), Value::Array(kept));
        if let Some(Value::Object(cc)) = body.get_mut("ConsumedCapacity") {
            add_units(cc, table_units);
            if let Some(Value::Object(table)) = cc.get_mut("Table") {
                add_units(table, table_units);
            }
        }
        Ok(DispatchResult {
            body: Value::Object(body),
            metrics,
        })
    }

    fn passes(&self, item: &Item) -> Result<bool, DynamoDbError> {
        let Some(filter) = &self.filter else {
            return Ok(true);
        };
        evaluate_condition(filter, item, &self.maps).map_err(|e| {
            let msg = e.to_string();
            if msg.starts_with("Invalid ") {
                DynamoDbError::ValidationException(msg)
            } else {
                DynamoDbError::ValidationException(format!("Invalid FilterExpression: {msg}"))
            }
        })
    }
}

fn add_units(capacity: &mut Map<String, Value>, units: f64) {
    let current = capacity
        .get("CapacityUnits")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    capacity.insert("CapacityUnits".to_owned(), Value::from(current + units));
}

fn internal(e: serde_json::Error) -> DynamoDbError {
    DynamoDbError::InternalServerError(format!("failed to convert an index row: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::partiql::ast::Statement;
    use extenddb_core::partiql::parse;

    fn where_of(sql: &str) -> WhereExpr {
        match parse(sql).unwrap() {
            Statement::Select(s) => s.where_clause.unwrap(),
            _ => panic!("not a select"),
        }
    }

    fn item(pairs: &[(&str, &str)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), AttributeValue::S((*v).to_owned())))
            .collect()
    }

    #[test]
    fn the_filter_is_evaluated_against_the_base_item() {
        let w = where_of("SELECT a FROM \"t\".\"idx\" WHERE hidden = ? AND kept = 'k'");
        let params = [AttributeValue::S("h".to_owned())];
        let join = BaseTableJoin::prepare(&LimitsConfig::default(), &params, &[&w]).unwrap();
        assert!(
            join.passes(&item(&[("hidden", "h"), ("kept", "k")]))
                .unwrap()
        );
        assert!(!join.passes(&item(&[("kept", "k")])).unwrap());
    }

    #[test]
    fn no_terms_keeps_every_item() {
        let join = BaseTableJoin::prepare(&LimitsConfig::default(), &[], &[]).unwrap();
        assert!(join.passes(&Item::new()).unwrap());
    }

    #[test]
    fn a_missing_parameter_is_reported_when_the_join_is_prepared() {
        let w = where_of("SELECT a FROM \"t\".\"idx\" WHERE hidden = ?");
        let err = BaseTableJoin::prepare(&LimitsConfig::default(), &[], &[&w])
            .err()
            .unwrap();
        assert_eq!(
            err.message(),
            "Number of parameters in request and statement don't match."
        );
    }

    #[test]
    fn units_are_added_to_an_existing_arm() {
        let mut arm = Map::new();
        arm.insert("CapacityUnits".to_owned(), Value::from(0.5));
        add_units(&mut arm, 1.5);
        assert_eq!(arm["CapacityUnits"], Value::from(2.0));
    }
}

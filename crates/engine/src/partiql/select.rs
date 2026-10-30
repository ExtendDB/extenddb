// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL `SELECT` executor.
//!
//! Plans the statement (point read / Query / Scan) via
//! [`extenddb_core::partiql::plan::classify`], composes the classic request the
//! matching data-plane handler already understands, drives
//! [`crate::handle_query`] / [`crate::handle_scan`], and shapes the response
//! under `Items` with an opaque, statement-bound `NextToken`. Every behaviour
//! is justified by a measurement against the live service; see the project's
//! PartiQL behavioural spec.
//!
//! A full-primary-key equality is routed through `handle_query` with both key
//! equalities in the `KeyConditionExpression`: that returns at most one item
//! and never a `LastEvaluatedKey`, which is the GetItem behaviour the service
//! shows, so no separate GetItem path is needed.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Map, Value};

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{
    CompareOp, Operand, Path, PathStep, Projection, SelectStatement, WhereExpr,
};
use extenddb_core::partiql::plan::{KeyNames, SelectPlan, classify};
use extenddb_core::types::{IndexType, KeySchemaElement, KeyType};
use extenddb_storage::error::StorageError;

use super::SingleRequest;
use super::common;
use super::expr_bridge::ExprBridge;
use crate::create_table::storage_err_to_dynamo;
use crate::{DispatchResult, OperationContext};

/// Execute a parsed `SELECT`.
///
/// # Errors
///
/// Returns the service's `ValidationException` / `ResourceNotFoundException`
/// for an invalid target, missing table/index, a GSI strong read, a parameter
/// count mismatch, or a `NextToken` that does not match the statement.
pub(super) async fn run(
    ctx: &OperationContext,
    stmt: SelectStatement,
    req: &SingleRequest,
) -> Result<DispatchResult, DynamoDbError> {
    // Resolve the target table's key metadata. A missing table surfaces as the
    // service's ResourceNotFoundException ("Requested resource not found").
    let key_info = ctx
        .table_key_info(&stmt.from.table)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Two projected paths that resolve to the same output identifier are
    // rejected by the service rather than silently collapsed.
    if let Projection::Paths(paths) = &stmt.projection {
        let mut seen = std::collections::HashSet::new();
        for path in paths {
            let key = output_key(path);
            if !seen.insert(key.clone()) {
                return Err(DynamoDbError::ValidationException(format!(
                    "Duplicate identifiers in select clause: {key}"
                )));
            }
        }
    }

    // Resolve the key schema of the target: the index's when one is named,
    // otherwise the base table's. A named index that does not exist is the
    // PartiQL-specific bare message (the classic path appends the index name).
    //
    // Measured order of the index checks on the service: a multi-attribute
    // key wins over everything (including `ConsistentRead`); on a vector index
    // the strong-read refusal wins over the vector refusal.
    let (target_schema, is_gsi) = if let Some(index_name) = &stmt.from.index {
        let is_vector = key_info
            .vector_indexes
            .iter()
            .any(|v| &v.index_name == index_name);
        if is_vector {
            if req.consistent_read {
                return Err(gsi_strong_read_error());
            }
            // A vector index is not reachable by Scan or Query; the service
            // refuses a PartiQL read of one with this exact message.
            return Err(DynamoDbError::ValidationException(
                "Scan operation not supported on this index type".to_owned(),
            ));
        }
        match ctx
            .storage
            .index_info_by_table_id(&key_info.table_id, index_name)
            .await
        {
            Ok(info) => {
                common::ensure_single_attribute_keys(&info.key_schema)?;
                (info.key_schema, info.index_type == IndexType::Gsi)
            }
            Err(StorageError::IndexNotFound(_)) => {
                return Err(DynamoDbError::ValidationException(
                    "The table does not have the specified index".to_owned(),
                ));
            }
            Err(e) => return Err(storage_err_to_dynamo(e)),
        }
    } else {
        common::ensure_single_attribute_keys(&key_info.key_schema)?;
        (key_info.key_schema.clone(), false)
    };

    // A strongly consistent read of a GSI is rejected before the data path.
    // The PartiQL message differs from the classic Query/Scan wording, so it is
    // emitted here rather than left to the underlying handler.
    if req.consistent_read && is_gsi {
        return Err(gsi_strong_read_error());
    }

    let (partition, sort) = key_names(&target_schema);
    let keys = KeyNames {
        partition: &partition,
        sort: sort.as_deref(),
    };

    // A NextToken is opaque and bound to the exact statement that produced it.
    let start_key = match &req.next_token {
        Some(token) => Some(decode_next_token(token, &req.statement)?),
        None => None,
    };

    // Plan with the core classifier, then split the WHERE into a key condition
    // and a residual filter. The split can still fall back to a Scan when a
    // "pk = <identifier>" (double-quoted RHS) is not a real value equality.
    let plan = classify(&stmt, keys);
    let built = if plan == SelectPlan::Scan {
        None
    } else {
        stmt.where_clause
            .as_ref()
            .and_then(|w| split_key_and_filter(w, keys))
    };

    let dispatched = match built {
        Some((key_terms, filter_terms)) => {
            query_request(
                ctx,
                &stmt,
                req,
                start_key.as_ref(),
                &key_terms,
                &filter_terms,
            )
            .await?
        }
        None => scan_request(ctx, &stmt, req, start_key.as_ref()).await?,
    };

    shape_response(dispatched, &req.statement, &stmt.projection)
}

/// Compose and dispatch a `Query` for a point-read or Query plan.
async fn query_request(
    ctx: &OperationContext,
    stmt: &SelectStatement,
    req: &SingleRequest,
    start_key: Option<&Value>,
    key_terms: &[&WhereExpr],
    filter_terms: &[&WhereExpr],
) -> Result<DispatchResult, DynamoDbError> {
    let mut bridge = ExprBridge::new(&req.parameters);

    let kce_parts: Vec<String> = key_terms
        .iter()
        .map(|t| bridge.render_condition(t))
        .collect::<Result<_, _>>()?;
    let kce = kce_parts.join(" AND ");

    let filter = render_filter(&mut bridge, filter_terms)?;

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
    // ORDER BY <key> DESC reverses the scan direction; ascending is the default.
    let forward = stmt.order_by.as_ref().is_none_or(|o| !o.descending);
    body.insert("ScanIndexForward".to_owned(), Value::Bool(forward));
    insert_names_values(&mut body, &bridge)?;
    insert_common(&mut body, req, start_key);

    crate::handle_query(Value::Object(body), ctx).await
}

/// Compose and dispatch a `Scan` for a Scan plan.
async fn scan_request(
    ctx: &OperationContext,
    stmt: &SelectStatement,
    req: &SingleRequest,
    start_key: Option<&Value>,
) -> Result<DispatchResult, DynamoDbError> {
    let mut bridge = ExprBridge::new(&req.parameters);

    let filter = match &stmt.where_clause {
        Some(w) => Some(bridge.render_condition(w)?),
        None => None,
    };

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
    insert_common(&mut body, req, start_key);

    crate::handle_scan(Value::Object(body), ctx).await
}

/// Join residual filter conjuncts into one `FilterExpression`, or `None`.
fn render_filter(
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
fn insert_common(body: &mut Map<String, Value>, req: &SingleRequest, start_key: Option<&Value>) {
    if let Some(limit) = req.limit {
        body.insert("Limit".to_owned(), Value::from(limit));
    }
    if let Some(key) = start_key {
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

/// Reshape a Query/Scan `DispatchResult` into the PartiQL `ExecuteStatement`
/// response: `Items` (projected), `ConsumedCapacity`, and, when the underlying
/// read was paged, both `LastEvaluatedKey` and an opaque statement-bound
/// `NextToken`.
fn shape_response(
    result: DispatchResult,
    statement: &str,
    projection: &Projection,
) -> Result<DispatchResult, DynamoDbError> {
    let DispatchResult { body, metrics } = result;
    let mut source = match body {
        Value::Object(m) => m,
        _ => Map::new(),
    };

    let raw_items = match source.remove("Items") {
        Some(Value::Array(items)) => items,
        _ => Vec::new(),
    };
    let items: Vec<Value> = match projection {
        Projection::All => raw_items,
        Projection::Paths(paths) => raw_items
            .iter()
            .map(|item| project_item(item, paths))
            .collect(),
    };

    let mut out = Map::new();
    out.insert("Items".to_owned(), Value::Array(items));
    if let Some(cc) = source.remove("ConsumedCapacity") {
        out.insert("ConsumedCapacity".to_owned(), cc);
    }
    if let Some(lek) = source.remove("LastEvaluatedKey") {
        out.insert(
            "NextToken".to_owned(),
            Value::String(encode_next_token(statement, &lek)),
        );
        out.insert("LastEvaluatedKey".to_owned(), lek);
    }

    Ok(DispatchResult {
        body: Value::Object(out),
        metrics,
    })
}

/// Project one wire-form item (`{"attr": {typed value}}`) down to the selected
/// paths. A plain or nested attribute keys on its leaf name; any path with a
/// list-index step keys on the literal path text. A path that does not resolve
/// is omitted, as the service omits a missing projected attribute.
pub(super) fn project_item(item: &Value, paths: &[Path]) -> Value {
    let mut out = Map::new();
    for path in paths {
        if let Some(value) = navigate(item, path) {
            out.insert(output_key(path), value);
        }
    }
    Value::Object(out)
}

/// Walk a wire-form item along a document path, descending into `M` for an
/// attribute step and `L` for a list-index step. Returns the leaf value, or
/// `None` if any step is absent or the wrong shape.
fn navigate(item: &Value, path: &Path) -> Option<Value> {
    let mut current = item.get(&path.root)?;
    for step in &path.steps {
        current = match step {
            PathStep::Attribute(name) => current.get("M")?.get(name)?,
            PathStep::Index(i) => current.get("L")?.get(*i)?,
        };
    }
    Some(current.clone())
}

/// The output map key for a projected path: the literal path text when it has a
/// list-index step, otherwise the leaf attribute name.
fn output_key(path: &Path) -> String {
    let has_index = path.steps.iter().any(|s| matches!(s, PathStep::Index(_)));
    if has_index {
        let mut text = path.root.clone();
        for step in &path.steps {
            match step {
                PathStep::Attribute(name) => {
                    text.push('.');
                    text.push_str(name);
                }
                PathStep::Index(i) => text.push_str(&format!("[{i}]")),
            }
        }
        text
    } else {
        // The leaf is the last attribute step, or the root when there are none.
        match path.steps.last() {
            Some(PathStep::Attribute(name)) => name.clone(),
            _ => path.root.clone(),
        }
    }
}

/// Partition and (optional) sort attribute names from a key schema.
/// The PartiQL wording for a `ConsistentRead` against a GSI (also returned for
/// a vector index, measured on the service).
fn gsi_strong_read_error() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "Strongly consistent read is not supported on Global Secondary Indexes".to_owned(),
    )
}

fn key_names(schema: &[KeySchemaElement]) -> (String, Option<String>) {
    let partition = schema
        .iter()
        .find(|k| k.key_type == KeyType::Hash)
        .map(|k| k.attribute_name.clone())
        .unwrap_or_default();
    let sort = schema
        .iter()
        .find(|k| k.key_type == KeyType::Range)
        .map(|k| k.attribute_name.clone());
    (partition, sort)
}

/// Flatten the top-level `AND` chain, split it into key-condition conjuncts and
/// residual filter conjuncts, and return them. Returns `None` (meaning Scan)
/// when the expression is not a flat `AND`, has no partition-key value
/// equality, or the partition-key equality's right side is not a value.
fn split_key_and_filter<'a>(
    expr: &'a WhereExpr,
    keys: KeyNames<'_>,
) -> Option<(Vec<&'a WhereExpr>, Vec<&'a WhereExpr>)> {
    let mut terms = Vec::new();
    if !flatten_and(expr, &mut terms) {
        return None;
    }

    let mut key_terms = Vec::new();
    let mut filter_terms = Vec::new();
    let mut pk_equality = false;

    for term in terms {
        if is_pk_value_equality(term, keys.partition) {
            pk_equality = true;
            key_terms.push(term);
        } else if is_sort_key_condition(term, keys.sort) {
            key_terms.push(term);
        } else {
            filter_terms.push(term);
        }
    }

    pk_equality.then_some((key_terms, filter_terms))
}

/// Flatten a top-level `AND` chain into its conjuncts. Returns false when an
/// `OR` or `NOT` sits at the top level, which forces a Scan.
fn flatten_and<'a>(expr: &'a WhereExpr, out: &mut Vec<&'a WhereExpr>) -> bool {
    match expr {
        WhereExpr::And(a, b) => flatten_and(a, out) && flatten_and(b, out),
        WhereExpr::Or(_, _) | WhereExpr::Not(_) => false,
        other => {
            out.push(other);
            true
        }
    }
}

/// Whether a term is `<partition> = <value>` (literal or parameter). A
/// double-quoted right side parses as a path, not a value, so it is not a key
/// equality and the statement degrades to a Scan.
fn is_pk_value_equality(term: &WhereExpr, partition: &str) -> bool {
    matches!(
        term,
        WhereExpr::Compare(left, CompareOp::Eq, right)
            if simple_path(left) == Some(partition) && is_value(right)
    )
}

/// Whether a term is a valid sort-key key condition (`=,<,<=,>,>=`, `BETWEEN`,
/// `begins_with`) with value operands. `<>` and `IN` are not key conditions.
fn is_sort_key_condition(term: &WhereExpr, sort: Option<&str>) -> bool {
    let Some(sort) = sort else {
        return false;
    };
    match term {
        WhereExpr::Compare(left, op, right) => {
            simple_path(left) == Some(sort)
                && is_value(right)
                && matches!(
                    op,
                    CompareOp::Eq | CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge
                )
        }
        WhereExpr::Between(left, low, high) => {
            simple_path(left) == Some(sort) && is_value(low) && is_value(high)
        }
        WhereExpr::BeginsWith(left, arg) => simple_path(left) == Some(sort) && is_value(arg),
        _ => false,
    }
}

/// The attribute name when the operand is a bare attribute path, else `None`.
fn simple_path(op: &Operand) -> Option<&str> {
    match op {
        Operand::Path(Path { root, steps }) if steps.is_empty() => Some(root.as_str()),
        _ => None,
    }
}

/// Whether an operand is a value slot (a literal or a positional parameter).
fn is_value(op: &Operand) -> bool {
    matches!(op, Operand::Literal(_) | Operand::Parameter(_))
}

/// Encode an opaque pagination token binding a `LastEvaluatedKey` to the exact
/// statement that produced it.
fn encode_next_token(statement: &str, last_evaluated_key: &Value) -> String {
    let payload = serde_json::json!({ "s": statement, "k": last_evaluated_key });
    BASE64.encode(payload.to_string().as_bytes())
}

/// Decode a pagination token and return its `LastEvaluatedKey`. A token that
/// does not decode, or whose embedded statement differs from the current one,
/// is the service's "NextToken does not match request".
fn decode_next_token(token: &str, statement: &str) -> Result<Value, DynamoDbError> {
    let mismatch =
        || DynamoDbError::ValidationException("NextToken does not match request".to_owned());
    let bytes = BASE64.decode(token).map_err(|_| mismatch())?;
    let decoded: Value = serde_json::from_slice(&bytes).map_err(|_| mismatch())?;
    if decoded.get("s").and_then(Value::as_str) != Some(statement) {
        return Err(mismatch());
    }
    decoded.get("k").cloned().ok_or_else(mismatch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::partiql::ast::Statement;
    use extenddb_core::partiql::parse;

    fn select(sql: &str) -> SelectStatement {
        match parse(sql).unwrap() {
            Statement::Select(s) => s,
            _ => panic!("not a select"),
        }
    }

    const KEYS: KeyNames<'static> = KeyNames {
        partition: "pk",
        sort: Some("sk"),
    };

    #[test]
    fn full_key_splits_both_equalities_into_key_terms() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a' AND sk = 'b'");
        let (key, filter) = split_key_and_filter(s.where_clause.as_ref().unwrap(), KEYS).unwrap();
        assert_eq!(key.len(), 2);
        assert!(filter.is_empty());
    }

    #[test]
    fn pk_plus_non_key_filter_separates_terms() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a' AND name = 'x'");
        let (key, filter) = split_key_and_filter(s.where_clause.as_ref().unwrap(), KEYS).unwrap();
        assert_eq!(key.len(), 1);
        assert_eq!(filter.len(), 1);
    }

    #[test]
    fn sort_in_list_is_a_filter_not_a_key_term() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a' AND sk IN ['x','y']");
        let (key, filter) = split_key_and_filter(s.where_clause.as_ref().unwrap(), KEYS).unwrap();
        assert_eq!(key.len(), 1);
        assert_eq!(filter.len(), 1);
    }

    #[test]
    fn double_quoted_rhs_is_not_a_key_equality() {
        // pk = "a" compares attribute pk to attribute a, not to a value, so
        // this is not a key equality and must fall back to a Scan.
        let s = select("SELECT * FROM \"t\" WHERE pk = \"a\"");
        assert!(split_key_and_filter(s.where_clause.as_ref().unwrap(), KEYS).is_none());
    }

    #[test]
    fn or_at_top_level_is_a_scan() {
        let s = select("SELECT * FROM \"t\" WHERE pk = 'a' OR sk = 'b'");
        assert!(split_key_and_filter(s.where_clause.as_ref().unwrap(), KEYS).is_none());
    }

    #[test]
    fn next_token_round_trips_and_binds_to_statement() {
        let lek = serde_json::json!({"pk": {"S": "p1"}, "sk": {"S": "s02"}});
        let token = encode_next_token("SELECT * FROM \"t\" WHERE pk='p1'", &lek);
        let decoded = decode_next_token(&token, "SELECT * FROM \"t\" WHERE pk='p1'").unwrap();
        assert_eq!(decoded, lek);
        let err = decode_next_token(&token, "SELECT * FROM \"t\" WHERE pk='p2'").unwrap_err();
        match err {
            DynamoDbError::ValidationException(m) => {
                assert_eq!(m, "NextToken does not match request")
            }
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn projects_nested_path_on_leaf_and_index_on_literal_text() {
        let item = serde_json::json!({
            "addr": {"M": {"city": {"S": "NYC"}}},
            "tags": {"L": [{"S": "a"}, {"S": "b"}]},
            "name": {"S": "n"}
        });
        let s = select("SELECT addr.city, tags[0] FROM \"t\" WHERE pk='a' AND sk='b'");
        let Projection::Paths(paths) = &s.projection else {
            panic!("expected paths");
        };
        let projected = project_item(&item, paths);
        assert_eq!(projected["city"]["S"], "NYC");
        assert_eq!(projected["tags[0]"]["S"], "a");
        assert!(projected.get("addr").is_none());
    }
}

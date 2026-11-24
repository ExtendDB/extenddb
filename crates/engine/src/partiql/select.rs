// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PartiQL `SELECT` executor.
//!
//! Reads the WHERE clause with [`WhereAnalysis`] (which disjuncts read by
//! key, their key conditions and filters), checks it the way the service
//! does, composes the classic request the matching data-plane handler
//! already understands, drives [`crate::handle_query`] /
//! [`crate::handle_scan`], and shapes the response under `Items` with an
//! opaque, statement-bound `NextToken`. Every behaviour is justified by a
//! measurement against the live service, cited where it is implemented.
//!
//! A read of a table or an index by several disjuncts, or of a table by one
//! that names a whole item, takes the disjuncts one after another, the
//! first page in key order and later pages in the order the visit rules
//! give (see [`read_steps`]). A disjunct that fails a check made when it is
//! read, or that the caller may not read, is left until the others have been
//! read.

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::partiql::ast::{Operand, Projection, SelectStatement, WhereExpr};
use extenddb_core::partiql::plan::{KeyNames, WhereAnalysis, keyed_condition};
use extenddb_core::types::{IndexType, KeySchemaElement};
use extenddb_storage::error::StorageError;

use super::SingleRequest;
use super::authorize::Gate;
use super::base_join::BaseTableJoin;
use super::common::{self, bound_value};
use super::expr_bridge::ExprBridge;
use super::index_read;
use super::next_token::{TOKEN_MISMATCH, decode_next_token, encode_next_token};
use crate::create_table::storage_err_to_dynamo;
use crate::{DispatchResult, OperationContext};

use order::direction;
use read_plan::{ReadPlan, check_keys, check_reads};
use request::{Paging, key_condition, query_request, render_filter, scan_request};
use response::{output_key, shape_response};
use steps::{StepTarget, read_steps};

pub(super) use read_plan::{MAX_DECOMPOSED_READS, TOO_MANY_DECOMPOSED_READS};
pub(super) use response::project_item;

mod order;
mod read_plan;
mod request;
mod response;
mod steps;
mod visits;

/// Refuse a projection list that names one output identifier twice, or
/// whose paths overlap. The service refuses two projected paths with one
/// identifier rather than collapsing them, ahead of two paths that overlap
/// (measured 2026-11-05: `a, a`, `m.k, k` and `m.k, m` on all three
/// operations).
///
/// # Errors
///
/// Returns the service's message for the first refusal.
pub(super) fn check_projection(stmt: &SelectStatement) -> Result<(), String> {
    let Projection::Paths(paths) = &stmt.projection else {
        return Ok(());
    };
    let mut seen = std::collections::HashSet::new();
    for path in paths {
        let key = output_key(path);
        if !seen.insert(key.clone()) {
            return Err(format!("Duplicate identifiers in select clause: {key}"));
        }
    }
    super::write::first_overlap(&paths.iter().collect::<Vec<_>>())
}

/// Execute a parsed `SELECT`, passing `gate` once the checks the service
/// makes before authorization are done, ahead of the checks made when a
/// disjunct is read (see `authorize::gate`). The disjuncts the caller may
/// not read are held back as the disjuncts that fail such a check are.
///
/// # Errors
///
/// Returns the service's `ValidationException` / `ResourceNotFoundException`
/// for an invalid target, missing table/index, a GSI strong read, a parameter
/// count mismatch, or a `NextToken` that does not match the statement, and
/// the statement's refusal.
// The checks follow the service's order; helpers would split that order across functions.
#[allow(clippy::too_many_lines)]
pub(super) async fn run(
    ctx: &OperationContext,
    stmt: &SelectStatement,
    req: &SingleRequest,
    gate: &mut Gate<'_>,
) -> Result<DispatchResult, DynamoDbError> {
    // Resolve the target table's key metadata. A missing table surfaces as the
    // service's ResourceNotFoundException ("Requested resource not found"),
    // except that a projected call (`COUNT(*)`) is reported ahead of it
    // (measured 2026-11-02).
    let key_info = match ctx.table_key_info(&stmt.from.table).await {
        Ok(info) => info,
        Err(e @ StorageError::TableNotFound(_)) => {
            return Err(match &stmt.projection_error {
                Some(message) => DynamoDbError::ValidationException(message.clone()),
                None => storage_err_to_dynamo(e),
            });
        }
        Err(e) => return Err(storage_err_to_dynamo(e)),
    };

    check_projection(stmt).map_err(DynamoDbError::ValidationException)?;

    // Resolve the key schema of the target: the index's when one is named,
    // otherwise the base table's. A named index that does not exist is the
    // PartiQL-specific bare message (the classic path appends the index name).
    //
    // Measured order of the index checks on the service: a multi-attribute
    // key wins over everything (including `ConsistentRead`); on a vector index
    // the strong-read refusal wins over the vector refusal.
    let (target_schema, index_info) = if let Some(index_name) = &stmt.from.index {
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
                (info.key_schema.clone(), Some(info))
            }
            // A projected call (`COUNT(*)`) is refused ahead of the index
            // lookup (measured 2026-11-02, with and without a WHERE).
            Err(StorageError::IndexNotFound(_)) => {
                return Err(DynamoDbError::ValidationException(
                    stmt.projection_error.clone().unwrap_or_else(|| {
                        "The table does not have the specified index".to_owned()
                    }),
                ));
            }
            Err(e) => return Err(storage_err_to_dynamo(e)),
        }
    } else {
        common::ensure_single_attribute_keys(&key_info.key_schema)?;
        (key_info.key_schema.clone(), None)
    };
    let is_gsi = index_info
        .as_ref()
        .is_some_and(|info| info.index_type == IndexType::Gsi);

    // On a table that exists, a projected call (`COUNT(*)`) is refused once the
    // target is resolved, so a multi-attribute key is reported ahead of it
    // (measured 2026-11-02).
    if let Some(message) = &stmt.projection_error {
        return Err(DynamoDbError::ValidationException(message.clone()));
    }

    // A strongly consistent read of a GSI is rejected before the data path.
    // The PartiQL message differs from the classic Query/Scan wording, so it is
    // emitted here rather than left to the underlying handler.
    if req.consistent_read && is_gsi {
        return Err(gsi_strong_read_error());
    }

    let (partition, sort) = common::key_names(&target_schema);
    let keys = KeyNames {
        partition: partition.unwrap_or_default(),
        sort,
    };

    // Read the WHERE against the target's keys, and refuse what the service
    // refuses about it and about the ORDER BY before anything is read, in
    // its order: a path into a key attribute, the ORDER BY terms, the key
    // values, the WHERE an ORDER BY reads, overlapping disjuncts and the
    // number of disjuncts (see `check_keys`). The checks a disjunct gets
    // when it is read come last (measured 2026-11-03: `pk IN [51 values]
    // AND sk = a` gets the refusal of too many disjuncts). A read of a
    // table refuses a path into a key attribute wherever the WHERE has one
    // (see `WhereAnalysis::path_into_key_refusal`), a read of an index only
    // in a disjunct read by key; a scan of an index with one is not
    // measured.
    let value_of = |op: &Operand| bound_value(op, &req.parameters).cloned();
    let analysis = stmt
        .where_clause
        .as_ref()
        .map(|w| WhereAnalysis::new(w, keys, &value_of));
    check_keys(
        analysis.as_ref(),
        stmt.order_by.as_ref(),
        keys,
        &key_info,
        index_info.is_none(),
        &req.parameters,
        &value_of,
    )?;

    // A NextToken is opaque and bound to the exact statement that produced
    // it. It is checked before the statement is authorized (measured
    // 2026-11-03), so ahead of the checks a disjunct gets when it is read.
    let start_key = req
        .next_token
        .as_deref()
        .map(|token| decode_next_token(token, &req.statement, &req.parameters))
        .transpose()?;

    // The checks made before authorization are done: authorize, each
    // disjunct of a read by key by itself, and hold back the ones the caller
    // may not read.
    let refused = gate.pass().await?;
    let checked = match &analysis {
        Some(analysis) => Some(check_reads(
            analysis,
            keys,
            &target_schema,
            &ctx.limits,
            &req.parameters,
            &value_of,
            &|read| refused.refusal(read, req),
        )?),
        None => None,
    };

    // An index that does not project every attribute refuses some reads and
    // serves others from the base table (see `index_read`).
    if let Some(info) = &index_info {
        let shared_filter = checked.as_ref().and_then(|c| c.shared_filter.as_deref());
        index_read::check(stmt, info, &key_info.key_schema, shared_filter)?;
    }

    let direction = direction(stmt.order_by.as_ref(), keys);
    let plan = ReadPlan::of(
        checked.as_ref(),
        keys,
        index_info.is_none(),
        direction,
        &value_of,
    );

    // A read that reaches back filters the base-table items, so the index read
    // under it carries no filter: a read of one disjunct filters on that
    // disjunct's conditions, a read of several on the WHERE with each key
    // condition compared with a boolean written for what it reads (see
    // `keyed_condition`), and a scan on the WHERE. Its errors lose the
    // expression prefix as every other read's do.
    let keyed_where = match (&plan, &stmt.where_clause) {
        (ReadPlan::Steps(_), Some(where_clause)) if index_info.is_some() => {
            Some(keyed_condition(where_clause, keys))
        }
        _ => None,
    };
    let join = match &index_info {
        Some(info) => {
            let where_terms: Vec<&WhereExpr> = keyed_where
                .as_ref()
                .or(stmt.where_clause.as_ref())
                .into_iter()
                .collect();
            let filter_terms = match &plan {
                ReadPlan::Key(read) => read.filter.as_slice(),
                ReadPlan::Steps(_) | ReadPlan::Scan => where_terms.as_slice(),
            };
            BaseTableJoin::for_read(
                ctx,
                req,
                stmt,
                info,
                &key_info.key_schema,
                Some(filter_terms),
            )
            .map_err(super::bare_expression_error)?
        }
        None => None,
    };

    let paging = Paging {
        start: start_key.as_ref(),
        limit: req.limit,
        descending: direction.items_descending,
    };
    let mut resume_after = None;
    let mut last_key = None;
    let dispatched = match &plan {
        ReadPlan::Key(read) => {
            let mut bridge = ExprBridge::new(&req.parameters);
            let kce = key_condition(&mut bridge, keys, read.partition, read.sort.as_ref())?;
            let filter = if join.is_some() {
                None
            } else {
                render_filter(&mut bridge, &read.filter)?
            };
            let result = query_request(ctx, stmt, req, paging, &bridge, kce, filter).await?;
            if read.returns_nothing() {
                without_items(result)
            } else {
                result
            }
        }
        ReadPlan::Steps(steps) => {
            let target = StepTarget {
                keys,
                last_key: last_key_names(keys, &key_info.key_schema),
                filtered: join.is_none(),
                refusal: checked.as_ref().and_then(|c| c.deferred.as_ref()),
            };
            let steps = read_steps(ctx, stmt, req, &target, steps, paging).await?;
            resume_after = steps.next;
            last_key = steps.last_key;
            steps.page
        }
        ReadPlan::Scan => {
            let filter = stmt.where_clause.as_ref().filter(|_| join.is_none());
            scan_request(ctx, stmt, req, paging, filter).await?
        }
    };
    let dispatched = match &join {
        Some(join) => join
            .apply(ctx, &key_info, req.consistent_read, dispatched)
            .await
            .map_err(super::bare_expression_error)?,
        None => dispatched,
    };

    let mut response = shape_response(dispatched, req, &stmt.projection);
    if let (Some(key), Value::Object(body)) = (last_key, &mut response.body) {
        body.insert("LastEvaluatedKey".to_owned(), key);
    }
    // A read of disjuncts one after another names where it goes on from,
    // the disjuncts held back for a refusal included: the request whose page
    // visits only them gets the refusal (see `read_steps`; measured
    // 2026-11-02: the first page of `pk IN ['a', <2049 bytes>]` holds item
    // 'a' and a NextToken, and the request with that token gets the key size
    // refusal).
    if let (Some(position), Value::Object(body)) = (resume_after, &mut response.body)
        && !body.contains_key("NextToken")
    {
        body.insert(
            "NextToken".to_owned(),
            Value::String(encode_next_token(
                &req.statement,
                &req.parameters,
                &position,
            )),
        );
    }
    Ok(response)
}

/// The attributes of the LastEvaluatedKey of a page of a read of steps: the
/// target's key, and for a read of an index the table's key after it, as a
/// Query of an index names them.
fn last_key_names<'k>(keys: KeyNames<'k>, table: &'k [KeySchemaElement]) -> Vec<&'k str> {
    let (partition, sort) = common::key_names(table);
    let mut names: Vec<&str> = std::iter::once(keys.partition).chain(keys.sort).collect();
    for name in partition.into_iter().chain(sort) {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// A read's result with its items left out, for a disjunct that returns
/// none of the items it reads (see `KeyRead::returns_nothing`). What it read
/// still counts, as a filter that keeps no item counts.
fn without_items(mut result: DispatchResult) -> DispatchResult {
    if let Value::Object(body) = &mut result.body {
        body.insert("Items".to_owned(), Value::Array(Vec::new()));
        body.insert("Count".to_owned(), Value::from(0));
    }
    result.metrics.returned_item_count = 0;
    result
}

/// The service's refusal of a NextToken that is not one this statement gave.
fn token_mismatch() -> DynamoDbError {
    DynamoDbError::ValidationException(TOKEN_MISMATCH.to_owned())
}

/// The PartiQL wording for a `ConsistentRead` against a GSI, also returned
/// for a vector index (measured on the service); it comes before the
/// statement is authorized (measured 2026-11-03).
pub(super) const GSI_STRONG_READ: &str =
    "Strongly consistent read is not supported on Global Secondary Indexes";

fn gsi_strong_read_error() -> DynamoDbError {
    DynamoDbError::ValidationException(GSI_STRONG_READ.to_owned())
}

#[cfg(test)]
mod tests;

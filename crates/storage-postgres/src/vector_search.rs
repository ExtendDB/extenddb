// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SearchVectors` for the PostgreSQL backend: exact nearest-neighbour scan.
//!
//! The whole search is one query. Distance, partition scoping, inline filters,
//! ordering and the row limit all evaluate in the server, so the wire carries the
//! `top_k` hits rather than every candidate row. That is the reason the embedding
//! is stored in pgvector's own column type: the SQLite backend has to stream every
//! row of a partition and compute distances in process, which is the cost
//! ADR-0004 accepted for a backend that cannot load an extension.
//!
//! Exact scan only, per ADR-0004 and the port plan. An approximate index is a
//! later `CREATE INDEX ... USING hnsw` against this same column, with no table
//! rewrite, which is what the column type buys.

use extenddb_core::types::{AttributeValue, DistanceFunction};
use extenddb_storage::error::StorageError;
use extenddb_storage::vector_lifecycle::partition_value;
use extenddb_storage::{
    BoxedFuture, VectorHit, VectorSearch, VectorSearchEngine, VectorSearchOutput,
    VectorSearchResult,
};
use pgvector::Vector;

use crate::PostgresEngine;
use crate::data::vector_table_name;

/// The SQL scoring expression for one distance function.
///
/// Every metric is ordered **ascending**, which is what lets one `ORDER BY` serve
/// all three: pgvector's `<#>` returns the negated inner product, so the most
/// similar row has the smallest value under each operator. The sign is undone
/// after ordering, in [`report_score`].
///
/// `$1` is the query vector and `${norm_param}` the caller's precomputed norm, bound
/// rather than interpolated: a norm formatted into the statement can render as `inf`
/// for a large query vector, which PostgreSQL then reads as a column name and the
/// search fails with a 500 for input the service answers.
fn score_expression(function: DistanceFunction, norm_param: usize) -> String {
    match function {
        // The CASE is a conformance requirement, not a nicety. pgvector's cosine
        // operator yields NaN when either side has zero norm, while the service
        // answers exactly 1.0 with a zero vector on either side (measured
        // 2026-08-19), which is also what the SQLite backend produces. Removing
        // the CASE would make a zero vector sort unpredictably and report NaN.
        DistanceFunction::Cosine => format!(
            "CASE WHEN nrm = 0 OR ${norm_param} = 0 THEN 1.0 \
             ELSE (embedding <=> $1)::float8 END"
        ),
        DistanceFunction::Euclidean => "(embedding <-> $1)::float8".to_owned(),
        DistanceFunction::DotProduct => "(embedding <#> $1)::float8".to_owned(),
    }
}

/// Turn the ordered SQL value into the score the engine contract defines.
///
/// Cosine and Euclidean report the distance itself, lower being more similar.
/// Dot product reports the raw inner product, higher being more similar, which is
/// the negation of what pgvector's operator returns.
fn report_score(function: DistanceFunction, ordered: f64) -> f64 {
    match function {
        DistanceFunction::Cosine | DistanceFunction::Euclidean => ordered,
        DistanceFunction::DotProduct => -ordered,
    }
}

impl VectorSearchEngine for PostgresEngine {
    fn search_vectors(&self, req: VectorSearch<'_>) -> BoxedFuture<'_, VectorSearchResult> {
        // The request borrows from the caller's frame, so own what the future needs.
        let table_id = req.key_info.table_id.clone();
        let index_name = req.index_name.to_owned();
        let query_vector = req.query_vector.to_vec();
        let cached_dimensions = req
            .key_info
            .vector_indexes
            .iter()
            .find(|vi| vi.index_name == index_name)
            .map(|vi| vi.dimensions);
        let top_k = req.top_k;
        let partition = partition_value(req.hash_key);
        let filters: Vec<(String, AttributeValue)> = req
            .filters
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).clone()))
            .collect();

        Box::pin(async move {
            let partition = partition?;

            // The index definition comes from the catalog, not from the cached key
            // info: the cache carries dimensions and the search schema but neither
            // the index id that names the data table nor the distance function,
            // without which a score cannot be computed or ordered.
            let row: Option<(String, i32, String, serde_json::Value)> = sqlx::query_as(
                "SELECT index_id, dimensions, distance_function, vector_attribute \
                 FROM vector_indexes WHERE table_id = $1 AND index_name = $2",
            )
            .bind(&table_id)
            .bind(&index_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (index_id, dimensions, distance_raw, vector_attribute_json) =
                row.ok_or_else(|| StorageError::IndexNotFound(index_name.clone()))?;
            // Stored as the serialized `VectorAttribute` rather than a bare name, so
            // it is read back the same way the write path wrote it.
            let vector_attribute_name = serde_json::from_value::<
                extenddb_core::types::VectorAttribute,
            >(vector_attribute_json)
            .map_err(|e| StorageError::Internal(format!("vector_attribute: {e}")))?
            .attribute_name;
            let dimensions = usize::try_from(dimensions).map_err(|_| {
                StorageError::Internal(format!("vector dimensions out of range: {dimensions}"))
            })?;
            let function: DistanceFunction = serde_json::from_value(serde_json::Value::String(
                distance_raw.clone(),
            ))
            .map_err(|e| StorageError::Internal(format!("unknown distance function: {e}")))?;

            if query_vector.len() != dimensions {
                // The engine validates this against the cached key info, so reaching
                // here means the catalog and the cache disagree with each other.
                return Err(StorageError::Validation(format!(
                    "query vector has {} dimensions, index expects {dimensions}",
                    query_vector.len()
                )));
            }
            if let Some(cached) = cached_dimensions
                && usize::try_from(cached).is_ok_and(|cached| cached != dimensions)
            {
                return Err(StorageError::Validation(format!(
                    "index {index_name} reports {cached} dimensions in the cached key info and \
                     {dimensions} in the catalog"
                )));
            }

            let query_norm = f64::from(query_vector.iter().map(|x| x * x).sum::<f32>().sqrt());
            let vec_table = vector_table_name(&index_id);

            // Filters are equality over the index's inline-filter attributes,
            // evaluated in SQL against the stored payload so that LIMIT applies
            // after filtering. Filtering after the limit would silently return
            // fewer than top_k matching rows.
            //
            // jsonb equality is well defined for these values because numbers are
            // normalised when an item is deserialised, so two equal numbers have
            // one representation.
            // $1 vector, $2 partition, $3 limit, $4 norm, then two per filter.
            const FIRST_FILTER_PARAM: usize = 5;
            let mut predicates = String::new();
            for i in 0..filters.len() {
                let name_param = FIRST_FILTER_PARAM + i * 2;
                let value_param = name_param + 1;
                predicates.push_str(&format!(
                    " AND item_data -> ${name_param} = ${value_param}::jsonb"
                ));
            }

            let sql = format!(
                "SELECT {score} AS score, embedding, item_data \
                 FROM {vec_table} WHERE part = $2{predicates} \
                 ORDER BY score ASC, base_pk ASC LIMIT $3",
                score = score_expression(function, 4),
            );

            // Bound as a typed vector rather than a text literal, so the value that
            // reaches the server is the same f32 sequence the engine validated.
            let mut query = sqlx::query_as::<_, (f64, Vector, serde_json::Value)>(&sql)
                .bind(Vector::from(query_vector))
                // Bytes, matching the BYTEA column: the unscoped sentinel contains
                // a NUL, so a text comparison would not even be storable.
                .bind(partition.into_bytes())
                .bind(top_k)
                .bind(query_norm);
            for (name, value) in &filters {
                let value_json = serde_json::to_string(value)
                    .map_err(|e| StorageError::Internal(format!("filter value: {e}")))?;
                query = query.bind(name).bind(value_json);
            }

            let rows = query
                .fetch_all(&self.data_pool)
                .await
                .map_err(crate::vector::map_vector_sql_error)?;

            let mut hits = Vec::with_capacity(rows.len());
            for (ordered, embedding, item_json) in rows {
                let mut item: extenddb_core::types::Item = serde_json::from_value(item_json)
                    .map_err(|e| StorageError::Internal(format!("stored item: {e}")))?;
                // Reinstated from the stored f32s rather than from a second copy in
                // the payload, so what comes back is the narrowed value that was
                // actually indexed. The engine drops it again unless a projection
                // expression names it.
                item.insert(
                    vector_attribute_name.clone(),
                    extenddb_core::validation::vector_item::vector_attribute(embedding.as_slice()),
                );
                hits.push(VectorHit {
                    item,
                    score: report_score(function, ordered),
                });
            }

            Ok(VectorSearchOutput {
                hits,
                distance_function: function,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_guards_a_zero_norm_on_either_side() {
        // The measured answer for a zero vector under cosine is exactly 1.0, on
        // either side. pgvector's operator returns NaN there, so the guard is what
        // makes the backend conformant rather than merely tidy.
        let sql = score_expression(DistanceFunction::Cosine, 4);
        assert!(sql.contains("nrm = 0"), "{sql}");
        assert!(sql.contains("THEN 1.0"), "{sql}");
        assert!(sql.contains("<=>"), "{sql}");
    }

    #[test]
    fn each_metric_uses_its_own_operator() {
        assert!(score_expression(DistanceFunction::Euclidean, 4).contains("<->"));
        assert!(score_expression(DistanceFunction::DotProduct, 4).contains("<#>"));
    }

    #[test]
    fn only_dot_product_has_its_sign_undone() {
        // Every metric is ordered ascending so that one ORDER BY serves all three;
        // the inner product is the one whose reported direction differs from the
        // ordered one.
        assert!((report_score(DistanceFunction::Cosine, 0.25) - 0.25).abs() < f64::EPSILON);
        assert!((report_score(DistanceFunction::Euclidean, 2.5) - 2.5).abs() < f64::EPSILON);
        assert!((report_score(DistanceFunction::DotProduct, -7.0) - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn the_query_norm_is_a_bind_parameter_and_never_formatted_in() {
        // A formatted norm renders as `inf` for a large query vector, and PostgreSQL
        // reads that as a column name: `ERROR: column "inf" does not exist`, a 500
        // for a search the service answers. Binding it also keeps the statement text
        // identical across queries, so the plan cache is not defeated per request.
        let sql = score_expression(DistanceFunction::Cosine, 4);
        assert!(
            sql.contains("$4 = 0"),
            "the norm must be a parameter: {sql}"
        );
        // `1.0` is the measured cosine answer for a zero-norm vector and belongs in
        // the statement. What must never appear is a rendered norm, whose failure mode
        // is the token `inf`. The signature is the real guarantee, since it takes a
        // parameter index and has no value to render; this catches a regression that
        // reintroduced one.
        assert!(
            !sql.contains("inf"),
            "a rendered norm reached the statement: {sql}"
        );
        assert_eq!(
            sql.matches("1.0").count(),
            1,
            "the only literal is the measured zero-norm score: {sql}"
        );
    }

    #[test]
    fn a_large_query_vector_does_not_overflow_the_norm() {
        // f32 accumulation overflowed to infinity here; f64 does not. The values are
        // ones the service accepts, so this is a search that must work rather than an
        // edge nobody reaches.
        let big = [1e38f32; 4];
        let norm = big
            .iter()
            .map(|x| f64::from(*x) * f64::from(*x))
            .sum::<f64>()
            .sqrt();
        assert!(norm.is_finite(), "norm overflowed: {norm}");

        // And the other end: f32 squares of 1e-30 underflow to zero, which made the
        // guard fire and every row score exactly 1.0, silently.
        let tiny = [1e-30f32; 4];
        let norm = tiny
            .iter()
            .map(|x| f64::from(*x) * f64::from(*x))
            .sum::<f64>()
            .sqrt();
        assert!(norm > 0.0, "norm underflowed to zero: {norm}");
    }
}

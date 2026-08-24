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
/// search fails with a 500 for input the service answers. The norm is bound for every
/// metric so one statement layout serves all three, and only the cosine expression
/// reads it; an unused trailing bind is accepted by the protocol, as is the resulting
/// gap in the numbering when filters follow.
///
/// # Every metric is kept finite here, in SQL
///
/// pgvector accumulates in single precision, so all three operators return a value
/// that cannot be serialised for magnitudes well below `f32::MAX`, measured on 0.8.0:
/// Euclidean overflows to `Infinity` above about 9.2e18 (its difference is doubled
/// before squaring, so it goes first), dot product returns `-Infinity` above about
/// 1.8e19, and cosine returns `NaN` at both ends, above about 1.8e19 and below about
/// 3.7e-23, because it divides two values that have both overflowed or both underflowed.
/// `serde_json` renders NaN and Infinity alike as `null`, so each of those reaches a
/// client as `"Score": null` on a 200 response.
///
/// The repair belongs here rather than where the score is reported, and that is not a
/// stylistic preference. PostgreSQL's ordering is already correct in the overflowed
/// cases: `Infinity` sorts last, so an overflowed Euclidean distance is correctly the
/// farthest, and `-Infinity` sorts first, correctly the most similar. Substituting a
/// value in Rust after the database has ordered and cut the rows would leave that
/// ordering untouched, so a hit reported as nearer could appear after one reported as
/// farther, breaking the most-similar-first contract. Clamping in the expression keeps
/// the value and the order consistent by construction.
///
/// `1e308` is a serialisation bound, not a measured answer, and it is not the true
/// distance: the real value is representable in `f64` and only pgvector's `f32`
/// accumulator loses it. Recovering it would mean unpacking every candidate vector and
/// recomputing outside the operator, which costs a per-row scan and gives up the index.
/// The difference from Amazon DynamoDB is recorded in `docs/differences-from-dynamodb.md`.
fn score_expression(function: DistanceFunction, norm_param: usize) -> String {
    match function {
        // The CASE is a conformance requirement, not a nicety. pgvector's cosine
        // operator yields NaN when either side has zero norm, while the service
        // answers exactly 1.0 with a zero vector on either side (measured
        // 2026-08-19), which is also what the SQLite backend produces. Removing
        // the CASE would make a zero vector sort unpredictably and report NaN.
        //
        // The wrapper is the same rule applied to the cases the guard cannot see: a
        // query vector whose f32 squares underflow (components around 1e-30, which
        // validation accepts) or magnitudes above about 1.8e19, where pgvector's own
        // norms underflow or overflow even though ours is correct. Both give NaN, and
        // no probed magnitude gave an infinity, so the NaN filter is the whole fix for
        // this metric: cosine distance has domain [0, 2] and the operator clamps.
        // NULLIF works as a NaN filter only because `NaN = NaN` is TRUE in PostgreSQL.
        // 1.0 is right under every reading: the true distance for orthogonal vectors,
        // the measured answer for a zero vector on either side, and what SQLite
        // returns for the same input.
        //
        // The wrapper also makes the two sides safe independently. The stored side is
        // masked today only because `nrm` comes from the shared `vector_norm`, which
        // accumulates in f32, so a tiny stored vector reads as zero and takes the
        // guard. Widening that function, the obvious later tidy-up, would open the
        // identical hole on the stored side; with this wrapper it cannot.
        DistanceFunction::Cosine => format!(
            "CASE WHEN nrm = 0 OR ${norm_param} = 0 THEN 1.0 \
             ELSE COALESCE(NULLIF((embedding <=> $1)::float8, 'NaN'::float8), 1.0) END"
        ),
        // A distance, so the overflow is at the top: cap it and the farthest row stays
        // farthest.
        DistanceFunction::Euclidean => "LEAST((embedding <-> $1)::float8, 1e308)".to_owned(),
        // The operator returns the NEGATED inner product, so its overflow is at the
        // bottom: floor it and the most similar row stays most similar, both here and
        // after `report_score` undoes the sign.
        DistanceFunction::DotProduct => "GREATEST((embedding <#> $1)::float8, -1e308)".to_owned(),
    }
}

/// The query vector's Euclidean norm, in double precision.
///
/// Each component is widened BEFORE it is squared, and the sum is accumulated as an
/// `f64`. Squaring in single precision and widening the result afterwards is the
/// same expression to read and a different function: `1e-30` squared underflows to
/// zero in `f32`, so the norm of a small-but-valid vector came out as zero, the
/// zero-vector guard fired, and every row scored exactly the same. That is a wrong
/// answer rather than an error, which is why it is a named function with its own
/// test rather than an inline expression.
///
/// Over the whole valid input domain the result is finite, and zero only for a
/// genuinely zero vector. Both ends have room to spare rather than being close
/// calls: the smallest positive `f32` subnormal squared is 1.96e-90, which is 234
/// orders of magnitude above the smallest positive `f64`, and 4096 components (the
/// dimension cap) at `f32::MAX` squared sum to 4.7e80, well inside `f64`'s range.
fn query_norm(values: &[f32]) -> f64 {
    values
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt()
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
        let base_key_schema = req.key_info.key_schema.clone();
        let base_attr_defs = req.key_info.attribute_definitions.clone();
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

            let query_norm = query_norm(&query_vector);
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

            // Ties break on the whole base key, not on the partition key alone: on a
            // composite-key table several rows share a `base_pk`, so ordering by it
            // alone leaves their relative order up to the plan, and two identical
            // searches can disagree about which of them the top_k cut keeps.
            let base_sks = crate::data::all_sort_key_info(&base_key_schema, &base_attr_defs);
            let tie_break = crate::data::vector_index::base_key_columns(&base_sks)
                .into_iter()
                .map(|col| format!("{col} ASC"))
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "SELECT {score} AS score, embedding, item_data \
                 FROM {vec_table} WHERE part = $2{predicates} \
                 ORDER BY score ASC, {tie_break} LIMIT $3",
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

            let rows = query.fetch_all(&self.data_pool).await.map_err(|e| {
                let mapped = crate::vector::map_vector_sql_error(e);
                // The data table is gone, which happens when the index is deleted
                // between the catalog read above and this statement. That is a
                // deleted index, so it answers like one instead of reporting an
                // internal failure the caller can do nothing about.
                if crate::gsi_queue::is_undefined_table(&mapped) {
                    StorageError::IndexNotFound(index_name.clone())
                } else {
                    mapped
                }
            })?;

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

    /// The guard cannot see pgvector's own single-precision norms, so the NaN it can
    /// return for a small-but-valid query vector is filtered in SQL as well.
    ///
    /// Without this, that NaN reaches a client as `"Score": null` on a 200 response,
    /// because a non-finite double serialises as null rather than failing. The filter
    /// is `NULLIF`, which works on NaN only because `NaN = NaN` is TRUE in PostgreSQL,
    /// so the assertion names it: an equivalent-looking rewrite with a comparison
    /// operator would silently stop filtering.
    #[test]
    fn cosine_substitutes_the_measured_answer_for_a_nan_distance() {
        let sql = score_expression(DistanceFunction::Cosine, 4);
        assert!(
            sql.contains("NULLIF") && sql.contains("'NaN'::float8"),
            "the NaN filter must survive: {sql}"
        );
        assert!(
            sql.contains("COALESCE"),
            "a filtered NaN must fall back to the measured answer: {sql}"
        );
        // Cosine is the only metric that divides, so it is the only one that can
        // produce a NaN; the other two overflow to an infinity and are bounded instead.
        for metric in [DistanceFunction::Euclidean, DistanceFunction::DotProduct] {
            let sql = score_expression(metric, 4);
            assert!(
                !sql.contains("NULLIF"),
                "only cosine divides, so only cosine needs the filter: {sql}"
            );
        }
    }

    /// Every metric's score stays finite, and each is bounded at the end its own
    /// accumulator overflows towards.
    ///
    /// A distance overflows upwards and is capped; the negated inner product overflows
    /// downwards and is floored. Getting the direction wrong would turn the farthest
    /// row into the nearest, which is why the bound is asserted per metric rather than
    /// as "a bound exists somewhere".
    #[test]
    fn each_metric_is_bounded_at_the_end_it_overflows_towards() {
        let euclidean = score_expression(DistanceFunction::Euclidean, 4);
        assert!(
            euclidean.contains("LEAST") && euclidean.contains("1e308"),
            "a distance must be capped above: {euclidean}"
        );
        let dot = score_expression(DistanceFunction::DotProduct, 4);
        assert!(
            dot.contains("GREATEST") && dot.contains("-1e308"),
            "the negated inner product must be floored below: {dot}"
        );
        // Cosine has domain [0, 2] and the operator clamps, so no probed magnitude
        // produced an infinity there: a bound would be dead code.
        let cosine = score_expression(DistanceFunction::Cosine, 4);
        assert!(
            !cosine.contains("LEAST") && !cosine.contains("GREATEST"),
            "cosine needs no magnitude bound: {cosine}"
        );
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
        // Both literals are named rather than counted, because both are deliberate and
        // a global count cannot say which one went missing.
        assert!(
            sql.contains("THEN 1.0"),
            "the zero-norm guard's measured answer is missing: {sql}"
        );
        assert!(
            sql.contains("), 1.0)"),
            "the NaN substitute is missing: {sql}"
        );
        assert_eq!(
            sql.matches("1.0").count(),
            2,
            "those two are the only literals the expression may carry: {sql}"
        );
    }

    /// The norm is finite for every valid input and zero only for a zero vector.
    ///
    /// Calls the function the search path calls. An earlier version of this test
    /// recomputed the arithmetic it was asserting, so it passed while the search path
    /// still accumulated in single precision: the test proved a property of itself.
    ///
    /// The magnitudes are the domain's extremes rather than samples. The smallest
    /// positive subnormal is the worst case for underflow and the dimension cap at
    /// `f32::MAX` is the worst case for overflow, so nothing between them can
    /// misbehave.
    #[test]
    fn the_query_norm_is_finite_and_only_zero_for_a_zero_vector() {
        assert!(
            query_norm(&[1e38f32; 4]).is_finite(),
            "single-precision accumulation overflowed to infinity here"
        );
        assert!(
            query_norm(&[f32::MAX; 4096]).is_finite(),
            "the dimension cap at f32::MAX must stay inside f64's range"
        );
        assert!(
            query_norm(&[1e-30f32; 4]) > 0.0,
            "a small but valid vector must not read as zero, or the guard fires and \
             every row scores the same"
        );
        // The smallest positive f32 subnormal: squaring it in f32 gives zero, in f64
        // gives 1.96e-90.
        assert!(
            query_norm(&[f32::from_bits(1)]) > 0.0,
            "the worst case for underflow must still be non-zero"
        );
        assert_eq!(
            query_norm(&[0.0f32; 4]),
            0.0,
            "a genuinely zero vector is the only input that may read as zero"
        );
    }
}

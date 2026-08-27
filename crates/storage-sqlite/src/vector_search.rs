// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Vector similarity search: exact scan over one partition.
//!
//! Exact rather than approximate, and that is a measured decision rather than a
//! placeholder. No SQLite vector extension meets this backend's constraints: the
//! static-musl `FROM scratch` build cannot `dlopen` a loadable extension, the one
//! extension with a compatible licence and an in-database index (`sqlite-vec`) is
//! brute force in every stable release anyway, and every option offering a real
//! ANN index either stores it in a sidecar file, forbids transactions, or is not
//! open source. See `docs/adr` for the full elimination.
//!
//! Measured cost on one core, warm cache, row-per-vector layout: roughly 213k to
//! 334k vectors/sec at 256 dimensions, 94k to 103k at 1024, and 39k to 43k at
//! 4096. So a partition stays inside a 10 ms budget up to about 1,000 vectors at
//! 1024 dimensions, and inside 100 ms up to about 10,000. The scan is dominated by
//! getting bytes out of SQLite rather than by the arithmetic, which is why a
//! zero-copy `&[f32]` view of the blob measured no faster than decoding per
//! element.

use extenddb_core::types::{AttributeValue, DistanceFunction, Item};
use extenddb_storage::error::StorageError;
use extenddb_storage::vector_lifecycle::partition_value;
use extenddb_storage::{
    BoxedFuture, VectorHit, VectorSearch, VectorSearchEngine, VectorSearchOutput,
    VectorSearchResult,
};

use crate::data::vector_table_name;
use crate::store::SqliteEngine;

/// Decode a stored vector blob into `f32`s.
///
/// Rejects a truncated blob rather than reading a short vector, because a
/// dimension mismatch would silently change every distance in the result.
fn decode_vector(bytes: &[u8], dimensions: usize) -> Result<Vec<f32>, StorageError> {
    if bytes.len() != dimensions * 4 {
        return Err(StorageError::Internal(format!(
            "stored vector is {} bytes, expected {} for {dimensions} dimensions",
            bytes.len(),
            dimensions * 4
        )));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

/// The Euclidean norm of a vector, in double precision.
///
/// Not the shared `vector_norm`, which returns an `f32` and is what the stored `nrm`
/// column holds: squaring an `f32` component in `f32` overflows above about 1.8e19 and
/// underflows below about 1e-22, both well inside the range of components validation
/// accepts, so the stored norm is unusable for deciding whether a vector is zero.
fn norm_f64(components: &[f32]) -> f64 {
    components
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt()
}

/// The inner product of two vectors, in double precision.
fn dot_f64(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum()
}

/// Score one candidate under the index's distance function.
///
/// Cosine and Euclidean are distances, so smaller is more similar; dot product is
/// a similarity, so larger is. The caller must not compare scores across
/// functions, which is why the output reports which one was used.
///
/// Every accumulation is `f64`, and that is a correctness requirement rather than
/// precision hygiene. In `f32` these overflow for components validation accepts, and
/// the result was a score that cannot be serialised at all: a Euclidean difference is
/// doubled before squaring, so `1e19` against `-1e19` gave `inf`; a dot product of
/// `2e19` gave `inf`; and cosine at `3.4e38` divided two overflowed values and gave
/// `NaN`. `serde_json` renders both as `null`, so a client received `"Score": null` on
/// a 200 response. Widened, the worst case in the whole domain is 4096 components at
/// `f32::MAX` squared, which is 4.7e80 and finite, so no clamping is needed here: the
/// values are simply correct. The PostgreSQL backend cannot do this, because the
/// arithmetic happens inside pgvector, so it bounds the result in SQL instead.
fn score(function: DistanceFunction, query: &[f32], candidate: &[f32]) -> f64 {
    match function {
        DistanceFunction::Cosine => {
            let query_norm = norm_f64(query);
            let candidate_norm = norm_f64(candidate);
            if query_norm == 0.0 || candidate_norm == 0.0 {
                // Undefined angle. Reported as maximally distant rather than as
                // an error, matching how a zero vector is treated elsewhere.
                //
                // Genuinely zero, not merely small: the norms above are computed in
                // f64 for exactly this decision. With an f32 norm a vector of 1e-30
                // components read as zero, so every row scored 1.0 and the search
                // silently returned a ranking it had not computed.
                return 1.0;
            }
            // Clamped because the quotient can exceed 1 by a float epsilon when the
            // vectors are identical, which made an exact self-match report a
            // NEGATIVE distance (-1.19e-07 was measured against a stored item's own
            // vector). Cosine distance has domain [0, 2], so a consumer that
            // clamps, or takes a square root of the score, sees a value the metric
            // cannot produce. The service returned +1.49e-08 for the same query.
            let similarity =
                (dot_f64(query, candidate) / (query_norm * candidate_norm)).clamp(-1.0, 1.0);
            1.0 - similarity
        }
        DistanceFunction::Euclidean => query
            .iter()
            .zip(candidate)
            .map(|(x, y)| {
                let d = f64::from(*x) - f64::from(*y);
                d * d
            })
            .sum::<f64>()
            .sqrt(),
        DistanceFunction::DotProduct => dot_f64(query, candidate),
    }
}

/// Keeps the best `k` seen so far, ordered by the index's distance function.
///
/// A full sort of the partition would dominate the scan for a large partition and
/// is unnecessary: only `k` rows are ever returned. Insertion into a `k`-sized
/// vector is cheap because the common case after the first `k` candidates is a
/// single comparison against the current worst.
struct TopK {
    k: usize,
    function: DistanceFunction,
    /// The decoded components ride along with each retained hit so the returned
    /// attribute is rebuilt only for the `k` survivors. Rebuilding during the scan
    /// would allocate a decimal string per component per row examined, which at
    /// 4096 dimensions over a large partition would cost far more than the scan.
    /// Moving the already-decoded vector in is free.
    hits: Vec<(f64, Item, Vec<f32>)>,
}

impl TopK {
    fn new(k: usize, function: DistanceFunction) -> Self {
        Self {
            k,
            function,
            hits: Vec::with_capacity(k.saturating_add(1)),
        }
    }

    /// True when `a` should rank ahead of `b`.
    fn ranks_before(&self, a: f64, b: f64) -> bool {
        self.function.ranks_before(a, b)
    }

    fn offer(&mut self, candidate_score: f64, item: Item, components: Vec<f32>) {
        if self.hits.len() < self.k {
            let pos = self
                .hits
                .iter()
                .position(|(s, _, _)| self.ranks_before(candidate_score, *s))
                .unwrap_or(self.hits.len());
            self.hits.insert(pos, (candidate_score, item, components));
            return;
        }
        if self.k == 0 {
            return;
        }
        let worst = self.hits[self.k - 1].0;
        if !self.ranks_before(candidate_score, worst) {
            return;
        }
        let pos = self
            .hits
            .iter()
            .position(|(s, _, _)| self.ranks_before(candidate_score, *s))
            .unwrap_or(self.k - 1);
        self.hits.insert(pos, (candidate_score, item, components));
        self.hits.truncate(self.k);
    }
}

impl VectorSearchEngine for SqliteEngine {
    fn search_vectors(&self, req: VectorSearch<'_>) -> BoxedFuture<'_, VectorSearchResult> {
        // The request borrows; own what the async body needs so the future is not
        // tied to the caller's frame.
        let table_id = req.key_info.table_id.clone();
        let index_name = req.index_name.to_owned();
        let query_vector = req.query_vector.to_vec();
        let top_k = req.top_k;
        let partition = partition_value(req.hash_key);
        let filters: Vec<(String, AttributeValue)> = req
            .filters
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).clone()))
            .collect();

        Box::pin(async move {
            let partition = partition?;

            // The index definition comes from the catalog rather than from
            // TableKeyInfo, because the cached key info carries dimensions and the
            // search schema but not the distance function, without which a score
            // cannot be computed or ordered.
            let row: Option<(String, i64, String, String)> = sqlx::query_as(
                "SELECT index_id, dimensions, distance_function, vector_attribute \
                 FROM vector_indexes WHERE table_id = ? AND index_name = ?",
            )
            .bind(&table_id)
            .bind(&index_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (index_id, dimensions, distance_raw, vector_attribute_json) =
                row.ok_or_else(|| StorageError::IndexNotFound(index_name.clone()))?;
            // Stored as the serialized `VectorAttribute`, not a bare name, so it is
            // deserialized exactly as the write path does. Treating the column as a
            // plain string yields the key `{"AttributeName":"emb"}`.
            let vector_attribute_name =
                serde_json::from_str::<extenddb_core::types::VectorAttribute>(
                    &vector_attribute_json,
                )
                .map_err(|e| StorageError::Internal(format!("vector_attribute: {e}")))?
                .attribute_name;
            let dimensions = usize::try_from(dimensions).map_err(|_| {
                StorageError::Internal(format!("vector dimensions out of range: {dimensions}"))
            })?;
            let function: DistanceFunction = serde_json::from_str(&format!("\"{distance_raw}\""))
                .map_err(|e| {
                StorageError::Internal(format!("unknown distance function: {e}"))
            })?;

            if query_vector.len() != dimensions {
                // Core validates this against the cached key info, so reaching
                // here means the catalog and the cache disagree.
                return Err(StorageError::Validation(format!(
                    "query vector has {} dimensions, index expects {dimensions}",
                    query_vector.len()
                )));
            }

            let vec_table = vector_table_name(&table_id, &index_id);
            // `nrm` is deliberately not selected. The stored norm is an f32 and cannot
            // be trusted for either the zero test or the cosine denominator, so the
            // scorer recomputes both sides in f64 from the vector it already decoded.
            // The column stays because dropping it would need a data migration for
            // no benefit; see the reasoning on `create_vector_data_table`.
            let sql = format!("SELECT vec, item_data FROM {vec_table} WHERE part = ?");

            let k = usize::try_from(top_k.max(0)).unwrap_or(0);
            let mut top = TopK::new(k, function);

            // Streamed rather than fetched all at once, so a large partition does
            // not allocate proportionally to its size. This is the reason the
            // row-per-vector layout was chosen over a packed blob per partition.
            use futures::TryStreamExt;
            let mut stream = sqlx::query_as::<_, (Vec<u8>, String)>(&sql)
                .bind(&partition)
                .fetch(&self.pool);

            while let Some((blob, item_json)) = stream
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let candidate = decode_vector(&blob, dimensions)?;
                let item: Item = serde_json::from_str(&item_json)
                    .map_err(|e| StorageError::Internal(format!("stored item: {e}")))?;

                // Inline-filter attributes are applied here rather than in SQL,
                // because they are item attributes rather than columns. Equality
                // only, which is all the wire surface admits today.
                if !filters.is_empty()
                    && !filters
                        .iter()
                        .all(|(name, expected)| item.get(name) == Some(expected))
                {
                    continue;
                }

                let candidate_score = score(function, &query_vector, &candidate);
                top.offer(candidate_score, item, candidate);
            }

            Ok(VectorSearchOutput {
                hits: top
                    .hits
                    .into_iter()
                    .map(|(score, mut item, components)| {
                        // Reinstated from the stored `f32`s rather than from a
                        // second copy in the payload, so what comes back is the
                        // narrowed value that was actually indexed. The engine drops
                        // it again unless a `ProjectionExpression` names it, and the
                        // billed byte count subtracts it, so putting it here does not
                        // change either the default response or the metric.
                        item.insert(
                            vector_attribute_name.clone(),
                            extenddb_core::validation::vector_item::vector_attribute(&components),
                        );
                        VectorHit { item, score }
                    })
                    .collect(),
                distance_function: function,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item() -> Item {
        Item::new()
    }

    #[test]
    fn cosine_of_identical_vectors_is_zero() {
        let v = [1.0f32, 2.0, 3.0];
        let s = score(DistanceFunction::Cosine, &v, &v);
        assert!(s.abs() < 1e-6, "expected ~0.0, got {s}");
    }

    /// Cosine distance has domain [0, 2], and a self-match must land on the zero
    /// end of it from ABOVE.
    ///
    /// This exists because the test above cannot catch the failure it was
    /// nominally covering: it asserts `s.abs() < 1e-6`, which is satisfied by
    /// -1.19e-07, the exact value a live self-match returned before the
    /// similarity was clamped. Taking the absolute value discards the sign, which
    /// was the only thing wrong.
    ///
    /// Many vectors are tried rather than one, because whether the f32 quotient
    /// lands above 1 depends on the particular rounding of that vector's norm, so
    /// a single hand-picked case would prove very little.
    #[test]
    fn cosine_distance_is_never_negative_for_a_self_match() {
        let mut seed = 0x2026_0811_u64;
        for _ in 0..2000 {
            // xorshift, so the case set is fixed and reproducible.
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let dim = 8 + (seed % 121) as usize;
            let mut v = Vec::with_capacity(dim);
            let mut s = seed;
            for _ in 0..dim {
                s = s.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                v.push(((s >> 33) as f32 / u32::MAX as f32) - 0.5);
            }
            if norm_f64(&v) == 0.0 {
                continue;
            }
            let d = score(DistanceFunction::Cosine, &v, &v);
            assert!(
                d >= 0.0,
                "cosine distance left its domain for a self-match: {d} (dim {dim})"
            );
            assert!(d < 1e-6, "a self-match must still be ~0: {d}");
        }
    }

    /// Every metric returns a FINITE score for the extremes of the input domain.
    ///
    /// This replaces a test that forced the cosine clamp by handing in a norm 1% below
    /// the true one. That mechanism is gone: the scorer computes both norms itself now,
    /// so a caller cannot understate them, and the clamp is exercised by the self-match
    /// property above instead.
    ///
    /// These are the magnitudes at which single-precision accumulation broke, measured
    /// before the widening: `1e19` against `-1e19` gave `inf` under Euclidean, because
    /// the difference is doubled before squaring; `2e19` gave `inf` under dot product;
    /// and `3.4e38` gave `NaN` under cosine, dividing two overflowed values. Each
    /// reached a client as `"Score": null` on a 200 response, because that is how
    /// `serde_json` renders a non-finite double.
    ///
    /// The JSON rendering is asserted rather than only `is_finite`, because null is what
    /// the client actually saw and it is what a regression would reintroduce.
    #[test]
    fn every_metric_scores_the_extremes_of_the_domain_as_a_finite_number() {
        let cases = [
            (
                DistanceFunction::Euclidean,
                [1e19f32, 0.0, 0.0, 0.0],
                [-1e19f32, 0.0, 0.0, 0.0],
            ),
            (
                DistanceFunction::DotProduct,
                [2e19f32, 0.0, 0.0, 0.0],
                [2e19f32, 0.0, 0.0, 0.0],
            ),
            (
                DistanceFunction::Cosine,
                [3.4e38f32, 1e38, 0.0, 0.0],
                [3.4e38f32, 0.0, 0.0, 0.0],
            ),
        ];
        for (function, query, candidate) in cases {
            let s = score(function, &query, &candidate);
            assert!(
                s.is_finite(),
                "{function:?} produced a non-finite score: {s}"
            );
            assert_ne!(
                serde_json::to_string(&s).expect("serialise the score"),
                "null",
                "{function:?} produced a score that serialises as null: {s}"
            );
        }

        // The other end: components small enough that their f32 squares underflow must
        // NOT read as a zero vector, or the guard fires and every row scores 1.0.
        let tiny = [1e-30f32, 0.0, 0.0, 0.0];
        assert!(
            score(DistanceFunction::Cosine, &tiny, &[1.0, 0.0, 0.0, 0.0]) < 1e-9,
            "a tiny vector parallel to the candidate is a near-zero distance, not the \
             zero-vector answer"
        );
        assert!(
            (score(DistanceFunction::Cosine, &tiny, &[0.0, 1.0, 0.0, 0.0]) - 1.0).abs() < 1e-9,
            "and orthogonal to it is exactly 1.0, by the metric rather than by the guard"
        );
    }

    #[test]
    fn cosine_of_opposite_vectors_is_two() {
        let a = [1.0f32, 0.0];
        let b = [-1.0f32, 0.0];
        let s = score(DistanceFunction::Cosine, &a, &b);
        assert!((s - 2.0).abs() < 1e-6, "expected ~2.0, got {s}");
    }

    #[test]
    fn a_zero_vector_is_maximally_distant_rather_than_an_error() {
        let a = [1.0f32, 0.0];
        let z = [0.0f32, 0.0];
        assert!((score(DistanceFunction::Cosine, &a, &z) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn euclidean_is_the_straight_line_distance() {
        let a = [0.0f32, 0.0];
        let b = [3.0f32, 4.0];
        let s = score(DistanceFunction::Euclidean, &a, &b);
        assert!((s - 5.0).abs() < 1e-6, "expected 5.0, got {s}");
    }

    #[test]
    fn dot_product_is_reported_raw_and_can_be_negative() {
        let a = [1.0f32, 0.0];
        let b = [-2.0f32, 0.0];
        let s = score(DistanceFunction::DotProduct, &a, &b);
        assert!((s + 2.0).abs() < 1e-6, "expected -2.0, got {s}");
    }

    /// The direction of "better" is not uniform, so top-k must consult the
    /// distance function. A single ordering would silently return the *worst*
    /// matches for dot product.
    #[test]
    fn top_k_orders_distances_ascending_and_similarities_descending() {
        let mut cosine = TopK::new(2, DistanceFunction::Cosine);
        for s in [0.9, 0.1, 0.5] {
            cosine.offer(s, item(), vec![]);
        }
        assert_eq!(
            cosine.hits.iter().map(|(s, _, _)| *s).collect::<Vec<_>>(),
            vec![0.1, 0.5]
        );

        let mut dot = TopK::new(2, DistanceFunction::DotProduct);
        for s in [0.9, 0.1, 0.5] {
            dot.offer(s, item(), vec![]);
        }
        assert_eq!(
            dot.hits.iter().map(|(s, _, _)| *s).collect::<Vec<_>>(),
            vec![0.9, 0.5]
        );
    }

    /// Each retained hit must keep its *own* vector. The components are inserted at
    /// a computed position alongside the score, so an off-by-one there would return
    /// a neighbour's vector against this item's attributes: wrong data, no error.
    #[test]
    fn a_retained_hit_keeps_its_own_vector() {
        let mut t = TopK::new(3, DistanceFunction::Cosine);
        // Offered worst-first so every insert lands at the front and the pairing is
        // exercised rather than incidentally correct.
        for (score, tag) in [(0.9f64, 9.0f32), (0.5, 5.0), (0.1, 1.0)] {
            t.offer(score, item(), vec![tag]);
        }
        let paired: Vec<(f64, f32)> = t
            .hits
            .iter()
            .map(|(s, _, components)| (*s, components[0]))
            .collect();
        assert_eq!(paired, vec![(0.1, 1.0), (0.5, 5.0), (0.9, 9.0)]);
    }

    #[test]
    fn top_k_of_zero_returns_nothing_rather_than_panicking() {
        let mut t = TopK::new(0, DistanceFunction::Cosine);
        t.offer(0.5, item(), vec![]);
        assert!(t.hits.is_empty());
    }

    #[test]
    fn a_truncated_stored_vector_is_rejected_rather_than_read_short() {
        let err = decode_vector(&[0u8; 8], 3).expect_err("must reject");
        assert!(
            format!("{err:?}").contains("expected 12"),
            "unexpected: {err:?}"
        );
    }
}

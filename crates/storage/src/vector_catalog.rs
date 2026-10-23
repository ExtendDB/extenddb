// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared decoding of vector index catalog rows.
//!
//! Every backend stores the same eight pieces of vector index metadata and then
//! has to turn them into the same two shapes: the `VectorIndexDescription` a
//! client reads in order to build a search, and the `VectorIndexKeyInfo` the
//! write path caches. The storage formats differ (PostgreSQL has JSONB columns,
//! SQLite has JSON text), so decoding a row stays with the backend. Everything
//! after that is rules rather than format, and several of those rules are
//! wire-visible: the index ARN, the readiness check, reporting no indexes as an
//! absent member rather than an empty list, and reporting an unscoped index's
//! search schema as absent rather than empty. Two copies of a wire-visible rule
//! is how two backends come to answer the same request differently, so they live
//! here once.
//!
//! Each backend reads its own row type with `sqlx::FromRow`, converts it into
//! [`VectorIndexCatalogRow`], and calls one of the two functions below.

use extenddb_core::types::{
    DistanceFunction, IndexStatus, Projection, SearchSchemaElement, VectorAttribute,
    VectorIndexDescription, VectorIndexKeyInfo,
};

use crate::error::StorageError;
use crate::util::index_arn;

/// A `vector_indexes` catalog row with its storage format already decoded.
///
/// JSON payloads arrive as `serde_json::Value` because that is what PostgreSQL
/// returns natively and what SQLite reaches by parsing its text column. Enum
/// tokens arrive as strings so that an unrecognised one can be reported with the
/// offending value rather than silently mapped to a fallback.
pub struct VectorIndexCatalogRow {
    pub index_name: String,
    /// Declared dimension count. Widened to `i64` so both backends' integer
    /// column types fit without either one narrowing before the range check.
    pub dimensions: i64,
    pub distance_function: String,
    pub vector_attribute: serde_json::Value,
    /// Absent for an unscoped index, which is a different state from an empty
    /// list and is preserved as such.
    pub search_schema: Option<serde_json::Value>,
    pub projection: serde_json::Value,
    pub index_status: String,
    /// Absent once the index is ACTIVE, which is how the service reports it.
    pub backfilling: Option<bool>,
}

/// The catalog's spelling of a distance function.
///
/// Stored as the wire token so a describe can hand it straight back, and derived
/// from the enum's own serialisation rather than a hand-written match, so adding a
/// distance function cannot silently persist the wrong string. Lives beside the
/// read direction, which parses this same token, so the pair cannot drift.
///
/// # Errors
///
/// Returns [`StorageError::Internal`] if the enum does not serialise to a string,
/// which would mean its representation changed under this function.
#[must_use = "the token is what gets stored"]
pub fn distance_function_token(
    distance_function: DistanceFunction,
) -> Result<String, StorageError> {
    match serde_json::to_value(distance_function)
        .map_err(|e| StorageError::Internal(e.to_string()))?
    {
        serde_json::Value::String(s) => Ok(s),
        other => Err(StorageError::Internal(format!(
            "distance function did not serialise to a string: {other}"
        ))),
    }
}

/// Decode one JSON payload, naming the column so a failure is diagnosable.
fn decode<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
    column: &str,
) -> Result<T, StorageError> {
    serde_json::from_value(value).map_err(|e| StorageError::Internal(format!("{column}: {e}")))
}

/// Parse a stored dimension count into the width the wire uses.
fn dimensions(raw: i64) -> Result<u32, StorageError> {
    u32::try_from(raw)
        .map_err(|_| StorageError::Internal(format!("vector dimensions out of range: {raw}")))
}

/// Parse a stored `IndexStatus` token, refusing one this build cannot read.
///
/// `IndexStatus` carries a catch-all variant so that parsing a *service*
/// response tolerates a status added after this build shipped. Reading our own
/// catalog is the opposite situation: a value we do not recognise is a corrupt
/// row or one written by a newer version, and reporting it to a client as
/// "UNKNOWN" would be describing an index whose state we do not know. So the
/// catch-all is rejected explicitly here, which the deserializer alone cannot do.
fn index_status(token: &str) -> Result<IndexStatus, StorageError> {
    let status: IndexStatus = decode(
        serde_json::Value::String(token.to_owned()),
        "vector index_status",
    )?;
    if status == IndexStatus::Unknown {
        return Err(StorageError::Internal(format!(
            "unrecognised vector index status in the catalog: {token}"
        )));
    }
    Ok(status)
}

/// Build the descriptions a `DescribeTable` or `UpdateTable` response carries.
///
/// Fails rather than defaulting on a bad row: a client uses this description to
/// build a search, so an index that cannot be described faithfully must not be
/// reported as if it could.
///
/// Returns `None` when the table has no vector indexes, because the response
/// member is absent in that case rather than an empty list.
///
/// # Errors
///
/// Returns [`StorageError::Internal`] when a payload cannot be decoded, a
/// dimension count does not fit, a status or distance function token is
/// unrecognised, or the resulting description reports a state the wire contract
/// forbids.
pub fn vector_index_descriptions(
    region: &str,
    account_id: &str,
    table_name: &str,
    rows: Vec<VectorIndexCatalogRow>,
) -> Result<Option<Vec<VectorIndexDescription>>, StorageError> {
    let mut descs: Vec<VectorIndexDescription> = Vec::with_capacity(rows.len());
    for row in rows {
        let vector_attribute: VectorAttribute = decode(row.vector_attribute, "vector_attribute")?;
        let search_schema: Option<Vec<SearchSchemaElement>> = row
            .search_schema
            .map(|value| decode(value, "vector search_schema"))
            .transpose()?;
        let projection: Projection = decode(row.projection, "vector projection")?;
        let distance_function: DistanceFunction = decode(
            serde_json::Value::String(row.distance_function),
            "vector distance_function",
        )?;
        let desc = VectorIndexDescription {
            index_name: row.index_name.clone(),
            vector_attribute,
            dimensions: dimensions(row.dimensions)?,
            search_schema,
            distance_function,
            index_status: index_status(&row.index_status)?,
            backfilling: row.backfilling,
            index_size_bytes: 0,
            item_count: 0,
            index_arn: index_arn(region, account_id, table_name, &row.index_name),
            projection: Some(projection),
        };
        // The readiness rule is core's, applied here so no backend can emit a
        // description the wire contract forbids. Cheaper to catch on the way out
        // than to debug from a client.
        desc.validate_readiness()
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        descs.push(desc);
    }
    Ok((!descs.is_empty()).then_some(descs))
}

/// Build the vector index metadata the write path caches on `TableKeyInfo`.
///
/// Note what this deliberately cannot carry: no index id and no distance
/// function, so a search still reads the catalog for them. Widening
/// `VectorIndexKeyInfo` would remove the last per-search catalog read.
///
/// This path deliberately does not look at `index_status`, and that asymmetry with
/// the describe path is load-bearing rather than an oversight: a catalog row whose
/// status cannot be read must fail a describe, which reports index state, but must
/// not fail every write to the table, which does not depend on it. Validating
/// everything everywhere here would turn one bad row into a table-wide data-plane
/// outage.
///
/// An unscoped index reports an empty search schema here, not `None`: the
/// distinction matters on the describe path, where the member's absence is
/// wire-visible, and does not matter to a write, which only asks which
/// attributes it must look at.
///
/// # Errors
///
/// Returns [`StorageError::Internal`] when a payload cannot be decoded or a
/// dimension count does not fit.
pub fn vector_index_key_info(
    rows: Vec<VectorIndexCatalogRow>,
) -> Result<Vec<VectorIndexKeyInfo>, StorageError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let attr: VectorAttribute = decode(row.vector_attribute, "vector_attribute")?;
        let search_schema: Vec<SearchSchemaElement> = match row.search_schema {
            Some(value) => decode(value, "vector search_schema")?,
            None => Vec::new(),
        };
        let projection: Projection = decode(row.projection, "vector projection")?;
        out.push(VectorIndexKeyInfo {
            index_name: row.index_name,
            dimensions: dimensions(row.dimensions)?,
            vector_attribute_name: attr.attribute_name,
            search_schema,
            projection,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(index_status: &str) -> VectorIndexCatalogRow {
        VectorIndexCatalogRow {
            index_name: "vidx".to_owned(),
            dimensions: 4,
            distance_function: "COSINE".to_owned(),
            vector_attribute: serde_json::json!({ "AttributeName": "emb" }),
            search_schema: Some(
                serde_json::json!([{ "AttributeName": "pk", "SearchSchemaElementType": "HASH" }]),
            ),
            projection: serde_json::json!({ "ProjectionType": "ALL" }),
            index_status: index_status.to_owned(),
            backfilling: None,
        }
    }

    #[test]
    fn a_table_with_no_vector_indexes_reports_an_absent_member() {
        // Absent, not an empty list: the service omits the member entirely, and a
        // client distinguishes the two.
        assert_eq!(
            vector_index_descriptions("us-east-1", "1", "t", Vec::new()).unwrap(),
            None
        );
    }

    #[test]
    fn an_active_index_is_described_with_its_arn() {
        let descs =
            vector_index_descriptions("us-east-1", "123456789012", "t", vec![row("ACTIVE")])
                .unwrap()
                .expect("one index");
        assert_eq!(descs.len(), 1);
        assert_eq!(descs[0].index_status, IndexStatus::Active);
        assert_eq!(descs[0].dimensions, 4);
        assert_eq!(
            descs[0].index_arn,
            "arn:aws:dynamodb:us-east-1:123456789012:table/t/index/vidx"
        );
    }

    #[test]
    fn an_unrecognised_status_is_refused_rather_than_reported_as_unknown() {
        // The catch-all variant exists for reading a service response, where a
        // new status must not break the client. Reading our own catalog, it would
        // turn a corrupt row into an index described with a state nobody knows,
        // so it is refused with the offending value named.
        let err = vector_index_descriptions("us-east-1", "1", "t", vec![row("ACITVE")])
            .expect_err("an unrecognised status must not be described");
        match err {
            StorageError::Internal(msg) => {
                assert!(msg.contains("ACITVE"), "{msg}");
                assert!(msg.contains("unrecognised"), "{msg}");
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn an_unrecognised_distance_function_is_refused() {
        let mut bad = row("ACTIVE");
        bad.distance_function = "MANHATTAN".to_owned();
        let err = vector_index_descriptions("us-east-1", "1", "t", vec![bad])
            .expect_err("an unrecognised distance function must not be described");
        assert!(matches!(err, StorageError::Internal(_)), "{err:?}");
    }

    #[test]
    fn an_active_index_reporting_backfilling_is_refused() {
        // The wire rule is that the member disappears once the index is active,
        // so reporting both is a contradiction a client would have to guess about.
        let mut bad = row("ACTIVE");
        bad.backfilling = Some(false);
        let err = vector_index_descriptions("us-east-1", "1", "t", vec![bad])
            .expect_err("ACTIVE with a Backfilling member must be refused");
        assert!(matches!(err, StorageError::Internal(_)), "{err:?}");
    }

    #[test]
    fn a_building_index_keeps_its_backfilling_state() {
        let mut building = row("CREATING");
        building.backfilling = Some(true);
        let descs = vector_index_descriptions("us-east-1", "1", "t", vec![building])
            .unwrap()
            .expect("one index");
        assert_eq!(descs[0].index_status, IndexStatus::Creating);
        assert_eq!(descs[0].backfilling, Some(true));
    }

    #[test]
    fn a_dimension_count_that_does_not_fit_is_refused() {
        let mut bad = row("ACTIVE");
        bad.dimensions = i64::from(u32::MAX) + 1;
        let err = vector_index_descriptions("us-east-1", "1", "t", vec![bad])
            .expect_err("a dimension count outside the wire width must be refused");
        match err {
            StorageError::Internal(msg) => assert!(msg.contains("out of range"), "{msg}"),
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn key_info_reports_an_unscoped_index_with_an_empty_search_schema() {
        // The opposite convention from the describe path, deliberately: a write
        // only asks which attributes it must look at, and an empty list answers
        // that without every caller having to unwrap an Option.
        let mut unscoped = row("ACTIVE");
        unscoped.search_schema = None;
        let info = vector_index_key_info(vec![unscoped]).unwrap();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].index_name, "vidx");
        assert_eq!(info[0].vector_attribute_name, "emb");
        assert!(info[0].search_schema.is_empty());
    }

    #[test]
    fn key_info_carries_the_search_schema_of_a_scoped_index() {
        let info = vector_index_key_info(vec![row("ACTIVE")]).unwrap();
        assert_eq!(info[0].search_schema.len(), 1);
        assert_eq!(info[0].search_schema[0].attribute_name, "pk");
    }

    #[test]
    fn key_info_refuses_a_payload_it_cannot_decode() {
        let mut bad = row("ACTIVE");
        bad.vector_attribute = serde_json::json!({ "Nonsense": true });
        let err = vector_index_key_info(vec![bad]).expect_err("a bad payload must be refused");
        match err {
            StorageError::Internal(msg) => assert!(msg.contains("vector_attribute"), "{msg}"),
            other => panic!("expected Internal, got {other:?}"),
        }
    }
}

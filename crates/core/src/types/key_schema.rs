// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde::{Deserialize, Serialize};

/// Key schema element — defines a key attribute and its role.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeySchemaElement {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "KeyType")]
    pub key_type: KeyType,
}

/// Key type — HASH (partition key) or RANGE (sort key).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum KeyType {
    #[serde(rename = "HASH")]
    Hash,
    #[serde(rename = "RANGE")]
    Range,
}

impl<'de> serde::Deserialize<'de> for KeyType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "HASH" => Ok(Self::Hash),
            "RANGE" => Ok(Self::Range),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'keySchema.1.member.keyType' \
                 failed to satisfy constraint: Member must satisfy enum value set: [HASH, RANGE]"
            ))),
        }
    }
}

/// Attribute definition — maps an attribute name to a scalar type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttributeDefinition {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "AttributeType")]
    pub attribute_type: ScalarAttributeType,
}

/// Scalar attribute type — only S, N, B are valid for key attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ScalarAttributeType {
    S,
    N,
    B,
}

impl<'de> serde::Deserialize<'de> for ScalarAttributeType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "S" => Ok(Self::S),
            "N" => Ok(Self::N),
            "B" => Ok(Self::B),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'attributeDefinitions.1.member.attributeType' \
                 failed to satisfy constraint: Member must satisfy enum value set: [B, N, S]"
            ))),
        }
    }
}

/// Lightweight key schema + attribute definitions for a table.
///
/// Used by data operations (`PutItem`, `GetItem`) that need key metadata
/// without the full `TableDescription` overhead. Includes stream specification
/// so write operations can check stream status without an extra SQL round-trip.
#[derive(Debug, Clone, Default)]
pub struct TableKeyInfo {
    pub table_name: String,
    pub account_id: String,
    pub table_id: String,
    pub key_schema: Vec<KeySchemaElement>,
    /// The base table's key schema (always populated).
    /// For base table operations this equals `key_schema`.
    /// For index operations `key_schema` has the index's schema while this
    /// retains the original table schema, enabling the storage layer to
    /// derive base PK/SK info for pagination without a catalog query.
    pub base_key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    /// Whether the table has at least one local secondary index.
    /// Used to decide whether `ItemCollectionMetrics` should be returned.
    pub has_lsi: bool,
    /// Global secondary indexes defined on the table (name, key schema, and
    /// projection). Populated from the catalog and carried on the cached
    /// `TableKeyInfo` so per-index consumed capacity can be computed without an
    /// extra `describe_table` round-trip per write.
    pub global_secondary_indexes: Vec<IndexInfo>,
    /// Local secondary indexes defined on the table (name, key schema, and
    /// projection). Same rationale as `global_secondary_indexes`.
    pub local_secondary_indexes: Vec<IndexInfo>,
    /// Stream specification for the table, if streams are configured.
    /// Cached here to avoid an extra `describe_table` call per write operation.
    pub stream_specification: Option<super::StreamSpecification>,
    /// Vector indexes on the table, if any. Carried so write operations can
    /// validate vector-valued and search-schema attributes without an extra
    /// catalog round-trip.
    pub vector_indexes: Vec<VectorIndexKeyInfo>,
}

/// Vector-index metadata needed by the write path to validate an item's
/// vector-valued attribute and its search-schema attributes.
#[derive(Debug, Clone)]
pub struct VectorIndexKeyInfo {
    /// Name of the vector index.
    pub index_name: String,
    /// Declared vector dimension.
    pub dimensions: u32,
    /// Item attribute that carries the vector.
    pub vector_attribute_name: String,
    /// Search-schema elements (partition key and inline filters).
    pub search_schema: Vec<super::table::SearchSchemaElement>,
    /// The index's projection. The write-capacity charge depends on it:
    /// measured against the service 2026-08-13, a change to a non-indexed
    /// attribute IS charged under `ALL` and is not under `KEYS_ONLY`, and
    /// under `KEYS_ONLY` the vector blob itself is not counted either.
    pub projection: super::table::Projection,
}

impl TableKeyInfo {
    /// Check the one invariant the keyed read paths depend on: every RANGE
    /// element of the table's own key schema has a matching entry in
    /// `attribute_definitions`.
    ///
    /// The storage backends derive the sort key's physical column from its
    /// attribute definition, so a key schema whose RANGE attribute has no
    /// definition makes them treat the table as partition-key-only and return an
    /// arbitrary item from the partition. That is a silent wrong-answer read, so
    /// the condition is checked where catalog metadata enters the system and
    /// reported as an error instead of being inferred away (issue #259).
    ///
    /// Only `base_key_schema` is checked. Secondary index key schemas are
    /// deliberately not, because a table written by an older build may carry an
    /// index whose key attributes were never persisted, and refusing to load its
    /// metadata would take the whole table offline rather than degrade that one
    /// index.
    ///
    /// # Errors
    ///
    /// Returns the offending attribute name and the definitions that were present
    /// when a RANGE attribute has no definition.
    pub fn validate_sort_key_definitions(&self) -> Result<(), String> {
        for element in &self.base_key_schema {
            if element.key_type != KeyType::Range {
                continue;
            }
            if !self
                .attribute_definitions
                .iter()
                .any(|ad| ad.attribute_name == element.attribute_name)
            {
                return Err(format!(
                    "table {} has sort key '{}' in its key schema with no matching \
                     AttributeDefinition (definitions present: [{}])",
                    self.table_name,
                    element.attribute_name,
                    self.attribute_definitions
                        .iter()
                        .map(|ad| ad.attribute_name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
        }
        Ok(())
    }
}

/// Extract all HASH key elements from a key schema (preserving order).
#[must_use]
pub fn hash_key_elements(key_schema: &[KeySchemaElement]) -> Vec<&KeySchemaElement> {
    key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Hash)
        .collect()
}

/// Extract all RANGE key elements from a key schema (preserving order).
#[must_use]
pub fn range_key_elements(key_schema: &[KeySchemaElement]) -> Vec<&KeySchemaElement> {
    key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Range)
        .collect()
}

/// Returns `true` if the key schema has more than one HASH or more than one RANGE element.
#[must_use]
pub fn is_multipart_key_schema(key_schema: &[KeySchemaElement]) -> bool {
    let hash_count = key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Hash)
        .count();
    let range_count = key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Range)
        .count();
    hash_count > 1 || range_count > 1
}

/// Index type — GSI or LSI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// Global secondary index — different partition key allowed.
    Gsi,
    /// Local secondary index — same partition key as base table.
    Lsi,
    /// Vector index — searched only via the vector search API, not Scan/Query.
    Vector,
}

/// Metadata for a secondary index, used by query/scan operations.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Name of the index.
    pub index_name: String,
    /// Unique identifier for the index (used as PG table name suffix).
    pub index_id: String,
    /// GSI or LSI.
    pub index_type: IndexType,
    /// Key schema of the index (HASH, optional RANGE).
    pub key_schema: Vec<KeySchemaElement>,
    /// Projection configuration.
    pub projection: super::table::Projection,
}

/// Index rows grouped by kind.
///
/// Returned by [`partition_indexes`]. Exists so a backend reading its own index
/// catalog does not match on [`IndexType`] itself: adding a variant would
/// otherwise break every such match, which is how adding the vector variant
/// broke a backend that will never serve one. New variants land here instead.
#[derive(Debug, Default, Clone)]
pub struct PartitionedIndexes {
    /// Global secondary indexes.
    pub gsis: Vec<IndexInfo>,
    /// Local secondary indexes.
    pub lsis: Vec<IndexInfo>,
    /// Vector indexes. A backend that does not declare vector support will never
    /// have created one, so this being non-empty in that case means the catalog
    /// disagrees with the backend's capability.
    pub vectors: Vec<IndexInfo>,
}

/// Group index rows by kind.
///
/// Callers take the groups they serve and ignore the rest, so a backend needs no
/// knowledge of index kinds it does not implement.
pub fn partition_indexes<I>(indexes: I) -> PartitionedIndexes
where
    I: IntoIterator<Item = IndexInfo>,
{
    let mut out = PartitionedIndexes::default();
    for info in indexes {
        match info.index_type {
            IndexType::Gsi => out.gsis.push(info),
            IndexType::Lsi => out.lsis.push(info),
            IndexType::Vector => out.vectors.push(info),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.into(),
            key_type,
        }
    }

    fn key_info_with(
        base_key_schema: Vec<KeySchemaElement>,
        attribute_definitions: Vec<AttributeDefinition>,
    ) -> TableKeyInfo {
        TableKeyInfo {
            table_name: "TestTable".into(),
            account_id: "123".into(),
            table_id: "t1".into(),
            key_schema: base_key_schema.clone(),
            base_key_schema,
            attribute_definitions,
            has_lsi: false,
            global_secondary_indexes: vec![],
            local_secondary_indexes: vec![],
            stream_specification: None,
            vector_indexes: vec![],
        }
    }

    fn ad(name: &str, attr_type: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.into(),
            attribute_type: attr_type,
        }
    }

    /// The state issue #259 left behind: the key schema still names a sort key but
    /// its definition was dropped, which is what made the read path fall back to a
    /// partition-only lookup. Loading such metadata must fail loudly.
    #[test]
    fn validate_sort_key_definitions_rejects_a_range_key_with_no_definition() {
        let info = key_info_with(
            vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)],
            vec![ad("pk", ScalarAttributeType::S)],
        );

        let err = info
            .validate_sort_key_definitions()
            .expect_err("a sort key with no attribute definition must be rejected");
        assert!(
            err.contains("sk"),
            "error should name the sort key, got: {err}"
        );
        assert!(
            err.contains("TestTable"),
            "error should name the table, got: {err}"
        );
    }

    #[test]
    fn validate_sort_key_definitions_accepts_a_complete_composite_key() {
        let info = key_info_with(
            vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)],
            vec![
                ad("pk", ScalarAttributeType::S),
                ad("sk", ScalarAttributeType::S),
            ],
        );

        assert!(info.validate_sort_key_definitions().is_ok());
    }

    /// A hash-only table has no RANGE element, so there is nothing to check. It
    /// must not be rejected for the absence of a sort key definition.
    #[test]
    fn validate_sort_key_definitions_accepts_a_hash_only_table() {
        let info = key_info_with(
            vec![ks("pk", KeyType::Hash)],
            vec![ad("pk", ScalarAttributeType::S)],
        );

        assert!(info.validate_sort_key_definitions().is_ok());
    }

    /// Secondary index key schemas are deliberately out of scope: a table written
    /// by an older build may carry an index whose key attributes were never
    /// persisted, and refusing to load the table would take it entirely offline.
    #[test]
    fn validate_sort_key_definitions_ignores_index_key_schemas() {
        let mut info = key_info_with(
            vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)],
            vec![
                ad("pk", ScalarAttributeType::S),
                ad("sk", ScalarAttributeType::S),
            ],
        );
        info.key_schema = vec![ks("f01", KeyType::Hash), ks("f02", KeyType::Range)];

        assert!(info.validate_sort_key_definitions().is_ok());
    }

    #[test]
    fn hash_key_elements_single() {
        let schema = vec![ks("pk", KeyType::Hash)];
        let hashes = hash_key_elements(&schema);
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].attribute_name, "pk");
    }

    #[test]
    fn hash_key_elements_with_range() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let hashes = hash_key_elements(&schema);
        assert_eq!(hashes.len(), 1);
        assert_eq!(hashes[0].attribute_name, "pk");
    }

    #[test]
    fn range_key_elements_present() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let ranges = range_key_elements(&schema);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].attribute_name, "sk");
    }

    #[test]
    fn range_key_elements_absent() {
        let schema = vec![ks("pk", KeyType::Hash)];
        let ranges = range_key_elements(&schema);
        assert!(ranges.is_empty());
    }

    #[test]
    fn is_multipart_single_hash() {
        let schema = vec![ks("pk", KeyType::Hash)];
        assert!(!is_multipart_key_schema(&schema));
    }

    #[test]
    fn is_multipart_hash_and_range() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        assert!(!is_multipart_key_schema(&schema));
    }

    #[test]
    fn is_multipart_two_hashes() {
        let schema = vec![ks("pk1", KeyType::Hash), ks("pk2", KeyType::Hash)];
        assert!(is_multipart_key_schema(&schema));
    }

    #[test]
    fn is_multipart_two_ranges() {
        let schema = vec![
            ks("pk", KeyType::Hash),
            ks("sk1", KeyType::Range),
            ks("sk2", KeyType::Range),
        ];
        assert!(is_multipart_key_schema(&schema));
    }

    #[test]
    fn key_schema_element_serde_roundtrip() {
        let elem = ks("pk", KeyType::Hash);
        let json = serde_json::to_string(&elem).unwrap();
        let parsed: KeySchemaElement = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, elem);
    }

    #[test]
    fn attribute_definition_serde_roundtrip() {
        let def = AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: ScalarAttributeType::S,
        };
        let json = serde_json::to_string(&def).unwrap();
        let parsed: AttributeDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, def);
    }

    #[test]
    fn scalar_attribute_types_serde() {
        for (typ, expected) in [
            (ScalarAttributeType::S, "\"S\""),
            (ScalarAttributeType::N, "\"N\""),
            (ScalarAttributeType::B, "\"B\""),
        ] {
            let json = serde_json::to_string(&typ).unwrap();
            assert_eq!(json, expected);
        }
    }

    #[test]
    fn table_key_info_base_key_schema_for_base_table() {
        let schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let info = TableKeyInfo {
            table_name: "TestTable".into(),
            account_id: "123".into(),
            table_id: "t1".into(),
            key_schema: schema.clone(),
            base_key_schema: schema.clone(),
            attribute_definitions: vec![],
            has_lsi: false,
            global_secondary_indexes: vec![],
            local_secondary_indexes: vec![],
            stream_specification: None,
            vector_indexes: vec![],
        };
        assert_eq!(info.key_schema, info.base_key_schema);
    }

    #[test]
    fn table_key_info_base_key_schema_for_index_query() {
        let base_schema = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let index_schema = vec![ks("gsi_pk", KeyType::Hash), ks("gsi_sk", KeyType::Range)];
        let info = TableKeyInfo {
            table_name: "TestTable".into(),
            account_id: "123".into(),
            table_id: "t1".into(),
            key_schema: index_schema.clone(),
            base_key_schema: base_schema.clone(),
            attribute_definitions: vec![],
            has_lsi: false,
            global_secondary_indexes: vec![],
            local_secondary_indexes: vec![],
            stream_specification: None,
            vector_indexes: vec![],
        };
        assert_eq!(info.key_schema, index_schema);
        assert_eq!(info.base_key_schema, base_schema);
        assert_ne!(info.key_schema, info.base_key_schema);
        assert_eq!(info.base_key_schema[0].attribute_name, "pk");
    }
}

#[cfg(test)]
mod partition_tests {
    use super::*;
    use crate::types::table::{Projection, ProjectionType};

    fn info(name: &str, index_type: IndexType) -> IndexInfo {
        IndexInfo {
            index_name: name.to_owned(),
            index_id: format!("{name}-id"),
            index_type,
            key_schema: Vec::new(),
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        }
    }

    /// The point of the helper: a caller takes the groups it serves and never
    /// matches on `IndexType`, so a new variant cannot break it.
    #[test]
    fn groups_each_kind_separately() {
        let out = partition_indexes(vec![
            info("g1", IndexType::Gsi),
            info("l1", IndexType::Lsi),
            info("v1", IndexType::Vector),
            info("g2", IndexType::Gsi),
        ]);
        assert_eq!(
            out.gsis
                .iter()
                .map(|i| i.index_name.as_str())
                .collect::<Vec<_>>(),
            ["g1", "g2"]
        );
        assert_eq!(
            out.lsis
                .iter()
                .map(|i| i.index_name.as_str())
                .collect::<Vec<_>>(),
            ["l1"]
        );
        assert_eq!(
            out.vectors
                .iter()
                .map(|i| i.index_name.as_str())
                .collect::<Vec<_>>(),
            ["v1"]
        );
    }

    /// Input order is preserved within a group, so a caller that relied on
    /// catalog ordering keeps it.
    #[test]
    fn preserves_input_order_within_a_group() {
        let out = partition_indexes(vec![
            info("b", IndexType::Gsi),
            info("a", IndexType::Gsi),
            info("c", IndexType::Gsi),
        ]);
        assert_eq!(
            out.gsis
                .iter()
                .map(|i| i.index_name.as_str())
                .collect::<Vec<_>>(),
            ["b", "a", "c"]
        );
    }

    #[test]
    fn empty_input_yields_empty_groups() {
        let out = partition_indexes(Vec::new());
        assert!(out.gsis.is_empty() && out.lsis.is_empty() && out.vectors.is_empty());
    }
}

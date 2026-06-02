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
#[derive(Debug, Clone)]
pub struct TableKeyInfo {
    pub table_name: String,
    pub account_id: String,
    pub table_id: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    /// Key schemas for secondary indexes that writes must validate.
    ///
    /// Write metadata populates ACTIVE indexes and any pending native index
    /// builds that can observe newly written rows. Lightweight read metadata
    /// can leave this empty.
    pub secondary_index_key_schemas: Vec<Vec<KeySchemaElement>>,
    /// Whether the table has at least one local secondary index.
    /// Used to decide whether `ItemCollectionMetrics` should be returned.
    pub has_lsi: bool,
    /// Stream specification for the table, if streams are configured.
    /// Cached here to avoid an extra `describe_table` call per write operation.
    pub stream_specification: Option<super::StreamSpecification>,
}

/// Resolved metadata for a table read path.
///
/// `table` always describes the base DynamoDB table. `index`, when present,
/// describes the secondary index selected by Query or Scan. Keeping both parts
/// together lets storage backends execute reads without re-fetching catalog
/// metadata or reconstructing the base-table key after index routing.
#[derive(Debug, Clone)]
pub struct TableReadInfo {
    pub table: TableKeyInfo,
    pub index: Option<IndexInfo>,
}

/// Extract all HASH key elements from a key schema (preserving order).
pub fn hash_key_elements(key_schema: &[KeySchemaElement]) -> Vec<&KeySchemaElement> {
    key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Hash)
        .collect()
}

/// Extract all RANGE key elements from a key schema (preserving order).
pub fn range_key_elements(key_schema: &[KeySchemaElement]) -> Vec<&KeySchemaElement> {
    key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Range)
        .collect()
}

/// Returns `true` if the key schema has more than one HASH or more than one RANGE element.
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

/// Build the key schema used by Query/Scan `LastEvaluatedKey` and
/// `ExclusiveStartKey`.
///
/// Base table reads use the table key schema. Secondary-index reads include
/// both the base table key and the index key attributes, deduplicated by
/// attribute name, matching DynamoDB's cursor format.
pub fn combined_lek_key_schema(
    base_key_schema: &[KeySchemaElement],
    index_info: Option<&IndexInfo>,
) -> Vec<KeySchemaElement> {
    let Some(index) = index_info else {
        return base_key_schema.to_vec();
    };

    let mut combined = base_key_schema.to_vec();
    for key in &index.key_schema {
        if !combined
            .iter()
            .any(|existing| existing.attribute_name == key.attribute_name)
        {
            combined.push(key.clone());
        }
    }
    combined
}

/// Index type — GSI or LSI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    /// Global secondary index — different partition key allowed.
    Gsi,
    /// Local secondary index — same partition key as base table.
    Lsi,
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

#[cfg(test)]
mod tests {
    use crate::types::{Projection, ProjectionType};

    use super::*;

    fn ks(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.into(),
            key_type,
        }
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
    fn combined_lek_key_schema_uses_base_key_for_table_reads() {
        let base = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];

        assert_eq!(combined_lek_key_schema(&base, None), base);
    }

    #[test]
    fn combined_lek_key_schema_adds_deduplicated_index_keys() {
        let base = vec![ks("pk", KeyType::Hash), ks("sk", KeyType::Range)];
        let index = IndexInfo {
            index_name: "by_gpk".to_owned(),
            index_id: "idx1".to_owned(),
            index_type: IndexType::Gsi,
            key_schema: vec![ks("gpk", KeyType::Hash), ks("sk", KeyType::Range)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        };

        let combined = combined_lek_key_schema(&base, Some(&index))
            .into_iter()
            .map(|key| key.attribute_name)
            .collect::<Vec<_>>();

        assert_eq!(combined, vec!["pk", "sk", "gpk"]);
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
}

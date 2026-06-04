// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Types for `ReturnConsumedCapacity`, `ConsumedCapacity`, and
//! `ReturnItemCollectionMetrics` / `ItemCollectionMetrics`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Controls whether consumed capacity information is returned in the response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReturnConsumedCapacity {
    /// No capacity information.
    #[default]
    None,
    /// Only aggregate table-level capacity.
    Total,
    /// Table-level plus per-index breakdown.
    Indexes,
}

impl<'de> Deserialize<'de> for ReturnConsumedCapacity {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "NONE" => Ok(Self::None),
            "TOTAL" => Ok(Self::Total),
            "INDEXES" => Ok(Self::Indexes),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'returnConsumedCapacity' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [INDEXES, TOTAL, NONE]"
            ))),
        }
    }
}

/// Capacity consumed by a single table (or index within a table).
#[derive(Debug, Clone, Serialize)]
pub struct Capacity {
    /// Approximate capacity units consumed.
    #[serde(rename = "CapacityUnits")]
    pub capacity_units: f64,
    /// Read capacity units consumed (present for read operations).
    #[serde(rename = "ReadCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub read_capacity_units: Option<f64>,
    /// Write capacity units consumed (present for write operations).
    #[serde(rename = "WriteCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub write_capacity_units: Option<f64>,
}

/// Consumed capacity information returned when requested.
#[derive(Debug, Clone, Serialize)]
pub struct ConsumedCapacity {
    /// Table name.
    #[serde(rename = "TableName")]
    pub table_name: String,
    /// Total capacity units consumed.
    #[serde(rename = "CapacityUnits")]
    pub capacity_units: f64,
    /// Read capacity units consumed (present for read operations).
    #[serde(rename = "ReadCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub read_capacity_units: Option<f64>,
    /// Write capacity units consumed (present for write operations).
    #[serde(rename = "WriteCapacityUnits", skip_serializing_if = "Option::is_none")]
    pub write_capacity_units: Option<f64>,
    /// Capacity consumed by the base table (present when `INDEXES` is requested).
    #[serde(rename = "Table", skip_serializing_if = "Option::is_none")]
    pub table: Option<Capacity>,
    /// Per-index capacity breakdown (present when `INDEXES` is requested).
    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_secondary_indexes: Option<HashMap<String, Capacity>>,
    /// Per-LSI capacity breakdown (present when `INDEXES` is requested).
    #[serde(
        rename = "LocalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub local_secondary_indexes: Option<HashMap<String, Capacity>>,
}

/// Controls whether the existing item is returned in the error response when a
/// condition check fails.
///
/// Applies to `PutItem`, `DeleteItem`, `UpdateItem`, and the four transaction
/// write sub-operations (`TransactPut`, `TransactDelete`, `TransactUpdate`,
/// `TransactConditionCheck`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReturnValuesOnConditionCheckFailure {
    /// Do not return the item (default).
    #[default]
    None,
    /// Return all attributes of the existing item.
    AllOld,
}

impl<'de> Deserialize<'de> for ReturnValuesOnConditionCheckFailure {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "NONE" => Ok(Self::None),
            "ALL_OLD" => Ok(Self::AllOld),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at \
                 'returnValuesOnConditionCheckFailure' failed to satisfy constraint: \
                 Member must satisfy enum value set: [ALL_OLD, NONE]"
            ))),
        }
    }
}

/// Controls whether item collection metrics are returned for write operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReturnItemCollectionMetrics {
    /// No metrics.
    #[default]
    None,
    /// Return size estimate for affected item collections.
    Size,
}

impl<'de> Deserialize<'de> for ReturnItemCollectionMetrics {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "NONE" => Ok(Self::None),
            "SIZE" => Ok(Self::Size),
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'returnItemCollectionMetrics' \
                 failed to satisfy constraint: Member must satisfy enum value set: \
                 [SIZE, NONE]"
            ))),
        }
    }
}

/// Metrics about an item collection (items sharing the same partition key)
/// affected by a write operation.
#[derive(Debug, Clone, Serialize)]
pub struct ItemCollectionMetrics {
    /// The partition key value of the affected item collection.
    #[serde(rename = "ItemCollectionKey")]
    pub item_collection_key: HashMap<String, super::AttributeValue>,
    /// Estimated size range of the item collection in GB.
    #[serde(rename = "SizeEstimateRangeGB")]
    pub size_estimate_range_gb: [f64; 2],
}

impl ConsumedCapacity {
    /// Build a `ConsumedCapacity` for a read operation with real capacity units.
    #[must_use]
    pub fn read(table_name: &str, cu: f64, indexes: bool) -> Self {
        Self {
            table_name: table_name.to_owned(),
            capacity_units: cu,
            read_capacity_units: Some(cu),
            write_capacity_units: None,
            table: if indexes {
                Some(Capacity {
                    capacity_units: cu,
                    read_capacity_units: Some(cu),
                    write_capacity_units: None,
                })
            } else {
                None
            },
            global_secondary_indexes: None,
            local_secondary_indexes: None,
        }
    }

    /// Build a `ConsumedCapacity` for a write operation with real capacity units.
    #[must_use]
    pub fn write(table_name: &str, cu: f64, indexes: bool) -> Self {
        Self {
            table_name: table_name.to_owned(),
            capacity_units: cu,
            read_capacity_units: None,
            write_capacity_units: Some(cu),
            table: if indexes {
                Some(Capacity {
                    capacity_units: cu,
                    read_capacity_units: None,
                    write_capacity_units: Some(cu),
                })
            } else {
                None
            },
            global_secondary_indexes: None,
            local_secondary_indexes: None,
        }
    }
}

impl ItemCollectionMetrics {
    /// Build a stub `ItemCollectionMetrics` with a synthetic size range.
    #[must_use]
    pub fn stub(pk_name: &str, pk_value: &super::AttributeValue) -> Self {
        Self {
            item_collection_key: HashMap::from([(pk_name.to_owned(), pk_value.clone())]),
            size_estimate_range_gb: [0.0, 1.0],
        }
    }
}

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

/// Capacity consumed by a vector index.
///
/// Vector indexes meter in their own units, separate from table read and write
/// capacity. A `SearchVectors` charge is reported twice, as
/// `VectorSearchRequestBytes` and `VectorSearchUnits` with the same value; a
/// write replicated into the index is reported as `VectorWriteRequestBytes`.
/// Each member is omitted rather than reported as zero when the operation does
/// not consume it.
///
/// The duplicated search member is measured, not a guess: probe P8 against real
/// Amazon DynamoDB on 2026-08-19 captured both members on every search, always
/// equal, under both `INDEXES` and `TOTAL`. The write side has no such capture,
/// so it carries the bytes member alone until one exists.
#[derive(Debug, Clone, Default, Serialize)]
pub struct VectorCapacity {
    /// Bytes consumed by a `SearchVectors` operation.
    #[serde(
        rename = "VectorSearchRequestBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub vector_search_request_bytes: Option<f64>,
    /// The same figure as `VectorSearchRequestBytes`, under the name a client
    /// reading units expects.
    #[serde(rename = "VectorSearchUnits", skip_serializing_if = "Option::is_none")]
    pub vector_search_units: Option<f64>,
    /// Bytes consumed replicating a write into the index.
    #[serde(
        rename = "VectorWriteRequestBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub vector_write_request_bytes: Option<f64>,
}

impl VectorCapacity {
    /// A search charge, which the service reports under both search member names
    /// with the same value.
    ///
    /// A constructor rather than a struct literal at the call site, so the two
    /// members cannot drift apart: there is no way to set one and forget the
    /// other.
    #[must_use]
    pub const fn search(bytes: f64) -> Self {
        Self {
            vector_search_request_bytes: Some(bytes),
            vector_search_units: Some(bytes),
            vector_write_request_bytes: None,
        }
    }

    /// A write-replication charge.
    #[must_use]
    pub const fn write(bytes: f64) -> Self {
        Self {
            vector_search_request_bytes: None,
            vector_search_units: None,
            vector_write_request_bytes: Some(bytes),
        }
    }
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
    /// Per-vector-index capacity breakdown, keyed by index name.
    ///
    /// Measured against the service 2026-08-13: reported only for `INDEXES`,
    /// not for `TOTAL` (which returns `TableName` and `CapacityUnits` alone,
    /// without even the `Table` breakdown), and absent entirely rather than
    /// empty when the operation charged no vector capacity.
    #[serde(rename = "VectorIndexes", skip_serializing_if = "Option::is_none")]
    pub vector_indexes: Option<HashMap<String, VectorCapacity>>,
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
    /// Attach a vector-index write charge, keyed by index name.
    ///
    /// A charge of `None` for an index means the operation did not touch that
    /// index's projected entry and so consumed nothing; such indexes are left
    /// out of the map entirely, and when no index is charged the whole
    /// `VectorIndexes` field is omitted. That matches the service, which omits
    /// rather than zero-fills (measured 2026-08-13).
    ///
    /// Only applied at `INDEXES` granularity: `TOTAL` does not carry the map.
    #[must_use]
    pub fn with_vector_writes(
        mut self,
        charges: impl IntoIterator<Item = (String, f64)>,
        indexes: bool,
    ) -> Self {
        if !indexes {
            return self;
        }
        let map: HashMap<String, VectorCapacity> = charges
            .into_iter()
            .map(|(name, bytes)| (name, VectorCapacity::write(bytes)))
            .collect();
        if !map.is_empty() {
            self.vector_indexes = Some(map);
        }
        self
    }

    /// Build a `ConsumedCapacity` for a read served by a secondary index.
    ///
    /// The index carries the read and the table's share is zero, with the
    /// aggregate being their sum; that is what makes the INDEXES breakdown a
    /// breakdown rather than a copy of the total (pinned by the ground-truth
    /// runs of 2026-08-24, us-east-1 and eu-west-2). `is_global` selects the
    /// map the index entry lands in.
    pub fn read_on_index(table_name: &str, index_name: &str, cu: f64, is_global: bool) -> Self {
        let index_arm = std::iter::once((
            index_name.to_owned(),
            Capacity {
                capacity_units: cu,
                read_capacity_units: None,
                write_capacity_units: None,
            },
        ))
        .collect();
        let (gsi, lsi) = if is_global {
            (Some(index_arm), None)
        } else {
            (None, Some(index_arm))
        };
        Self {
            table_name: table_name.to_owned(),
            capacity_units: cu,
            read_capacity_units: None,
            write_capacity_units: None,
            table: Some(Capacity {
                capacity_units: 0.0,
                read_capacity_units: None,
                write_capacity_units: None,
            }),
            global_secondary_indexes: gsi,
            local_secondary_indexes: lsi,
            vector_indexes: None,
        }
    }

    /// Build a `ConsumedCapacity` for a read operation with real capacity units.
    #[must_use]
    pub fn read(table_name: &str, cu: f64, indexes: bool) -> Self {
        Self {
            table_name: table_name.to_owned(),
            capacity_units: cu,
            read_capacity_units: None,
            write_capacity_units: None,
            table: if indexes {
                Some(Capacity {
                    capacity_units: cu,
                    read_capacity_units: None,
                    write_capacity_units: None,
                })
            } else {
                None
            },
            global_secondary_indexes: None,
            local_secondary_indexes: None,
            vector_indexes: None,
        }
    }

    /// Build a `ConsumedCapacity` for a write operation with real capacity units.
    #[must_use]
    pub fn write(table_name: &str, cu: f64, indexes: bool) -> Self {
        Self {
            table_name: table_name.to_owned(),
            capacity_units: cu,
            read_capacity_units: None,
            write_capacity_units: None,
            table: if indexes {
                Some(Capacity {
                    capacity_units: cu,
                    read_capacity_units: None,
                    write_capacity_units: None,
                })
            } else {
                None
            },
            global_secondary_indexes: None,
            local_secondary_indexes: None,
            vector_indexes: None,
        }
    }

    /// Build a `ConsumedCapacity` for a transaction read (`TransactGetItems`).
    ///
    /// Unlike single-item and batch reads, real DynamoDB includes the granular
    /// `ReadCapacityUnits` sub-field for transactions — both at the top level
    /// and inside the nested `Table` breakdown at INDEXES granularity.
    #[must_use]
    pub fn transact_read(table_name: &str, cu: f64, indexes: bool) -> Self {
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
            vector_indexes: None,
        }
    }

    /// Build the `ConsumedCapacity` for an idempotent-replay response: the
    /// replay re-reads the stored result transactionally, so it reports READ
    /// capacity recomputed against the item size, never the stored write
    /// magnitude. Measured against the service (a ~1.5KB put: first call 4
    /// WCU, same-token replay 2 RCU with no write arm).
    ///
    /// The magnitude is exact for Put operations, whose request carries the
    /// full item. For Delete/Update/ConditionCheck the caller only holds the
    /// key size, so the figure is a floor (the 1-RCU minimum in practice);
    /// recomputing against the stored item would need a read the replay path
    /// deliberately does not perform.
    #[must_use]
    pub fn transact_replay_read(table_name: &str, cu: f64, indexes: bool) -> Self {
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
            vector_indexes: None,
        }
    }

    /// Build a `ConsumedCapacity` for a transaction write (`TransactWriteItems`).
    ///
    /// Unlike single-item and batch writes, real DynamoDB includes the granular
    /// `WriteCapacityUnits` sub-field for transactions — both at the top level
    /// and inside the nested `Table` breakdown at INDEXES granularity.
    #[must_use]
    pub fn transact_write(table_name: &str, cu: f64, indexes: bool) -> Self {
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
            vector_indexes: None,
        }
    }

    /// Build a write `ConsumedCapacity` whose aggregate total includes the base
    /// table plus every affected secondary index.
    ///
    /// `base_cu` is the base-table write capacity. `gsi`/`lsi` map each affected
    /// index name to its write capacity. DynamoDB reports the aggregate total as
    /// `base + Σ(GSI) + Σ(LSI)`, and — in `INDEXES` mode — the per-table `Table`
    /// capacity plus the two per-index maps.
    ///
    /// When `breakdown` is true (`INDEXES` mode) the `Table`,
    /// `GlobalSecondaryIndexes` and `LocalSecondaryIndexes` fields are populated;
    /// when false (`TOTAL` mode) only the aggregate is returned, but that
    /// aggregate still reflects the index writes.
    #[must_use]
    pub fn write_indexed(
        table_name: &str,
        base_cu: f64,
        gsi: HashMap<String, f64>,
        lsi: HashMap<String, f64>,
        breakdown: bool,
    ) -> Self {
        let total = base_cu + gsi.values().sum::<f64>() + lsi.values().sum::<f64>();
        Self {
            table_name: table_name.to_owned(),
            capacity_units: total,
            read_capacity_units: None,
            write_capacity_units: None,
            table: breakdown.then(|| Capacity::units(base_cu)),
            global_secondary_indexes: if breakdown { map_or_none(gsi) } else { None },
            local_secondary_indexes: if breakdown { map_or_none(lsi) } else { None },
            vector_indexes: None,
        }
    }
}

impl Capacity {
    /// Capacity for a single-item write, which reports only `CapacityUnits`.
    #[must_use]
    fn units(cu: f64) -> Self {
        Self {
            capacity_units: cu,
            read_capacity_units: None,
            write_capacity_units: None,
        }
    }
}

/// Convert a per-index units map into `Some(map of Capacity)`, or `None` when
/// empty so the field is omitted from the response.
fn map_or_none(units: HashMap<String, f64>) -> Option<HashMap<String, Capacity>> {
    if units.is_empty() {
        None
    } else {
        Some(
            units
                .into_iter()
                .map(|(name, cu)| (name, Capacity::units(cu)))
                .collect(),
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexed_write_omits_granular_write_capacity_units() {
        let capacity = ConsumedCapacity::write_indexed(
            "table",
            1.0,
            HashMap::from([("gsi".to_owned(), 1.0)]),
            HashMap::from([("lsi".to_owned(), 1.0)]),
            true,
        );
        let Ok(value) = serde_json::to_value(capacity) else {
            panic!("indexed capacity should serialize");
        };

        assert!(value.get("WriteCapacityUnits").is_none());
        assert!(value["Table"].get("WriteCapacityUnits").is_none());
        assert!(
            value["GlobalSecondaryIndexes"]["gsi"]
                .get("WriteCapacityUnits")
                .is_none()
        );
        assert!(
            value["LocalSecondaryIndexes"]["lsi"]
                .get("WriteCapacityUnits")
                .is_none()
        );
    }

    /// A search charge serialises both measured members and nothing else.
    ///
    /// Probe P8 (2026-08-19, real Amazon DynamoDB) captured the whole
    /// `SearchVectors` shape as
    /// `{"VectorSearchRequestBytes": 1024.0, "VectorSearchUnits": 1024.0}` under
    /// both `INDEXES` and `TOTAL`. A client reading the units member got `null`
    /// from ExtendDB and a number from the service.
    #[test]
    fn a_search_charge_carries_both_measured_members() {
        let capacity = VectorCapacity::search(2048.0);
        let Ok(value) = serde_json::to_value(capacity) else {
            panic!("vector capacity should serialize");
        };
        let members: Vec<&str> = value
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(members, ["VectorSearchRequestBytes", "VectorSearchUnits"]);
        assert_eq!(value["VectorSearchRequestBytes"], 2048.0);
        assert_eq!(value["VectorSearchUnits"], 2048.0);
    }

    /// The write charge is untouched by the search-side addition. The service's
    /// write-side units member is NOT measured, so nothing may be invented for
    /// it: a write charge still carries exactly one member.
    #[test]
    fn a_write_charge_carries_only_the_measured_bytes_member() {
        let capacity = ConsumedCapacity::write("table", 1.0, true)
            .with_vector_writes([("vidx".to_owned(), 512.0)], true);
        let Ok(value) = serde_json::to_value(capacity) else {
            panic!("indexed capacity should serialize");
        };
        let members: Vec<&str> = value["VectorIndexes"]["vidx"]
            .as_object()
            .expect("object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(members, ["VectorWriteRequestBytes"]);
    }
}

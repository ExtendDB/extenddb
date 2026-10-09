// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The table definition a backup records, shared by the SQL backends.
//!
//! RestoreTableFromBackup reproduces what the service reproduces: key schema,
//! attribute definitions, global and local secondary indexes, billing mode and
//! provisioned throughput, table class, and encryption settings. Streams, TTL,
//! tags, deletion protection, and point-in-time recovery settings are not
//! restored, matching the service. The key schema and attribute definitions
//! keep their own catalog columns; everything else lives here.
//!
//! Stored in the wire's own shape behind a version marker, not as a copy of
//! catalog rows. A backup outlives the schema that produced it, so freezing
//! physical column names into it would let a later catalog change silently
//! alter the meaning of backups already on disk. A reader that finds a version
//! it does not know refuses the restore rather than guessing.

use extenddb_core::types::{
    BillingMode, CreateTableInput, GsiInput, KeySchemaElement, KeyType, LsiInput,
    OnDemandThroughput, ProvisionedThroughput,
};
use serde::{Deserialize, Serialize};

use crate::error::StorageError;

/// Items a backup or a restore buffers before writing them out, at most.
pub const COPY_BATCH_ITEMS: usize = 500;

/// Bytes of stored item JSON a backup or a restore buffers before writing
/// them out, at most (one item over the budget is still taken whole). Counted
/// on the stored text, not the DynamoDB item size, because the text is what
/// is held: a 400 KB item can be several MB of JSON (a list of booleans is
/// about seven times its DynamoDB size).
pub const COPY_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Capacity overrides for one `RestoreTableFromBackup` request.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RestoreTableOverrides {
    /// Replacement billing mode, if the request supplied one.
    pub billing_mode: Option<BillingMode>,
    /// Replacement table throughput, if the request supplied one.
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

impl RestoreTableOverrides {
    /// Apply overrides to a legacy backup's populated `CreateTableInput`.
    ///
    /// Legacy backups do not contain a [`BackupTableDefinition`], so backends
    /// first reconstruct the source defaults and then call this method.
    pub fn apply_to_create_input(&self, input: &mut CreateTableInput) {
        if let Some(billing_mode) = self.billing_mode {
            input.billing_mode = Some(billing_mode);
        }
        if let Some(throughput) = &self.provisioned_throughput {
            input.provisioned_throughput = Some(throughput.clone());
        }

        // Apply overrides before normalizing throughput for on-demand billing.
        if matches!(
            input.billing_mode.as_ref(),
            Some(BillingMode::PayPerRequest)
        ) {
            input.provisioned_throughput = None;
            for index in input.global_secondary_indexes.iter_mut().flatten() {
                index.provisioned_throughput = None;
            }
        }
    }
}

/// Refuse to restore a table whose base key has more than one HASH or more
/// than one RANGE attribute.
///
/// Multi-part base keys are an opt-in preview (`enable_multipart_keys`), and
/// the item read and write paths of the SQL backends address only the first
/// HASH and first RANGE attribute of a base table. A restore that laid such a
/// table out by its full key would produce rows those paths cannot find; one
/// that followed the read paths would collapse distinct items. Refusing is the
/// only result that is not silently wrong.
///
/// # Errors
///
/// [`StorageError::Unsupported`] for a multi-part base key.
pub fn ensure_single_part_base_key(
    key_schema: &[KeySchemaElement],
    backup_arn: &str,
) -> Result<(), StorageError> {
    let hashes = key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Hash)
        .count();
    let ranges = key_schema
        .iter()
        .filter(|k| k.key_type == KeyType::Range)
        .count();
    if hashes <= 1 && ranges <= 1 {
        return Ok(());
    }
    Err(StorageError::Unsupported(format!(
        "backup {backup_arn} is of a table with a multi-part key ({hashes} HASH, {ranges} \
         RANGE attributes); restoring it is not supported"
    )))
}

/// The only definition version this build writes and reads.
pub const BACKUP_DEFINITION_VERSION: u32 = 1;

/// The parts of a table's definition a restore recreates, beyond its keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BackupTableDefinition {
    #[serde(rename = "Version")]
    pub version: u32,
    /// `PROVISIONED` or `PAY_PER_REQUEST`.
    #[serde(rename = "BillingMode")]
    pub billing_mode: String,
    #[serde(
        rename = "ProvisionedThroughput",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    #[serde(rename = "GlobalSecondaryIndexes", default)]
    pub global_secondary_indexes: Vec<GsiInput>,
    #[serde(rename = "LocalSecondaryIndexes", default)]
    pub local_secondary_indexes: Vec<LsiInput>,
    #[serde(
        rename = "TableClass",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub table_class: Option<String>,
    #[serde(
        rename = "SSESpecification",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub sse_specification: Option<serde_json::Value>,
    #[serde(
        rename = "OnDemandThroughput",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub on_demand_throughput: Option<OnDemandThroughput>,
    /// Names of the source table's vector indexes. Restore refuses a backup
    /// that has any, rather than producing a table without them.
    #[serde(rename = "VectorIndexNames", default)]
    pub vector_index_names: Vec<String>,
}

impl BackupTableDefinition {
    /// Parse a stored definition, refusing a version this build cannot read.
    ///
    /// # Errors
    ///
    /// [`StorageError::Unsupported`] for an unknown version, and
    /// [`StorageError::Internal`] for a document that does not parse.
    pub fn from_json(value: serde_json::Value, backup_arn: &str) -> Result<Self, StorageError> {
        let version = value.get("Version").and_then(serde_json::Value::as_u64);
        if version != Some(u64::from(BACKUP_DEFINITION_VERSION)) {
            return Err(StorageError::Unsupported(format!(
                "backup {backup_arn} carries a table definition this build cannot read \
                 (version {version:?})"
            )));
        }
        serde_json::from_value(value).map_err(|e| {
            StorageError::Internal(format!("backup {backup_arn} table definition: {e}"))
        })
    }

    /// Serialize for storage.
    ///
    /// # Errors
    ///
    /// [`StorageError::Internal`] if serialization fails.
    pub fn to_json(&self) -> Result<serde_json::Value, StorageError> {
        let mut definition = self.clone();
        definition.normalize_provisioned_throughput();
        serde_json::to_value(definition).map_err(|e| StorageError::Internal(e.to_string()))
    }

    /// Remove catalog throughput left behind by a switch to on-demand billing.
    fn normalize_provisioned_throughput(&mut self) {
        if self.billing_mode != "PAY_PER_REQUEST" {
            return;
        }
        self.provisioned_throughput = None;
        for index in &mut self.global_secondary_indexes {
            index.provisioned_throughput = None;
        }
    }

    /// Refuse a definition the restore cannot reproduce in full.
    ///
    /// # Errors
    ///
    /// [`StorageError::Unsupported`] if the source table had vector indexes.
    pub fn ensure_restorable(&self, backup_arn: &str) -> Result<(), StorageError> {
        if self.vector_index_names.is_empty() {
            return Ok(());
        }
        Err(StorageError::Unsupported(format!(
            "backup {backup_arn} has {} vector index(es); restoring a table with vector \
             indexes is not supported by this storage backend",
            self.vector_index_names.len()
        )))
    }

    /// Apply the definition and explicit request overrides to a restore target's
    /// `CreateTableInput`.
    ///
    /// # Errors
    ///
    /// [`StorageError::Validation`] when switching a backup with a GSI that has
    /// no provisioned throughput to provisioned billing. The request would need
    /// `GlobalSecondaryIndexOverride`, which ExtendDB does not support.
    pub fn apply_to(
        mut self,
        input: &mut CreateTableInput,
        overrides: &RestoreTableOverrides,
    ) -> Result<(), StorageError> {
        if let Some(billing_mode) = overrides.billing_mode {
            self.billing_mode = match billing_mode {
                BillingMode::Provisioned => "PROVISIONED".to_owned(),
                BillingMode::PayPerRequest => "PAY_PER_REQUEST".to_owned(),
            };
        }
        if let Some(throughput) = &overrides.provisioned_throughput {
            self.provisioned_throughput = Some(throughput.clone());
        }

        if self.billing_mode == "PROVISIONED"
            && (overrides.billing_mode.is_some() || overrides.provisioned_throughput.is_some())
            && let Some(index) = self.global_secondary_indexes.iter().find(|index| {
                index
                    .provisioned_throughput
                    .as_ref()
                    .is_none_or(|throughput| {
                        throughput.read_capacity_units < 1 || throughput.write_capacity_units < 1
                    })
            })
        {
            return Err(StorageError::Validation(format!(
                "One or more parameter values were invalid: GlobalSecondaryIndexOverride must \
                 be specified for index: {} when BillingModeOverride is PROVISIONED",
                index.index_name
            )));
        }

        // Apply overrides before normalizing throughput for on-demand billing.
        self.normalize_provisioned_throughput();
        let on_demand = self.billing_mode == "PAY_PER_REQUEST";
        input.billing_mode = Some(if on_demand {
            BillingMode::PayPerRequest
        } else {
            BillingMode::Provisioned
        });
        input.provisioned_throughput = self.provisioned_throughput;
        input.global_secondary_indexes =
            (!self.global_secondary_indexes.is_empty()).then_some(self.global_secondary_indexes);
        input.local_secondary_indexes =
            (!self.local_secondary_indexes.is_empty()).then_some(self.local_secondary_indexes);
        input.table_class = self.table_class;
        input.sse_specification = self.sse_specification;
        input.on_demand_throughput = self.on_demand_throughput;
        Ok(())
    }
}

/// Throughput stored in a catalog `provisioned_throughput` column, which holds
/// either the request shape or the description shape depending on the writer.
/// Both carry the two capacity members under the same names.
#[must_use]
pub fn throughput_from_catalog(value: Option<&serde_json::Value>) -> Option<ProvisionedThroughput> {
    let v = value?;
    let read = v.get("ReadCapacityUnits")?.as_i64()?;
    let write = v.get("WriteCapacityUnits")?.as_i64()?;
    Some(ProvisionedThroughput {
        read_capacity_units: read,
        write_capacity_units: write,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::{KeySchemaElement, KeyType, Projection, ProjectionType};

    fn sample() -> BackupTableDefinition {
        BackupTableDefinition {
            version: BACKUP_DEFINITION_VERSION,
            billing_mode: "PROVISIONED".to_owned(),
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 7,
                write_capacity_units: 9,
            }),
            global_secondary_indexes: vec![GsiInput {
                index_name: "g".to_owned(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "gpk".to_owned(),
                    key_type: KeyType::Hash,
                }],
                projection: Projection {
                    projection_type: ProjectionType::KeysOnly,
                    non_key_attributes: None,
                },
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 3,
                    write_capacity_units: 4,
                }),
            }],
            local_secondary_indexes: Vec::new(),
            table_class: Some("STANDARD_INFREQUENT_ACCESS".to_owned()),
            sse_specification: None,
            on_demand_throughput: None,
            vector_index_names: Vec::new(),
        }
    }

    #[test]
    fn refuses_multi_part_base_keys() {
        let k = |n: &str, t| KeySchemaElement {
            attribute_name: n.to_owned(),
            key_type: t,
        };
        assert!(ensure_single_part_base_key(&[k("a", KeyType::Hash)], "arn").is_ok());
        assert!(
            ensure_single_part_base_key(&[k("a", KeyType::Hash), k("b", KeyType::Range)], "arn")
                .is_ok()
        );
        for ks in [
            vec![k("a", KeyType::Hash), k("b", KeyType::Hash)],
            vec![
                k("a", KeyType::Hash),
                k("b", KeyType::Range),
                k("c", KeyType::Range),
            ],
        ] {
            assert!(matches!(
                ensure_single_part_base_key(&ks, "arn"),
                Err(StorageError::Unsupported(_))
            ));
        }
    }

    #[test]
    fn round_trips_through_json() {
        let def = sample();
        let back = BackupTableDefinition::from_json(def.to_json().unwrap(), "arn").unwrap();
        assert_eq!(back, def);
    }

    #[test]
    fn refuses_an_unknown_version() {
        let mut json = sample().to_json().unwrap();
        json["Version"] = serde_json::json!(2);
        let err = BackupTableDefinition::from_json(json, "arn").unwrap_err();
        assert!(matches!(err, StorageError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn refuses_vector_indexes() {
        let mut def = sample();
        def.vector_index_names.push("v".to_owned());
        assert!(matches!(
            def.ensure_restorable("arn"),
            Err(StorageError::Unsupported(_))
        ));
    }

    #[test]
    fn applies_to_a_create_input() {
        let mut input = CreateTableInput::default();
        sample()
            .apply_to(&mut input, &RestoreTableOverrides::default())
            .unwrap();
        assert_eq!(input.billing_mode, Some(BillingMode::Provisioned));
        assert_eq!(
            input
                .provisioned_throughput
                .as_ref()
                .map(|p| p.read_capacity_units),
            Some(7)
        );
        assert_eq!(
            input.global_secondary_indexes.as_ref().map(Vec::len),
            Some(1)
        );
        assert!(input.local_secondary_indexes.is_none());
        assert_eq!(
            input.table_class.as_deref(),
            Some("STANDARD_INFREQUENT_ACCESS")
        );
    }

    #[test]
    fn provisioned_table_round_trip_keeps_throughput() {
        let definition = sample();
        let captured = definition.to_json().unwrap();
        let restored = BackupTableDefinition::from_json(captured, "arn").unwrap();
        let mut input = CreateTableInput::default();
        restored
            .apply_to(&mut input, &RestoreTableOverrides::default())
            .unwrap();

        assert_eq!(input.billing_mode, Some(BillingMode::Provisioned));
        assert_eq!(
            input.provisioned_throughput.map(|throughput| (
                throughput.read_capacity_units,
                throughput.write_capacity_units
            )),
            Some((7, 9))
        );
        assert_eq!(
            input.global_secondary_indexes.unwrap()[0]
                .provisioned_throughput
                .as_ref()
                .map(|throughput| (
                    throughput.read_capacity_units,
                    throughput.write_capacity_units
                )),
            Some((3, 4))
        );
    }

    #[test]
    fn pay_per_request_table_with_stale_throughput_is_normalized() {
        let mut definition = sample();
        definition.billing_mode = "PAY_PER_REQUEST".to_owned();
        definition.global_secondary_indexes.clear();

        let captured = definition.to_json().unwrap();
        assert!(captured.get("ProvisionedThroughput").is_none());

        let mut input = CreateTableInput::default();
        definition
            .apply_to(&mut input, &RestoreTableOverrides::default())
            .unwrap();
        assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
        assert!(input.provisioned_throughput.is_none());
    }

    #[test]
    fn pay_per_request_gsi_with_stale_throughput_is_normalized() {
        let mut definition = sample();
        definition.billing_mode = "PAY_PER_REQUEST".to_owned();
        definition.provisioned_throughput = None;

        let captured = definition.to_json().unwrap();
        assert!(
            captured["GlobalSecondaryIndexes"][0]
                .get("ProvisionedThroughput")
                .is_none()
        );

        let mut input = CreateTableInput::default();
        definition
            .apply_to(&mut input, &RestoreTableOverrides::default())
            .unwrap();
        assert!(
            input.global_secondary_indexes.unwrap()[0]
                .provisioned_throughput
                .is_none()
        );
    }

    #[test]
    fn reads_both_catalog_throughput_shapes() {
        let input = serde_json::json!({"ReadCapacityUnits": 5, "WriteCapacityUnits": 6});
        let desc = serde_json::json!({
            "ReadCapacityUnits": 5, "WriteCapacityUnits": 6, "NumberOfDecreasesToday": 0
        });
        for v in [input, desc] {
            let pt = throughput_from_catalog(Some(&v)).unwrap();
            assert_eq!((pt.read_capacity_units, pt.write_capacity_units), (5, 6));
        }
        assert!(throughput_from_catalog(None).is_none());
    }

    #[test]
    fn pay_per_request_override_drops_table_and_gsi_throughput() {
        let overrides = RestoreTableOverrides {
            billing_mode: Some(BillingMode::PayPerRequest),
            provisioned_throughput: None,
        };
        let mut input = CreateTableInput::default();
        sample().apply_to(&mut input, &overrides).unwrap();

        assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
        assert!(input.provisioned_throughput.is_none());
        assert!(
            input.global_secondary_indexes.unwrap()[0]
                .provisioned_throughput
                .is_none()
        );
    }

    #[test]
    fn provisioned_override_with_gsi_without_throughput_is_refused_before_apply() {
        let mut definition = sample();
        definition.billing_mode = "PAY_PER_REQUEST".to_owned();
        definition.provisioned_throughput = None;
        definition.global_secondary_indexes[0].provisioned_throughput = None;
        let overrides = RestoreTableOverrides {
            billing_mode: Some(BillingMode::Provisioned),
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5,
            }),
        };
        let mut input = CreateTableInput::default();

        let err = definition.apply_to(&mut input, &overrides).unwrap_err();

        match err {
            StorageError::Validation(message) => assert_eq!(
                message,
                "One or more parameter values were invalid: GlobalSecondaryIndexOverride must \
                 be specified for index: g when BillingModeOverride is PROVISIONED"
            ),
            other => panic!("expected Validation error, got {other:?}"),
        }
        assert_eq!(input, CreateTableInput::default());
    }

    #[test]
    fn provisioned_override_replaces_table_throughput() {
        let mut definition = sample();
        definition.billing_mode = "PAY_PER_REQUEST".to_owned();
        definition.provisioned_throughput = None;
        definition.global_secondary_indexes.clear();

        let overrides = RestoreTableOverrides {
            billing_mode: Some(BillingMode::Provisioned),
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5,
            }),
        };
        let mut input = CreateTableInput::default();
        definition.apply_to(&mut input, &overrides).unwrap();

        assert_eq!(input.billing_mode, Some(BillingMode::Provisioned));
        assert_eq!(
            input.provisioned_throughput.map(|throughput| (
                throughput.read_capacity_units,
                throughput.write_capacity_units
            )),
            Some((5, 5))
        );
    }
}

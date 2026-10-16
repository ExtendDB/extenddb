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
//! The stream specification and TTL description are recorded too, but only so
//! DescribeBackup can report them in `SourceTableFeatureDetails`; a restore
//! ignores them.
//!
//! The engine, not the backend, turns a backup into the restore target's
//! `CreateTableInput` ([`BackupRestoreSource::restore_input`]) and runs the
//! refusals, so every backend restores the same table from the same backup.
//!
//! Stored in the wire's own shape behind a version marker, not as a copy of
//! catalog rows. A backup outlives the schema that produced it, so freezing
//! physical column names into it would let a later catalog change silently
//! alter the meaning of backups already on disk. A reader that finds a version
//! it does not know refuses the restore rather than guessing.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, GsiInput, KeySchemaElement, KeyType,
    LsiInput, OnDemandThroughput, ProvisionedThroughput, StreamSpecification,
    TimeToLiveDescription,
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

/// The overrides of one `RestoreTableFromBackup` request that change the
/// restored table. Parsed and shape-checked by the engine; backends never see
/// it (they receive the resolved `CreateTableInput`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RestoreTableOverrides {
    /// Replacement billing mode, if the request supplied one.
    pub billing_mode: Option<BillingMode>,
    /// Replacement table throughput, if the request supplied one.
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    /// Replacement `SSESpecification` (`SSESpecificationOverride`), if the
    /// request supplied one. ExtendDB records SSE settings as metadata.
    pub sse_specification: Option<serde_json::Value>,
}

impl RestoreTableOverrides {
    /// True when the request supplied no override that changes the table.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.billing_mode.is_none()
            && self.provisioned_throughput.is_none()
            && self.sse_specification.is_none()
    }

    /// Apply overrides to a legacy backup's populated `CreateTableInput`.
    ///
    /// Legacy backups do not contain a [`BackupTableDefinition`], so the
    /// source defaults are reconstructed first and then this is applied.
    pub fn apply_to_create_input(&self, input: &mut CreateTableInput) {
        if let Some(billing_mode) = self.billing_mode {
            input.billing_mode = Some(billing_mode);
        }
        if let Some(throughput) = &self.provisioned_throughput {
            input.provisioned_throughput = Some(throughput.clone());
        }
        if let Some(sse) = &self.sse_specification {
            input.sse_specification = Some(sse.clone());
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

/// The refusal for a table or backup that has vector indexes. One message for
/// CreateBackup (which refuses up front) and RestoreTableFromBackup, so a
/// client sees the same reason at either step. `subject` is `table <name>` or
/// `backup <arn>`.
#[must_use]
pub fn vector_index_refusal(subject: &str, count: usize) -> StorageError {
    StorageError::Unsupported(format!(
        "{subject} has {count} vector index(es); restoring a table with vector indexes is \
         not supported by this storage backend"
    ))
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
    /// The source table's TTL description at CreateBackup, as
    /// DescribeTimeToLive reported it. For DescribeBackup only; a restore does
    /// not restore TTL. Absent in definitions written before this field.
    #[serde(
        rename = "TimeToLiveDescription",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub time_to_live: Option<TimeToLiveDescription>,
    /// The source table's stream specification at CreateBackup, as
    /// DescribeTable reported it. For DescribeBackup only; a restore does not
    /// restore streams. Absent in definitions written before this field.
    #[serde(
        rename = "StreamSpecification",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_specification: Option<StreamSpecification>,
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
        Err(vector_index_refusal(
            &format!("backup {backup_arn}"),
            self.vector_index_names.len(),
        ))
    }

    /// Apply the definition and explicit request overrides to a restore target's
    /// `CreateTableInput`.
    ///
    /// A PROVISIONED target whose recorded definition is also PROVISIONED
    /// gives every GSI that recorded no throughput, or 0 read and 0 write
    /// units, the target table's throughput. CreateTable accepts such a GSI,
    /// and switching an on-demand table to PROVISIONED with UpdateTable leaves
    /// its GSIs so, so their backups must restore. The service cannot produce
    /// such a backup; the choice is ExtendDB's. Any other GSI throughput below
    /// 1 is left for `validate_create_table` to refuse.
    ///
    /// # Errors
    ///
    /// [`StorageError::Validation`] when switching an on-demand backup with a
    /// GSI that has no provisioned throughput to provisioned billing. The
    /// service refuses the same request (it would need
    /// `GlobalSecondaryIndexOverride`, which ExtendDB does not support), so the
    /// message says what does work.
    pub fn apply_to(
        mut self,
        input: &mut CreateTableInput,
        overrides: &RestoreTableOverrides,
    ) -> Result<(), StorageError> {
        let recorded_on_demand = self.billing_mode == "PAY_PER_REQUEST";
        if let Some(billing_mode) = overrides.billing_mode {
            self.billing_mode = match billing_mode {
                BillingMode::Provisioned => "PROVISIONED".to_owned(),
                BillingMode::PayPerRequest => "PAY_PER_REQUEST".to_owned(),
            };
        }
        if let Some(throughput) = &overrides.provisioned_throughput {
            self.provisioned_throughput = Some(throughput.clone());
        }

        if self.billing_mode == "PROVISIONED" {
            if recorded_on_demand {
                if let Some(index) = self.global_secondary_indexes.iter().find(|index| {
                    index
                        .provisioned_throughput
                        .as_ref()
                        .is_none_or(|throughput| {
                            throughput.read_capacity_units < 1
                                || throughput.write_capacity_units < 1
                        })
                }) {
                    return Err(StorageError::Validation(gsi_throughput_refusal(
                        &index.index_name,
                    )));
                }
            } else if let Some(table) = self.provisioned_throughput.clone() {
                for index in &mut self.global_secondary_indexes {
                    let unset = index
                        .provisioned_throughput
                        .as_ref()
                        .is_none_or(|t| t.read_capacity_units == 0 && t.write_capacity_units == 0);
                    if unset {
                        index.provisioned_throughput = Some(table.clone());
                    }
                }
            }
        }
        if let Some(sse) = &overrides.sse_specification {
            self.sse_specification = Some(sse.clone());
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

/// The refusal for a PROVISIONED restore of a backup whose GSI has no
/// throughput to restore with.
fn gsi_throughput_refusal(index_name: &str) -> String {
    format!(
        "One or more parameter values were invalid: index {index_name} has no provisioned \
         throughput to restore with BillingModeOverride PROVISIONED, and \
         GlobalSecondaryIndexOverride is not supported on ExtendDB. Restore with \
         BillingModeOverride PAY_PER_REQUEST, then switch the table to PROVISIONED with \
         UpdateTable."
    )
}

/// Throughput a legacy PROVISIONED backup restores with when its source's
/// throughput was not recorded. Unchanged from catalog 0.0.3.
const LEGACY_DEFAULT_THROUGHPUT: i64 = 5;

/// A backup as the engine needs it to restore it and to describe it.
///
/// Returned by `BackupEngine::backup_restore_source` for an AVAILABLE backup.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupRestoreSource {
    pub backup_arn: String,
    /// Epoch seconds, sub-second precision. Reported as the restore's
    /// `RestoreSummary.RestoreDateTime`, as the service does.
    pub backup_creation_date_time: f64,
    pub source_table_name: String,
    pub source_table_arn: String,
    pub source_table_id: String,
    pub key_schema: Vec<KeySchemaElement>,
    pub attribute_definitions: Vec<AttributeDefinition>,
    /// `PROVISIONED` or `PAY_PER_REQUEST`.
    pub billing_mode: String,
    /// The source table's throughput at CreateBackup, if recorded.
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    pub item_count: i64,
    pub table_size_bytes: i64,
    pub table_creation_date_time: f64,
    /// `None`: a backup written before catalog 0.0.4, which carries only its
    /// keys and billing mode.
    pub definition: Option<BackupTableDefinition>,
    /// For a backup without a `definition` only: the number of vector
    /// indexes the backend recorded for the source table by other means
    /// (PostgreSQL's `backups.vector_indexes` snapshot), 0 if none or not
    /// recorded. Ignored when `definition` is present, whose
    /// `vector_index_names` is authoritative.
    pub legacy_vector_index_count: usize,
}

impl BackupRestoreSource {
    /// Refuse a backup no backend can restore faithfully: a multi-part base
    /// key, or vector indexes.
    ///
    /// # Errors
    ///
    /// [`StorageError::Unsupported`].
    pub fn ensure_restorable(&self) -> Result<(), StorageError> {
        ensure_single_part_base_key(&self.key_schema, &self.backup_arn)?;
        match &self.definition {
            Some(definition) => definition.ensure_restorable(&self.backup_arn),
            None if self.legacy_vector_index_count > 0 => Err(vector_index_refusal(
                &format!("backup {}", self.backup_arn),
                self.legacy_vector_index_count,
            )),
            None => Ok(()),
        }
    }

    /// The restore target's `CreateTableInput`: the recorded definition (or,
    /// for a legacy backup, the keys, billing mode, and throughput) with the
    /// request's overrides applied. The caller validates the result with
    /// `validate_create_table`.
    ///
    /// # Errors
    ///
    /// [`StorageError::Validation`] from [`BackupTableDefinition::apply_to`].
    pub fn restore_input(
        &self,
        target_table_name: &str,
        overrides: &RestoreTableOverrides,
    ) -> Result<CreateTableInput, StorageError> {
        let mut input = CreateTableInput {
            table_name: target_table_name.to_owned(),
            key_schema: self.key_schema.clone(),
            attribute_definitions: self.attribute_definitions.clone(),
            ..Default::default()
        };
        match &self.definition {
            Some(definition) => definition.clone().apply_to(&mut input, overrides)?,
            None => {
                // A backup taken before table definitions were recorded
                // carries only its keys and billing mode, so it restores as it
                // always has: no secondary indexes, and the recorded
                // throughput (5/5 if none) for a provisioned table. Those
                // backups recorded every attribute definition of the source,
                // including ones only its indexes used; with no indexes to
                // name them, keep only the key's, or CreateTable validation
                // refuses the input.
                input.attribute_definitions.retain(|a| {
                    self.key_schema
                        .iter()
                        .any(|k| k.attribute_name == a.attribute_name)
                });
                let on_demand = self.billing_mode == "PAY_PER_REQUEST";
                input.billing_mode = Some(if on_demand {
                    BillingMode::PayPerRequest
                } else {
                    BillingMode::Provisioned
                });
                input.provisioned_throughput = (!on_demand).then(|| {
                    self.provisioned_throughput
                        .clone()
                        .unwrap_or(ProvisionedThroughput {
                            read_capacity_units: LEGACY_DEFAULT_THROUGHPUT,
                            write_capacity_units: LEGACY_DEFAULT_THROUGHPUT,
                        })
                });
                overrides.apply_to_create_input(&mut input);
            }
        }
        Ok(input)
    }
}

/// What a backend is asked to do for one `RestoreTableFromBackup`, after the
/// engine has resolved and validated it.
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreRequest {
    pub backup_arn: String,
    /// Recorded for the target's `RestoreSummary.SourceTableArn`.
    pub source_table_arn: String,
    /// Recorded for the target's `RestoreSummary.RestoreDateTime`: the source
    /// backup's creation time (measured on the service), epoch seconds.
    pub restore_date_time: f64,
    /// The target table, fully resolved and validated by the engine. Create
    /// exactly this; `input.table_name` is the target name.
    pub input: CreateTableInput,
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
            time_to_live: None,
            stream_specification: None,
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
            ..Default::default()
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
            ..Default::default()
        };
        let mut input = CreateTableInput::default();

        let err = definition.apply_to(&mut input, &overrides).unwrap_err();

        match err {
            StorageError::Validation(message) => {
                assert_eq!(message, gsi_throughput_refusal("g"));
                assert!(message.contains("index g"), "{message}");
                assert!(message.contains("PAY_PER_REQUEST"), "{message}");
                assert!(message.contains("UpdateTable"), "{message}");
                assert!(
                    !message.contains("must be specified"),
                    "must not point at a refused override: {message}"
                );
            }
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
            ..Default::default()
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

    #[test]
    fn a_legacy_restore_keeps_only_the_key_attribute_definitions() {
        let def = |n: &str| AttributeDefinition {
            attribute_name: n.to_owned(),
            attribute_type: extenddb_core::types::ScalarAttributeType::S,
        };
        let source = BackupRestoreSource {
            backup_arn: "arn".to_owned(),
            backup_creation_date_time: 0.0,
            source_table_name: "t".to_owned(),
            source_table_arn: "arn:t".to_owned(),
            source_table_id: "id".to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            }],
            attribute_definitions: vec![def("pk"), def("gpk"), def("lsk")],
            billing_mode: "PAY_PER_REQUEST".to_owned(),
            provisioned_throughput: None,
            item_count: 0,
            table_size_bytes: 0,
            table_creation_date_time: 0.0,
            definition: None,
            legacy_vector_index_count: 0,
        };
        let input = source
            .restore_input("dst", &RestoreTableOverrides::default())
            .unwrap();
        assert_eq!(input.attribute_definitions, vec![def("pk")]);
        assert!(input.global_secondary_indexes.is_none());
        extenddb_core::validation::validate_create_table(
            &input,
            &extenddb_core::limits::LimitsConfig::default(),
        )
        .expect("a legacy backup of an indexed table restores");
    }

    #[test]
    fn a_definition_written_before_ttl_and_streams_still_parses() {
        let mut json = sample().to_json().unwrap();
        let obj = json.as_object_mut().unwrap();
        obj.remove("TimeToLiveDescription");
        obj.remove("StreamSpecification");
        let def = BackupTableDefinition::from_json(json, "arn").unwrap();
        assert!(def.time_to_live.is_none());
        assert!(def.stream_specification.is_none());
    }

    #[test]
    fn ttl_and_stream_round_trip_but_are_not_restored() {
        use extenddb_core::types::{StreamViewType, TimeToLiveStatus};
        let mut def = sample();
        def.time_to_live = Some(TimeToLiveDescription {
            time_to_live_status: TimeToLiveStatus::Enabled,
            attribute_name: Some("exp".to_owned()),
        });
        def.stream_specification = Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        });
        let json = def.to_json().unwrap();
        assert_eq!(json["TimeToLiveDescription"]["AttributeName"], "exp");
        assert_eq!(json["StreamSpecification"]["StreamEnabled"], true);
        let back = BackupTableDefinition::from_json(json, "arn").unwrap();
        assert_eq!(back, def);

        let mut input = CreateTableInput::default();
        back.apply_to(&mut input, &RestoreTableOverrides::default())
            .unwrap();
        assert!(input.stream_specification.is_none());
    }

    #[test]
    fn a_provisioned_gsi_without_throughput_takes_the_table_throughput() {
        let pairs = |input: &CreateTableInput| {
            input
                .global_secondary_indexes
                .iter()
                .flatten()
                .map(|i| {
                    i.provisioned_throughput
                        .as_ref()
                        .map(|t| (t.read_capacity_units, t.write_capacity_units))
                })
                .collect::<Vec<_>>()
        };
        // Recorded with no throughput (CreateTable PROVISIONED accepts a GSI
        // without one) and with 0/0 (UpdateTable to PROVISIONED leaves an
        // on-demand table's GSIs so).
        for recorded in [
            None,
            Some(ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            }),
        ] {
            let mut def = sample();
            def.global_secondary_indexes[0].provisioned_throughput = recorded.clone();
            let mut input = CreateTableInput::default();
            def.clone()
                .apply_to(&mut input, &RestoreTableOverrides::default())
                .unwrap();
            assert_eq!(pairs(&input), vec![Some((7, 9))], "{recorded:?}");

            let overrides = RestoreTableOverrides {
                billing_mode: Some(BillingMode::Provisioned),
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 5,
                    write_capacity_units: 6,
                }),
                ..Default::default()
            };
            let mut input = CreateTableInput::default();
            def.apply_to(&mut input, &overrides).unwrap();
            assert_eq!(pairs(&input), vec![Some((5, 6))], "{recorded:?} + override");
        }

        // Invalid recorded throughput is not replaced; validation refuses it.
        for (read, write) in [(-1, 5), (0, 5), (5, 0)] {
            let mut def = sample();
            def.global_secondary_indexes[0].provisioned_throughput = Some(ProvisionedThroughput {
                read_capacity_units: read,
                write_capacity_units: write,
            });
            let mut input = CreateTableInput::default();
            def.apply_to(&mut input, &RestoreTableOverrides::default())
                .unwrap();
            assert_eq!(pairs(&input), vec![Some((read, write))]);
        }
    }

    #[test]
    fn sse_override_replaces_the_recorded_specification() {
        let mut def = sample();
        def.sse_specification = Some(serde_json::json!({"Enabled": false}));
        let overrides = RestoreTableOverrides {
            sse_specification: Some(serde_json::json!({"Enabled": true, "SSEType": "KMS"})),
            ..Default::default()
        };
        let mut input = CreateTableInput::default();
        def.apply_to(&mut input, &overrides).unwrap();
        assert_eq!(
            input.sse_specification,
            Some(serde_json::json!({"Enabled": true, "SSEType": "KMS"}))
        );
    }

    fn source(definition: Option<BackupTableDefinition>) -> BackupRestoreSource {
        BackupRestoreSource {
            backup_arn: "arn".to_owned(),
            backup_creation_date_time: 1.5,
            source_table_name: "src".to_owned(),
            source_table_arn: "arn:t".to_owned(),
            source_table_id: "id".to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            }],
            attribute_definitions: Vec::new(),
            billing_mode: "PROVISIONED".to_owned(),
            provisioned_throughput: None,
            item_count: 0,
            table_size_bytes: 0,
            table_creation_date_time: 1.0,
            definition,
            legacy_vector_index_count: 0,
        }
    }

    #[test]
    fn legacy_restore_input_keeps_the_old_defaults() {
        let legacy = source(None);
        let input = legacy
            .restore_input("dst", &RestoreTableOverrides::default())
            .unwrap();
        assert_eq!(input.table_name, "dst");
        assert_eq!(input.billing_mode, Some(BillingMode::Provisioned));
        assert_eq!(
            input
                .provisioned_throughput
                .map(|t| (t.read_capacity_units, t.write_capacity_units)),
            Some((5, 5))
        );
        assert!(input.global_secondary_indexes.is_none());

        let mut recorded = source(None);
        recorded.provisioned_throughput = Some(ProvisionedThroughput {
            read_capacity_units: 8,
            write_capacity_units: 9,
        });
        let input = recorded
            .restore_input(
                "dst",
                &RestoreTableOverrides {
                    sse_specification: Some(serde_json::json!({"Enabled": true})),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(
            input
                .provisioned_throughput
                .map(|t| (t.read_capacity_units, t.write_capacity_units)),
            Some((8, 9))
        );
        assert_eq!(
            input.sse_specification,
            Some(serde_json::json!({"Enabled": true}))
        );

        let mut on_demand = source(None);
        on_demand.billing_mode = "PAY_PER_REQUEST".to_owned();
        let input = on_demand
            .restore_input("dst", &RestoreTableOverrides::default())
            .unwrap();
        assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
        assert!(input.provisioned_throughput.is_none());
    }

    #[test]
    fn source_refusals_cover_keys_and_vector_indexes() {
        assert!(source(Some(sample())).ensure_restorable().is_ok());
        assert!(source(None).ensure_restorable().is_ok());

        let mut multi = source(None);
        multi.key_schema.push(KeySchemaElement {
            attribute_name: "pk2".to_owned(),
            key_type: KeyType::Hash,
        });
        assert!(matches!(
            multi.ensure_restorable(),
            Err(StorageError::Unsupported(_))
        ));

        let mut def = sample();
        def.vector_index_names.push("v".to_owned());
        assert!(matches!(
            source(Some(def)).ensure_restorable(),
            Err(StorageError::Unsupported(m)) if m.contains("1 vector index")
        ));

        let mut legacy = source(None);
        legacy.legacy_vector_index_count = 2;
        assert!(matches!(
            legacy.ensure_restorable(),
            Err(StorageError::Unsupported(m)) if m.contains("2 vector index")
        ));
        // The definition is authoritative when present.
        let mut with_def = source(Some(sample()));
        with_def.legacy_vector_index_count = 2;
        assert!(with_def.ensure_restorable().is_ok());
    }

    #[test]
    fn vector_refusal_reads_the_same_for_a_table_and_a_backup() {
        let StorageError::Unsupported(t) = vector_index_refusal("table T", 1) else {
            panic!()
        };
        let StorageError::Unsupported(b) = vector_index_refusal("backup arn", 1) else {
            panic!()
        };
        assert_eq!(
            t.trim_start_matches("table T"),
            b.trim_start_matches("backup arn")
        );
    }
}

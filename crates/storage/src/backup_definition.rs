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

use extenddb_core::backup_format::SourceTableSchema;
use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, GsiInput, KeySchemaElement, KeyType,
    LsiInput, OnDemandThroughput, ProvisionedThroughput, SseDescription, StreamSpecification,
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

    /// Apply the definition and the request's billing, throughput, and SSE
    /// overrides to a restore target's `CreateTableInput`. The index and
    /// on-demand throughput overrides are the engine's
    /// (`extenddb_engine::backup::overrides`), applied to the result.
    ///
    /// A PROVISIONED target whose recorded definition is also PROVISIONED
    /// gives every GSI that recorded no throughput, or 0 read and 0 write
    /// units, the target table's throughput. CreateTable accepts such a GSI,
    /// and switching an on-demand table to PROVISIONED with UpdateTable leaves
    /// its GSIs so, so their backups must restore. The service cannot produce
    /// such a backup; the choice is ExtendDB's. Any other GSI throughput below
    /// 1 is left for `validate_create_table` to refuse. An on-demand backup
    /// restored as PROVISIONED keeps its GSIs without throughput here; the
    /// engine refuses that unless a `GlobalSecondaryIndexOverride` supplies
    /// each index's throughput, as the service does.
    ///
    /// `TableClass` is not restored: the service restores `STANDARD` whatever
    /// the source's class was (measured on DynamoDB), so the
    /// recorded class is kept for `DescribeBackup` only.
    pub fn apply_to(mut self, input: &mut CreateTableInput, overrides: &RestoreTableOverrides) {
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

        if self.billing_mode == "PROVISIONED"
            && !recorded_on_demand
            && let Some(table) = self.provisioned_throughput.clone()
        {
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
        input.table_class = None;
        input.sse_specification = self.sse_specification;
        input.on_demand_throughput = self.on_demand_throughput;
    }
}

/// Throughput a legacy PROVISIONED backup restores with when its source's
/// throughput was not recorded. Unchanged from catalog 0.0.3.
const LEGACY_DEFAULT_THROUGHPUT: i64 = 5;

/// A backup as the engine needs it to restore it and to describe it.
///
/// Returned by `LegacyBackupReader::restore_source` for an AVAILABLE backup.
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
    /// request's billing, throughput, and SSE overrides applied. The engine
    /// applies the index and on-demand overrides to the result and validates
    /// it with `validate_create_table`.
    #[must_use]
    pub fn restore_input(
        &self,
        target_table_name: &str,
        overrides: &RestoreTableOverrides,
    ) -> CreateTableInput {
        let mut input = CreateTableInput {
            table_name: target_table_name.to_owned(),
            key_schema: self.key_schema.clone(),
            attribute_definitions: self.attribute_definitions.clone(),
            ..Default::default()
        };
        match &self.definition {
            Some(definition) => definition.clone().apply_to(&mut input, overrides),
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
        input
    }
}

/// What a backend can state about the vector indexes of an in-catalog
/// backup's source table: the three states of a manifest's
/// `source_table.vector_indexes`, of which `Unknown` has no manifest form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyVectorState {
    /// The backend has no vector support (MongoDB, Cassandra): `null`.
    Unsupported,
    /// The backend supports vector indexes and its record states that the
    /// table had none: a definition with no `VectorIndexNames`, or on
    /// PostgreSQL a backup without a definition whose `vector_indexes`
    /// snapshot is NULL or empty. `[]`.
    NoneRecorded,
    /// The backend supports vector indexes and recorded nothing about them:
    /// a SQLite backup taken before catalog 0.0.4, which has no definition.
    Unknown,
}

/// The manifest `source_table` of a copy of an in-catalog backup: the table
/// the legacy restore of `source` creates, so a restore of the copy creates
/// the same table (`restore_input_from_manifest` of the result equals
/// `source.restore_input` for the same overrides, up to the SSE
/// specification, below).
///
/// The keys, the attribute definitions (trimmed to the keys for a backup
/// with no definition, as the legacy restore trims them), the billing mode,
/// the throughput (5/5 for a provisioned backup that recorded none), the
/// secondary indexes (a provisioned GSI that recorded no throughput carries
/// the table's), the on-demand throughput and the SSE settings are those of
/// `source.restore_input` with no overrides. `table_class`, `time_to_live`
/// and `stream_specification` come from the definition, for DescribeBackup;
/// the SSE specification is recorded as the description DescribeBackup
/// reports for it ([`SseDescription::from_specification`]). A restore maps
/// an `ENABLED` description back to `{"Enabled": true, "SSEType": "KMS"}`,
/// so a `KMSMasterKeyId` the specification carried is not in the copy.
/// `vector_indexes` is `null` for [`LegacyVectorState::Unsupported`] and `[]`
/// for [`LegacyVectorState::NoneRecorded`].
///
/// # Errors
///
/// [`StorageError::Unsupported`] for what
/// [`BackupRestoreSource::ensure_restorable`] refuses (a multi-part base key,
/// vector indexes), and for [`LegacyVectorState::Unknown`]: the copy would
/// have to guess the table's vector indexes.
pub fn source_table_from_legacy(
    source: &BackupRestoreSource,
    vector_state: LegacyVectorState,
    region: &str,
    account_id: &str,
) -> Result<SourceTableSchema, StorageError> {
    source.ensure_restorable()?;
    let vector_indexes = match vector_state {
        LegacyVectorState::Unsupported => None,
        LegacyVectorState::NoneRecorded => Some(Vec::new()),
        LegacyVectorState::Unknown => {
            return Err(StorageError::Unsupported(format!(
                "backup {} has no recorded table definition (it was taken before catalog \
                 0.0.4), so its vector indexes cannot be stated",
                source.backup_arn
            )));
        }
    };
    let input = source.restore_input(&source.source_table_name, &RestoreTableOverrides::default());
    let definition = source.definition.as_ref();
    Ok(SourceTableSchema {
        table_name: source.source_table_name.clone(),
        table_id: source.source_table_id.clone(),
        table_arn: source.source_table_arn.clone(),
        table_creation_date_time: source.table_creation_date_time,
        key_schema: input.key_schema,
        attribute_definitions: input.attribute_definitions,
        billing_mode: input.billing_mode.unwrap_or(BillingMode::PayPerRequest),
        provisioned_throughput: input.provisioned_throughput,
        on_demand_throughput: input.on_demand_throughput,
        table_class: definition.and_then(|d| d.table_class.clone()),
        sse_description: SseDescription::from_specification(
            input.sse_specification.as_ref(),
            region,
            account_id,
        ),
        global_secondary_indexes: input.global_secondary_indexes.unwrap_or_default(),
        local_secondary_indexes: input.local_secondary_indexes.unwrap_or_default(),
        vector_indexes,
        stream_specification: definition.and_then(|d| d.stream_specification.clone()),
        time_to_live: definition.and_then(|d| d.time_to_live.clone()),
        item_count: source.item_count,
        table_size_bytes: source.table_size_bytes,
    })
}

/// What the engine answers a `RestoreTableFromBackup` with, beside the
/// target's description: the three members of the `RestoreSummary`, from a
/// legacy backup's [`BackupRestoreSource`] or a store-backed backup's
/// manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreSource {
    pub backup_arn: String,
    pub source_table_arn: String,
    /// Epoch seconds; reported as `RestoreSummary.RestoreDateTime`, as the
    /// service does.
    pub backup_creation_date_time: f64,
}

impl From<&BackupRestoreSource> for RestoreSource {
    fn from(source: &BackupRestoreSource) -> Self {
        Self {
            backup_arn: source.backup_arn.clone(),
            source_table_arn: source.source_table_arn.clone(),
            backup_creation_date_time: source.backup_creation_date_time,
        }
    }
}

/// The restore target's `CreateTableInput` for a store-backed backup: the
/// manifest's `source_table` with the request's billing, throughput and SSE
/// overrides applied, by the rule [`BackupTableDefinition::apply_to`] applies
/// to a legacy definition (one rule for both paths: the GSI throughput
/// defaulting, the on-demand normalization, `TableClass` not restored). The
/// engine applies the index and on-demand overrides to the result and
/// validates it with `validate_create_table`. `vector_indexes` is the
/// manifest's list when it is non-empty (a target without vector support has
/// refused it before this is called), else `None`; the recorded SSE
/// description maps back to the specification the backends derive it from
/// (an `ENABLED` description was an enabled `KMS` specification). Streams,
/// TTL and tags are not restored, as the service does not.
#[must_use]
pub fn restore_input_from_manifest(
    source: &SourceTableSchema,
    target_table_name: &str,
    overrides: &RestoreTableOverrides,
) -> CreateTableInput {
    let mut input = CreateTableInput {
        table_name: target_table_name.to_owned(),
        key_schema: source.key_schema.clone(),
        attribute_definitions: source.attribute_definitions.clone(),
        ..Default::default()
    };
    let definition = BackupTableDefinition {
        version: BACKUP_DEFINITION_VERSION,
        billing_mode: crate::backup_catalog::billing_mode_name(source.billing_mode).to_owned(),
        provisioned_throughput: source.provisioned_throughput.clone(),
        global_secondary_indexes: source.global_secondary_indexes.clone(),
        local_secondary_indexes: source.local_secondary_indexes.clone(),
        table_class: source.table_class.clone(),
        sse_specification: source
            .sse_description
            .as_ref()
            .filter(|sse| sse.status == "ENABLED")
            .map(|_| serde_json::json!({"Enabled": true, "SSEType": "KMS"})),
        on_demand_throughput: source.on_demand_throughput.clone(),
        vector_index_names: Vec::new(),
        time_to_live: None,
        stream_specification: None,
    };
    definition.apply_to(&mut input, overrides);
    input.vector_indexes = source.vector_indexes.clone().filter(|v| !v.is_empty());
    input
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
        sample().apply_to(&mut input, &RestoreTableOverrides::default());
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
        // Not restored: the service restores STANDARD whatever the source's
        // class was (measured on DynamoDB). The class stays in the definition for
        // DescribeBackup.
        assert_eq!(input.table_class, None);
    }

    #[test]
    fn provisioned_table_round_trip_keeps_throughput() {
        let definition = sample();
        let captured = definition.to_json().unwrap();
        let restored = BackupTableDefinition::from_json(captured, "arn").unwrap();
        let mut input = CreateTableInput::default();
        restored.apply_to(&mut input, &RestoreTableOverrides::default());

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
        definition.apply_to(&mut input, &RestoreTableOverrides::default());
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
        definition.apply_to(&mut input, &RestoreTableOverrides::default());
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
        sample().apply_to(&mut input, &overrides);

        assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
        assert!(input.provisioned_throughput.is_none());
        assert!(
            input.global_secondary_indexes.unwrap()[0]
                .provisioned_throughput
                .is_none()
        );
    }

    #[test]
    fn provisioned_override_on_an_on_demand_backup_leaves_its_gsis_without_throughput() {
        // The engine refuses this input unless a GlobalSecondaryIndexOverride
        // supplies each index's throughput (the service's rule); the storage
        // layer only shapes the input.
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
        definition.apply_to(&mut input, &overrides);
        assert_eq!(input.billing_mode, Some(BillingMode::Provisioned));
        assert!(
            input.global_secondary_indexes.unwrap()[0]
                .provisioned_throughput
                .is_none()
        );
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
        definition.apply_to(&mut input, &overrides);

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
        let input = source.restore_input("dst", &RestoreTableOverrides::default());
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
        back.apply_to(&mut input, &RestoreTableOverrides::default());
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
                .apply_to(&mut input, &RestoreTableOverrides::default());
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
            def.apply_to(&mut input, &overrides);
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
            def.apply_to(&mut input, &RestoreTableOverrides::default());
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
        def.apply_to(&mut input, &overrides);
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
        let input = legacy.restore_input("dst", &RestoreTableOverrides::default());
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
        let input = recorded.restore_input(
            "dst",
            &RestoreTableOverrides {
                sse_specification: Some(serde_json::json!({"Enabled": true})),
                ..Default::default()
            },
        );
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
        let input = on_demand.restore_input("dst", &RestoreTableOverrides::default());
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

    /// Every combination of the definition shapes a legacy backup can carry,
    /// as a [`BackupRestoreSource`].
    #[allow(clippy::too_many_lines)]
    fn legacy_sources() -> Vec<BackupRestoreSource> {
        let key = |n: &str, t| KeySchemaElement {
            attribute_name: n.to_owned(),
            key_type: t,
        };
        let attr = |n: &str| AttributeDefinition {
            attribute_name: n.to_owned(),
            attribute_type: extenddb_core::types::ScalarAttributeType::S,
        };
        let throughput = |r, w| ProvisionedThroughput {
            read_capacity_units: r,
            write_capacity_units: w,
        };
        let gsi = |t: Option<ProvisionedThroughput>| GsiInput {
            index_name: "g".to_owned(),
            key_schema: vec![key("gpk", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
            provisioned_throughput: t,
        };
        let lsi = || LsiInput {
            index_name: "l".to_owned(),
            key_schema: vec![key("pk", KeyType::Hash), key("lsk", KeyType::Range)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        };
        let key_schemas = [
            vec![key("pk", KeyType::Hash)],
            vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        ];
        let gsis = [
            Vec::new(),
            vec![gsi(Some(throughput(3, 4)))],
            vec![gsi(None)],
            vec![gsi(Some(throughput(0, 0)))],
        ];
        let sses = [
            None,
            Some(serde_json::json!({"Enabled": true, "SSEType": "KMS"})),
            Some(serde_json::json!({"Enabled": true})),
            Some(serde_json::json!({"Enabled": false})),
        ];
        let ttl = TimeToLiveDescription {
            time_to_live_status: extenddb_core::types::TimeToLiveStatus::Enabled,
            attribute_name: Some("ttl".to_owned()),
        };
        let stream = StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(extenddb_core::types::StreamViewType::KeysOnly),
        };
        let mut sources = Vec::new();
        for key_schema in &key_schemas {
            for billing in ["PROVISIONED", "PAY_PER_REQUEST"] {
                for recorded in [None, Some(throughput(8, 9))] {
                    let mut attributes: Vec<AttributeDefinition> =
                        key_schema.iter().map(|k| attr(&k.attribute_name)).collect();
                    // The index attributes, which a pre-0.0.4 backup recorded
                    // with its keys and no indexes to name them.
                    attributes.extend([attr("gpk"), attr("lsk")]);
                    let base = BackupRestoreSource {
                        backup_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/src/backup/\
                                     01757500000123-73d8d5bc"
                            .to_owned(),
                        backup_creation_date_time: 1_757_500_000.123,
                        source_table_name: "src".to_owned(),
                        source_table_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/src"
                            .to_owned(),
                        source_table_id: "id".to_owned(),
                        key_schema: key_schema.clone(),
                        attribute_definitions: attributes,
                        billing_mode: billing.to_owned(),
                        provisioned_throughput: recorded.clone(),
                        item_count: 12,
                        table_size_bytes: 345,
                        table_creation_date_time: 1_757_400_000.0,
                        definition: None,
                        legacy_vector_index_count: 0,
                    };
                    // A backup taken before catalog 0.0.4: keys, billing and
                    // the throughput column only.
                    sources.push(base.clone());
                    for gsis in &gsis {
                        for lsis in [Vec::new(), vec![lsi()]] {
                            for sse in &sses {
                                for on_demand in [
                                    None,
                                    Some(OnDemandThroughput {
                                        max_read_request_units: Some(10),
                                        max_write_request_units: None,
                                    }),
                                ] {
                                    for extras in [false, true] {
                                        let mut source = base.clone();
                                        source.definition = Some(BackupTableDefinition {
                                            version: BACKUP_DEFINITION_VERSION,
                                            billing_mode: billing.to_owned(),
                                            provisioned_throughput: recorded.clone(),
                                            global_secondary_indexes: gsis.clone(),
                                            local_secondary_indexes: lsis.clone(),
                                            table_class: extras
                                                .then(|| "STANDARD_INFREQUENT_ACCESS".to_owned()),
                                            sse_specification: sse.clone(),
                                            on_demand_throughput: on_demand.clone(),
                                            vector_index_names: Vec::new(),
                                            time_to_live: extras.then(|| ttl.clone()),
                                            stream_specification: extras.then(|| stream.clone()),
                                        });
                                        sources.push(source);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        sources
    }

    /// The SSE settings a target created from `input` reports, which is what
    /// the two paths must agree on: the specification itself differs in
    /// spelling (`{"Enabled": false}` against none, `{"Enabled": true}`
    /// against the same with `"SSEType": "KMS"`).
    fn reported_sse(input: &CreateTableInput) -> Option<SseDescription> {
        SseDescription::from_specification(
            input.sse_specification.as_ref(),
            "us-east-1",
            "123456789012",
        )
    }

    #[test]
    fn a_copy_of_a_legacy_backup_restores_the_table_its_legacy_restore_creates() {
        let overrides = [
            RestoreTableOverrides::default(),
            RestoreTableOverrides {
                billing_mode: Some(BillingMode::PayPerRequest),
                ..Default::default()
            },
            RestoreTableOverrides {
                sse_specification: Some(serde_json::json!({"Enabled": true, "SSEType": "KMS"})),
                ..Default::default()
            },
            RestoreTableOverrides {
                sse_specification: Some(serde_json::json!({"Enabled": false})),
                ..Default::default()
            },
        ];
        let sources = legacy_sources();
        assert!(sources.len() > 1000, "{} shapes", sources.len());
        let mut checked = 0;
        for source in &sources {
            for state in [
                LegacyVectorState::Unsupported,
                LegacyVectorState::NoneRecorded,
            ] {
                let table =
                    source_table_from_legacy(source, state, "us-east-1", "123456789012").unwrap();
                // The manifest member survives its own serialization, as the
                // manifest writer and parser carry it.
                let json = serde_json::to_value(&table).unwrap();
                let back: SourceTableSchema = serde_json::from_value(json).unwrap();
                assert_eq!(back, table);
                assert_eq!(
                    table.vector_indexes,
                    (state == LegacyVectorState::NoneRecorded).then(Vec::new)
                );
                for o in &overrides {
                    let legacy = source.restore_input("dst", o);
                    let mut copy = restore_input_from_manifest(&table, "dst", o);
                    // Without an SSE override, the copy restores the one
                    // specification an ENABLED description maps back to.
                    assert!(
                        o.sse_specification.is_some()
                            || copy.sse_specification.is_none()
                            || copy.sse_specification
                                == Some(serde_json::json!({"Enabled": true, "SSEType": "KMS"})),
                        "{:?}",
                        copy.sse_specification
                    );
                    assert_eq!(
                        reported_sse(&copy),
                        reported_sse(&legacy),
                        "{source:?} {o:?}"
                    );
                    copy.sse_specification.clone_from(&legacy.sse_specification);
                    assert_eq!(copy, legacy, "{source:?} {o:?}");
                    checked += 1;
                }
            }
        }
        assert_eq!(checked, sources.len() * 2 * overrides.len());
    }

    #[test]
    fn a_copy_records_the_described_features_and_the_legacy_identity() {
        let source = legacy_sources()
            .into_iter()
            .find(|s| {
                s.definition.as_ref().is_some_and(|d| {
                    d.table_class.is_some()
                        && d.sse_specification.is_some()
                        && d.billing_mode == "PROVISIONED"
                        && d.provisioned_throughput.is_some()
                        && d.global_secondary_indexes
                            .first()
                            .is_some_and(|g| g.provisioned_throughput.is_none())
                })
            })
            .unwrap();
        let table = source_table_from_legacy(
            &source,
            LegacyVectorState::NoneRecorded,
            "us-east-1",
            "123456789012",
        )
        .unwrap();
        let definition = source.definition.clone().unwrap();
        assert_eq!(table.table_name, "src");
        assert_eq!(table.table_id, "id");
        assert_eq!(table.table_arn, source.source_table_arn);
        assert!((table.table_creation_date_time - 1_757_400_000.0).abs() < f64::EPSILON);
        assert_eq!(table.item_count, 12);
        assert_eq!(table.table_size_bytes, 345);
        assert_eq!(table.table_class, definition.table_class);
        assert_eq!(table.time_to_live, definition.time_to_live);
        assert_eq!(table.stream_specification, definition.stream_specification);
        assert_eq!(
            table.sse_description.map(|d| d.status),
            Some("ENABLED".to_owned())
        );
        // The provisioned GSI with no throughput carries the table's, as the
        // legacy restore creates it.
        let gsi_throughput = table.global_secondary_indexes[0]
            .provisioned_throughput
            .clone()
            .map(|t| (t.read_capacity_units, t.write_capacity_units));
        assert_eq!(gsi_throughput, Some((8, 9)));

        // A backup with no definition records its keys and nothing more.
        let mut bare = source.clone();
        bare.definition = None;
        let table = source_table_from_legacy(
            &bare,
            LegacyVectorState::Unsupported,
            "us-east-1",
            "123456789012",
        )
        .unwrap();
        assert_eq!(
            table
                .attribute_definitions
                .iter()
                .map(|a| a.attribute_name.as_str())
                .collect::<Vec<_>>(),
            table
                .key_schema
                .iter()
                .map(|k| k.attribute_name.as_str())
                .collect::<Vec<_>>()
        );
        assert!(table.global_secondary_indexes.is_empty());
        assert!(table.table_class.is_none());
        assert!(table.time_to_live.is_none());
        assert!(table.stream_specification.is_none());
        assert!(table.vector_indexes.is_none());
    }

    #[test]
    fn a_copy_drops_a_kms_key_id_and_nothing_else() {
        let mut def = sample();
        def.sse_specification = Some(serde_json::json!({
            "Enabled": true, "SSEType": "KMS", "KMSMasterKeyId": "alias/k"
        }));
        let source = source(Some(def));
        let table = source_table_from_legacy(
            &source,
            LegacyVectorState::NoneRecorded,
            "us-east-1",
            "123456789012",
        )
        .unwrap();
        let none = RestoreTableOverrides::default();
        let legacy = source.restore_input("dst", &none);
        let mut copy = restore_input_from_manifest(&table, "dst", &none);
        assert_eq!(
            legacy.sse_specification.as_ref().unwrap()["KMSMasterKeyId"],
            "alias/k"
        );
        assert_eq!(
            copy.sse_specification,
            Some(serde_json::json!({"Enabled": true, "SSEType": "KMS"}))
        );
        copy.sse_specification.clone_from(&legacy.sse_specification);
        assert_eq!(copy, legacy);
    }

    #[test]
    fn a_copy_refuses_what_the_legacy_restore_refuses_and_an_unknown_vector_state() {
        let state = LegacyVectorState::NoneRecorded;
        let copy = |s: &BackupRestoreSource, state| {
            source_table_from_legacy(s, state, "us-east-1", "123456789012")
        };
        // No definition and nothing recorded about vector indexes.
        assert!(matches!(
            copy(&source(None), LegacyVectorState::Unknown),
            Err(StorageError::Unsupported(m))
                if m.contains("no recorded table definition") && m.contains("vector indexes")
        ));
        // A definition with no vector index names copies in a state the
        // backend can state; `Unknown` is refused even with a definition.
        assert!(copy(&source(Some(sample())), state).is_ok());
        assert!(matches!(
            copy(&source(Some(sample())), LegacyVectorState::Unknown),
            Err(StorageError::Unsupported(_))
        ));
        let mut def = sample();
        def.vector_index_names.push("v".to_owned());
        assert!(matches!(
            copy(&source(Some(def)), state),
            Err(StorageError::Unsupported(m)) if m.contains("1 vector index")
        ));
        let mut counted = source(None);
        counted.legacy_vector_index_count = 2;
        assert!(matches!(
            copy(&counted, state),
            Err(StorageError::Unsupported(m)) if m.contains("2 vector index")
        ));
        let mut multi = source(None);
        multi.key_schema.push(KeySchemaElement {
            attribute_name: "pk2".to_owned(),
            key_type: KeyType::Hash,
        });
        assert!(matches!(
            copy(&multi, state),
            Err(StorageError::Unsupported(m)) if m.contains("multi-part key")
        ));
    }
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Engine backup rules, against a `BackupEngine` double that records what the
//! engine asked of it.

use std::collections::HashMap;
use std::sync::Mutex;

use extenddb_core::error::DynamoDbError;
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeDefinition, BackupDescription, BackupDetails, BackupSummary, BillingMode,
    BillingModeSummary, ContinuousBackupsDescription, GsiDescription, GsiInput, KeySchemaElement,
    KeyType, LsiInput, Projection, ProjectionType, ProvisionedThroughput,
    ProvisionedThroughputDescription, ScalarAttributeType, SourceTableDetails, StreamSpecification,
    StreamViewType, TableDescription, TableStatus, TimeToLiveDescription, TimeToLiveStatus,
};
use extenddb_storage::backup_definition::{BACKUP_DEFINITION_VERSION, BackupTableDefinition};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, BackupRestoreSource, BoxedFuture, RestoreRequest};
use serde_json::{Value, json};

use super::*;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const CREATED: f64 = 1_791_976_431.438;

fn arn(account: &str) -> String {
    format!("arn:aws:dynamodb:{REGION}:{account}:table/Music/backup/01791976431438-73d8d5bc")
}

fn source_arn() -> String {
    format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music")
}

// --- double -----------------------------------------------------------------

#[derive(Default)]
struct Fake {
    sources: Mutex<HashMap<String, BackupRestoreSource>>,
    descriptions: Mutex<HashMap<String, BackupDescription>>,
    summaries: Mutex<Vec<BackupSummary>>,
    restore_error: Mutex<Option<StorageError>>,
    /// Returned (once) by `backup_restore_source` instead of the stored source.
    source_error: Mutex<Option<StorageError>>,
    restores: Mutex<Vec<RestoreRequest>>,
    source_reads: Mutex<usize>,
    listed_for: Mutex<Vec<Option<String>>>,
    /// When set, `backup_restore_source` waits for a permit before answering.
    source_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
}

impl Fake {
    fn with_source(source: BackupRestoreSource) -> Self {
        let fake = Self::default();
        fake.sources
            .lock()
            .unwrap()
            .insert(source.backup_arn.clone(), source);
        fake
    }
    fn restores(&self) -> Vec<RestoreRequest> {
        self.restores.lock().unwrap().clone()
    }
    fn source_reads(&self) -> usize {
        *self.source_reads.lock().unwrap()
    }
}

fn unused<T: Send + 'static>() -> BoxedFuture<'static, Result<T, StorageError>> {
    Box::pin(async { Err(StorageError::Internal("not used by these tests".to_owned())) })
}

impl BackupEngine for Fake {
    fn create_backup(
        &self,
        _: &str,
        _: &str,
        _: &str,
    ) -> BoxedFuture<'_, Result<BackupDetails, StorageError>> {
        unused()
    }

    fn describe_backup(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxedFuture<'_, Result<BackupDescription, StorageError>> {
        let found = self.descriptions.lock().unwrap().get(backup_arn).cloned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            found.ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))
        })
    }

    fn list_backups(
        &self,
        _account_id: &str,
        table_name: Option<&str>,
    ) -> BoxedFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        self.listed_for
            .lock()
            .unwrap()
            .push(table_name.map(str::to_owned));
        let all = self.summaries.lock().unwrap().clone();
        Box::pin(async move { Ok(all) })
    }

    fn delete_backup(
        &self,
        _: &str,
        _: &str,
    ) -> BoxedFuture<'_, Result<BackupDescription, StorageError>> {
        unused()
    }

    fn backup_restore_source(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxedFuture<'_, Result<BackupRestoreSource, StorageError>> {
        *self.source_reads.lock().unwrap() += 1;
        let err = self.source_error.lock().unwrap().take();
        let found = self.sources.lock().unwrap().get(backup_arn).cloned();
        let backup_arn = backup_arn.to_owned();
        let gate = self.source_gate.clone();
        Box::pin(async move {
            if let Some(gate) = gate {
                let _permit = gate.acquire().await.expect("gate closed");
            }
            if let Some(e) = err {
                return Err(e);
            }
            found.ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))
        })
    }

    fn restore_table_from_backup(
        &self,
        _account_id: &str,
        request: RestoreRequest,
    ) -> BoxedFuture<'_, Result<TableDescription, StorageError>> {
        let err = self.restore_error.lock().unwrap().take();
        let desc = described(&request);
        self.restores.lock().unwrap().push(request);
        Box::pin(async move {
            match err {
                Some(e) => Err(e),
                None => Ok(desc),
            }
        })
    }

    fn describe_continuous_backups(
        &self,
        _: &str,
        _: &str,
    ) -> BoxedFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        unused()
    }

    fn update_continuous_backups(
        &self,
        _: &str,
        _: &str,
        _: bool,
    ) -> BoxedFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        unused()
    }
}

/// What a backend reports for a CREATING target: deliberately the shape the
/// engine must correct (ACTIVE GSI status, a pay-per-request timestamp, a
/// backend-side summary with the wrong time).
fn described(request: &RestoreRequest) -> TableDescription {
    let input = &request.input;
    TableDescription {
        table_name: input.table_name.clone(),
        key_schema: input.key_schema.clone(),
        attribute_definitions: input.attribute_definitions.clone(),
        table_status: TableStatus::Creating,
        billing_mode_summary: Some(BillingModeSummary {
            billing_mode: input.billing_mode.unwrap_or(BillingMode::Provisioned),
            last_update_to_pay_per_request_date_time: Some(1.0),
        }),
        provisioned_throughput: ProvisionedThroughputDescription {
            read_capacity_units: 9,
            write_capacity_units: 9,
            ..Default::default()
        },
        global_secondary_indexes: input.global_secondary_indexes.as_ref().map(|gsis| {
            gsis.iter()
                .map(|g| GsiDescription {
                    index_name: g.index_name.clone(),
                    key_schema: g.key_schema.clone(),
                    projection: g.projection.clone(),
                    index_status: "ACTIVE".to_owned(),
                    provisioned_throughput: Some(ProvisionedThroughputDescription {
                        read_capacity_units: 9,
                        write_capacity_units: 9,
                        ..Default::default()
                    }),
                    index_size_bytes: 0,
                    item_count: 0,
                    index_arn: String::new(),
                })
                .collect()
        }),
        restore_summary: Some(RestoreSummary {
            source_backup_arn: Some("wrong".to_owned()),
            source_table_arn: None,
            restore_date_time: 42.0,
            restore_in_progress: false,
        }),
        ..Default::default()
    }
}

// --- fixtures ---------------------------------------------------------------

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn attr(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    }
}

fn throughput(read: i64, write: i64) -> ProvisionedThroughput {
    ProvisionedThroughput {
        read_capacity_units: read,
        write_capacity_units: write,
    }
}

fn gsi(name: &str, pk: &str, pt: Option<ProvisionedThroughput>) -> GsiInput {
    GsiInput {
        index_name: name.to_owned(),
        key_schema: vec![key(pk, KeyType::Hash)],
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
        provisioned_throughput: pt,
    }
}

fn definition(billing: &str) -> BackupTableDefinition {
    let provisioned = billing == "PROVISIONED";
    BackupTableDefinition {
        version: BACKUP_DEFINITION_VERSION,
        billing_mode: billing.to_owned(),
        provisioned_throughput: provisioned.then(|| throughput(7, 8)),
        global_secondary_indexes: vec![gsi("gsi1", "gpk", provisioned.then(|| throughput(3, 4)))],
        local_secondary_indexes: vec![LsiInput {
            index_name: "lsi1".to_owned(),
            key_schema: vec![key("pk", KeyType::Hash), key("lsk", KeyType::Range)],
            projection: Projection {
                projection_type: ProjectionType::KeysOnly,
                non_key_attributes: None,
            },
        }],
        table_class: None,
        sse_specification: None,
        on_demand_throughput: None,
        vector_index_names: Vec::new(),
        time_to_live: None,
        stream_specification: None,
    }
}

fn source(definition: Option<BackupTableDefinition>) -> BackupRestoreSource {
    let billing = definition
        .as_ref()
        .map_or("PROVISIONED".to_owned(), |d| d.billing_mode.clone());
    BackupRestoreSource {
        backup_arn: arn(ACCOUNT),
        backup_creation_date_time: CREATED,
        source_table_name: "Music".to_owned(),
        source_table_arn: source_arn(),
        source_table_id: "tid".to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: if definition.is_some() {
            vec![attr("pk"), attr("sk"), attr("gpk"), attr("lsk")]
        } else {
            vec![attr("pk"), attr("sk")]
        },
        provisioned_throughput: definition
            .as_ref()
            .and_then(|d| d.provisioned_throughput.clone()),
        billing_mode: billing,
        item_count: 3,
        table_size_bytes: 30,
        table_creation_date_time: 1.0,
        definition,
        legacy_vector_index_count: 0,
    }
}

fn restore_body(extra: Value) -> Value {
    let mut body = json!({ "TargetTableName": "MusicRestored", "BackupArn": arn(ACCOUNT) });
    for (k, v) in extra.as_object().unwrap() {
        body[k] = v.clone();
    }
    body
}

async fn restore(fake: &Fake, body: Value) -> Result<Value, DynamoDbError> {
    restore_table_from_backup(fake, ACCOUNT, &LimitsConfig::default(), &body)
        .await
        .map(|(_, v)| v)
}

fn validation_message(err: DynamoDbError) -> String {
    match err {
        DynamoDbError::ValidationException(m) => m,
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

// --- BackupArn ---------------------------------------------------------------

#[test]
fn own_account_arn_is_accepted() {
    let body = json!({ "BackupArn": arn(ACCOUNT) });
    assert_eq!(backup_arn_field(&body, ACCOUNT).unwrap(), arn(ACCOUNT));
}

#[test]
fn other_account_arn_is_denied() {
    let body = json!({ "BackupArn": arn("999999999999") });
    let err = backup_arn_field(&body, ACCOUNT).unwrap_err();
    assert!(
        matches!(err, DynamoDbError::AccessDeniedException(_)),
        "{err:?}"
    );
}

#[test]
fn account_prefix_does_not_match() {
    let body = json!({ "BackupArn": arn("12345678901") });
    assert!(matches!(
        backup_arn_field(&body, ACCOUNT).unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));
}

#[test]
fn malformed_arn_is_a_validation_exception() {
    for candidate in [
        "not-an-arn",
        "arn:aws:dynamodb",
        "arn:aws:dynamodb:us-east-1",
        "",
        "arn:aws:dynamodb:us-east-1::table/Music/backup/1",
        "arn:aws:dynamodb:us-east-1:table/Music/backup/123456789012",
        "arn:aws:dynamodb:us-east-1:123456789012:table/Music",
        "arn:aws:dynamodb:us-east-1:123456789012:table/Music/stream/1",
        "arn:aws:dynamodb:us-east-1:999999999999:table/Music/index/x",
    ] {
        let body = json!({ "BackupArn": candidate });
        let err = backup_arn_field(&body, ACCOUNT).unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(_)),
            "{candidate:?}: {err:?}"
        );
    }
}

#[test]
fn missing_arn_field_is_a_validation_error() {
    let err = backup_arn_field(&json!({}), ACCOUNT).unwrap_err();
    assert!(matches!(err, DynamoDbError::ValidationException(m) if m.contains("backupArn")));
}

/// A request dropped while the engine is still reading the backup (the client
/// disconnected) does not cancel the restore: it still reaches the backend.
#[tokio::test]
async fn a_restore_request_dropped_while_the_backup_is_read_still_restores() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Waker};

    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    fake.source_gate = Some(Arc::clone(&gate));
    let fake = Arc::new(fake);

    let after_ran = Arc::new(tokio::sync::Notify::new());
    let notify = Arc::clone(&after_ran);
    let mut request = Box::pin(restore_table_from_backup_detached(
        Arc::clone(&fake),
        Arc::from(ACCOUNT),
        Arc::new(LimitsConfig::default()),
        restore_body(json!({})),
        move |_, target| async move {
            assert_eq!(target, "MusicRestored");
            notify.notify_one();
        },
    ));
    // Poll once so the restore starts, wait until it is blocked reading the
    // backup, then drop the request as a disconnecting client's would be.
    let mut cx = Context::from_waker(Waker::noop());
    assert!(request.as_mut().poll(&mut cx).is_pending());
    for _ in 0..200 {
        if fake.source_reads() == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(fake.source_reads(), 1, "the restore never read the backup");
    drop(request);
    assert!(
        fake.restores().is_empty(),
        "precondition: not registered yet"
    );

    gate.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(5), after_ran.notified())
        .await
        .expect("the dropped request's restore never reached the backend");
    assert_eq!(fake.restores().len(), 1);
}

#[tokio::test]
async fn restore_checks_the_arn_before_the_target_name() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    // "x" is shorter than the 3-character minimum table name.
    let malformed = json!({ "TargetTableName": "x", "BackupArn": "garbage" });
    let msg = validation_message(restore(&fake, malformed).await.unwrap_err());
    assert!(msg.contains("BackupArn"), "{msg}");

    let foreign = json!({ "TargetTableName": "x", "BackupArn": arn("999999999999") });
    assert!(matches!(
        restore(&fake, foreign).await.unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));

    let missing_name = json!({ "BackupArn": arn("999999999999") });
    assert!(matches!(
        restore(&fake, missing_name).await.unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));

    // With a good ARN, the bad name is what is reported.
    let bad_name = json!({ "TargetTableName": "x", "BackupArn": arn(ACCOUNT) });
    let msg = validation_message(restore(&fake, bad_name).await.unwrap_err());
    assert!(!msg.contains("BackupArn"), "{msg}");
    assert_eq!(fake.source_reads(), 0, "no storage read for a bad request");
    assert!(fake.restores().is_empty());
}

// --- overrides ---------------------------------------------------------------

#[test]
fn restore_request_without_overrides_is_accepted() {
    assert!(
        parse_restore_table_overrides(&restore_body(json!({})))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn provisioned_override_with_throughput_is_accepted() {
    let overrides = parse_restore_table_overrides(&json!({
        "BillingModeOverride": "PROVISIONED",
        "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
    }))
    .unwrap();
    validate_override_shape(&overrides).unwrap();
    validate_override_for_source(&overrides, "PAY_PER_REQUEST").unwrap();
    assert_eq!(overrides.billing_mode, Some(BillingMode::Provisioned));
    assert_eq!(overrides.provisioned_throughput, Some(throughput(5, 5)));
}

#[test]
fn provisioned_override_without_throughput_is_refused() {
    let overrides =
        parse_restore_table_overrides(&json!({ "BillingModeOverride": "PROVISIONED" })).unwrap();
    assert!(matches!(
        validate_override_shape(&overrides).unwrap_err(),
        DynamoDbError::ValidationException(_)
    ));
}

#[test]
fn throughput_override_under_pay_per_request_is_refused() {
    let overrides = parse_restore_table_overrides(&json!({
        "BillingModeOverride": "PAY_PER_REQUEST",
        "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
    }))
    .unwrap();
    let msg =
        validation_message(validate_override_for_source(&overrides, "PROVISIONED").unwrap_err());
    assert!(msg.contains(
        "ProvisionedThroughputOverride can only be specified with BillingModeOverride PROVISIONED"
    ));
}

#[test]
fn invalid_provisioned_throughput_override_matches_create_table() {
    let expected = "One or more parameter values were invalid: ReadCapacityUnits and \
                    WriteCapacityUnits must both be greater than or equal to 1 for table";
    for (read, write) in [(0, 5), (5, 0), (-1, 5), (5, -1)] {
        let overrides = parse_restore_table_overrides(&json!({
            "BillingModeOverride": "PROVISIONED",
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": read, "WriteCapacityUnits": write },
        }))
        .unwrap();
        assert_eq!(
            validation_message(validate_override_shape(&overrides).unwrap_err()),
            expected
        );
    }
}

#[test]
fn missing_provisioned_throughput_override_member_matches_create_table() {
    for (member, body) in [
        (
            "ReadCapacityUnits",
            json!({ "BillingModeOverride": "PROVISIONED",
                    "ProvisionedThroughputOverride": { "WriteCapacityUnits": 5 } }),
        ),
        (
            "WriteCapacityUnits",
            json!({ "BillingModeOverride": "PROVISIONED",
                    "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5 } }),
        ),
    ] {
        match parse_restore_table_overrides(&body).unwrap_err() {
            DynamoDbError::SerializationException(m) => {
                assert!(m.contains(&format!("missing field `{member}`")), "{m}");
            }
            other => panic!("expected SerializationException, got {other:?}"),
        }
    }
}

#[test]
fn on_demand_throughput_and_vector_index_overrides_are_refused() {
    for field in ["OnDemandThroughputOverride", "VectorIndexOverride"] {
        let body = restore_body(json!({ field: {} }));
        assert_eq!(
            validation_message(parse_restore_table_overrides(&body).unwrap_err()),
            format!("RestoreTableFromBackup does not support {field} on ExtendDB")
        );
    }
}

#[test]
fn null_override_members_are_absent() {
    let body = restore_body(json!({
        "GlobalSecondaryIndexOverride": null,
        "LocalSecondaryIndexOverride": null,
        "SSESpecificationOverride": null,
        "OnDemandThroughputOverride": null,
        "VectorIndexOverride": null,
        "BillingModeOverride": null,
        "ProvisionedThroughputOverride": null,
    }));
    assert!(parse_restore_table_overrides(&body).unwrap().is_empty());
}

#[test]
fn non_empty_index_overrides_are_refused() {
    for field in [
        "GlobalSecondaryIndexOverride",
        "LocalSecondaryIndexOverride",
    ] {
        let body = restore_body(json!({ field: [{ "IndexName": "g" }] }));
        let msg = validation_message(parse_restore_table_overrides(&body).unwrap_err());
        assert!(msg.contains(field) && msg.contains("non-empty"), "{msg}");

        let body = restore_body(json!({ field: "nope" }));
        assert!(matches!(
            parse_restore_table_overrides(&body).unwrap_err(),
            DynamoDbError::SerializationException(_)
        ));
    }
}

#[tokio::test]
async fn empty_index_overrides_are_no_ops() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    restore(
        &fake,
        restore_body(json!({
            "GlobalSecondaryIndexOverride": [],
            "LocalSecondaryIndexOverride": [],
        })),
    )
    .await
    .unwrap();
    let input = &fake.restores()[0].input;
    assert_eq!(
        input.global_secondary_indexes.as_ref().unwrap()[0].index_name,
        "gsi1"
    );
    assert_eq!(
        input.local_secondary_indexes.as_ref().unwrap()[0].index_name,
        "lsi1"
    );
}

#[tokio::test]
async fn sse_override_replaces_the_recorded_specification() {
    let mut def = definition("PROVISIONED");
    def.sse_specification = Some(json!({ "Enabled": false }));
    let fake = Fake::with_source(source(Some(def)));
    let sse = json!({ "Enabled": true, "SSEType": "KMS", "KMSMasterKeyId": "alias/k" });
    restore(
        &fake,
        restore_body(json!({ "SSESpecificationOverride": sse })),
    )
    .await
    .unwrap();
    assert_eq!(fake.restores()[0].input.sse_specification, Some(sse));

    // A legacy backup takes the override too.
    let fake = Fake::with_source(source(None));
    restore(
        &fake,
        restore_body(json!({ "SSESpecificationOverride": { "Enabled": true } })),
    )
    .await
    .unwrap();
    assert_eq!(
        fake.restores()[0].input.sse_specification,
        Some(json!({ "Enabled": true }))
    );

    let body = restore_body(json!({ "SSESpecificationOverride": "on" }));
    assert!(matches!(
        parse_restore_table_overrides(&body).unwrap_err(),
        DynamoDbError::SerializationException(_)
    ));
}

#[tokio::test]
async fn billing_overrides_shape_the_restore_input() {
    // PROVISIONED -> PAY_PER_REQUEST drops table and GSI throughput.
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    let out = restore(
        &fake,
        restore_body(json!({ "BillingModeOverride": "PAY_PER_REQUEST" })),
    )
    .await
    .unwrap();
    let input = &fake.restores()[0].input;
    assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
    assert!(input.provisioned_throughput.is_none());
    assert!(
        input.global_secondary_indexes.as_ref().unwrap()[0]
            .provisioned_throughput
            .is_none()
    );
    let td = &out["TableDescription"];
    assert_eq!(td["ProvisionedThroughput"]["ReadCapacityUnits"], 0);
    assert_eq!(
        td["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"]["ReadCapacityUnits"],
        0
    );

    // A throughput override on a PROVISIONED backup replaces the table's.
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    restore(
        &fake,
        restore_body(json!({
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": 11, "WriteCapacityUnits": 12 },
        })),
    )
    .await
    .unwrap();
    let input = &fake.restores()[0].input;
    assert_eq!(input.provisioned_throughput, Some(throughput(11, 12)));
    assert_eq!(
        input.global_secondary_indexes.as_ref().unwrap()[0].provisioned_throughput,
        Some(throughput(3, 4))
    );

    // A throughput override on an on-demand backup without a billing override.
    let fake = Fake::with_source(source(Some(definition("PAY_PER_REQUEST"))));
    let err = restore(
        &fake,
        restore_body(json!({
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 },
        })),
    )
    .await
    .unwrap_err();
    assert!(validation_message(err).contains("can only be specified"));
    assert!(fake.restores().is_empty());
}

#[tokio::test]
async fn provisioned_override_on_an_on_demand_gsi_says_what_works() {
    let fake = Fake::with_source(source(Some(definition("PAY_PER_REQUEST"))));
    let err = restore(
        &fake,
        restore_body(json!({
            "BillingModeOverride": "PROVISIONED",
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
        })),
    )
    .await
    .unwrap_err();
    let msg = validation_message(err);
    assert!(msg.contains("index gsi1"), "{msg}");
    assert!(msg.contains("BillingModeOverride PAY_PER_REQUEST"), "{msg}");
    assert!(msg.contains("UpdateTable"), "{msg}");
    assert!(!msg.contains("must be specified"), "{msg}");
    assert!(fake.restores().is_empty());
}

#[tokio::test]
async fn override_shape_is_refused_before_any_storage_read() {
    let fake = Fake::default();
    let err = restore(
        &fake,
        restore_body(json!({ "BillingModeOverride": "PROVISIONED" })),
    )
    .await
    .unwrap_err();
    assert!(validation_message(err).contains("ProvisionedThroughputOverride must"));
    assert_eq!(fake.source_reads(), 0);
}

// --- refusals and validation --------------------------------------------------

#[tokio::test]
async fn multi_part_key_backups_are_refused_in_the_engine() {
    let mut src = source(Some(definition("PROVISIONED")));
    src.key_schema.push(key("pk2", KeyType::Hash));
    let fake = Fake::with_source(src);
    let msg = validation_message(restore(&fake, restore_body(json!({}))).await.unwrap_err());
    assert!(msg.contains("multi-part key"), "{msg}");
    assert!(fake.restores().is_empty());
}

#[tokio::test]
async fn vector_index_backups_are_refused_in_the_engine() {
    let mut def = definition("PAY_PER_REQUEST");
    def.vector_index_names.push("v".to_owned());
    let fake = Fake::with_source(source(Some(def)));
    let msg = validation_message(restore(&fake, restore_body(json!({}))).await.unwrap_err());
    assert!(msg.contains("vector index"), "{msg}");
    assert!(fake.restores().is_empty());

    let mut legacy = source(None);
    legacy.legacy_vector_index_count = 1;
    let fake = Fake::with_source(legacy);
    let msg = validation_message(restore(&fake, restore_body(json!({}))).await.unwrap_err());
    assert!(msg.contains("vector index"), "{msg}");
    assert!(fake.restores().is_empty());
}

#[tokio::test]
async fn a_stored_definition_create_table_would_refuse_is_refused() {
    type Corrupt = fn(&mut BackupRestoreSource);
    let cases: [(&str, Corrupt); 7] = [
        (
            "101 projected attributes split across a GSI and an LSI",
            |s| {
                let d = s.definition.as_mut().unwrap();
                d.global_secondary_indexes[0].projection = include(0..60);
                d.local_secondary_indexes[0].projection = include(60..101);
            },
        ),
        ("GSI on an undefined attribute", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes[0].key_schema[0].attribute_name = "nowhere".to_owned();
        }),
        ("negative GSI throughput", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes[0].provisioned_throughput = Some(throughput(-1, 1));
        }),
        ("GSI throughput with one unit at 0", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes[0].provisioned_throughput = Some(throughput(0, 1));
        }),
        ("negative table throughput", |s| {
            s.definition.as_mut().unwrap().provisioned_throughput = Some(throughput(1, -1));
        }),
        ("too many GSIs", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes = (0..30)
                .map(|i| gsi(&format!("g{i}"), "gpk", Some(throughput(1, 1))))
                .collect();
        }),
        ("too many projected attributes", |s| {
            let d = s.definition.as_mut().unwrap();
            d.global_secondary_indexes[0].projection = Projection {
                projection_type: ProjectionType::Include,
                non_key_attributes: Some((0..101).map(|i| format!("a{i}")).collect()),
            };
        }),
    ];
    for (what, corrupt) in cases {
        let mut src = source(Some(definition("PROVISIONED")));
        corrupt(&mut src);
        let fake = Fake::with_source(src);
        let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
        assert!(
            matches!(err, DynamoDbError::ValidationException(_)),
            "{what}: {err:?}"
        );
        assert!(fake.restores().is_empty(), "{what}: reached storage");
    }
}

fn include(range: std::ops::Range<usize>) -> Projection {
    Projection {
        projection_type: ProjectionType::Include,
        non_key_attributes: Some(range.map(|i| format!("a{i}")).collect()),
    }
}

#[tokio::test]
async fn one_hundred_projected_attributes_are_accepted() {
    let mut src = source(Some(definition("PROVISIONED")));
    let d = src.definition.as_mut().unwrap();
    d.global_secondary_indexes[0].projection = include(0..60);
    d.local_secondary_indexes[0].projection = include(60..100);
    let fake = Fake::with_source(src);
    restore(&fake, restore_body(json!({}))).await.unwrap();
    assert_eq!(fake.restores().len(), 1);
}

/// A PROVISIONED table whose GSI has no throughput (CreateTable accepts one)
/// or 0/0 (what UpdateTable to PROVISIONED leaves on an on-demand table's
/// GSIs) restores, with no override and with a PROVISIONED override; the GSI
/// takes the target table's throughput.
#[tokio::test]
async fn a_table_create_table_accepted_restores() {
    // The CreateTable half: the shape is accepted there.
    let mut created = source(Some(definition("PROVISIONED")))
        .restore_input("Music", &RestoreTableOverrides::default())
        .unwrap();
    created.global_secondary_indexes.as_mut().unwrap()[0].provisioned_throughput = None;
    extenddb_core::validation::validate_create_table(&created, &LimitsConfig::default())
        .expect("CreateTable accepts a PROVISIONED GSI without throughput");

    for recorded in [None, Some(throughput(0, 0))] {
        for (body, expect) in [
            (json!({}), throughput(7, 8)),
            (
                json!({
                    "BillingModeOverride": "PROVISIONED",
                    "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 6 },
                }),
                throughput(5, 6),
            ),
        ] {
            let mut src = source(Some(definition("PROVISIONED")));
            src.definition.as_mut().unwrap().global_secondary_indexes[0].provisioned_throughput =
                recorded.clone();
            let fake = Fake::with_source(src);
            restore(&fake, restore_body(body.clone()))
                .await
                .unwrap_or_else(|e| panic!("{recorded:?} {body}: {e:?}"));
            let input = &fake.restores()[0].input;
            assert_eq!(
                input.global_secondary_indexes.as_ref().unwrap()[0].provisioned_throughput,
                Some(expect),
                "{recorded:?} {body}"
            );
        }
    }
}

#[tokio::test]
async fn a_missing_backup_is_backup_not_found() {
    let fake = Fake::default();
    assert!(matches!(
        restore(&fake, restore_body(json!({}))).await.unwrap_err(),
        DynamoDbError::BackupNotFoundException(_)
    ));
}

// --- the storage request and the response ------------------------------------

#[tokio::test]
async fn the_backend_receives_the_resolved_request() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    restore(&fake, restore_body(json!({}))).await.unwrap();
    let [request] = fake.restores().try_into().unwrap();
    assert_eq!(request.backup_arn, arn(ACCOUNT));
    assert_eq!(request.source_table_arn, source_arn());
    assert!((request.restore_date_time - CREATED).abs() < f64::EPSILON);
    let input = request.input;
    assert_eq!(input.table_name, "MusicRestored");
    assert_eq!(input.key_schema.len(), 2);
    assert_eq!(input.billing_mode, Some(BillingMode::Provisioned));
    assert_eq!(input.provisioned_throughput, Some(throughput(7, 8)));
    assert_eq!(
        input.global_secondary_indexes.unwrap()[0].provisioned_throughput,
        Some(throughput(3, 4))
    );
    assert_eq!(input.local_secondary_indexes.unwrap().len(), 1);
    assert!(input.stream_specification.is_none());
}

#[tokio::test]
async fn a_legacy_backup_restores_with_its_old_defaults() {
    let fake = Fake::with_source(source(None));
    restore(&fake, restore_body(json!({}))).await.unwrap();
    let input = &fake.restores()[0].input;
    assert_eq!(input.provisioned_throughput, Some(throughput(5, 5)));
    assert!(input.global_secondary_indexes.is_none());
    assert!(input.local_secondary_indexes.is_none());
}

#[tokio::test]
async fn the_response_matches_the_service_shape() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    let out = restore(&fake, restore_body(json!({}))).await.unwrap();
    let td = &out["TableDescription"];
    assert_eq!(td["TableStatus"], "CREATING");
    let summary = &td["RestoreSummary"];
    assert_eq!(summary["SourceBackupArn"], arn(ACCOUNT));
    assert_eq!(summary["SourceTableArn"], source_arn());
    assert_eq!(summary["RestoreDateTime"], CREATED);
    assert_eq!(summary["RestoreInProgress"], true);
    let gsi = td["GlobalSecondaryIndexes"][0].as_object().unwrap();
    assert!(!gsi.contains_key("IndexStatus"), "{gsi:?}");
    assert!(gsi.contains_key("IndexName"));
    assert!(
        td["BillingModeSummary"]
            .get("LastUpdateToPayPerRequestDateTime")
            .is_none()
    );
    assert!(
        td["TableThroughputModeSummary"]
            .get("LastUpdateToPayPerRequestDateTime")
            .is_none()
    );
    assert_eq!(
        td["TableThroughputModeSummary"]["TableThroughputMode"],
        "PROVISIONED"
    );
}

#[tokio::test]
async fn a_taken_name_is_table_already_exists_on_restore_only() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    *fake.restore_error.lock().unwrap() =
        Some(StorageError::TableAlreadyExists("MusicRestored".to_owned()));
    let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
    match &err {
        DynamoDbError::TableAlreadyExistsException(m) => {
            assert_eq!(m, "Table already exists: MusicRestored");
        }
        other => panic!("expected TableAlreadyExistsException, got {other:?}"),
    }
    assert_eq!(
        err.full_error_type(),
        "com.amazonaws.dynamodb.v20120810#TableAlreadyExistsException"
    );

    // CreateTable keeps ResourceInUseException.
    assert!(matches!(
        crate::create_table::storage_err_to_dynamo(StorageError::TableAlreadyExists(
            "t".to_owned()
        )),
        DynamoDbError::ResourceInUseException(_)
    ));
}

#[tokio::test]
async fn synchronous_backend_refusals_keep_their_classes() {
    for (storage, check) in [
        (
            StorageError::LimitExceeded("busy".to_owned()),
            (|e: &DynamoDbError| matches!(e, DynamoDbError::LimitExceededException(_)))
                as fn(&DynamoDbError) -> bool,
        ),
        (
            StorageError::Validation(format!("Backup not found: {}", arn(ACCOUNT))),
            |e| matches!(e, DynamoDbError::BackupNotFoundException(_)),
        ),
    ] {
        let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
        *fake.restore_error.lock().unwrap() = Some(storage);
        let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
        assert!(check(&err), "{err:?}");
    }
}

#[test]
fn an_unsupported_feature_is_a_validation_exception() {
    let msg = "restoring a table with vector indexes is not supported by this storage backend";
    assert_eq!(
        validation_message(storage_err_to_dynamo(StorageError::Unsupported(
            msg.to_owned()
        ))),
        msg
    );
}

// --- CreateBackup -------------------------------------------------------------

#[test]
fn create_backup_refuses_a_table_with_vector_indexes() {
    let plain = TableDescription {
        table_name: "T".to_owned(),
        ..Default::default()
    };
    assert!(refuse_backup_of_vector_table(&plain).is_ok());

    let with_vectors: TableDescription = serde_json::from_value(json!({
        "TableName": "T",
        "KeySchema": [],
        "AttributeDefinitions": [],
        "TableStatus": "ACTIVE",
        "CreationDateTime": 0.0,
        "TableSizeBytes": 0,
        "ItemCount": 0,
        "TableArn": "",
        "TableId": "",
        "ProvisionedThroughput": { "ReadCapacityUnits": 0, "WriteCapacityUnits": 0,
                                   "NumberOfDecreasesToday": 0 },
        "DeletionProtectionEnabled": false,
        "VectorIndexes": [{
            "IndexName": "v",
            "VectorAttribute": { "AttributeName": "emb" },
            "Dimensions": 3,
            "DistanceFunction": "COSINE",
            "IndexStatus": "ACTIVE",
            "IndexSizeBytes": 0,
            "ItemCount": 0,
            "IndexArn": ""
        }]
    }))
    .unwrap();
    let msg = validation_message(refuse_backup_of_vector_table(&with_vectors).unwrap_err());
    assert!(msg.starts_with("table T has 1 vector index(es)"), "{msg}");

    // The restore's refusal reads the same after the subject.
    let mut def = definition("PAY_PER_REQUEST");
    def.vector_index_names.push("v".to_owned());
    let restore_msg = validation_message(storage_err_to_dynamo(
        def.ensure_restorable(&arn(ACCOUNT)).unwrap_err(),
    ));
    assert_eq!(
        msg.trim_start_matches("table T"),
        restore_msg.trim_start_matches(&format!("backup {}", arn(ACCOUNT)))
    );
}

// --- DescribeBackup -----------------------------------------------------------

fn backup_description(status: &str, billing: &str) -> BackupDescription {
    BackupDescription {
        backup_details: BackupDetails {
            backup_arn: arn(ACCOUNT),
            backup_name: "b".to_owned(),
            backup_status: status.to_owned(),
            backup_type: "USER".to_owned(),
            backup_size_bytes: 30,
            backup_creation_date_time: CREATED,
        },
        source_table_details: SourceTableDetails {
            table_name: "Music".to_owned(),
            table_id: "tid".to_owned(),
            table_arn: source_arn(),
            billing_mode: Some(billing.to_owned()),
            ..Default::default()
        },
        source_table_feature_details: None,
    }
}

fn fake_with_backup(status: &str, src: BackupRestoreSource) -> Fake {
    let billing = src.billing_mode.clone();
    let fake = Fake::with_source(src);
    fake.descriptions
        .lock()
        .unwrap()
        .insert(arn(ACCOUNT), backup_description(status, &billing));
    fake
}

async fn describe(fake: &Fake) -> Value {
    serde_json::to_value(
        describe_backup(fake, ACCOUNT, REGION, &arn(ACCOUNT))
            .await
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn describe_backup_reports_features_of_an_on_demand_table() {
    let mut def = definition("PAY_PER_REQUEST");
    def.time_to_live = Some(TimeToLiveDescription {
        time_to_live_status: TimeToLiveStatus::Enabled,
        attribute_name: Some("exp".to_owned()),
    });
    def.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewImage),
    });
    def.sse_specification = Some(json!({ "Enabled": true }));
    let fake = fake_with_backup("AVAILABLE", source(Some(def)));
    let v = describe(&fake).await;

    let pt = &v["SourceTableDetails"]["ProvisionedThroughput"];
    assert_eq!(
        pt,
        &json!({ "ReadCapacityUnits": 0, "WriteCapacityUnits": 0 })
    );
    let f = &v["SourceTableFeatureDetails"];
    assert_eq!(f["GlobalSecondaryIndexes"][0]["IndexName"], "gsi1");
    assert_eq!(
        f["GlobalSecondaryIndexes"][0]["KeySchema"][0]["AttributeName"],
        "gpk"
    );
    assert_eq!(
        f["GlobalSecondaryIndexes"][0]["Projection"]["ProjectionType"],
        "ALL"
    );
    assert_eq!(
        f["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"],
        json!({ "ReadCapacityUnits": 0, "WriteCapacityUnits": 0 })
    );
    assert_eq!(f["LocalSecondaryIndexes"][0]["IndexName"], "lsi1");
    assert!(
        f["LocalSecondaryIndexes"][0]
            .get("ProvisionedThroughput")
            .is_none()
    );
    assert_eq!(
        f["TimeToLiveDescription"],
        json!({ "TimeToLiveStatus": "ENABLED", "AttributeName": "exp" })
    );
    assert_eq!(
        f["StreamDescription"],
        json!({ "StreamEnabled": true, "StreamViewType": "NEW_IMAGE" })
    );
    assert_eq!(f["SSEDescription"]["Status"], "ENABLED");
    assert_eq!(
        f["SSEDescription"]["KMSMasterKeyArn"],
        format!("arn:aws:kms:{REGION}:{ACCOUNT}:key/default")
    );
}

#[tokio::test]
async fn describe_backup_reports_provisioned_throughput_and_omits_absent_members() {
    let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
    let v = describe(&fake).await;
    assert_eq!(
        v["SourceTableDetails"]["ProvisionedThroughput"],
        json!({ "ReadCapacityUnits": 7, "WriteCapacityUnits": 8 })
    );
    let f = v["SourceTableFeatureDetails"].as_object().unwrap();
    assert_eq!(
        f["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"],
        json!({ "ReadCapacityUnits": 3, "WriteCapacityUnits": 4 })
    );
    for absent in [
        "TimeToLiveDescription",
        "StreamDescription",
        "SSEDescription",
    ] {
        assert!(!f.contains_key(absent), "{absent}: {f:?}");
    }
}

#[tokio::test]
async fn describe_backup_of_a_legacy_backup_has_no_feature_details() {
    let mut src = source(None);
    src.provisioned_throughput = Some(throughput(2, 3));
    let fake = fake_with_backup("AVAILABLE", src);
    let v = describe(&fake).await;
    assert!(v.get("SourceTableFeatureDetails").is_none(), "{v}");
    assert_eq!(
        v["SourceTableDetails"]["ProvisionedThroughput"],
        json!({ "ReadCapacityUnits": 2, "WriteCapacityUnits": 3 })
    );
}

/// A definition that does not parse or that this build cannot read does not
/// stop DescribeBackup: the backend's BackupDetails are returned without
/// feature details. The errors are the ones every backend returns for those
/// cases, built by `BackupTableDefinition::from_json` itself. A failure to
/// reach the database still propagates: PostgreSQL's `backup_restore_source`
/// reports one (pool timeout, connection reset, failover) as `Transient`
/// with this message shape, and a `Connection` error propagates too; neither
/// is answered as a description without feature details.
#[tokio::test]
async fn describe_backup_survives_a_definition_that_does_not_read() {
    let arn_text = arn(ACCOUNT);
    let undecodable = BackupTableDefinition::from_json(
        json!({ "Version": BACKUP_DEFINITION_VERSION, "BillingMode": 7 }),
        &arn_text,
    )
    .expect_err("a member of the wrong type does not parse");
    assert!(
        matches!(undecodable, StorageError::Internal(_)),
        "{undecodable:?}"
    );
    let unknown_version = BackupTableDefinition::from_json(
        json!({ "Version": BACKUP_DEFINITION_VERSION + 1 }),
        &arn_text,
    )
    .expect_err("an unknown version is refused");
    assert!(
        matches!(unknown_version, StorageError::Unsupported(_)),
        "{unknown_version:?}"
    );
    for err in [undecodable, unknown_version] {
        let what = format!("{err:?}");
        let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
        *fake.source_error.lock().unwrap() = Some(err);
        let desc = describe_backup(&fake, ACCOUNT, REGION, &arn_text)
            .await
            .unwrap_or_else(|e| panic!("{what}: {e:?}"));
        assert_eq!(fake.source_reads(), 1, "{what}");
        assert_eq!(desc.backup_details.backup_status, "AVAILABLE", "{what}");
        assert_eq!(desc.backup_details.backup_arn, arn_text, "{what}");
        assert!(desc.source_table_feature_details.is_none(), "{what}");
        assert!(
            desc.source_table_details.provisioned_throughput.is_some(),
            "{what}: required member"
        );
    }

    for err in [
        StorageError::Transient(
            "Database error: pool timed out while waiting for an open connection".to_owned(),
        ),
        StorageError::Connection("connection refused".to_owned()),
    ] {
        let what = format!("{err:?}");
        let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
        *fake.source_error.lock().unwrap() = Some(err);
        // The backup operations' mapping reports both as a 500
        // (`storage_err_to_dynamo` in backup.rs), which SDKs retry.
        match describe_backup(&fake, ACCOUNT, REGION, &arn_text).await {
            Err(DynamoDbError::InternalServerError(_)) => {}
            other => panic!("{what} must reach the client as an error: {other:?}"),
        }
        assert_eq!(fake.source_reads(), 1, "{what}");
    }
}

#[tokio::test]
async fn describe_backup_of_a_backup_that_is_not_available_uses_the_backend_row() {
    let fake = fake_with_backup("CREATING", source(Some(definition("PROVISIONED"))));
    let v = describe(&fake).await;
    assert_eq!(fake.source_reads(), 0);
    assert!(v.get("SourceTableFeatureDetails").is_none());
    assert_eq!(v["BackupDetails"]["BackupStatus"], "CREATING");
    // Required member, still present.
    assert!(
        v["SourceTableDetails"]
            .get("ProvisionedThroughput")
            .is_some()
    );

    // AVAILABLE in describe, gone by the source read: described without
    // feature details rather than failing.
    let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
    fake.sources.lock().unwrap().clear();
    let v = describe(&fake).await;
    assert_eq!(fake.source_reads(), 1);
    assert!(v.get("SourceTableFeatureDetails").is_none());
}

// --- ListBackups ----------------------------------------------------------------

fn summary(n: u64, millis: u64) -> BackupSummary {
    let id = extenddb_storage::util::format_backup_id(millis, u32::try_from(n).unwrap());
    BackupSummary {
        backup_arn: extenddb_storage::util::backup_arn(REGION, ACCOUNT, "Music", &id),
        backup_name: format!("b{n}"),
        table_name: "Music".to_owned(),
        table_id: "tid".to_owned(),
        table_arn: source_arn(),
        backup_status: "AVAILABLE".to_owned(),
        backup_type: "USER".to_owned(),
        backup_size_bytes: 0,
        backup_creation_date_time: extenddb_storage::util::backup_creation_date_time(&id).unwrap(),
    }
}

fn fake_with_summaries(summaries: Vec<BackupSummary>) -> Fake {
    let fake = Fake::default();
    *fake.summaries.lock().unwrap() = summaries;
    fake
}

async fn list(fake: &Fake, body: Value) -> Result<Value, DynamoDbError> {
    list_backups(fake, ACCOUNT, &body).await
}

fn arns(page: &Value) -> Vec<String> {
    page["BackupSummaries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["BackupArn"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn list_backups_defaults_to_one_hundred() {
    let fake = fake_with_summaries((0..150).map(|n| summary(n, 1_000 + n)).collect());
    let page = list(&fake, json!({})).await.unwrap();
    assert_eq!(arns(&page).len(), 100);
    assert_eq!(
        page["LastEvaluatedBackupArn"].as_str(),
        Some(arns(&page)[99].as_str())
    );
    assert_eq!(page["BackupSummaries"][0]["TableId"], "tid");
}

#[tokio::test]
async fn list_backups_limit_is_bounded() {
    let fake = fake_with_summaries(vec![summary(1, 1)]);
    for bad in [0, -1, 101] {
        let msg = validation_message(list(&fake, json!({ "Limit": bad })).await.unwrap_err());
        assert!(msg.contains("'limit'"), "{msg}");
    }
    assert!(matches!(
        list(&fake, json!({ "Limit": "ten" })).await.unwrap_err(),
        DynamoDbError::SerializationException(_)
    ));
    for ok in [1, 100] {
        list(&fake, json!({ "Limit": ok })).await.unwrap();
    }
}

#[tokio::test]
async fn list_backups_pages_in_a_stable_order_exactly_once() {
    // Out of order, with two backups in the same millisecond.
    let all = vec![
        summary(5, 500),
        summary(1, 100),
        summary(3, 300),
        summary(2, 300),
        summary(4, 400),
    ];
    let mut expected: Vec<BackupSummary> = all.clone();
    expected.sort_by(|a, b| {
        a.backup_creation_date_time
            .total_cmp(&b.backup_creation_date_time)
            .then_with(|| a.backup_arn.cmp(&b.backup_arn))
    });
    let expected: Vec<String> = expected.into_iter().map(|s| s.backup_arn).collect();

    let fake = fake_with_summaries(all);
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut body = json!({ "Limit": 2 });
        if let Some(c) = &cursor {
            body["ExclusiveStartBackupArn"] = json!(c);
        }
        let page = list(&fake, body).await.unwrap();
        pages += 1;
        seen.extend(arns(&page));
        match page.get("LastEvaluatedBackupArn") {
            Some(c) => cursor = Some(c.as_str().unwrap().to_owned()),
            None => break,
        }
        assert!(pages < 10, "paging did not terminate");
    }
    assert_eq!(seen, expected);
    assert_eq!(pages, 3);
}

#[tokio::test]
async fn list_backups_last_page_has_no_cursor() {
    let fake = fake_with_summaries((0..4).map(|n| summary(n, n)).collect());
    let first = list(&fake, json!({ "Limit": 2 })).await.unwrap();
    let cursor = first["LastEvaluatedBackupArn"].as_str().unwrap().to_owned();
    let second = list(
        &fake,
        json!({ "Limit": 2, "ExclusiveStartBackupArn": cursor }),
    )
    .await
    .unwrap();
    assert_eq!(arns(&second).len(), 2);
    assert!(second.get("LastEvaluatedBackupArn").is_none());

    // Exactly the limit, from the start.
    let all = list(&fake, json!({ "Limit": 4 })).await.unwrap();
    assert!(all.get("LastEvaluatedBackupArn").is_none());
    // Empty listing.
    let none = list(&fake_with_summaries(vec![]), json!({})).await.unwrap();
    assert_eq!(none, json!({ "BackupSummaries": [] }));
}

#[tokio::test]
async fn list_backups_resumes_after_a_cursor_whose_backup_was_deleted() {
    let all: Vec<BackupSummary> = (0..5).map(|n| summary(n, 100 * (n + 1))).collect();
    let deleted = all[2].backup_arn.clone();
    let remaining: Vec<BackupSummary> = all
        .iter()
        .filter(|s| s.backup_arn != deleted)
        .cloned()
        .collect();
    let fake = fake_with_summaries(remaining);
    let page = list(&fake, json!({ "ExclusiveStartBackupArn": deleted }))
        .await
        .unwrap();
    assert_eq!(
        arns(&page),
        vec![all[3].backup_arn.clone(), all[4].backup_arn.clone()]
    );

    // A deleted cursor after every remaining backup: an empty last page.
    let fake = fake_with_summaries(all[..2].to_vec());
    let page = list(&fake, json!({ "ExclusiveStartBackupArn": deleted }))
        .await
        .unwrap();
    assert!(arns(&page).is_empty());
    assert!(page.get("LastEvaluatedBackupArn").is_none());
}

#[tokio::test]
async fn list_backups_refuses_a_cursor_it_cannot_place() {
    let fake = fake_with_summaries(vec![summary(1, 1)]);
    // Well formed, ours, not listed, and its id carries no time.
    let legacy = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/1489602797149-x");
    let msg = validation_message(
        list(&fake, json!({ "ExclusiveStartBackupArn": legacy }))
            .await
            .unwrap_err(),
    );
    assert!(msg.contains("ExclusiveStartBackupArn"), "{msg}");

    assert!(matches!(
        list(&fake, json!({ "ExclusiveStartBackupArn": "garbage" }))
            .await
            .unwrap_err(),
        DynamoDbError::ValidationException(_)
    ));
    assert!(matches!(
        list(
            &fake,
            json!({ "ExclusiveStartBackupArn": arn("999999999999") })
        )
        .await
        .unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));
}

#[tokio::test]
async fn list_backups_passes_the_table_filter_through() {
    let fake = fake_with_summaries(vec![]);
    list(&fake, json!({ "TableName": "Music" })).await.unwrap();
    list(&fake, json!({})).await.unwrap();
    assert_eq!(
        *fake.listed_for.lock().unwrap(),
        vec![Some("Music".to_owned()), None]
    );
}

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
    KeyType, LsiInput, OnDemandThroughput, Projection, ProjectionType, ProvisionedThroughput,
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

    /// Removes the backup and answers its description as the backends do:
    /// the row's status (not yet `DELETED`), `SourceTableDetails` without
    /// `ProvisionedThroughput`, no feature details.
    fn delete_backup(
        &self,
        _account_id: &str,
        backup_arn: &str,
    ) -> BoxedFuture<'_, Result<BackupDescription, StorageError>> {
        let found = self.descriptions.lock().unwrap().remove(backup_arn);
        self.sources.lock().unwrap().remove(backup_arn);
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            found.ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))
        })
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
                    warm_throughput: None,
                    warm_throughput_record: None,
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
        warm_throughput: None,
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
    restore_table_from_backup(fake, ACCOUNT, REGION, &LimitsConfig::default(), &body)
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

/// The ARN the engine looks a `BackupArn` up under first: the one this
/// deployment issues for the backup named.
fn arn_field(body: &Value) -> Result<String, DynamoDbError> {
    backup_arn_field(body, ACCOUNT, REGION).map(|a| a.canonical().to_owned())
}

/// The spellings of an ARN as `backup_arn_field` resolves them for `arn`.
fn own(arn: &str) -> BackupArnSpellings {
    backup_arn_field(&json!({ "BackupArn": arn }), ACCOUNT, REGION).unwrap()
}

fn limits() -> LimitsConfig {
    LimitsConfig::default()
}

#[test]
fn own_account_arn_is_accepted() {
    let body = json!({ "BackupArn": arn(ACCOUNT) });
    assert_eq!(arn_field(&body).unwrap(), arn(ACCOUNT));
}

#[test]
fn other_account_arn_is_denied() {
    let body = json!({ "BackupArn": arn("999999999999") });
    let err = arn_field(&body).unwrap_err();
    assert!(
        matches!(err, DynamoDbError::AccessDeniedException(_)),
        "{err:?}"
    );
}

#[test]
fn account_prefix_does_not_match() {
    let body = json!({ "BackupArn": arn("12345678901") });
    assert!(matches!(
        arn_field(&body).unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));
}

/// Measured on DynamoDB: the shapes the service refuses as `ValidationException`
/// with `Invalid Request: BackupArn is not valid` (not an ARN, oversize,
/// control characters, a slash or `..` in the region) or `Invalid Backup ARN`
/// (an id suffix of the wrong length or not hex, a wildcard id), on every
/// member that carries a backup ARN. Which text goes with which input was
/// not captured, so the split is the parser's (see `BackupArnError`).
#[tokio::test]
async fn malformed_arn_is_a_validation_exception_with_the_services_text() {
    const NOT_VALID: &str = "Invalid Request: BackupArn is not valid";
    const INVALID_ID: &str = "Invalid Backup ARN";
    let id = "01791976431438-73d8d5bc";
    let long_table = "x".repeat(1000);
    let cases: Vec<(String, &str)> = vec![
        ("not-an-arn".to_owned(), NOT_VALID),
        ("arn:aws:dynamodb".to_owned(), NOT_VALID),
        (format!("arn:aws:dynamodb:{REGION}"), NOT_VALID),
        (String::new(), NOT_VALID),
        (
            format!("arn:aws:dynamodb:{REGION}::table/Music/backup/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:table/Music/backup/{ACCOUNT}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/stream/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:999999999999:table/Music/index/x"),
            NOT_VALID,
        ),
        // Oversize (the model's max is 1024).
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{long_table}/backup/{id}"),
            NOT_VALID,
        ),
        // Control characters, inside and at the end (a NUL byte was a 500 on
        // PostgreSQL before the check).
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/{id}\u{0}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Mu\u{1}sic/backup/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/{id}\n\u{7}"),
            NOT_VALID,
        ),
        // A slash or `..` in the region.
        (
            format!("arn:aws:dynamodb:us/east-1:{ACCOUNT}:table/Music/backup/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:..:{ACCOUNT}:table/Music/backup/{id}"),
            NOT_VALID,
        ),
        (
            format!("arn:aws:dynamodb:us-east-1/..:{ACCOUNT}:table/Music/backup/{id}"),
            NOT_VALID,
        ),
        // The id: wrong length, not hex, a wildcard.
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/1"),
            INVALID_ID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/*"),
            INVALID_ID,
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-*"),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5b"
            ),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bcd"
            ),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73D8D5BC"
            ),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bg"
            ),
            INVALID_ID,
        ),
        (
            format!(
                "arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/0179197643143x-73d8d5bc"
            ),
            INVALID_ID,
        ),
    ];
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    for (candidate, expected) in &cases {
        for (member, result) in [
            ("BackupArn", arn_field(&json!({ "BackupArn": candidate }))),
            (
                "RestoreTableFromBackup",
                restore(&fake, restore_body(json!({ "BackupArn": candidate })))
                    .await
                    .map(|_| String::new()),
            ),
            (
                "ExclusiveStartBackupArn",
                list(&fake, json!({ "ExclusiveStartBackupArn": candidate }))
                    .await
                    .map(|_| String::new()),
            ),
        ] {
            let msg = validation_message(result.unwrap_err());
            assert_eq!(msg, *expected, "{member} {candidate:?}");
        }
    }
    assert_eq!(fake.source_reads(), 0);
    assert!(fake.restores().is_empty());
}

/// Measured on DynamoDB: the service trims a trailing newline and resolves an ARN
/// naming another region of the caller's account. The engine hands the
/// backend the ARN it issued: trimmed, in its own region.
#[tokio::test]
async fn whitespace_is_trimmed_and_another_region_resolves() {
    let ours = arn(ACCOUNT);
    for candidate in [
        format!("{ours}\n"),
        format!("  {ours}\t\r\n"),
        format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc"),
        format!(
            "arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc\n"
        ),
    ] {
        assert_eq!(
            arn_field(&json!({ "BackupArn": candidate })).unwrap(),
            ours,
            "{candidate:?}"
        );
        let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
        restore(&fake, restore_body(json!({ "BackupArn": candidate })))
            .await
            .unwrap_or_else(|e| panic!("{candidate:?}: {e:?}"));
        assert_eq!(fake.restores()[0].backup_arn, ours, "{candidate:?}");
    }
    // Another region of another account is still that account's.
    let foreign =
        "arn:aws:dynamodb:eu-west-1:999999999999:table/Music/backup/01791976431438-73d8d5bc";
    assert!(matches!(
        arn_field(&json!({ "BackupArn": foreign })).unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));
    // Whitespace inside is not trimmed.
    let inside =
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Mu sic/backup/01791976431438-73d8d5bc");
    assert_eq!(
        validation_message(arn_field(&json!({ "BackupArn": inside })).unwrap_err()),
        "Invalid Request: BackupArn is not valid"
    );
}

/// A backup created under an earlier `server.region` is stored with that
/// region in its ARN, which `ListBackups` reports as is. `DescribeBackup`,
/// `DeleteBackup`, and `RestoreTableFromBackup` look the ARN up rebuilt in
/// the server's region first and, when that is not found, as written: the
/// stored backup stays reachable by the ARN the server reports for it, and
/// the engine acts on whichever spelling resolved.
#[tokio::test]
async fn a_backup_stored_under_another_region_resolves_as_written() {
    let stored =
        format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc");
    let ours = arn(ACCOUNT);
    let spellings = own(&stored);
    assert_eq!(spellings.canonical(), ours);
    assert_eq!(spellings.as_written.as_deref(), Some(stored.as_str()));
    // The server's own spelling has no second spelling to try.
    assert_eq!(own(&ours).as_written, None);
    assert_eq!(own(&format!(" {ours}\n")).as_written, None, "trimmed first");

    let stored_source = || {
        let mut src = source(Some(definition("PROVISIONED")));
        src.backup_arn.clone_from(&stored);
        src
    };
    let with_stored = || {
        let fake = Fake::with_source(stored_source());
        fake.descriptions.lock().unwrap().insert(
            stored.clone(),
            backup_description("AVAILABLE", "PROVISIONED"),
        );
        fake
    };

    // DescribeBackup: canonical not found, as written found.
    let fake = with_stored();
    let desc = describe_backup(&fake, ACCOUNT, REGION, &spellings)
        .await
        .unwrap();
    assert_eq!(desc.backup_details.backup_status, "AVAILABLE");
    assert!(
        desc.source_table_feature_details.is_some(),
        "source read under the stored ARN"
    );
    assert_eq!(fake.source_reads(), 1);

    // RestoreTableFromBackup: registered against the stored spelling.
    let fake = with_stored();
    restore(&fake, restore_body(json!({ "BackupArn": stored })))
        .await
        .unwrap();
    assert_eq!(fake.restores()[0].backup_arn, stored);
    assert_eq!(
        fake.source_reads(),
        2,
        "canonical (not found), then as written"
    );

    // DeleteBackup: the full description, and the stored row is gone.
    let fake = with_stored();
    let deleted = delete_backup(&fake, ACCOUNT, REGION, &spellings)
        .await
        .unwrap();
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    assert!(deleted.source_table_feature_details.is_some());
    assert!(fake.descriptions.lock().unwrap().is_empty());
    assert!(fake.sources.lock().unwrap().is_empty());

    // The canonical spelling wins when both exist: the backend is asked for
    // it alone (one read), and the stored other-region row is untouched.
    let fake = with_stored();
    fake.sources
        .lock()
        .unwrap()
        .insert(ours.clone(), source(Some(definition("PAY_PER_REQUEST"))));
    fake.descriptions.lock().unwrap().insert(
        ours.clone(),
        backup_description("AVAILABLE", "PAY_PER_REQUEST"),
    );
    restore(&fake, restore_body(json!({ "BackupArn": stored })))
        .await
        .unwrap();
    assert_eq!(fake.restores()[0].backup_arn, ours);
    assert_eq!(fake.source_reads(), 1);
    let deleted = delete_backup(&fake, ACCOUNT, REGION, &spellings)
        .await
        .unwrap();
    assert_eq!(deleted.backup_details.backup_status, "DELETED");
    assert!(fake.descriptions.lock().unwrap().contains_key(&stored));
    assert!(!fake.descriptions.lock().unwrap().contains_key(&ours));

    // Neither spelling: BackupNotFoundException from the backend, after both
    // were tried; the server's own spelling is tried once.
    let fake = Fake::default();
    for (body, reads) in [(stored.clone(), 2), (ours.clone(), 1)] {
        let before = fake.source_reads();
        let err = restore(&fake, restore_body(json!({ "BackupArn": body })))
            .await
            .unwrap_err();
        assert!(
            matches!(err, DynamoDbError::BackupNotFoundException(_)),
            "{err:?}"
        );
        assert_eq!(fake.source_reads() - before, reads);
        assert!(matches!(
            describe_backup(&fake, ACCOUNT, REGION, &own(&body))
                .await
                .unwrap_err(),
            DynamoDbError::BackupNotFoundException(_)
        ));
        assert!(matches!(
            delete_backup(&fake, ACCOUNT, REGION, &own(&body))
                .await
                .unwrap_err(),
            DynamoDbError::BackupNotFoundException(_)
        ));
    }

    // An error other than not-found on the canonical spelling is not a
    // reason to try the other one.
    let fake = with_stored();
    *fake.source_error.lock().unwrap() =
        Some(StorageError::Connection("connection refused".to_owned()));
    let err = restore(&fake, restore_body(json!({ "BackupArn": stored })))
        .await
        .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::InternalServerError(_)),
        "{err:?}"
    );
    assert_eq!(fake.source_reads(), 1);
    assert!(fake.restores().is_empty());
}

/// An ARN in another partition names nothing here: `ValidationException`
/// (the service's class for one naming the caller's account), before the
/// account is compared, on every member that carries a backup ARN.
#[tokio::test]
async fn an_arn_in_another_partition_is_a_validation_exception() {
    let ours = arn(ACCOUNT);
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    fake.descriptions
        .lock()
        .unwrap()
        .insert(ours.clone(), backup_description("AVAILABLE", "PROVISIONED"));
    *fake.summaries.lock().unwrap() = vec![summary(1, 1)];
    for partition in ["aws-cn", "aws-us-gov", "aws-iso", "AWS", "x"] {
        for account in [ACCOUNT, "999999999999"] {
            let candidate = format!(
                "arn:{partition}:dynamodb:{REGION}:{account}:table/Music/backup/01791976431438-73d8d5bc"
            );
            let err = arn_field(&json!({ "BackupArn": candidate })).unwrap_err();
            assert_eq!(
                validation_message(err),
                "Invalid Request: BackupArn is not valid"
            );
            let err = restore(&fake, restore_body(json!({ "BackupArn": candidate })))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DynamoDbError::ValidationException(_)),
                "{err:?}"
            );
            let err = list(&fake, json!({ "ExclusiveStartBackupArn": candidate }))
                .await
                .unwrap_err();
            assert!(
                matches!(err, DynamoDbError::ValidationException(_)),
                "{err:?}"
            );
        }
    }
    assert_eq!(fake.source_reads(), 0);
    assert!(fake.restores().is_empty());
    // Liveness: the same ARN in this server's partition resolves.
    assert_eq!(
        ours,
        extenddb_storage::util::backup_arn(REGION, ACCOUNT, "Music", "01791976431438-73d8d5bc")
    );
    assert_eq!(arn_field(&json!({ "BackupArn": ours })).unwrap(), ours);
    restore(&fake, restore_body(json!({}))).await.unwrap();
    assert_eq!(fake.restores().len(), 1);
}

#[test]
fn missing_arn_field_is_a_validation_error() {
    let err = arn_field(&json!({})).unwrap_err();
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
        Arc::from(REGION),
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
fn vector_index_override_is_refused() {
    let body = restore_body(json!({ "VectorIndexOverride": {} }));
    assert_eq!(
        validation_message(parse_restore_table_overrides(&body).unwrap_err()),
        "RestoreTableFromBackup does not support VectorIndexOverride on ExtendDB"
    );
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
fn index_and_on_demand_overrides_of_the_wrong_type_are_serialization_errors() {
    for (field, value) in [
        ("GlobalSecondaryIndexOverride", json!("nope")),
        ("LocalSecondaryIndexOverride", json!({})),
        ("OnDemandThroughputOverride", json!([1])),
        // An entry without its required members.
        (
            "GlobalSecondaryIndexOverride",
            json!([{ "IndexName": "gsi1" }]),
        ),
        (
            "LocalSecondaryIndexOverride",
            json!([{ "IndexName": "lsi1" }]),
        ),
        (
            "OnDemandThroughputOverride",
            json!({ "MaxReadRequestUnits": "x" }),
        ),
    ] {
        let body = restore_body(json!({ field: value }));
        assert!(
            matches!(
                overrides::IndexOverrides::parse(&body).unwrap_err(),
                DynamoDbError::SerializationException(_)
            ),
            "{field} {value}"
        );
    }
}

#[tokio::test]
async fn a_malformed_index_override_is_refused_before_any_storage_read() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    let err = restore(
        &fake,
        restore_body(json!({ "GlobalSecondaryIndexOverride": [{ "IndexName": "gsi1" }] })),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, DynamoDbError::SerializationException(_)),
        "{err:?}"
    );
    assert_eq!(fake.source_reads(), 0);
}

/// The backup's GSI and LSI as a client would send them back: the shape
/// `definition` records, in the wire's own form.
fn gsi_entry(pt: Option<(i64, i64)>) -> Value {
    let mut entry = json!({
        "IndexName": "gsi1",
        "KeySchema": [{ "AttributeName": "gpk", "KeyType": "HASH" }],
        "Projection": { "ProjectionType": "ALL" },
    });
    if let Some((r, w)) = pt {
        entry["ProvisionedThroughput"] = json!({ "ReadCapacityUnits": r, "WriteCapacityUnits": w });
    }
    entry
}

fn lsi_entry() -> Value {
    json!({
        "IndexName": "lsi1",
        "KeySchema": [
            { "AttributeName": "pk", "KeyType": "HASH" },
            { "AttributeName": "lsk", "KeyType": "RANGE" },
        ],
        "Projection": { "ProjectionType": "KEYS_ONLY" },
    })
}

/// The target input of a restore of `src` with `body`'s overrides, or the
/// engine's refusal.
async fn restored_input(
    src: BackupRestoreSource,
    body: Value,
) -> Result<CreateTableInput, DynamoDbError> {
    let fake = Fake::with_source(src);
    restore(&fake, restore_body(body)).await?;
    Ok(fake.restores()[0].input.clone())
}

/// The attribute names `input` defines.
fn attr_names(input: &CreateTableInput) -> Vec<&str> {
    input
        .attribute_definitions
        .iter()
        .map(|a| a.attribute_name.as_str())
        .collect()
}

fn gsi_names(input: &CreateTableInput) -> Vec<&str> {
    input
        .global_secondary_indexes
        .iter()
        .flatten()
        .map(|g| g.index_name.as_str())
        .collect()
}

fn gsi_throughputs(input: &CreateTableInput) -> Vec<Option<(i64, i64)>> {
    input
        .global_secondary_indexes
        .iter()
        .flatten()
        .map(|g| {
            g.provisioned_throughput
                .as_ref()
                .map(|t| (t.read_capacity_units, t.write_capacity_units))
        })
        .collect()
}

/// A backup with two GSIs, the second with an INCLUDE projection.
fn two_gsis(billing: &str) -> BackupTableDefinition {
    let mut def = definition(billing);
    let mut second = gsi(
        "gsi2",
        "gpk",
        (billing == "PROVISIONED").then(|| throughput(1, 2)),
    );
    second.projection = Projection {
        projection_type: ProjectionType::Include,
        non_key_attributes: Some(vec!["a".to_owned(), "b".to_owned()]),
    };
    def.global_secondary_indexes.push(second);
    def
}

fn gsi2_entry(nka: Value, pt: Option<(i64, i64)>) -> Value {
    let mut entry = json!({
        "IndexName": "gsi2",
        "KeySchema": [{ "AttributeName": "gpk", "KeyType": "HASH" }],
        "Projection": { "ProjectionType": "INCLUDE", "NonKeyAttributes": nka },
    });
    if let Some((r, w)) = pt {
        entry["ProvisionedThroughput"] = json!({ "ReadCapacityUnits": r, "WriteCapacityUnits": w });
    }
    entry
}

/// Measured on DynamoDB: an empty index override list
/// restores no index of that kind. The attribute definitions only those
/// indexes used go with them, or CreateTable's validation would refuse the
/// input.
#[tokio::test]
async fn an_empty_index_override_list_restores_no_indexes_of_that_kind() {
    for def in [definition("PROVISIONED"), definition("PAY_PER_REQUEST")] {
        let input = restored_input(
            source(Some(def.clone())),
            json!({ "GlobalSecondaryIndexOverride": [] }),
        )
        .await
        .unwrap();
        assert!(input.global_secondary_indexes.is_none(), "{input:?}");
        assert_eq!(
            input.local_secondary_indexes.as_ref().map(Vec::len),
            Some(1)
        );
        assert_eq!(attr_names(&input), ["pk", "sk", "lsk"]);

        let input = restored_input(
            source(Some(def.clone())),
            json!({ "LocalSecondaryIndexOverride": [] }),
        )
        .await
        .unwrap();
        assert!(input.local_secondary_indexes.is_none());
        assert_eq!(gsi_names(&input), ["gsi1"]);
        assert_eq!(attr_names(&input), ["pk", "sk", "gpk"]);

        let input = restored_input(
            source(Some(def)),
            json!({ "GlobalSecondaryIndexOverride": [], "LocalSecondaryIndexOverride": [] }),
        )
        .await
        .unwrap();
        assert!(input.global_secondary_indexes.is_none());
        assert!(input.local_secondary_indexes.is_none());
        assert_eq!(attr_names(&input), ["pk", "sk"]);
        extenddb_core::validation::validate_create_table(&input, &LimitsConfig::default())
            .expect("CreateTable accepts the pruned input");
    }
    // Also with a billing override to PROVISIONED on an on-demand backup:
    // with no GSIs left there is nothing that needs throughput.
    let input = restored_input(
        source(Some(definition("PAY_PER_REQUEST"))),
        json!({
            "BillingModeOverride": "PROVISIONED",
            "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 6 },
            "GlobalSecondaryIndexOverride": [],
        }),
    )
    .await
    .unwrap();
    assert!(input.global_secondary_indexes.is_none());
    assert_eq!(input.provisioned_throughput, Some(throughput(5, 6)));
}

/// Measured on DynamoDB: the service applies a changed
/// index list. A subset restores those indexes, a changed GSI throughput is
/// applied, the list's order is the restored order, and a list that
/// describes the backup's indexes unchanged restores the same table as no
/// list at all.
#[tokio::test]
async fn index_override_lists_are_applied() {
    let plain = restored_input(source(Some(definition("PROVISIONED"))), json!({}))
        .await
        .unwrap();
    assert_eq!(gsi_throughputs(&plain), [Some((3, 4))]);
    assert_eq!(attr_names(&plain), ["pk", "sk", "gpk", "lsk"]);

    // Unchanged, as the console sends it (the backup's throughput).
    let same = restored_input(
        source(Some(definition("PROVISIONED"))),
        json!({
            "GlobalSecondaryIndexOverride": [gsi_entry(Some((3, 4)))],
            "LocalSecondaryIndexOverride": [lsi_entry()],
        }),
    )
    .await
    .unwrap();
    assert_eq!(same, plain);

    // A changed GSI throughput.
    let changed = restored_input(
        source(Some(definition("PROVISIONED"))),
        json!({ "GlobalSecondaryIndexOverride": [gsi_entry(Some((30, 40)))] }),
    )
    .await
    .unwrap();
    assert_eq!(gsi_throughputs(&changed), [Some((30, 40))]);
    assert_eq!(gsi_names(&changed), ["gsi1"]);
    assert_eq!(attr_names(&changed), attr_names(&plain));

    // A subset: the backup has two GSIs, the list names one. The other
    // index's attribute definition goes with it.
    let subset = restored_input(
        source(Some(two_gsis("PROVISIONED"))),
        json!({ "GlobalSecondaryIndexOverride": [gsi2_entry(json!(["b", "a"]), Some((1, 2)))] }),
    )
    .await
    .unwrap();
    assert_eq!(gsi_names(&subset), ["gsi2"]);
    assert_eq!(gsi_throughputs(&subset), [Some((1, 2))]);
    assert_eq!(
        subset.global_secondary_indexes.as_ref().unwrap()[0]
            .projection
            .non_key_attributes,
        Some(vec!["a".to_owned(), "b".to_owned()]),
        "the backup's projection is restored, not the entry's spelling of it"
    );
    assert_eq!(attr_names(&subset), ["pk", "sk", "gpk", "lsk"]);
    extenddb_core::validation::validate_create_table(&subset, &LimitsConfig::default()).unwrap();

    // The list's order is the restored order.
    let reordered = restored_input(
        source(Some(two_gsis("PROVISIONED"))),
        json!({ "GlobalSecondaryIndexOverride": [
            gsi2_entry(json!(["a", "b"]), Some((1, 2))),
            gsi_entry(Some((3, 4))),
        ] }),
    )
    .await
    .unwrap();
    assert_eq!(gsi_names(&reordered), ["gsi2", "gsi1"]);

    // An LSI subset of one (the only one) and an on-demand backup's GSIs,
    // listed without throughput as DescribeBackup's {0, 0} cannot be sent.
    let on_demand = restored_input(source(Some(two_gsis("PAY_PER_REQUEST"))), json!({}))
        .await
        .unwrap();
    let listed = restored_input(
        source(Some(two_gsis("PAY_PER_REQUEST"))),
        json!({
            "GlobalSecondaryIndexOverride": [gsi_entry(None), gsi2_entry(json!(["a", "b"]), None)],
            "LocalSecondaryIndexOverride": [lsi_entry()],
        }),
    )
    .await
    .unwrap();
    assert_eq!(listed, on_demand);
}

/// The DescribeBackup echo (a console or SDK restore flow) restores exactly
/// the table the restore creates without it, for both billing modes; sent
/// with the `{0, 0}` DescribeBackup reports for an on-demand backup's GSI it
/// is refused as CreateTable refuses `{0, 0}`.
#[tokio::test]
async fn index_overrides_echoing_describe_backup_restore_the_same_table() {
    for def in [definition("PAY_PER_REQUEST"), definition("PROVISIONED")] {
        let on_demand = def.billing_mode == "PAY_PER_REQUEST";
        let fake = fake_with_backup("AVAILABLE", source(Some(def.clone())));
        let features = describe(&fake).await["SourceTableFeatureDetails"].clone();
        let mut gsis = features["GlobalSecondaryIndexes"].clone();
        let lsis = features["LocalSecondaryIndexes"].clone();
        assert!(
            gsis.as_array().is_some_and(|g| g.len() == 1),
            "precondition, DescribeBackup reports the GSI: {features}"
        );
        if on_demand {
            assert_eq!(
                gsis[0]["ProvisionedThroughput"],
                json!({ "ReadCapacityUnits": 0, "WriteCapacityUnits": 0 }),
                "precondition: an on-demand backup's GSI is reported as {{0, 0}}"
            );
            let zero_echo = restored_input(
                source(Some(def.clone())),
                json!({ "GlobalSecondaryIndexOverride": gsis.clone() }),
            )
            .await
            .unwrap_err();
            let msg = validation_message(zero_echo);
            assert!(
                msg.contains("gsi1") && msg.contains("should not be specified"),
                "{msg}"
            );
            for g in gsis.as_array_mut().unwrap() {
                g.as_object_mut().unwrap().remove("ProvisionedThroughput");
            }
        }
        let plain = restored_input(source(Some(def.clone())), json!({}))
            .await
            .unwrap();
        let echoed = restored_input(
            source(Some(def)),
            json!({
                "GlobalSecondaryIndexOverride": gsis,
                "LocalSecondaryIndexOverride": lsis,
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("the DescribeBackup echo was refused: {e:?}"));
        assert_eq!(echoed, plain);
    }
}

/// Measured on DynamoDB: refused by the service and by ExtendDB: an unknown or
/// changed index definition, a throughput member contradicting the billing
/// mode, `{0, 0}`. ExtendDB's own: a member it cannot apply.
#[tokio::test]
async fn index_overrides_the_service_refuses_are_refused() {
    let entry_with = |key: &str, value: Value| {
        let mut e = gsi_entry(Some((3, 4)));
        e[key] = value;
        e
    };
    let cases: [(&str, Value, &str); 13] = [
        (
            "an index the backup does not have",
            json!({ "LocalSecondaryIndexOverride": [], "GlobalSecondaryIndexOverride": [
                entry_with("IndexName", json!("other")),
            ] }),
            "global secondary index other is not in the backup",
        ),
        (
            "listed twice",
            json!({ "GlobalSecondaryIndexOverride": [
                gsi_entry(Some((3, 4))), gsi_entry(Some((3, 4))),
            ] }),
            "global secondary index gsi1 is listed more than once",
        ),
        (
            "key schema",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "KeySchema", json!([{ "AttributeName": "pk", "KeyType": "HASH" }]),
            )] }),
            "index gsi1: KeySchema differs from the backup's",
        ),
        (
            "key schema order",
            json!({ "LocalSecondaryIndexOverride": [{
                "IndexName": "lsi1",
                "KeySchema": [
                    { "AttributeName": "lsk", "KeyType": "RANGE" },
                    { "AttributeName": "pk", "KeyType": "HASH" },
                ],
                "Projection": { "ProjectionType": "KEYS_ONLY" },
            }] }),
            "index lsi1: KeySchema differs from the backup's",
        ),
        (
            "projection type",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "Projection", json!({ "ProjectionType": "KEYS_ONLY" }),
            )] }),
            "index gsi1: Projection differs from the backup's",
        ),
        (
            "projected attributes",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "Projection", json!({ "ProjectionType": "ALL", "NonKeyAttributes": ["x"] }),
            )] }),
            "index gsi1: Projection differs from the backup's",
        ),
        (
            "throughput on an on-demand restore",
            json!({
                "BillingModeOverride": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexOverride": [gsi_entry(Some((3, 4)))],
            }),
            "ProvisionedThroughput should not be specified for index: gsi1",
        ),
        (
            "{0, 0}",
            json!({ "GlobalSecondaryIndexOverride": [gsi_entry(Some((0, 0)))] }),
            "ReadCapacityUnits and WriteCapacityUnits must both be greater than or equal to 1",
        ),
        (
            "an index on-demand throughput on a PROVISIONED restore",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "OnDemandThroughput", json!({ "MaxReadRequestUnits": 10 }),
            )] }),
            "index gsi1: OnDemandThroughput can only be specified when the restored table's \
             BillingMode is PAY_PER_REQUEST",
        ),
        (
            "a warm throughput on a GSI entry",
            json!({ "GlobalSecondaryIndexOverride": [entry_with(
                "WarmThroughput", json!({ "ReadUnitsPerSecond": 12000 }),
            )] }),
            // Row S6d, whole: the service's own sentence.
            "One or more parameter values were invalid: WarmThroughput is not supported on \
             Global Secondary Indexes via RestoreTableFromBackup",
        ),
        (
            "an LSI member ExtendDB cannot apply",
            json!({ "LocalSecondaryIndexOverride": [{
                "IndexName": "lsi1",
                "KeySchema": [
                    { "AttributeName": "pk", "KeyType": "HASH" },
                    { "AttributeName": "lsk", "KeyType": "RANGE" },
                ],
                "Projection": { "ProjectionType": "KEYS_ONLY" },
                "ProvisionedThroughput": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 },
            }] }),
            "index lsi1: member ProvisionedThroughput is not supported",
        ),
        (
            "an LSI the backup does not have",
            json!({ "LocalSecondaryIndexOverride": [{
                "IndexName": "lsi2",
                "KeySchema": [
                    { "AttributeName": "pk", "KeyType": "HASH" },
                    { "AttributeName": "lsk", "KeyType": "RANGE" },
                ],
                "Projection": { "ProjectionType": "KEYS_ONLY" },
            }] }),
            "local secondary index lsi2 is not in the backup",
        ),
        (
            "an LSI listed twice",
            json!({ "LocalSecondaryIndexOverride": [lsi_entry(), lsi_entry()] }),
            "local secondary index lsi1 is listed more than once",
        ),
    ];
    for (what, body, expect) in cases {
        let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
        let msg = validation_message(restore(&fake, restore_body(body)).await.unwrap_err());
        assert!(msg.contains(expect), "{what}: {msg}");
        assert!(fake.restores().is_empty(), "{what}: reached storage");
    }

    // A legacy backup restores no index, so any listed one is not in it; an
    // empty list changes nothing.
    let fake = Fake::with_source(source(None));
    let msg = validation_message(
        restore(
            &fake,
            restore_body(json!({ "GlobalSecondaryIndexOverride": [gsi_entry(Some((1, 1)))] })),
        )
        .await
        .unwrap_err(),
    );
    assert!(
        msg.ends_with("global secondary index gsi1 is not in the backup"),
        "{msg}"
    );
    let plain = restored_input(source(None), json!({})).await.unwrap();
    let empty = restored_input(
        source(None),
        json!({ "GlobalSecondaryIndexOverride": [], "LocalSecondaryIndexOverride": [] }),
    )
    .await
    .unwrap();
    assert_eq!(empty, plain);
}

/// Measured on DynamoDB: the two requests the service refuses and ExtendDB
/// accepted: a GSI entry without throughput on a PROVISIONED restore, and
/// `BillingModeOverride` `PROVISIONED` with no GSI list on a backup with
/// GSIs, both with the service's text. The service's text conditions on the
/// override being present, so the rule applies whatever the backup's billing
/// mode and whatever throughput it recorded for the index (the PROVISIONED
/// backup and stale-throughput cases were not measured). The
/// on-demand-to-PROVISIONED route works when the list supplies each index's
/// throughput.
#[tokio::test]
async fn a_provisioned_restore_needs_each_gsis_throughput_overridden() {
    let expected = "Invalid Request: Must override ProvisionedThroughput if BillingMode is \
                    overridden to PROVISIONED for index gsi1";
    let provisioned = json!({
        "BillingModeOverride": "PROVISIONED",
        "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
    });
    let with = |extra: Value| {
        let mut body = provisioned.clone();
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        body
    };
    // An on-demand backup whose GSI still carries a throughput from before a
    // billing switch (what UpdateTable to PAY_PER_REQUEST leaves behind).
    let mut stale = definition("PAY_PER_REQUEST");
    stale.global_secondary_indexes[0].provisioned_throughput = Some(throughput(3, 4));
    let refused: [(&str, BackupTableDefinition, Value); 6] = [
        (
            "no GSI list, on-demand backup",
            definition("PAY_PER_REQUEST"),
            provisioned.clone(),
        ),
        (
            "no GSI list, PROVISIONED backup (the override, not the recorded throughput, decides)",
            definition("PROVISIONED"),
            provisioned.clone(),
        ),
        (
            "no GSI list, on-demand backup with a stale recorded throughput",
            stale,
            provisioned.clone(),
        ),
        (
            "a GSI entry without throughput, on-demand backup",
            definition("PAY_PER_REQUEST"),
            with(json!({ "GlobalSecondaryIndexOverride": [gsi_entry(None)] })),
        ),
        (
            "a GSI entry without throughput, PROVISIONED backup with the override",
            definition("PROVISIONED"),
            with(json!({ "GlobalSecondaryIndexOverride": [gsi_entry(None)] })),
        ),
        (
            "a GSI entry without throughput, PROVISIONED backup without the override",
            definition("PROVISIONED"),
            json!({ "GlobalSecondaryIndexOverride": [gsi_entry(None)] }),
        ),
    ];
    for (what, def, body) in refused {
        let fake = Fake::with_source(source(Some(def)));
        let msg = validation_message(restore(&fake, restore_body(body)).await.unwrap_err());
        assert_eq!(msg, expected, "{what}");
        assert!(fake.restores().is_empty(), "{what}: reached storage");
    }
    // The first GSI of two is the one named.
    let fake = Fake::with_source(source(Some(two_gsis("PAY_PER_REQUEST"))));
    let msg = validation_message(
        restore(&fake, restore_body(provisioned.clone()))
            .await
            .unwrap_err(),
    );
    assert_eq!(msg, expected);

    // The route that works: the list supplies the throughput.
    let input = restored_input(
        source(Some(two_gsis("PAY_PER_REQUEST"))),
        with(json!({ "GlobalSecondaryIndexOverride": [
            gsi_entry(Some((7, 8))),
            gsi2_entry(json!(["a", "b"]), Some((9, 10))),
        ] })),
    )
    .await
    .unwrap();
    assert_eq!(input.billing_mode, Some(BillingMode::Provisioned));
    assert_eq!(input.provisioned_throughput, Some(throughput(5, 5)));
    assert_eq!(gsi_throughputs(&input), [Some((7, 8)), Some((9, 10))]);
    extenddb_core::validation::validate_create_table(&input, &LimitsConfig::default()).unwrap();

    // A PROVISIONED backup restored with no override keeps its recorded GSI
    // throughput; only the override asks for the list.
    let input = restored_input(source(Some(definition("PROVISIONED"))), json!({}))
        .await
        .unwrap();
    assert_eq!(gsi_throughputs(&input), [Some((3, 4))]);
    // And with no GSIs, the billing override to PROVISIONED needs nothing.
    let mut none = definition("PAY_PER_REQUEST");
    none.global_secondary_indexes.clear();
    let mut src = source(Some(none));
    src.attribute_definitions
        .retain(|a| a.attribute_name != "gpk");
    let input = restored_input(src, provisioned).await.unwrap();
    assert!(input.global_secondary_indexes.is_none());
    assert_eq!(input.provisioned_throughput, Some(throughput(5, 5)));
}

fn on_demand(read: Option<i64>, write: Option<i64>) -> OnDemandThroughput {
    OnDemandThroughput {
        max_read_request_units: read,
        max_write_request_units: write,
    }
}

/// Measured on DynamoDB: the service applies `OnDemandThroughputOverride` on an
/// on-demand restore, changed or with one member, including on a PROVISIONED
/// backup restored as on-demand; a GSI entry's `OnDemandThroughput` is
/// accepted (and, ExtendDB's choice, not recorded). On a PROVISIONED restore
/// the member contradicts the billing mode and is refused.
#[tokio::test]
async fn an_on_demand_throughput_override_is_applied_on_an_on_demand_restore() {
    let with = |recorded: Option<OnDemandThroughput>| {
        let mut def = definition("PAY_PER_REQUEST");
        def.on_demand_throughput = recorded;
        source(Some(def))
    };
    let applied: [(
        Option<OnDemandThroughput>,
        Value,
        Option<OnDemandThroughput>,
    ); 6] = [
        (
            Some(on_demand(Some(50), Some(60))),
            json!({ "MaxReadRequestUnits": 50, "MaxWriteRequestUnits": 60 }),
            Some(on_demand(Some(50), Some(60))),
        ),
        (
            Some(on_demand(Some(50), Some(60))),
            json!({ "MaxReadRequestUnits": 50, "MaxWriteRequestUnits": 61 }),
            Some(on_demand(Some(50), Some(61))),
        ),
        (
            None,
            json!({ "MaxReadRequestUnits": 10 }),
            Some(on_demand(Some(10), None)),
        ),
        (
            Some(on_demand(Some(50), Some(60))),
            json!({ "MaxWriteRequestUnits": -1 }),
            Some(on_demand(None, Some(-1))),
        ),
        (
            None,
            json!({ "MaxReadRequestUnits": -1, "MaxWriteRequestUnits": -1 }),
            Some(on_demand(Some(-1), Some(-1))),
        ),
        // An empty override records no maxima, as CreateTable without the
        // member does.
        (Some(on_demand(Some(50), Some(60))), json!({}), None),
    ];
    for (recorded, given, expected) in applied {
        let input = restored_input(
            with(recorded.clone()),
            json!({ "OnDemandThroughputOverride": given }),
        )
        .await
        .unwrap_or_else(|e| panic!("{recorded:?} {given}: {e:?}"));
        assert_eq!(input.on_demand_throughput, expected, "{recorded:?} {given}");
        assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
    }
    // Without the member the backup's is restored.
    let input = restored_input(with(Some(on_demand(Some(50), Some(60)))), json!({}))
        .await
        .unwrap();
    assert_eq!(
        input.on_demand_throughput,
        Some(on_demand(Some(50), Some(60)))
    );

    // A PROVISIONED backup restored as on-demand takes it.
    let input = restored_input(
        source(Some(definition("PROVISIONED"))),
        json!({
            "BillingModeOverride": "PAY_PER_REQUEST",
            "OnDemandThroughputOverride": { "MaxReadRequestUnits": 7, "MaxWriteRequestUnits": 8 },
        }),
    )
    .await
    .unwrap();
    assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
    assert!(input.provisioned_throughput.is_none());
    assert_eq!(
        input.on_demand_throughput,
        Some(on_demand(Some(7), Some(8)))
    );

    // A GSI entry's OnDemandThroughput on an on-demand restore: accepted, the
    // index restored without a maximum (ExtendDB records none per index).
    let plain = restored_input(source(Some(definition("PAY_PER_REQUEST"))), json!({}))
        .await
        .unwrap();
    let input = restored_input(
        source(Some(definition("PAY_PER_REQUEST"))),
        json!({ "GlobalSecondaryIndexOverride": [{
            "IndexName": "gsi1",
            "KeySchema": [{ "AttributeName": "gpk", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
            "OnDemandThroughput": { "MaxReadRequestUnits": 10, "MaxWriteRequestUnits": -1 },
        }] }),
    )
    .await
    .unwrap();
    assert_eq!(input, plain);

    // Refused: an unknown member, and the member on a PROVISIONED restore
    // (from a PROVISIONED backup or by override).
    let fake = Fake::with_source(with(None));
    let msg = validation_message(
        restore(
            &fake,
            restore_body(json!({ "OnDemandThroughputOverride": {
                "MaxReadRequestUnits": 50, "MaxWriteRequestUnits": 60, "Other": 1,
            } })),
        )
        .await
        .unwrap_err(),
    );
    assert!(
        msg.ends_with("OnDemandThroughputOverride: member Other is not supported"),
        "{msg}"
    );
    assert!(fake.restores().is_empty());
    for (def, body) in [
        (
            definition("PROVISIONED"),
            json!({ "OnDemandThroughputOverride": { "MaxReadRequestUnits": 1 } }),
        ),
        (
            definition("PAY_PER_REQUEST"),
            json!({
                "OnDemandThroughputOverride": {},
                "BillingModeOverride": "PROVISIONED",
                "ProvisionedThroughputOverride": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 },
            }),
        ),
    ] {
        let mut def = def;
        def.global_secondary_indexes.clear();
        let fake = Fake::with_source(source(Some(def)));
        let msg = validation_message(restore(&fake, restore_body(body)).await.unwrap_err());
        assert!(
            msg.ends_with(
                "OnDemandThroughputOverride can only be specified when the restored table's \
                 BillingMode is PAY_PER_REQUEST"
            ),
            "{msg}"
        );
        assert!(fake.restores().is_empty());
    }
}

/// Measured on DynamoDB: the service restores `STANDARD` whatever the source's
/// table class; the recorded class is for DescribeBackup only.
#[tokio::test]
async fn table_class_is_not_restored() {
    let mut def = definition("PROVISIONED");
    def.table_class = Some("STANDARD_INFREQUENT_ACCESS".to_owned());
    let input = restored_input(source(Some(def)), json!({})).await.unwrap();
    assert_eq!(input.table_class, None);
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
async fn provisioned_override_on_an_on_demand_gsi_has_the_services_text() {
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
    assert_eq!(
        validation_message(err),
        "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden to \
         PROVISIONED for index gsi1"
    );
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
/// GSIs) restores with no override, the GSI taking the target table's
/// throughput. With `BillingModeOverride` `PROVISIONED` and no GSI list it is
/// refused like any backup with a GSI (the override, not the recorded
/// throughput, decides); the list route restores it.
#[tokio::test]
async fn a_table_create_table_accepted_restores() {
    // The CreateTable half: the shape is accepted there.
    let mut created = source(Some(definition("PROVISIONED")))
        .restore_input("Music", &RestoreTableOverrides::default());
    created.global_secondary_indexes.as_mut().unwrap()[0].provisioned_throughput = None;
    extenddb_core::validation::validate_create_table(&created, &LimitsConfig::default())
        .expect("CreateTable accepts a PROVISIONED GSI without throughput");

    let provisioned = json!({
        "BillingModeOverride": "PROVISIONED",
        "ProvisionedThroughputOverride": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 6 },
    });
    for recorded in [None, Some(throughput(0, 0))] {
        let src = || {
            let mut src = source(Some(definition("PROVISIONED")));
            src.definition.as_mut().unwrap().global_secondary_indexes[0].provisioned_throughput =
                recorded.clone();
            src
        };
        let fake = Fake::with_source(src());
        restore(&fake, restore_body(json!({})))
            .await
            .unwrap_or_else(|e| panic!("{recorded:?}: {e:?}"));
        let input = &fake.restores()[0].input;
        assert_eq!(
            input.global_secondary_indexes.as_ref().unwrap()[0].provisioned_throughput,
            Some(throughput(7, 8)),
            "{recorded:?}"
        );

        let fake = Fake::with_source(src());
        let msg = validation_message(
            restore(&fake, restore_body(provisioned.clone()))
                .await
                .unwrap_err(),
        );
        assert_eq!(
            msg,
            "Invalid Request: Must override ProvisionedThroughput if BillingMode is overridden \
             to PROVISIONED for index gsi1",
            "{recorded:?}"
        );
        assert!(fake.restores().is_empty());

        let mut with_list = provisioned.clone();
        with_list["GlobalSecondaryIndexOverride"] = json!([gsi_entry(Some((5, 6)))]);
        let fake = Fake::with_source(src());
        restore(&fake, restore_body(with_list))
            .await
            .unwrap_or_else(|e| panic!("{recorded:?}: {e:?}"));
        let input = &fake.restores()[0].input;
        assert_eq!(
            input.global_secondary_indexes.as_ref().unwrap()[0].provisioned_throughput,
            Some(throughput(5, 6)),
            "{recorded:?}"
        );
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
    // Omitted by the service here; the backend's description carries both.
    assert!(td.get("DeletionProtectionEnabled").is_none(), "{td}");
    let pt = td["ProvisionedThroughput"].as_object().unwrap();
    assert!(!pt.contains_key("NumberOfDecreasesToday"), "{pt:?}");
    assert_eq!(pt["ReadCapacityUnits"], 9);
    let backend = serde_json::to_value(described(&fake.restores()[0])).unwrap();
    assert_eq!(backend["DeletionProtectionEnabled"], false);
    assert_eq!(
        backend["ProvisionedThroughput"]["NumberOfDecreasesToday"],
        0
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

/// Measured on DynamoDB: a target name taken by a
/// `CREATING` restore is `TableInUseException` `Table: <name> is already
/// being restored from backup: <arn>`; a backup with a restore from it still
/// `CREATING` is `BackupInUseException`. The backends raise the storage
/// errors; this is their mapping.
#[tokio::test]
async fn a_name_being_restored_is_table_in_use_and_a_backup_being_restored_is_backup_in_use() {
    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    *fake.restore_error.lock().unwrap() = Some(StorageError::TableBeingRestored {
        table_name: "MusicRestored".to_owned(),
        backup_arn: arn(ACCOUNT),
    });
    let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
    match &err {
        DynamoDbError::TableInUseException(m) => assert_eq!(
            m,
            &format!(
                "Table: MusicRestored is already being restored from backup: {}",
                arn(ACCOUNT)
            )
        ),
        other => panic!("expected TableInUseException, got {other:?}"),
    }
    assert_eq!(
        err.full_error_type(),
        "com.amazonaws.dynamodb.v20120810#TableInUseException"
    );

    let fake = Fake::with_source(source(Some(definition("PROVISIONED"))));
    *fake.restore_error.lock().unwrap() = Some(StorageError::BackupInUse(format!(
        "Backup {} is being restored",
        arn(ACCOUNT)
    )));
    let err = restore(&fake, restore_body(json!({}))).await.unwrap_err();
    assert!(
        matches!(err, DynamoDbError::BackupInUseException(_)),
        "{err:?}"
    );

    // Elsewhere the name is a table in use. A plain CreateTable into the
    // name answers the service's text for a taken name (measured on
    // DynamoDB), without the backup ARN.
    assert!(matches!(
        storage_err_to_dynamo(StorageError::TableBeingRestored {
            table_name: "t".to_owned(),
            backup_arn: "a".to_owned(),
        }),
        DynamoDbError::ResourceInUseException(_)
    ));
    match crate::create_table::storage_err_to_dynamo(StorageError::TableBeingRestored {
        table_name: "MusicRestored".to_owned(),
        backup_arn: arn(ACCOUNT),
    }) {
        DynamoDbError::ResourceInUseException(m) => {
            assert_eq!(m, "Table already exists: MusicRestored");
        }
        other => panic!("expected ResourceInUseException, got {other:?}"),
    }
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

/// Measured on DynamoDB: `BackupName` and `TableName` are
/// constrained as the service model says, with the service's texts; both
/// constraints failing join under `2 validation errors detected:`.
#[test]
fn backup_and_table_names_are_validated_as_the_model_says() {
    let ok = ["abc", "a.b-c_D", &"x".repeat(255)];
    for name in ok {
        validate_name_member(name, "backupName", BACKUP_NAME_BOUNDS)
            .unwrap_or_else(|e| panic!("{name}: {e:?}"));
    }
    let constraint = |v: &str, path: &str, c: &str| {
        format!("Value '{v}' at '{path}' failed to satisfy constraint: {c}")
    };
    let short = "Member must have length greater than or equal to 3";
    let long = "Member must have length less than or equal to 255";
    let pattern = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+";
    let too_long = "x".repeat(256);
    let huge = "y".repeat(200 * 1024);
    let cases: Vec<(&str, &str, String)> = vec![
        (
            "",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("", "backupName", short)
            ),
        ),
        (
            "ab",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("ab", "backupName", short)
            ),
        ),
        (
            &too_long,
            "tableName",
            format!(
                "1 validation error detected: {}",
                constraint(&too_long, "tableName", long)
            ),
        ),
        (
            "a b c",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("a b c", "backupName", pattern)
            ),
        ),
        (
            "abc\u{0}",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("abc\u{0}", "backupName", pattern)
            ),
        ),
        (
            "b\u{e9}k",
            "backupName",
            format!(
                "1 validation error detected: {}",
                constraint("b\u{e9}k", "backupName", pattern)
            ),
        ),
        (
            "a!",
            "backupName",
            format!(
                "2 validation errors detected: {}; {}",
                constraint("a!", "backupName", short),
                constraint("a!", "backupName", pattern)
            ),
        ),
    ];
    for (value, path, expected) in cases {
        assert_eq!(
            validation_message(validate_name_member(value, path, BACKUP_NAME_BOUNDS).unwrap_err()),
            expected,
            "{value:?}"
        );
    }
    // A 200 KB name with a NUL byte (a 500 on Cassandra and PostgreSQL
    // before the check) is refused with both constraints named.
    let bad = format!("{huge}\u{0}");
    let msg = validation_message(
        validate_name_member(&bad, "backupName", BACKUP_NAME_BOUNDS).unwrap_err(),
    );
    assert!(
        msg.starts_with("2 validation errors detected: "),
        "{}",
        &msg[..60]
    );
    assert!(msg.contains(long) && msg.contains(pattern));

    // Through the handlers' members.
    let body = json!({ "TableName": "ok_table", "BackupName": "x" });
    let name = create_backup_table_name(&body, ACCOUNT, &limits()).unwrap();
    assert_eq!(name, "ok_table");
    assert!(matches!(
        validate_name_member(body["BackupName"].as_str().unwrap(), "backupName", BACKUP_NAME_BOUNDS)
            .unwrap_err(),
        DynamoDbError::ValidationException(m) if m.contains("'backupName'")
    ));
    let msg = validation_message(
        create_backup_table_name(&json!({ "TableName": "a*" }), ACCOUNT, &limits()).unwrap_err(),
    );
    assert!(
        msg.contains("'tableName'") && msg.contains(pattern),
        "{msg}"
    );
}

/// Length is counted in characters, as the model's bounds are: a two-byte
/// character is one. `TableName` members take the deployment's table-name
/// limits (`CreateBackup` and `ListBackups` agree with `CreateTable`);
/// `BackupName` keeps the model's 3 to 255.
#[tokio::test]
async fn name_lengths_are_counted_in_characters_and_table_names_take_the_limits() {
    let pattern = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+";
    // Two characters, four bytes: too short, and not in the pattern (two
    // clauses, not one).
    let msg = validation_message(
        validate_name_member("\u{e9}\u{e9}", "backupName", BACKUP_NAME_BOUNDS).unwrap_err(),
    );
    assert!(msg.starts_with("2 validation errors detected: "), "{msg}");
    assert!(msg.contains("length greater than or equal to 3"), "{msg}");
    assert!(msg.contains(pattern), "{msg}");
    // 255 two-byte characters (510 bytes): within the length bound, so only
    // the pattern clause.
    let wide = "\u{e9}".repeat(255);
    let msg = validation_message(
        validate_name_member(&wide, "backupName", BACKUP_NAME_BOUNDS).unwrap_err(),
    );
    assert_eq!(
        msg,
        format!(
            "1 validation error detected: Value '{wide}' at 'backupName' failed to satisfy \
             constraint: {pattern}"
        )
    );

    // A deployment that raised the maximum: a 300-character table can be
    // backed up and listed; the backup name stays at 255.
    let raised = LimitsConfig {
        max_table_name_length: 300,
        min_table_name_length: 2,
        ..Default::default()
    };
    let long = "t".repeat(300);
    assert_eq!(
        create_backup_table_name(&json!({ "TableName": long }), ACCOUNT, &raised).unwrap(),
        long
    );
    assert_eq!(
        create_backup_table_name(&json!({ "TableName": "ab" }), ACCOUNT, &raised).unwrap(),
        "ab"
    );
    let arn_form = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{long}");
    assert_eq!(
        create_backup_table_name(&json!({ "TableName": arn_form }), ACCOUNT, &raised).unwrap(),
        long
    );
    let fake = fake_with_summaries(vec![]);
    list_backups(
        &fake,
        ACCOUNT,
        REGION,
        &raised,
        &json!({ "TableName": long }),
    )
    .await
    .unwrap();
    assert_eq!(
        fake.listed_for.lock().unwrap()[0].as_deref(),
        Some(long.as_str())
    );
    // The defaults still refuse it, with the bound in the text.
    let msg = validation_message(
        create_backup_table_name(&json!({ "TableName": long }), ACCOUNT, &limits()).unwrap_err(),
    );
    assert!(msg.contains("length less than or equal to 255"), "{msg}");
    let msg = validation_message(
        list_backups(
            &fake,
            ACCOUNT,
            REGION,
            &limits(),
            &json!({ "TableName": long }),
        )
        .await
        .unwrap_err(),
    );
    assert!(msg.contains("length less than or equal to 255"), "{msg}");
    // A lowered maximum is reported with its own number.
    let lowered = LimitsConfig {
        max_table_name_length: 10,
        ..Default::default()
    };
    let msg = validation_message(
        create_backup_table_name(&json!({ "TableName": "elevenchars" }), ACCOUNT, &lowered)
            .unwrap_err(),
    );
    assert!(msg.contains("length less than or equal to 10"), "{msg}");
    let msg = validation_message(
        validate_name_member(&"b".repeat(300), "backupName", BACKUP_NAME_BOUNDS).unwrap_err(),
    );
    assert!(msg.contains("length less than or equal to 255"), "{msg}");
}

/// Measured on DynamoDB: `CreateBackup` accepts the
/// table's ARN as `TableName`. Another account's is refused as a backup ARN
/// of another account is; another region's resolves (both ExtendDB's choice,
/// not measured).
#[test]
fn create_backup_table_name_may_be_the_tables_arn() {
    let own = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music");
    assert_eq!(
        create_backup_table_name(&json!({ "TableName": own }), ACCOUNT, &limits()).unwrap(),
        "Music"
    );
    let other_region = format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music");
    assert_eq!(
        create_backup_table_name(&json!({ "TableName": other_region }), ACCOUNT, &limits())
            .unwrap(),
        "Music"
    );
    let foreign = format!("arn:aws:dynamodb:{REGION}:999999999999:table/Music");
    assert!(matches!(
        create_backup_table_name(&json!({ "TableName": foreign }), ACCOUNT, &limits()).unwrap_err(),
        DynamoDbError::AccessDeniedException(_)
    ));
    for bad in [
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/01791976431438-73d8d5bc"),
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/index/i"),
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:index/Music"),
        format!("arn:aws:s3:{REGION}:{ACCOUNT}:table/Music"),
        format!("arn:aws-cn:dynamodb:{REGION}:{ACCOUNT}:table/Music"),
        format!("arn:aws:dynamodb::{ACCOUNT}:table/Music"),
        format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}"),
        "arn:".to_owned(),
    ] {
        let msg = validation_message(
            create_backup_table_name(&json!({ "TableName": bad }), ACCOUNT, &limits()).unwrap_err(),
        );
        assert_eq!(
            msg, "Invalid Request: TableName is not a valid table name or table ARN",
            "{bad}"
        );
    }
    // The name part is checked as a name.
    let short = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/ab");
    let msg = validation_message(
        create_backup_table_name(&json!({ "TableName": short }), ACCOUNT, &limits()).unwrap_err(),
    );
    assert!(
        msg.contains("'tableName'") && msg.contains("length greater than or equal to 3"),
        "{msg}"
    );
    // Missing: the null-member text.
    let msg =
        validation_message(create_backup_table_name(&json!({}), ACCOUNT, &limits()).unwrap_err());
    assert!(msg.contains("Value null at 'tableName'"), "{msg}");
}

/// Measured on DynamoDB: `CreateBackup` on a table that
/// does not exist or is still `CREATING` is `TableNotFoundException`
/// `Table not found: <name>`, whatever the backend's own payload says.
#[test]
fn create_backup_on_a_missing_or_creating_table_is_table_not_found() {
    for storage in [
        // PostgreSQL's payload and the bare name the other backends give.
        StorageError::TableNotFound("Table not found: Music".to_owned()),
        StorageError::TableNotFound("Music".to_owned()),
        StorageError::TableNotActive("Music".to_owned()),
    ] {
        let what = format!("{storage:?}");
        let err = create_backup_storage_err_to_dynamo(storage, "Music");
        match &err {
            DynamoDbError::TableNotFoundException(m) => {
                assert_eq!(m, "Table not found: Music", "{what}")
            }
            other => panic!("{what}: expected TableNotFoundException, got {other:?}"),
        }
        assert_eq!(
            err.full_error_type(),
            "com.amazonaws.dynamodb.v20120810#TableNotFoundException"
        );
    }
    // Other errors keep the generic mapping.
    assert!(matches!(
        create_backup_storage_err_to_dynamo(StorageError::LimitExceeded("x".to_owned()), "Music"),
        DynamoDbError::LimitExceededException(_)
    ));
    // The data plane keeps ResourceNotFoundException for a missing table.
    assert!(matches!(
        storage_err_to_dynamo(StorageError::TableNotFound("Music".to_owned())),
        DynamoDbError::ResourceNotFoundException(_)
    ));
}

// --- DeleteBackup -------------------------------------------------------------

/// Measured on DynamoDB: `DeleteBackup` answers the full
/// DescribeBackup description with `BackupStatus` `DELETED`, including the
/// required `SourceTableDetails.ProvisionedThroughput` and the
/// `SourceTableFeatureDetails` the backend's row alone does not carry.
#[tokio::test]
async fn delete_backup_answers_the_full_description_as_deleted() {
    for def in [definition("PROVISIONED"), definition("PAY_PER_REQUEST")] {
        let on_demand = def.billing_mode == "PAY_PER_REQUEST";
        let fake = fake_with_backup("AVAILABLE", source(Some(def)));
        let described = describe(&fake).await;
        let deleted = delete_backup(&fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
            .await
            .unwrap();
        let deleted = serde_json::to_value(deleted).unwrap();
        assert_eq!(deleted["BackupDetails"]["BackupStatus"], "DELETED");
        assert_eq!(
            deleted["SourceTableDetails"]["ProvisionedThroughput"],
            if on_demand {
                json!({ "ReadCapacityUnits": 0, "WriteCapacityUnits": 0 })
            } else {
                json!({ "ReadCapacityUnits": 7, "WriteCapacityUnits": 8 })
            }
        );
        assert_eq!(
            deleted["SourceTableFeatureDetails"]["GlobalSecondaryIndexes"][0]["IndexName"],
            "gsi1"
        );
        assert_eq!(
            deleted["SourceTableFeatureDetails"]["LocalSecondaryIndexes"][0]["IndexName"],
            "lsi1"
        );
        // Everything but the status is what DescribeBackup said.
        let mut expected = described.clone();
        expected["BackupDetails"]["BackupStatus"] = json!("DELETED");
        assert_eq!(deleted, expected);
        // The backup is gone.
        assert!(matches!(
            delete_backup(&fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
                .await
                .unwrap_err(),
            DynamoDbError::BackupNotFoundException(_)
        ));
        assert_eq!(
            fake.source_reads(),
            3,
            "one read by DescribeBackup, one per delete attempt"
        );
    }

    // A backup whose definition this build cannot read, or that does not
    // parse, is still deleted, described without feature details.
    for err in [
        StorageError::Unsupported("version 9".to_owned()),
        StorageError::Internal("does not parse".to_owned()),
    ] {
        let what = format!("{err:?}");
        let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
        *fake.source_error.lock().unwrap() = Some(err);
        let deleted = delete_backup(&fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
            .await
            .unwrap_or_else(|e| panic!("{what}: {e:?}"));
        assert_eq!(deleted.backup_details.backup_status, "DELETED", "{what}");
        assert!(deleted.source_table_feature_details.is_none(), "{what}");
        assert!(
            deleted
                .source_table_details
                .provisioned_throughput
                .is_some(),
            "{what}"
        );
        assert!(
            fake.descriptions.lock().unwrap().is_empty(),
            "{what}: not deleted"
        );
    }
    // A failure to reach the database propagates, and nothing is deleted.
    let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
    *fake.source_error.lock().unwrap() = Some(StorageError::Connection("refused".to_owned()));
    assert!(matches!(
        delete_backup(&fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
            .await
            .unwrap_err(),
        DynamoDbError::InternalServerError(_)
    ));
    assert_eq!(fake.descriptions.lock().unwrap().len(), 1);
}

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
        describe_backup(fake, ACCOUNT, REGION, &own(&arn(ACCOUNT)))
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

/// A definition this build cannot read (a later version) does not stop
/// DescribeBackup: the backend's BackupDetails are returned without feature
/// details. A definition that does not parse is `Internal`, and propagates as
/// a 500 like a failure to reach the database (`Transient`, `Connection`):
/// a wrong definition row is a database fault, and describing the backup as
/// on-demand `{0, 0}` without feature details would mask it as a wrong answer
/// while RestoreTableFromBackup on the same read is a 500.
/// The errors are the ones every backend returns for those cases, built by
/// `BackupTableDefinition::from_json` itself.
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
    {
        let err = unknown_version;
        let what = format!("{err:?}");
        let fake = fake_with_backup("AVAILABLE", source(Some(definition("PROVISIONED"))));
        *fake.source_error.lock().unwrap() = Some(err);
        let desc = describe_backup(&fake, ACCOUNT, REGION, &own(&arn_text))
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
        undecodable,
        StorageError::Internal("column \"definition\" does not exist".to_owned()),
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
        match describe_backup(&fake, ACCOUNT, REGION, &own(&arn_text)).await {
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
    list_backups(fake, ACCOUNT, REGION, &limits(), &body).await
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
    // Well formed, ours, not listed, and its id (one of the two shapes
    // earlier builds issued: 13 digits with the hex suffix, 13 bare digits)
    // carries no time: `backup_id_epoch_millis` needs the 14-digit form.
    for id in ["1791976431438-73d8d5bc", "1791976431438"] {
        let legacy = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Music/backup/{id}");
        let msg = validation_message(
            list(&fake, json!({ "ExclusiveStartBackupArn": legacy }))
                .await
                .unwrap_err(),
        );
        assert!(msg.contains("ExclusiveStartBackupArn"), "{id}: {msg}");
        assert!(msg.contains("carries no creation time"), "{id}: {msg}");
    }

    assert_eq!(
        validation_message(
            list(&fake, json!({ "ExclusiveStartBackupArn": "garbage" }))
                .await
                .unwrap_err()
        ),
        "Invalid Request: BackupArn is not valid"
    );
}

/// Measured on DynamoDB: a cursor of another account,
/// another region, or a table other than `TableName`'s is a
/// `ValidationException`, in that order; the account text is the service's,
/// the other two complete the fragments captured ("does not match the
/// region", "does not match the table name predicate").
#[tokio::test]
async fn list_backups_cursor_must_match_the_account_region_and_table() {
    let fake = fake_with_summaries(vec![summary(1, 1), summary(2, 2)]);
    let id = "01791976431438-73d8d5bc";
    let cases = [
        (
            format!("arn:aws:dynamodb:{REGION}:999999999999:table/Music/backup/{id}"),
            None,
            "account",
        ),
        (
            format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Music/backup/{id}"),
            None,
            "region",
        ),
        (
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Other/backup/{id}"),
            Some("Music"),
            "table name predicate",
        ),
        // Account before region before table.
        (
            format!("arn:aws:dynamodb:eu-west-1:999999999999:table/Other/backup/{id}"),
            Some("Music"),
            "account",
        ),
        (
            format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/Other/backup/{id}"),
            Some("Music"),
            "region",
        ),
    ];
    for (cursor, table, what) in cases {
        let mut body = json!({ "ExclusiveStartBackupArn": cursor });
        if let Some(table) = table {
            body["TableName"] = json!(table);
        }
        let msg = validation_message(list(&fake, body).await.unwrap_err());
        assert_eq!(
            msg,
            format!(
                "Invalid Request: The supplied ExclusiveStartBackupArn does not match the {what}"
            ),
            "{cursor}"
        );
    }
    // Another table's cursor without TableName is fine (the listing is the
    // account's), and a trailing newline is trimmed.
    let cursor = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/Other/backup/{id}\n");
    list(&fake, json!({ "ExclusiveStartBackupArn": cursor }))
        .await
        .unwrap();
    // The cursor of the listing itself resumes after it.
    let first = summary(1, 1).backup_arn;
    let page = list(
        &fake,
        json!({ "ExclusiveStartBackupArn": format!("{first}\n"), "TableName": "Music" }),
    )
    .await
    .unwrap();
    assert_eq!(arns(&page), vec![summary(2, 2).backup_arn]);
}

/// A backup created under an earlier `server.region` keeps that region in its
/// stored ARN, and `ListBackups` hands that ARN out as the cursor; the next
/// page accepts it (trimmed). Another region's ARN that is not a listed
/// backup's is still refused.
#[tokio::test]
async fn list_backups_pages_past_a_backup_stored_under_another_region() {
    let mut old = summary(2, 2);
    old.backup_arn = old.backup_arn.replace(REGION, "eu-west-1");
    let all = vec![summary(1, 1), old.clone(), summary(3, 3)];
    let fake = fake_with_summaries(all);
    let mut seen = Vec::new();
    let mut body = json!({ "Limit": 2 });
    let first = list(&fake, body.clone()).await.unwrap();
    seen.extend(arns(&first));
    assert_eq!(
        first["LastEvaluatedBackupArn"].as_str(),
        Some(old.backup_arn.as_str())
    );
    body["ExclusiveStartBackupArn"] = json!(format!("{}\n", old.backup_arn));
    for table in [None, Some("Music")] {
        if let Some(t) = table {
            body["TableName"] = json!(t);
        }
        let second = list(&fake, body.clone()).await.unwrap();
        assert_eq!(arns(&second), vec![summary(3, 3).backup_arn], "{body}");
    }
    seen.extend(arns(&list(&fake, body.clone()).await.unwrap()));
    assert_eq!(seen.len(), 3);
    let stranger = summary(9, 9).backup_arn.replace(REGION, "eu-west-1");
    let msg = validation_message(
        list(&fake, json!({ "ExclusiveStartBackupArn": stranger }))
            .await
            .unwrap_err(),
    );
    assert_eq!(
        msg,
        "Invalid Request: The supplied ExclusiveStartBackupArn does not match the region"
    );
}

/// Measured on DynamoDB: `TableName` on `ListBackups` is checked as the service
/// model constrains it.
#[tokio::test]
async fn list_backups_table_name_is_validated() {
    let fake = fake_with_summaries(vec![summary(1, 1)]);
    let msg = validation_message(list(&fake, json!({ "TableName": "a" })).await.unwrap_err());
    assert_eq!(
        msg,
        "1 validation error detected: Value 'a' at 'tableName' failed to satisfy constraint: \
         Member must have length greater than or equal to 3"
    );
    assert!(fake.listed_for.lock().unwrap().is_empty());
    list(&fake, json!({ "TableName": "Music" })).await.unwrap();
    assert_eq!(
        fake.listed_for.lock().unwrap().as_slice(),
        [Some("Music".to_owned())]
    );
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

#[tokio::test]
async fn list_backups_filters_on_backup_type() {
    let mut system = summary(2, 200);
    system.backup_type = "SYSTEM".to_owned();
    let mut aws_backup = summary(3, 300);
    aws_backup.backup_type = "AWS_BACKUP".to_owned();
    let user = summary(1, 100);
    let fake = fake_with_summaries(vec![user.clone(), system.clone(), aws_backup.clone()]);
    for (filter, expected) in [
        (json!(null), vec![user.backup_arn.clone()]),
        (json!("USER"), vec![user.backup_arn.clone()]),
        (json!("SYSTEM"), vec![system.backup_arn.clone()]),
        (json!("AWS_BACKUP"), vec![aws_backup.backup_arn.clone()]),
        (
            json!("ALL"),
            vec![
                user.backup_arn.clone(),
                system.backup_arn.clone(),
                aws_backup.backup_arn.clone(),
            ],
        ),
    ] {
        let page = list(&fake, json!({ "BackupType": filter })).await.unwrap();
        assert_eq!(arns(&page), expected, "{filter}");
    }
    let page = list(&fake, json!({})).await.unwrap();
    assert_eq!(arns(&page), vec![user.backup_arn.clone()]);

    let msg = validation_message(
        list(&fake, json!({ "BackupType": "user" }))
            .await
            .unwrap_err(),
    );
    assert_eq!(
        msg,
        "1 validation error detected: Value 'user' at 'backupType' failed to satisfy \
         constraint: Member must satisfy enum value set: [USER, SYSTEM, AWS_BACKUP, ALL]"
    );
    assert!(matches!(
        list(&fake, json!({ "BackupType": 1 })).await.unwrap_err(),
        DynamoDbError::SerializationException(_)
    ));
}

#[tokio::test]
async fn list_backups_filters_on_the_time_range_before_paging() {
    // Creation times 1.000, 2.000, 3.000, 4.000, 5.000 s.
    let all: Vec<BackupSummary> = (1..=5).map(|n| summary(n, 1_000 * n)).collect();
    let arn_of = |n: usize| all[n - 1].backup_arn.clone();
    let fake = fake_with_summaries(all.clone());
    for (body, expected) in [
        // Lower bound inclusive, upper bound exclusive, at the millisecond.
        (json!({ "TimeRangeLowerBound": 2.0 }), vec![2, 3, 4, 5]),
        (json!({ "TimeRangeLowerBound": 2.001 }), vec![3, 4, 5]),
        (json!({ "TimeRangeUpperBound": 4.0 }), vec![1, 2, 3]),
        (json!({ "TimeRangeUpperBound": 4.001 }), vec![1, 2, 3, 4]),
        (
            json!({ "TimeRangeLowerBound": 2, "TimeRangeUpperBound": 4 }),
            vec![2, 3],
        ),
        (
            json!({ "TimeRangeLowerBound": 3, "TimeRangeUpperBound": 3 }),
            vec![],
        ),
        (
            json!({ "TimeRangeLowerBound": null, "TimeRangeUpperBound": null }),
            vec![1, 2, 3, 4, 5],
        ),
        (
            json!({ "TimeRangeLowerBound": 0, "TimeRangeUpperBound": 0 }),
            vec![],
        ),
    ] {
        let page = list(&fake, body.clone()).await.unwrap();
        let want: Vec<String> = expected.into_iter().map(arn_of).collect();
        assert_eq!(arns(&page), want, "{body}");
    }

    // Measured on DynamoDB: a lower bound above the upper
    // bound and a bound before the epoch are refused.
    assert_eq!(
        validation_message(
            list(
                &fake,
                json!({ "TimeRangeLowerBound": 4, "TimeRangeUpperBound": 2 })
            )
            .await
            .unwrap_err()
        ),
        "Time range lower bound must be less than or equal to upper bound"
    );
    assert_eq!(
        validation_message(
            list(
                &fake,
                json!({ "TimeRangeLowerBound": 2.5, "TimeRangeUpperBound": 2.4999 })
            )
            .await
            .unwrap_err()
        ),
        "Time range lower bound must be less than or equal to upper bound"
    );
    for member in ["TimeRangeLowerBound", "TimeRangeUpperBound"] {
        for at in [-1, -1_000_000_000] {
            let msg = validation_message(list(&fake, json!({ member: at })).await.unwrap_err());
            assert!(msg.contains(member), "{member} {at}: {msg}");
            assert!(msg.contains("1970-01-01T00:00:00Z"), "{member} {at}: {msg}");
        }
    }

    // Paging walks the filtered list: Limit counts only backups in range, and
    // the last in-range page has no cursor although later backups exist.
    let mut seen = Vec::new();
    let mut body = json!({ "TimeRangeLowerBound": 2, "TimeRangeUpperBound": 4.5, "Limit": 2 });
    let first = list(&fake, body.clone()).await.unwrap();
    seen.extend(arns(&first));
    assert_eq!(
        first["LastEvaluatedBackupArn"].as_str(),
        Some(arn_of(3).as_str())
    );
    body["ExclusiveStartBackupArn"] = first["LastEvaluatedBackupArn"].clone();
    let second = list(&fake, body).await.unwrap();
    seen.extend(arns(&second));
    assert!(
        second.get("LastEvaluatedBackupArn").is_none(),
        "the last page carries a cursor"
    );
    assert_eq!(seen, vec![arn_of(2), arn_of(3), arn_of(4)]);

    for member in ["TimeRangeLowerBound", "TimeRangeUpperBound"] {
        assert!(matches!(
            list(&fake, json!({ member: "2026-01-01" })).await.unwrap_err(),
            DynamoDbError::SerializationException(m) if m.contains(member)
        ));
    }
}

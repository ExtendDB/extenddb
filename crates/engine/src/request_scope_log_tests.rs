// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The fault log line names the request it belongs to. Each engine mapping
//! arm that turns a `StorageError` into a redacted 500 writes the line through
//! `log_storage_fault!`, which reads the request id and operation from the
//! request scope the server sets around the request. Inside a scope the two
//! fields carry the request's values; outside one the line carries neither.
//! The macro expands at its call site, so each line keeps the site's own
//! target. The detached restore re-enters the scope in its spawned task.

use std::sync::{Arc, Mutex};

use extenddb_core::error::DynamoDbError;
use extenddb_storage::error::StorageError;
use extenddb_storage::request_scope::{RequestScope, with_scope};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

/// What the subscriber saw of one error-level event: the `request_id` and
/// `operation` fields (`None` when the field was not recorded), the message
/// and the event's static target.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Seen {
    request_id: Option<String>,
    operation: Option<String>,
    message: String,
    target: String,
}

impl Visit for Seen {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "request_id" => self.request_id = Some(value.to_owned()),
            "operation" => self.operation = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

#[derive(Default)]
struct Captured(Arc<Mutex<Vec<Seen>>>);

impl<S: Subscriber> Layer<S> for Captured {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if *event.metadata().level() == Level::ERROR {
            let mut seen = Seen {
                target: event.metadata().target().to_owned(),
                ..Seen::default()
            };
            event.record(&mut seen);
            self.0.lock().unwrap().push(seen);
        }
    }
}

/// Runs `f` under a fresh subscriber and returns every error event it logged.
fn error_events_during(f: impl FnOnce()) -> Vec<Seen> {
    let captured = Captured::default();
    let events = Arc::clone(&captured.0);
    let subscriber = tracing_subscriber::registry().with(captured);
    let _guard = tracing::subscriber::set_default(subscriber);
    f();
    events.lock().unwrap().clone()
}

/// Runs `fut` under a fresh subscriber and returns every error event it logged.
async fn error_events_during_async<F: std::future::Future>(fut: F) -> Vec<Seen> {
    let captured = Captured::default();
    let events = Arc::clone(&captured.0);
    let subscriber = tracing_subscriber::registry().with(captured);
    let _guard = tracing::subscriber::set_default(subscriber);
    fut.await;
    events.lock().unwrap().clone()
}

fn scope() -> RequestScope {
    RequestScope::new("req-0f2a".to_owned(), "CreateTable".to_owned())
}

/// A mapping arm: its name, the error that reaches it, the function, the
/// message text its line has always carried, and the target of the module
/// that owns the arm.
struct Site {
    name: &'static str,
    error: fn() -> StorageError,
    map: fn(StorageError) -> DynamoDbError,
    message: &'static str,
    target: &'static str,
}

fn internal() -> StorageError {
    StorageError::Internal("driver text".to_owned())
}

/// Every engine arm that maps a `StorageError` to a redacted 500.
fn sites() -> Vec<Site> {
    vec![
        Site {
            name: "create_table::storage_err_to_dynamo, Internal",
            error: internal,
            map: crate::create_table::storage_err_to_dynamo,
            message: "storage internal error",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "create_table::storage_err_to_dynamo, CatalogVersionMismatch",
            error: || StorageError::CatalogVersionMismatch {
                expected: "3".to_owned(),
                found: "2".to_owned(),
            },
            map: crate::create_table::storage_err_to_dynamo,
            message: "Catalog version mismatch: expected 3, found 2",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "create_table::storage_err_to_dynamo, CatalogNotInitialized",
            error: || StorageError::CatalogNotInitialized,
            map: crate::create_table::storage_err_to_dynamo,
            message: "Catalog not initialized",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "create_table::storage_err_to_dynamo, IdempotentReplay",
            error: || StorageError::IdempotentReplay,
            map: crate::create_table::storage_err_to_dynamo,
            message: "Unexpected idempotency error in generic error handler",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "backup::storage_err_to_dynamo",
            error: internal,
            map: crate::backup::storage_err_to_dynamo,
            message: "backup storage error",
            target: "extenddb_engine::backup",
        },
        Site {
            name: "update_table::update_table_err_to_dynamo",
            error: internal,
            map: |e| crate::update_table::update_table_err_to_dynamo(e, "t"),
            message: "storage internal error",
            target: "extenddb_engine::update_table",
        },
        Site {
            name: "streams::storage_to_dynamo",
            error: internal,
            map: crate::streams::storage_to_dynamo,
            message: "storage internal error",
            target: "extenddb_engine::streams",
        },
        Site {
            name: "ttl::storage_to_dynamo",
            error: internal,
            map: crate::ttl::storage_to_dynamo,
            message: "storage internal error",
            target: "extenddb_engine::ttl",
        },
        Site {
            name: "sanitize_storage_error",
            error: internal,
            map: crate::sanitize_storage_error,
            message: "storage internal error",
            target: "extenddb_engine",
        },
    ]
}

#[tokio::test]
async fn the_fault_line_names_the_request_inside_a_scope_at_every_site() {
    for site in sites() {
        let logged = with_scope(scope(), async {
            error_events_during(|| {
                let err = (site.map)((site.error)());
                assert!(
                    matches!(err, DynamoDbError::InternalServerError(_)),
                    "{}",
                    site.name
                );
            })
        })
        .await;
        assert_eq!(
            logged,
            vec![Seen {
                request_id: Some("req-0f2a".to_owned()),
                operation: Some("CreateTable".to_owned()),
                message: site.message.to_owned(),
                target: site.target.to_owned(),
            }],
            "{}: the fault line did not carry the scope's request id and operation",
            site.name
        );
    }
}

#[tokio::test]
async fn the_fault_line_carries_no_request_fields_outside_a_scope_at_every_site() {
    for site in sites() {
        let logged = error_events_during(|| {
            let err = (site.map)((site.error)());
            assert!(
                matches!(err, DynamoDbError::InternalServerError(_)),
                "{}",
                site.name
            );
        });
        assert_eq!(
            logged,
            vec![Seen {
                request_id: None,
                operation: None,
                message: site.message.to_owned(),
                target: site.target.to_owned(),
            }],
            "{}: a fault outside a request carried a request id or operation",
            site.name
        );
    }
}

/// A backup engine whose source lookup fails with a storage fault. The other
/// methods are never reached: the restore reads the source first.
struct FaultingBackups;

impl extenddb_storage::BackupEngine for FaultingBackups {
    fn create_backup(
        &self,
        _: &str,
        _: &str,
        _: &str,
    ) -> extenddb_storage::BoxedFuture<'_, Result<extenddb_core::types::BackupDetails, StorageError>>
    {
        unreachable!("not reached by the restore")
    }
    fn describe_backup(
        &self,
        _: &str,
        _: &str,
    ) -> extenddb_storage::BoxedFuture<
        '_,
        Result<extenddb_core::types::BackupDescription, StorageError>,
    > {
        unreachable!("not reached by the restore")
    }
    fn list_backups(
        &self,
        _: &str,
        _: Option<&str>,
    ) -> extenddb_storage::BoxedFuture<
        '_,
        Result<Vec<extenddb_core::types::BackupSummary>, StorageError>,
    > {
        unreachable!("not reached by the restore")
    }
    fn delete_backup(
        &self,
        _: &str,
        _: &str,
    ) -> extenddb_storage::BoxedFuture<
        '_,
        Result<extenddb_core::types::BackupDescription, StorageError>,
    > {
        unreachable!("not reached by the restore")
    }
    fn backup_restore_source(
        &self,
        _: &str,
        _: &str,
    ) -> extenddb_storage::BoxedFuture<
        '_,
        Result<extenddb_storage::BackupRestoreSource, StorageError>,
    > {
        Box::pin(async { Err(internal()) })
    }
    fn restore_table_from_backup(
        &self,
        _: &str,
        _: extenddb_storage::RestoreRequest,
    ) -> extenddb_storage::BoxedFuture<
        '_,
        Result<extenddb_core::types::TableDescription, StorageError>,
    > {
        unreachable!("not reached by the restore")
    }
    fn describe_continuous_backups(
        &self,
        _: &str,
        _: &str,
    ) -> extenddb_storage::BoxedFuture<
        '_,
        Result<extenddb_core::types::ContinuousBackupsDescription, StorageError>,
    > {
        unreachable!("not reached by the restore")
    }
    fn update_continuous_backups(
        &self,
        _: &str,
        _: &str,
        _: bool,
    ) -> extenddb_storage::BoxedFuture<
        '_,
        Result<extenddb_core::types::ContinuousBackupsDescription, StorageError>,
    > {
        unreachable!("not reached by the restore")
    }
}

/// `RestoreTableFromBackup` runs its restore in a task of its own. The task
/// does not inherit the request scope, so the engine carries it across; the
/// `backup storage error` line written inside the task names the request.
#[tokio::test]
async fn the_detached_restore_carries_the_scope_into_its_task() {
    let body = serde_json::json!({
        "BackupArn": "arn:aws:dynamodb:us-east-1:123456789012:table/src/backup/01700000000000-abcdef12",
        "TargetTableName": "restored",
    });
    let limits = Arc::new(extenddb_core::limits::LimitsConfig::default());
    let logged = with_scope(
        RequestScope::new(
            "req-restore".to_owned(),
            "RestoreTableFromBackup".to_owned(),
        ),
        error_events_during_async(async {
            let result = crate::backup::restore_table_from_backup_detached(
                Arc::new(FaultingBackups),
                Arc::from("123456789012"),
                Arc::from("us-east-1"),
                limits,
                body,
                |_, _| async {},
            )
            .await;
            assert!(matches!(result, Err(DynamoDbError::InternalServerError(_))));
        }),
    )
    .await;
    assert_eq!(
        logged.len(),
        1,
        "one fault line for the failed source lookup"
    );
    assert_eq!(logged[0].request_id.as_deref(), Some("req-restore"));
    assert_eq!(
        logged[0].operation.as_deref(),
        Some("RestoreTableFromBackup")
    );
    assert_eq!(logged[0].message, "backup storage error");
    assert_eq!(logged[0].target, "extenddb_engine::backup");
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The fault log line names the request it belongs to. Each engine mapping
//! arm that turns a `StorageError` into a redacted 500 or a retryable 503
//! writes the line through `log_storage_fault!`, which reads the request id
//! and operation from the request scope the server sets around the request.
//! Inside a scope the two fields carry the request's values; outside one the
//! line carries neither. The macro expands at its call site, so each line
//! keeps the site's own target and level. The detached restore re-enters the
//! scope in its spawned task, so the store read its request makes there
//! names the request too.

use std::sync::{Arc, Mutex};

use extenddb_core::error::DynamoDbError;
use extenddb_storage::error::StorageError;
use extenddb_storage::request_scope::{RequestScope, with_scope};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

/// What the subscriber saw of one error- or warn-level event: the
/// `request_id` and `operation` fields (`None` when the field was not
/// recorded), the message, the event's static target and its level.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Seen {
    request_id: Option<String>,
    operation: Option<String>,
    message: String,
    target: String,
    level: String,
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
        if *event.metadata().level() <= Level::WARN {
            let mut seen = Seen {
                target: event.metadata().target().to_owned(),
                level: event.metadata().level().to_string(),
                ..Seen::default()
            };
            event.record(&mut seen);
            self.0.lock().unwrap().push(seen);
        }
    }
}

/// Runs `f` under a fresh subscriber and returns every error and warn event
/// it logged. A second registered subscriber makes `tracing` compute a newly hit
/// callsite's interest from every registered subscriber, so a parallel test
/// with none cannot leave an error callsite disabled for this one.
fn error_events_during(f: impl FnOnce()) -> Vec<Seen> {
    let _second = tracing::Dispatch::new(tracing_subscriber::registry());
    let captured = Captured::default();
    let events = Arc::clone(&captured.0);
    let subscriber = tracing_subscriber::registry().with(captured);
    let _guard = tracing::subscriber::set_default(subscriber);
    f();
    events.lock().unwrap().clone()
}

/// Runs `fut` under a fresh subscriber and returns every error and warn
/// event it logged, with the second subscriber of [`error_events_during`].
async fn error_events_during_async<F: std::future::Future>(fut: F) -> Vec<Seen> {
    let _second = tracing::Dispatch::new(tracing_subscriber::registry());
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
/// status the client gets, the message text and the level its line has
/// always carried, and the target of the module that owns the arm.
struct Site {
    name: &'static str,
    error: fn() -> StorageError,
    map: fn(StorageError) -> DynamoDbError,
    status: u16,
    message: &'static str,
    level: &'static str,
    target: &'static str,
}

fn internal() -> StorageError {
    StorageError::Internal("driver text".to_owned())
}

/// Every engine arm that maps a `StorageError` to a redacted 500 or to a
/// retryable 503 and logs the fault.
fn sites() -> Vec<Site> {
    vec![
        Site {
            name: "create_table::storage_err_to_dynamo, Internal",
            error: internal,
            map: crate::create_table::storage_err_to_dynamo,
            status: 500,
            message: "storage internal error",
            level: "ERROR",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "create_table::storage_err_to_dynamo, IdempotentReplay",
            error: || StorageError::IdempotentReplay,
            map: crate::create_table::storage_err_to_dynamo,
            status: 500,
            message: "Unexpected idempotency error in generic error handler",
            level: "ERROR",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "create_table::storage_err_to_dynamo, AlreadyExists",
            error: || StorageError::AlreadyExists("row text".to_owned()),
            map: crate::create_table::storage_err_to_dynamo,
            status: 500,
            message: "unexpected already-exists storage error",
            level: "ERROR",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "create_table::storage_err_to_dynamo, Transient",
            error: || StorageError::Transient("driver text".to_owned()),
            map: crate::create_table::storage_err_to_dynamo,
            status: 503,
            message: "transient storage error",
            level: "WARN",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "create_table::storage_err_to_dynamo, SnapshotExpired",
            error: || StorageError::SnapshotExpired("driver text".to_owned()),
            map: crate::create_table::storage_err_to_dynamo,
            status: 503,
            message: "storage snapshot expired",
            level: "WARN",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "create_table::storage_err_to_dynamo, Connection",
            error: || StorageError::Connection("driver text".to_owned()),
            map: crate::create_table::storage_err_to_dynamo,
            status: 503,
            message: "storage connection error",
            level: "ERROR",
            target: "extenddb_engine::create_table",
        },
        Site {
            name: "backup::storage_err_to_dynamo",
            error: internal,
            map: crate::backup::storage_err_to_dynamo,
            status: 500,
            message: "backup storage error",
            level: "ERROR",
            target: "extenddb_engine::backup",
        },
        Site {
            name: "update_table::update_table_err_to_dynamo",
            error: internal,
            map: |e| crate::update_table::update_table_err_to_dynamo(e, "t"),
            status: 500,
            message: "storage internal error",
            level: "ERROR",
            target: "extenddb_engine::update_table",
        },
        Site {
            name: "streams::storage_to_dynamo",
            error: internal,
            map: crate::streams::storage_to_dynamo,
            status: 500,
            message: "storage internal error",
            level: "ERROR",
            target: "extenddb_engine::streams",
        },
        Site {
            name: "ttl::storage_to_dynamo",
            error: internal,
            map: crate::ttl::storage_to_dynamo,
            status: 500,
            message: "storage internal error",
            level: "ERROR",
            target: "extenddb_engine::ttl",
        },
        Site {
            name: "sanitize_storage_error",
            error: internal,
            map: crate::sanitize_storage_error,
            status: 500,
            message: "storage internal error",
            level: "ERROR",
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
                assert_eq!(err.status_code(), site.status, "{}", site.name);
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
                level: site.level.to_owned(),
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
            assert_eq!(err.status_code(), site.status, "{}", site.name);
        });
        assert_eq!(
            logged,
            vec![Seen {
                request_id: None,
                operation: None,
                message: site.message.to_owned(),
                target: site.target.to_owned(),
                level: site.level.to_owned(),
            }],
            "{}: a fault outside a request carried a request id or operation",
            site.name
        );
    }
}

/// Runs `RestoreTableFromBackup` through its detached entry point inside a
/// scope for `request_id`, checks that the client gets a redacted 500, and
/// returns the error and warn lines written meanwhile.
async fn detached_restore_fault_lines(
    backups: Arc<crate::backup::tests::Fake>,
    runtime: crate::backup::BackupRuntime,
    body: serde_json::Value,
    request_id: &str,
) -> Vec<Seen> {
    let limits = Arc::new(extenddb_core::limits::LimitsConfig::default());
    with_scope(
        RequestScope::new(request_id.to_owned(), "RestoreTableFromBackup".to_owned()),
        error_events_during_async(async {
            let result = crate::backup::restore_table_from_backup_detached(
                backups,
                Arc::new(runtime),
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
    .await
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
    // A backup engine whose source lookup fails with a storage fault. It has
    // no catalog row for the ARN, so the restore reads the source next.
    let backups = crate::backup::tests::Fake::default();
    *backups.source_error.lock().unwrap() = Some(internal());
    let logged = detached_restore_fault_lines(
        Arc::new(backups),
        crate::backup::tests::runtime(),
        body,
        "req-restore",
    )
    .await;
    assert_eq!(
        logged,
        vec![Seen {
            request_id: Some("req-restore".to_owned()),
            operation: Some("RestoreTableFromBackup".to_owned()),
            message: "backup storage error".to_owned(),
            target: "extenddb_engine::backup".to_owned(),
            level: "ERROR".to_owned(),
        }],
        "one fault line for the failed source lookup, naming the request"
    );
}

/// Inside the same task, the restore request reads the manifest's checksum
/// sidecar and the manifest from the backup store. A read that fails answers
/// the client a redacted 500, and its line names the request.
#[tokio::test]
async fn the_restore_tasks_failed_store_read_names_the_request() {
    let (backups, runtime, body) = crate::backup::tests::restore_from_an_unreadable_store();
    let logged = detached_restore_fault_lines(backups, runtime, body, "req-store").await;
    assert_eq!(
        logged,
        vec![Seen {
            request_id: Some("req-store".to_owned()),
            operation: Some("RestoreTableFromBackup".to_owned()),
            message: "backup arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/\
                      01791976431438-73d8d5bc: reading extenddb-manifest.sha256 from the store \
                      failed"
                .to_owned(),
            target: "extenddb_engine::backup::restore".to_owned(),
            level: "ERROR".to_owned(),
        }],
        "one fault line for the failed store read, naming the request"
    );
}

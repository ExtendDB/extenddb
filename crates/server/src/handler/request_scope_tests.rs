// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The request scope the handler sets from authentication through dispatch.
//! Three tests drive a request through `handle_request` against an in-memory
//! engine: one checks the bounds (no scope on the task before the request,
//! none after the response, none at the `request complete` event the response
//! segment writes); one reads the scope current when the auth provider runs,
//! the first call inside the span; one reads it at the engine's dispatch entry
//! event, the last segment inside the span, and checks it carries the request
//! id the response returned in `x-amzn-RequestId`, the operation and both SDK
//! correlation headers. The remaining tests check `request_scope_for` and
//! `header_string` on their own. That every `.await` reached inside the span
//! sees the scope, including the engine's error mappers and the detached
//! restore, is tested in the storage crate (`request_scope`) and the engine
//! crate (`request_scope_log_tests`).

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use extenddb_auth::{AuthIdentity, AuthProvider};
use extenddb_core::error::DynamoDbError;
use extenddb_storage::request_scope::{RequestScope, current};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

use super::throttle_wire_tests::{build_state, build_state_with};
use super::{MAX_SDK_HEADER_LEN, header_string, request_scope_for};
use crate::AppState;

/// Records the scope current on the task at two events: the engine's dispatch
/// entry (target `extenddb_engine::dispatch`, the last segment inside the
/// span) and the handler's `request complete` (the response segment, outside
/// the span).
#[derive(Default)]
struct ScopeAtEvents {
    at_dispatch: Arc<Mutex<Vec<Option<RequestScope>>>>,
    at_complete: Arc<Mutex<Vec<Option<RequestScope>>>>,
}

impl<S: Subscriber> Layer<S> for ScopeAtEvents {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() == "extenddb_engine::dispatch" {
            self.at_dispatch.lock().unwrap().push(current());
            return;
        }
        let mut message = Message::default();
        event.record(&mut message);
        if message.0 == "request complete" {
            self.at_complete.lock().unwrap().push(current());
        }
    }
}

#[derive(Default)]
struct Message(String);

impl tracing::field::Visit for Message {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// An auth provider that records the scope current when it runs, then answers
/// with the default account like the fixture's stub.
struct ScopeAtAuth {
    account_id: String,
    seen: Arc<Mutex<Vec<Option<RequestScope>>>>,
}

#[async_trait::async_trait]
impl AuthProvider for ScopeAtAuth {
    async fn authenticate(
        &self,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<AuthIdentity, DynamoDbError> {
        self.seen.lock().unwrap().push(current());
        Ok(AuthIdentity::User {
            account_id: self.account_id.clone(),
            user_name: "dev".to_owned(),
        })
    }
}

const INVOCATION_ID: &str = "11111111-2222-3333-4444-555555555555";
const SDK_REQUEST: &str = "attempt=2; max=4";

async fn fire(
    state: Arc<AppState>,
    operation: &str,
    body: &'static str,
    extra: &[(&'static str, &str)],
) -> (u16, String) {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        format!("DynamoDB_20120810.{operation}")
            .parse()
            .expect("target header"),
    );
    for (name, value) in extra {
        headers.insert(*name, value.parse().expect("header value"));
    }
    let response = super::handle_request(
        State(state),
        "/".parse().expect("uri"),
        axum::extract::ConnectInfo(std::net::SocketAddr::from(([127, 0, 0, 1], 40001))),
        headers,
        Ok(Bytes::from(body)),
    )
    .await;
    let status = response.status().as_u16();
    let request_id = response
        .headers()
        .get("x-amzn-requestid")
        .and_then(|v| v.to_str().ok())
        .expect("response carries x-amzn-RequestId")
        .to_owned();
    (status, request_id)
}

fn expect_scope(seen: &Option<RequestScope>, request_id: &str, operation: &str) {
    let scope = seen
        .as_ref()
        .expect("the segment ran inside the request scope");
    assert_eq!(scope.request_id, request_id);
    assert_eq!(scope.operation, operation);
    assert_eq!(scope.sdk_invocation_id.as_deref(), Some(INVOCATION_ID));
    assert_eq!(scope.sdk_request.as_deref(), Some(SDK_REQUEST));
}

#[tokio::test]
async fn no_scope_on_the_task_before_the_request_or_after_the_response() {
    assert_eq!(current(), None, "ambient scope before the request");
    let (state, _) = build_state().await;
    let (status, _) = fire(state, "ListTables", "{}", &[]).await;
    assert_eq!(status, 200);
    assert_eq!(current(), None, "the scope leaked past the handler");
}

#[tokio::test]
async fn the_scope_is_current_when_the_auth_provider_runs() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let (state, _) = build_state_with(
        move |account_id| {
            Arc::new(ScopeAtAuth {
                account_id,
                seen: recorded,
            })
        },
        None,
    )
    .await;

    let (status, request_id) = fire(
        state,
        "ListTables",
        "{}",
        &[
            ("amz-sdk-invocation-id", INVOCATION_ID),
            ("amz-sdk-request", SDK_REQUEST),
        ],
    )
    .await;
    assert_eq!(status, 200);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "one authenticate call per request");
    expect_scope(&seen[0], &request_id, "ListTables");
}

#[tokio::test]
async fn the_scope_is_current_at_dispatch_entry_and_gone_at_request_complete() {
    let (state, _) = build_state().await;
    // Register the two callsites this test reads before the subscriber goes
    // in. While exactly one scoped subscriber exists, `tracing` computes a
    // newly hit callsite's interest from the hitting thread's own dispatcher,
    // so a sibling test with no subscriber first-hitting `request complete`
    // would cache it as skipped for the whole process. Hitting both here
    // registers them; `set_default` then recomputes every registered callsite
    // against this layer.
    let (status, _) = fire(Arc::clone(&state), "ListTables", "{}", &[]).await;
    assert_eq!(status, 200);
    let layer = ScopeAtEvents::default();
    let at_dispatch = Arc::clone(&layer.at_dispatch);
    let at_complete = Arc::clone(&layer.at_complete);
    let subscriber = tracing_subscriber::registry().with(layer);
    let _guard = tracing::subscriber::set_default(subscriber);

    let (status, request_id) = fire(
        state,
        "ListTables",
        "{}",
        &[
            ("amz-sdk-invocation-id", INVOCATION_ID),
            ("amz-sdk-request", SDK_REQUEST),
        ],
    )
    .await;
    assert_eq!(status, 200);

    let at_dispatch = at_dispatch.lock().unwrap();
    assert_eq!(at_dispatch.len(), 1, "one dispatch entry event per request");
    expect_scope(&at_dispatch[0], &request_id, "ListTables");

    // The response segment runs outside the span: the `request complete`
    // event it writes sees no scope.
    let at_complete = at_complete.lock().unwrap();
    assert_eq!(
        at_complete.as_slice(),
        &[None],
        "the scope reached the response segment"
    );
}

#[test]
fn the_scope_carries_the_request_id_operation_and_both_sdk_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "amz-sdk-invocation-id",
        INVOCATION_ID.parse().expect("header value"),
    );
    headers.insert(
        "amz-sdk-request",
        SDK_REQUEST.parse().expect("header value"),
    );

    let scope = request_scope_for("req-7c1d", "PutItem", &headers);

    assert_eq!(scope.request_id, "req-7c1d");
    assert_eq!(scope.operation, "PutItem");
    assert_eq!(scope.sdk_invocation_id.as_deref(), Some(INVOCATION_ID));
    assert_eq!(scope.sdk_request.as_deref(), Some(SDK_REQUEST));
}

#[test]
fn the_sdk_headers_are_absent_from_the_scope_when_the_client_sent_none() {
    let scope = request_scope_for("req-9a0b", "Query", &HeaderMap::new());

    assert_eq!(scope.request_id, "req-9a0b");
    assert_eq!(scope.operation, "Query");
    assert_eq!(scope.sdk_invocation_id, None);
    assert_eq!(scope.sdk_request, None);
}

#[test]
fn a_header_that_is_not_visible_ascii_reads_as_absent() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "amz-sdk-request",
        axum::http::HeaderValue::from_bytes(&[0xff, 0xfe]).expect("opaque header bytes"),
    );
    // `HeaderValue::to_str` accepts visible ASCII only; UTF-8 above 0x7e is
    // refused as well, so a value with an accented letter also reads as absent.
    headers.insert(
        "amz-sdk-invocation-id",
        axum::http::HeaderValue::from_bytes("caf\u{e9}".as_bytes()).expect("utf8 header bytes"),
    );
    assert_eq!(header_string(&headers, "amz-sdk-request"), None);
    assert_eq!(header_string(&headers, "amz-sdk-invocation-id"), None);
}

#[test]
fn a_header_longer_than_the_cap_reads_as_absent() {
    let mut headers = HeaderMap::new();
    let at_cap = "a".repeat(MAX_SDK_HEADER_LEN);
    let over_cap = "a".repeat(MAX_SDK_HEADER_LEN + 1);
    headers.insert(
        "amz-sdk-invocation-id",
        at_cap.parse().expect("header value"),
    );
    headers.insert("amz-sdk-request", over_cap.parse().expect("header value"));
    assert_eq!(
        header_string(&headers, "amz-sdk-invocation-id").as_deref(),
        Some(at_cap.as_str())
    );
    assert_eq!(header_string(&headers, "amz-sdk-request"), None);
}

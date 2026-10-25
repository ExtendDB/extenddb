// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The request observer, driven through `handle_request` against an in-memory
//! engine: one record per request that reached authentication, with the
//! status and error type the client received, the request id the response
//! carried, the table and account, the body hash and the SDK headers; a
//! refusal before dispatch (authentication, the throttle) is recorded with its
//! error type; a request the client abandons mid-flight is recorded with
//! status 0; a request refused before authentication is not recorded; without
//! an observer the handler builds no record.

use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use extenddb_auth::{AuthIdentity, AuthProvider};
use extenddb_core::error::DynamoDbError;
use serde_json::{Value, json};

use super::throttle_wire_tests::{build_state_with, create_table};
use crate::AppState;
use crate::extensions::{RequestObserver, RequestRecord, body_hash};

#[derive(Default)]
struct Recording(Mutex<Vec<RequestRecord>>);

impl RequestObserver for Recording {
    fn on_response(&self, record: RequestRecord) {
        self.0.lock().unwrap().push(record);
    }
}

impl Recording {
    fn records(&self) -> Vec<RequestRecord> {
        self.0.lock().unwrap().clone()
    }
}

/// The fixture's stub, with a switch to refuse.
struct SwitchAuth {
    account_id: String,
    refuse: bool,
}

#[async_trait::async_trait]
impl AuthProvider for SwitchAuth {
    async fn authenticate(
        &self,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<AuthIdentity, DynamoDbError> {
        if self.refuse {
            return Err(DynamoDbError::UnrecognizedClientException(
                "The security token included in the request is invalid.".to_owned(),
            ));
        }
        Ok(AuthIdentity::User {
            account_id: self.account_id.clone(),
            user_name: "dev".to_owned(),
        })
    }
}

/// A state with the recording observer installed.
async fn observed_state(refuse_auth: bool) -> (Arc<AppState>, String, Arc<Recording>) {
    let observer = Arc::new(Recording::default());
    let installed: Arc<dyn RequestObserver> = Arc::clone(&observer) as Arc<dyn RequestObserver>;
    let (state, account_id) = build_state_with(
        move |account_id| {
            Arc::new(SwitchAuth {
                account_id,
                refuse: refuse_auth,
            })
        },
        Some(installed),
    )
    .await;
    (state, account_id, observer)
}

const INVOCATION_ID: &str = "11111111-2222-3333-4444-555555555555";

async fn fire(state: &Arc<AppState>, operation: &str, body: &Value) -> (u16, String) {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        format!("DynamoDB_20120810.{operation}")
            .parse()
            .expect("target header"),
    );
    headers.insert(
        "amz-sdk-invocation-id",
        INVOCATION_ID.parse().expect("header value"),
    );
    headers.insert(
        "amz-sdk-request",
        "attempt=1; max=3".parse().expect("header value"),
    );
    let response = super::handle_request(
        State(state.clone()),
        "/".parse().expect("uri"),
        axum::extract::ConnectInfo(std::net::SocketAddr::from(([127, 0, 0, 1], 40002))),
        headers,
        Ok(Bytes::from(body.to_string())),
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

#[tokio::test]
async fn a_served_request_is_recorded_once_with_the_response_it_got() {
    let (state, account_id, observer) = observed_state(false).await;
    let body = json!({});
    let (status, request_id) = fire(&state, "ListTables", &body).await;
    assert_eq!(status, 200);

    let records = observer.records();
    assert_eq!(records.len(), 1);
    let r = &records[0];
    assert_eq!(r.request_id, request_id);
    assert_eq!(r.operation, "ListTables");
    assert_eq!(r.table_name, None);
    assert_eq!(r.account_id.as_deref(), Some(account_id.as_str()));
    assert_eq!(r.status, 200);
    assert_eq!(r.error_type, None);
    assert_eq!(r.body_hash, body_hash(body.to_string().as_bytes()));
    assert_eq!(r.sdk_invocation_id.as_deref(), Some(INVOCATION_ID));
    assert_eq!(r.sdk_request.as_deref(), Some("attempt=1; max=3"));
    assert!(r.latency_us > 0);
}

#[tokio::test]
async fn a_dispatch_error_is_recorded_with_its_error_type_and_table() {
    let (state, _, observer) = observed_state(false).await;
    let (status, request_id) = fire(&state, "DescribeTable", &json!({"TableName": "absent"})).await;
    assert_eq!(status, 400);

    let records = observer.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].request_id, request_id);
    assert_eq!(records[0].table_name.as_deref(), Some("absent"));
    assert_eq!(records[0].status, 400);
    assert_eq!(
        records[0].error_type.as_deref(),
        Some("ResourceNotFoundException")
    );
}

#[tokio::test]
async fn a_throttle_refusal_is_recorded_with_its_error_type() {
    let (state, account_id, observer) = observed_state(false).await;
    let table = "observed-throttle";
    create_table(
        &state,
        table,
        json!({"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}}),
    )
    .await;
    let before = observer.records().len();
    state.throttle.consume(&account_id, table, 0.0, 2.0);

    let (status, request_id) = fire(
        &state,
        "PutItem",
        &json!({"TableName": table, "Item": {"pk": {"S": "1"}}}),
    )
    .await;
    assert_eq!(status, 400);

    let records = observer.records();
    assert_eq!(
        records.len(),
        before + 1,
        "one record for the refused request"
    );
    let r = records.last().expect("record");
    assert_eq!(r.request_id, request_id);
    assert_eq!(r.operation, "PutItem");
    assert_eq!(r.table_name.as_deref(), Some(table));
    assert_eq!(r.account_id.as_deref(), Some(account_id.as_str()));
    assert_eq!(r.status, 400);
    assert_eq!(
        r.error_type.as_deref(),
        Some("ProvisionedThroughputExceededException")
    );
}

#[tokio::test]
async fn an_authentication_refusal_is_recorded_with_the_table_and_no_account() {
    let (state, _, observer) = observed_state(true).await;
    let (status, request_id) = fire(
        &state,
        "PutItem",
        &json!({"TableName": "t1", "Item": {"pk": {"S": "1"}}}),
    )
    .await;
    assert_eq!(status, 400);

    let records = observer.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].request_id, request_id);
    assert_eq!(records[0].operation, "PutItem");
    assert_eq!(records[0].table_name.as_deref(), Some("t1"));
    assert_eq!(records[0].account_id, None);
    assert_eq!(
        records[0].error_type.as_deref(),
        Some("UnrecognizedClientException")
    );
}

#[tokio::test]
async fn a_request_refused_before_authentication_is_not_recorded() {
    let (state, _, observer) = observed_state(false).await;
    let (status, _) = fire(&state, "NoSuchOperation", &json!({})).await;
    assert_eq!(status, 400);
    assert!(
        observer.records().is_empty(),
        "unknown operation left no record"
    );

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        "DynamoDB_20120810.ListTables"
            .parse()
            .expect("target header"),
    );
    let response = super::handle_request(
        State(state.clone()),
        "/".parse().expect("uri"),
        axum::extract::ConnectInfo(std::net::SocketAddr::from(([127, 0, 0, 1], 40002))),
        headers,
        Ok(Bytes::from("{not json")),
    )
    .await;
    assert_eq!(response.status().as_u16(), 400);
    assert!(
        observer.records().is_empty(),
        "a body that is not JSON left no record"
    );
}

#[tokio::test]
async fn an_abandoned_request_is_recorded_with_status_zero() {
    let (state, _, observer) = observed_state(false).await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        "DynamoDB_20120810.ListTables"
            .parse()
            .expect("target header"),
    );
    let fut = super::handle_request(
        State(state.clone()),
        "/".parse().expect("uri"),
        axum::extract::ConnectInfo(std::net::SocketAddr::from(([127, 0, 0, 1], 40002))),
        headers,
        Ok(Bytes::from("{}")),
    );
    // Poll once so the handler passes the pre-auth checks and arms the guard,
    // then drop the future as hyper does when the client disconnects.
    let mut fut = Box::pin(fut);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let polled = fut.as_mut().poll(&mut cx);
    assert!(
        polled.is_pending(),
        "the in-memory engine yields at least once before answering, so the request is in flight after one poll"
    );
    drop(fut);

    let records = observer.records();
    assert_eq!(records.len(), 1, "the abandoned request was recorded once");
    assert_eq!(records[0].status, 0);
    assert_eq!(records[0].operation, "ListTables");
    assert_eq!(records[0].error_type, None);
}

#[tokio::test]
async fn without_an_observer_the_request_is_served_as_before() {
    let (state, _) = build_state_with(
        |account_id| {
            Arc::new(SwitchAuth {
                account_id,
                refuse: false,
            })
        },
        None,
    )
    .await;
    let (status, _) = fire(&state, "ListTables", &json!({})).await;
    assert_eq!(status, 200);
}

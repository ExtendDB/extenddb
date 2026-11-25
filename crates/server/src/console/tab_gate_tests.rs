// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The session check of an extension tab, `require_console_session`, for
//! each state of the session a request carries and each answer the catalog
//! gives when the check reads the principal's password hash. The tab page
//! behind the check answers with the admin flag it was given and whether the
//! form field `x` reached it, and counts its calls.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::body::Body;
use axum::http::header::{CONTENT_TYPE, COOKIE, LOCATION};
use axum::http::{Request, StatusCode};
use axum::routing::any;
use axum::{Extension, Router};
use tower::ServiceExt;

use super::test_catalog::{CountingStore, StoredHash, console_state, session_hash};
use super::{ConsoleIdentity, require_console_session};
use crate::console::session::CredentialStamp;
use crate::management::CallerIdentity;

/// Who logged in for the session the request carries.
#[derive(Clone, Copy, Debug)]
enum Holder {
    Admin,
    IamUser,
    /// The request carries no session cookie.
    Nobody,
}

/// The `_csrf` field of the request's form body.
#[derive(Clone, Copy, Debug)]
enum Csrf {
    Absent,
    SessionToken,
    /// A token of the same length that is not the session's.
    OtherToken,
    /// The session's token in a JSON body, which is not a form.
    InJson,
}

struct Row {
    name: &'static str,
    holder: Holder,
    stored: StoredHash,
    method: &'static str,
    csrf: Csrf,
    status: StatusCode,
    location: Option<&'static str>,
    body: &'static str,
}

const LOGIN: Option<&str> = Some("/console/login");

#[rustfmt::skip]
const ROWS: &[Row] = &[
    Row { name: "admin", holder: Holder::Admin, stored: StoredHash::LoginHash, method: "GET", csrf: Csrf::Absent, status: StatusCode::OK, location: None, body: "admin=true x=false" },
    Row { name: "IAM user", holder: Holder::IamUser, stored: StoredHash::LoginHash, method: "GET", csrf: Csrf::Absent, status: StatusCode::OK, location: None, body: "admin=false x=false" },
    Row { name: "no cookie", holder: Holder::Nobody, stored: StoredHash::LoginHash, method: "GET", csrf: Csrf::Absent, status: StatusCode::SEE_OTHER, location: LOGIN, body: "" },
    Row { name: "deleted principal", holder: Holder::IamUser, stored: StoredHash::Missing, method: "GET", csrf: Csrf::Absent, status: StatusCode::SEE_OTHER, location: LOGIN, body: "" },
    Row { name: "password changed", holder: Holder::Admin, stored: StoredHash::Changed, method: "GET", csrf: Csrf::Absent, status: StatusCode::SEE_OTHER, location: LOGIN, body: "" },
    Row { name: "catalog unreadable", holder: Holder::Admin, stored: StoredHash::Unreadable, method: "GET", csrf: Csrf::Absent, status: StatusCode::SERVICE_UNAVAILABLE, location: None, body: "The catalog is unavailable. Try again." },
    Row { name: "POST with the session's token", holder: Holder::IamUser, stored: StoredHash::LoginHash, method: "POST", csrf: Csrf::SessionToken, status: StatusCode::OK, location: None, body: "admin=false x=true" },
    Row { name: "POST without a token", holder: Holder::Admin, stored: StoredHash::LoginHash, method: "POST", csrf: Csrf::Absent, status: StatusCode::FORBIDDEN, location: None, body: "CSRF token mismatch" },
    Row { name: "POST with another token", holder: Holder::Admin, stored: StoredHash::LoginHash, method: "POST", csrf: Csrf::OtherToken, status: StatusCode::FORBIDDEN, location: None, body: "CSRF token mismatch" },
    Row { name: "POST with the token in JSON", holder: Holder::Admin, stored: StoredHash::LoginHash, method: "POST", csrf: Csrf::InJson, status: StatusCode::FORBIDDEN, location: None, body: "CSRF token mismatch" },
    Row { name: "DELETE without a token", holder: Holder::Admin, stored: StoredHash::LoginHash, method: "DELETE", csrf: Csrf::Absent, status: StatusCode::FORBIDDEN, location: None, body: "CSRF token mismatch" },
    Row { name: "PROPFIND without a token", holder: Holder::Admin, stored: StoredHash::LoginHash, method: "PROPFIND", csrf: Csrf::Absent, status: StatusCode::FORBIDDEN, location: None, body: "CSRF token mismatch" },
    Row { name: "POST with no cookie", holder: Holder::Nobody, stored: StoredHash::LoginHash, method: "POST", csrf: Csrf::Absent, status: StatusCode::SEE_OTHER, location: LOGIN, body: "" },
    Row { name: "POST for a deleted principal", holder: Holder::Admin, stored: StoredHash::Missing, method: "POST", csrf: Csrf::SessionToken, status: StatusCode::SEE_OTHER, location: LOGIN, body: "" },
];

#[tokio::test]
async fn the_tab_session_check_answers_for_each_session_state() {
    for row in ROWS {
        let store = Arc::new(CountingStore::answering(row.stored));
        let state = console_state(store.clone());
        let identity = match row.holder {
            Holder::Admin => Some(CallerIdentity::Admin("admin".to_owned())),
            Holder::IamUser => Some(CallerIdentity::IamUser {
                account_id: "444455556666".to_owned(),
                user_name: "bob".to_owned(),
            }),
            Holder::Nobody => None,
        };
        let session = match identity {
            Some(identity) => {
                let stamp = CredentialStamp::of(&session_hash());
                Some(state.sessions.create(identity, stamp).await)
            }
            None => None,
        };
        let form_type = "application/x-www-form-urlencoded";
        let (content_type, body) = match (row.csrf, &session) {
            _ if row.method == "GET" => (form_type, String::new()),
            (Csrf::SessionToken, Some((_, token))) => (form_type, format!("_csrf={token}&x=1")),
            (Csrf::OtherToken, Some((_, token))) => {
                (form_type, format!("_csrf={}&x=1", "0".repeat(token.len())))
            }
            (Csrf::InJson, Some((_, token))) => (
                "application/json",
                format!(r#"{{"_csrf":"{token}","x":1}}"#),
            ),
            _ => (form_type, "x=1".to_owned()),
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let page = Router::new()
            .route(
                "/",
                any(
                    move |identity: Extension<ConsoleIdentity>, body: String| async move {
                        seen.fetch_add(1, Ordering::SeqCst);
                        format!("admin={} x={}", identity.is_admin, body.ends_with("x=1"))
                    },
                ),
            )
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&state),
                require_console_session,
            ));
        let mut request = Request::builder()
            .method(row.method)
            .uri("/")
            .header(CONTENT_TYPE, content_type);
        if let Some((cookie, _)) = &session {
            request = request.header(COOKIE, format!("extenddb_session={cookie}"));
        }
        let response = page
            .oneshot(request.body(Body::from(body)).expect("request"))
            .await
            .expect("infallible");

        let name = row.name;
        assert_eq!(response.status(), row.status, "{name}");
        let location = response
            .headers()
            .get(LOCATION)
            .map(|v| v.to_str().expect("ASCII location").to_owned());
        assert_eq!(location.as_deref(), row.location, "{name}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(String::from_utf8_lossy(&body), row.body, "{name}");
        let expected_calls = usize::from(row.status == StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls, "{name}");
        let expected_reads = usize::from(session.is_some());
        assert_eq!(store.hash_reads(), expected_reads, "{name}");
        assert_eq!(store.calls(), 0, "{name}");
    }
}

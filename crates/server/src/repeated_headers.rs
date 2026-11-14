// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Refuse a request that repeats a header the server reads as one value.
//!
//! A request carrying more than one `host` or `authorization` header is
//! refused before routing, as the service's front end does (400, connection
//! closed, no request id). Either duplicate is a header-confusion shape a
//! proxy in front of this server could resolve differently from the server:
//! the SigV4 check binds the host it signs, the console binds a session to a
//! host, and the management API reads one Basic credential. Applied as a
//! layer on the whole router so the DynamoDB route, `/console`,
//! `/management`, `/health` and `/metrics` all answer the same way.

use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Whether `headers` carries more than one `host` or `authorization` value.
fn has_repeated_host_or_authorization(headers: &HeaderMap) -> bool {
    headers.get_all(header::HOST).iter().count() > 1
        || headers.get_all(header::AUTHORIZATION).iter().count() > 1
}

/// The refusal: 400 with the connection closed and a plain text body.
fn refusal() -> Response {
    (
        StatusCode::BAD_REQUEST,
        [(header::CONNECTION, "close")],
        "400 Bad Request",
    )
        .into_response()
}

/// Middleware: refuse a request with a repeated `host` or `authorization`
/// header; pass every other request on.
pub(crate) async fn refuse_repeated_headers(request: Request, next: Next) -> Response {
    if has_repeated_host_or_authorization(request.headers()) {
        return refusal();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::{get, post};
    use tower::ServiceExt;

    /// A router shaped like the server's: a data-plane route, a nested
    /// console and a nested management API, with the guard layered on the
    /// whole thing after nesting.
    fn router() -> Router {
        let console = Router::new().route("/login", get(|| async { "login" }));
        let management = Router::new().route("/accounts", get(|| async { "accounts" }));
        Router::new()
            .route("/", post(|| async { "data plane" }))
            .route("/health", get(|| async { "healthy" }))
            .nest("/console", console)
            .nest("/management", management)
            .layer(axum::middleware::from_fn(refuse_repeated_headers))
    }

    async fn send(
        headers: &[(&str, &str)],
        method: &str,
        path: &str,
    ) -> (StatusCode, Option<String>, String) {
        let mut builder = axum::http::Request::builder().method(method).uri(path);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let response = router()
            .oneshot(builder.body(Body::empty()).expect("request"))
            .await
            .expect("response");
        let status = response.status();
        let connection = response
            .headers()
            .get(header::CONNECTION)
            .map(|v| v.to_str().expect("ascii").to_owned());
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (
            status,
            connection,
            String::from_utf8(body.to_vec()).expect("utf8"),
        )
    }

    #[tokio::test]
    async fn a_repeated_host_is_refused_on_every_route() {
        for (method, path) in [
            ("POST", "/"),
            ("GET", "/health"),
            ("GET", "/console/login"),
            ("GET", "/management/accounts"),
            ("GET", "/no-such-route"),
        ] {
            let (status, connection, body) = send(
                &[("host", "a.example"), ("host", "b.example")],
                method,
                path,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}");
            assert_eq!(connection.as_deref(), Some("close"), "{method} {path}");
            assert_eq!(body, "400 Bad Request", "{method} {path}");
        }
    }

    #[tokio::test]
    async fn a_repeated_authorization_is_refused_on_every_route() {
        for (method, path) in [
            ("POST", "/"),
            ("GET", "/console/login"),
            ("GET", "/management/accounts"),
        ] {
            let (status, connection, body) = send(
                &[
                    ("host", "a.example"),
                    ("authorization", "Basic YWRtaW46eA=="),
                    ("authorization", "Basic YWRtaW46eQ=="),
                ],
                method,
                path,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}");
            assert_eq!(connection.as_deref(), Some("close"), "{method} {path}");
            assert_eq!(body, "400 Bad Request", "{method} {path}");
        }
    }

    #[tokio::test]
    async fn single_headers_and_repeated_other_headers_pass() {
        let (status, _, body) = send(
            &[
                ("host", "a.example"),
                ("authorization", "Basic YWRtaW46eA=="),
                ("x-amz-date", "20260101T000000Z"),
                ("x-amz-date", "20260102T000000Z"),
            ],
            "GET",
            "/console/login",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "login");
    }
}

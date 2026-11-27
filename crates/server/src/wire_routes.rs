// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The router's fallback: the answer to every request no other route takes.
//!
//! A path under `/management` or `/console` that their routers do not take
//! keeps the empty 404 it had before: axum hands such a path to this fallback,
//! since neither nested router has a fallback of its own.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{FromRequest, Request, State};
use axum::handler::Handler;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::AppState;
use crate::response::{AMZ_JSON_1_0, wire_response};

/// The second media type the API is served in.
const JSON: &str = "application/json";

/// The service's 404 page: 272 bytes, no newline after `</html>`.
const NOT_FOUND_PAGE: &str = concat!(
    r#"<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "#,
    r#""http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">"#,
    "\n",
    r#"<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">"#,
    "\n<head>\n  <title>Page Not Found</title>\n</head>\n<body>Page Not Found</body>\n</html>",
);

/// Where `start_server` nests ExtendDB's own routers.
/// Checked here, not by fallbacks on them: a server with no catalog store mounts neither.
const EXTENDDB_PREFIXES: [&str; 2] = ["/management", "/console"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Answer {
    Api,
    Health,
    NotFoundPage,
    /// The empty 404 of a path under an ExtendDB router.
    EmptyNotFound,
}

fn answer(method: &Method, path: &str, query: Option<&str>) -> Answer {
    let under = |prefix: &str| {
        path.strip_prefix(prefix)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    };
    if EXTENDDB_PREFIXES.into_iter().any(under) {
        return Answer::EmptyNotFound;
    }
    match *method {
        Method::POST => Answer::Api,
        Method::GET if path == "/ping" || (path == "/" && query.is_none()) => Answer::Health,
        _ => Answer::NotFoundPage,
    }
}

fn served_media_type(headers: &HeaderMap) -> Option<&'static str> {
    let value = headers.get(header::CONTENT_TYPE)?.to_str().ok()?;
    let essence = value.split(';').next().unwrap_or_default().trim();
    [AMZ_JSON_1_0, JSON]
        .into_iter()
        .find(|served| essence.eq_ignore_ascii_case(served))
}

pub(crate) async fn fallback(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let uri = request.uri();
    match answer(request.method(), uri.path(), uri.query()) {
        Answer::Api => serve_api(state, request).await,
        Answer::Health => health(&state.region),
        Answer::NotFoundPage => not_found_page(),
        Answer::EmptyNotFound => StatusCode::NOT_FOUND.into_response(),
    }
}

/// The API, through the handler `POST /` had, with its extractors. In another
/// media type the service reads the body through the limit before it answers
/// the 404 page, so a body over it gets 413 however it is framed (2026-11-17).
async fn serve_api(state: Arc<AppState>, request: Request) -> Response {
    let Some(media_type) = served_media_type(request.headers()) else {
        let request_id = uuid::Uuid::new_v4().to_string();
        return match Bytes::from_request(request, &state).await {
            Ok(_) => not_found_page(),
            Err(rejection) => crate::handler::body_refusal(&rejection, &request_id),
        };
    };
    let mut response = Handler::call(crate::handler::handle_request, request, state).await;
    let headers = response.headers_mut();
    if media_type == JSON
        && headers
            .get(header::CONTENT_TYPE)
            .is_some_and(|v| v == AMZ_JSON_1_0)
    {
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(JSON));
    }
    response
}

/// The health text, with the server's region where the service names its own.
fn health(region: &str) -> Response {
    let body = format!("healthy: dynamodb.{region}.amazonaws.com ").into_bytes();
    let request_id = uuid::Uuid::new_v4().to_string();
    wire_response(StatusCode::OK, Some("text/plain"), body, &request_id)
}

fn not_found_page() -> Response {
    let body = NOT_FOUND_PAGE.as_bytes().to_vec();
    let request_id = uuid::Uuid::new_v4().to_string();
    wire_response(StatusCode::NOT_FOUND, None, body, &request_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rows the wire tests do not send.
    #[test]
    fn each_request_gets_the_services_answer() {
        let cases = [
            (Method::POST, "/managementx", Answer::Api),
            (Method::GET, "/consoles", Answer::NotFoundPage),
            (Method::HEAD, "/console/no-such-page", Answer::EmptyNotFound),
        ];
        for (method, path, expected) in cases {
            assert_eq!(answer(&method, path, None), expected, "{method} {path}");
        }
    }

    #[test]
    fn the_not_found_page_is_272_bytes() {
        assert_eq!(NOT_FOUND_PAGE.len(), 272);
    }
}

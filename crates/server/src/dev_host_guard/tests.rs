// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit and router-level tests for the developer-mode host guard.

use super::*;
use axum::body::Body;
use axum::routing::{get, post};
use tower::ServiceExt as _;

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.append(
            axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    h
}

fn uri(s: &str) -> Uri {
    s.parse().unwrap()
}

fn allow_path(path: &str, headers: &[(&str, &str)], allowed: &[&str]) -> bool {
    let names: Vec<String> = allowed.iter().map(|s| (*s).to_owned()).collect();
    matches!(
        evaluate(&self::headers(headers), &uri(path), &names),
        Decision::Allow
    )
}

fn allow(headers: &[(&str, &str)], allowed: &[&str]) -> bool {
    allow_path("/", headers, allowed)
}

/// The headers Node's built-in `fetch` (undici) sends with no caller-set
/// headers, as captured from Node 22.
const UNDICI_HEADERS: [(&str, &str); 4] = [
    ("sec-fetch-mode", "cors"),
    ("accept", "*/*"),
    ("accept-language", "*"),
    ("user-agent", "node"),
];

/// Admin HTTP Basic credentials. The guard runs before authentication, so the
/// value only has to look like what a page that knows the password sends.
const ADMIN_AUTH: &str = "Basic YWRtaW46YWRtaW4tcGFzc3dvcmQ=";

// --- browser-marker detection ---

#[test]
fn non_browser_request_has_no_marker() {
    assert!(!is_browser_request(&headers(&[("host", "rebind.example")])));
    assert!(!is_browser_request(&headers(&[(
        "x-amz-target",
        "DynamoDB_20120810.ListTables"
    )])));
}

#[test]
fn origin_or_sec_fetch_site_is_a_marker() {
    assert!(is_browser_request(&headers(&[("origin", "http://x")])));
    assert!(is_browser_request(&headers(&[("origin", "null")])));
    assert!(is_browser_request(&headers(&[(
        "sec-fetch-site",
        "same-origin"
    )])));
}

#[test]
fn undici_fetch_headers_are_not_a_browser_request() {
    assert!(!is_browser_request(&headers(&UNDICI_HEADERS)));
}

#[test]
fn other_sec_fetch_headers_alone_are_not_a_marker() {
    assert!(!is_browser_request(&headers(&[("sec-fetch-mode", "cors")])));
    assert!(!is_browser_request(&headers(&[(
        "sec-fetch-dest",
        "empty"
    )])));
    assert!(!is_browser_request(&headers(&[("sec-fetch-user", "?1")])));
}

// --- admin paths ---

#[test]
fn admin_path_covers_management_and_metrics_and_below() {
    for path in [
        "/management",
        "/management/",
        "/management/accounts",
        "/management/accounts/123456789012/users",
        "/metrics",
        "/metrics/",
    ] {
        assert!(is_admin_path(path), "{path} is an admin path");
    }
    for path in [
        "/",
        "/health",
        "/managementx",
        "/metricsx",
        "/console/metrics",
        "/console/metrics-data",
        "/console/login",
    ] {
        assert!(!is_admin_path(path), "{path} is not an admin path");
    }
}

// --- host parsing ---

#[test]
fn host_label_strips_port_brackets_and_one_trailing_dot() {
    assert_eq!(host_label("rebind.example:18602"), Some("rebind.example"));
    assert_eq!(host_label("127.0.0.1:18602"), Some("127.0.0.1"));
    assert_eq!(host_label("[::1]:18602"), Some("::1"));
    assert_eq!(host_label("[::1]"), Some("::1"));
    assert_eq!(host_label("localhost."), Some("localhost"));
    assert_eq!(host_label("::1:7"), None); // unbracketed IPv6 is malformed
    assert_eq!(host_label("[::1]junk"), None);
    assert_eq!(host_label("[localhost]"), None);
    assert_eq!(host_label("[127.0.0.1]"), None);
    assert_eq!(host_label(""), None);
    assert_eq!(host_label("."), None);
}

#[test]
fn classify_ip_dns_and_invalid() {
    assert_eq!(classify("127.0.0.1"), HostKind::Ip);
    assert_eq!(classify("::1"), HostKind::Ip);
    assert_eq!(classify("localhost"), HostKind::Dns);
    assert_eq!(classify("app.localhost"), HostKind::Dns);
    assert_eq!(classify("under_score.test"), HostKind::Dns);
    assert_eq!(classify("bad host"), HostKind::Invalid);
    assert_eq!(classify("a..b"), HostKind::Invalid);
    assert_eq!(classify(&"a".repeat(64)), HostKind::Invalid); // label too long
}

#[test]
fn loopback_name_covers_localhost_and_dot_localhost() {
    assert!(is_loopback_name("localhost"));
    assert!(is_loopback_name("LocalHost"));
    assert!(is_loopback_name("app.localhost"));
    assert!(is_loopback_name("a.b.localhost"));
    assert!(!is_loopback_name(".localhost"));
    assert!(!is_loopback_name("notlocalhost"));
    assert!(!is_loopback_name("localhost.evil.com"));
}

// --- the decision ---

#[test]
fn rebinding_browser_host_is_refused() {
    assert!(!allow(
        &[
            ("host", "rebind.example:1"),
            ("origin", "http://rebind.example:1")
        ],
        &[]
    ));
    // each marker on its own
    assert!(!allow(
        &[("host", "rebind.example:1"), ("origin", "null")],
        &[]
    ));
    assert!(!allow(
        &[
            ("host", "rebind.example:1"),
            ("sec-fetch-site", "same-origin")
        ],
        &[]
    ));
}

#[test]
fn undici_fetch_with_a_compose_host_is_served() {
    let mut with_host = UNDICI_HEADERS.to_vec();
    with_host.push(("host", "extenddb:18000"));
    assert!(allow(&with_host, &[]));
}

#[test]
fn loopback_and_ip_browser_hosts_are_served() {
    for host in ["localhost:1", "127.0.0.1:1", "[::1]:1", "app.localhost:1"] {
        assert!(
            allow(&[("host", host), ("origin", "http://x")], &[]),
            "{host} should be served"
        );
    }
}

#[test]
fn listed_browser_host_is_served_case_insensitively() {
    assert!(allow(
        &[("host", "Dev.Test:1"), ("origin", "http://x")],
        &["dev.test"]
    ));
    assert!(!allow(
        &[("host", "other.test:1"), ("origin", "http://x")],
        &["dev.test"]
    ));
}

#[test]
fn non_browser_request_is_served_whatever_the_host() {
    // No marker: a Compose service name, an SDK Host, anything.
    assert!(allow(&[("host", "rebind.example:1")], &[]));
    assert!(allow(&[("host", "extenddb:1")], &[]));
    assert!(allow(&[("host", "host.docker.internal:1")], &[]));
}

#[test]
fn admin_path_is_checked_on_the_host_alone() {
    let rebind = [("host", "rebind.example:1"), ("authorization", ADMIN_AUTH)];
    assert!(!allow_path("/management/accounts", &rebind, &[]));
    assert!(!allow_path("/metrics", &rebind, &[]));
    for host in ["localhost:1", "127.0.0.1:1", "dev.test:1"] {
        let marked_none = [("host", host), ("authorization", ADMIN_AUTH)];
        assert!(
            allow_path("/management/accounts", &marked_none, &["dev.test"]),
            "{host} should be served"
        );
    }
    // The other routes keep the marker rule.
    assert!(allow_path("/health", &rebind, &[]));
    assert!(allow_path("/console/login", &rebind, &[]));
}

#[test]
fn missing_repeated_and_mismatched_host_are_refused() {
    // Missing Host on a browser request (no authority in a "/" uri).
    assert!(matches!(
        evaluate(&headers(&[("origin", "http://x")]), &uri("/"), &[]),
        Decision::Reject(RejectReason::MissingHost)
    ));
    // Missing Host on an admin route, no marker.
    assert!(matches!(
        evaluate(&headers(&[]), &uri("/management/accounts"), &[]),
        Decision::Reject(RejectReason::MissingHost)
    ));
    // Repeated Host.
    assert!(matches!(
        evaluate(
            &headers(&[
                ("host", "localhost"),
                ("host", "rebind.example"),
                ("origin", "http://x")
            ]),
            &uri("/"),
            &[]
        ),
        Decision::Reject(RejectReason::RepeatedHost)
    ));
    // Absolute-form target and Host header disagree.
    assert!(matches!(
        evaluate(
            &headers(&[("host", "localhost"), ("origin", "http://x")]),
            &uri("http://rebind.example/"),
            &[]
        ),
        Decision::Reject(RejectReason::HostMismatch)
    ));
    // Absolute-form target and Host header agree (ports ignored): served.
    assert!(matches!(
        evaluate(
            &headers(&[("host", "localhost:1"), ("origin", "http://x")]),
            &uri("http://localhost/"),
            &[]
        ),
        Decision::Allow
    ));
}

#[test]
fn invalid_host_is_refused() {
    assert!(matches!(
        evaluate(
            &headers(&[("host", "bad_host .x"), ("origin", "http://x")]),
            &uri("/"),
            &[]
        ),
        Decision::Reject(RejectReason::InvalidHost(_))
    ));
}

// --- setting parser and rule text ---

#[test]
fn parse_keeps_host_names_and_drops_junk() {
    let (kept, dropped) =
        parse_dev_allowed_hosts("dev.test, My.Host , , bad host, with:port, 10.0.0.5, trailing.");
    assert_eq!(kept, vec!["dev.test", "my.host", "10.0.0.5", "trailing"]);
    assert_eq!(dropped, vec!["bad host", "with:port"]);
}

#[test]
fn a_port_suffixed_entry_is_dropped_with_what_to_list_instead() {
    let (kept, dropped) = parse_dev_allowed_hosts("app.test:3000");
    assert!(kept.is_empty(), "{kept:?}");
    assert_eq!(dropped, vec!["app.test:3000"]);
    assert_eq!(
        dropped_entry_note(&dropped[0]),
        "dev_allowed_hosts entry 'app.test:3000' was ignored: list a bare host name with no \
         port, scheme or wildcard, such as app.test."
    );
}

#[test]
fn rule_line_states_the_rule_and_the_listed_names() {
    let none = rule_line(&[]);
    assert!(none.starts_with(&rule()), "{none}");
    assert!(none.ends_with("Listed hosts: (none)"), "{none}");
    assert!(
        none.contains("/management") && none.contains("/metrics"),
        "{none}"
    );
    let two = rule_line(&["dev.test".to_owned(), "api.test".to_owned()]);
    assert!(two.ends_with("Listed hosts: dev.test, api.test"), "{two}");
}

#[test]
fn allowed_hosts_is_empty_outside_dev_mode() {
    assert!(allowed_hosts(false, Some("dev.test")).is_empty());
    assert_eq!(
        &*allowed_hosts(true, Some("Dev.Test, bad host")),
        ["dev.test"]
    );
    assert!(allowed_hosts(true, None).is_empty());
}

// --- router wiring and response shape ---

fn routes() -> Router {
    Router::new()
        .route("/", post(|| async { "api-ok" }))
        .route("/health", get(|| async { "health-ok" }))
        .route("/metrics", get(|| async { "metrics-ok" }))
        .route("/management/accounts", get(|| async { "mgmt-ok" }))
        .route(
            "/console/login",
            get(|| async { "console-ok" }).post(|| async { "console-ok" }),
        )
}

fn guarded_router(allowed: Vec<String>) -> Router {
    install(routes(), true, allowed.into())
}

async fn body_string(resp: Response) -> (StatusCode, String, String) {
    let status = resp.status();
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        content_type,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

async fn send(router: Router, method: &str, path: &str, hdrs: &[(&str, &str)]) -> Response {
    let mut req = Request::builder().method(method).uri(path);
    for (k, v) in hdrs {
        req = req.header(*k, *v);
    }
    let body = if method == "POST" {
        Body::from("{}")
    } else {
        Body::empty()
    };
    router.oneshot(req.body(body).unwrap()).await.unwrap()
}

#[tokio::test]
async fn api_rebinding_browser_request_gets_dynamodb_json_403() {
    let resp = send(
        guarded_router(vec![]),
        "POST",
        "/",
        &[
            ("host", "rebind.example:1"),
            ("origin", "http://rebind.example:1"),
            ("x-amz-target", "DynamoDB_20120810.CreateTable"),
        ],
    )
    .await;
    let (status, content_type, body) = body_string(resp).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(content_type, "application/x-amz-json-1.0");
    assert!(body.contains("AccessDeniedException"), "body: {body}");
    assert!(body.contains("rebind.example"), "names the host: {body}");
    assert!(body.contains(SETTING_KEY), "names the setting: {body}");
    assert!(!body.contains("api-ok"), "handler must not have run");
}

#[tokio::test]
async fn api_sdk_request_with_compose_host_is_served() {
    // No browser marker, a Compose-style Host: served.
    let resp = send(
        guarded_router(vec![]),
        "POST",
        "/",
        &[
            ("host", "extenddb:18000"),
            ("x-amz-target", "DynamoDB_20120810.ListTables"),
        ],
    )
    .await;
    let (status, _, body) = body_string(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "api-ok");
}

#[tokio::test]
async fn api_undici_fetch_request_with_compose_host_is_served() {
    let mut hdrs = UNDICI_HEADERS.to_vec();
    hdrs.push(("host", "extenddb"));
    hdrs.push(("x-amz-target", "DynamoDB_20120810.ListTables"));
    let resp = send(guarded_router(vec![]), "POST", "/", &hdrs).await;
    let (status, _, body) = body_string(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "api-ok");
}

#[tokio::test]
async fn api_browser_request_with_loopback_host_is_served() {
    for host in ["localhost:1", "127.0.0.1:1", "[::1]:1", "x.localhost:1"] {
        let resp = send(
            guarded_router(vec![]),
            "POST",
            "/",
            &[("host", host), ("origin", "http://x")],
        )
        .await;
        let (status, _, body) = body_string(resp).await;
        assert_eq!(status, StatusCode::OK, "{host}");
        assert_eq!(body, "api-ok", "{host}");
    }
}

#[tokio::test]
async fn api_browser_request_with_listed_host_is_served() {
    let resp = send(
        guarded_router(vec!["dev.test".to_owned()]),
        "POST",
        "/",
        &[("host", "dev.test:1"), ("origin", "http://x")],
    )
    .await;
    let (status, _, body) = body_string(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "api-ok");
}

#[tokio::test]
async fn console_rebinding_browser_request_gets_html_403() {
    for method in ["GET", "POST"] {
        let resp = send(
            guarded_router(vec![]),
            method,
            "/console/login",
            &[
                ("host", "rebind.example:1"),
                ("origin", "http://rebind.example:1"),
            ],
        )
        .await;
        let (status, content_type, body) = body_string(resp).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method}");
        assert!(
            content_type.starts_with("text/html"),
            "{method}: {content_type}"
        );
        assert!(body.contains("rebind.example"), "{method}: {body}");
        assert!(
            !body.contains("console-ok"),
            "{method}: handler must not run"
        );
    }
}

#[tokio::test]
async fn admin_get_with_no_marker_under_a_rebinding_host_gets_403() {
    for path in ["/management/accounts", "/metrics"] {
        let resp = send(
            guarded_router(vec![]),
            "GET",
            path,
            &[("host", "rebind.example:1"), ("authorization", ADMIN_AUTH)],
        )
        .await;
        let (status, content_type, body) = body_string(resp).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
        assert_eq!(content_type, "application/x-amz-json-1.0", "{path}");
        assert!(body.contains("AccessDeniedException"), "{path}: {body}");
        assert!(body.contains("rebind.example"), "{path}: {body}");
    }
}

#[tokio::test]
async fn admin_get_under_loopback_or_listed_host_is_served() {
    for host in ["localhost:1", "127.0.0.1:1", "dev.test:1"] {
        let resp = send(
            guarded_router(vec!["dev.test".to_owned()]),
            "GET",
            "/management/accounts",
            &[("host", host), ("authorization", ADMIN_AUTH)],
        )
        .await;
        let (status, _, body) = body_string(resp).await;
        assert_eq!(status, StatusCode::OK, "{host}");
        assert_eq!(body, "mgmt-ok", "{host}");
    }
}

#[tokio::test]
async fn health_with_no_marker_under_a_rebinding_host_is_served() {
    let resp = send(
        guarded_router(vec![]),
        "GET",
        "/health",
        &[("host", "rebind.example:1")],
    )
    .await;
    let (status, _, body) = body_string(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "health-ok");
}

#[tokio::test]
async fn production_mode_installs_no_guard() {
    // `install` with dev_mode false returns the router unchanged, so the
    // requests the guard refuses in developer mode reach their handlers.
    let router = install(routes(), false, Arc::from(Vec::new()));
    let resp = send(
        router.clone(),
        "POST",
        "/",
        &[
            ("host", "rebind.example:1"),
            ("origin", "http://rebind.example:1"),
        ],
    )
    .await;
    let (status, _, body) = body_string(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "api-ok");
    let resp = send(
        router,
        "GET",
        "/management/accounts",
        &[("host", "rebind.example:1"), ("authorization", ADMIN_AUTH)],
    )
    .await;
    let (status, _, body) = body_string(resp).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "mgmt-ok");
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! `start_server` driven over a loopback socket: an extension router is
//! mounted behind the security layer by the server's own wiring, the built-in
//! routes answer the same with an extension mounted, the extension body limit
//! is axum's default, and a request the client abandons mid-flight reaches the
//! observer once with status 0.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use extenddb_auth::{AuthIdentity, AuthProvider};
use extenddb_core::error::DynamoDbError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::throttle_wire_tests::build_state_with;
use crate::extensions::{RequestObserver, RequestRecord};

#[derive(Default)]
struct Recording(Mutex<Vec<RequestRecord>>);

impl RequestObserver for Recording {
    fn on_response(&self, record: RequestRecord) {
        self.0.lock().unwrap().push(record);
    }
}

/// The fixture's stub with an optional delay, so a client can go away while
/// the request is inside authentication.
struct SlowAuth {
    account_id: String,
    delay_ms: u64,
}

#[async_trait::async_trait]
impl AuthProvider for SlowAuth {
    async fn authenticate(
        &self,
        _headers: &HeaderMap,
        _body: &[u8],
    ) -> Result<AuthIdentity, DynamoDbError> {
        if self.delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
        }
        Ok(AuthIdentity::User {
            account_id: self.account_id.clone(),
            user_name: "dev".to_owned(),
        })
    }
}

struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Reply {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// One HTTP/1.1 exchange over a fresh connection with `Connection: close`.
async fn raw(addr: SocketAddr, request: &[u8]) -> Reply {
    let mut s = TcpStream::connect(addr).await.expect("connect");
    s.write_all(request).await.expect("write");
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.expect("read");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").expect("head and body");
    let mut lines = head.lines();
    let status: u16 = lines
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status")
        .parse()
        .expect("status number");
    let mut headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    headers.sort();
    let chunked = headers
        .iter()
        .any(|(k, v)| k == "transfer-encoding" && v == "chunked");
    let body = if chunked {
        let mut out = String::new();
        let mut rest = body;
        loop {
            let (size, after) = rest.split_once("\r\n").expect("chunk size");
            let n = usize::from_str_radix(size.trim(), 16).expect("hex");
            if n == 0 {
                break;
            }
            out.push_str(&after[..n]);
            rest = &after[n + 2..];
        }
        out
    } else {
        body.to_owned()
    };
    Reply {
        status,
        headers,
        body,
    }
}

fn get_req(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").into_bytes()
}

fn post_req(path: &str, extra: &str, body: &[u8]) -> Vec<u8> {
    let mut v = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n{extra}Content-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    v.extend_from_slice(body);
    v
}

/// Bind `127.0.0.1:0`, build the fixture state and spawn `start_server` with
/// the given extension routers. `start_server` runs until a signal, so the
/// caller aborts the returned task.
async fn serve(
    extensions: Vec<(String, Router)>,
    observer: Option<Arc<Recording>>,
    auth_delay_ms: u64,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    serve_with_tabs(extensions, Vec::new(), observer, auth_delay_ms).await
}

/// Credentials of the admin seeded on the fixture's catalog for the console
/// tab tests.
const ADMIN_NAME: &str = "tab-admin";
const ADMIN_PASSWORD: &str = "tab-admin-password-1";

/// `serve` with console tabs; the fixture's catalog gets one admin so a test
/// can log in to the console.
async fn serve_with_tabs(
    extensions: Vec<(String, Router)>,
    tabs: Vec<crate::extensions::ConsoleTab>,
    observer: Option<Arc<Recording>>,
    auth_delay_ms: u64,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let installed = observer.map(|o| o as Arc<dyn RequestObserver>);
    let (state, _) = build_state_with(
        move |account_id| {
            Arc::new(SlowAuth {
                account_id,
                delay_ms: auth_delay_ms,
            })
        },
        installed,
    )
    .await;
    let state = Arc::try_unwrap(state)
        .ok()
        .expect("the fixture holds the only reference");
    if !tabs.is_empty() {
        let hash = crate::management::password::hash_password(ADMIN_PASSWORD.to_owned())
            .await
            .expect("bcrypt");
        state
            .catalog_store
            .as_ref()
            .expect("fixture has a catalog store")
            .create_admin(ADMIN_NAME, &hash)
            .await
            .expect("seed admin");
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let task = tokio::spawn(async move {
        crate::start_server(listener, state, None, None, extensions, tabs)
            .await
            .expect("start_server");
    });
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    (addr, task)
}

fn probe_router() -> Router {
    Router::new().route("/", get(|| async { "probe" })).route(
        "/echo",
        post(|body: Bytes| async move { body.len().to_string() }),
    )
}

const SECURITY: [(&str, &str); 3] = [
    ("x-content-type-options", "nosniff"),
    ("x-frame-options", "DENY"),
    ("referrer-policy", "strict-origin-when-cross-origin"),
];

#[tokio::test]
async fn start_server_mounts_extensions_behind_the_security_layer() {
    let (addr, task) = serve(vec![("/_probe".to_owned(), probe_router())], None, 0).await;

    let probe = raw(addr, &get_req("/_probe")).await;
    assert_eq!(probe.status, 200, "{}", probe.body);
    assert_eq!(probe.body, "probe");
    for (name, value) in SECURITY {
        assert_eq!(
            probe.header(name),
            Some(value),
            "extension response carries {name}"
        );
    }

    let health = raw(addr, &get_req("/health")).await;
    assert_eq!(health.status, 200);
    assert!(health.body.contains("healthy"), "{}", health.body);
    for (name, value) in SECURITY {
        assert_eq!(health.header(name), Some(value));
    }

    // Body limits: an extension route is bounded by axum's default (2 MB);
    // the request handler at `/` by the 16 MB DynamoDB limit.
    let three_mb = vec![b'a'; 3 * 1024 * 1024];
    let echo = raw(addr, &post_req("/_probe/echo", "", &three_mb)).await;
    assert_eq!(echo.status, 413, "{}", echo.body);
    let root = raw(addr, &post_req("/", "", &three_mb)).await;
    assert_eq!(root.status, 400, "{}", root.body);
    assert!(
        root.body.contains("MissingAuthenticationToken"),
        "{}",
        root.body
    );
    let small = raw(addr, &post_req("/_probe/echo", "", b"hello")).await;
    assert_eq!(small.status, 200);
    assert_eq!(small.body, "5");

    task.abort();
}

/// Status, headers and body with the volatile parts normalised: request id,
/// crc32, `date`, cookie values and the listen address in the console footer.
fn normalised(reply: &Reply, addr: SocketAddr) -> String {
    let mut h: Vec<String> = reply
        .headers
        .iter()
        .map(|(k, v)| match k.as_str() {
            "x-amzn-requestid" => format!("{k}: RID"),
            "x-amz-crc32" => format!("{k}: CRC"),
            "date" => format!("{k}: DATE"),
            "set-cookie" => format!("{k}: {}", v.split('=').next().unwrap_or("")),
            _ => format!("{k}: {v}"),
        })
        .collect();
    h.sort();
    let mut body = reply.body.replace(&addr.to_string(), "ADDR");
    if let Some(rid) = reply.header("x-amzn-requestid") {
        body = body.replace(rid, "RID");
    }
    format!("status={}\n{}\nbody={body}\n", reply.status, h.join("\n"))
}

#[tokio::test]
async fn built_in_routes_answer_the_same_with_an_extension_mounted() {
    let (plain_addr, plain) = serve(Vec::new(), None, 0).await;
    let (ext_addr, ext) = serve(vec![("/_x".to_owned(), probe_router())], None, 0).await;

    let list = post_req("/", "X-Amz-Target: DynamoDB_20120810.ListTables\r\n", b"{}");
    let requests: Vec<(&str, Vec<u8>)> = vec![
        ("GET /console/", get_req("/console/")),
        ("GET /console", get_req("/console")),
        ("GET /console/login", get_req("/console/login")),
        ("GET /console/nope", get_req("/console/nope")),
        ("GET /management/accounts", get_req("/management/accounts")),
        ("GET /management", get_req("/management")),
        ("GET /health", get_req("/health")),
        ("GET /metrics", get_req("/metrics")),
        ("GET /nope", get_req("/nope")),
        ("GET /", get_req("/")),
        ("POST / ListTables", list),
    ];
    for (name, req) in &requests {
        let a = normalised(&raw(plain_addr, req).await, plain_addr);
        let b = normalised(&raw(ext_addr, req).await, ext_addr);
        assert_eq!(a, b, "{name} differs with an extension mounted");
    }
    assert_eq!(raw(plain_addr, &get_req("/_x")).await.status, 404);
    assert_eq!(raw(ext_addr, &get_req("/_x")).await.status, 200);

    plain.abort();
    ext.abort();
}

#[tokio::test]
async fn a_client_that_goes_away_mid_request_is_recorded_once_with_status_zero() {
    let observer = Arc::new(Recording::default());
    let (addr, task) = serve(Vec::new(), Some(observer.clone()), 400).await;

    let list = post_req("/", "X-Amz-Target: DynamoDB_20120810.ListTables\r\n", b"{}");
    let mut s = TcpStream::connect(addr).await.expect("connect");
    s.write_all(&list).await.expect("write");
    // hyper has read the request and the handler is inside authentication.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    drop(s);
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    let records = observer.0.lock().unwrap().clone();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].status, 0);
    assert_eq!(records[0].operation, "ListTables");
    assert_eq!(records[0].account_id, None);

    task.abort();
}

/// A tab page that renders the console layout from the identity the session
/// check inserted, so the test can read the navigation bar, plus a POST route
/// that acts only when the form's `_csrf` field is the session's token.
fn tab_router() -> Router {
    #[derive(serde::Deserialize)]
    struct Act {
        #[serde(rename = "_csrf", default)]
        csrf: String,
    }
    Router::new()
        .route(
            "/",
            get(
                |identity: axum::Extension<crate::console::ConsoleIdentity>| async move {
                    let nav = crate::console::html::nav_bar(&identity.label, &identity.tabs);
                    axum::response::Html(crate::console::html::layout_with_version_csrf(
                        "Chaos",
                        &nav,
                        &format!(
                            "<p id=\"who\">{}</p><p id=\"admin\">{}</p>",
                            crate::console::html::escape(&identity.label),
                            identity.is_admin
                        ),
                        Some(&identity.version_info),
                        &identity.csrf_token,
                    ))
                },
            ),
        )
        .route(
            "/act",
            post(
                |identity: axum::Extension<crate::console::ConsoleIdentity>,
                 axum::Form(form): axum::Form<Act>| async move {
                    if identity.csrf_matches(&form.csrf) {
                        (axum::http::StatusCode::OK, "acted")
                    } else {
                        (axum::http::StatusCode::FORBIDDEN, "CSRF token mismatch")
                    }
                },
            ),
        )
}

/// Log in to the console as the seeded admin and return the session cookie.
async fn login(addr: SocketAddr) -> String {
    let body = format!("username={ADMIN_NAME}&password={ADMIN_PASSWORD}");
    let reply = raw(
        addr,
        &post_req(
            "/console/login",
            "Content-Type: application/x-www-form-urlencoded\r\n",
            body.as_bytes(),
        ),
    )
    .await;
    assert_eq!(reply.status, 303, "{}", reply.body);
    let cookie = reply.header("set-cookie").expect("session cookie set");
    cookie.split(';').next().expect("cookie pair").to_owned()
}

fn post_req_with_cookie(path: &str, cookie: &str, body: &[u8]) -> Vec<u8> {
    let mut v = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nCookie: {cookie}\r\n\
         Content-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    v.extend_from_slice(body);
    v
}

fn get_req_with_cookie(path: &str, cookie: &str) -> Vec<u8> {
    format!(
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\nCookie: {cookie}\r\n\r\n"
    )
    .into_bytes()
}

#[tokio::test]
async fn a_console_tab_is_behind_the_session_check_and_listed_in_the_nav_bar() {
    // The label carries the characters the bar must escape.
    let tabs = vec![crate::extensions::ConsoleTab {
        label: r#"Chaos <b>&"x"</b>"#.to_owned(),
        slug: "chaos".to_owned(),
        router: tab_router(),
    }];
    const ESCAPED_LINK: &str =
        r#"<a href="/console/chaos">Chaos &lt;b&gt;&amp;&quot;x&quot;&lt;/b&gt;</a>"#;
    let (addr, task) = serve_with_tabs(Vec::new(), tabs, None, 0).await;

    // Without a session: the page, a non-route under it and the slash form
    // all redirect to the login page, so the slug is not confirmed.
    for path in ["/console/chaos", "/console/chaos/other", "/console/chaos/"] {
        let r = raw(addr, &get_req(path)).await;
        assert_eq!(r.status, 303, "{path}: {}", r.body);
        assert_eq!(r.header("location"), Some("/console/login"), "{path}");
    }

    let cookie = login(addr).await;

    // With a session: the page renders inside the console layout with the
    // admin's label and the tab in the navigation bar; the slash form
    // redirects to the page; a non-route under the tab is 404.
    let page = raw(addr, &get_req_with_cookie("/console/chaos", &cookie)).await;
    assert_eq!(page.status, 200, "{}", page.body);
    assert!(page.body.contains("<nav>"), "{}", page.body);
    assert!(page.body.contains(ESCAPED_LINK), "{}", page.body);
    assert!(!page.body.contains("<b>&\"x\"</b>"), "{}", page.body);
    assert!(
        page.body.contains(r#"<p id="who">Admin: tab-admin</p>"#),
        "{}",
        page.body
    );
    for (name, value) in SECURITY {
        assert_eq!(page.header(name), Some(value), "tab page carries {name}");
    }
    assert!(
        !page.body.contains("Self-signed TLS certificate"),
        "the tab footer is the logged-in pages' footer: {}",
        page.body
    );
    assert!(
        page.body.contains(r#"<p id="admin">true</p>"#),
        "{}",
        page.body
    );

    // The page's meta tag carries the session's CSRF token; a POST with it
    // acts, a POST without it is refused as the built-in pages refuse.
    let token = page
        .body
        .split(r#"name="csrf-token" content=""#)
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("csrf meta tag")
        .to_owned();
    let act = raw(
        addr,
        &post_req_with_cookie(
            "/console/chaos/act",
            &cookie,
            format!("_csrf={token}&x=1").as_bytes(),
        ),
    )
    .await;
    assert_eq!(act.status, 200, "{}", act.body);
    let refused = raw(
        addr,
        &post_req_with_cookie("/console/chaos/act", &cookie, b"x=1"),
    )
    .await;
    assert_eq!(refused.status, 403, "{}", refused.body);
    let no_session = raw(addr, &post_req("/console/chaos/act", "", b"x=1")).await;
    assert_eq!(no_session.status, 303, "{}", no_session.body);
    assert_eq!(no_session.header("location"), Some("/console/login"));

    let slash = raw(addr, &get_req_with_cookie("/console/chaos/", &cookie)).await;
    assert_eq!(slash.status, 308, "{}", slash.body);
    assert_eq!(slash.header("location"), Some("/console/chaos"));

    let missing = raw(addr, &get_req_with_cookie("/console/chaos/other", &cookie)).await;
    assert_eq!(missing.status, 404, "{}", missing.body);

    // A built-in console page lists the tab too.
    let home = raw(addr, &get_req_with_cookie("/console", &cookie)).await;
    assert_eq!(home.status, 200, "{}", home.body);
    assert!(home.body.contains(ESCAPED_LINK), "{}", home.body);

    task.abort();
}

#[tokio::test]
async fn without_tabs_the_console_nav_bar_and_routes_are_unchanged() {
    let (addr, task) = serve_with_tabs(Vec::new(), Vec::new(), None, 0).await;
    let r = raw(addr, &get_req("/console/chaos")).await;
    // No tab mounted: the console's own fallback answers, behind no gate.
    assert_eq!(r.status, 404, "{}", r.body);
    let login_page = raw(addr, &get_req("/console/login")).await;
    assert_eq!(login_page.status, 200);
    assert!(!login_page.body.contains("/console/chaos"));
    task.abort();
}

/// `start_server` refuses a bad console tab slug and a bad extension prefix
/// with the error naming the cause, before it serves a request; the checks
/// run in `start_server` itself, not only in `cmd_serve`, so a caller that
/// embeds the server gets them too.
#[tokio::test]
async fn start_server_refuses_a_reserved_slug_and_a_reserved_prefix_before_serving() {
    for (extensions, tabs, expected) in [
        (
            Vec::new(),
            vec![crate::extensions::ConsoleTab {
                label: "Login".to_owned(),
                slug: "login".to_owned(),
                router: tab_router(),
            }],
            "console tab slug 'login' is a built-in console route",
        ),
        (
            vec![("/health/live".to_owned(), probe_router())],
            Vec::new(),
            "extension router prefix '/health/live' collides with '/health'",
        ),
    ] {
        let (state, _) = build_state_with(
            |account_id| {
                Arc::new(SlowAuth {
                    account_id,
                    delay_ms: 0,
                })
            },
            None,
        )
        .await;
        let state = Arc::try_unwrap(state)
            .ok()
            .expect("the fixture holds the only reference");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let err = crate::start_server(listener, state, None, None, extensions, tabs)
            .await
            .expect_err("start_server refuses");
        assert_eq!(err.to_string(), expected);
    }
}

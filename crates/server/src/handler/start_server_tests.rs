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
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let task = tokio::spawn(async move {
        crate::start_server(listener, state, None, None, extensions)
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

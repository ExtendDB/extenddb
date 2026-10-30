// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the listener: request timeouts, the first-byte deadline
//! and `TCP_NODELAY` on both listener modes.

use super::*;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

/// Bind a loopback listener and connect one client to it.
async fn bind_and_connect() -> (TcpListener, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let client = TcpStream::connect(addr).await.expect("connect");
    (listener, client)
}

/// The plaintext path: a connection that sends part of its request
/// headers and then stalls is closed once `request_timeout` passes,
/// as it is on the TLS path. The client reads EOF (or a reset) well
/// before a bound of several times the timeout; without the timer the
/// read blocks until that bound.
#[tokio::test]
async fn plaintext_listener_closes_a_stalled_header_phase() {
    use tokio::io::AsyncReadExt;

    let timeout = Duration::from_millis(300);
    let (listener, mut client) = bind_and_connect().await;
    let app = Router::new().route("/", axum::routing::get(|| async { "ok" }));
    let server = tokio::spawn(serve_app(listener, app, timeout, None, None));

    // A request line and one header, no terminating blank line.
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .expect("write partial headers");
    let mut buf = [0u8; 64];
    let outcome = tokio::time::timeout(timeout * 10, client.read(&mut buf)).await;
    server.abort();
    match outcome {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(n)) => panic!("server answered {n} bytes instead of closing the connection"),
        Err(_) => panic!(
            "server held a stalled header phase open for {:?} on the plaintext listener",
            timeout * 10
        ),
    }
}

/// The TLS path: the acceptor that runs before the TLS handshake sets
/// `TCP_NODELAY` on the accepted socket, read back from the socket the
/// acceptor hands on.
#[tokio::test]
async fn tls_acceptor_sets_tcp_nodelay_before_the_handshake() {
    use axum_server::accept::Accept;

    let (listener, mut client) = bind_and_connect().await;
    // A TLS ClientHello starts with the handshake record type.
    client
        .write_all(&[0x16])
        .await
        .expect("write ClientHello byte");
    let (accepted, _) = listener.accept().await.expect("accept");
    assert!(
        !accepted.nodelay().expect("read TCP_NODELAY"),
        "a fresh accepted socket has Nagle on"
    );

    let acceptor = HttpsRedirectAcceptor {
        addr: listener.local_addr().expect("local addr"),
        first_byte_timeout: Duration::from_secs(5),
    };
    let (stream, ()) = acceptor
        .accept(accepted, ())
        .await
        .expect("a TLS ClientHello passes through to the TLS acceptor");
    assert!(stream.nodelay().expect("read TCP_NODELAY"));
}

/// The plaintext path: the acceptor sets `TCP_NODELAY` on every socket
/// it accepts.
#[tokio::test]
async fn plaintext_listener_sets_tcp_nodelay_on_accepted_sockets() {
    use axum_server::accept::Accept;

    let (listener, client) = bind_and_connect().await;
    let (accepted, peer) = listener.accept().await.expect("accept");
    assert_eq!(peer, client.local_addr().expect("client addr"));
    assert!(
        !accepted.nodelay().expect("read TCP_NODELAY"),
        "a fresh accepted socket has Nagle on"
    );
    let (stream, ()) = NodelayAcceptor
        .accept(accepted, ())
        .await
        .expect("the plaintext acceptor hands the socket on");
    assert!(stream.nodelay().expect("read TCP_NODELAY"));
}

/// Read from `client` until EOF, an error, or `bound`. Returns `Ok(())` for
/// EOF or a reset, `Err(n)` for `n` unexpected bytes, and panics with `what`
/// when the server holds the connection past `bound`.
async fn expect_closed_within(client: &mut TcpStream, bound: Duration, what: &str) {
    use tokio::io::AsyncReadExt;

    let mut buf = [0u8; 64];
    match tokio::time::timeout(bound, client.read(&mut buf)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {}
        Ok(Ok(n)) => panic!("{what}: server answered {n} bytes instead of closing the connection"),
        Err(_) => panic!("{what}: server held the connection open for {bound:?}"),
    }
}

/// The plaintext path: a connection that sends nothing after connecting is
/// closed once `request_timeout` passes. The header read timer never starts
/// for this shape, because the version sniff runs first.
#[tokio::test]
async fn plaintext_listener_closes_a_connection_that_sends_nothing() {
    let timeout = Duration::from_millis(300);
    let (listener, mut client) = bind_and_connect().await;
    let app = Router::new().route("/", axum::routing::get(|| async { "ok" }));
    let server = tokio::spawn(serve_app(listener, app, timeout, None, None));

    expect_closed_within(&mut client, timeout * 10, "plaintext, zero bytes").await;
    server.abort();
}

/// The plaintext path: a client that sends 16 bytes of the HTTP/2 preface
/// and stops is still in the version sniff; it is closed at the deadline.
#[tokio::test]
async fn plaintext_listener_closes_a_partial_h2_preface() {
    let timeout = Duration::from_millis(300);
    let (listener, mut client) = bind_and_connect().await;
    let app = Router::new().route("/", axum::routing::get(|| async { "ok" }));
    let server = tokio::spawn(serve_app(listener, app, timeout, None, None));

    client
        .write_all(&H2_PREFACE[..16])
        .await
        .expect("write partial preface");
    expect_closed_within(&mut client, timeout * 10, "plaintext, 16 preface bytes").await;
    server.abort();
}

/// Write a self-signed certificate for `localhost` and its key into `dir`,
/// returning the server's TLS config and the certificate's DER bytes for the
/// client's trust store.
fn self_signed_localhost(dir: &std::path::Path) -> (ServerTlsConfig, Vec<u8>) {
    let params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).expect("params");
    let key_pair = rcgen::KeyPair::generate().expect("key pair");
    let cert = params.self_signed(&key_pair).expect("self-signed cert");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, cert.pem()).expect("write cert");
    std::fs::write(&key_path, key_pair.serialize_pem()).expect("write key");
    (
        ServerTlsConfig {
            cert_path,
            key_path,
        },
        cert.der().to_vec(),
    )
}

/// Complete a TLS handshake with the server at `addr` as a client that
/// trusts `cert_der`, send `after_handshake` as application data, then read
/// until EOF, an error, or `bound`. Runs rustls over a blocking socket so
/// the test needs no async TLS client. Returns `Ok(())` for EOF or an error
/// from the server side, `Err(n)` for `n` application bytes received, and
/// panics when the bound passes with the connection still open.
fn tls_client_expect_closed_within(
    addr: std::net::SocketAddr,
    cert_der: Vec<u8>,
    after_handshake: &'static [u8],
    bound: Duration,
    what: &'static str,
) {
    // The rustls client needs the process-level provider as well. serve_app
    // installs it in the TLS arm, but this client can reach the builder
    // before the server task gets there, and ClientConfig::builder panics
    // when no provider is installed. Installing it twice is not an error
    // here: the second call reports that one is already set.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(rustls::pki_types::CertificateDer::from(cert_der))
        .expect("trust the test certificate");
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
    let mut conn = rustls::ClientConnection::new(std::sync::Arc::new(config), server_name)
        .expect("client connection");
    let mut sock = std::net::TcpStream::connect(addr).expect("connect");
    sock.set_read_timeout(Some(bound)).expect("read timeout");
    while conn.is_handshaking() {
        conn.complete_io(&mut sock).expect("TLS handshake");
    }
    let mut tls = rustls::Stream::new(&mut conn, &mut sock);
    if !after_handshake.is_empty() {
        std::io::Write::write_all(&mut tls, after_handshake).expect("write application data");
        std::io::Write::flush(&mut tls).expect("flush");
    }
    let mut buf = [0u8; 64];
    match std::io::Read::read(&mut tls, &mut buf) {
        Ok(0) => {}
        Ok(n) => panic!("{what}: server answered {n} bytes instead of closing the connection"),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) =>
        {
            panic!("{what}: server held the connection open for {bound:?}")
        }
        // A reset, a close_notify reported as an error, or an unexpected
        // EOF all mean the server dropped the connection.
        Err(_) => {}
    }
}

/// Start the TLS listener on a loopback port with `timeout`, returning its
/// address, the certificate to trust, the temp dir holding the key pair and
/// the server task.
async fn start_tls_listener(
    timeout: Duration,
) -> (
    std::net::SocketAddr,
    Vec<u8>,
    std::path::PathBuf,
    tokio::task::JoinHandle<Result<(), anyhow::Error>>,
) {
    let dir = std::env::temp_dir().join(format!(
        "extenddb-listener-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let (tls, cert_der) = self_signed_localhost(&dir);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let app = Router::new().route("/", axum::routing::get(|| async { "ok" }));
    let server = tokio::spawn(serve_app(listener, app, timeout, Some(tls), None));
    (addr, cert_der, dir, server)
}

/// The TLS path: a client that completes the handshake and then sends no
/// application data is closed once `request_timeout` passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_listener_closes_a_connection_that_sends_nothing_after_the_handshake() {
    let timeout = Duration::from_millis(300);
    let (addr, cert_der, dir, server) = start_tls_listener(timeout).await;
    tokio::task::spawn_blocking(move || {
        tls_client_expect_closed_within(
            addr,
            cert_der,
            b"",
            timeout * 10,
            "TLS, zero bytes after the handshake",
        )
    })
    .await
    .expect("client task");
    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}

/// The TLS path: 16 bytes of the HTTP/2 preface after the handshake, then
/// silence, is closed at the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_listener_closes_a_partial_h2_preface() {
    let timeout = Duration::from_millis(300);
    let (addr, cert_der, dir, server) = start_tls_listener(timeout).await;
    tokio::task::spawn_blocking(move || {
        tls_client_expect_closed_within(
            addr,
            cert_der,
            &H2_PREFACE[..16],
            timeout * 10,
            "TLS, 16 preface bytes",
        )
    })
    .await
    .expect("client task");
    server.abort();
    let _ = std::fs::remove_dir_all(dir);
}

/// The TLS path: a connection that sends no byte at all (not even a
/// ClientHello) is dropped by the redirect acceptor's peek at the deadline.
#[tokio::test]
async fn tls_acceptor_drops_a_connection_that_sends_nothing_before_the_handshake() {
    use axum_server::accept::Accept;

    let timeout = Duration::from_millis(300);
    let (listener, _client) = bind_and_connect().await;
    let (accepted, _) = listener.accept().await.expect("accept");
    let acceptor = HttpsRedirectAcceptor {
        addr: listener.local_addr().expect("local addr"),
        first_byte_timeout: timeout,
    };
    let outcome = tokio::time::timeout(timeout * 10, acceptor.accept(accepted, ())).await;
    match outcome {
        Ok(Err(e)) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut, "{e}"),
        Ok(Ok(_)) => panic!("acceptor passed a connection that sent nothing"),
        Err(_) => panic!(
            "acceptor waited more than {:?} for the first byte",
            timeout * 10
        ),
    }
}

/// The deadline is dropped once the first byte arrives: a client that sends
/// a complete request late, but before the deadline, is served.
#[tokio::test]
async fn plaintext_listener_serves_a_request_that_arrives_before_the_deadline() {
    use tokio::io::AsyncReadExt;

    let timeout = Duration::from_millis(600);
    let (listener, mut client) = bind_and_connect().await;
    let app = Router::new().route("/", axum::routing::get(|| async { "ok" }));
    let server = tokio::spawn(serve_app(listener, app, timeout, None, None));

    tokio::time::sleep(timeout / 3).await;
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");
    let mut response = Vec::new();
    tokio::time::timeout(timeout * 10, client.read_to_end(&mut response))
        .await
        .expect("response within the bound")
        .expect("read response");
    server.abort();
    let text = String::from_utf8_lossy(&response);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.ends_with("ok"), "{text}");
}

/// The deadline is dropped exactly where the auto builder stops sniffing: a
/// byte that differs from the preface, or the whole preface; a prefix of it
/// keeps the deadline armed.
#[tokio::test]
async fn deadline_tracks_the_version_sniff() {
    let mut adapter = FirstByteDeadline::new(tokio::io::empty(), Duration::from_secs(1));
    assert!(!adapter.sniff_decided(&H2_PREFACE[..16]));
    assert!(!adapter.sniff_decided(&H2_PREFACE[16..23]));
    assert!(adapter.sniff_decided(&H2_PREFACE[23..]));

    let mut adapter = FirstByteDeadline::new(tokio::io::empty(), Duration::from_secs(1));
    assert!(adapter.sniff_decided(b"G"));

    let mut adapter = FirstByteDeadline::new(tokio::io::empty(), Duration::from_secs(1));
    assert!(!adapter.sniff_decided(b"PRI"));
    assert!(adapter.sniff_decided(b" + HTTP"));
}

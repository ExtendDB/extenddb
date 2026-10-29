// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The listener: serves the built router over TLS or plaintext, applies the
//! per-connection request timeouts, sets `TCP_NODELAY` on accepted sockets,
//! redirects plain HTTP arriving on the TLS port, and handles shutdown.

use std::path::PathBuf;
use std::time::Duration;

use axum::Router;

use crate::ServerTlsConfig;

/// Serve `app` on `listener` until a shutdown signal arrives.
///
/// When `tls` is `Some`, the server serves HTTPS using `axum-server` with
/// rustls. When `tls` is `None`, the server serves plaintext HTTP. Both arms
/// run on the same `axum-server` builder, so the request timeouts, the
/// shutdown handle and `TCP_NODELAY` apply to every listener mode.
pub(crate) async fn serve_app(
    listener: tokio::net::TcpListener,
    app: Router,
    request_timeout: Duration,
    tls: Option<ServerTlsConfig>,
    pid_file: Option<PathBuf>,
) -> Result<(), anyhow::Error> {
    let local_addr = listener.local_addr()?;

    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();
    let shutdown_pid = pid_file.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(5)));
        // P50: Log timeout but don't call std::process::exit — let the
        // runtime shut down normally so destructors (including ZeroizeOnDrop) run.
        tokio::time::sleep(Duration::from_secs(10)).await;
        tracing::warn!("Graceful shutdown timed out, forcing PID file cleanup");
        cleanup_pid_file(shutdown_pid.as_deref());
    });

    let std_listener = listener.into_std()?;
    let make_service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();

    if let Some(tls_cfg) = tls {
        // P57 Bug 2 fix: rustls 0.23 requires an explicit CryptoProvider.
        // Install aws-lc-rs as the default before creating any TLS config.
        // Ignore the error if a provider was already installed (e.g., by sqlx).
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        tracing::info!("extenddb listening on {local_addr} (HTTPS)");
        let rustls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
            &tls_cfg.cert_path,
            &tls_cfg.key_path,
        )
        .await
        // AI-3: The error intentionally does not include the file path to avoid
        // leaking filesystem structure in logs. See docs/troubleshooting.md for
        // the "Failed to load TLS certificates" entry.
        .map_err(|e| anyhow::anyhow!("Failed to load TLS certificates: {e}"))?;

        // AI-2: Use a custom acceptor that peeks the first byte of each
        // connection. If it's plain HTTP (not 0x16 TLS ClientHello), write
        // a 301 redirect to HTTPS and reject the connection before the TLS
        // handshake. This gives users a helpful redirect instead of a
        // confusing TLS handshake failure. The same acceptor sets
        // TCP_NODELAY on every accepted socket before the handshake.
        let redirect_acceptor = HttpsRedirectAcceptor { addr: local_addr };
        let server = axum_server::from_tcp_rustls(std_listener, rustls_config)?
            .map(|tls| tls.acceptor(redirect_acceptor))
            .handle(handle);
        with_request_timeouts(server, request_timeout)
            .serve(make_service)
            .await?;
    } else {
        tracing::info!("extenddb listening on {local_addr}");
        let server = axum_server::from_tcp(std_listener)?
            .acceptor(NodelayAcceptor)
            .handle(handle);
        with_request_timeouts(server, request_timeout)
            .serve(make_service)
            .await?;
    }

    // Normal shutdown path — clean up PID file after connections drain.
    cleanup_pid_file(pid_file.as_deref());

    Ok(())
}

/// Apply the per-connection request timeouts to a server, in either
/// listener mode.
///
/// HTTP/1: a connection that has not finished sending its request headers
/// within `request_timeout` is closed; the same bound applies to the idle
/// wait for the next request on a keep-alive connection.
///
/// HTTP/2: a connection that stops answering PING frames is closed. A client
/// that sends the preface and then stalls holds the connection for at most
/// interval plus timeout.
fn with_request_timeouts<A: axum_server::Address, Acc>(
    mut server: axum_server::Server<A, Acc>,
    request_timeout: Duration,
) -> axum_server::Server<A, Acc> {
    server
        .http_builder()
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(request_timeout);
    server
        .http_builder()
        .http2()
        .timer(hyper_util::rt::TokioTimer::new())
        .keep_alive_interval(Some(request_timeout))
        .keep_alive_timeout(request_timeout);
    server
}

/// Remove the PID file if it exists. Best-effort — log but don't fail.
fn cleanup_pid_file(pid_file: Option<&std::path::Path>) {
    if let Some(path) = pid_file
        && let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("Failed to remove PID file {}: {e}", path.display());
    }
}

/// Set `TCP_NODELAY` on an accepted socket.
///
/// With Nagle's algorithm on, the kernel holds a small write while an earlier
/// small write on the connection is unacknowledged, and the client's delayed
/// ACK leaves with its next request on that connection or when its timer
/// fires (about 40 ms on Linux). Once one response is written while the
/// previous one is unacknowledged, each response waits for the client's next
/// request, and the reply to that request is held behind it in turn. The
/// response latency then equals the gap between the client's requests on the
/// connection, for every gap under the delayed-ACK timer. `TCP_NODELAY` sends
/// each write as it is made. The option lives on the TCP socket, so under TLS
/// it is set before the handshake and applies below the TLS stream. A failure
/// to set it is logged at debug and the connection is served with the kernel
/// default.
fn set_tcp_nodelay(stream: &tokio::net::TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::debug!("Failed to set TCP_NODELAY on accepted connection: {e}");
    }
}

/// The plaintext acceptor: sets `TCP_NODELAY` on every accepted socket (see
/// [`set_tcp_nodelay`]) and hands it on unchanged.
///
/// `axum_server` 0.8 ships a `NoDelayAcceptor`, but it rejects the
/// connection when the option cannot be set; this one logs and serves it,
/// as the TLS acceptor does.
#[derive(Clone)]
struct NodelayAcceptor;

impl<S: Send + 'static> axum_server::accept::Accept<tokio::net::TcpStream, S> for NodelayAcceptor {
    type Stream = tokio::net::TcpStream;
    type Service = S;
    type Future = std::future::Ready<std::io::Result<(Self::Stream, Self::Service)>>;

    fn accept(&self, stream: tokio::net::TcpStream, service: S) -> Self::Future {
        set_tcp_nodelay(&stream);
        std::future::ready(Ok((stream, service)))
    }
}

/// AI-2: Acceptor that detects plain HTTP connections on the TLS port.
///
/// Sets `TCP_NODELAY` on the accepted socket first (see [`set_tcp_nodelay`]).
/// Then peeks the first byte of the connection. If it's `0x16` (TLS
/// `ClientHello`), the connection passes through to the TLS acceptor. If it's
/// a plain HTTP verb, a 301 redirect to `https://` is written and the
/// connection is rejected with an IO error (which `axum_server` handles by
/// dropping it).
///
/// `axum_server` 0.8 ships a `NoDelayAcceptor`, but it cannot be stacked under
/// this acceptor without a generic wrapper, so this acceptor sets the option
/// itself, and logs instead of rejecting on failure.
#[derive(Clone)]
struct HttpsRedirectAcceptor {
    addr: std::net::SocketAddr,
}

impl<S: Send + 'static> axum_server::accept::Accept<tokio::net::TcpStream, S>
    for HttpsRedirectAcceptor
{
    type Stream = tokio::net::TcpStream;
    type Service = S;
    type Future = std::pin::Pin<
        Box<
            dyn std::future::Future<Output = std::io::Result<(Self::Stream, Self::Service)>> + Send,
        >,
    >;

    fn accept(&self, stream: tokio::net::TcpStream, service: S) -> Self::Future {
        let addr = self.addr;
        Box::pin(async move {
            set_tcp_nodelay(&stream);
            let mut peek_buf = [0u8; 1];
            match stream.peek(&mut peek_buf).await {
                Ok(0) => {
                    // Connection closed before sending data.
                    Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "empty connection",
                    ))
                }
                Ok(_) if peek_buf[0] == 0x16 => {
                    // TLS ClientHello — pass through to TLS acceptor.
                    Ok((stream, service))
                }
                Ok(_) => {
                    // Plain HTTP — send redirect and reject.
                    send_https_redirect(&stream, addr);
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "plain HTTP redirected to HTTPS",
                    ))
                }
                Err(e) => Err(e),
            }
        })
    }
}

/// Write an HTTP 301 redirect response to HTTPS on the raw TCP stream.
fn send_https_redirect(stream: &tokio::net::TcpStream, addr: std::net::SocketAddr) {
    let location = format!("https://{addr}/");
    let response = format!(
        "HTTP/1.1 301 Moved Permanently\r\n\
         Location: {location}\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\r\n"
    );
    // Best-effort write — if it fails, the client already got a connection error.
    let _ = stream.try_write(response.as_bytes());
}

#[cfg(unix)]
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let Ok(mut sigterm) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    else {
        tracing::error!("Failed to register SIGTERM handler, falling back to Ctrl+C only");
        let _ = ctrl_c.await;
        tracing::info!("Shutdown signal received, draining connections...");
        return;
    };
    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm.recv() => {},
    }
    tracing::info!("Shutdown signal received, draining connections...");
}

/// Non-unix: there is no SIGTERM; Ctrl+C (which tokio maps to the console
/// control handler on Windows) is the only shutdown signal — and it only
/// fires for an interactive console. Supervisors that terminate the process
/// directly (including the npm launcher's `stop()`, which is
/// `TerminateProcess` on Windows) hard-kill without reaching this path, so
/// the worker drain is skipped. That is acceptable: drain only cancels and
/// joins background workers — it flushes nothing, and storage commits
/// per-transaction.
#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("Shutdown signal received, draining connections...");
}

#[cfg(test)]
mod tests {
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
}

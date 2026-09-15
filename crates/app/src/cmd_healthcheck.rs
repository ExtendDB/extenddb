// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb healthcheck` — probe the local `/health` endpoint over HTTPS.
//!
//! Intended for a container `HEALTHCHECK`: it sends an HTTPS `GET /health` over
//! loopback and exits 0 or 1. It needs no shell or `curl`, so it also works on
//! a minimal `distroless`/`scratch` image.
//!
//! This checks liveness: exit 0 means the process is listening, completing TLS,
//! and serving HTTP. That is the signal a container `HEALTHCHECK` or a Kubernetes
//! liveness probe wants, since its job is to restart a wedged process. A liveness
//! probe should deliberately not fail on a backend outage. If it did, a shared
//! database briefly going away would restart every replica at once and make the
//! outage worse.
//!
//! What this does not give you is readiness. `/health` is a static handler that
//! never queries the storage backend, so a replica whose backend has gone away
//! still reports healthy and keeps receiving traffic. A backend that is
//! unreachable at startup does stop the server from listening at all, so that
//! case is caught. Closing the gap properly means adding a separate readiness
//! endpoint backed by a cheap cached round-trip to the storage layer and
//! pointing readiness probes at that, rather than making `/health` query the
//! backend and losing its value as a liveness signal.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use clap::Args;

use extenddb_config as config;

/// Port used when neither `--endpoint` nor the config file supplies one.
const DEFAULT_PORT: u16 = 18443;

/// End-to-end deadline for the network probe, including address resolution,
/// connect, TLS handshake, request write, and response read. The remaining
/// budget is applied before every blocking operation so a peer cannot keep the
/// probe alive indefinitely by making partial progress.
const TIMEOUT: Duration = Duration::from_secs(3);

/// Maximum response bytes read. Only the HTTP status line is used, so accepting
/// an unbounded body would waste memory without improving the health verdict.
const MAX_RESPONSE_BYTES: u64 = 8 * 1024;

#[derive(Args)]
pub struct HealthcheckArgs {
    /// Path to the config file, used to find the port when --endpoint is not given
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// Override the port to probe (defaults to the port in the config file)
    #[arg(short, long)]
    port: Option<u16>,

    /// Endpoint to probe, e.g. https://127.0.0.1:18443. Overrides --port and
    /// the config.
    #[arg(long)]
    endpoint: Option<String>,
}

/// Host and port to probe.
#[derive(Debug, PartialEq, Eq)]
struct Target {
    host: String,
    port: u16,
}

impl Target {
    /// `host:port`, bracketing a bare IPv6 literal so the result parses as an
    /// address.
    fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// Run the health check. Returns `Ok(())` when healthy, `Err` otherwise.
pub fn run(args: &HealthcheckArgs) -> anyhow::Result<()> {
    let target = resolve_target(args)?;
    let status = probe(&target)?;
    if (200..300).contains(&status) {
        Ok(())
    } else {
        anyhow::bail!("/health returned HTTP {status}")
    }
}

/// Decide what to probe: `--endpoint` if given, otherwise the address the
/// configured `bind_addr` implies, on `--port` or the configured port.
fn resolve_target(args: &HealthcheckArgs) -> anyhow::Result<Target> {
    if let Some(ep) = &args.endpoint {
        return parse_endpoint(ep);
    }
    // A config file that exists but cannot be parsed is worth reporting rather
    // than papering over by falling back to defaults.
    let (host, config_port) = if std::path::Path::new(&args.config).exists() {
        let cfg = config::load(&args.config)
            .map_err(|e| anyhow::anyhow!("Failed to load config '{}': {e}", args.config))?;
        (probe_host(&cfg.server.bind_addr), cfg.server.port)
    } else {
        // Zero-config (the dev container case): the server derives its address
        // from `EXTENDDB__SERVER__*` environment overrides on built-in
        // defaults, so the probe must honour the same overrides or a server
        // moved with `EXTENDDB__SERVER__PORT` serves correctly while sitting
        // unhealthy forever. An unparseable port is reported rather than
        // defaulted: `serve` would refuse the same value, so a silently
        // "healthy" default-port probe would mask the real failure.
        let host = std::env::var("EXTENDDB__SERVER__BIND_ADDR")
            .map_or_else(|_| "127.0.0.1".to_owned(), |b| probe_host(&b));
        let port = match std::env::var("EXTENDDB__SERVER__PORT") {
            Ok(v) => v.trim().parse::<u16>().map_err(|e| {
                anyhow::anyhow!("EXTENDDB__SERVER__PORT '{v}' is not a valid port: {e}")
            })?,
            Err(_) => DEFAULT_PORT,
        };
        (host, port)
    };
    Ok(Target {
        host,
        port: args.port.unwrap_or(config_port),
    })
}

/// The address to probe for a server bound to `bind_addr`.
///
/// A wildcard bind is not itself connectable, so it maps to the loopback address
/// of the same family. Any other bind address is probed as configured, which
/// matters for an IPv6-only deployment: assuming `127.0.0.1` reports a healthy
/// server bound to `::1` as unhealthy.
fn probe_host(bind_addr: &str) -> String {
    match bind_addr.trim() {
        "" | "0.0.0.0" => "127.0.0.1".to_owned(),
        "::" | "[::]" | "::0" => "::1".to_owned(),
        other => other
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned(),
    }
}

/// Parse `[scheme://]host[:port][/path]` into a [`Target`].
///
/// The scheme and path are accepted and then ignored: the transport is decided
/// by the build, not the URL. A production build always speaks TLS because the
/// server has no plaintext mode; a `dev-mode` build always speaks plain HTTP
/// because that server has no TLS. Always requests `/health`.
fn parse_endpoint(endpoint: &str) -> anyhow::Result<Target> {
    let ep = endpoint.trim();
    let rest = ep
        .strip_prefix("https://")
        .or_else(|| ep.strip_prefix("http://"))
        .unwrap_or(ep);
    // Keep only the authority, dropping any path or query.
    let authority = rest
        .split(['/', '?'])
        .next()
        .unwrap_or("")
        .trim_end_matches('.');
    if authority.is_empty() {
        anyhow::bail!("--endpoint '{endpoint}' has no host");
    }

    // Bracketed IPv6 literal: [::1] or [::1]:18443.
    if let Some(after_bracket) = authority.strip_prefix('[') {
        let (host, tail) = after_bracket
            .split_once(']')
            .ok_or_else(|| anyhow::anyhow!("--endpoint '{endpoint}' has an unclosed '['"))?;
        return Ok(Target {
            host: host.to_owned(),
            port: parse_port(tail, endpoint)?,
        });
    }

    // An unbracketed IPv6 literal has more than one colon and cannot carry a
    // port, so treat the whole thing as the host.
    if authority.matches(':').count() > 1 {
        return Ok(Target {
            host: authority.to_owned(),
            port: DEFAULT_PORT,
        });
    }

    match authority.split_once(':') {
        Some((host, port)) => Ok(Target {
            host: host.to_owned(),
            port: parse_port(&format!(":{port}"), endpoint)?,
        }),
        None => Ok(Target {
            host: authority.to_owned(),
            port: DEFAULT_PORT,
        }),
    }
}

/// Parse a `":<port>"` suffix, falling back to the default port when there is
/// no suffix at all.
fn parse_port(suffix: &str, endpoint: &str) -> anyhow::Result<u16> {
    match suffix.strip_prefix(':') {
        None if suffix.is_empty() => Ok(DEFAULT_PORT),
        None => anyhow::bail!("--endpoint '{endpoint}' has trailing garbage after the host"),
        Some(p) => p
            .parse()
            .map_err(|e| anyhow::anyhow!("--endpoint '{endpoint}' has an invalid port '{p}': {e}")),
    }
}

/// Return the time left before `deadline`, or a timeout error when exhausted.
fn remaining_budget(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "healthcheck deadline exceeded"))
}

/// A TCP stream that applies the probe's remaining budget before every socket
/// read and write, including the calls rustls makes internally during the TLS
/// handshake. A fixed per-syscall timeout is insufficient because a peer can
/// make partial progress just before each timeout and keep the probe alive.
struct DeadlineStream {
    inner: TcpStream,
    deadline: Instant,
}

impl DeadlineStream {
    fn new(inner: TcpStream, deadline: Instant) -> Self {
        Self { inner, deadline }
    }
}

impl Read for DeadlineStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner
            .set_read_timeout(Some(remaining_budget(self.deadline)?))?;
        self.inner.read(buf)
    }
}

impl Write for DeadlineStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner
            .set_write_timeout(Some(remaining_budget(self.deadline)?))?;
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner
            .set_write_timeout(Some(remaining_budget(self.deadline)?))?;
        self.inner.flush()
    }
}

/// Make an HTTPS `GET /health` request. Returns the HTTP status code.
fn probe(target: &Target) -> anyhow::Result<u16> {
    let deadline = Instant::now() + TIMEOUT;

    // A dev-mode build serves plain HTTP, so probing it over TLS fails the
    // handshake and reports a healthy server as unhealthy. Production builds
    // have no plaintext mode, so this branch does not exist in them: it is
    // compiled out, and `postgres`/`mongodb` cannot enable `dev-mode`.
    if cfg!(feature = "dev-mode") {
        let tcp = connect(target, deadline)?;
        let mut tcp = DeadlineStream::new(tcp, deadline);
        tcp.write_all(http_request(target).as_bytes())
            .map_err(|e| anyhow::anyhow!("Failed to send request: {e}"))?;
        return read_status(&mut tcp);
    }

    // rustls 0.23 requires an explicit CryptoProvider. Installing it is
    // idempotent, so ignore the error if one is already installed.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let tls_config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert))
        .with_no_client_auth();

    let server_name = rustls::pki_types::ServerName::try_from(target.host.clone())
        .map_err(|e| anyhow::anyhow!("Invalid server name '{}': {e}", target.host))?;

    let mut conn = rustls::ClientConnection::new(Arc::new(tls_config), server_name)
        .map_err(|e| anyhow::anyhow!("TLS setup failed: {e}"))?;

    let tcp = connect(target, deadline)?;
    let mut tcp = DeadlineStream::new(tcp, deadline);

    let mut tls = rustls::Stream::new(&mut conn, &mut tcp);
    tls.write_all(http_request(target).as_bytes())
        .map_err(|e| anyhow::anyhow!("Failed to send request: {e}"))?;

    read_status(&mut tls)
}

/// The `/health` request. `Connection: close` lets the read finish on EOF.
fn http_request(target: &Target) -> String {
    let authority = target.authority();
    format!("GET /health HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n")
}

/// Read a response and return its HTTP status code.
fn read_status(stream: &mut impl Read) -> anyhow::Result<u16> {
    let mut response = Vec::new();
    match stream.take(MAX_RESPONSE_BYTES).read_to_end(&mut response) {
        Ok(_) => {}
        // We asked for `Connection: close`, so the server closing the
        // connection after the response is expected.
        Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {}
        Err(e) => return Err(anyhow::anyhow!("Read error: {e}")),
    }

    let status_line = response
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .unwrap_or("");
    status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("No HTTP status in response"))
}

/// Resolve the target without allowing a wedged system resolver to outlive the
/// probe deadline. A timed-out resolver thread is detached; the short-lived CLI
/// process exits immediately and the operating system tears it down.
fn resolve(target: &Target, deadline: Instant) -> anyhow::Result<Vec<SocketAddr>> {
    let authority = target.authority();
    let resolve_authority = authority.clone();
    let (sender, receiver) = mpsc::sync_channel(1);
    let worker = thread::Builder::new()
        .name("healthcheck-resolver".to_owned())
        .spawn(move || {
            let result = resolve_authority
                .to_socket_addrs()
                .map(|addrs| addrs.collect());
            let _ = sender.send(result);
        })
        .map_err(|e| anyhow::anyhow!("Cannot start resolver for {authority}: {e}"))?;

    let remaining = remaining_budget(deadline)
        .map_err(|e| anyhow::anyhow!("Cannot resolve {authority}: {e}"))?;
    match receiver.recv_timeout(remaining) {
        Ok(result) => {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("Resolver for {authority} panicked"))?;
            result.map_err(|e| anyhow::anyhow!("Cannot resolve {authority}: {e}"))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            anyhow::bail!("Cannot resolve {authority}: healthcheck deadline exceeded")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            let _ = worker.join();
            anyhow::bail!("Cannot resolve {authority}: resolver stopped without a result")
        }
    }
}

/// Connect within the shared probe deadline, trying every resolved address.
///
/// Trying all of them matters for a name like `localhost` that resolves to both
/// `::1` and `127.0.0.1`, since the server may be bound to only one of them.
fn connect(target: &Target, deadline: Instant) -> anyhow::Result<TcpStream> {
    let authority = target.authority();
    let addrs = resolve(target, deadline)?;
    if addrs.is_empty() {
        anyhow::bail!("Cannot resolve {authority}: no addresses");
    }
    let mut last_err = None;
    for addr in addrs {
        let remaining = remaining_budget(deadline)
            .map_err(|e| anyhow::anyhow!("Cannot connect to {authority}: {e}"))?;
        match TcpStream::connect_timeout(&addr, remaining) {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    Err(anyhow::anyhow!(
        "Cannot connect to {authority}: {}",
        last_err.expect("non-empty address list yields an error")
    ))
}

/// A rustls verifier that accepts any server certificate.
///
/// The health check makes no trust decision. All it reports is whether the
/// server answered, and all it reads from the response is the HTTP status line.
/// The default deployment serves a self-signed certificate, so validating it
/// would mean distributing a trust anchor to every probe for no benefit.
///
/// This applies to `--endpoint` as well as to the loopback default, so a
/// non-loopback endpoint could be answered by an impostor. The worst outcome is
/// a wrong health verdict, but only point `--endpoint` at a server you
/// control.
#[derive(Debug)]
struct AcceptAnyServerCert;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_PORT, Target, parse_endpoint};

    fn target(host: &str, port: u16) -> Target {
        Target {
            host: host.to_owned(),
            port,
        }
    }

    #[test]
    fn parses_scheme_host_and_port() {
        assert_eq!(
            parse_endpoint("https://127.0.0.1:18443").unwrap(),
            target("127.0.0.1", 18443)
        );
        // A plaintext scheme is accepted and ignored, since the probe always
        // uses TLS.
        assert_eq!(
            parse_endpoint("http://127.0.0.1:9000").unwrap(),
            target("127.0.0.1", 9000)
        );
        assert_eq!(
            parse_endpoint("  https://extenddb.svc:8443/  ").unwrap(),
            target("extenddb.svc", 8443)
        );
    }

    #[test]
    fn defaults_the_port_when_absent() {
        assert_eq!(
            parse_endpoint("https://localhost").unwrap(),
            target("localhost", DEFAULT_PORT)
        );
        assert_eq!(
            parse_endpoint("localhost").unwrap(),
            target("localhost", DEFAULT_PORT)
        );
    }

    #[test]
    fn ignores_a_path() {
        assert_eq!(
            parse_endpoint("https://127.0.0.1:18443/health").unwrap(),
            target("127.0.0.1", 18443)
        );
    }

    #[test]
    fn parses_ipv6_literals() {
        assert_eq!(
            parse_endpoint("https://[::1]:18443").unwrap(),
            target("::1", 18443)
        );
        assert_eq!(
            parse_endpoint("https://[::1]").unwrap(),
            target("::1", DEFAULT_PORT)
        );
        // Unbracketed IPv6 is treated as a bare host on the default port.
        assert_eq!(parse_endpoint("::1").unwrap(), target("::1", DEFAULT_PORT));
    }

    #[test]
    fn maps_bind_addr_to_a_probe_host() {
        use super::probe_host;
        // Wildcard binds map to loopback of the same family.
        assert_eq!(probe_host("0.0.0.0"), "127.0.0.1");
        assert_eq!(probe_host(""), "127.0.0.1");
        assert_eq!(probe_host("::"), "::1");
        assert_eq!(probe_host("[::]"), "::1");
        // Anything else is probed as configured.
        assert_eq!(probe_host("127.0.0.1"), "127.0.0.1");
        assert_eq!(probe_host("::1"), "::1");
        assert_eq!(probe_host("[::1]"), "::1");
        assert_eq!(probe_host("10.0.0.5"), "10.0.0.5");
    }

    #[test]
    fn rejects_malformed_endpoints() {
        assert!(parse_endpoint("https://").is_err());
        assert!(parse_endpoint("https://127.0.0.1:notaport").is_err());
        assert!(parse_endpoint("https://[::1:18443").is_err());
    }
}

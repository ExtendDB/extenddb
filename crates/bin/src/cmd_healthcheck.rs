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

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use clap::Args;

use crate::config;

/// Port used when neither `--endpoint` nor the config file supplies one.
const DEFAULT_PORT: u16 = 18443;

/// Bound on connect, read, and write. `TcpStream::connect` has no timeout of its
/// own, so without this an unreachable address hangs for the OS default of
/// roughly two minutes, far longer than any health-check interval.
const TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Args)]
pub struct HealthcheckArgs {
    /// Path to the config file, used to find the port when --endpoint is not given
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// Endpoint to probe, e.g. https://127.0.0.1:18443. Overrides the config.
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

/// Decide what to probe: `--endpoint` if given, otherwise loopback on the port
/// from the config file.
fn resolve_target(args: &HealthcheckArgs) -> anyhow::Result<Target> {
    if let Some(ep) = &args.endpoint {
        return parse_endpoint(ep);
    }
    // A config file that exists but cannot be parsed is worth reporting rather
    // than papering over by falling back to the default port.
    let port = if std::path::Path::new(&args.config).exists() {
        config::load(&args.config)
            .map_err(|e| anyhow::anyhow!("Failed to load config '{}': {e}", args.config))?
            .server
            .port
    } else {
        DEFAULT_PORT
    };
    Ok(Target {
        host: "127.0.0.1".to_owned(),
        port,
    })
}

/// Parse `[scheme://]host[:port][/path]` into a [`Target`].
///
/// The scheme and path are accepted and then ignored. The probe always speaks
/// TLS, because the server has no plaintext mode, and always requests
/// `/health`.
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

/// Make an HTTPS `GET /health` request. Returns the HTTP status code.
fn probe(target: &Target) -> anyhow::Result<u16> {
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

    let mut tcp = connect(target)?;
    let _ = tcp.set_read_timeout(Some(TIMEOUT));
    let _ = tcp.set_write_timeout(Some(TIMEOUT));

    let authority = target.authority();
    let request = format!("GET /health HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n");

    let mut tls = rustls::Stream::new(&mut conn, &mut tcp);
    tls.write_all(request.as_bytes())
        .map_err(|e| anyhow::anyhow!("Failed to send request: {e}"))?;

    let mut response = String::new();
    match tls.read_to_string(&mut response) {
        Ok(_) => {}
        // We asked for `Connection: close`, so the server closing the
        // connection after the response is expected.
        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
        Err(e) => return Err(anyhow::anyhow!("Read error: {e}")),
    }

    let status_line = response.lines().next().unwrap_or("");
    status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("No HTTP status in response"))
}

/// Connect with a bounded timeout, trying every resolved address.
///
/// Trying all of them matters for a name like `localhost` that resolves to both
/// `::1` and `127.0.0.1`, since the server may be bound to only one of them.
fn connect(target: &Target) -> anyhow::Result<TcpStream> {
    let authority = target.authority();
    let addrs: Vec<_> = authority
        .to_socket_addrs()
        .map_err(|e| anyhow::anyhow!("Cannot resolve {authority}: {e}"))?
        .collect();
    if addrs.is_empty() {
        anyhow::bail!("Cannot resolve {authority}: no addresses");
    }
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, TIMEOUT) {
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
    fn rejects_malformed_endpoints() {
        assert!(parse_endpoint("https://").is_err());
        assert!(parse_endpoint("https://127.0.0.1:notaport").is_err());
        assert!(parse_endpoint("https://[::1:18443").is_err());
    }
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Developer-mode host guard.
//!
//! Developer mode serves plain HTTP on loopback with the IAM policy decision
//! open for an authenticated caller, and it seeds a well-known credential. A
//! web page served from a name that later resolves to the loopback address is
//! then same-origin with this server, so the browser sends the page's requests
//! to it with no preflight, and the page can sign them with the published
//! credential. This guard closes that path.
//!
//! A request is a browser request when it carries an `Origin` header or a
//! `Sec-Fetch-Site` header. A browser sends `Origin` on every request whose
//! method is not `GET` or `HEAD` and on every cross-origin CORS request, and it
//! sends `Sec-Fetch-*` headers only to `https` and loopback URLs. A page script
//! cannot remove either header, because both are forbidden request-header
//! names. The other `Sec-Fetch-*` headers do not count on their own: Node's
//! built-in `fetch` sends `Sec-Fetch-Mode: cors` by itself, and every browser
//! that sends Fetch Metadata sends `Sec-Fetch-Site` with it. The AWS SDKs and
//! the CLI send none of these headers.
//!
//! In developer mode a browser request is served only when its host is an IP
//! address, `localhost`, a name ending in `.localhost`, or a name the operator
//! lists in [`SETTING_KEY`]. Every other browser request is refused with 403
//! before authentication. A call to the DynamoDB API and a console login are
//! `POST`s, which carry `Origin`, so a rebinding page can make neither.
//!
//! A same-origin `GET` or `HEAD` from a page served over plain HTTP from any
//! other name carries neither header, and the management routes and `/metrics`
//! answer `GET` with the admin password. Every request to `/management` and
//! below, and to `/metrics`, is therefore checked against the host rule
//! whether or not it carries a marker. A client that calls them through
//! another name, such as a Compose service name, needs that name listed. Any
//! other request with no marker is served as before, whatever its host.
//!
//! Firefox before 70 and EdgeHTML send no `Origin` on a `POST` and no
//! `Sec-Fetch-*` headers, so outside the admin routes the guard serves their
//! requests as it serves a non-browser client. The guard is installed only in
//! developer mode; production serving is unchanged.

use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};
use extenddb_core::error::DynamoDbError;

/// Configuration key for the operator-listed host names.
pub(crate) const SETTING_KEY: &str = "server.dev_allowed_hosts";
/// Environment-variable form of [`SETTING_KEY`].
pub(crate) const SETTING_ENV: &str = "EXTENDDB__SERVER__DEV_ALLOWED_HOSTS";

/// Path prefixes checked against the host rule on every request: the routes
/// that answer `GET` with the admin password.
const ADMIN_PREFIXES: [&str; 2] = ["/management", "/metrics"];

/// Per-request state for the host guard: the operator-listed host names,
/// validated and lowercased at startup.
#[derive(Clone)]
struct DevHostGuard {
    allowed: Arc<[String]>,
}

/// Wrap `app` in the guard when `dev_mode` is true; return it unchanged
/// otherwise. `start_server` calls this before it adds the security headers,
/// so the guard's 403s carry them.
pub(crate) fn install(app: Router, dev_mode: bool, allowed: Arc<[String]>) -> Router {
    if !dev_mode {
        return app;
    }
    let guard = Arc::new(DevHostGuard { allowed });
    app.layer(axum::middleware::from_fn_with_state(guard, enforce))
}

/// Parse [`SETTING_KEY`] at startup and log the rule and each dropped entry.
/// Returns the kept names; outside developer mode the setting is ignored and
/// the list is empty.
pub(crate) fn allowed_hosts(dev_mode: bool, raw: Option<&str>) -> Arc<[String]> {
    if !dev_mode {
        return Arc::from(Vec::new());
    }
    let (kept, dropped) = parse_dev_allowed_hosts(raw.unwrap_or(""));
    for entry in &dropped {
        tracing::warn!("DEVELOPER MODE: {}", dropped_entry_note(entry));
    }
    tracing::warn!("DEVELOPER MODE: {}", rule_line(&kept));
    Arc::from(kept)
}

/// The note for an entry [`parse_dev_allowed_hosts`] dropped, saying what a
/// kept entry looks like, for the startup banner and the log.
pub fn dropped_entry_note(entry: &str) -> String {
    format!(
        "dev_allowed_hosts entry '{entry}' was ignored: list a bare host name with no port, \
         scheme or wildcard, such as app.test."
    )
}

/// The host rule, as the startup banner, the log and the 403 message state it.
fn rule() -> String {
    format!(
        "ExtendDB serves a browser request, and any request to /management or /metrics, only \
         when its host is an IP address, localhost, a name ending in .localhost, or a name in \
         {SETTING_KEY} (environment variable {SETTING_ENV})."
    )
}

/// The host rule followed by the names kept from the setting, for the startup
/// banner and the log.
pub fn rule_line(kept: &[String]) -> String {
    let listed = if kept.is_empty() {
        "(none)".to_owned()
    } else {
        kept.join(", ")
    };
    format!("{} Listed hosts: {listed}", rule())
}

/// Whether a request carries a browser marker (`Origin` or `Sec-Fetch-Site`).
/// Header names in [`HeaderMap`] are stored lowercased.
fn is_browser_request(headers: &HeaderMap) -> bool {
    headers.contains_key("origin") || headers.contains_key("sec-fetch-site")
}

/// Whether a path is `/management`, `/metrics`, or a path below either.
fn is_admin_path(path: &str) -> bool {
    ADMIN_PREFIXES.iter().any(|prefix| {
        path.strip_prefix(prefix)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    })
}

/// Classification of a host token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostKind {
    /// An IPv4 or IPv6 literal.
    Ip,
    /// A syntactically valid DNS name.
    Dns,
    /// Neither; refused before any comparison.
    Invalid,
}

fn classify(host: &str) -> HostKind {
    if host.parse::<IpAddr>().is_ok() {
        HostKind::Ip
    } else if is_valid_dns_name(host) {
        HostKind::Dns
    } else {
        HostKind::Invalid
    }
}

/// A DNS name: 1 to 253 characters, dot-separated labels of 1 to 63 characters
/// drawn from letters, digits, hyphen and underscore. An empty label (a leading
/// or trailing dot, or two dots in a row) is rejected.
fn is_valid_dns_name(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    })
}

/// Whether a DNS name is `localhost` or ends in `.localhost` with at least one
/// label before the suffix.
fn is_loopback_name(host: &str) -> bool {
    const SUFFIX: &str = ".localhost";
    host.eq_ignore_ascii_case("localhost")
        || (host.len() > SUFFIX.len()
            && host[host.len() - SUFFIX.len()..].eq_ignore_ascii_case(SUFFIX))
}

/// Extract the host token from an authority or `Host`-header value: drop any
/// port, unwrap IPv6 brackets, and ignore a single trailing dot. Returns `None`
/// when the value has no host part or is structurally malformed, including
/// brackets around anything but an IPv6 address.
fn host_label(raw: &str) -> Option<&str> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let host = if let Some(rest) = raw.strip_prefix('[') {
        // Bracketed IPv6: [addr] or [addr]:port.
        let end = rest.find(']')?;
        let after = &rest[end + 1..];
        if !(after.is_empty() || after.starts_with(':')) {
            return None;
        }
        let inner = &rest[..end];
        if inner.parse::<Ipv6Addr>().is_err() {
            return None;
        }
        inner
    } else {
        match raw.split_once(':') {
            // A second colon means an unbracketed IPv6, which is not valid in a
            // Host header or authority.
            Some((_, port)) if port.contains(':') => return None,
            Some((h, _)) => h,
            None => raw,
        }
    };
    let host = host.strip_suffix('.').unwrap_or(host);
    if host.is_empty() {
        return None;
    }
    Some(host)
}

/// Why a request was refused. Carries the offending host where one was
/// determined, for the message.
enum RejectReason {
    /// A valid host that is neither loopback nor listed.
    UnlistedHost(String),
    /// A host token that is not an IP literal or a valid DNS name.
    InvalidHost(String),
    /// No readable `Host` header and no request-target authority.
    MissingHost,
    /// More than one `Host` header.
    RepeatedHost,
    /// A `Host` header and an absolute-form request target that name different
    /// hosts.
    HostMismatch,
}

impl RejectReason {
    fn message(&self) -> String {
        let rule = rule();
        match self {
            Self::UnlistedHost(h) => {
                format!("Developer mode refused a request for host '{h}'. {rule}")
            }
            Self::InvalidHost(h) => {
                format!("Developer mode refused a request: '{h}' is not a valid host name. {rule}")
            }
            Self::MissingHost => {
                format!("Developer mode refused a request with no usable Host header. {rule}")
            }
            Self::RepeatedHost => {
                format!("Developer mode refused a request with more than one Host header. {rule}")
            }
            Self::HostMismatch => format!(
                "Developer mode refused a request whose Host header and request target name \
                 different hosts. {rule}"
            ),
        }
    }
}

/// The guard decision for one request.
enum Decision {
    /// Serve the request unchanged.
    Allow,
    /// Refuse before authentication.
    Reject(RejectReason),
}

/// Decide whether a request is served. A request with no browser marker is
/// allowed unless it is for an admin route. Every other request is allowed
/// only when its single, agreeing host is an IP literal, a loopback name, or a
/// listed name.
fn evaluate(headers: &HeaderMap, uri: &Uri, allowed: &[String]) -> Decision {
    if !is_browser_request(headers) && !is_admin_path(uri.path()) {
        return Decision::Allow;
    }

    // More than one Host header is refused: a split header lets a page
    // disagree with what verification reads.
    let mut host_values = headers.get_all("host").iter();
    let first = host_values.next();
    if host_values.next().is_some() {
        return Decision::Reject(RejectReason::RepeatedHost);
    }
    let host_header = match first {
        Some(v) => match v.to_str() {
            Ok(s) => Some(s),
            Err(_) => return Decision::Reject(RejectReason::MissingHost),
        },
        None => None,
    };
    let authority = uri.authority().map(axum::http::uri::Authority::as_str);

    // The host is the Host header, or the request-target authority when there
    // is no Host. When both are present they must name the same host (ports
    // ignored), or the request is refused.
    let raw = match (host_header, authority) {
        (Some(h), Some(a)) => match (host_label(h), host_label(a)) {
            (Some(x), Some(y)) if x.eq_ignore_ascii_case(y) => h,
            _ => return Decision::Reject(RejectReason::HostMismatch),
        },
        (Some(h), None) => h,
        (None, Some(a)) => a,
        (None, None) => return Decision::Reject(RejectReason::MissingHost),
    };

    let Some(host) = host_label(raw) else {
        return Decision::Reject(RejectReason::InvalidHost(raw.trim().to_owned()));
    };

    match classify(host) {
        HostKind::Ip => Decision::Allow,
        HostKind::Invalid => Decision::Reject(RejectReason::InvalidHost(host.to_owned())),
        HostKind::Dns => {
            if is_loopback_name(host) || allowed.iter().any(|a| a.eq_ignore_ascii_case(host)) {
                Decision::Allow
            } else {
                Decision::Reject(RejectReason::UnlistedHost(host.to_owned()))
            }
        }
    }
}

/// axum middleware: refuse a dev-mode request whose host is foreign, before
/// the request reaches authentication.
async fn enforce(State(cfg): State<Arc<DevHostGuard>>, req: Request, next: Next) -> Response {
    match evaluate(req.headers(), req.uri(), &cfg.allowed) {
        Decision::Allow => next.run(req).await,
        Decision::Reject(reason) => reject_response(req.uri().path(), &reason),
    }
}

/// Build the 403 for a refused request, in the shape its surface uses: the
/// console error page for a `/console` path, the `DynamoDB` JSON error for
/// everything else.
fn reject_response(path: &str, reason: &RejectReason) -> Response {
    let message = reason.message();
    if path.starts_with("/console") {
        (
            StatusCode::FORBIDDEN,
            Html(crate::console::host_refused_page(&message)),
        )
            .into_response()
    } else {
        let request_id = uuid::Uuid::new_v4().to_string();
        crate::response::error_response_with_status(
            &DynamoDbError::AccessDeniedException(message),
            &request_id,
            StatusCode::FORBIDDEN,
        )
    }
}

/// Parse the comma-separated [`SETTING_KEY`] value into the host names kept and
/// the entries dropped. An entry must be a bare host name (an IP literal or a
/// valid DNS name, a single trailing dot ignored) with no port, scheme or
/// wildcard; any other entry is dropped, and the startup banner names it. Kept
/// names are lowercased for a case-insensitive comparison against a request
/// host.
pub fn parse_dev_allowed_hosts(raw: &str) -> (Vec<String>, Vec<String>) {
    let mut kept = Vec::new();
    let mut dropped = Vec::new();
    for entry in raw.split(',') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let candidate = trimmed.strip_suffix('.').unwrap_or(trimmed);
        match classify(candidate) {
            HostKind::Dns | HostKind::Ip => kept.push(candidate.to_ascii_lowercase()),
            HostKind::Invalid => dropped.push(trimmed.to_owned()),
        }
    }
    (kept, dropped)
}

#[cfg(test)]
mod tests;

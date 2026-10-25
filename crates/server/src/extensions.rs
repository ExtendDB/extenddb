// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Extension points a thin binary hands to the server at startup.
//!
//! A backend crate's `main` calls `extenddb_app::run_with(build, extensions)`
//! with an [`Extensions`] value. Its HTTP routers are mounted under their
//! prefixes after the built-in routes and before the response security layer,
//! so an extension response carries the same security headers as a built-in
//! one. Its [`RequestObserver`], when set, receives one [`RequestRecord`] per
//! request that reached authentication: the operation, the status, the error
//! type, the latency, a hash of the request body and the two SDK correlation
//! headers. The record is built only when an observer is installed; with none,
//! the request path hashes no body and builds no record.

use std::hash::{DefaultHasher, Hasher};
use std::sync::Arc;

use axum::Router;

/// What a thin binary adds to the server. Build it from
/// [`Extensions::default`] with [`with_router`](Self::with_router) and
/// [`with_request_observer`](Self::with_request_observer); the struct may gain
/// fields in a minor release.
#[derive(Default)]
#[non_exhaustive]
pub struct Extensions {
    /// Routers to mount, each under an absolute prefix of literal segments
    /// that is not `/`, is not at or under a built-in route, and is not at,
    /// under or above another extension's prefix. A violation is a startup
    /// error. The server applies its response security headers to these
    /// routes and nothing else: no authentication, no request rate limit, and
    /// axum's default 2 MB body limit for the limited extractors (none for a
    /// raw `Body`). A route that changes state or reads data authenticates in
    /// its own handler or layer.
    pub http: Vec<(String, Router)>,
    /// Receives one record per request that reached authentication.
    pub request_observer: Option<Arc<dyn RequestObserver>>,
}

impl Extensions {
    /// Add a router to mount under `prefix`.
    #[must_use]
    pub fn with_router(mut self, prefix: impl Into<String>, router: Router) -> Self {
        self.http.push((prefix.into(), router));
        self
    }

    /// Install the per-request observer.
    #[must_use]
    pub fn with_request_observer(mut self, observer: Arc<dyn RequestObserver>) -> Self {
        self.request_observer = Some(observer);
        self
    }
}

/// A per-request hook. The server calls [`on_response`](Self::on_response)
/// once per request after the response is built, on the request's own task,
/// outside the request scope and holding no lock. The call runs before the
/// response is written to the socket, so a slow observer delays the response;
/// an observer that does more than record should hand the record to its own
/// task. Calls for different requests arrive concurrently from the connection
/// tasks, in no defined order. The method must not panic: a panic there fails
/// the request and closes its connection.
pub trait RequestObserver: Send + Sync {
    /// One request's outcome.
    fn on_response(&self, record: RequestRecord);
}

/// What the server reports about one request.
///
/// Built through the server only; the struct may gain fields in a minor
/// release.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RequestRecord {
    /// The request id the client received in `x-amzn-RequestId`.
    pub request_id: String,
    /// The operation name from `X-Amz-Target`.
    pub operation: String,
    /// The request's top-level `TableName`, when it has one: `None` for the
    /// batch, transaction, PartiQL and stream operations, whose tables are
    /// inside the request body.
    pub table_name: Option<String>,
    /// The authenticated account: `Some` on every recorded request that got a
    /// response after authentication, `None` on an authentication refusal and
    /// on an abandoned request (status 0).
    pub account_id: Option<String>,
    /// The HTTP status the client received, or 0 when no response was built
    /// because the client went away while the request was in flight.
    pub status: u16,
    /// The `__type` of the error the client received, without its namespace,
    /// or `None` on success.
    pub error_type: Option<String>,
    /// Handler wall time in microseconds, from the handler's entry (the body
    /// has been read by then) to the record.
    pub latency_us: u64,
    /// A 64-bit hash of the raw request body, so an observer can tell retries
    /// of one request from distinct requests without holding the body. The
    /// standard library's default hasher: compare values within one process
    /// only, not across builds.
    pub body_hash: u64,
    /// `amz-sdk-invocation-id` when the client sent it.
    pub sdk_invocation_id: Option<String>,
    /// `amz-sdk-request` when the client sent it.
    pub sdk_request: Option<String>,
}

/// Hash of the raw request body for [`RequestRecord::body_hash`].
pub(crate) fn body_hash(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(bytes);
    hasher.finish()
}

/// Prefixes the server owns; an extension may not mount at or under them.
pub(crate) const RESERVED_PREFIXES: &[&str] = &["/health", "/metrics", "/management", "/console"];

/// Why an extension router could not be mounted. The server does not start.
#[derive(Debug)]
pub enum ExtensionMountError {
    /// The prefix does not start with `/`.
    NotAbsolute(String),
    /// The prefix is `/`, which would shadow the request handler.
    Root,
    /// The prefix ends with `/`, has an empty segment (`//`), has a parameter
    /// or wildcard segment (`{`, `}`, `*`), or has a byte no request path can
    /// carry (`?`, `#`, a space, a control byte). `Router::nest` refuses or
    /// misroutes the first group, an extension at `/{id}` would take every
    /// top-level path the server does not register, and a router under the
    /// last group mounts but no request can reach it.
    BadShape(String),
    /// The prefix is a reserved prefix, is at or under one, or is at, under
    /// or above an earlier extension's prefix.
    Collision {
        /// The prefix that was refused.
        prefix: String,
        /// The route it collides with.
        conflicts_with: String,
    },
}

impl std::fmt::Display for ExtensionMountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAbsolute(prefix) => {
                write!(f, "extension router prefix '{prefix}' must start with '/'")
            }
            Self::Root => write!(
                f,
                "extension router prefix '/' is not allowed: it would shadow the \
                 request handler at the root path"
            ),
            Self::BadShape(prefix) => write!(
                f,
                "extension router prefix '{prefix}' must be one or more non-empty \
                 literal path segments with no trailing '/', no parameter or wildcard, \
                 and no '?', '#', space or control byte"
            ),
            Self::Collision {
                prefix,
                conflicts_with,
            } => write!(
                f,
                "extension router prefix '{prefix}' collides with '{conflicts_with}'"
            ),
        }
    }
}

impl std::error::Error for ExtensionMountError {}

/// `a` is `b`, or is under `b` (`b` followed by `/`).
fn at_or_under(a: &str, b: &str) -> bool {
    a == b || (a.len() > b.len() && a.starts_with(b) && a.as_bytes()[b.len()] == b'/')
}

/// Check one prefix against the rules and the prefixes already taken.
pub(crate) fn validate_extension_prefix(
    prefix: &str,
    already_taken: &[String],
) -> Result<(), ExtensionMountError> {
    if !prefix.starts_with('/') {
        return Err(ExtensionMountError::NotAbsolute(prefix.to_owned()));
    }
    if prefix == "/" {
        return Err(ExtensionMountError::Root);
    }
    if prefix.ends_with('/')
        || prefix.contains("//")
        || prefix
            .bytes()
            .any(|b| matches!(b, b'{' | b'}' | b'*' | b'?' | b'#' | b' ') || b.is_ascii_control())
    {
        return Err(ExtensionMountError::BadShape(prefix.to_owned()));
    }
    if let Some(reserved) = RESERVED_PREFIXES.iter().find(|r| at_or_under(prefix, r)) {
        return Err(ExtensionMountError::Collision {
            prefix: prefix.to_owned(),
            conflicts_with: (*reserved).to_owned(),
        });
    }
    if let Some(taken) = already_taken
        .iter()
        .find(|t| at_or_under(prefix, t) || at_or_under(t, prefix))
    {
        return Err(ExtensionMountError::Collision {
            prefix: prefix.to_owned(),
            conflicts_with: taken.clone(),
        });
    }
    Ok(())
}

/// Check every prefix in `http` as [`mount_extensions`] will, without
/// mounting. The server calls this before it connects to the backend or
/// forks, so a refused prefix reaches stderr instead of a daemon's log.
pub fn validate_extension_prefixes(http: &[(String, Router)]) -> Result<(), ExtensionMountError> {
    let mut taken: Vec<String> = Vec::with_capacity(http.len());
    for (prefix, _) in http {
        validate_extension_prefix(prefix, &taken)?;
        taken.push(prefix.clone());
    }
    Ok(())
}

/// Nest each router under its prefix, in order, after checking it.
pub(crate) fn mount_extensions(
    mut app: Router,
    http: Vec<(String, Router)>,
) -> Result<Router, ExtensionMountError> {
    let mut taken: Vec<String> = Vec::with_capacity(http.len());
    for (prefix, router) in http {
        validate_extension_prefix(&prefix, &taken)?;
        app = app.nest(&prefix, router);
        taken.push(prefix);
    }
    Ok(app)
}

#[cfg(test)]
mod tests;

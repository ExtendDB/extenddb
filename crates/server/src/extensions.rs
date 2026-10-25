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
//! the request path hashes no body and builds no record. Its [`ConsoleTab`]s
//! are pages nested under `/console/<slug>` behind the console's session
//! check and listed in the console's navigation bar.

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
    /// Pages added to the console: each is a link in the console's navigation
    /// bar and a router nested at `/console/<slug>` behind the console's
    /// session check, so an extension page is reachable only by a logged-in
    /// console user and renders inside the console's layout.
    pub console_tabs: Vec<ConsoleTab>,
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

    /// Add a console tab: `label` is the link text, `slug` one path segment
    /// of `[a-z0-9-]`, `router` the page at `/console/<slug>`.
    #[must_use]
    pub fn with_console_tab(
        mut self,
        label: impl Into<String>,
        slug: impl Into<String>,
        router: Router,
    ) -> Self {
        self.console_tabs.push(ConsoleTab {
            label: label.into(),
            slug: slug.into(),
            router,
        });
        self
    }
}

/// A page a thin binary adds to the console.
///
/// The router is nested at `/console/<slug>` inside the console and wrapped in
/// the console's session check: a request without a live session is redirected
/// to the login page before it reaches the router, and one with a session
/// carries a [`crate::console::ConsoleIdentity`] request extension, so the
/// page can render the console's navigation bar and layout through
/// [`crate::console::html`]. Every path under the slug meets the session
/// check, including paths the router has no route for: the server installs a
/// plain 404 fallback on the tab router inside the check, and a fallback the
/// extension set on its router is replaced by it. `/console/<slug>/` redirects
/// to `/console/<slug>`, behind the same check. `slug` is one path segment of
/// `[a-z0-9-]` that is not a built-in console route and not another tab's;
/// `label` is the link text. Build it through
/// [`Extensions::with_console_tab`]; the struct may gain fields in a minor
/// release.
#[non_exhaustive]
pub struct ConsoleTab {
    /// The link text in the navigation bar.
    pub label: String,
    /// The path segment under `/console/`.
    pub slug: String,
    /// The page.
    pub router: Router,
}

/// Console paths the server owns at the first segment under `/console/`; a
/// tab may not take one.
const CONSOLE_RESERVED_SLUGS: &[&str] = &[
    "login",
    "logout",
    "docs",
    "metrics",
    "metrics-data",
    "settings",
    "cache",
    "accounts",
    "users",
    "groups",
    "roles",
    "policies",
    "access-keys",
];

/// Check one tab slug against the rule and the slugs already taken.
pub(crate) fn validate_console_slug(
    slug: &str,
    already_taken: &[String],
) -> Result<(), ExtensionMountError> {
    let well_formed = !slug.is_empty()
        && slug
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !well_formed {
        return Err(ExtensionMountError::BadSlug(slug.to_owned()));
    }
    if CONSOLE_RESERVED_SLUGS.contains(&slug) {
        return Err(ExtensionMountError::ReservedSlug(slug.to_owned()));
    }
    if already_taken.iter().any(|t| t == slug) {
        return Err(ExtensionMountError::RepeatedSlug(slug.to_owned()));
    }
    Ok(())
}

/// Check every tab slug in `tabs` as the server will at startup, without
/// mounting.
pub fn validate_console_slugs(tabs: &[ConsoleTab]) -> Result<(), ExtensionMountError> {
    let mut taken: Vec<String> = Vec::with_capacity(tabs.len());
    for tab in tabs {
        validate_console_slug(&tab.slug, &taken)?;
        taken.push(tab.slug.clone());
    }
    Ok(())
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
    /// A console tab slug is not one segment of `[a-z0-9-]`.
    BadSlug(String),
    /// A console tab slug is one of the console's own first segments.
    ReservedSlug(String),
    /// A console tab slug repeats an earlier tab's.
    RepeatedSlug(String),
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
            Self::BadSlug(slug) => write!(
                f,
                "console tab slug '{slug}' must be one segment of [a-z0-9-]"
            ),
            Self::ReservedSlug(slug) => {
                write!(f, "console tab slug '{slug}' is a built-in console route")
            }
            Self::RepeatedSlug(slug) => {
                write!(f, "console tab slug '{slug}' is used by an earlier tab")
            }
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

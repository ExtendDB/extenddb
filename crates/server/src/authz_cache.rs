// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `CachedAuthzStore` — a stale-while-revalidate cache layered on top of any
//! [`extenddb_storage::AuthorizationStore`].
//!
//! Caches the inputs to IAM evaluation:
//!
//! - Identity policies (user / role) — as parsed `Arc<PolicyDocument>`s,
//!   eliminating per-request JSON parse cost.
//! - Group policies for a user — same shape.
//! - Permissions boundary — `Option<Arc<PolicyDocument>>`.
//! - Principal tags — pre-flattened `HashMap<String, String>`.
//! - Resource tags — same.
//! - Role-session data (policy + session tags) — pre-parsed.
//!
//! Self-induced changes from the management API are propagated via the
//! [`AuthCacheRegistry`] (write-through invalidation). Off-instance changes
//! propagate within the configured TTL.
//!
//! See `docs/design/12-auth-authz-cache.md` for the full design.

#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::sync::Arc;

use extenddb_auth::policy::document::PolicyDocument;
use extenddb_cache::{Loader, SwrCache, SwrCacheConfig};
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::{OpError, OpResult};
use futures::FutureExt;
use futures::future::BoxFuture;

/// Pre-parsed session data: `session_policy` is parsed into a
/// `PolicyDocument` (or absent), and `session_tags` is already a
/// `HashMap` keyed by tag name.
#[derive(Clone)]
pub struct CachedSessionData {
    pub session_policy: Option<Arc<PolicyDocument>>,
    pub session_tags: HashMap<String, String>,
}

/// Configuration for the authorization cache. Each sub-cache is sized
/// independently. Defaults match the recommendations in the design doc.
#[derive(Debug, Clone)]
pub struct AuthzCacheConfig {
    pub identity_policies: SwrCacheConfig,
    pub group_policies: SwrCacheConfig,
    pub boundary: SwrCacheConfig,
    pub principal_tags: SwrCacheConfig,
    pub resource_tags: SwrCacheConfig,
    pub session_data: SwrCacheConfig,
}

impl Default for AuthzCacheConfig {
    fn default() -> Self {
        let base = SwrCacheConfig::default();
        Self {
            identity_policies: SwrCacheConfig {
                name: "identity_policies",
                ..base.clone()
            },
            group_policies: SwrCacheConfig {
                name: "group_policies",
                ..base.clone()
            },
            boundary: SwrCacheConfig {
                name: "boundary",
                ..base.clone()
            },
            principal_tags: SwrCacheConfig {
                name: "principal_tags",
                ..base.clone()
            },
            resource_tags: SwrCacheConfig {
                name: "resource_tags",
                ..base.clone()
            },
            session_data: SwrCacheConfig {
                name: "session_data",
                ..base
            },
        }
    }
}

/// Pre-parsed list of policies, wrapped in `Arc` so cache hits are O(1) —
/// one atomic increment instead of an N-element Vec clone.
pub type PolicyList = Arc<Vec<Arc<PolicyDocument>>>;

/// Pre-parsed tag map, wrapped in `Arc` for the same reason.
pub type TagMap = Arc<HashMap<String, String>>;

/// Cached authorization store. Wraps any [`AuthorizationStore`] and exposes
/// `fetch_*` methods that return pre-parsed values from an in-memory SWR
/// cache.
///
/// Cloning is cheap; clones share the underlying caches.
///
/// Cache value types are wrapped in `Arc` so a hit is one atomic increment
/// rather than an O(N) deep clone of a `Vec` / `HashMap` per request.
#[derive(Clone)]
pub struct CachedAuthzStore {
    user_policies: SwrCache<(String, String), PolicyList, OpError>,
    user_group_policies: SwrCache<(String, String), PolicyList, OpError>,
    user_boundary: SwrCache<(String, String), Arc<PolicyDocument>, OpError>,
    user_tags: SwrCache<(String, String), TagMap, OpError>,
    role_policies: SwrCache<(String, String), PolicyList, OpError>,
    role_boundary: SwrCache<(String, String), Arc<PolicyDocument>, OpError>,
    role_tags: SwrCache<(String, String), TagMap, OpError>,
    session_data: SwrCache<(String, String, String), Arc<CachedSessionData>, OpError>,
    resource_tags: SwrCache<String, TagMap, OpError>,
}

impl CachedAuthzStore {
    /// Wrap `inner` with the configured caches. The store is consumed into
    /// `Arc<dyn AuthorizationStore>` so the loader closures share it.
    #[must_use]
    pub fn new(inner: Arc<dyn AuthorizationStore>, cfg: AuthzCacheConfig) -> Self {
        Self::build(inner, cfg, false)
    }

    /// Construct a pass-through `CachedAuthzStore` that bypasses every
    /// sub-cache, calling the underlying store directly. Used when
    /// `auth.cache.enabled = false` to avoid the cache wrapping overhead
    /// while keeping the call graph unchanged.
    #[must_use]
    pub fn pass_through(inner: Arc<dyn AuthorizationStore>, cfg: AuthzCacheConfig) -> Self {
        Self::build(inner, cfg, true)
    }

    fn build(
        inner: Arc<dyn AuthorizationStore>,
        cfg: AuthzCacheConfig,
        pass_through: bool,
    ) -> Self {
        // Closures can't be generic over the key/value/error types used by
        // each subcache, so we inline the branch with a small helper fn.
        fn mk_cache<K, V, E>(
            loader: extenddb_cache::Loader<K, V, E>,
            cache_cfg: extenddb_cache::SwrCacheConfig,
            pass_through: bool,
        ) -> extenddb_cache::SwrCache<K, V, E>
        where
            K: std::hash::Hash + Eq + Send + Sync + Clone + std::fmt::Debug + 'static,
            V: Clone + Send + Sync + 'static,
            E: Clone + Send + Sync + std::fmt::Debug + 'static,
        {
            if pass_through {
                extenddb_cache::SwrCache::pass_through(loader, cache_cfg)
            } else {
                extenddb_cache::SwrCache::new(loader, cache_cfg)
            }
        }
        Self {
            user_policies: mk_cache(
                make_user_policies_loader(inner.clone()),
                cfg.identity_policies.clone(),
                pass_through,
            ),
            user_group_policies: mk_cache(
                make_user_group_policies_loader(inner.clone()),
                cfg.group_policies.clone(),
                pass_through,
            ),
            user_boundary: mk_cache(
                make_user_boundary_loader(inner.clone()),
                cfg.boundary.clone(),
                pass_through,
            ),
            user_tags: mk_cache(
                make_user_tags_loader(inner.clone()),
                cfg.principal_tags.clone(),
                pass_through,
            ),
            role_policies: mk_cache(
                make_role_policies_loader(inner.clone()),
                cfg.identity_policies.clone(),
                pass_through,
            ),
            role_boundary: mk_cache(
                make_role_boundary_loader(inner.clone()),
                cfg.boundary,
                pass_through,
            ),
            role_tags: mk_cache(
                make_role_tags_loader(inner.clone()),
                cfg.principal_tags,
                pass_through,
            ),
            session_data: mk_cache(
                make_session_data_loader(inner.clone()),
                cfg.session_data,
                pass_through,
            ),
            resource_tags: mk_cache(
                make_resource_tags_loader(inner),
                cfg.resource_tags,
                pass_through,
            ),
        }
    }

    // ── Read methods used by `authorization.rs` ────────────────────────

    /// Fetch the user's inline + attached identity policies, parsed.
    ///
    /// Returns an `Arc<Vec<...>>` so cache hits are O(1). Callers that need
    /// `&[Arc<PolicyDocument>]` can `&*` through the outer Arc.
    pub async fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<PolicyList> {
        Ok(self
            .user_policies
            .get((account_id.to_owned(), user_name.to_owned()))
            .await?
            .unwrap_or_else(|| Arc::new(Vec::new())))
    }

    /// Fetch all policies attached to the user via group membership.
    pub async fn fetch_user_group_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<PolicyList> {
        Ok(self
            .user_group_policies
            .get((account_id.to_owned(), user_name.to_owned()))
            .await?
            .unwrap_or_else(|| Arc::new(Vec::new())))
    }

    /// Fetch the user's permissions boundary, parsed. Returns `None` if no
    /// boundary is set.
    pub async fn fetch_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Option<Arc<PolicyDocument>>> {
        self.user_boundary
            .get((account_id.to_owned(), user_name.to_owned()))
            .await
    }

    /// Fetch the user's principal tags as a flat map.
    pub async fn fetch_user_tags(&self, account_id: &str, user_name: &str) -> OpResult<TagMap> {
        Ok(self
            .user_tags
            .get((account_id.to_owned(), user_name.to_owned()))
            .await?
            .unwrap_or_else(|| Arc::new(HashMap::new())))
    }

    /// Fetch the role's inline + attached identity policies, parsed.
    pub async fn fetch_role_policies(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<PolicyList> {
        Ok(self
            .role_policies
            .get((account_id.to_owned(), role_name.to_owned()))
            .await?
            .unwrap_or_else(|| Arc::new(Vec::new())))
    }

    /// Fetch the role's permissions boundary, parsed.
    pub async fn fetch_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Option<Arc<PolicyDocument>>> {
        self.role_boundary
            .get((account_id.to_owned(), role_name.to_owned()))
            .await
    }

    /// Fetch the role's principal tags.
    pub async fn fetch_role_tags(&self, account_id: &str, role_name: &str) -> OpResult<TagMap> {
        Ok(self
            .role_tags
            .get((account_id.to_owned(), role_name.to_owned()))
            .await?
            .unwrap_or_else(|| Arc::new(HashMap::new())))
    }

    /// Fetch role-session data (parsed session policy + tags).
    pub async fn fetch_session_data(
        &self,
        account_id: &str,
        role_name: &str,
        session_name: &str,
    ) -> OpResult<Option<Arc<CachedSessionData>>> {
        self.session_data
            .get((
                account_id.to_owned(),
                role_name.to_owned(),
                session_name.to_owned(),
            ))
            .await
    }

    /// Fetch resource tags as a flat map. Empty for wildcard resources.
    pub async fn fetch_resource_tags(&self, arn: &str) -> OpResult<TagMap> {
        if arn.ends_with("/*") {
            return Ok(Arc::new(HashMap::new()));
        }
        Ok(self
            .resource_tags
            .get(arn.to_owned())
            .await?
            .unwrap_or_else(|| Arc::new(HashMap::new())))
    }

    // ── Invalidation API ───────────────────────────────────────────────

    pub async fn invalidate_user_policies(&self, account_id: &str, user_name: &str) {
        self.user_policies
            .invalidate(&(account_id.to_owned(), user_name.to_owned()))
            .await;
    }

    pub async fn invalidate_user_group_policies(&self, account_id: &str, user_name: &str) {
        self.user_group_policies
            .invalidate(&(account_id.to_owned(), user_name.to_owned()))
            .await;
    }

    /// Fan out a group-membership-affecting event (group deleted, group
    /// policy attached/detached) to the cached `user_group_policies` entry
    /// for each member.
    ///
    /// The `user_group_policies` cache is keyed by `(account_id, user_name)`,
    /// not by group, because the loader flattens group membership at fetch
    /// time. Callers pass the member list they already had to enumerate
    /// (e.g. via `get_group_detail`) so we don't re-query.
    ///
    /// Per-user attributes that aren't touched by group events
    /// (`user_policies`, `user_boundary`, `user_tags`) are intentionally
    /// out of scope — they have their own invalidation methods called from
    /// the corresponding per-user mutation sites.
    pub async fn invalidate_users_group_policies(&self, account_id: &str, user_names: &[String]) {
        for u in user_names {
            self.invalidate_user_group_policies(account_id, u).await;
        }
    }

    pub async fn invalidate_user_boundary(&self, account_id: &str, user_name: &str) {
        self.user_boundary
            .invalidate(&(account_id.to_owned(), user_name.to_owned()))
            .await;
    }

    pub async fn invalidate_user_tags(&self, account_id: &str, user_name: &str) {
        self.user_tags
            .invalidate(&(account_id.to_owned(), user_name.to_owned()))
            .await;
    }

    pub async fn invalidate_role_policies(&self, account_id: &str, role_name: &str) {
        self.role_policies
            .invalidate(&(account_id.to_owned(), role_name.to_owned()))
            .await;
    }

    pub async fn invalidate_role_boundary(&self, account_id: &str, role_name: &str) {
        self.role_boundary
            .invalidate(&(account_id.to_owned(), role_name.to_owned()))
            .await;
    }

    pub async fn invalidate_role_tags(&self, account_id: &str, role_name: &str) {
        self.role_tags
            .invalidate(&(account_id.to_owned(), role_name.to_owned()))
            .await;
    }

    pub async fn invalidate_session(&self, account_id: &str, role_name: &str, session_name: &str) {
        self.session_data
            .invalidate(&(
                account_id.to_owned(),
                role_name.to_owned(),
                session_name.to_owned(),
            ))
            .await;
    }

    pub async fn invalidate_resource_tags(&self, arn: &str) {
        self.resource_tags.invalidate(&arn.to_owned()).await;
    }

    /// Drop every cached session-data entry for `(account_id, role_name)`.
    /// Used when a role is deleted; the cache's key is
    /// `(account, role, session)`, so we predicate on the first two.
    pub fn invalidate_role_sessions(&self, account_id: &str, role_name: &str) {
        let acct = account_id.to_owned();
        let role = role_name.to_owned();
        // Best-effort: log on failure (only happens at shutdown).
        if let Err(e) = self
            .session_data
            .invalidate_if(move |k| k.0 == acct && k.1 == role)
        {
            tracing::warn!(
                ?e,
                "session_data invalidate_role_sessions predicate registration failed"
            );
        }
    }

    /// Invalidate every cached entry across every subcache that belongs to
    /// `account_id`. Used by `delete_account`.
    ///
    /// Each subcache is iterated independently since their key shapes
    /// differ. The resource-tags cache is keyed by ARN; we match the
    /// account-id segment structurally (5th colon-delimited field) rather
    /// than via substring search to avoid false positives if an ARN has
    /// the same digits embedded elsewhere.
    pub fn invalidate_account(&self, account_id: &str) {
        let acct = account_id.to_owned();
        let user_acct = acct.clone();
        let pred_user = move |k: &(String, String)| k.0 == user_acct;
        let user_acct = acct.clone();
        let pred_session = move |k: &(String, String, String)| k.0 == user_acct;
        let pred_arn = move |k: &String| {
            // ARN format: arn:aws:dynamodb:{region}:{account_id}:table/...
            // We only need the 5th segment, so plain `split` works (we
            // never look past the 6th segment, which may contain colons
            // for stream labels).
            k.split(':').nth(4) == Some(acct.as_str())
        };

        let log_err = |name: &str, e: extenddb_cache::PredicateError| {
            tracing::warn!(
                cache = name,
                ?e,
                "invalidate_account predicate registration failed"
            );
        };

        if let Err(e) = self.user_policies.invalidate_if(pred_user.clone()) {
            log_err("user_policies", e);
        }
        if let Err(e) = self.user_group_policies.invalidate_if(pred_user.clone()) {
            log_err("user_group_policies", e);
        }
        if let Err(e) = self.user_boundary.invalidate_if(pred_user.clone()) {
            log_err("user_boundary", e);
        }
        if let Err(e) = self.user_tags.invalidate_if(pred_user.clone()) {
            log_err("user_tags", e);
        }
        if let Err(e) = self.role_policies.invalidate_if(pred_user.clone()) {
            log_err("role_policies", e);
        }
        if let Err(e) = self.role_boundary.invalidate_if(pred_user.clone()) {
            log_err("role_boundary", e);
        }
        if let Err(e) = self.role_tags.invalidate_if(pred_user) {
            log_err("role_tags", e);
        }
        if let Err(e) = self.session_data.invalidate_if(pred_session) {
            log_err("session_data", e);
        }
        if let Err(e) = self.resource_tags.invalidate_if(pred_arn) {
            log_err("resource_tags", e);
        }
    }

    /// Drop every cached entry across every sub-cache. Used by the manual
    /// `cache invalidate all` admin endpoint; not called from any normal
    /// write-through path. The credential cache lives in `extenddb-auth`
    /// and is swept separately by the caller.
    pub fn invalidate_all(&self) {
        self.user_policies.invalidate_all();
        self.user_group_policies.invalidate_all();
        self.user_boundary.invalidate_all();
        self.user_tags.invalidate_all();
        self.role_policies.invalidate_all();
        self.role_boundary.invalidate_all();
        self.role_tags.invalidate_all();
        self.session_data.invalidate_all();
        self.resource_tags.invalidate_all();
    }

    /// Snapshot per-sub-cache counters for export to `/auth-cache-metrics`.
    /// Returns `None` if metrics are unavailable for any reason (currently
    /// always returns `Some`).
    pub fn metrics_snapshot(&self) -> Option<AuthzCacheMetricsSnapshot> {
        Some(AuthzCacheMetricsSnapshot {
            user_policies: self.user_policies.metrics().snapshot(),
            user_group_policies: self.user_group_policies.metrics().snapshot(),
            user_boundary: self.user_boundary.metrics().snapshot(),
            user_tags: self.user_tags.metrics().snapshot(),
            role_policies: self.role_policies.metrics().snapshot(),
            role_boundary: self.role_boundary.metrics().snapshot(),
            role_tags: self.role_tags.metrics().snapshot(),
            session_data: self.session_data.metrics().snapshot(),
            resource_tags: self.resource_tags.metrics().snapshot(),
        })
    }

    /// Returns `true` when every sub-cache is in pass-through mode (i.e. the
    /// authz cache was constructed via `pass_through`). All sub-caches are
    /// configured with the same `pass_through` flag at construction time, so
    /// a single sub-cache's state is representative of the whole.
    #[must_use]
    pub fn is_pass_through(&self) -> bool {
        self.user_policies.is_pass_through()
    }
}

/// Per-sub-cache metric snapshot for the `/auth-cache-metrics` endpoint.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AuthzCacheMetricsSnapshot {
    pub user_policies: extenddb_cache::SwrMetricsSnapshot,
    pub user_group_policies: extenddb_cache::SwrMetricsSnapshot,
    pub user_boundary: extenddb_cache::SwrMetricsSnapshot,
    pub user_tags: extenddb_cache::SwrMetricsSnapshot,
    pub role_policies: extenddb_cache::SwrMetricsSnapshot,
    pub role_boundary: extenddb_cache::SwrMetricsSnapshot,
    pub role_tags: extenddb_cache::SwrMetricsSnapshot,
    pub session_data: extenddb_cache::SwrMetricsSnapshot,
    pub resource_tags: extenddb_cache::SwrMetricsSnapshot,
}

impl extenddb_auth::AuthzCacheInvalidator for CachedAuthzStore {
    fn invalidate_user_policies<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_user_policies(account_id, user_name))
    }

    fn invalidate_user_group_policies<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_user_group_policies(account_id, user_name))
    }

    fn invalidate_users_group_policies<'a>(
        &'a self,
        account_id: &'a str,
        user_names: &'a [String],
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_users_group_policies(account_id, user_names))
    }

    fn invalidate_user_boundary<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_user_boundary(account_id, user_name))
    }

    fn invalidate_user_tags<'a>(
        &'a self,
        account_id: &'a str,
        user_name: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_user_tags(account_id, user_name))
    }

    fn invalidate_role_policies<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_role_policies(account_id, role_name))
    }

    fn invalidate_role_boundary<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_role_boundary(account_id, role_name))
    }

    fn invalidate_role_tags<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_role_tags(account_id, role_name))
    }

    fn invalidate_session<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
        session_name: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_session(account_id, role_name, session_name))
    }

    fn invalidate_resource_tags<'a>(&'a self, arn: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(self.invalidate_resource_tags(arn))
    }

    fn invalidate_role_sessions<'a>(
        &'a self,
        account_id: &'a str,
        role_name: &'a str,
    ) -> BoxFuture<'a, ()> {
        // The synchronous fanout returns immediately; wrap in a ready future
        // to satisfy the trait signature.
        self.invalidate_role_sessions(account_id, role_name);
        Box::pin(std::future::ready(()))
    }

    fn invalidate_account<'a>(&'a self, account_id: &'a str) -> BoxFuture<'a, ()> {
        self.invalidate_account(account_id);
        Box::pin(std::future::ready(()))
    }

    fn invalidate_all(&self) {
        self.invalidate_all();
    }
}

// ── Loader factories ───────────────────────────────────────────────────
//
// Each `make_*_loader` returns a `Loader` that issues the corresponding
// `AuthorizationStore` call and, where applicable, parses JSON into
// `Arc<PolicyDocument>`. Parse failures bubble up as `OpError::Internal`,
// which the caller in `authorization.rs` translates to a fail-closed
// `AccessDeniedException`.

type PolDocsLoader = Loader<(String, String), PolicyList, OpError>;
type BoundaryLoader = Loader<(String, String), Arc<PolicyDocument>, OpError>;
type TagsLoader = Loader<(String, String), TagMap, OpError>;
type SessionLoader = Loader<(String, String, String), Arc<CachedSessionData>, OpError>;
type ResourceTagsLoader = Loader<String, TagMap, OpError>;

// Policy loaders surface `Ok(None)` when the underlying store returns an
// empty Vec — same rationale as the tag loaders below. "Principal exists
// with no inline policies" is functionally identical to "principal does
// not exist" through this API, AND both should expire on the short
// `negative_ttl`, not the long `ttl`. Otherwise probes against
// nonexistent principals fill the LRU with empty positive entries.
fn make_user_policies_loader(inner: Arc<dyn AuthorizationStore>) -> PolDocsLoader {
    Arc::new(move |(account_id, user_name): (String, String)|
        -> BoxFuture<'static, OpResult<Option<PolicyList>>> {
        let inner = inner.clone();
        async move {
            let raw = inner.fetch_user_policies(&account_id, &user_name).await?;
            policy_list_if_nonempty(&raw)
        }
        .boxed()
    })
}

fn make_user_group_policies_loader(inner: Arc<dyn AuthorizationStore>) -> PolDocsLoader {
    Arc::new(move |(account_id, user_name): (String, String)|
        -> BoxFuture<'static, OpResult<Option<PolicyList>>> {
        let inner = inner.clone();
        async move {
            let raw = inner.fetch_user_group_policies(&account_id, &user_name).await?;
            policy_list_if_nonempty(&raw)
        }
        .boxed()
    })
}

fn make_role_policies_loader(inner: Arc<dyn AuthorizationStore>) -> PolDocsLoader {
    Arc::new(move |(account_id, role_name): (String, String)|
        -> BoxFuture<'static, OpResult<Option<PolicyList>>> {
        let inner = inner.clone();
        async move {
            let raw = inner.fetch_role_policies(&account_id, &role_name).await?;
            policy_list_if_nonempty(&raw)
        }
        .boxed()
    })
}

/// Parse `raw` JSON policy strings; return `Ok(None)` if `raw` is empty
/// (so the SWR cache stores it as a negative entry with the short
/// `negative_ttl`); otherwise wrap the parsed documents in `Some`.
/// Parse errors propagate.
fn policy_list_if_nonempty(raw: &[String]) -> OpResult<Option<PolicyList>> {
    if raw.is_empty() {
        return Ok(None);
    }
    parse_documents(raw).map(|v| Some(Arc::new(v)))
}

fn make_user_boundary_loader(inner: Arc<dyn AuthorizationStore>) -> BoundaryLoader {
    Arc::new(move |(account_id, user_name): (String, String)|
        -> BoxFuture<'static, OpResult<Option<Arc<PolicyDocument>>>> {
        let inner = inner.clone();
        async move {
            match inner.fetch_user_boundary(&account_id, &user_name).await? {
                Some(json_str) => Ok(Some(parse_document(&json_str)?)),
                None => Ok(None),
            }
        }
        .boxed()
    })
}

fn make_role_boundary_loader(inner: Arc<dyn AuthorizationStore>) -> BoundaryLoader {
    Arc::new(move |(account_id, role_name): (String, String)|
        -> BoxFuture<'static, OpResult<Option<Arc<PolicyDocument>>>> {
        let inner = inner.clone();
        async move {
            match inner.fetch_role_boundary(&account_id, &role_name).await? {
                Some(json_str) => Ok(Some(parse_document(&json_str)?)),
                None => Ok(None),
            }
        }
        .boxed()
    })
}

// Tag loaders surface `Ok(None)` when the underlying store returns an
// empty Vec. The storage trait can't distinguish "principal exists with no
// tags" from "principal does not exist" — both yield an empty Vec — but
// for the cache that distinction is moot: both must return an empty
// `TagMap` to the caller, AND both should expire on the short
// `negative_ttl`, not the long `ttl`. Otherwise an attacker probing
// random principals fills the LRU with empty entries that linger for
// `ttl_seconds`, evicting useful entries. With negative-caching, those
// probes are bounded to `negative_ttl_seconds`.
fn make_user_tags_loader(inner: Arc<dyn AuthorizationStore>) -> TagsLoader {
    Arc::new(move |(account_id, user_name): (String, String)|
        -> BoxFuture<'static, OpResult<Option<TagMap>>> {
        let inner = inner.clone();
        async move {
            let pairs = inner.fetch_user_tags(&account_id, &user_name).await?;
            Ok(map_if_nonempty(pairs))
        }
        .boxed()
    })
}

fn make_role_tags_loader(inner: Arc<dyn AuthorizationStore>) -> TagsLoader {
    Arc::new(move |(account_id, role_name): (String, String)|
        -> BoxFuture<'static, OpResult<Option<TagMap>>> {
        let inner = inner.clone();
        async move {
            let pairs = inner.fetch_role_tags(&account_id, &role_name).await?;
            Ok(map_if_nonempty(pairs))
        }
        .boxed()
    })
}

/// Wrap a tag pair list as a positive cache entry only if non-empty;
/// otherwise return `None` so the SWR cache stores it as a negative entry
/// with the short `negative_ttl`. See the loader comment above for the
/// rationale.
fn map_if_nonempty(pairs: Vec<(String, String)>) -> Option<TagMap> {
    if pairs.is_empty() {
        None
    } else {
        Some(Arc::new(pairs.into_iter().collect()))
    }
}

fn make_resource_tags_loader(inner: Arc<dyn AuthorizationStore>) -> ResourceTagsLoader {
    Arc::new(
        move |arn: String| -> BoxFuture<'static, OpResult<Option<TagMap>>> {
            let inner = inner.clone();
            async move {
                let pairs = inner.fetch_resource_tags(&arn).await?;
                Ok(map_if_nonempty(pairs))
            }
            .boxed()
        },
    )
}

fn make_session_data_loader(inner: Arc<dyn AuthorizationStore>) -> SessionLoader {
    Arc::new(move |(account_id, role_name, session_name): (String, String, String)|
        -> BoxFuture<'static, OpResult<Option<Arc<CachedSessionData>>>> {
        let inner = inner.clone();
        async move {
            let raw: Option<SessionData> = inner
                .fetch_session_data(&account_id, &role_name, &session_name)
                .await?;
            let Some(data) = raw else { return Ok(None) };

            let session_policy = match data.session_policy {
                Some(json_str) => Some(parse_document(&json_str)?),
                None => None,
            };
            let session_tags: HashMap<String, String> = data.session_tags.into_iter().collect();
            Ok(Some(Arc::new(CachedSessionData {
                session_policy,
                session_tags,
            })))
        }
        .boxed()
    })
}

fn parse_documents(jsons: &[String]) -> OpResult<Vec<Arc<PolicyDocument>>> {
    let mut docs = Vec::with_capacity(jsons.len());
    for s in jsons {
        docs.push(parse_document(s)?);
    }
    Ok(docs)
}

fn parse_document(s: &str) -> OpResult<Arc<PolicyDocument>> {
    PolicyDocument::from_json(s)
        .map(Arc::new)
        .map_err(|e| OpError::Internal(format!("policy parse failed: {e}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
    use extenddb_storage::management_store::OpResult;
    use futures::future::BoxFuture;

    use super::*;

    type PrincipalKey = (String, String);
    type SessionKey = (String, String, String);
    type TagPairs = Vec<(String, String)>;

    /// Counts calls per method so tests can assert the cache prevents repeats.
    #[derive(Default)]
    struct CountingAuthzStore {
        user_policies_calls: AtomicUsize,
        user_group_policies_calls: AtomicUsize,
        user_boundary_calls: AtomicUsize,
        user_tags_calls: AtomicUsize,
        role_policies_calls: AtomicUsize,
        role_boundary_calls: AtomicUsize,
        role_tags_calls: AtomicUsize,
        session_data_calls: AtomicUsize,
        resource_tags_calls: AtomicUsize,

        user_policies: Mutex<HashMap<PrincipalKey, Vec<String>>>,
        user_group_policies: Mutex<HashMap<PrincipalKey, Vec<String>>>,
        user_boundary: Mutex<HashMap<PrincipalKey, Option<String>>>,
        user_tags: Mutex<HashMap<PrincipalKey, TagPairs>>,
        role_policies: Mutex<HashMap<PrincipalKey, Vec<String>>>,
        role_boundary: Mutex<HashMap<PrincipalKey, Option<String>>>,
        role_tags: Mutex<HashMap<PrincipalKey, TagPairs>>,
        session_data: Mutex<HashMap<SessionKey, Option<SessionData>>>,
        resource_tags: Mutex<HashMap<String, TagPairs>>,
    }

    impl AuthorizationStore for CountingAuthzStore {
        fn fetch_user_policies(
            &self,
            account_id: &str,
            user_name: &str,
        ) -> BoxFuture<'_, OpResult<Vec<String>>> {
            self.user_policies_calls.fetch_add(1, Ordering::SeqCst);
            let key = (account_id.to_owned(), user_name.to_owned());
            let v = self
                .user_policies
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(v) })
        }

        fn fetch_user_group_policies(
            &self,
            account_id: &str,
            user_name: &str,
        ) -> BoxFuture<'_, OpResult<Vec<String>>> {
            self.user_group_policies_calls
                .fetch_add(1, Ordering::SeqCst);
            let key = (account_id.to_owned(), user_name.to_owned());
            let v = self
                .user_group_policies
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(v) })
        }

        fn fetch_user_boundary(
            &self,
            account_id: &str,
            user_name: &str,
        ) -> BoxFuture<'_, OpResult<Option<String>>> {
            self.user_boundary_calls.fetch_add(1, Ordering::SeqCst);
            let key = (account_id.to_owned(), user_name.to_owned());
            let v = self
                .user_boundary
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or(None);
            Box::pin(async move { Ok(v) })
        }

        fn fetch_user_tags(
            &self,
            account_id: &str,
            user_name: &str,
        ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
            self.user_tags_calls.fetch_add(1, Ordering::SeqCst);
            let key = (account_id.to_owned(), user_name.to_owned());
            let v = self
                .user_tags
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(v) })
        }

        fn fetch_role_policies(
            &self,
            account_id: &str,
            role_name: &str,
        ) -> BoxFuture<'_, OpResult<Vec<String>>> {
            self.role_policies_calls.fetch_add(1, Ordering::SeqCst);
            let key = (account_id.to_owned(), role_name.to_owned());
            let v = self
                .role_policies
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(v) })
        }

        fn fetch_role_boundary(
            &self,
            account_id: &str,
            role_name: &str,
        ) -> BoxFuture<'_, OpResult<Option<String>>> {
            self.role_boundary_calls.fetch_add(1, Ordering::SeqCst);
            let key = (account_id.to_owned(), role_name.to_owned());
            let v = self
                .role_boundary
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or(None);
            Box::pin(async move { Ok(v) })
        }

        fn fetch_role_tags(
            &self,
            account_id: &str,
            role_name: &str,
        ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
            self.role_tags_calls.fetch_add(1, Ordering::SeqCst);
            let key = (account_id.to_owned(), role_name.to_owned());
            let v = self
                .role_tags
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(v) })
        }

        fn fetch_session_data(
            &self,
            account_id: &str,
            role_name: &str,
            session_name: &str,
        ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
            self.session_data_calls.fetch_add(1, Ordering::SeqCst);
            let key = (
                account_id.to_owned(),
                role_name.to_owned(),
                session_name.to_owned(),
            );
            let v = self
                .session_data
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or(None);
            Box::pin(async move { Ok(v) })
        }

        fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
            self.resource_tags_calls.fetch_add(1, Ordering::SeqCst);
            let key = arn.to_owned();
            let v = self
                .resource_tags
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_default();
            Box::pin(async move { Ok(v) })
        }
    }

    fn fast_authz_cfg() -> AuthzCacheConfig {
        let base = SwrCacheConfig {
            ttl: Duration::from_millis(200),
            soft_ttl: Duration::from_millis(50),
            negative_ttl: Duration::from_millis(50),
            max_entries: 100,
            name: "test",
        };
        AuthzCacheConfig {
            identity_policies: SwrCacheConfig {
                name: "ip",
                ..base.clone()
            },
            group_policies: SwrCacheConfig {
                name: "gp",
                ..base.clone()
            },
            boundary: SwrCacheConfig {
                name: "b",
                ..base.clone()
            },
            principal_tags: SwrCacheConfig {
                name: "pt",
                ..base.clone()
            },
            resource_tags: SwrCacheConfig {
                name: "rt",
                ..base.clone()
            },
            session_data: SwrCacheConfig { name: "sd", ..base },
        }
    }

    fn allow_policy(action: &str) -> String {
        format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"{action}","Resource":"*"}}]}}"#
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn user_policies_cache_round_trip() {
        let inner = Arc::new(CountingAuthzStore::default());
        inner.user_policies.lock().unwrap().insert(
            ("a".to_owned(), "u".to_owned()),
            vec![allow_policy("dynamodb:GetItem")],
        );
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());

        let p1 = cache.fetch_user_policies("a", "u").await.unwrap();
        let p2 = cache.fetch_user_policies("a", "u").await.unwrap();

        assert_eq!(p1.len(), 1);
        assert_eq!(p2.len(), 1);
        assert_eq!(
            inner.user_policies_calls.load(Ordering::SeqCst),
            1,
            "second call must hit cache"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalidate_user_policies_forces_refetch() {
        let inner = Arc::new(CountingAuthzStore::default());
        inner.user_policies.lock().unwrap().insert(
            ("a".to_owned(), "u".to_owned()),
            vec![allow_policy("dynamodb:GetItem")],
        );
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());

        cache.fetch_user_policies("a", "u").await.unwrap();
        cache.invalidate_user_policies("a", "u").await;
        cache.fetch_user_policies("a", "u").await.unwrap();

        assert_eq!(
            inner.user_policies_calls.load(Ordering::SeqCst),
            2,
            "invalidation forces re-fetch"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unparseable_policy_yields_error() {
        let inner = Arc::new(CountingAuthzStore::default());
        inner.user_policies.lock().unwrap().insert(
            ("a".to_owned(), "u".to_owned()),
            vec!["not-json".to_owned()],
        );
        let cache = CachedAuthzStore::new(inner, fast_authz_cfg());

        let result = cache.fetch_user_policies("a", "u").await;
        assert!(matches!(result, Err(OpError::Internal(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn missing_boundary_returns_none() {
        let inner = Arc::new(CountingAuthzStore::default());
        let cache = CachedAuthzStore::new(inner, fast_authz_cfg());

        let r = cache.fetch_user_boundary("a", "u").await.unwrap();
        assert!(r.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn resource_tags_wildcard_short_circuits() {
        let inner = Arc::new(CountingAuthzStore::default());
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());

        let tags = cache
            .fetch_resource_tags("arn:aws:dynamodb:us-east-1:1:table/*")
            .await
            .unwrap();
        assert!(tags.is_empty());
        assert_eq!(
            inner.resource_tags_calls.load(Ordering::SeqCst),
            0,
            "wildcard resource ARN must short-circuit before any DB call"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn user_tags_round_trip() {
        let inner = Arc::new(CountingAuthzStore::default());
        inner.user_tags.lock().unwrap().insert(
            ("a".to_owned(), "u".to_owned()),
            vec![("Team".to_owned(), "Eng".to_owned())],
        );
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());

        let t1 = cache.fetch_user_tags("a", "u").await.unwrap();
        let t2 = cache.fetch_user_tags("a", "u").await.unwrap();

        assert_eq!(t1.get("Team"), Some(&"Eng".to_owned()));
        assert_eq!(t2.get("Team"), Some(&"Eng".to_owned()));
        assert_eq!(inner.user_tags_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn session_data_round_trip() {
        let inner = Arc::new(CountingAuthzStore::default());
        inner.session_data.lock().unwrap().insert(
            ("a".to_owned(), "r".to_owned(), "s".to_owned()),
            Some(SessionData {
                session_policy: Some(allow_policy("dynamodb:Query")),
                session_tags: vec![("Project".to_owned(), "Atlas".to_owned())],
            }),
        );
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());

        let s1 = cache
            .fetch_session_data("a", "r", "s")
            .await
            .unwrap()
            .unwrap();
        let s2 = cache
            .fetch_session_data("a", "r", "s")
            .await
            .unwrap()
            .unwrap();

        assert!(s1.session_policy.is_some());
        assert!(s2.session_policy.is_some());
        assert_eq!(s1.session_tags.get("Project"), Some(&"Atlas".to_owned()));
        assert_eq!(inner.session_data_calls.load(Ordering::SeqCst), 1);
    }

    // ─────────────────────────────────────────────────────────────────
    // PR3 tests — fanout invalidation
    // ─────────────────────────────────────────────────────────────────

    /// `invalidate_role_sessions` drops every cached session for a given
    /// (account, role), regardless of session_name.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalidate_role_sessions_drops_all_sessions_for_role() {
        let inner = Arc::new(CountingAuthzStore::default());
        // Two sessions for the same role.
        for s in &["s1", "s2"] {
            inner.session_data.lock().unwrap().insert(
                ("a".to_owned(), "r".to_owned(), (*s).to_owned()),
                Some(SessionData {
                    session_policy: None,
                    session_tags: vec![],
                }),
            );
        }
        // One session for a different role — must NOT be invalidated.
        inner.session_data.lock().unwrap().insert(
            ("a".to_owned(), "other-role".to_owned(), "s1".to_owned()),
            Some(SessionData {
                session_policy: None,
                session_tags: vec![],
            }),
        );
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());

        // Prime all three sessions.
        cache.fetch_session_data("a", "r", "s1").await.unwrap();
        cache.fetch_session_data("a", "r", "s2").await.unwrap();
        cache
            .fetch_session_data("a", "other-role", "s1")
            .await
            .unwrap();
        let primed = inner.session_data_calls.load(Ordering::SeqCst);

        cache.invalidate_role_sessions("a", "r");
        // moka's invalidate_entries_if is async; let it settle.
        cache.session_data.run_pending_tasks().await;

        // Now re-fetch — sessions for role "r" hit the inner store again;
        // the session for "other-role" remains cached.
        cache.fetch_session_data("a", "r", "s1").await.unwrap();
        cache.fetch_session_data("a", "r", "s2").await.unwrap();
        cache
            .fetch_session_data("a", "other-role", "s1")
            .await
            .unwrap();

        let after = inner.session_data_calls.load(Ordering::SeqCst);
        assert_eq!(
            after - primed,
            2,
            "sessions for role 'r' must be re-fetched (got {} new calls)",
            after - primed
        );
    }

    /// `invalidate_account` drops every cached entry across every authz
    /// subcache that belongs to an account, leaving entries for other
    /// accounts intact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalidate_account_drops_every_subcache_for_that_account() {
        let inner = Arc::new(CountingAuthzStore::default());
        // Account "a1": user policy + boundary + tags.
        inner.user_policies.lock().unwrap().insert(
            ("a1".to_owned(), "u".to_owned()),
            vec![allow_policy("dynamodb:GetItem")],
        );
        inner.user_tags.lock().unwrap().insert(
            ("a1".to_owned(), "u".to_owned()),
            vec![("Team".to_owned(), "Eng".to_owned())],
        );
        // Account "a2": should be untouched.
        inner.user_policies.lock().unwrap().insert(
            ("a2".to_owned(), "u".to_owned()),
            vec![allow_policy("dynamodb:Query")],
        );
        let cache = CachedAuthzStore::new(inner.clone(), fast_authz_cfg());

        // Prime caches.
        cache.fetch_user_policies("a1", "u").await.unwrap();
        cache.fetch_user_tags("a1", "u").await.unwrap();
        cache.fetch_user_policies("a2", "u").await.unwrap();

        let policies_calls = inner.user_policies_calls.load(Ordering::SeqCst);
        let tags_calls = inner.user_tags_calls.load(Ordering::SeqCst);

        cache.invalidate_account("a1");
        cache.user_policies.run_pending_tasks().await;
        cache.user_tags.run_pending_tasks().await;

        // Re-fetch a1 → cache miss; a2 → still cached.
        cache.fetch_user_policies("a1", "u").await.unwrap();
        cache.fetch_user_tags("a1", "u").await.unwrap();
        cache.fetch_user_policies("a2", "u").await.unwrap();

        assert_eq!(
            inner.user_policies_calls.load(Ordering::SeqCst),
            policies_calls + 1,
            "a1 user_policies re-fetched, a2 untouched"
        );
        assert_eq!(
            inner.user_tags_calls.load(Ordering::SeqCst),
            tags_calls + 1,
            "a1 user_tags re-fetched"
        );
    }
}

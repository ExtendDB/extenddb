// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Management API for extenddb — admin, account, IAM user, group, role, and policy CRUD.
//!
//! All endpoints live under `/management/*` and are authenticated via HTTP
//! Basic auth. Admin users authenticate with `admin_name:password`. IAM users
//! authenticate with `account_id/user_name:password` for self-service operations.
//! The management API reads/writes the catalog database directly.

mod account;
pub(crate) use account::generate_account_id;
mod admin;
mod assume_role;
/// Shared with the web console so both write surfaces validate a trust
/// policy the same way.
pub(crate) use assume_role::validate_trust_policy;
pub(crate) mod auth;
pub mod cache_invalidate;
mod cache_metrics;
pub(crate) mod crypto;
mod iam_group;
mod iam_policy;
mod iam_role;
mod iam_user;
mod iam_user_self;
pub(crate) mod ops;
pub mod ops_settings;
pub(crate) mod password;
mod permissions_boundary;
mod settings;

use std::sync::Arc;

use axum::Router;
use axum::routing::{delete, get, post, put};

pub use auth::CallerIdentity;

/// Shared state for management API handlers.
pub struct ManagementState {
    /// Catalog store implementing operational storage traits.
    pub catalog_store: Arc<dyn extenddb_storage::CatalogStore>,
    /// Auth/authz cache registry. Used by mutation handlers to issue
    /// write-through invalidations so self-induced IAM changes propagate
    /// instantly within the local instance.
    pub auth_cache: extenddb_auth::AuthCacheRegistry,
    /// Concrete authorization cache handle, used by the `/management/auth-cache-metrics`
    /// endpoint to expose per-sub-cache counters. The same instance is held
    /// trait-object-style in `auth_cache.authz` for invalidation calls.
    pub authz_cache: Arc<crate::CachedAuthzStore>,
    /// Concrete `TableKeyInfo` cache handle, used by the auth-cache-metrics
    /// endpoint. Same instance is held in `auth_cache.table_key_info`.
    pub table_key_info_cache: Arc<crate::CachedTableKeyInfoStore>,
    /// Server region, resolved into `aws:RequestedRegion` during trust policy
    /// evaluation.
    pub region: Arc<str>,
    /// Whether the listener serves TLS, resolved into `aws:SecureTransport`
    /// during trust policy evaluation.
    pub secure_transport: bool,
}

/// Build the management API router.
pub fn router() -> Router<Arc<ManagementState>> {
    Router::new()
        // Admin user endpoints
        .route("/admins", post(admin::create_admin))
        .route("/admins", get(admin::list_admins))
        .route("/admins/{name}", delete(admin::delete_admin))
        .route("/admins/{name}/password", put(admin::change_admin_password))
        // Account endpoints
        .route("/accounts", post(account::create_account))
        .route("/accounts", get(account::list_accounts))
        .route("/accounts/{id}", delete(account::delete_account))
        // IAM user endpoints (admin only)
        .route("/accounts/{id}/users", post(iam_user::create_user))
        .route("/accounts/{id}/users", get(iam_user::list_users))
        .route("/accounts/{id}/users/{name}", delete(iam_user::delete_user))
        // IAM user self-service endpoints
        .route(
            "/accounts/{id}/users/{name}/access-keys",
            post(iam_user_self::create_access_key),
        )
        .route(
            "/accounts/{id}/users/{name}/access-keys",
            get(iam_user_self::list_access_keys),
        )
        .route(
            "/accounts/{id}/users/{name}/access-keys/import",
            post(iam_user_self::import_access_key),
        )
        .route(
            "/accounts/{id}/users/{name}/access-keys/{key_id}",
            delete(iam_user_self::delete_access_key),
        )
        .route(
            "/accounts/{id}/users/{name}/password",
            put(iam_user_self::change_user_password),
        )
        // IAM group endpoints (admin only)
        .route("/accounts/{id}/groups", post(iam_group::create_group))
        .route("/accounts/{id}/groups", get(iam_group::list_groups))
        .route(
            "/accounts/{id}/groups/{name}",
            delete(iam_group::delete_group),
        )
        .route(
            "/accounts/{id}/groups/{name}/members",
            post(iam_group::add_member),
        )
        .route(
            "/accounts/{id}/groups/{name}/members/{user}",
            delete(iam_group::remove_member),
        )
        // IAM role endpoints (admin only)
        .route("/accounts/{id}/roles", post(iam_role::create_role))
        .route("/accounts/{id}/roles", get(iam_role::list_roles))
        .route("/accounts/{id}/roles/{name}", delete(iam_role::delete_role))
        // IAM role tag endpoints (admin only)
        .route("/accounts/{id}/roles/{name}/tags", put(iam_role::tag_role))
        .route(
            "/accounts/{id}/roles/{name}/tags",
            delete(iam_role::untag_role),
        )
        .route(
            "/accounts/{id}/roles/{name}/tags",
            get(iam_role::list_role_tags),
        )
        // AssumeRole endpoint (admin only)
        .route(
            "/accounts/{id}/roles/{name}/assume",
            post(assume_role::assume_role),
        )
        // IAM policy endpoints (admin only)
        .route(
            "/accounts/{id}/users/{name}/policy/{policy}",
            put(iam_policy::put_user_policy),
        )
        .route(
            "/accounts/{id}/users/{name}/policies",
            get(iam_policy::list_user_policies),
        )
        .route(
            "/accounts/{id}/users/{name}/policy/{policy}",
            delete(iam_policy::delete_user_policy),
        )
        .route(
            "/accounts/{id}/groups/{name}/policy/{policy}",
            put(iam_policy::put_group_policy),
        )
        .route(
            "/accounts/{id}/groups/{name}/policies",
            get(iam_policy::list_group_policies),
        )
        .route(
            "/accounts/{id}/groups/{name}/policy/{policy}",
            delete(iam_policy::delete_group_policy),
        )
        // Role policy endpoints (admin only)
        .route(
            "/accounts/{id}/roles/{name}/policy/{policy}",
            put(iam_policy::put_role_policy),
        )
        .route(
            "/accounts/{id}/roles/{name}/policies",
            get(iam_policy::list_role_policies),
        )
        .route(
            "/accounts/{id}/roles/{name}/policy/{policy}",
            delete(iam_policy::delete_role_policy),
        )
        // IAM user tag endpoints (admin only)
        .route("/accounts/{id}/users/{name}/tags", put(iam_user::tag_user))
        .route(
            "/accounts/{id}/users/{name}/tags",
            delete(iam_user::untag_user),
        )
        .route(
            "/accounts/{id}/users/{name}/tags",
            get(iam_user::list_user_tags),
        )
        // Permissions boundary endpoints (admin only)
        .route(
            "/accounts/{id}/users/{name}/permissions-boundary",
            put(permissions_boundary::set_user_boundary),
        )
        .route(
            "/accounts/{id}/users/{name}/permissions-boundary",
            get(permissions_boundary::get_user_boundary),
        )
        .route(
            "/accounts/{id}/users/{name}/permissions-boundary",
            delete(permissions_boundary::delete_user_boundary),
        )
        .route(
            "/accounts/{id}/roles/{name}/permissions-boundary",
            put(permissions_boundary::set_role_boundary),
        )
        .route(
            "/accounts/{id}/roles/{name}/permissions-boundary",
            get(permissions_boundary::get_role_boundary),
        )
        .route(
            "/accounts/{id}/roles/{name}/permissions-boundary",
            delete(permissions_boundary::delete_role_boundary),
        )
        // Settings endpoints (admin only)
        .route("/settings", get(settings::list_settings))
        .route("/settings/{key}", get(settings::get_setting))
        .route("/settings/{key}", put(settings::set_setting))
        // Cache observability (admin only). Exposes per-cache hit/miss/
        // refresh counters and current entry counts. Authenticated via the
        // standard admin Basic auth.
        .route(
            "/auth-cache-metrics",
            get(cache_metrics::auth_cache_metrics),
        )
        // Manual cache invalidation (admin only break-glass tool).
        // See docs/design/12-auth-authz-cache.md §6.1.
        .route("/cache/invalidate", post(cache_invalidate::invalidate))
}

/// Validate an IAM name the way the management API does: 1-128 ASCII bytes of
/// alphanumerics and `-_.+=@`. IAM's own rules differ: it also allows a comma,
/// and it caps role names at 64 characters.
pub(crate) fn is_valid_iam_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'+' | b'=' | b'@')
        })
}

/// Reject a tag set with two keys that collide under ASCII case folding.
///
/// The live service treats tag keys as case insensitive for uniqueness: a
/// `TagUser`/`TagRole`/`TagResource` request carrying both `Team` and `team`
/// is rejected ("Duplicate tag keys found. Please note that Tag keys are case
/// insensitive."). Two keys that differ by more than ASCII case (for example
/// `mass` and `ma\u{00df}`) are distinct and allowed. Returns the duplicate key
/// when found.
pub(crate) fn duplicate_tag_key_ci(tags: &[(String, String)]) -> Option<String> {
    let mut seen: Vec<String> = Vec::with_capacity(tags.len());
    for (k, _) in tags {
        let lower = k.to_ascii_lowercase();
        if seen.contains(&lower) {
            return Some(k.clone());
        }
        seen.push(lower);
    }
    None
}

/// Validate a tag set against the live IAM service's tag rules, returning an
/// error string for the first violation. Each key is 1-128 characters and each
/// value at most 256 characters (counted in characters, not bytes), drawn from
/// IAM's grammar (`[\p{L}\p{Z}\p{N}_.:/=+\-@]`, which excludes combining
/// marks and most punctuation), and a key must not use the reserved `aws:`
/// prefix. The empty value and a leading or trailing space are allowed, matching
/// the service.
pub(crate) fn validate_iam_tags(tags: &[(String, String)]) -> Result<(), String> {
    for (k, v) in tags {
        let n = k.chars().count();
        if n == 0 || n > 128 {
            return Err("tag key must be 1-128 characters".to_owned());
        }
        if k.to_ascii_lowercase().starts_with("aws:") {
            return Err("tag key must not begin with the reserved prefix aws:".to_owned());
        }
        if !k.chars().all(is_iam_tag_char) {
            return Err(format!("tag key contains an invalid character: {k}"));
        }
        if v.chars().count() > 256 {
            return Err("tag value must not exceed 256 characters".to_owned());
        }
        if !v.chars().all(is_iam_tag_char) {
            return Err("tag value contains an invalid character".to_owned());
        }
    }
    Ok(())
}

/// Whether `c` is admissible in an IAM tag key or value.
///
/// IAM's tag grammar is `[\p{L}\p{Z}\p{N}_.:/=+\-@]`: any Unicode Letter
/// (Lu, Ll, Lt, Lm, Lo), Number (Nd, Nl, No) or Separator (Zs, Zl, Zp), plus the
/// listed symbols. These are Unicode general categories, matched here through
/// `general_category_group`, not `char::is_alphabetic` / `char::is_numeric`:
/// `is_alphabetic` is the Alphabetic property, a superset of `\p{L}` that admits
/// Other_Alphabetic combining marks such as U+0345 (category Mn) that IAM
/// rejects, and an ASCII-space-only separator check rejects the `\p{Z}`
/// separators (U+00A0, U+2002, U+3000 and the rest) that IAM accepts.
///
/// `unicode-properties` carries Unicode 15.1 general categories; the case-fold
/// table in `extenddb_auth` is Unicode 15.0.0. Unicode 15.1 added only uncased
/// characters (CJK Unified Ideographs Extension I and five ideographic
/// description characters), so its case folding is identical to 15.0.0 and
/// the fold table holds under either version.
fn is_iam_tag_char(c: char) -> bool {
    use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};
    matches!(c, '_' | '.' | ':' | '/' | '=' | '+' | '-' | '@')
        || matches!(
            c.general_category_group(),
            GeneralCategoryGroup::Letter
                | GeneralCategoryGroup::Number
                | GeneralCategoryGroup::Separator
        )
}

/// Validate admin name: 1-64 chars, alphanumeric, hyphens, or underscores.
fn is_valid_admin_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tag_validation_tests {
    use super::{duplicate_tag_key_ci, validate_iam_tags};

    fn t(k: &str, v: &str) -> Vec<(String, String)> {
        vec![(k.to_owned(), v.to_owned())]
    }

    #[test]
    fn accepts_service_valid_tags() {
        assert!(validate_iam_tags(&t("team", "prod")).is_ok());
        assert!(validate_iam_tags(&t("a_.:/=+-@b", "v_.:/=+-@")).is_ok());
        assert!(validate_iam_tags(&t("my team", "east coast")).is_ok()); // space ok
        assert!(validate_iam_tags(&t("team", "")).is_ok()); // empty value ok
        assert!(validate_iam_tags(&t(" team", "prod")).is_ok()); // leading space ok
        assert!(validate_iam_tags(&t("equipe\u{00e9}", "prod")).is_ok()); // precomposed letter
    }

    #[test]
    fn rejects_service_invalid_tags() {
        assert!(validate_iam_tags(&t("Equipe\u{0301}", "prod")).is_err()); // combining mark
        assert!(validate_iam_tags(&t("a#b", "prod")).is_err());
        assert!(validate_iam_tags(&t("a!b", "prod")).is_err());
        assert!(validate_iam_tags(&t("aws:team", "prod")).is_err()); // reserved prefix
        assert!(validate_iam_tags(&t("AWS:team", "prod")).is_err()); // case-insensitive prefix
        assert!(validate_iam_tags(&t(&"k".repeat(129), "prod")).is_err()); // key too long
        assert!(validate_iam_tags(&t("team", &"v".repeat(257))).is_err()); // value too long
        assert!(validate_iam_tags(&t("team", "v\u{0301}")).is_err()); // combining mark in value
    }

    #[test]
    fn tag_charset_matches_unicode_general_categories() {
        // Separators (Zs, Zl, Zp) are accepted, matching live IAM.
        for sep in [
            '\u{00a0}', // no-break space (Zs)
            '\u{2002}', // en space (Zs)
            '\u{2003}', // em space (Zs)
            '\u{3000}', // ideographic space (Zs)
            '\u{2028}', // line separator (Zl)
            '\u{2029}', // paragraph separator (Zp)
            '\u{202f}', // narrow no-break space (Zs)
        ] {
            let k = format!("a{sep}b");
            assert!(validate_iam_tags(&t(&k, "v")).is_ok(), "sep {sep:?} key");
            assert!(validate_iam_tags(&t("k", &k)).is_ok(), "sep {sep:?} value");
        }
        // Numbers beyond Nd: Nl and No are accepted.
        assert!(validate_iam_tags(&t("a\u{2160}b", "v")).is_ok()); // roman numeral one (Nl)
        assert!(validate_iam_tags(&t("a\u{00b2}b", "v")).is_ok()); // superscript two (No)
        assert!(validate_iam_tags(&t("a\u{00bd}b", "v")).is_ok()); // vulgar fraction half (No)
        // Marks (Mn) are rejected even when Alphabetic (Other_Alphabetic),
        // matching live IAM.
        assert!(validate_iam_tags(&t("a\u{0345}b", "v")).is_err()); // ypogegrammeni (Mn, Other_Alphabetic)
        assert!(validate_iam_tags(&t("a\u{0301}b", "v")).is_err()); // combining acute (Mn)
        assert!(validate_iam_tags(&t("k", "v\u{0345}")).is_err()); // in a value too
        // Symbols are rejected: an emoji (So).
        assert!(validate_iam_tags(&t("a\u{1f600}b", "v")).is_err());
        // Control: ASCII space (Zs) still accepted.
        assert!(validate_iam_tags(&t("a b", "v")).is_ok());
    }

    #[test]
    fn duplicate_ascii_case_key_detected() {
        let dup = duplicate_tag_key_ci(&[
            ("Team".to_owned(), "1".to_owned()),
            ("team".to_owned(), "2".to_owned()),
        ]);
        assert_eq!(dup.as_deref(), Some("team"));
        // mass and sharp-s differ by more than ASCII case: distinct, allowed.
        assert!(
            duplicate_tag_key_ci(&[
                ("mass".to_owned(), "A".to_owned()),
                ("ma\u{00df}".to_owned(), "B".to_owned()),
            ])
            .is_none()
        );
    }
}

#[cfg(test)]
mod iam_name_tests {
    use super::is_valid_iam_name;

    #[test]
    fn accepts_management_api_names() {
        assert!(is_valid_iam_name("a"));
        assert!(is_valid_iam_name("Role-1_2.3+4=5@6"));
        assert!(is_valid_iam_name(&"x".repeat(128)));
    }

    #[test]
    fn rejects_empty_long_and_invalid_characters() {
        assert!(!is_valid_iam_name(""));
        assert!(!is_valid_iam_name(&"x".repeat(129)));
        // The names the console accepted before #445: space, bang, star.
        assert!(!is_valid_iam_name("bad role!*"));
        assert!(!is_valid_iam_name("bad name!*"));
        assert!(!is_valid_iam_name("a/b"));
        assert!(!is_valid_iam_name("caf\u{00e9}"));
    }
}

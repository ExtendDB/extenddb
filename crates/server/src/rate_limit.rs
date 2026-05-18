// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Login rate limiting and account lockout.
//!
//! All state lives in the storage backend (e.g. `login_attempts` table in
//! PostgreSQL) so multiple ExtendDB instances sharing a catalog see a consistent
//! view. No in-process caching.

use extenddb_storage::management_store::RateLimitStore;

/// Maximum failed login attempts per principal in the lookback window before
/// the account is locked out.
const MAX_FAILURES_PER_PRINCIPAL: i64 = 5;

/// Maximum failed login attempts per source IP in the lookback window.
const MAX_FAILURES_PER_IP: i64 = 20;

/// Lookback window in seconds for counting failed attempts.
const WINDOW_SECONDS: i64 = 900; // 15 minutes

/// Check whether a login attempt should be blocked due to rate limiting or
/// account lockout. Returns `Ok(())` if the attempt may proceed, or
/// `Err(message)` if it should be rejected.
///
/// # Errors
///
/// Returns `Err(String)` when the principal is locked out, the source IP
/// exceeds the rate limit, or a database error occurs during the check.
pub async fn check_login_allowed(
    store: &dyn RateLimitStore,
    principal: &str,
    source_ip: Option<&str>,
) -> Result<(), String> {
    // Check per-principal lockout.
    let principal_failures = store
        .count_principal_failures(principal, WINDOW_SECONDS)
        .await
        .map_err(|e| {
            tracing::error!("Rate limit check failed: {e:?}");
            "Internal error".to_owned()
        })?;

    if principal_failures >= MAX_FAILURES_PER_PRINCIPAL {
        tracing::warn!(
            "Account lockout: principal {principal} has {principal_failures} failed attempts in window",
        );
        return Err("Too many failed login attempts. Please try again later.".to_owned());
    }

    // Check per-IP rate limit.
    if let Some(ip) = source_ip {
        let ip_failures = store
            .count_ip_failures(ip, WINDOW_SECONDS)
            .await
            .map_err(|e| {
                tracing::error!("Rate limit check failed: {e:?}");
                "Internal error".to_owned()
            })?;

        if ip_failures >= MAX_FAILURES_PER_IP {
            tracing::warn!("IP rate limit: {ip} has {ip_failures} failed attempts in window",);
            return Err(
                "Too many failed login attempts from this address. Please try again later."
                    .to_owned(),
            );
        }
    }

    Ok(())
}

/// Record a failed login attempt.
///
/// **Design decision (fail-open):** If the storage write fails, the attempt
/// is logged but not rejected. This preserves availability during storage outages
/// at the cost of allowing an attacker to bypass rate limiting while storage is
/// unreachable. The per-principal lockout check in `check_login_allowed` already
/// fails-closed (storage error → reject).
pub async fn record_failed_login(
    store: &dyn RateLimitStore,
    principal: &str,
    source_ip: Option<&str>,
) {
    store.record_failed_login(principal, source_ip).await;
}

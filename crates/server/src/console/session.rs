// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! In-memory session store for the management console.
//!
//! Sessions are random 32-byte hex tokens stored in a cookie. They map to a
//! `CallerIdentity` (admin or IAM user) and a CSRF token for form protection.
//! Sessions expire after 8 hours of inactivity, and 24 hours after login
//! however active they are. A background reaper is not needed: expired
//! sessions are pruned lazily on lookup.
//!
//! Each session also holds a [`CredentialStamp`] of the password hash its
//! principal logged in with. [`SessionStore::validate`] reads the principal's
//! current hash from the catalog on every request, and ends the session when
//! the principal no longer exists or its hash has changed. Every server reads
//! the same catalog, so deleting a principal or changing its password through
//! any server ends its sessions on all of them, each at the next request it
//! serves for that session.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use extenddb_storage::management_store::{OpResult, PasswordHashStore};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::management::CallerIdentity;

/// How long a session remains valid without activity.
const SESSION_TTL: Duration = Duration::from_secs(8 * 3600);

/// How long a session remains valid after login, however active it is.
///
/// Activity refreshes the idle timeout, so without this cap a session token
/// that is used at least every [`SESSION_TTL`] would stay valid until the
/// process restarts. The cookie's `Max-Age` does not bound it: the server
/// never sees that attribute, and a stolen token can be sent without it.
const SESSION_MAX_LIFETIME: Duration = Duration::from_secs(24 * 3600);

/// Maximum sessions before lazy pruning triggers on insert.
const MAX_SESSIONS: usize = 10_000;

/// SHA-256 digest of the password hash a session was created against.
///
/// bcrypt draws a new salt for every hash, so each password change stores a
/// different hash, even when the new password equals the old one. The
/// session keeps this digest rather than the hash itself.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialStamp([u8; 32]);

impl CredentialStamp {
    /// The stamp of a stored password hash.
    #[must_use]
    pub fn of(password_hash: &str) -> Self {
        Self(Sha256::digest(password_hash.as_bytes()).into())
    }
}

struct SessionEntry {
    identity: CallerIdentity,
    csrf_token: String,
    stamp: CredentialStamp,
    created_at: Instant,
    last_active: Instant,
}

impl SessionEntry {
    /// Whether the session has been idle longer than `ttl`, or has existed
    /// longer than `max_lifetime`, as of `now`.
    fn is_expired(&self, now: Instant, ttl: Duration, max_lifetime: Duration) -> bool {
        now.saturating_duration_since(self.last_active) > ttl
            || now.saturating_duration_since(self.created_at) > max_lifetime
    }
}

/// Result of [`SessionStore::validate`].
pub enum SessionCheck {
    /// The session is live and its principal still has the password hash the
    /// session was created against.
    Valid {
        identity: CallerIdentity,
        csrf_token: String,
    },
    /// There is no such session, it expired, or its principal was deleted or
    /// had its password changed. In the last two cases the session has been
    /// removed from the store.
    Invalid,
    /// The catalog could not be read. The session is kept, and the request
    /// must be refused.
    Unavailable,
}

/// Thread-safe in-memory session store.
///
/// Uses `tokio::sync::Mutex` because this store is accessed from async
/// handlers. `std::sync::Mutex` would risk deadlocks if held across `.await`
/// points and poisons on panic.
pub struct SessionStore {
    sessions: Mutex<HashMap<String, SessionEntry>>,
    /// How long a session lasts without activity: [`SESSION_TTL`], except in
    /// tests.
    ttl: Duration,
    /// How long a session lasts after login: [`SESSION_MAX_LIFETIME`], except
    /// in tests.
    max_lifetime: Duration,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    /// Create an empty session store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            ttl: SESSION_TTL,
            max_lifetime: SESSION_MAX_LIFETIME,
        }
    }

    /// Create an empty session store whose sessions last `ttl` without
    /// activity and `max_lifetime` after login.
    #[cfg(test)]
    fn with_limits(ttl: Duration, max_lifetime: Duration) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            ttl,
            max_lifetime,
        }
    }

    /// Create a session for the given identity, bound to the stamp of the
    /// password hash the login verified. Returns `(session_token, csrf_token)`.
    pub async fn create(
        &self,
        identity: CallerIdentity,
        stamp: CredentialStamp,
    ) -> (String, String) {
        let token = generate_token();
        let csrf = generate_token();
        let now = Instant::now();
        let entry = SessionEntry {
            identity,
            csrf_token: csrf.clone(),
            stamp,
            created_at: now,
            last_active: now,
        };
        let mut map = self.sessions.lock().await;
        // Lazy prune if too many sessions.
        if map.len() >= MAX_SESSIONS {
            let (ttl, max_lifetime) = (self.ttl, self.max_lifetime);
            map.retain(|_, e| !e.is_expired(now, ttl, max_lifetime));
        }
        map.insert(token.clone(), entry);
        (token, csrf)
    }

    /// Check a session token for a console request.
    ///
    /// Looks the session up, then reads the principal's current password hash
    /// from `catalog`. Only a request that passes the check refreshes the
    /// session's last-active time, so requests refused while the catalog
    /// cannot be read do not keep an idle session alive. The store lock is
    /// not held during the read.
    pub async fn validate(&self, token: &str, catalog: &dyn PasswordHashStore) -> SessionCheck {
        let Some((identity, csrf_token, stamp)) = self.lookup(token).await else {
            return SessionCheck::Invalid;
        };
        match current_password_hash(catalog, &identity).await {
            Ok(Some(hash)) if CredentialStamp::of(&hash) == stamp => {
                self.touch(token).await;
                SessionCheck::Valid {
                    identity,
                    csrf_token,
                }
            }
            Ok(_) => {
                self.remove(token).await;
                SessionCheck::Invalid
            }
            Err(e) => {
                tracing::warn!("console session check could not read the catalog: {e:?}");
                SessionCheck::Unavailable
            }
        }
    }

    /// Look up a session by token. Returns `(identity, csrf_token, stamp)` if
    /// it has neither been idle too long nor outlived its maximum lifetime.
    /// Does not refresh the last-active timestamp.
    async fn lookup(&self, token: &str) -> Option<(CallerIdentity, String, CredentialStamp)> {
        let mut map = self.sessions.lock().await;
        let entry = map.get(token)?;
        if entry.is_expired(Instant::now(), self.ttl, self.max_lifetime) {
            map.remove(token);
            return None;
        }
        Some((
            entry.identity.clone(),
            entry.csrf_token.clone(),
            entry.stamp.clone(),
        ))
    }

    /// Record activity on a session that passed its check.
    async fn touch(&self, token: &str) {
        if let Some(entry) = self.sessions.lock().await.get_mut(token) {
            entry.last_active = Instant::now();
        }
    }

    /// Remove a session (logout).
    pub async fn remove(&self, token: &str) {
        let mut map = self.sessions.lock().await;
        map.remove(token);
    }
}

/// The principal's stored password hash: `None` if the principal does not
/// exist or has no console password.
async fn current_password_hash(
    catalog: &dyn PasswordHashStore,
    identity: &CallerIdentity,
) -> OpResult<Option<String>> {
    match identity {
        CallerIdentity::Admin(name) => catalog.get_admin_password_hash(name).await,
        CallerIdentity::IamUser {
            account_id,
            user_name,
        } => catalog.get_user_password_hash(account_id, user_name).await,
    }
}

/// Generate a cryptographically random 32-byte hex token.
fn generate_token() -> String {
    use rand::Rng;
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    hex_encode(&bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! In-memory session store for the management console.
//!
//! Sessions are random 32-byte hex tokens stored in a cookie. They map to a
//! `CallerIdentity` (admin or IAM user) and a CSRF token for form protection.
//! Sessions expire after 8 hours of inactivity. A background reaper is not
//! needed: expired sessions are pruned lazily on lookup.
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
    last_active: Instant,
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
        let entry = SessionEntry {
            identity,
            csrf_token: csrf.clone(),
            stamp,
            last_active: Instant::now(),
        };
        let mut map = self.sessions.lock().await;
        // Lazy prune if too many sessions.
        if map.len() >= MAX_SESSIONS {
            let now = Instant::now();
            map.retain(|_, e| now.duration_since(e.last_active) < SESSION_TTL);
        }
        map.insert(token.clone(), entry);
        (token, csrf)
    }

    /// Check a session token for a console request.
    ///
    /// Looks the session up, refreshing its last-active time, then reads the
    /// principal's current password hash from `catalog`. The store lock is
    /// not held during the read.
    pub async fn validate(&self, token: &str, catalog: &dyn PasswordHashStore) -> SessionCheck {
        let Some((identity, csrf_token, stamp)) = self.lookup(token).await else {
            return SessionCheck::Invalid;
        };
        match current_password_hash(catalog, &identity).await {
            Ok(Some(hash)) if CredentialStamp::of(&hash) == stamp => SessionCheck::Valid {
                identity,
                csrf_token,
            },
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
    /// it has not expired, refreshing the last-active timestamp.
    async fn lookup(&self, token: &str) -> Option<(CallerIdentity, String, CredentialStamp)> {
        let mut map = self.sessions.lock().await;
        let entry = map.get_mut(token)?;
        if entry.last_active.elapsed() > SESSION_TTL {
            map.remove(token);
            return None;
        }
        entry.last_active = Instant::now();
        Some((
            entry.identity.clone(),
            entry.csrf_token.clone(),
            entry.stamp.clone(),
        ))
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

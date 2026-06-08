// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! In-memory session store for the management console.
//!
//! Sessions are random 32-byte hex tokens stored in a cookie. They map to a
//! `CallerIdentity` (admin or IAM user) and a CSRF token for form protection.
//! Sessions expire after 8 hours of inactivity. A background reaper is not
//! needed — expired sessions are pruned lazily on lookup.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::management::CallerIdentity;

/// How long a session remains valid without activity.
const SESSION_TTL: Duration = Duration::from_secs(8 * 3600);

/// Maximum sessions before lazy pruning triggers on insert.
const MAX_SESSIONS: usize = 10_000;

struct SessionEntry {
    identity: CallerIdentity,
    csrf_token: String,
    last_active: Instant,
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

    /// Create a session for the given identity. Returns `(session_token, csrf_token)`.
    pub async fn create(&self, identity: CallerIdentity) -> (String, String) {
        let token = generate_token();
        let csrf = generate_token();
        let entry = SessionEntry {
            identity,
            csrf_token: csrf.clone(),
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

    /// Look up a session by token. Returns `(identity, csrf_token)` if valid,
    /// refreshing the last-active timestamp. Returns `None` if expired or not found.
    pub async fn get(&self, token: &str) -> Option<(CallerIdentity, String)> {
        let mut map = self.sessions.lock().await;
        let entry = map.get_mut(token)?;
        if entry.last_active.elapsed() > SESSION_TTL {
            map.remove(token);
            return None;
        }
        entry.last_active = Instant::now();
        Some((entry.identity.clone(), entry.csrf_token.clone()))
    }

    /// Remove a session (logout).
    pub async fn remove(&self, token: &str) {
        let mut map = self.sessions.lock().await;
        map.remove(token);
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

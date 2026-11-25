// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The cache entry of one access key, the value `lookup` gives a request,
//! the scope-date rules, and the temporary buffers of the cache.

use std::sync::{Arc, Mutex, PoisonError};

use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use web_time::{Duration, Instant};
use zeroize::Zeroize;

use crate::StoredCredential;
use crate::secmem::{KeySlot, StoreFull};
use crate::sigv4::verify::constant_time_eq;

/// The clock skew the server accepts, which decides when two scope dates are
/// valid at once (`verify.rs`, `MAX_CLOCK_SKEW_SECS`).
const SKEW: time::Duration = time::Duration::seconds(15 * 60);

/// A `SigV4` scope date, `YYYYMMDD`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ScopeDate([u8; 8]);

/// The signing key of one scope date, shared with the requests that use it.
pub(super) type DatedKey = (ScopeDate, Arc<KeySlot>);

impl ScopeDate {
    pub(super) fn parse(date: &str) -> Option<Self> {
        date.as_bytes().try_into().ok().map(Self)
    }

    pub(super) fn of(date: time::Date) -> Self {
        let text = format!(
            "{:04}{:02}{:02}",
            date.year(),
            u8::from(date.month()),
            date.day()
        );
        // A year outside four digits cannot come from a server clock; the
        // all-zero date matches no request and only skips caching.
        Self::parse(&text).unwrap_or(Self([b'0'; 8]))
    }

    pub(super) fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("")
    }
}

/// The scope dates the server accepts at `now`: the UTC date, and the
/// adjacent date when `now` is within the skew window of midnight. From
/// 23:45 to 00:15 UTC one access key can therefore need two signing keys.
pub(super) fn valid_dates(now: OffsetDateTime) -> (ScopeDate, Option<ScopeDate>) {
    let today = now.date();
    let adjacent = if now.time() < time::Time::MIDNIGHT + SKEW {
        today.previous_day()
    } else if now.time() >= time::Time::MIDNIGHT - SKEW {
        today.next_day()
    } else {
        None
    };
    (ScopeDate::of(today), adjacent.map(ScopeDate::of))
}

/// The cache entry of one access key. No plaintext secret, no plaintext
/// token.
pub struct CachedCredential {
    /// The account ID that owns this credential.
    pub account_id: String,
    /// The user name (for AKIA* keys) or role name (for ASIA* session keys).
    pub principal_name: String,
    /// For session credentials: the session name.
    pub session_name: Option<String>,
    /// Whether this is a session credential (ASIA*).
    pub is_session: bool,
    /// Whether the credential is active. Inactive keys are cached so the auth
    /// layer can produce the correct error response.
    pub is_active: bool,
    /// For session credentials: the absolute expiry time. Auth checks this
    /// on every cache hit so cached sessions do not outlive their
    /// `expires_at` window.
    pub expires_at: Option<OffsetDateTime>,
    /// The SHA-256 of the session token.
    session_token_hash: Option<[u8; 32]>,
    /// The signing key of each scope date in use, at most two.
    pub(super) keys: Mutex<[Option<DatedKey>; 2]>,
}

impl CachedCredential {
    pub(super) fn from_stored(stored: &StoredCredential) -> Self {
        Self {
            account_id: stored.account_id.clone(),
            principal_name: stored.principal_name.clone(),
            session_name: stored.session_name.clone(),
            is_session: stored.is_session,
            is_active: stored.is_active,
            expires_at: stored.expires_at,
            session_token_hash: stored
                .session_token
                .as_deref()
                .map(|token| Sha256::digest(token.as_bytes()).into()),
            keys: Mutex::new([None, None]),
        }
    }

    /// Whether `other` names the same identity: every field but the keys.
    pub(super) fn same_identity(&self, other: &Self) -> bool {
        let Self {
            account_id,
            principal_name,
            session_name,
            is_session,
            is_active,
            expires_at,
            session_token_hash,
            keys: _,
        } = self;
        *account_id == other.account_id
            && *principal_name == other.principal_name
            && *session_name == other.session_name
            && *is_session == other.is_session
            && *is_active == other.is_active
            && *expires_at == other.expires_at
            && *session_token_hash == other.session_token_hash
    }

    /// Whether `presented` is the session token: its SHA-256 is compared
    /// with the stored hash in constant time, over a fixed 32 bytes, so
    /// neither the token's contents nor its length changes the timing.
    /// `false` for a long-term key, which has no token.
    #[must_use]
    pub fn session_token_matches(&self, presented: &str) -> bool {
        let Some(stored) = &self.session_token_hash else {
            return false;
        };
        let presented: [u8; 32] = Sha256::digest(presented.as_bytes()).into();
        constant_time_eq(&presented, stored)
    }

    pub(super) fn key(&self, date: ScopeDate) -> Option<Arc<KeySlot>> {
        self.keys
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .flatten()
            .find(|(d, _)| *d == date)
            .map(|(_, slot)| Arc::clone(slot))
    }

    /// Record `slot` as the signing key of `date`, in a free position, in
    /// the position that already holds `date` (two requests derived it at
    /// once), or in the position of a stale date: one that is neither the
    /// current UTC date nor the adjacent date inside the skew window at
    /// `now`. When both positions hold other valid dates, the key serves this
    /// request only.
    pub(super) fn insert_key(&self, date: ScopeDate, slot: Arc<KeySlot>, now: OffsetDateTime) {
        let (today, adjacent) = valid_dates(now);
        let mut keys = self.keys.lock().unwrap_or_else(PoisonError::into_inner);
        let position = keys.iter().position(|k| match k {
            None => true,
            Some((d, _)) => *d == date || (*d != today && Some(*d) != adjacent),
        });
        if let Some(position) = position {
            keys[position] = Some((date, slot));
        }
    }
}

/// What `lookup` gives one request: the entry, and the signing key of the
/// request's scope date. The key is in the entry's slot, or in a temporary
/// buffer that is wiped and returned to the pool when this value drops.
pub struct ResolvedCredential {
    pub(super) entry: Arc<CachedCredential>,
    pub(super) key: Key,
}

pub(super) enum Key {
    Slot(Arc<KeySlot>),
    Buffer(BufferLease),
}

/// Where a key about to be derived goes.
pub(super) enum Destination {
    Slot(KeySlot),
    Buffer(BufferLease),
}

impl ResolvedCredential {
    /// The cached fields of the credential.
    #[must_use]
    pub fn credential(&self) -> &CachedCredential {
        &self.entry
    }

    /// The 32-byte `SigV4` signing key for the request's scope date.
    #[must_use]
    pub fn signing_key(&self) -> &[u8; 32] {
        match &self.key {
            Key::Slot(slot) => slot.expose(),
            Key::Buffer(lease) => lease.expose(),
        }
    }
}

impl std::fmt::Debug for ResolvedCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedCredential")
            .field("account_id", &self.entry.account_id)
            .field("principal_name", &self.entry.principal_name)
            .field("is_session", &self.entry.is_session)
            .field("is_active", &self.entry.is_active)
            .field(
                "key",
                &match &self.key {
                    Key::Slot(_) => "slot",
                    Key::Buffer(_) => "temporary buffer",
                },
            )
            .finish_non_exhaustive()
    }
}

/// The temporary buffers of a cache: one slot per runtime worker thread,
/// reserved when the cache is built, for a miss that finds the key store
/// full. A request takes its buffer after the credential read, and the
/// derive and the signature check then run in one synchronous section, so
/// each worker thread holds one buffer at a time.
pub(super) struct BufferPool {
    free: Mutex<Vec<KeySlot>>,
    pub(super) full_warning: Warning,
    pub(super) exhausted_warning: Warning,
}

impl BufferPool {
    pub(super) fn new(workers: usize) -> Arc<Self> {
        let mut free = Vec::with_capacity(workers);
        for _ in 0..workers {
            match KeySlot::new() {
                Ok(slot) => free.push(slot),
                Err(StoreFull) => {
                    tracing::error!(
                        reserved = free.len(),
                        wanted = workers,
                        "the locked key store is too small to reserve one temporary buffer \
                         per worker thread; it needs two slots per cache entry, one per key \
                         that startup loads, and one per worker thread"
                    );
                    break;
                }
            }
        }
        Arc::new(Self {
            free: Mutex::new(free),
            full_warning: Warning::default(),
            exhausted_warning: Warning::default(),
        })
    }

    pub(super) fn lease(self: &Arc<Self>) -> Option<BufferLease> {
        let slot = self
            .free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()?;
        Some(BufferLease {
            pool: Arc::clone(self),
            slot: Some(slot),
        })
    }
}

/// A temporary buffer on loan from the pool, wiped and returned on drop.
pub(super) struct BufferLease {
    pool: Arc<BufferPool>,
    /// `Some` until `drop` returns the slot to the pool.
    slot: Option<KeySlot>,
}

impl BufferLease {
    pub(super) fn expose(&self) -> &[u8; 32] {
        self.slot
            .as_ref()
            .expect("a leased buffer holds its slot until it drops")
            .expose()
    }

    pub(super) fn expose_mut(&mut self) -> &mut [u8; 32] {
        self.slot
            .as_mut()
            .expect("a leased buffer holds its slot until it drops")
            .expose_mut()
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        if let Some(mut slot) = self.slot.take() {
            slot.expose_mut().zeroize();
            self.pool
                .free
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(slot);
        }
    }
}

/// A warning logged at most once a minute.
#[derive(Default)]
pub(super) struct Warning {
    last: Mutex<Option<Instant>>,
}

impl Warning {
    pub(super) fn due(&self) -> bool {
        let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Instant::now();
        if last.is_some_and(|t| now.duration_since(t) < Duration::from_secs(60)) {
            return false;
        }
        *last = Some(now);
        true
    }
}

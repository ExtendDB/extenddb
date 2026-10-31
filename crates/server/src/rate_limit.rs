// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Login rate limiting and account lockout.
//!
//! Recorded failures live in the storage backend (the `login_attempts`
//! table), keyed by principal and by the address the attempt came from, so
//! every `ExtendDB` instance sharing a catalog sees the same history. Attempts
//! that have been admitted but not yet finished are counted in a process-wide
//! registry: an attempt reserves its slot before the password is checked, and
//! the reservation counts against later attempts until the failure is
//! recorded. A burst of concurrent wrong passwords therefore runs no more
//! password checks than the cap allows.
//!
//! Limits are applied per source address. Failures from one address never
//! lock a principal out of another address, so a third party who knows an
//! admin's name cannot deny that admin the console or the management API.
//! The source address is the peer address of the TCP connection; behind a
//! reverse proxy that is the proxy's address for every caller, and `ExtendDB`
//! does not read `X-Forwarded-For`, so all callers behind one proxy share
//! one set of limits. An IPv6 peer is counted by its /64 prefix (see
//! `limiter_source`).

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

use extenddb_storage::management_store::RateLimitStore;

/// Maximum failed attempts for one principal from one source address in the
/// window. Once reached, that address is refused for that principal until the
/// oldest failure leaves the window.
pub const MAX_FAILURES_PER_PRINCIPAL: i64 = 5;

/// Maximum failed attempts from one source address in the window, over all
/// principals.
pub const MAX_FAILURES_PER_IP: i64 = 20;

/// Lookback window in seconds for counting failed attempts.
pub const WINDOW_SECONDS: i64 = 900; // 15 minutes

/// Message returned when the (principal, source) pair is over its limit.
pub const PRINCIPAL_LIMITED: &str = "Too many failed login attempts. Please try again later.";

/// Message returned when the source address is over its limit.
pub const SOURCE_LIMITED: &str =
    "Too many failed login attempts from this address. Please try again later.";

/// Why a login attempt was not admitted.
#[derive(Debug, PartialEq, Eq)]
pub enum LoginRefusal {
    /// The principal from this source, or this source overall, has too many
    /// recent failures. Callers answer 429 with the message.
    TooManyAttempts(&'static str),
    /// The attempts store could not be read. Callers answer 500; nothing is
    /// recorded, and no password is checked.
    Internal,
}

/// An admitted login attempt. It holds one reservation against the limits of
/// its principal and source until it is recorded as a failure or dropped.
///
/// Drop it after a successful login, or when the password could not be
/// checked, to release the reservation without recording anything.
#[must_use = "the attempt holds a reservation until it is recorded or dropped"]
pub struct LoginAttempt {
    principal: String,
    source_ip: String,
}

impl LoginAttempt {
    /// The source key the attempt is counted under: an IPv4 address, or an
    /// IPv6 /64 prefix.
    #[must_use]
    pub fn source_ip(&self) -> &str {
        &self.source_ip
    }

    /// Record the attempt as a failed login, then release its reservation.
    ///
    /// The row is written before the reservation is released, so an admission
    /// check running at the same time sees the attempt in one or the other.
    ///
    /// The write is fail-open: if the store cannot record the failure, the
    /// error is logged and the limits are not tightened, which keeps logins
    /// available during a storage outage at the cost of not counting failures
    /// while storage is unreachable.
    pub async fn record_failure(self, store: &dyn RateLimitStore) {
        store
            .record_failed_login(&self.principal, Some(&self.source_ip))
            .await;
        // `self` drops here, after the row is written.
    }
}

impl Drop for LoginAttempt {
    fn drop(&mut self) {
        registry().release(&self.source_ip, &self.principal);
    }
}

/// Admit a login attempt for `principal` from `peer`, or refuse it.
///
/// The attempt is admitted when the failures recorded for the pair in the
/// window plus the attempts in flight for the pair (this one included) do not
/// exceed [`MAX_FAILURES_PER_PRINCIPAL`], and the same holds for the source
/// against [`MAX_FAILURES_PER_IP`].
///
/// # Errors
///
/// [`LoginRefusal::TooManyAttempts`] when a limit is reached and
/// [`LoginRefusal::Internal`] when the store cannot be read.
pub async fn begin_login(
    store: &dyn RateLimitStore,
    principal: &str,
    peer: SocketAddr,
) -> Result<LoginAttempt, LoginRefusal> {
    begin_login_from(store, principal, limiter_source(&peer)).await
}

/// The source key a peer's login attempts are counted under.
///
/// An IPv4 peer, and an IPv6 peer carrying an IPv4-mapped address, count
/// under the IPv4 address. Any other IPv6 peer counts under its /64 prefix
/// (`2001:db8:1:2::/64`): a /64 is the block usually assigned to one host or
/// site, and a client can send from any address in it, so keying on the full
/// address would give it a fresh set of limits for every address it picks.
///
/// This applies to the limiter only. `aws:SourceIp` still sees the full
/// address through [`crate::authorization::condition_source_ip`].
fn limiter_source(peer: &SocketAddr) -> String {
    match peer.ip().to_canonical() {
        IpAddr::V6(v6) => {
            let prefix = Ipv6Addr::from(u128::from(v6) & !(u128::from(u64::MAX)));
            format!("{prefix}/64")
        }
        v4 @ IpAddr::V4(_) => v4.to_string(),
    }
}

async fn begin_login_from(
    store: &dyn RateLimitStore,
    principal: &str,
    source_ip: String,
) -> Result<LoginAttempt, LoginRefusal> {
    // Reserve first, then count. The numbers returned include this attempt
    // and every other attempt in flight for the pair and the source at this
    // moment. An attempt admitted earlier is still in flight (so it is in the
    // numbers) or has written its row before releasing (so it is in the
    // count). One that is in both is counted twice, which only refuses
    // sooner. The reservation is released by `Drop` if the attempt is
    // refused.
    let (pair_in_flight, source_in_flight) = registry().reserve(&source_ip, principal);
    let attempt = LoginAttempt {
        principal: principal.to_owned(),
        source_ip,
    };

    let recorded_for_pair = store
        .count_principal_failures(&attempt.principal, &attempt.source_ip, WINDOW_SECONDS)
        .await
        .map_err(|e| {
            tracing::error!("Rate limit check failed: {e:?}");
            LoginRefusal::Internal
        })?;
    if recorded_for_pair + pair_in_flight > MAX_FAILURES_PER_PRINCIPAL {
        tracing::warn!(
            "Account lockout: principal {} from {} has {recorded_for_pair} failed attempts \
             and {} in flight in window",
            attempt.principal,
            attempt.source_ip,
            pair_in_flight - 1,
        );
        return Err(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED));
    }

    let recorded_for_source = store
        .count_ip_failures(&attempt.source_ip, WINDOW_SECONDS)
        .await
        .map_err(|e| {
            tracing::error!("Rate limit check failed: {e:?}");
            LoginRefusal::Internal
        })?;
    if recorded_for_source + source_in_flight > MAX_FAILURES_PER_IP {
        tracing::warn!(
            "IP rate limit: {} has {recorded_for_source} failed attempts and {} in flight in window",
            attempt.source_ip,
            source_in_flight - 1,
        );
        return Err(LoginRefusal::TooManyAttempts(SOURCE_LIMITED));
    }

    Ok(attempt)
}

// ── In-flight registry ─────────────────────────────────────────────────

#[derive(Default)]
struct SourceInFlight {
    total: i64,
    by_principal: HashMap<String, i64>,
}

/// Attempts admitted and not yet recorded or dropped, by source address and
/// principal. An entry exists only while something is in flight for it.
#[derive(Default)]
struct InFlightRegistry {
    sources: Mutex<HashMap<String, SourceInFlight>>,
}

impl InFlightRegistry {
    fn lock(&self) -> MutexGuard<'_, HashMap<String, SourceInFlight>> {
        self.sources.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Count one more attempt in flight for the pair and the source. Returns
    /// the pair's and the source's in-flight counts including this attempt.
    fn reserve(&self, source_ip: &str, principal: &str) -> (i64, i64) {
        let mut sources = self.lock();
        let source = sources.entry(source_ip.to_owned()).or_default();
        source.total += 1;
        let pair = source.by_principal.entry(principal.to_owned()).or_default();
        *pair += 1;
        (*pair, source.total)
    }

    fn release(&self, source_ip: &str, principal: &str) {
        let mut sources = self.lock();
        let Some(source) = sources.get_mut(source_ip) else {
            return;
        };
        if let Some(pair) = source.by_principal.get_mut(principal) {
            *pair -= 1;
            if *pair <= 0 {
                source.by_principal.remove(principal);
            }
        }
        source.total -= 1;
        if source.total <= 0 {
            sources.remove(source_ip);
        }
    }

    /// In-flight attempts for the pair, for tests.
    #[cfg(test)]
    fn in_flight(&self, source_ip: &str, principal: &str) -> i64 {
        self.lock()
            .get(source_ip)
            .and_then(|s| s.by_principal.get(principal))
            .copied()
            .unwrap_or(0)
    }

    /// Whether the source has any attempt in flight, for tests.
    #[cfg(test)]
    fn tracks(&self, source_ip: &str) -> bool {
        self.lock().contains_key(source_ip)
    }
}

fn registry() -> &'static InFlightRegistry {
    static REGISTRY: LazyLock<InFlightRegistry> = LazyLock::new(InFlightRegistry::default);
    &REGISTRY
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use extenddb_storage::management_store::{OpError, OpResult};
    use futures::future::BoxFuture;

    use super::*;

    /// An attempts store in memory: recorded failures as (principal, source)
    /// rows. `delay` slows the count queries so concurrent attempts overlap,
    /// and `broken` makes every read fail.
    #[derive(Default)]
    struct MemoryStore {
        rows: Mutex<Vec<(String, Option<String>)>>,
        delay: Option<Duration>,
        broken: AtomicBool,
    }

    impl MemoryStore {
        fn rows_for(&self, principal: &str, source_ip: &str) -> i64 {
            self.rows
                .lock()
                .unwrap()
                .iter()
                .filter(|(p, s)| p == principal && s.as_deref() == Some(source_ip))
                .count() as i64
        }

        fn row_count(&self) -> usize {
            self.rows.lock().unwrap().len()
        }

        async fn read(&self) -> OpResult<()> {
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            if self.broken.load(Ordering::SeqCst) {
                return Err(OpError::Internal("store is down".to_owned()));
            }
            Ok(())
        }
    }

    impl RateLimitStore for MemoryStore {
        fn count_principal_failures(
            &self,
            principal: &str,
            source_ip: &str,
            _window_seconds: i64,
        ) -> BoxFuture<'_, OpResult<i64>> {
            let principal = principal.to_owned();
            let source_ip = source_ip.to_owned();
            Box::pin(async move {
                self.read().await?;
                Ok(self.rows_for(&principal, &source_ip))
            })
        }

        fn count_ip_failures(
            &self,
            source_ip: &str,
            _window_seconds: i64,
        ) -> BoxFuture<'_, OpResult<i64>> {
            let source_ip = source_ip.to_owned();
            Box::pin(async move {
                self.read().await?;
                let rows = self.rows.lock().unwrap();
                Ok(rows
                    .iter()
                    .filter(|(_, s)| s.as_deref() == Some(source_ip.as_str()))
                    .count() as i64)
            })
        }

        fn record_failed_login(
            &self,
            principal: &str,
            source_ip: Option<&str>,
        ) -> BoxFuture<'_, ()> {
            let row = (principal.to_owned(), source_ip.map(str::to_owned));
            Box::pin(async move {
                self.rows.lock().unwrap().push(row);
            })
        }

        fn cleanup_old_attempts(&self, _max_age_seconds: i64) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    fn peer(ip: &str) -> SocketAddr {
        format!("{ip}:40000").parse().unwrap()
    }

    async fn fail_times(store: &MemoryStore, principal: &str, ip: &str, times: usize) {
        for _ in 0..times {
            let attempt = begin_login(store, principal, peer(ip)).await.unwrap();
            attempt.record_failure(store).await;
        }
    }

    // Every test uses its own addresses: the registry is process-wide.

    #[tokio::test]
    async fn failures_from_one_address_do_not_lock_the_principal_out_of_another() {
        let store = MemoryStore::default();
        fail_times(&store, "admin", "198.51.100.1", 5).await;
        assert_eq!(store.rows_for("admin", "198.51.100.1"), 5);

        // The limited address is refused for this principal.
        assert_eq!(
            begin_login(&store, "admin", peer("198.51.100.1"))
                .await
                .err(),
            Some(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED))
        );
        // Another address is admitted, and a success leaves no row.
        let attempt = begin_login(&store, "admin", peer("198.51.100.2"))
            .await
            .expect("another address is not affected");
        drop(attempt);
        assert_eq!(store.rows_for("admin", "198.51.100.2"), 0);
        // The limited address may still log in as someone else.
        let _other = begin_login(&store, "other", peer("198.51.100.1"))
            .await
            .expect("the limit is per principal and source");
    }

    #[tokio::test]
    async fn a_source_address_is_limited_across_principals() {
        let store = MemoryStore::default();
        for n in 0..MAX_FAILURES_PER_IP {
            fail_times(&store, &format!("user{n}"), "198.51.100.20", 1).await;
        }
        assert_eq!(
            begin_login(&store, "user-new", peer("198.51.100.20"))
                .await
                .err(),
            Some(LoginRefusal::TooManyAttempts(SOURCE_LIMITED))
        );
        let _elsewhere = begin_login(&store, "user-new", peer("198.51.100.21"))
            .await
            .expect("another address is not affected");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_concurrent_burst_admits_no_more_than_the_cap() {
        let store = Arc::new(MemoryStore {
            delay: Some(Duration::from_millis(50)),
            ..MemoryStore::default()
        });
        let mut tasks = Vec::new();
        for _ in 0..40 {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                begin_login(&*store, "admin", peer("198.51.100.30")).await
            }));
        }
        let mut admitted = Vec::new();
        for task in tasks {
            if let Ok(attempt) = task.await.unwrap() {
                admitted.push(attempt);
            }
        }
        // Admitted attempts are still in flight, so exactly the cap got in.
        assert_eq!(admitted.len() as i64, MAX_FAILURES_PER_PRINCIPAL);
        assert_eq!(
            registry().in_flight("198.51.100.30", "admin"),
            MAX_FAILURES_PER_PRINCIPAL
        );
        assert_eq!(store.row_count(), 0, "admission records nothing");

        // Once they are recorded, the address is out for this principal.
        for attempt in admitted {
            attempt.record_failure(&*store).await;
        }
        assert_eq!(store.rows_for("admin", "198.51.100.30"), 5);
        assert!(!registry().tracks("198.51.100.30"));
        assert_eq!(
            begin_login(&*store, "admin", peer("198.51.100.30"))
                .await
                .err(),
            Some(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED))
        );
    }

    #[tokio::test]
    async fn a_dropped_attempt_releases_its_reservation_without_a_row() {
        let store = MemoryStore::default();
        let attempt = begin_login(&store, "admin", peer("198.51.100.40"))
            .await
            .unwrap();
        assert_eq!(registry().in_flight("198.51.100.40", "admin"), 1);
        drop(attempt);
        assert!(!registry().tracks("198.51.100.40"));
        assert_eq!(store.row_count(), 0);
    }

    #[tokio::test]
    async fn a_store_error_is_internal_and_holds_no_reservation() {
        let store = MemoryStore::default();
        store.broken.store(true, Ordering::SeqCst);
        assert_eq!(
            begin_login(&store, "admin", peer("198.51.100.50"))
                .await
                .err(),
            Some(LoginRefusal::Internal)
        );
        assert!(!registry().tracks("198.51.100.50"));
    }

    #[test]
    fn an_ipv6_peer_is_keyed_by_its_64_and_ipv4_by_its_address() {
        assert_eq!(
            limiter_source(&peer("[2001:db8:1:2:aaaa:bbbb:cccc:dddd]")),
            "2001:db8:1:2::/64"
        );
        assert_eq!(
            limiter_source(&peer("[::ffff:198.51.100.7]")),
            "198.51.100.7"
        );
        assert_eq!(limiter_source(&peer("198.51.100.7")), "198.51.100.7");
    }

    #[tokio::test]
    async fn addresses_in_one_ipv6_64_share_the_pair_limit_and_another_64_does_not() {
        let store = MemoryStore::default();
        for n in 1..=MAX_FAILURES_PER_PRINCIPAL {
            let attempt = begin_login(&store, "admin", peer(&format!("[2001:db8:64:1::{n:x}]")))
                .await
                .unwrap();
            attempt.record_failure(&store).await;
        }
        assert_eq!(store.rows_for("admin", "2001:db8:64:1::/64"), 5);

        // A sixth address in the same /64 is refused for the principal.
        assert_eq!(
            begin_login(&store, "admin", peer("[2001:db8:64:1:ffff::99]"))
                .await
                .err(),
            Some(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED))
        );
        // The neighbouring /64 is a different source.
        let _other = begin_login(&store, "admin", peer("[2001:db8:64:2::1]"))
            .await
            .expect("another /64 is not affected");
    }

    #[tokio::test]
    async fn addresses_in_one_ipv6_64_share_the_source_limit_and_another_64_does_not() {
        let store = MemoryStore::default();
        for n in 1..=MAX_FAILURES_PER_IP {
            let attempt = begin_login(
                &store,
                &format!("user{n}"),
                peer(&format!("[2001:db8:64:3::{n:x}]")),
            )
            .await
            .unwrap();
            attempt.record_failure(&store).await;
        }
        assert_eq!(
            begin_login(&store, "user-new", peer("[2001:db8:64:3::abcd]"))
                .await
                .err(),
            Some(LoginRefusal::TooManyAttempts(SOURCE_LIMITED))
        );
        let _other = begin_login(&store, "user-new", peer("[2001:db8:64:4::abcd]"))
            .await
            .expect("another /64 is not affected");
    }

    #[tokio::test]
    async fn an_ipv4_mapped_peer_counts_as_its_ipv4_address() {
        let store = MemoryStore::default();
        fail_times(&store, "admin", "198.51.100.60", 5).await;
        assert_eq!(
            begin_login(&store, "admin", peer("[::ffff:198.51.100.60]"))
                .await
                .err(),
            Some(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED))
        );
    }
}

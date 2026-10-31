// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for login rate limiting, against an attempts store in memory.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use extenddb_storage::management_store::{OpError, OpResult};
use futures::future::BoxFuture;
use tokio::sync::{Notify, Semaphore};

use super::*;

/// An attempts store in memory: recorded failures as (principal, source)
/// rows. `delay` slows the count queries so concurrent attempts overlap,
/// and `broken` makes every read fail. With `write_gate` set, a failure
/// write announces itself on `write_started` and then waits for a permit
/// before the row lands.
#[derive(Default)]
pub(crate) struct MemoryStore {
    rows: Mutex<Vec<(String, Option<String>)>>,
    delay: Option<Duration>,
    broken: AtomicBool,
    write_gate: Option<Semaphore>,
    write_started: Notify,
}

impl MemoryStore {
    pub(crate) fn rows_for(&self, principal: &str, source_ip: &str) -> i64 {
        self.rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, s)| p == principal && s.as_deref() == Some(source_ip))
            .count() as i64
    }

    pub(crate) fn row_count(&self) -> usize {
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

    fn record_failed_login(&self, principal: &str, source_ip: Option<&str>) -> BoxFuture<'_, ()> {
        let row = (principal.to_owned(), source_ip.map(str::to_owned));
        Box::pin(async move {
            if let Some(gate) = &self.write_gate {
                self.write_started.notify_one();
                let _permit = gate.acquire().await.unwrap();
                self.rows.lock().unwrap().push(row);
                return;
            }
            self.rows.lock().unwrap().push(row);
        })
    }

    fn cleanup_old_attempts(&self, _max_age_seconds: i64) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

pub(crate) fn peer(ip: &str) -> SocketAddr {
    format!("{ip}:40000").parse().unwrap()
}

/// Log output collected by [`capture_logs`].
#[derive(Clone, Default)]
pub(crate) struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Collect this thread's log output until the guard drops. Use it from a
/// test on the default current-thread runtime, where the code under test
/// logs on the test's own thread.
pub(crate) fn capture_logs() -> (CapturedLogs, tracing::subscriber::DefaultGuard) {
    let logs = CapturedLogs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    (logs, tracing::subscriber::set_default(subscriber))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admission_while_a_failure_is_being_written_still_counts_it() {
    let store = Arc::new(MemoryStore {
        write_gate: Some(Semaphore::new(0)),
        ..MemoryStore::default()
    });
    let source = "198.51.100.70";
    let mut admitted = Vec::new();
    for _ in 0..MAX_FAILURES_PER_PRINCIPAL {
        admitted.push(begin_login(&*store, "admin", peer(source)).await.unwrap());
    }

    // Record one failure and hold its row write open.
    let first = admitted.pop().unwrap();
    let writer = {
        let store = Arc::clone(&store);
        tokio::spawn(async move { first.record_failure(&*store).await })
    };
    store.write_started.notified().await;

    // The failure has neither landed as a row nor finished. It must still
    // count against the pair, so the pair stays at its cap.
    assert_eq!(
        begin_login(&*store, "admin", peer(source)).await.err(),
        Some(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED))
    );

    store.write_gate.as_ref().unwrap().add_permits(8);
    writer.await.unwrap();
    for attempt in admitted {
        attempt.record_failure(&*store).await;
    }
    assert_eq!(store.rows_for("admin", source), MAX_FAILURES_PER_PRINCIPAL);
    assert!(!registry().tracks(source));
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

#[tokio::test]
async fn the_lockout_log_line_escapes_the_principal() {
    let store = MemoryStore::default();
    let principal = "admin\nERROR forged line";
    fail_times(&store, principal, "198.51.100.85", 5).await;

    let (logs, _guard) = capture_logs();
    assert_eq!(
        begin_login(&store, principal, peer("198.51.100.85"))
            .await
            .err(),
        Some(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED))
    );
    let text = logs.text();
    assert!(
        text.contains(r#"principal "admin\nERROR forged line" from 198.51.100.85"#),
        "{text}"
    );
    assert!(!text.contains("\nERROR forged line"), "{text}");
}

#[tokio::test]
async fn a_principal_over_the_length_limit_is_refused_before_the_store_is_read() {
    let store = MemoryStore::default();
    // Every read fails, so an attempt that reads the store is Internal.
    store.broken.store(true, Ordering::SeqCst);
    let too_long = "a".repeat(MAX_PRINCIPAL_LEN + 1);
    assert_eq!(
        begin_login(&store, &too_long, peer("198.51.100.80"))
            .await
            .err(),
        Some(LoginRefusal::InvalidCredentials)
    );
    assert!(!registry().tracks("198.51.100.80"));

    // A principal at the limit is checked against the store.
    let at_limit = "a".repeat(MAX_PRINCIPAL_LEN);
    assert_eq!(
        begin_login(&store, &at_limit, peer("198.51.100.80"))
            .await
            .err(),
        Some(LoginRefusal::Internal)
    );
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

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::StoredCredential;
use crate::secmem;
use crate::sigv4::signing_key::derive_signing_key;
use crate::test_support::{is_child, run_test_in_child};

const SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";

/// In-process credential store backed by a Mutex-protected map. Counts
/// `lookup_credential` invocations, which is the decrypt count, so tests can
/// assert cache behavior.
struct CountingCredStore {
    creds: Mutex<std::collections::HashMap<String, StoredCredential>>,
    calls: AtomicUsize,
}

impl CountingCredStore {
    fn new() -> Self {
        Self {
            creds: Mutex::new(std::collections::HashMap::new()),
            calls: AtomicUsize::new(0),
        }
    }

    fn insert(&self, key: &str, cred: StoredCredential) {
        self.creds.lock().unwrap().insert(key.to_owned(), cred);
    }

    fn remove(&self, key: &str) {
        self.creds.lock().unwrap().remove(key);
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl CredentialStore for CountingCredStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.creds.lock().unwrap().get(access_key_id).cloned())
    }
}

fn make_cred(secret: &str, principal: &str) -> StoredCredential {
    StoredCredential {
        secret_key: secret.to_owned(),
        account_id: "123456789012".to_owned(),
        principal_name: principal.to_owned(),
        session_name: None,
        is_session: false,
        session_token: None,
        is_active: true,
        expires_at: None,
    }
}

fn fast_config() -> SwrCacheConfig {
    SwrCacheConfig {
        ttl: Duration::from_millis(200),
        soft_ttl: Duration::from_millis(50),
        negative_ttl: Duration::from_millis(50),
        max_entries: 100,
        name: "test-cred",
    }
}

fn cached(inner: Arc<CountingCredStore>) -> CachedCredentialStore {
    CachedCredentialStore::with_arc(inner, fast_config(), REGION, 1)
}

/// A UTC instant from its calendar fields.
fn at(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> OffsetDateTime {
    let date =
        time::Date::from_calendar_date(year, time::Month::try_from(month).unwrap(), day).unwrap();
    date.with_hms(hour, minute, second).unwrap().assume_utc()
}

/// The scope date of the current UTC date.
fn today() -> String {
    ScopeDate::of(OffsetDateTime::now_utc().date())
        .as_str()
        .to_owned()
}

async fn lookup(store: &CachedCredentialStore, key: &str) -> Option<ResolvedCredential> {
    store.lookup(key, &today()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalidate_principal_drops_all_keys_for_user() {
    let inner = Arc::new(CountingCredStore::new());
    inner.insert("AKIAALICE1", make_cred("s1", "alice"));
    inner.insert("AKIAALICE2", make_cred("s2", "alice"));
    inner.insert("AKIABOB", make_cred("s3", "bob"));
    let store = cached(inner.clone());

    // Prime all three.
    let _ = lookup(&store, "AKIAALICE1").await.unwrap();
    let _ = lookup(&store, "AKIAALICE2").await.unwrap();
    let _ = lookup(&store, "AKIABOB").await.unwrap();
    assert_eq!(inner.calls(), 3);

    store.invalidate_principal("123456789012", "alice").unwrap();
    store.cache.run_pending_tasks().await;

    // Re-fetch each.
    let _ = lookup(&store, "AKIAALICE1").await.unwrap();
    let _ = lookup(&store, "AKIAALICE2").await.unwrap();
    let _ = lookup(&store, "AKIABOB").await.unwrap();
    assert_eq!(
        inner.calls(),
        5,
        "alice's two keys re-fetched (2); bob still cached (0). Initial=3, total=5."
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalidate_account_drops_all_creds_for_account() {
    let inner = Arc::new(CountingCredStore::new());
    let mut a1 = make_cred("s1", "alice");
    a1.account_id = "a1".to_owned();
    let mut a2 = make_cred("s2", "alice");
    a2.account_id = "a2".to_owned();
    inner.insert("AKIAA1", a1);
    inner.insert("AKIAA2", a2);
    let store = cached(inner.clone());

    let _ = lookup(&store, "AKIAA1").await.unwrap();
    let _ = lookup(&store, "AKIAA2").await.unwrap();
    assert_eq!(inner.calls(), 2);

    store.invalidate_account("a1").unwrap();
    store.cache.run_pending_tasks().await;

    let _ = lookup(&store, "AKIAA1").await.unwrap();
    let _ = lookup(&store, "AKIAA2").await.unwrap();
    assert_eq!(
        inner.calls(),
        3,
        "AKIAA1 (account a1) re-fetched; AKIAA2 (account a2) still cached"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pass_through_skips_cache_on_every_lookup() {
    let inner = Arc::new(CountingCredStore::new());
    inner.insert("AKIATEST", make_cred("s", "alice"));
    let store = CachedCredentialStore::pass_through_arc(inner.clone(), fast_config(), REGION, 1);

    for _ in 0..5 {
        let r = lookup(&store, "AKIATEST").await.unwrap();
        assert_eq!(r.credential().principal_name, "alice");
    }
    assert_eq!(
        inner.calls(),
        5,
        "pass-through must invoke inner store on every call"
    );
}

/// A second lookup for the same key and date is a hit: no decrypt, and the
/// signing key is the one the miss derived, which equals the key
/// `derive_signing_key` gives for the same inputs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_lookup_hits_cache_and_derives_nothing() {
    let inner = Arc::new(CountingCredStore::new());
    inner.insert("AKIATEST", make_cred(SECRET, "alice"));
    let store = cached(inner.clone());
    let date = today();

    let r1 = store.lookup("AKIATEST", &date).await.unwrap().unwrap();
    let r2 = store.lookup("AKIATEST", &date).await.unwrap().unwrap();

    assert_eq!(r1.credential().principal_name, "alice");
    assert_eq!(r2.credential().principal_name, "alice");
    assert_eq!(inner.calls(), 1, "second lookup must hit cache");
    let expected = derive_signing_key(SECRET, &date, REGION, "dynamodb");
    assert_eq!(r1.signing_key(), &*expected);
    assert_eq!(r2.signing_key(), &*expected);
}

/// A cached key asked for a scope date it has no key for costs one decrypt
/// and one derive. When that read gives the entry's identity, the new key is
/// cached beside the first. When it gives another (the access key ID was
/// deleted and imported for another principal, or changed, through another
/// server that shares the catalog), the request is answered from that read
/// with its key, and the entry is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_scope_date_is_one_decrypt_and_answers_from_that_read() {
    /// The change to the credential before the new date's read, and whether
    /// the entry is kept.
    type Case = (fn(&mut StoredCredential), bool);
    let cases: [Case; 8] = [
        (|_| {}, true),
        (|c| *c = make_cred("bobSecretAccessKey", "bob"), false),
        (|c| c.account_id = "210987654321".to_owned(), false),
        (|c| c.session_name = Some("s".to_owned()), false),
        (|c| c.is_session = true, false),
        (|c| c.session_token = Some("t".to_owned()), false),
        (|c| c.is_active = false, false),
        (|c| c.expires_at = Some(OffsetDateTime::UNIX_EPOCH), false),
    ];
    for (row, (change, kept)) in cases.into_iter().enumerate() {
        let mut cred = make_cred(SECRET, "alice");
        let inner = Arc::new(CountingCredStore::new());
        inner.insert("AKIATEST", cred.clone());
        let store = cached(inner.clone());
        let first = store.lookup("AKIATEST", "20260415").await.unwrap().unwrap();
        change(&mut cred);
        inner.insert("AKIATEST", cred.clone());
        let calls = inner.calls();
        let one_date_before = entry::valid_dates(OffsetDateTime::now_utc()).1.is_none();
        let r = store.lookup("AKIATEST", "20260416").await.unwrap().unwrap();
        assert_eq!(inner.calls(), calls + 1, "row {row}: one decrypt");
        let key = derive_signing_key(&cred.secret_key, "20260416", REGION, "dynamodb");
        assert_eq!(r.signing_key(), &*key, "row {row}: the key of the read");
        let read = CachedCredential::from_stored(&cred);
        assert!(
            r.credential().same_identity(&read),
            "row {row}: the read's identity"
        );
        let again = store.lookup("AKIATEST", "20260416").await.unwrap().unwrap();
        for answer in [&r, &again] {
            assert_eq!(Arc::ptr_eq(&first.entry, &answer.entry), kept, "row {row}");
        }
        if kept && one_date_before && entry::valid_dates(OffsetDateTime::now_utc()).1.is_none() {
            // From 23:45 to 00:15 UTC the load fills both slots with the two
            // valid dates, so the test's fixed date gets no slot to hit.
            assert_eq!(inner.calls(), calls + 1, "the new date is a hit now");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalidate_forces_refetch() {
    let inner = Arc::new(CountingCredStore::new());
    inner.insert("AKIATEST", make_cred("s", "alice"));
    let store = cached(inner.clone());

    let _ = lookup(&store, "AKIATEST").await.unwrap();
    store.invalidate("AKIATEST").await;
    let _ = lookup(&store, "AKIATEST").await.unwrap();

    assert_eq!(
        inner.calls(),
        2,
        "invalidation must force a re-fetch from the inner store"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negative_lookup_is_cached_then_picks_up_new_key_after_negative_ttl() {
    let inner = Arc::new(CountingCredStore::new());
    let store = cached(inner.clone());

    // Initially missing.
    let r1 = lookup(&store, "AKIANEW").await;
    assert!(r1.is_none());

    // Within negative_ttl, second call hits cache.
    let r2 = lookup(&store, "AKIANEW").await;
    assert!(r2.is_none());
    assert_eq!(inner.calls(), 1);

    // Operator inserts the key.
    inner.insert("AKIANEW", make_cred("s", "bob"));

    // Wait past negative_ttl (50ms).
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Lookup now sees it.
    let r3 = lookup(&store, "AKIANEW").await.unwrap();
    assert_eq!(r3.credential().principal_name, "bob");
}

/// CB-12 (cache-hit path): a cached session credential whose `expires_at`
/// has passed must NOT authenticate, even when the cache TTL hasn't
/// elapsed yet. Without the cache-hit expiry check, a session could
/// keep working for up to `auth.cache.ttl_seconds` past its expiry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_session_past_expires_at_returns_expired_token() {
    let inner = Arc::new(CountingCredStore::new());
    // A session that "expires" 10 seconds in the past. The CountingCredStore
    // mock returns the credential unconditionally; the wrapper layer is
    // what we're testing here.
    let mut cred = make_cred("s", "alice");
    cred.is_session = true;
    cred.session_token = Some("tok".to_owned());
    cred.expires_at = Some(OffsetDateTime::now_utc() - time::Duration::seconds(10));
    inner.insert("ASIATEST00000000", cred);

    let store = cached(inner.clone());

    let r1 = store.lookup("ASIATEST00000000", &today()).await;
    match r1 {
        Err(DynamoDbError::ExpiredTokenException(_)) => {}
        Err(other) => {
            panic!("expected ExpiredTokenException from cache-hit expiry check, got {other:?}")
        }
        Ok(_) => panic!("expected ExpiredTokenException, got Ok"),
    }
    // The cached entry was dropped on the expiry check. A second call
    // re-hits the inner store rather than serving the cached expired cred.
    let calls_after_first = inner.calls();
    let r2 = store.lookup("ASIATEST00000000", &today()).await;
    match r2 {
        Err(DynamoDbError::ExpiredTokenException(_)) => {}
        Err(other) => {
            panic!("expected ExpiredTokenException on re-fetch, got {other:?}")
        }
        Ok(_) => panic!("expected ExpiredTokenException on re-fetch, got Ok"),
    }
    assert!(
        inner.calls() > calls_after_first,
        "expired-session cache hit must drop the entry, forcing a re-fetch on the next call \
         (calls_after_first={calls_after_first}, after_second={})",
        inner.calls()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalidation_after_delete_returns_none() {
    let inner = Arc::new(CountingCredStore::new());
    inner.insert("AKIATEST", make_cred("s", "alice"));
    let store = cached(inner.clone());

    // Prime cache.
    let r1 = lookup(&store, "AKIATEST").await.unwrap();
    assert_eq!(r1.credential().principal_name, "alice");

    // Simulate admin deletion: remove from inner, invalidate cache.
    inner.remove("AKIATEST");
    store.invalidate("AKIATEST").await;

    // Next lookup reflects the deletion.
    let r2 = lookup(&store, "AKIATEST").await;
    assert!(r2.is_none(), "post-invalidation lookup must see deletion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_lookups_for_same_key_collapse_to_few_loads() {
    let inner = Arc::new(CountingCredStore::new());
    inner.insert("AKIATEST", make_cred("s", "alice"));
    let store = cached(inner.clone());

    let mut handles = Vec::new();
    for _ in 0..64 {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            lookup(&store, "AKIATEST")
                .await
                .unwrap()
                .credential()
                .account_id
                .clone()
        }));
    }
    for h in handles {
        let _ = h.await.unwrap();
    }

    // The cache deduplicates *post-warmup*; cold-start may incur a few
    // duplicate inserts on the way to the first cached entry. Verify the
    // deduplication is meaningful (at most the caller count, and warmup is cheap).
    let n = inner.calls();
    assert!(
        n <= 64,
        "loader call count must not exceed concurrent caller count, got {n}"
    );
    // Once warm, no further lookups.
    let _ = lookup(&store, "AKIATEST").await.unwrap();
    assert_eq!(inner.calls(), n, "warm lookup must hit cache");
}

/// The scope dates valid at a server time: the UTC date alone from 00:15 to
/// 23:45, and the adjacent date too within the 900 s skew window of midnight.
#[test]
fn valid_dates_follow_the_skew_window() {
    let cases = [
        (at(2026, 4, 15, 0, 0, 0), "20260415", Some("20260414")),
        (at(2026, 4, 15, 0, 14, 59), "20260415", Some("20260414")),
        (at(2026, 4, 15, 0, 15, 0), "20260415", None),
        (at(2026, 4, 15, 12, 0, 0), "20260415", None),
        (at(2026, 4, 15, 23, 44, 59), "20260415", None),
        (at(2026, 4, 15, 23, 45, 0), "20260415", Some("20260416")),
        (at(2026, 4, 15, 23, 59, 59), "20260415", Some("20260416")),
        (at(2026, 12, 31, 23, 50, 0), "20261231", Some("20270101")),
        (at(2027, 1, 1, 0, 10, 0), "20270101", Some("20261231")),
    ];
    for (now, today, adjacent) in cases {
        let (t, a) = valid_dates(now);
        assert_eq!(t.as_str(), today, "{now}");
        assert_eq!(
            a.map(|d| d.as_str().to_owned()).as_deref(),
            adjacent,
            "{now}"
        );
    }
}

/// Which slot a new scope date takes: a free one, the one holding the same
/// date, or the one holding a date that is neither the UTC date nor the
/// adjacent date inside the skew window.
#[test]
fn a_new_date_takes_the_slot_of_a_stale_date() {
    let slot = || Arc::new(secmem::KeySlot::new().unwrap());
    let date = |s: &str| ScopeDate::parse(s).unwrap();
    // (slots held, now, new date, dates held afterwards, position of the new slot)
    let cases = [
        // A free slot is used first.
        (
            vec!["20260415"],
            at(2026, 4, 15, 23, 50, 0),
            "20260416",
            vec!["20260415", "20260416"],
            Some(1),
        ),
        // Yesterday is stale at 23:50, so tomorrow takes its slot.
        (
            vec!["20260414", "20260415"],
            at(2026, 4, 15, 23, 50, 0),
            "20260416",
            vec!["20260416", "20260415"],
            Some(0),
        ),
        // At 00:10 yesterday is still valid, so the day before is the stale one.
        (
            vec!["20260413", "20260414"],
            at(2026, 4, 15, 0, 10, 0),
            "20260415",
            vec!["20260415", "20260414"],
            Some(0),
        ),
        // Both dates valid: the key serves the request only, nothing changes.
        (
            vec!["20260414", "20260415"],
            at(2026, 4, 15, 0, 10, 0),
            "20260416",
            vec!["20260414", "20260415"],
            None,
        ),
        // A date derived twice at once replaces its own slot, not a valid one.
        (
            vec!["20260414", "20260415"],
            at(2026, 4, 15, 0, 10, 0),
            "20260415",
            vec!["20260414", "20260415"],
            Some(1),
        ),
    ];
    for (held, now, new, expected, position) in cases {
        let entry = CachedCredential::from_stored(&make_cred(SECRET, "alice"));
        {
            let mut keys = entry.keys.lock().unwrap();
            for (i, d) in held.iter().enumerate() {
                keys[i] = Some((date(d), slot()));
            }
        }
        let new_slot = slot();
        entry.insert_key(date(new), Arc::clone(&new_slot), now);
        let keys = entry.keys.lock().unwrap();
        let after: Vec<String> = keys
            .iter()
            .flatten()
            .map(|(d, _)| d.as_str().to_owned())
            .collect();
        assert_eq!(after, expected, "held {held:?} at {now}, new {new}");
        let found = keys
            .iter()
            .position(|k| k.as_ref().is_some_and(|(_, s)| Arc::ptr_eq(s, &new_slot)));
        assert_eq!(found, position, "held {held:?} at {now}, new {new}");
    }
}

/// The session token compare: the presented token's SHA-256 against the
/// stored hash. A long-term key matches no token.
#[test]
fn session_token_matches_compares_hashes() {
    let session = |token: &str| {
        let mut cred = make_cred(SECRET, "alice");
        cred.is_session = true;
        cred.session_token = Some(token.to_owned());
        CachedCredential::from_stored(&cred)
    };
    let long_term = CachedCredential::from_stored(&make_cred(SECRET, "alice"));
    let cases = [
        (&session("stored-token"), "stored-token", true),
        (&session("stored-token"), "stored-tokem", false),
        (&session("stored-token"), "stored-token-and-more", false),
        (&session("stored-token"), "", false),
        (&long_term, "stored-token", false),
        (&long_term, "", false),
    ];
    for (entry, presented, expected) in cases {
        assert_eq!(
            entry.session_token_matches(presented),
            expected,
            "{presented:?}"
        );
    }
}

/// A scope date that is not eight bytes is a scope complaint, not a lookup.
#[tokio::test]
async fn a_malformed_scope_date_is_refused_before_the_lookup() {
    let inner = Arc::new(CountingCredStore::new());
    inner.insert("AKIATEST", make_cred(SECRET, "alice"));
    let store = cached(inner.clone());
    for date in ["", "2026041", "202604155"] {
        match store.lookup("AKIATEST", date).await {
            Err(DynamoDbError::InvalidSignatureException(msg)) => {
                assert_eq!(msg, SCOPE_DATE_COMPLAINT, "{date:?}");
            }
            other => panic!("{date:?}: expected InvalidSignatureException, got {other:?}"),
        }
    }
    assert_eq!(inner.calls(), 0);
}

const FULL_STORE_CHILD: &str = "EXTENDDB_TEST_FULL_STORE_CHILD";

/// A store too small for the live entries: the miss derives into a
/// temporary buffer and caches no key, and when every buffer is in use the
/// request fails with `InternalServerError`. Runs in a child process, which
/// owns the process-wide store.
#[test]
fn a_full_store_uses_the_temporary_buffer_then_refuses() {
    let output = run_test_in_child(
        "credential_cache::tests::full_store_child_process",
        FULL_STORE_CHILD,
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_store_child_process() {
    if !is_child(FULL_STORE_CHILD) {
        return;
    }
    assert_eq!(secmem::init_unlocked(4), secmem::Protection::Unlocked);
    let inner = Arc::new(CountingCredStore::new());
    inner.insert("AKIABOB", make_cred(SECRET, "bob"));
    // The cache reserves its two buffers; the test holds every other slot.
    let store = CachedCredentialStore::with_arc(inner.clone(), fast_config(), REGION, 2);
    let mut held = Vec::new();
    while let Ok(slot) = secmem::KeySlot::new() {
        held.push(slot);
    }
    assert_eq!(held.len(), 2);
    let date = today();
    let expected = derive_signing_key(SECRET, &date, REGION, "dynamodb");

    // No free slot: the key is derived into a buffer, and is correct. The
    // load caches the fields only, so this first lookup decrypts twice: once
    // in the loader and once for the key.
    let bob = store.lookup("AKIABOB", &date).await.unwrap().unwrap();
    assert_eq!(bob.signing_key(), &*expected);
    assert_eq!(inner.calls(), 2);
    // No key was cached: the next lookup decrypts again, into the second
    // buffer.
    let bob_again = store.lookup("AKIABOB", &date).await.unwrap().unwrap();
    assert_eq!(inner.calls(), 3);

    // Both buffers are on loan: the next miss fails closed. The buffer is
    // taken after the read, so the refused request has decrypted once.
    match store.lookup("AKIABOB", &date).await {
        Err(DynamoDbError::InternalServerError(msg)) => {
            assert_eq!(msg, AUTH_INTERNAL_ERROR);
        }
        other => panic!("expected InternalServerError, got {other:?}"),
    }
    assert_eq!(inner.calls(), 4);

    // A returned buffer serves the next request.
    drop(bob_again);
    let third = store.lookup("AKIABOB", &date).await.unwrap().unwrap();
    assert_eq!(inner.calls(), 5);
    drop(third);
    drop(bob);

    // With a slot free again the key is cached, and the next lookup is a hit.
    held.pop();
    let cached = store.lookup("AKIABOB", &date).await.unwrap().unwrap();
    assert_eq!(cached.signing_key(), &*expected);
    assert_eq!(inner.calls(), 6);
    let _ = store.lookup("AKIABOB", &date).await.unwrap().unwrap();
    assert_eq!(inner.calls(), 6, "the cached key is a hit");
}

const ONE_THREAD_CHILD: &str = "EXTENDDB_TEST_ONE_THREAD_CHILD";

/// One worker thread, one temporary buffer and no free slot: two requests
/// that wait on the credential read at the same time both succeed, because
/// each takes the buffer after its read and gives it back before its task
/// yields again. Runs in a child process, which owns the process-wide store.
#[test]
fn one_buffer_serves_two_requests_that_wait_on_the_read_at_once() {
    let output = run_test_in_child(
        "credential_cache::tests::one_thread_child_process",
        ONE_THREAD_CHILD,
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A credential store whose read waits, as a database round trip does, so
/// the task yields its worker thread during the read.
struct SlowCredStore(CountingCredStore);

#[async_trait::async_trait]
impl CredentialStore for SlowCredStore {
    async fn lookup_credential(
        &self,
        access_key_id: &str,
    ) -> Result<Option<StoredCredential>, DynamoDbError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.0.lookup_credential(access_key_id).await
    }
}

/// One request: the lookup, then a use of the key that ends before the task
/// yields again, as `authenticate` uses it.
async fn key_matches(
    store: &CachedCredentialStore,
    date: &str,
    expected: &[u8; 32],
) -> Result<Option<bool>, DynamoDbError> {
    store
        .lookup("AKIABOB", date)
        .await
        .map(|resolved| resolved.map(|r| r.signing_key() == expected))
}

#[tokio::test(flavor = "current_thread")]
async fn one_thread_child_process() {
    if !is_child(ONE_THREAD_CHILD) {
        return;
    }
    assert_eq!(secmem::init_unlocked(1), secmem::Protection::Unlocked);
    let inner = CountingCredStore::new();
    inner.insert("AKIABOB", make_cred(SECRET, "bob"));
    let config = SwrCacheConfig {
        ttl: Duration::from_secs(60),
        soft_ttl: Duration::from_secs(30),
        negative_ttl: Duration::from_secs(5),
        max_entries: 100,
        name: "test-cred",
    };
    // One worker thread: the store's one slot becomes the one buffer.
    let store = CachedCredentialStore::with_arc(Arc::new(SlowCredStore(inner)), config, REGION, 1);
    assert!(
        secmem::KeySlot::new().is_err(),
        "the store has no free slot"
    );
    let date = today();
    let expected = derive_signing_key(SECRET, &date, REGION, "dynamodb");

    assert_eq!(
        key_matches(&store, &date, &expected).await.unwrap(),
        Some(true)
    );
    let (a, b) = tokio::join!(
        key_matches(&store, &date, &expected),
        key_matches(&store, &date, &expected)
    );
    assert_eq!(a.unwrap(), Some(true));
    assert_eq!(b.unwrap(), Some(true));
}

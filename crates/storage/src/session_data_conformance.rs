// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend-independent check that a role session's data is read by the
//! session's access key, run by each backend's own test suite against a real
//! database.
//!
//! `AssumeRole` stores a new session, with a new access key, for every call,
//! so a role can hold several sessions with one session name. The check
//! stores four sessions of one role, each with a session policy and session
//! tags of its own:
//!
//! - under one session name, an expired session, then a live one;
//! - under another session name, two live sessions.
//!
//! Before it stores them, it checks that its sessions are these two pairs
//! and that no two of them share a policy or tags. It then reads each
//! session's data with
//! [`AuthorizationStore::fetch_session_data`](crate::authorization_store::AuthorizationStore::fetch_session_data)
//! by the session's access key, and checks that each read returns that
//! session's own policy and tags, and an issue time (`created_at`) inside the
//! window of the session's `store_session` call: from `CLOCK_MARGIN` before
//! the call started, rounded down to the whole second (the Cassandra store
//! reads an issue time in whole seconds), to `CLOCK_MARGIN` after the call
//! returned. Before it reads, it checks that the later session of each name
//! started at least `BETWEEN_SESSIONS` after the earlier one's call returned:
//! without that pause, a store that answers the time of the read, or the
//! issue time of the other session of the name, could pass. A key no session
//! holds must read `None`. The expired session's key reads its own data too:
//! authentication refuses the key, and the read does not check the expiry
//! again. A store that deletes expired sessions in the background
//! ([`Expectations::deletes_expired_sessions`]) may read `None` for it
//! instead, so on such a store the check does not show that an expired
//! session's data is read.
//!
//! The PostgreSQL and Cassandra stores take the issue time from the
//! database's clock; the margin lets that clock run up to `CLOCK_MARGIN`
//! ahead of or behind the clock of the host that runs the check.
//!
//! The SQLite and PostgreSQL stores write no session without an encryption
//! key, so their tests give the store one. The check stores a placeholder for
//! each session's encrypted secret, which it never decrypts.

use std::time::Duration;

use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::CatalogStore;
use crate::authorization_store::SessionData;
use crate::iam_leftovers_conformance::{allow, trust};

/// What the backend under test does beyond the common contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Expectations {
    /// The store deletes an expired session in the background, as a TTL index
    /// on `iam_sessions.expires_at` does, so the expired session's key may read
    /// `None`.
    pub deletes_expired_sessions: bool,
}

const ROLE: &str = "SessionDataRole";

/// Stands in for each session's encrypted secret; the check never decrypts it.
const PLACEHOLDER_SECRET: &[u8] = b"placeholder for an encrypted secret";

/// How far the database's clock may run ahead of or behind the clock of the
/// host that runs the check, for a store that takes a session's issue time
/// from the database's clock (see the module doc). Each side of a session's
/// [`Window`] extends this far past the session's store call.
const CLOCK_MARGIN: Duration = Duration::from_millis(100);

/// Two sessions stored under one session name are at least this far apart: a
/// second and twice [`CLOCK_MARGIN`]. [`check_pauses`] checks that the later
/// session of each name started at least this long after the earlier one's
/// `store_session` call returned. The earlier session of each name is then
/// read at least this long after its call returned, so a store that answers
/// the time of the read for the issue time fails the check, even one that
/// reads an issue time in whole seconds, as the Cassandra store does, on a
/// clock up to [`CLOCK_MARGIN`] behind. So does a store that answers the
/// issue time of the other session of the name.
const BETWEEN_SESSIONS: Duration = Duration::from_secs(1)
    .saturating_add(CLOCK_MARGIN)
    .saturating_add(CLOCK_MARGIN);

/// One session the check stores, under session name `name`. Its session
/// policy allows `action` only and its one session tag holds its access key,
/// so each session's data differs from every other's. [`check_own_data`]
/// compares a read with [`Session::policy`] and [`Session::tags`].
struct Session {
    access_key_id: String,
    name: &'static str,
    expires_at: OffsetDateTime,
    action: &'static str,
}

impl Session {
    fn policy(&self) -> Value {
        allow(self.action)
    }

    /// Whether the session has expired at `now`.
    fn expired(&self, now: OffsetDateTime) -> bool {
        self.expires_at < now
    }

    /// The session's tags, as [`SessionData::session_tags`] holds them.
    fn tags(&self) -> Vec<(String, String)> {
        vec![("session".to_owned(), self.access_key_id.clone())]
    }

    /// [`Session::tags`] as `store_session` takes them.
    fn stored_tags(&self) -> Value {
        self.tags()
            .into_iter()
            .map(|(key, value)| json!({"Key": key, "Value": value}))
            .collect()
    }
}

/// The access key the check gives the session `label` in `account_id`. The
/// account id is in each key, so two runs in one database (as the Cassandra
/// suite's runs share a keyspace) store different keys.
fn key(account_id: &str, label: &str) -> String {
    format!("ASIA{account_id}{label}")
}

/// The four sessions, in the order the check stores them: under one name an
/// expired session and then a live one, under another two live sessions.
fn sessions(account_id: &str, now: OffsetDateTime) -> [Session; 4] {
    // AssumeRole stores whole seconds.
    let whole = now.replace_nanosecond(0).unwrap_or(now);
    let session = |label, name, expires_in, action| Session {
        access_key_id: key(account_id, label),
        name,
        expires_at: whole + expires_in,
        action,
    };
    let hour = time::Duration::hours(1);
    [
        session("EXPIRED", "reused-after-expiry", -hour, "dynamodb:GetItem"),
        session("LIVE", "reused-after-expiry", hour, "dynamodb:PutItem"),
        session("FIRST", "reused-while-live", hour, "dynamodb:Query"),
        session("SECOND", "reused-while-live", hour, "dynamodb:Scan"),
    ]
}

/// Run the check in `account_id`, which must not exist yet, and delete the
/// account at the end.
///
/// # Panics
///
/// When a store call fails, when a read does not return the session's own
/// policy and tags with an issue time inside the window of its store call
/// (on a store that deletes expired sessions, the expired session may read
/// `None`), or when a key no session holds reads data. Before any read, also
/// when the check's own sessions are not the two pairs of the module doc
/// (`check_shape`), when two of them share a session policy or session tags
/// (`check_distinct`), or when two of one name were stored less than
/// `BETWEEN_SESSIONS` apart (`check_pauses`).
pub async fn run(store: &dyn CatalogStore, account_id: &str, expect: Expectations) {
    store
        .create_account(account_id, "session-data")
        .await
        .unwrap_or_else(|e| panic!("create the account: {e:?}"));
    store
        .create_role(account_id, ROLE, &trust(account_id))
        .await
        .unwrap_or_else(|e| panic!("create the role: {e:?}"));

    let now = OffsetDateTime::now_utc();
    let sessions = sessions(account_id, now);
    check_shape(&sessions, now);
    check_distinct(&sessions);
    let calls = store_sessions(store, account_id, &sessions).await;
    check_pauses(&sessions, &calls);
    check_reads(store, &sessions, &calls, now, expect).await;

    let unknown = key(account_id, "UNKNOWN");
    assert!(
        store
            .fetch_session_data(&unknown)
            .await
            .unwrap_or_else(|e| panic!("fetch the data of key {unknown}: {e:?}"))
            .is_none(),
        "a key no session holds reads session data"
    );

    store
        .delete_account(account_id)
        .await
        .unwrap_or_else(|e| panic!("delete the account: {e:?}"));
}

/// Check that `sessions` are the two pairs of the module doc: grouped by
/// session name, in the order they are stored, one name holds a session
/// expired at `now` and then a live one, and the other two live sessions.
/// [`check_pauses`] pairs sessions by name, and [`check_reads`] reads the
/// expired session: with no two sessions of one name, a store that reads by
/// session name could pass, and with no expired session, one that leaves out
/// expired sessions.
fn check_shape(sessions: &[Session], now: OffsetDateTime) {
    let mut names: Vec<&str> = Vec::new();
    for session in sessions {
        if !names.contains(&session.name) {
            names.push(session.name);
        }
    }
    let mut by_name: Vec<Vec<&str>> = names
        .iter()
        .map(|name| {
            sessions
                .iter()
                .filter(|session| session.name == *name)
                .map(|session| {
                    if session.expired(now) {
                        "expired"
                    } else {
                        "live"
                    }
                })
                .collect()
        })
        .collect();
    by_name.sort_unstable();
    assert_eq!(
        by_name,
        [vec!["expired", "live"], vec!["live", "live"]],
        "each session name's sessions, in the order they are stored, expired or live at {now}"
    );
}

/// Check that no two sessions share a session policy or session tags, so that
/// a read of another session's data cannot pass [`check_own_data`] for the
/// session's own.
fn check_distinct(sessions: &[Session]) {
    for (n, a) in sessions.iter().enumerate() {
        for b in &sessions[n + 1..] {
            let (a_key, b_key) = (&a.access_key_id, &b.access_key_id);
            assert_ne!(
                a.policy(),
                b.policy(),
                "sessions {a_key} and {b_key} have one session policy"
            );
            assert_ne!(
                a.tags(),
                b.tags(),
                "sessions {a_key} and {b_key} have the same session tags"
            );
        }
    }
}

/// When one session's `store_session` call started and when it returned.
struct Call {
    started: OffsetDateTime,
    returned: OffsetDateTime,
}

/// The times between which one session's issue time must fall: from
/// [`CLOCK_MARGIN`] before its `store_session` call started, rounded down to
/// the whole second, to [`CLOCK_MARGIN`] after the call returned.
#[derive(Debug)]
struct Window {
    from: OffsetDateTime,
    until: OffsetDateTime,
}

impl Window {
    /// The window of a store call that started at `started` and returned at
    /// `returned`. The margin comes off before the rounding: a whole-second
    /// issue time from a clock behind the host's can fall in the second
    /// before the one in which the call started.
    fn around(started: OffsetDateTime, returned: OffsetDateTime) -> Self {
        let earliest = started - CLOCK_MARGIN;
        Self {
            // The Cassandra store reads an issue time in whole seconds.
            from: earliest.replace_nanosecond(0).unwrap_or(earliest),
            until: returned + CLOCK_MARGIN,
        }
    }

    fn holds(&self, issued: OffsetDateTime) -> bool {
        self.from <= issued && issued <= self.until
    }
}

/// Store `sessions` in order, at least [`BETWEEN_SESSIONS`] apart where two
/// of them in a row share a session name, and return each store [`Call`].
/// The pause between two such sessions is [`CLOCK_MARGIN`] longer than
/// [`BETWEEN_SESSIONS`]. `tokio::time::sleep` times it on the monotonic clock,
/// and [`check_pauses`] measures it on the wall clock, which a time service
/// can set back while the pause runs; the extra [`CLOCK_MARGIN`] lets the
/// wall clock go back that far without failing a correct store.
async fn store_sessions(
    store: &dyn CatalogStore,
    account_id: &str,
    sessions: &[Session],
) -> Vec<Call> {
    let mut calls = Vec::with_capacity(sessions.len());
    for (n, session) in sessions.iter().enumerate() {
        if n > 0 && sessions[n - 1].name == session.name {
            tokio::time::sleep(BETWEEN_SESSIONS + CLOCK_MARGIN).await;
        }
        let started = OffsetDateTime::now_utc();
        store
            .store_session(
                &format!("{}-token", session.access_key_id),
                &session.access_key_id,
                PLACEHOLDER_SECRET,
                account_id,
                ROLE,
                session.name,
                &Some(session.stored_tags()),
                &Some(session.policy()),
                session.expires_at,
            )
            .await
            .unwrap_or_else(|e| panic!("store session {}: {e:?}", session.access_key_id));
        calls.push(Call {
            started,
            returned: OffsetDateTime::now_utc(),
        });
    }
    calls
}

/// Check that the later session of each session name started at least
/// [`BETWEEN_SESSIONS`] after the earlier one's store call returned, which
/// the issue-time checks of [`check_reads`] rely on (see
/// [`BETWEEN_SESSIONS`]). It pairs sessions by name, not by their place in
/// `sessions`, so a pause that [`store_sessions`] leaves out, or one that a
/// new order of the sessions skips, fails here on a correct store.
fn check_pauses(sessions: &[Session], calls: &[Call]) {
    for (n, (earlier, earlier_call)) in sessions.iter().zip(calls).enumerate() {
        for (later, later_call) in sessions[n + 1..].iter().zip(&calls[n + 1..]) {
            if later.name == earlier.name {
                assert!(
                    later_call.started >= earlier_call.returned + BETWEEN_SESSIONS,
                    "the store call of session {} started at {}, less than {BETWEEN_SESSIONS:?} \
                     after that of session {}, of the same name, returned at {}",
                    later.access_key_id,
                    later_call.started,
                    earlier.access_key_id,
                    earlier_call.returned
                );
            }
        }
    }
}

/// Read each session's data by its access key and check that it is the
/// session's own, with an issue time inside the [`Window`] of the session's
/// store call. `now` is the time the sessions' expiry was set from.
async fn check_reads(
    store: &dyn CatalogStore,
    sessions: &[Session],
    calls: &[Call],
    now: OffsetDateTime,
    expect: Expectations,
) {
    assert_eq!(
        calls.len(),
        sessions.len(),
        "every session has the times of its store call"
    );
    for (session, call) in sessions.iter().zip(calls) {
        let window = Window::around(call.started, call.returned);
        let label = &session.access_key_id;
        let read = store
            .fetch_session_data(label)
            .await
            .unwrap_or_else(|e| panic!("fetch the data of session {label}: {e:?}"));
        let Some(data) = read else {
            assert!(
                session.expired(now) && expect.deletes_expired_sessions,
                "session {label} reads no data"
            );
            continue;
        };
        let issued = check_own_data(session, &data);
        assert!(
            window.holds(issued),
            "session {label} reads issue time {issued}, outside the {} to {} of its store call",
            window.from,
            window.until
        );
    }
}

/// Check that `data` is `session`'s own policy and tags, and return its issue
/// time.
fn check_own_data(session: &Session, data: &SessionData) -> OffsetDateTime {
    let label = &session.access_key_id;
    let policy: Option<Value> = data.session_policy.as_deref().map(|text| {
        serde_json::from_str(text)
            .unwrap_or_else(|e| panic!("session {label}'s policy {text:?} is not JSON: {e}"))
    });
    assert_eq!(
        policy,
        Some(session.policy()),
        "session {label} reads another session policy"
    );
    assert_eq!(
        data.session_tags,
        session.tags(),
        "session {label} reads other session tags"
    );
    let created_at = data
        .created_at
        .as_deref()
        .unwrap_or_else(|| panic!("session {label} reads no issue time"));
    OffsetDateTime::parse(created_at, &Rfc3339)
        .unwrap_or_else(|e| panic!("session {label}'s issue time {created_at:?}: {e}"))
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;

    use super::{BETWEEN_SESSIONS, Window};

    /// Store calls of 5 ms that start at each millisecond of one second, as
    /// `(started, returned)`.
    fn calls() -> impl Iterator<Item = (OffsetDateTime, OffsetDateTime)> {
        let second = OffsetDateTime::from_unix_timestamp(1_790_000_000).expect("a time");
        (0..1000).map(move |ms| {
            let started = second + time::Duration::milliseconds(ms);
            (started, started + time::Duration::milliseconds(5))
        })
    }

    /// `t` on a clock `offset_ms` ahead of the host's (behind when negative),
    /// to the millisecond and in whole seconds.
    fn on_clock(t: OffsetDateTime, offset_ms: i64) -> [OffsetDateTime; 2] {
        let t = t + time::Duration::milliseconds(offset_ms);
        [t, t.replace_nanosecond(0).unwrap_or(t)]
    }

    /// An issue time taken when the call started or when it returned (and so
    /// any time between), on a clock up to 100 ms ahead or behind, to the
    /// millisecond or in whole seconds, falls in the call's window. Among the
    /// cases: a call that starts 20 ms into a second, whose whole-second issue
    /// time from a clock 50 ms behind is in the second before, which a window
    /// that rounds down before it takes off the margin would refuse.
    #[test]
    fn the_window_holds_an_issue_time_from_a_clock_up_to_100_ms_off() {
        for (started, returned) in calls() {
            let window = Window::around(started, returned);
            for offset in -100..=100 {
                for during in [started, returned] {
                    for issued in on_clock(during, offset) {
                        assert!(
                            window.holds(issued),
                            "{issued} from a call of {started} to {returned}, outside {window:?}"
                        );
                    }
                }
            }
        }
    }

    /// An issue time from a clock 101 ms ahead, taken when the call returned,
    /// falls outside the call's window. So does one from a clock 101 ms
    /// behind, taken when the call started, to the millisecond or in whole
    /// seconds, for a call that starts 100 ms into a second: 100 ms before
    /// that call started is a whole second, where its window starts.
    #[test]
    fn the_window_refuses_an_issue_time_from_a_clock_101_ms_off() {
        for (started, returned) in calls() {
            let window = Window::around(started, returned);
            let issued = returned + time::Duration::milliseconds(101);
            assert!(
                !window.holds(issued),
                "{issued} from a call of {started} to {returned}, inside {window:?}"
            );
        }
        let (started, returned) = calls()
            .nth(100)
            .expect("a call that starts 100 ms into a second");
        let window = Window::around(started, returned);
        for issued in on_clock(started, -101) {
            assert!(
                !window.holds(issued),
                "{issued} from a call of {started} to {returned}, inside {window:?}"
            );
        }
    }

    /// An issue time taken [`BETWEEN_SESSIONS`] after the call returned, or
    /// that long before it started, on a clock up to 100 ms ahead or behind,
    /// to the millisecond or in whole seconds, falls outside the call's
    /// window.
    #[test]
    fn the_window_refuses_an_issue_time_a_pause_away() {
        let pause = time::Duration::try_from(BETWEEN_SESSIONS).expect("a pause of a second or two");
        for (started, returned) in calls() {
            let window = Window::around(started, returned);
            for offset in -100..=100 {
                for issued in on_clock(returned + pause, offset)
                    .into_iter()
                    .chain(on_clock(started - pause, offset))
                {
                    assert!(
                        !window.holds(issued),
                        "{issued} from a call of {started} to {returned}, inside {window:?}"
                    );
                }
            }
        }
    }
}

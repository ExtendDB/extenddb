// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How long requests wait on a read of the setting that does not end, and
//! how many reads such a read lets the server send.

use std::time::Duration;

use tokio::time::Instant;

use super::*;

/// A read that does not end is the only read sent, also across changes
/// through the management API, so it holds one catalog connection.
#[tokio::test(start_paused = true)]
async fn changes_while_a_read_does_not_end_send_no_more_reads() {
    let store = Store::new(Answer::Hang);
    let settings = Settings::new(store.clone());
    assert!(is_unavailable(&settings.enabled_accounts().await));
    for _ in 0..3 {
        settings.changed();
        assert!(is_unavailable(&settings.enabled_accounts().await));
    }
    assert_eq!(store.reads(), 1, "reads sent while the first had not ended");
}

/// Before any read has answered, only the first request waits on a read
/// that does not end. Requests sent later, after each refresh interval and
/// after a change, answer at once.
#[tokio::test(start_paused = true)]
async fn a_read_that_does_not_end_delays_only_the_first_request() {
    let store = Store::new(Answer::Hang);
    let settings = Settings::new(store.clone());
    let started = Instant::now();
    assert!(is_unavailable(&settings.enabled_accounts().await));
    assert_eq!(started.elapsed(), READ_TIMEOUT);
    for round in 0..12 {
        if round == 3 {
            settings.changed();
        }
        tokio::time::advance(REFRESH).await;
        let started = Instant::now();
        // the test's own deadline, so a request that waits again fails here
        let answer = tokio::time::timeout(READ_TIMEOUT / 2, settings.enabled_accounts())
            .await
            .expect("the request does not wait on the read again");
        assert!(is_unavailable(&answer));
        assert_eq!(started.elapsed(), Duration::ZERO, "round {round}");
    }
    assert_eq!(store.reads(), 1);
}

/// A request that arrives while another waits on the first read waits only
/// until READ_TIMEOUT after that read was sent.
#[tokio::test(start_paused = true)]
async fn a_second_request_waits_only_for_the_rest_of_the_read_timeout() {
    let store = Store::new(Answer::Hang);
    let settings = Arc::new(Settings::new(store.clone()));
    let first = {
        let settings = Arc::clone(&settings);
        tokio::spawn(async move { settings.enabled_accounts().await })
    };
    while store.reads() < 1 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(READ_TIMEOUT / 2).await;
    let started = Instant::now();
    // the test's own deadline, so a request that waits a full READ_TIMEOUT
    // of its own fails here
    let answer = tokio::time::timeout(READ_TIMEOUT, settings.enabled_accounts())
        .await
        .expect("the wait ends with the first read's timeout");
    assert!(is_unavailable(&answer));
    assert_eq!(started.elapsed(), READ_TIMEOUT / 2);
    assert!(is_unavailable(&first.await.unwrap()));
    assert_eq!(store.reads(), 1);
}

/// A request that finds a read in flight that the request which sent it
/// stopped waiting on waits only for the rest of READ_TIMEOUT.
#[tokio::test(start_paused = true)]
async fn a_request_after_a_dropped_wait_waits_only_for_the_rest() {
    let store = Store::new(Answer::Hang);
    let settings = Arc::new(Settings::new(store.clone()));
    let first = {
        let settings = Arc::clone(&settings);
        tokio::spawn(async move { settings.enabled_accounts().await })
    };
    while store.reads() < 1 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(READ_TIMEOUT / 4).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let started = Instant::now();
    // the test's own deadline, so a request that waits a full READ_TIMEOUT
    // of its own fails here
    let answer = tokio::time::timeout(READ_TIMEOUT, settings.enabled_accounts())
        .await
        .expect("the wait ends with the read's timeout");
    assert!(is_unavailable(&answer));
    assert_eq!(started.elapsed(), READ_TIMEOUT * 3 / 4);
    assert_eq!(store.reads(), 1);
}

/// The admin guide and the upgrade note state the two intervals: a read is
/// kept for one second, and a read is waited on for at most two.
#[tokio::test(start_paused = true)]
async fn a_read_is_kept_for_one_second_and_waited_on_for_two() {
    let store = Store::value("all");
    let settings = Settings::new(store.clone());
    settings.enabled_accounts().await.unwrap();
    tokio::time::advance(Duration::from_millis(999)).await;
    settings.enabled_accounts().await.unwrap();
    assert_eq!(store.reads(), 1, "the read was not kept for one second");
    tokio::time::advance(Duration::from_millis(1)).await;
    settings.enabled_accounts().await.unwrap();
    assert_eq!(store.reads(), 2, "the read was kept past one second");

    let settings = Settings::new(Store::new(Answer::Hang));
    let started = Instant::now();
    // the test's own deadline, past the documented two seconds
    let answer = tokio::time::timeout(Duration::from_secs(3), settings.enabled_accounts())
        .await
        .expect("the read is waited on for at most two seconds");
    assert!(is_unavailable(&answer));
    assert_eq!(started.elapsed(), Duration::from_secs(2));
}

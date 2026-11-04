// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The start of the API request that a storage call serves.
//!
//! A backend that stops retrying a contended write after a deadline needs to
//! know when the request began. A caller that makes several storage calls for
//! one request runs them inside [`scope`], and the backend measures the
//! deadline from the instant the scope carries, so the calls share one
//! deadline instead of each getting a deadline of its own. `BatchWriteItem`
//! does this for its write requests. Outside a scope [`started`] is `None`
//! and the backend measures from the start of each call, which is what a
//! single-item request and a background worker get.

use std::future::Future;
use std::time::Instant;

tokio::task_local! {
    static REQUEST_STARTED: Instant;
}

/// Run `f` with `started` as the start of the request its storage calls
/// serve.
pub async fn scope<F: Future>(started: Instant, f: F) -> F::Output {
    REQUEST_STARTED.scope(started, f).await
}

/// The start of the request the running storage call serves, when its caller
/// set one with [`scope`].
#[must_use]
pub fn started() -> Option<Instant> {
    REQUEST_STARTED.try_with(|started| *started).ok()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn a_call_sees_the_start_of_the_scope_it_runs_in() {
        assert_eq!(started(), None, "no scope, no request start");
        let earlier = Instant::now()
            .checked_sub(Duration::from_secs(5))
            .expect("the process clock reaches five seconds back");
        let seen = scope(earlier, async {
            // A storage call awaited inside the scope sees the same start.
            let inner = async { started() }.await;
            (started(), inner)
        })
        .await;
        assert_eq!(seen, (Some(earlier), Some(earlier)));
        assert_eq!(started(), None, "the start does not outlive its scope");
    }

    #[tokio::test]
    async fn an_inner_scope_replaces_the_start_until_it_ends() {
        let outer = Instant::now();
        let inner = outer + Duration::from_secs(1);
        let seen = scope(outer, async {
            let in_inner = scope(inner, async { started() }).await;
            (in_inner, started())
        })
        .await;
        assert_eq!(seen, (Some(inner), Some(outer)));
    }
}

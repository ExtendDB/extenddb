// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request-scoped correlation data for storage backends and the engine.
//!
//! The server sets a [`RequestScope`] from the start of authentication to the
//! end of dispatch for each client request. A storage backend or an engine
//! error mapper running inside that span reads it with [`current`] or
//! [`with_current`] to put the request id the client received in
//! `x-amzn-RequestId` on its own log lines.
//!
//! The scope is a Tokio task-local. It is visible to every `.await` reached
//! inline on the request's task, including nested and boxed futures. A task
//! started with `tokio::spawn` or `spawn_blocking` does not inherit it, so
//! [`current`] returns `None` there, as it does in queue workers, in tests and
//! in any task running outside a request. Code that spawns a task on behalf of
//! a request and wants the task's log lines correlated reads [`current`]
//! before the spawn and re-enters the scope inside it with
//! [`with_scope_opt`]. The engine makes its storage calls inline on the
//! request task, so a backend serving the request sees the scope.
//!
//! The two SDK fields are header values the client chose. Treat them as
//! untrusted text: write them to a log line if useful, never into a query, a
//! key or a file name.

/// Correlation data for the client request a task is serving.
///
/// Construct it with [`RequestScope::new`] and [`RequestScope::with_sdk_headers`];
/// the struct may gain fields in a minor release.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RequestScope {
    /// The server's request id, the value returned to the client in
    /// `x-amzn-RequestId`.
    pub request_id: String,
    /// The DynamoDB operation name from `X-Amz-Target`, for example `PutItem`.
    pub operation: String,
    /// `amz-sdk-invocation-id` when the client sent it. The AWS SDKs set one
    /// value per call and resend it on each retry of that call.
    pub sdk_invocation_id: Option<String>,
    /// `amz-sdk-request` when the client sent it. The AWS SDKs send their
    /// attempt counter here, in the form `attempt=2; max=4`.
    pub sdk_request: Option<String>,
}

impl RequestScope {
    /// A scope for `request_id` and `operation` with no SDK headers.
    #[must_use]
    pub fn new(request_id: String, operation: String) -> Self {
        Self {
            request_id,
            operation,
            sdk_invocation_id: None,
            sdk_request: None,
        }
    }

    /// The same scope with the two SDK correlation headers set.
    #[must_use]
    pub fn with_sdk_headers(
        mut self,
        sdk_invocation_id: Option<String>,
        sdk_request: Option<String>,
    ) -> Self {
        self.sdk_invocation_id = sdk_invocation_id;
        self.sdk_request = sdk_request;
        self
    }
}

tokio::task_local! {
    static SCOPE: RequestScope;
}

/// Run `fut` with `scope` visible to every [`current`] and [`with_current`]
/// call made from inside it on the same task.
pub async fn with_scope<F: std::future::Future>(scope: RequestScope, fut: F) -> F::Output {
    SCOPE.scope(scope, fut).await
}

/// Run `fut` inside `scope` when there is one, plainly when there is none.
/// For a task spawned on behalf of a request: read [`current`] before the
/// spawn and pass it here inside the task.
pub async fn with_scope_opt<F: std::future::Future>(
    scope: Option<RequestScope>,
    fut: F,
) -> F::Output {
    match scope {
        Some(scope) => SCOPE.scope(scope, fut).await,
        None => fut.await,
    }
}

/// The scope of the request this task is serving, cloned, or `None` outside a
/// request (workers, tests, spawned tasks).
pub fn current() -> Option<RequestScope> {
    SCOPE.try_with(RequestScope::clone).ok()
}

/// Borrow the scope without cloning and map it with `f`; `None` outside a
/// request.
pub fn with_current<R>(f: impl FnOnce(&RequestScope) -> R) -> Option<R> {
    SCOPE.try_with(f).ok()
}

/// The request id and operation of the current request, for a log line. Both
/// are `None` outside a request; `tracing` records nothing for a `None`, so a
/// line written from a worker carries neither field rather than a fabricated
/// value.
pub fn log_fields() -> (Option<String>, Option<String>) {
    with_current(|s| (s.request_id.clone(), s.operation.clone())).unzip()
}

#[cfg(test)]
mod tests {
    use super::{RequestScope, current, log_fields, with_current, with_scope, with_scope_opt};

    fn scope(request_id: &str) -> RequestScope {
        RequestScope::new(request_id.to_owned(), "PutItem".to_owned())
            .with_sdk_headers(Some("inv-1".to_owned()), None)
    }

    #[tokio::test]
    async fn current_is_none_outside_a_scope() {
        assert_eq!(current(), None);
        assert_eq!(with_current(|s| s.request_id.clone()), None);
        assert_eq!(log_fields(), (None, None));
    }

    #[tokio::test]
    async fn current_is_some_and_equal_inside_a_scope() {
        let expected = scope("req-eq");
        let seen = with_scope(expected.clone(), async { current() }).await;
        assert_eq!(seen, Some(expected));
    }

    #[tokio::test]
    async fn the_constructors_set_the_fields_they_name() {
        let plain = RequestScope::new("req-c".to_owned(), "Query".to_owned());
        assert_eq!(plain.request_id, "req-c");
        assert_eq!(plain.operation, "Query");
        assert_eq!(plain.sdk_invocation_id, None);
        assert_eq!(plain.sdk_request, None);
        let full =
            plain.with_sdk_headers(Some("inv".to_owned()), Some("attempt=1; max=3".to_owned()));
        assert_eq!(full.sdk_invocation_id.as_deref(), Some("inv"));
        assert_eq!(full.sdk_request.as_deref(), Some("attempt=1; max=3"));
    }

    #[tokio::test]
    async fn log_fields_carry_the_id_and_operation_inside_a_scope() {
        let seen = with_scope(scope("req-log"), async { log_fields() }).await;
        assert_eq!(
            seen,
            (Some("req-log".to_owned()), Some("PutItem".to_owned()))
        );
    }

    #[tokio::test]
    async fn visible_through_nested_awaits_and_a_boxed_future() {
        async fn inner() -> Option<String> {
            tokio::task::yield_now().await;
            with_current(|s| s.request_id.clone())
        }
        let boxed: std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>>>> =
            Box::pin(inner());
        let seen = with_scope(scope("req-nested"), boxed).await;
        assert_eq!(seen.as_deref(), Some("req-nested"));
    }

    #[tokio::test]
    async fn a_spawned_task_does_not_inherit_the_scope() {
        let seen = with_scope(scope("req-spawn"), async {
            tokio::spawn(async { current() })
                .await
                .expect("spawned task joins")
        })
        .await;
        assert_eq!(seen, None);
    }

    #[tokio::test]
    async fn a_spawned_task_re_enters_the_scope_it_was_handed() {
        let seen = with_scope(scope("req-carry"), async {
            let carried = current();
            tokio::spawn(with_scope_opt(carried, async { current() }))
                .await
                .expect("spawned task joins")
        })
        .await;
        assert_eq!(seen, Some(scope("req-carry")));
    }

    #[tokio::test]
    async fn with_scope_opt_without_a_scope_runs_plainly() {
        let seen = with_scope_opt(None, async { current() }).await;
        assert_eq!(seen, None);
    }

    #[tokio::test]
    async fn two_concurrent_scopes_each_see_their_own_value() {
        async fn observe(id: &str) -> String {
            // Yield between reads so the two futures interleave on the task.
            let first = with_current(|s| s.request_id.clone()).expect("scope present");
            tokio::task::yield_now().await;
            let second = with_current(|s| s.request_id.clone()).expect("scope present");
            assert_eq!(first, second, "scope stayed stable across the yield");
            assert_eq!(first, id);
            first
        }
        let a = with_scope(scope("req-a"), observe("req-a"));
        let b = with_scope(scope("req-b"), observe("req-b"));
        let (ra, rb) = tokio::join!(a, b);
        assert_eq!(ra, "req-a");
        assert_eq!(rb, "req-b");
        // Outside both scopes again.
        assert_eq!(current(), None);
    }
}

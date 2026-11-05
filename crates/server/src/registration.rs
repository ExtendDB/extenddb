// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What the server does when its backend reports the serving registration
//! lost ([`ServingRegistrar`]).
//!
//! A migration can start on the catalog once the registration is gone, so
//! from that moment the server admits no new request: each one gets
//! `ServiceUnavailable` (HTTP 503), which the AWS SDKs retry, and `/health`
//! answers the same. It also stops its background workers and the tasks
//! they started: their token fires, and the ones still running after 5 s are
//! aborted. What requests started goes on until it ends or the process
//! exits: the requests already running, and the work they left running in
//! the background, which is a `RestoreTableFromBackup`'s item copy on every
//! backend, an `UpdateTimeToLive` backfill on Cassandra and PostgreSQL, and
//! a GSI or vector index build on PostgreSQL. PostgreSQL's GSI propagation
//! workers, which the engine starts with itself, and MongoDB's startup pass
//! that rebuilds documents stored without their key (#473) write only the
//! data database, and go on too. The server then asks the backend to register
//! again at once and then every second, and when the backend reports a new
//! registration with the catalog's status unchanged, it starts a new set of
//! workers and serves again. When the backend refuses (a migration holds the
//! migration lock or waits for the servers to stop, or the status changed),
//! when 30 s pass without a registration, or when a worker could not be
//! stopped, the server asks for a stop: `serve` then stops the way a
//! shutdown signal stops it (the listener drains, letting requests already
//! running finish, the PID file is removed) and exits 1. A signal that came
//! first decides the exit status, also when a stop is asked for during its
//! drain.
//!
//! The server learns of a loss at its backend's next renewal, and serves as
//! before until then; a migration that takes the catalog in that time runs
//! beside it, as it would beside a server whose host stopped answering.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use extenddb_core::error::DynamoDbError;
use extenddb_storage::CancellationToken;
use extenddb_storage::hooks::{Reregistration, ServingRegistrar};

/// How long the server tries to register again before it stops: the bound
/// the Cassandra and MongoDB backends give a registration they cannot renew.
pub(crate) const WINDOW: Duration = Duration::from_secs(30);

/// The server's background workers, which the supervisor stops while the
/// server holds no registration and starts again once it holds a new one.
#[async_trait::async_trait]
pub(crate) trait WorkerControl: Send + Sync {
    /// Stop the workers that run now, and report whether every worker the
    /// server started has ended.
    async fn stop(&self) -> bool;

    /// Start a new set of workers.
    async fn start(&self);
}

/// The pause between two attempts to register again.
const RETRY: Duration = Duration::from_secs(1);

/// The message of the `ServiceUnavailable` a refused request gets.
const REFUSED: &str = "The server is registering again with its catalog. Retry the request.";

/// Whether the server admits new requests, and whether it must stop. Shared
/// by the request path, the supervisor and `serve`.
#[derive(Clone, Default)]
pub(crate) struct Admission {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    refusing: AtomicBool,
    stop: CancellationToken,
    reason: OnceLock<String>,
    /// A shutdown signal ended the server's wait before any stop was asked
    /// for.
    signalled: AtomicBool,
}

impl Admission {
    /// Whether new requests are refused now.
    pub(crate) fn refusing(&self) -> bool {
        self.inner.refusing.load(Ordering::Acquire)
    }

    fn refuse(&self) {
        self.inner.refusing.store(true, Ordering::Release);
    }

    fn admit(&self) {
        self.inner.refusing.store(false, Ordering::Release);
    }

    /// Ask `serve` to stop through its shutdown path, giving why. The first
    /// reason is kept.
    pub(crate) fn stop(&self, reason: String) {
        let _ = self.inner.reason.set(reason);
        self.inner.stop.cancel();
    }

    /// Resolves once a stop has been asked for.
    pub(crate) async fn stop_requested(&self) {
        self.inner.stop.cancelled().await;
    }

    /// Why a stop was asked for, or `None` when none was.
    pub(crate) fn stop_reason(&self) -> Option<&str> {
        self.inner.reason.get().map(String::as_str)
    }

    /// Record that a shutdown signal, not a stop, ended the server's wait.
    pub(crate) fn mark_signalled(&self) {
        self.inner.signalled.store(true, Ordering::Release);
    }

    /// Why the server stopped, when a stop it asked for ended its wait: a
    /// stop asked for while a signal's drain ran is not why it stopped.
    pub(crate) fn stopped_for(&self) -> Option<&str> {
        if self.inner.signalled.load(Ordering::Acquire) {
            return None;
        }
        self.stop_reason()
    }
}

/// The layer in front of every route: while the server is refusing, the
/// request is answered with `ServiceUnavailable` and does not reach its
/// handler.
pub(crate) async fn refuse_while_unregistered(
    State(admission): State<Admission>,
    request: Request,
    next: Next,
) -> Response {
    if admission.refusing() {
        let request_id = uuid::Uuid::new_v4().to_string();
        return crate::response::error_response(
            &DynamoDbError::ServiceUnavailable(REFUSED.to_owned()),
            &request_id,
        );
    }
    next.run(request).await
}

/// Watch the registration until `shutdown` fires: on each loss refuse new
/// requests, stop the workers and register again within [`WINDOW`], or ask
/// for a stop.
pub(crate) async fn supervise(
    registrar: Arc<dyn ServingRegistrar>,
    admission: Admission,
    shutdown: CancellationToken,
    workers: Arc<dyn WorkerControl>,
) {
    supervise_within(registrar, admission, shutdown, workers, WINDOW, RETRY).await;
}

/// How one loss ended.
enum Outcome {
    Registered { attempts: u32, after: Duration },
    Stop(String),
    Shutdown,
}

/// [`supervise`] with the window and the pause between attempts given, for
/// tests.
async fn supervise_within(
    registrar: Arc<dyn ServingRegistrar>,
    admission: Admission,
    shutdown: CancellationToken,
    workers: Arc<dyn WorkerControl>,
    window: Duration,
    retry: Duration,
) {
    loop {
        let reason = tokio::select! {
            () = shutdown.cancelled() => return,
            reason = registrar.lost() => reason,
        };
        admission.refuse();
        tracing::warn!(
            "{reason}; new requests get ServiceUnavailable and the background workers stop while \
             the server registers again, for up to {} s",
            window.as_secs()
        );
        // A migration can take the catalog once the lost registration
        // expires, so the workers stop now, before it can, and start again
        // only under a new registration.
        if !workers.stop().await {
            ask_for_stop(
                &admission,
                &shutdown,
                format!(
                    "the server stops: {reason}, and a background worker did not stop, so it does \
                     not register again"
                ),
            );
            return;
        }
        match register_again(registrar.as_ref(), &shutdown, window, retry).await {
            Outcome::Registered { attempts, after } => {
                workers.start().await;
                admission.admit();
                tracing::info!(
                    "registered again with the catalog after {} ms and {attempts} attempt(s), and \
                     its status is the one the server started with; the background workers run \
                     again and requests are served",
                    after.as_millis()
                );
            }
            Outcome::Stop(why) => {
                ask_for_stop(
                    &admission,
                    &shutdown,
                    format!("the server stops: {reason}, and it could not register again: {why}"),
                );
                return;
            }
            Outcome::Shutdown => return,
        }
    }
}

/// Log `message`, fire the shutdown token and ask `serve` to stop.
fn ask_for_stop(admission: &Admission, shutdown: &CancellationToken, message: String) {
    tracing::error!("{message}");
    shutdown.cancel();
    admission.stop(message);
}

/// Attempts until one registers, the backend refuses, `window` passes or
/// `shutdown` fires. Each attempt is bounded by the time left.
async fn register_again(
    registrar: &dyn ServingRegistrar,
    shutdown: &CancellationToken,
    window: Duration,
    retry: Duration,
) -> Outcome {
    let started = Instant::now();
    let mut attempts = 0;
    let mut last = String::new();
    loop {
        let left = window.saturating_sub(started.elapsed());
        if left.is_zero() {
            return Outcome::Stop(format!(
                "no attempt succeeded within {} s ({attempts} attempt(s); the last one: {last})",
                window.as_secs()
            ));
        }
        attempts += 1;
        let answer = tokio::select! {
            () = shutdown.cancelled() => return Outcome::Shutdown,
            answer = tokio::time::timeout(left, registrar.register_again()) => answer,
        };
        match answer {
            Ok(Reregistration::Registered) => {
                return Outcome::Registered {
                    attempts,
                    after: started.elapsed(),
                };
            }
            Ok(Reregistration::Refused(why)) => return Outcome::Stop(why),
            Ok(Reregistration::Unavailable(why)) => {
                tracing::warn!("attempt {attempts} to register again failed: {why}");
                last = why;
            }
            Err(_) => {
                last = format!("it did not finish within {} ms", left.as_millis());
            }
        }
        let pause = retry.min(window.saturating_sub(started.elapsed()));
        tokio::select! {
            () = shutdown.cancelled() => return Outcome::Shutdown,
            () = tokio::time::sleep(pause) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use extenddb_storage::CancellationToken;
    use extenddb_storage::hooks::{Reregistration, ServingRegistrar};
    use tokio::sync::Notify;
    use tower::ServiceExt;

    use super::{Admission, WorkerControl, refuse_while_unregistered, supervise_within};

    /// Workers that record when the supervisor stops and starts them, and
    /// whether the server was refusing requests then; `stops` says whether
    /// a stop ends them.
    struct Recorded {
        events: Mutex<Vec<(&'static str, bool)>>,
        admission: OnceLock<Admission>,
        stops: bool,
    }

    impl Recorded {
        fn new(stops: bool) -> Arc<Self> {
            Arc::new(Self {
                events: Mutex::new(Vec::new()),
                admission: OnceLock::new(),
                stops,
            })
        }

        fn record(&self, event: &'static str) {
            let refusing = self.admission.get().is_some_and(Admission::refusing);
            self.events.lock().unwrap().push((event, refusing));
        }

        fn events(&self) -> Vec<(&'static str, bool)> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WorkerControl for Recorded {
        async fn stop(&self) -> bool {
            self.record("stop");
            self.stops
        }

        async fn start(&self) {
            self.record("start");
        }
    }

    /// A registrar whose losses a test fires, and whose attempts answer from
    /// a script; an empty script answers `Unavailable`, and `None` in it is
    /// an attempt that never finishes. Each attempt records whether the
    /// server was refusing requests when it was made.
    #[derive(Default)]
    struct Scripted {
        lose: Notify,
        answers: Mutex<VecDeque<Option<Reregistration>>>,
        attempts: AtomicU32,
        admission: OnceLock<Admission>,
        refusing_at_attempts: Mutex<Vec<bool>>,
    }

    impl Scripted {
        fn answering(answers: Vec<Option<Reregistration>>) -> Arc<Self> {
            Arc::new(Self {
                answers: Mutex::new(answers.into()),
                ..Self::default()
            })
        }

        fn attempts(&self) -> u32 {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ServingRegistrar for Scripted {
        async fn lost(&self) -> String {
            self.lose.notified().await;
            "the session ended".to_owned()
        }

        async fn register_again(&self) -> Reregistration {
            if let Some(admission) = self.admission.get() {
                self.refusing_at_attempts
                    .lock()
                    .unwrap()
                    .push(admission.refusing());
            }
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let answer = self.answers.lock().unwrap().pop_front();
            match answer {
                Some(Some(answer)) => answer,
                Some(None) => std::future::pending().await,
                None => Reregistration::Unavailable("no answer".to_owned()),
            }
        }
    }

    async fn until(what: &str, mut done: impl FnMut() -> bool) {
        let started = Instant::now();
        while !done() {
            assert!(started.elapsed() < Duration::from_secs(10), "{what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn start(
        registrar: &Arc<Scripted>,
        window: Duration,
    ) -> (Admission, CancellationToken, tokio::task::JoinHandle<()>) {
        start_with(registrar, window, &Recorded::new(true))
    }

    fn start_with(
        registrar: &Arc<Scripted>,
        window: Duration,
        workers: &Arc<Recorded>,
    ) -> (Admission, CancellationToken, tokio::task::JoinHandle<()>) {
        let admission = Admission::default();
        let _ = registrar.admission.set(admission.clone());
        let _ = workers.admission.set(admission.clone());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(supervise_within(
            Arc::clone(registrar) as Arc<dyn ServingRegistrar>,
            admission.clone(),
            shutdown.clone(),
            Arc::clone(workers) as Arc<dyn WorkerControl>,
            window,
            Duration::from_millis(20),
        ));
        (admission, shutdown, task)
    }

    #[tokio::test]
    async fn a_lost_registration_refuses_requests_until_it_registers_again() {
        let registrar = Scripted::answering(vec![
            Some(Reregistration::Unavailable("connection refused".to_owned())),
            Some(Reregistration::Unavailable("connection refused".to_owned())),
            Some(Reregistration::Registered),
        ]);
        let workers = Recorded::new(true);
        let (admission, shutdown, task) = start_with(&registrar, Duration::from_secs(5), &workers);
        assert!(!admission.refusing());
        registrar.lose.notify_one();
        until("three attempts", || registrar.attempts() == 3).await;
        until("admitting once registered", || !admission.refusing()).await;
        assert_eq!(
            *registrar.refusing_at_attempts.lock().unwrap(),
            [true, true, true],
            "requests were admitted while the server held no registration"
        );
        // The workers stopped while new requests were refused, before the
        // first attempt, and started again under the new registration.
        assert_eq!(workers.events(), [("stop", true), ("start", true)]);
        assert_eq!(admission.stop_reason(), None);
        // A second loss is handled the same way.
        registrar
            .answers
            .lock()
            .unwrap()
            .push_back(Some(Reregistration::Registered));
        registrar.lose.notify_one();
        until("a fourth attempt", || registrar.attempts() == 4).await;
        until("admitting again", || !admission.refusing()).await;
        assert!(
            registrar.refusing_at_attempts.lock().unwrap()[3],
            "requests were admitted while the server held no registration"
        );
        assert_eq!(
            workers.events(),
            [
                ("stop", true),
                ("start", true),
                ("stop", true),
                ("start", true)
            ]
        );
        shutdown.cancel();
        task.await.unwrap();
        assert_eq!(admission.stop_reason(), None);
    }

    /// A worker that a stop could not end may still write when a migration
    /// takes the catalog, so the server stops instead of registering again.
    #[tokio::test]
    async fn a_worker_that_does_not_stop_stops_the_server_without_an_attempt() {
        let registrar = Scripted::answering(vec![Some(Reregistration::Registered)]);
        let workers = Recorded::new(false);
        let (admission, shutdown, task) = start_with(&registrar, Duration::from_secs(5), &workers);
        registrar.lose.notify_one();
        tokio::time::timeout(Duration::from_secs(10), admission.stop_requested())
            .await
            .expect("a stop is asked for");
        task.await.unwrap();
        assert!(shutdown.is_cancelled());
        assert_eq!(registrar.attempts(), 0, "it registered again");
        assert_eq!(workers.events(), [("stop", true)]);
        let reason = admission.stop_reason().unwrap();
        assert!(
            reason.contains("the session ended") && reason.contains("did not stop"),
            "{reason}"
        );
        assert!(
            admission.refusing(),
            "admitting after a worker kept running"
        );
    }

    #[tokio::test]
    async fn a_refusal_asks_for_a_stop_naming_it() {
        let registrar = Scripted::answering(vec![Some(Reregistration::Refused(
            "a migration is running on this catalog".to_owned(),
        ))]);
        let workers = Recorded::new(true);
        let (admission, shutdown, task) = start_with(&registrar, Duration::from_secs(5), &workers);
        registrar.lose.notify_one();
        tokio::time::timeout(Duration::from_secs(10), admission.stop_requested())
            .await
            .expect("a stop is asked for");
        // The workers stopped at the loss and do not start again, and the
        // shutdown token fires with the stop, before the listener drains.
        assert_eq!(workers.events(), [("stop", true)]);
        assert!(shutdown.is_cancelled(), "the shutdown token did not fire");
        task.await.unwrap();
        let reason = admission.stop_reason().unwrap();
        assert!(
            reason.contains("the session ended")
                && reason.contains("a migration is running on this catalog"),
            "{reason}"
        );
        assert!(admission.refusing(), "admitting after a refusal");
        assert_eq!(registrar.attempts(), 1);
        assert_eq!(admission.stopped_for(), Some(reason));
    }

    /// A signal that ended the server's wait first decides how it ends: a
    /// stop asked for during the signal's drain leaves the exit status 0.
    #[test]
    fn a_stop_during_a_signals_drain_is_not_why_the_server_stopped() {
        let signalled = Admission::default();
        signalled.mark_signalled();
        signalled.stop("the window passed".to_owned());
        assert_eq!(signalled.stopped_for(), None);
        let stopped = Admission::default();
        stopped.stop("the window passed".to_owned());
        assert_eq!(stopped.stopped_for(), Some("the window passed"));
    }

    #[tokio::test]
    async fn the_server_stops_when_the_window_passes_without_a_registration() {
        let registrar = Scripted::answering(Vec::new());
        let window = Duration::from_millis(300);
        let (admission, _shutdown, task) = start(&registrar, window);
        registrar.lose.notify_one();
        let lost_at = Instant::now();
        tokio::time::timeout(Duration::from_secs(10), admission.stop_requested())
            .await
            .expect("a stop is asked for");
        let elapsed = lost_at.elapsed();
        task.await.unwrap();
        assert!(elapsed >= window, "stopped {elapsed:?} after the loss");
        let reason = admission.stop_reason().unwrap();
        assert!(
            reason.contains("no attempt succeeded") && reason.contains("no answer"),
            "{reason}"
        );
        assert!(registrar.attempts() > 1);
    }

    #[tokio::test]
    async fn an_attempt_that_never_answers_is_bounded_by_the_window() {
        let registrar = Scripted::answering(vec![None]);
        let window = Duration::from_millis(300);
        let (admission, _shutdown, task) = start(&registrar, window);
        registrar.lose.notify_one();
        tokio::time::timeout(Duration::from_secs(10), admission.stop_requested())
            .await
            .expect("a stop is asked for");
        task.await.unwrap();
        let reason = admission.stop_reason().unwrap();
        assert!(reason.contains("did not finish"), "{reason}");
    }

    #[tokio::test]
    async fn a_shutdown_while_registering_again_asks_for_no_stop() {
        let registrar = Scripted::answering(vec![None]);
        let (admission, shutdown, task) = start(&registrar, Duration::from_secs(30));
        registrar.lose.notify_one();
        until("an attempt started", || registrar.attempts() == 1).await;
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the supervisor returns at shutdown")
            .unwrap();
        assert_eq!(admission.stop_reason(), None);
    }

    #[tokio::test]
    async fn a_refusing_server_answers_service_unavailable_on_every_route() {
        let admission = Admission::default();
        let app = Router::new()
            .route("/", get(|| async { "served" }))
            .route("/health", get(|| async { "healthy" }))
            .layer(axum::middleware::from_fn_with_state(
                admission.clone(),
                refuse_while_unregistered,
            ));
        let get = |path: &str| Request::get(path).body(Body::empty()).unwrap();
        let served = app.clone().oneshot(get("/")).await.unwrap();
        assert_eq!(served.status(), StatusCode::OK);
        admission.refuse();
        for path in ["/", "/health"] {
            let refused = app.clone().oneshot(get(path)).await.unwrap();
            assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
            assert_eq!(
                refused.headers()["content-type"],
                "application/x-amz-json-1.0"
            );
            assert!(refused.headers().contains_key("x-amzn-RequestId"));
            let body = axum::body::to_bytes(refused.into_body(), 4096)
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                body["__type"],
                "com.amazonaws.dynamodb.v20120810#ServiceUnavailable"
            );
        }
        admission.admit();
        let served = app.oneshot(get("/health")).await.unwrap();
        assert_eq!(served.status(), StatusCode::OK);
    }
}

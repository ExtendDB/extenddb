// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authentication helpers for the management API.
//!
//! Supports two authentication modes:
//! - Admin users: `admin_name:password` (Basic auth)
//! - IAM users: `account_id/user_name:password` (Basic auth, for self-service)

use std::net::SocketAddr;

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use extenddb_storage::management_store::{AdminStore, OpError};

use crate::rate_limit::LoginRefusal;

/// Maximum allowed length for the Authorization header (8 KB).
const MAX_AUTH_HEADER_LEN: usize = 8 * 1024;

/// WWW-Authenticate header value for 401 responses (RFC 7235).
const WWW_AUTHENTICATE_BASIC: &str = "Basic realm=\"extenddb\"";

/// The 401 answered to a wrong password or an unknown principal.
fn invalid_credentials() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
        "Invalid credentials",
    )
        .into_response()
}

/// Log an error from a password check under `context`. A verification that
/// found no free bcrypt slot in time is an overload, not a storage error:
/// [`extenddb_storage::password::verify_password`] has already logged it at
/// WARN under its own message, so it is not logged again here.
pub(crate) fn log_password_check_error(context: &str, error: &OpError) {
    if !extenddb_storage::password::is_overloaded(error) {
        tracing::error!("{context}: {error:?}");
    }
}

/// The answer to a login attempt the rate limiter did not admit.
fn refusal_response(refusal: LoginRefusal) -> Response {
    match refusal {
        LoginRefusal::TooManyAttempts(msg) => (StatusCode::TOO_MANY_REQUESTS, msg).into_response(),
        LoginRefusal::InvalidCredentials => invalid_credentials(),
        LoginRefusal::Internal => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Identifies the authenticated caller of a management API request.
#[derive(Debug, Clone)]
pub enum CallerIdentity {
    /// An admin user (full management access).
    Admin(String),
    /// An IAM user (self-service access only).
    IamUser {
        account_id: String,
        user_name: String,
    },
}

/// Extract and verify credentials from the `Authorization: Basic ...` header.
///
/// Tries admin auth first (`name:password`). If the admin name is not found,
/// tries IAM user auth (`account_id/user_name:password`). An admitted
/// attempt runs one bcrypt check whatever the name. When an admin has the
/// name, the check is against the admin's hash. Otherwise the IAM user step
/// runs it, against the user's hash, or against a fixed hash when no IAM user
/// with a console password has the name.
///
/// Returns the caller identity on success, or an error response on failure.
///
/// `peer` is the address of the connection the request arrived on. Failed
/// attempts are counted per principal and peer address and per peer address
/// (see [`crate::rate_limit`]); an attempt over either limit is answered 429
/// before any password is checked. A wrong password is recorded as a failure;
/// a storage error while checking it is answered 500 and recorded as nothing.
/// A principal longer than [`crate::rate_limit::MAX_PRINCIPAL_LEN`] bytes is
/// answered 401 and recorded as nothing.
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
pub async fn authenticate(
    headers: &HeaderMap,
    store: &dyn extenddb_storage::CatalogStore,
    rate_limiter: &dyn extenddb_storage::management_store::RateLimitStore,
    peer: SocketAddr,
) -> Result<CallerIdentity, Response> {
    let header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
                "Missing Authorization header",
            )
                .into_response()
        })?;

    // S-2: Cap Authorization header length to prevent heap abuse.
    if header.len() > MAX_AUTH_HEADER_LEN {
        return Err((
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
            "Authorization header exceeds maximum allowed length",
        )
            .into_response());
    }

    let encoded = header.strip_prefix("Basic ").ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
            "Invalid Authorization scheme",
        )
            .into_response()
    })?;

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| {
            (
                StatusCode::UNAUTHORIZED,
                [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
                "Invalid base64 in Authorization header",
            )
                .into_response()
        })?;

    let credentials = String::from_utf8(decoded).map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
            "Invalid UTF-8 in credentials",
        )
            .into_response()
    })?;

    let (username, password) = credentials.split_once(':').ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
            "Invalid credentials format",
        )
            .into_response()
    })?;

    // Reserve the attempt before any password is checked, so a burst cannot
    // run more checks than the limits allow.
    let attempt = crate::rate_limit::begin_login(rate_limiter, username, peer)
        .await
        .map_err(refusal_response)?;

    // Try admin auth first. A storage error returns 500 without recording a
    // failure: no password was checked, so nothing counts against the caller.
    match try_admin_auth(username, password, store).await? {
        AdminAuth::Authenticated(identity) => return Ok(identity),
        AdminAuth::WrongPassword => {
            attempt.record_failure(rate_limiter).await;
            return Err(invalid_credentials());
        }
        AdminAuth::NotFound => {} // Admin not found, try IAM.
    }

    // Try IAM user auth: username format is `account_id/user_name`.
    match try_iam_user_auth(username, password, store).await? {
        Some(identity) => Ok(identity),
        None => {
            attempt.record_failure(rate_limiter).await;
            Err(invalid_credentials())
        }
    }
}

/// Authenticate as admin only. Returns error if caller is not an admin.
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
pub async fn authenticate_admin(
    headers: &HeaderMap,
    store: &dyn extenddb_storage::CatalogStore,
    rate_limiter: &dyn extenddb_storage::management_store::RateLimitStore,
    peer: SocketAddr,
) -> Result<String, Response> {
    match authenticate(headers, store, rate_limiter, peer).await? {
        CallerIdentity::Admin(name) => Ok(name),
        CallerIdentity::IamUser { .. } => {
            Err((StatusCode::FORBIDDEN, "Admin access required").into_response())
        }
    }
}

/// Outcome of checking a password against the admin store.
enum AdminAuth {
    Authenticated(CallerIdentity),
    /// The admin exists and the password is wrong; do not fall through to IAM.
    WrongPassword,
    /// No admin of that name; fall through to IAM user auth.
    NotFound,
}

/// Check `password` for admin `username`. `Err` is the 500 answered to a
/// storage error, including a timed-out wait for a bcrypt slot.
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
async fn try_admin_auth(
    username: &str,
    password: &str,
    store: &dyn AdminStore,
) -> Result<AdminAuth, Response> {
    let result = store
        .verify_admin_password(username, password)
        .await
        .map_err(|e| {
            log_password_check_error("Management API: DB error during auth", &e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?;

    Ok(match result {
        Some(true) => AdminAuth::Authenticated(CallerIdentity::Admin(username.to_owned())),
        Some(false) => AdminAuth::WrongPassword,
        None => AdminAuth::NotFound,
    })
}

/// Check `password` for IAM user `account_id/user_name`. `Ok(None)` is a
/// wrong password, an unknown user or a name without `/`; `Err` is the 500
/// answered to a storage error, including a timed-out wait for a bcrypt slot.
///
/// A name without `/` names no IAM user. It is checked against a fixed hash,
/// so that it runs one bcrypt check, as a login for an existing admin or IAM
/// user does.
#[allow(clippy::similar_names)]
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
async fn try_iam_user_auth(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::CatalogStore,
) -> Result<Option<CallerIdentity>, Response> {
    let user = username.split_once('/');
    let checked = match user {
        Some((acct_id, uname)) => {
            store
                .verify_iam_user_password(acct_id, uname, password)
                .await
        }
        None => extenddb_storage::password::verify_stored_password(password.to_owned(), None).await,
    };
    let valid = checked.map_err(|e| {
        log_password_check_error("Management API: DB error during IAM user auth", &e);
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    })?;

    Ok(match user {
        Some((acct_id, uname)) if valid => Some(CallerIdentity::IamUser {
            account_id: acct_id.to_owned(),
            user_name: uname.to_owned(),
        }),
        _ => None,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use extenddb_storage::management_store::{AdminEntry, ManagementStore, OpResult};
    use extenddb_storage::password::{VERIFY_OVERLOADED, count_verifications};
    use futures::future::BoxFuture;

    use super::*;
    use crate::rate_limit::PRINCIPAL_LIMITED;
    use crate::rate_limit::tests::{MemoryStore, capture_logs, peer};

    /// The password of every principal in [`login_catalog`].
    const PASSWORD: &str = "correct horse";

    /// A password no principal in [`login_catalog`] has.
    const WRONG: &str = "battery staple";

    /// bcrypt's lowest cost, for the hashes in [`login_catalog`]. It keeps the
    /// tests fast and tells a check against a stored hash apart from one
    /// against the fixed hash, which is at the default cost.
    const TEST_COST: u32 = 4;

    /// An in-memory SQLite catalog with the admins `alice` and `team/root`,
    /// and in account 123456789012 the IAM user `bob`, who has a console
    /// password, and `carol`, who has none. Every password is [`PASSWORD`],
    /// hashed at [`TEST_COST`].
    pub(crate) async fn login_catalog() -> extenddb_storage_sqlite::SqliteCatalogStore {
        // One connection keeps the in-memory database alive for the test.
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        let migrations = extenddb_storage_sqlite::migrations::SqliteMigrations::on_pool(
            pool.clone(),
            ":memory:",
        )
        .expect("one connection");
        extenddb_storage::migrations::Runner::new(&migrations, "test")
            .migrate()
            .await
            .expect("migrate the catalog");
        let catalog = extenddb_storage_sqlite::SqliteCatalogStore::new(pool);
        let hash = bcrypt::hash(PASSWORD, TEST_COST).unwrap();
        for admin in ["alice", "team/root"] {
            catalog.create_admin(admin, &hash).await.unwrap();
        }
        catalog
            .create_account("123456789012", "logins")
            .await
            .unwrap();
        catalog
            .create_user("123456789012", "bob", Some(&hash))
            .await
            .unwrap();
        catalog
            .create_user("123456789012", "carol", None)
            .await
            .unwrap();
        catalog
    }

    /// How a login attempt in [`LOGIN_CASES`] ends.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Outcome {
        Admin,
        IamUser,
        Refused,
    }

    impl Outcome {
        pub(crate) fn of(identity: Option<&CallerIdentity>) -> Self {
            match identity {
                Some(CallerIdentity::Admin(_)) => Self::Admin,
                Some(CallerIdentity::IamUser { .. }) => Self::IamUser,
                None => Self::Refused,
            }
        }
    }

    /// A login attempt against [`login_catalog`].
    pub(crate) struct LoginCase {
        pub(crate) case: &'static str,
        pub(crate) username: &'static str,
        pub(crate) password: &'static str,
        pub(crate) outcome: Outcome,
        /// The bcrypt cost of the one check the login runs: [`TEST_COST`]
        /// against a stored hash, bcrypt's default cost against the fixed
        /// hash.
        pub(crate) cost: u32,
    }

    /// One login attempt for each path through the admin and IAM user checks.
    pub(crate) const LOGIN_CASES: &[LoginCase] = &[
        LoginCase {
            case: "admin, right password",
            username: "alice",
            password: PASSWORD,
            outcome: Outcome::Admin,
            cost: TEST_COST,
        },
        LoginCase {
            case: "admin, wrong password",
            username: "alice",
            password: WRONG,
            outcome: Outcome::Refused,
            cost: TEST_COST,
        },
        LoginCase {
            case: "unknown name without a slash",
            username: "nobody",
            password: PASSWORD,
            outcome: Outcome::Refused,
            cost: bcrypt::DEFAULT_COST,
        },
        LoginCase {
            case: "IAM user, right password",
            username: "123456789012/bob",
            password: PASSWORD,
            outcome: Outcome::IamUser,
            cost: TEST_COST,
        },
        LoginCase {
            case: "IAM user, wrong password",
            username: "123456789012/bob",
            password: WRONG,
            outcome: Outcome::Refused,
            cost: TEST_COST,
        },
        LoginCase {
            case: "IAM user without a console password",
            username: "123456789012/carol",
            password: PASSWORD,
            outcome: Outcome::Refused,
            cost: bcrypt::DEFAULT_COST,
        },
        LoginCase {
            case: "unknown IAM user",
            username: "123456789012/nobody",
            password: PASSWORD,
            outcome: Outcome::Refused,
            cost: bcrypt::DEFAULT_COST,
        },
        LoginCase {
            case: "unknown account",
            username: "111122223333/bob",
            password: PASSWORD,
            outcome: Outcome::Refused,
            cost: bcrypt::DEFAULT_COST,
        },
        LoginCase {
            case: "admin named with a slash, wrong password",
            username: "team/root",
            password: WRONG,
            outcome: Outcome::Refused,
            cost: TEST_COST,
        },
    ];

    /// An `Authorization: Basic` header for `username` and `password`.
    fn basic_auth(username: &str, password: &str) -> HeaderMap {
        let token =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Basic {token}").parse().unwrap());
        headers
    }

    #[tokio::test]
    async fn every_login_path_runs_one_bcrypt_check() {
        let catalog = login_catalog().await;
        let attempts = MemoryStore::default();
        // Each case uses its own source address: the in-flight registry is
        // process-wide.
        for (n, login) in (1..).zip(LOGIN_CASES) {
            let ip = format!("192.0.2.{n}");
            let headers = basic_auth(login.username, login.password);
            let (result, costs) =
                count_verifications(authenticate(&headers, &catalog, &attempts, peer(&ip))).await;
            assert_eq!(
                costs,
                [login.cost],
                "{}: bcrypt checks and their cost",
                login.case
            );
            let identity = match result {
                Ok(identity) => Some(identity),
                Err(response) => {
                    assert_eq!(
                        response.status(),
                        StatusCode::UNAUTHORIZED,
                        "{}",
                        login.case
                    );
                    None
                }
            };
            assert_eq!(
                Outcome::of(identity.as_ref()),
                login.outcome,
                "{}",
                login.case
            );
            let failures = i64::from(login.outcome == Outcome::Refused);
            assert_eq!(
                attempts.rows_for(login.username, &ip),
                failures,
                "{}: failures recorded",
                login.case
            );
        }
    }

    /// An admin store whose password check fails with `OpError::Internal`
    /// carrying `message`. Logins call nothing else on it.
    pub(crate) struct FailingAdmins {
        pub(crate) message: &'static str,
    }

    impl AdminStore for FailingAdmins {
        fn create_admin(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<()>> {
            unreachable!("a login does not create admins")
        }

        fn list_admins(&self) -> BoxFuture<'_, OpResult<Vec<AdminEntry>>> {
            unreachable!("a login does not list admins")
        }

        fn delete_admin(&self, _: &str) -> BoxFuture<'_, OpResult<()>> {
            unreachable!("a login does not delete admins")
        }

        fn change_admin_password(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<()>> {
            unreachable!("a login does not change passwords")
        }

        fn verify_admin_password(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Option<bool>>> {
            let error = OpError::Internal(self.message.to_owned());
            Box::pin(async move { Err(error) })
        }
    }

    #[test]
    fn a_refused_attempt_is_answered_by_its_reason() {
        let limited = refusal_response(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED));
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

        // The attempts store could not be read: a server error, not a limit.
        let internal = refusal_response(LoginRefusal::Internal);
        assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // A principal over the length limit gets the wrong-password answer.
        let too_long = refusal_response(LoginRefusal::InvalidCredentials);
        assert_eq!(too_long.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            too_long.headers()[axum::http::header::WWW_AUTHENTICATE],
            WWW_AUTHENTICATE_BASIC
        );
    }

    #[tokio::test]
    async fn a_store_error_in_the_admin_check_answers_500_and_is_logged_as_one() {
        let admins = FailingAdmins {
            message: "store is down",
        };
        let (logs, _guard) = capture_logs();
        let Err(response) = try_admin_auth("admin", "pw", &admins).await else {
            panic!("a store error is not a login outcome");
        };
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let text = logs.text();
        assert!(
            text.contains("ERROR") && text.contains("Management API: DB error during auth"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_bcrypt_slot_timeout_answers_500_and_is_not_logged_as_a_store_error() {
        let admins = FailingAdmins {
            message: VERIFY_OVERLOADED,
        };
        let (logs, _guard) = capture_logs();
        let Err(response) = try_admin_auth("admin", "pw", &admins).await else {
            panic!("an overload is not a login outcome");
        };
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let text = logs.text();
        assert!(!text.contains("DB error"), "{text}");
    }
}

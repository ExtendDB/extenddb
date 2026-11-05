// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Login, logout, and session management pages.

use std::sync::Arc;

use axum::Form;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use extenddb_storage::management_store::RateLimitStore;
use serde::Deserialize;

use crate::console::ConsoleState;
use crate::console::html;
use crate::management::CallerIdentity;
use crate::management::auth::log_password_check_error;
use crate::rate_limit::{self, LoginAttempt, LoginRefusal};

use super::extract_session_token;

/// GET /console/login — render the login form.
pub async fn login_page(State(state): State<Arc<ConsoleState>>) -> Html<String> {
    Html(login_html(None, &state.listen_url))
}

/// POST /console/login — validate credentials and create a session.
#[derive(Deserialize)]
pub struct LoginForm {
    username: String,
    password: String,
}

pub async fn login_submit(
    State(state): State<Arc<ConsoleState>>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Form(form): Form<LoginForm>,
) -> Response {
    // Reserve the attempt before any password is checked. Limits are per
    // username and peer address, so failures from elsewhere do not lock this
    // address out.
    let attempt = match rate_limit::begin_login(&*state.catalog_store, &form.username, addr).await {
        Ok(attempt) => attempt,
        Err(refusal) => return refusal_page(refusal, &state.listen_url),
    };

    // Try admin auth first, then IAM user auth. A storage error (including a
    // timed-out wait for a bcrypt slot) is answered 500 and recorded as
    // nothing: no password was checked.
    let checked = check_credentials(&form.username, &form.password, &*state.catalog_store).await;
    let identity =
        match settle_attempt(attempt, checked, &*state.catalog_store, &state.listen_url).await {
            Ok(identity) => identity,
            Err(page) => return page,
        };

    let (token, _csrf) = state.sessions.create(identity).await;
    let cookie = format!(
        "extenddb_session={token}; Path=/console; HttpOnly; SameSite=Strict; Max-Age=28800"
    );

    (
        StatusCode::SEE_OTHER,
        [("location", "/console"), ("set-cookie", &cookie)],
    )
        .into_response()
}

/// POST /console/logout — destroy session and redirect to login.
pub async fn logout(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if let Some(token) = extract_session_token(&headers) {
        state.sessions.remove(&token).await;
    }
    let cookie =
        "extenddb_session=; Path=/console; HttpOnly; SameSite=Strict; Max-Age=0".to_string();
    (
        StatusCode::SEE_OTHER,
        [("location", "/console/login"), ("set-cookie", &cookie)],
    )
        .into_response()
}

fn login_html(error: Option<&str>, listen_url: &str) -> String {
    let error_html = error.map(html::alert_error).unwrap_or_default();

    html::layout_full_with_url(
        "Login",
        r#"<nav><span class="brand">extenddb Console</span></nav>"#,
        &format!(
            r#"<div style="max-width:400px;margin:3rem auto">
<div class="card">
<h1>Login</h1>
{error_html}
<form method="post" action="/console/login">
<label for="username">Username</label>
<input id="username" name="username" type="text" required style="width:100%"
       placeholder="admin or account_id/user_name" autocomplete="username">
<label for="password">Password</label>
<input id="password" name="password" type="password" required style="width:100%"
       autocomplete="current-password">
<div style="margin-top:1rem">
<button class="btn btn-primary" type="submit" style="width:100%">Sign in</button>
</div>
</form>
</div>
<p style="text-align:center;margin-top:1rem;font-size:0.85rem;color:#666">
Admin users: enter your admin username.<br>
IAM users: enter <code>account_id/user_name</code>.
</p>
</div>"#
        ),
        None,
        None,
        Some(listen_url),
    )
}

/// Result of an admin login attempt — three-state to distinguish "not found"
/// (fall through to IAM) from "wrong password" (reject immediately).
enum AdminLoginResult {
    Authenticated(CallerIdentity),
    WrongPassword,
    NotFound,
}

/// The login page with a server error, answered when the store failed before
/// or while the password was checked. Nothing is recorded against the caller.
fn internal_error(listen_url: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Html(login_html(
            Some("Internal error. Please try again later."),
            listen_url,
        )),
    )
        .into_response()
}

/// The login page answered to a wrong password or an unknown principal.
fn invalid_credentials(listen_url: &str) -> Response {
    (
        StatusCode::OK,
        Html(login_html(Some("Invalid credentials"), listen_url)),
    )
        .into_response()
}

/// The login page answered to an attempt the rate limiter did not admit.
fn refusal_page(refusal: LoginRefusal, listen_url: &str) -> Response {
    match refusal {
        LoginRefusal::TooManyAttempts(msg) => (
            StatusCode::TOO_MANY_REQUESTS,
            Html(login_html(Some(msg), listen_url)),
        )
            .into_response(),
        LoginRefusal::InvalidCredentials => invalid_credentials(listen_url),
        LoginRefusal::Internal => internal_error(listen_url),
    }
}

/// Check the form's credentials, admin first and then IAM user. `Ok(None)`
/// is a wrong password or an unknown principal; `Err(())` is a storage
/// error, including a timed-out wait for a bcrypt slot.
async fn check_credentials(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::CatalogStore,
) -> Result<Option<CallerIdentity>, ()> {
    match try_admin_login(username, password, store).await? {
        AdminLoginResult::Authenticated(id) => Ok(Some(id)),
        // Admin exists, wrong password: do NOT fall through.
        AdminLoginResult::WrongPassword => Ok(None),
        // Admin not found: try IAM user auth.
        AdminLoginResult::NotFound => try_iam_login(username, password, store).await,
    }
}

/// Settle an admitted attempt with the result of its password check. A
/// wrong password is recorded as a failure and answered with the form's
/// error. A storage error is answered 500 and recorded as nothing; dropping
/// the attempt releases its reservation.
async fn settle_attempt(
    attempt: LoginAttempt,
    checked: Result<Option<CallerIdentity>, ()>,
    rate_limiter: &dyn RateLimitStore,
    listen_url: &str,
) -> Result<CallerIdentity, Response> {
    match checked {
        Ok(Some(identity)) => Ok(identity),
        Ok(None) => {
            attempt.record_failure(rate_limiter).await;
            Err(invalid_credentials(listen_url))
        }
        Err(()) => Err(internal_error(listen_url)),
    }
}

/// `Err(())` is a storage error; the caller answers 500 and records nothing.
async fn try_admin_login(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::management_store::AdminStore,
) -> Result<AdminLoginResult, ()> {
    let result = store
        .verify_admin_password(username, password)
        .await
        .map_err(|e| log_password_check_error("Console login: DB error during admin auth", &e))?;
    Ok(match result {
        Some(true) => AdminLoginResult::Authenticated(CallerIdentity::Admin(username.to_owned())),
        Some(false) => AdminLoginResult::WrongPassword,
        None => AdminLoginResult::NotFound,
    })
}

/// `Ok(None)` is a wrong password, an unknown user or a name without `/`;
/// `Err(())` is a storage error, which the caller answers 500 and records as
/// nothing. A name without `/` names no IAM user. It is checked against a
/// fixed hash, so that it runs one bcrypt check, as a login for an existing
/// admin or IAM user does.
async fn try_iam_login(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::management_store::ManagementStore,
) -> Result<Option<CallerIdentity>, ()> {
    let user = username.split_once('/');
    let checked = match user {
        Some((acct_id, uname)) => {
            store
                .verify_iam_user_password(acct_id, uname, password)
                .await
        }
        None => extenddb_storage::password::verify_stored_password(password.to_owned(), None).await,
    };
    let ok = checked.map_err(|e| {
        log_password_check_error("Console login: DB error during IAM user auth", &e)
    })?;
    Ok(match user {
        Some((acct_id, uname)) if ok => Some(CallerIdentity::IamUser {
            account_id: acct_id.to_owned(),
            user_name: uname.to_owned(),
        }),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use extenddb_storage::password::VERIFY_OVERLOADED;

    use super::*;
    use crate::management::auth::tests::FailingAdmins;
    use crate::rate_limit::PRINCIPAL_LIMITED;
    use crate::rate_limit::tests::{MemoryStore, capture_logs, peer};

    const LISTEN_URL: &str = "https://127.0.0.1:8443";

    async fn body_text(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn a_refused_attempt_is_answered_by_its_reason() {
        let limited = refusal_page(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED), LISTEN_URL);
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(body_text(limited).await.contains(PRINCIPAL_LIMITED));

        // The attempts store could not be read: a server error, not a limit.
        let internal = refusal_page(LoginRefusal::Internal, LISTEN_URL);
        assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // A principal over the length limit gets the wrong-password page.
        let too_long = refusal_page(LoginRefusal::InvalidCredentials, LISTEN_URL);
        assert_eq!(too_long.status(), StatusCode::OK);
        assert!(body_text(too_long).await.contains("Invalid credentials"));
    }

    #[tokio::test]
    async fn a_store_error_in_the_admin_check_is_an_error_not_an_unknown_admin() {
        let admins = FailingAdmins {
            message: "store is down",
        };
        let (logs, _guard) = capture_logs();
        assert!(try_admin_login("admin", "pw", &admins).await.is_err());
        let text = logs.text();
        assert!(
            text.contains("ERROR") && text.contains("Console login: DB error during admin auth"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_bcrypt_slot_timeout_is_an_error_and_is_not_logged_as_a_store_error() {
        let admins = FailingAdmins {
            message: VERIFY_OVERLOADED,
        };
        let (logs, _guard) = capture_logs();
        assert!(try_admin_login("admin", "pw", &admins).await.is_err());
        let text = logs.text();
        assert!(!text.contains("DB error"), "{text}");
    }

    // Each test uses its own source address: the in-flight registry is
    // process-wide.

    #[tokio::test]
    async fn a_store_error_during_the_password_check_answers_500_and_records_nothing() {
        let attempts = MemoryStore::default();
        let attempt = rate_limit::begin_login(&attempts, "admin", peer("198.51.100.90"))
            .await
            .unwrap();
        let page = settle_attempt(attempt, Err(()), &attempts, LISTEN_URL)
            .await
            .unwrap_err();
        assert_eq!(page.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(attempts.row_count(), 0);
    }

    #[tokio::test]
    async fn a_wrong_password_answers_the_form_error_and_records_one_failure() {
        let attempts = MemoryStore::default();
        let attempt = rate_limit::begin_login(&attempts, "admin", peer("198.51.100.91"))
            .await
            .unwrap();
        let page = settle_attempt(attempt, Ok(None), &attempts, LISTEN_URL)
            .await
            .unwrap_err();
        assert_eq!(page.status(), StatusCode::OK);
        assert!(body_text(page).await.contains("Invalid credentials"));
        assert_eq!(attempts.rows_for("admin", "198.51.100.91"), 1);
    }

    #[tokio::test]
    async fn every_credential_check_runs_one_bcrypt_check() {
        use extenddb_storage::password::count_verifications;

        use crate::management::auth::tests::{LOGIN_CASES, Outcome, login_catalog};

        let catalog = login_catalog().await;
        for login in LOGIN_CASES {
            let (checked, costs) =
                count_verifications(check_credentials(login.username, login.password, &catalog))
                    .await;
            assert_eq!(
                costs,
                [login.cost],
                "{}: bcrypt checks and their cost",
                login.case
            );
            let identity = checked.unwrap_or_else(|()| panic!("{}: storage error", login.case));
            assert_eq!(
                Outcome::of(identity.as_ref()),
                login.outcome,
                "{}",
                login.case
            );
        }
    }
}

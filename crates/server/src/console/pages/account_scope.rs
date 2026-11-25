// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The account check for the console pages under
//! `/console/accounts/{account_id}`.
//!
//! An admin session may use the pages of every account. An IAM-user session
//! may use the pages of its own account only. Every handler under that path
//! gets its session from [`require_account_session`], which applies the
//! check right after the session check of [`require_session`]. The session
//! check reads the password hash of the session's own principal, and the
//! handler reads nothing else from the catalog before the account check.
//!
//! Inside its own account an IAM-user session may use the pages of its own
//! user only: the user page and the access key forms, which get their session
//! from [`require_user_session`]. Every other page and form under the path is
//! for admins, and its handler redirects an IAM-user session to the console
//! home page before it reads anything from the catalog other than that
//! session check. This matches the management API, which gives an IAM user
//! its own access keys and password and refuses it every other read and
//! write.

use std::sync::Arc;

use axum::http::HeaderMap;
use axum::response::{IntoResponse, Redirect, Response};

use crate::console::ConsoleState;
use crate::management::CallerIdentity;

use super::{SessionData, require_session};

/// [`require_session`] for a page of the account `account_id`.
///
/// An IAM-user session whose own account is not `account_id` gets a redirect
/// to the console home page. The check reads nothing from the catalog, so
/// another account's ID and an ID that no account has get the same answer.
#[allow(clippy::result_large_err)]
pub(super) async fn require_account_session(
    headers: &HeaderMap,
    state: &Arc<ConsoleState>,
    account_id: &str,
) -> Result<SessionData, Response> {
    let session = require_session(headers, state).await?;
    account_scope(&session.identity, account_id)?;
    Ok(session)
}

/// [`require_account_session`] for a page of the user `user_name` in the
/// account `account_id`: the user page and the forms that create and delete
/// the user's access keys.
///
/// An admin session may use the pages of every user. An IAM-user session may
/// use the pages of its own user only, by the rule the management API applies
/// to an IAM user's access keys: the account ID and the user name in the path
/// both equal the session's own. Any other user's page redirects it to the
/// console home page. The check reads nothing from the catalog, so another
/// user's name and a name that no user has get the same answer.
#[allow(clippy::result_large_err)]
pub(super) async fn require_user_session(
    headers: &HeaderMap,
    state: &Arc<ConsoleState>,
    account_id: &str,
    user_name: &str,
) -> Result<SessionData, Response> {
    let session = require_account_session(headers, state, account_id).await?;
    user_scope(&session.identity, account_id, user_name)?;
    Ok(session)
}

/// `Ok` when `identity` may use the pages of the account `account_id`, or a
/// redirect to the console home page when it may not.
#[allow(clippy::result_large_err)]
fn account_scope(identity: &CallerIdentity, account_id: &str) -> Result<(), Response> {
    match identity {
        CallerIdentity::Admin(_) => Ok(()),
        CallerIdentity::IamUser {
            account_id: own, ..
        } if own == account_id => Ok(()),
        CallerIdentity::IamUser { .. } => Err(Redirect::to("/console").into_response()),
    }
}

/// `Ok` when `identity` may use the pages of the user `user_name` in the
/// account `account_id`, or a redirect to the console home page when it may
/// not.
#[allow(clippy::result_large_err)]
fn user_scope(
    identity: &CallerIdentity,
    account_id: &str,
    user_name: &str,
) -> Result<(), Response> {
    match identity {
        CallerIdentity::Admin(_) => Ok(()),
        CallerIdentity::IamUser {
            account_id: own_account,
            user_name: own_user,
        } if own_account == account_id && own_user == user_name => Ok(()),
        CallerIdentity::IamUser { .. } => Err(Redirect::to("/console").into_response()),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::http::header::LOCATION;
    use axum::response::Response;

    use super::{account_scope, user_scope};
    use crate::management::CallerIdentity;

    const OWN_ACCOUNT: &str = "444455556666";
    const OTHER_ACCOUNT: &str = "111122223333";
    /// An ID that no account has. The check reads no catalog, so it cannot
    /// tell this ID from `OTHER_ACCOUNT`, and the tests need no store.
    const MISSING_ACCOUNT: &str = "999999999999";

    fn admin() -> CallerIdentity {
        CallerIdentity::Admin("admin".to_owned())
    }

    fn iam_user() -> CallerIdentity {
        CallerIdentity::IamUser {
            account_id: OWN_ACCOUNT.to_owned(),
            user_name: "bob".to_owned(),
        }
    }

    /// The status, `Location` header and body of a refusal.
    async fn answer(response: Response) -> (StatusCode, Option<String>, Vec<u8>) {
        let status = response.status();
        let location = response
            .headers()
            .get(LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        (status, location, body.to_vec())
    }

    fn refusal(identity: &CallerIdentity, account_id: &str) -> Response {
        account_scope(identity, account_id).expect_err("the check let the session in")
    }

    #[test]
    fn iam_user_may_use_its_own_account() {
        assert!(account_scope(&iam_user(), OWN_ACCOUNT).is_ok());
    }

    #[tokio::test]
    async fn iam_user_is_redirected_to_the_console_from_another_account() {
        let (status, location, body) = answer(refusal(&iam_user(), OTHER_ACCOUNT)).await;

        assert_eq!(status, StatusCode::SEE_OTHER);
        assert_eq!(location.as_deref(), Some("/console"));
        assert!(body.is_empty(), "{}", String::from_utf8_lossy(&body));
    }

    #[tokio::test]
    async fn another_account_and_a_missing_account_get_the_same_answer() {
        let other = answer(refusal(&iam_user(), OTHER_ACCOUNT)).await;
        let missing = answer(refusal(&iam_user(), MISSING_ACCOUNT)).await;

        assert_eq!(other, missing);
    }

    #[test]
    fn iam_user_is_refused_an_id_that_only_resembles_its_own() {
        for id in [
            "44445555666",
            "4444555566660",
            "0444455556666",
            " 444455556666",
            "444455556666/",
            "",
        ] {
            assert!(
                account_scope(&iam_user(), id).is_err(),
                "an IAM user of {OWN_ACCOUNT} was let into {id:?}"
            );
        }
    }

    #[test]
    fn admin_may_use_every_account() {
        for id in [OWN_ACCOUNT, OTHER_ACCOUNT, MISSING_ACCOUNT] {
            assert!(account_scope(&admin(), id).is_ok(), "admin refused {id}");
        }
    }

    /// The name of the user `iam_user` logs in as.
    const OWN_USER: &str = "bob";

    #[test]
    fn iam_user_may_use_its_own_user_pages() {
        assert!(user_scope(&iam_user(), OWN_ACCOUNT, OWN_USER).is_ok());
    }

    #[tokio::test]
    async fn iam_user_is_redirected_to_the_console_from_another_users_pages() {
        for (account, user) in [
            (OWN_ACCOUNT, "alice"),
            (OTHER_ACCOUNT, OWN_USER),
            (OTHER_ACCOUNT, "alice"),
        ] {
            let refused =
                user_scope(&iam_user(), account, user).expect_err("the check let the session in");
            let (status, location, body) = answer(refused).await;

            assert_eq!(status, StatusCode::SEE_OTHER, "{account}/{user}");
            assert_eq!(location.as_deref(), Some("/console"), "{account}/{user}");
            assert!(body.is_empty(), "{}", String::from_utf8_lossy(&body));
        }
    }

    #[tokio::test]
    async fn another_user_and_a_missing_user_get_the_same_answer() {
        // The check reads no catalog, so it cannot tell a user that exists
        // from a name that no user has.
        let other = user_scope(&iam_user(), OWN_ACCOUNT, "alice").expect_err("let in");
        let missing =
            user_scope(&iam_user(), OWN_ACCOUNT, "nobody-has-this-name").expect_err("let in");

        assert_eq!(answer(other).await, answer(missing).await);
    }

    #[test]
    fn iam_user_is_refused_a_name_that_only_resembles_its_own() {
        for name in [
            "Bob", "BOB", "bo", "bobb", " bob", "bob ", "bob/", "", "bob\0",
        ] {
            assert!(
                user_scope(&iam_user(), OWN_ACCOUNT, name).is_err(),
                "{OWN_ACCOUNT}/{OWN_USER} was let into the pages of {name:?}"
            );
        }
    }

    #[test]
    fn admin_may_use_every_users_pages() {
        for (account, user) in [
            (OWN_ACCOUNT, OWN_USER),
            (OWN_ACCOUNT, "alice"),
            (OTHER_ACCOUNT, "alice"),
            (MISSING_ACCOUNT, "nobody-has-this-name"),
        ] {
            assert!(
                user_scope(&admin(), account, user).is_ok(),
                "admin refused {account}/{user}"
            );
        }
    }
}

#[cfg(test)]
#[path = "account_scope_reads_tests.rs"]
mod reads_tests;

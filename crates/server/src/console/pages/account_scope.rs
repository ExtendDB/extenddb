// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The account check for the console pages under
//! `/console/accounts/{account_id}`.
//!
//! An admin session may use the pages of every account. An IAM-user session
//! may use the pages of its own account only. Every handler under that path
//! gets its session from [`require_account_session`], which applies the
//! check before the handler reads anything from the catalog.

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

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use axum::http::header::LOCATION;
    use axum::response::Response;

    use super::account_scope;
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
        match account_scope(identity, account_id) {
            Ok(()) => panic!("the session was let into account {account_id}"),
            Err(response) => response,
        }
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
}

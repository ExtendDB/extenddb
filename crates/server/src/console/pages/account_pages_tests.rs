// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the console dashboard: the deployment-wide counts are shown to
//! admin sessions only.

use axum::http::StatusCode;
use axum::response::Response;

use super::render_dashboard;
use crate::console::pages::SessionData;
use crate::management::CallerIdentity;

/// Counts no test data would produce by chance, so a test can look for them
/// in a page.
const ACCOUNT_COUNT: i64 = 7_351;
const ADMIN_COUNT: i64 = 4_219;

fn session(identity: CallerIdentity) -> SessionData {
    SessionData {
        identity,
        csrf_token: "csrf-token".to_owned(),
    }
}

fn iam_user() -> CallerIdentity {
    CallerIdentity::IamUser {
        account_id: "444455556666".to_owned(),
        user_name: "bob".to_owned(),
    }
}

async fn body_text(response: Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    String::from_utf8(bytes.to_vec()).expect("UTF-8 response body")
}

#[tokio::test]
async fn iam_user_dashboard_has_no_deployment_count() {
    // The counts are passed in to check that the page leaves them out even
    // when they are at hand; the handler does not read them for an IAM user.
    let response = render_dashboard(
        &session(iam_user()),
        Some((ACCOUNT_COUNT, ADMIN_COUNT)),
        "test-version",
    );

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_text(response).await;
    assert!(body.contains("<h1>Dashboard</h1>"), "{body}");
    assert!(!body.contains(&ACCOUNT_COUNT.to_string()), "{body}");
    assert!(!body.contains(&ADMIN_COUNT.to_string()), "{body}");
    assert!(!body.contains("Admin Users"), "{body}");
    assert!(!body.contains("<p>Accounts</p>"), "{body}");
    assert!(
        body.contains(r#"<a href="/console/accounts/444455556666">"#),
        "{body}"
    );
}

#[tokio::test]
async fn admin_dashboard_shows_the_deployment_counts() {
    let response = render_dashboard(
        &session(CallerIdentity::Admin("admin".to_owned())),
        Some((ACCOUNT_COUNT, ADMIN_COUNT)),
        "test-version",
    );

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_text(response).await;
    assert!(
        body.contains(&format!(
            "<h2 style=\"margin:0\">{ACCOUNT_COUNT}</h2>\n<p>Accounts</p>"
        )),
        "{body}"
    );
    assert!(
        body.contains(&format!(
            "<h2 style=\"margin:0\">{ADMIN_COUNT}</h2>\n<p>Admin Users</p>"
        )),
        "{body}"
    );
}

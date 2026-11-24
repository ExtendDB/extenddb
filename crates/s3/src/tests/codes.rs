// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The S3 codes about the server's own signing identity, account or clock,
//! read from the failed call S3 answers.

use super::*;
use crate::{SERVER_CREDENTIAL_CODES, is_server_credential_code, is_server_credential_error};

/// Each code with the status S3 answers it with.
const SERVER_FAULTS: &[(u16, &str)] = &[
    (403, "SignatureDoesNotMatch"),
    (403, "InvalidAccessKeyId"),
    (400, "ExpiredToken"),
    (400, "InvalidToken"),
    (400, "TokenRefreshRequired"),
    (403, "RequestTimeTooSkewed"),
    (400, "CredentialsNotSupported"),
    (403, "InvalidSecurity"),
    (400, "MissingSecurityHeader"),
    (403, "AccountProblem"),
    (403, "NotSignedUp"),
];

#[test]
fn the_server_credential_codes_are_the_listed_ones() {
    let listed: Vec<_> = SERVER_FAULTS.iter().map(|(_, code)| *code).collect();
    assert_eq!(listed, SERVER_CREDENTIAL_CODES);
    for code in SERVER_CREDENTIAL_CODES {
        assert!(is_server_credential_code(Some(code)), "{code}");
    }
    assert!(!is_server_credential_code(Some("AllAccessDisabled")));
    assert!(!is_server_credential_code(Some("AccessDenied")));
    assert!(!is_server_credential_code(None));
}

#[tokio::test]
async fn a_failed_call_is_read_by_the_code_s3_answered() {
    for &(status, code) in SERVER_FAULTS {
        let (clients, _replay) = clients(vec![s3_error(status, code, "S3 text.")]);
        let error = list_one(&clients, "loads").await.unwrap_err();
        assert!(is_server_credential_error(&error), "{code}");
    }
    for (status, code) in [(403, "AllAccessDisabled"), (403, "AccessDenied")] {
        let (clients, _replay) = clients(vec![s3_error(status, code, "S3 text.")]);
        let error = list_one(&clients, "loads").await.unwrap_err();
        assert!(!is_server_credential_error(&error), "{code}");
    }
    // No answer at all: the replay client has nothing to answer with.
    let (clients, _replay) = clients(Vec::new());
    let error = list_one(&clients, "loads").await.unwrap_err();
    assert!(!is_server_credential_error(&error), "{error:?}");
}

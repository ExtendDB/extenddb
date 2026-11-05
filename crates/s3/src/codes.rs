// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The S3 error codes about the server's own signing identity, account or
//! clock, and the check of a failed call against them.

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};

/// The S3 codes that describe the server's own signing identity, account
/// or clock, never the caller's request: a store signs every request with
/// the server's credentials, so a caller cannot cause them and cannot fix
/// them, and the operator reads S3's code and message in the store's log.
/// `AllAccessDisabled` names the bucket's account and stays the caller's.
pub const SERVER_CREDENTIAL_CODES: &[&str] = &[
    "SignatureDoesNotMatch",
    "InvalidAccessKeyId",
    "ExpiredToken",
    "InvalidToken",
    "TokenRefreshRequired",
    "RequestTimeTooSkewed",
    "CredentialsNotSupported",
    "InvalidSecurity",
    "MissingSecurityHeader",
    "AccountProblem",
    "NotSignedUp",
];

/// Whether an S3 error code is one of [`SERVER_CREDENTIAL_CODES`].
#[must_use]
pub fn is_server_credential_code(code: Option<&str>) -> bool {
    code.is_some_and(|code| SERVER_CREDENTIAL_CODES.contains(&code))
}

/// Whether a failed call is S3's answer with one of
/// [`SERVER_CREDENTIAL_CODES`]. A failure S3 did not answer (a connection
/// failure, a timeout) carries no code and is not.
#[must_use]
pub fn is_server_credential_error<E: ProvideErrorMetadata>(
    error: &SdkError<E, HttpResponse>,
) -> bool {
    is_server_credential_code(error.code())
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! S3 failures as the client reads them: the `FailureCode` each S3 error
//! code maps to, and the `FailureMessage`, S3's own message followed by the
//! tail the service renders for the call that failed (measured on the
//! service). Transport failures, 5xx answers, throttling, the codes about
//! the server's own credentials (`extenddb_s3::SERVER_CREDENTIAL_CODES`)
//! and codes outside the table are the server's: they answer
//! `InternalServerError` and their detail, S3's code and message included,
//! goes to the log at ERROR.

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};

use crate::bulk_put::source::StoreFault;

/// The tail the service appends to S3's message, by the kind of call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Tail {
    /// Input reads, the Start-time list, the manifest write and the
    /// Start-time encryption check: `(Service: Amazon S3; Status Code: ...;
    /// Proxy: null)`.
    AmazonS3,
    /// Report writes, the first-record check included: `(Service: S3,
    /// Status Code: ...) (SDK Attempt Count: 1)`.
    S3,
}

/// What one failed S3 call means for the operation.
#[derive(Debug)]
pub(crate) enum Outcome {
    /// A fault the client sees, rendered.
    Fault(StoreFault),
    /// A ranged read that starts at or past the object's end (416): the
    /// object is shorter than it was when enumerated, read as end of file.
    PastEnd,
    /// The server's failure; the caller logs the error.
    Internal,
}

/// Classify one failed call. `after_first_byte` is recorded on the fault:
/// whether bytes of the object were read before the call failed.
pub(crate) fn classify<E>(
    error: &SdkError<E, HttpResponse>,
    tail: Tail,
    after_first_byte: bool,
) -> Outcome
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let (SdkError::ServiceError(_), Some(response)) = (error, error.raw_response()) else {
        return Outcome::Internal;
    };
    if extenddb_s3::is_server_credential_error(error) {
        return Outcome::Internal;
    }
    let status = response.status().as_u16();
    let code = error.code();
    if status == 416 || code == Some("InvalidRange") {
        return Outcome::PastEnd;
    }
    let failure_code = match code {
        Some(code) if status < 500 => failure_code(code),
        _ => None,
    };
    let (Some(code), Some(failure_code)) = (code, failure_code) else {
        return Outcome::Internal;
    };
    let message = error
        .message()
        .map_or_else(|| default_message(code).to_owned(), str::to_owned);
    let request_id = response.headers().get("x-amz-request-id").unwrap_or("null");
    // An S3-compatible service that sends no extended request id renders
    // `null` in its place.
    let extended_id = response.headers().get("x-amz-id-2").unwrap_or("null");
    let tail = match tail {
        Tail::AmazonS3 => format!(
            " (Service: Amazon S3; Status Code: {status}; Error Code: {code}; Request ID: \
             {request_id}; S3 Extended Request ID: {extended_id}; Proxy: null)"
        ),
        // Every mapped code is a 4xx the SDK does not retry, so the
        // attempt count is 1.
        Tail::S3 => format!(
            " (Service: S3, Status Code: {status}, Request ID: {request_id}, Extended Request \
             ID: {extended_id}) (SDK Attempt Count: 1)"
        ),
    };
    Outcome::Fault(StoreFault {
        code: failure_code,
        message: format!("{message}{tail}"),
        after_first_byte,
    })
}

/// The message for a code when S3 sent none (an error without a body).
fn default_message(code: &str) -> &'static str {
    match code {
        "NoSuchBucket" => "The specified bucket does not exist",
        "NoSuchKey" => "The specified key does not exist.",
        "AccessDenied" => "Access Denied",
        _ => "",
    }
}

/// The `FailureCode` for an S3 error code: `S3` followed by the code for an
/// S3 code (measured: `S3AccessDenied`, `S3NoSuchBucket`, `S3NoSuchKey`),
/// and `Kms` followed by the exception name without its `KMS` lead for a
/// `KMS.` code (measured: `KmsDisabledException`,
/// `KmsInvalidKeyUsageException`). `RequestTimeout` and `SlowDown`, which
/// the SDK retries, and any code outside the tables are `None`.
pub(crate) fn failure_code(code: &str) -> Option<&'static str> {
    if let Some(name) = code.strip_prefix("KMS.") {
        return KMS_CODES
            .iter()
            .find(|(kms, _)| *kms == name)
            .map(|(_, failure)| *failure);
    }
    S3_CODES
        .iter()
        .find(|(s3, _)| *s3 == code)
        .map(|(_, failure)| *failure)
}

/// The `KMS.` codes S3 answers when a KMS call it makes for the request
/// fails, without the `KMS.` lead.
const KMS_CODES: &[(&str, &str)] = &[
    ("DisabledException", "KmsDisabledException"),
    ("InvalidKeyUsageException", "KmsInvalidKeyUsageException"),
    ("KMSInvalidStateException", "KmsInvalidStateException"),
    ("InvalidStateException", "KmsInvalidStateException"),
    ("NotFoundException", "KmsNotFoundException"),
    ("KeyUnavailableException", "KmsKeyUnavailableException"),
    ("AccessDeniedException", "KmsAccessDeniedException"),
    (
        "InvalidCiphertextException",
        "KmsInvalidCiphertextException",
    ),
    ("IncorrectKeyException", "KmsIncorrectKeyException"),
    (
        "InvalidGrantTokenException",
        "KmsInvalidGrantTokenException",
    ),
    ("OptInRequired", "KmsOptInRequired"),
];

macro_rules! s3_codes {
    ($($code:ident),* $(,)?) => {
        &[$((stringify!($code), concat!("S3", stringify!($code)))),*]
    };
}

/// The S3 error codes a client error (4xx) of the calls the store makes
/// can carry, from the S3 error code list, less the codes about the
/// server's own credentials (`extenddb_s3::SERVER_CREDENTIAL_CODES`): the
/// store signs every request with the server's credentials, so those
/// answer `InternalServerError`, a Start answers 500 and creates no
/// operation, and the operator reads S3's code and message in the ERROR
/// line.
const S3_CODES: &[(&str, &str)] = s3_codes!(
    AccessDenied,
    AllAccessDisabled,
    AuthorizationHeaderMalformed,
    BadDigest,
    ConditionalRequestConflict,
    EntityTooLarge,
    EntityTooSmall,
    IllegalLocationConstraintException,
    IncompleteBody,
    InvalidArgument,
    InvalidBucketName,
    InvalidBucketState,
    InvalidDigest,
    InvalidEncryptionAlgorithmError,
    InvalidObjectState,
    InvalidPart,
    InvalidPartOrder,
    InvalidPayer,
    InvalidRequest,
    InvalidStorageClass,
    InvalidURI,
    KeyTooLongError,
    MalformedXML,
    MetadataTooLarge,
    MethodNotAllowed,
    MissingContentLength,
    MissingRequestBodyError,
    NoSuchBucket,
    NoSuchKey,
    NoSuchUpload,
    NoSuchVersion,
    NotImplemented,
    OperationAborted,
    PermanentRedirect,
    PreconditionFailed,
    Redirect,
    TemporaryRedirect,
    UnexpectedContent,
    XAmzContentSHA256Mismatch,
);

#[cfg(test)]
mod tests {
    use extenddb_s3::{SERVER_CREDENTIAL_CODES, is_server_credential_code};

    use super::*;

    #[test]
    fn failure_codes_follow_the_measured_naming() {
        assert_eq!(failure_code("AccessDenied"), Some("S3AccessDenied"));
        assert_eq!(failure_code("NoSuchBucket"), Some("S3NoSuchBucket"));
        assert_eq!(failure_code("NoSuchKey"), Some("S3NoSuchKey"));
        assert_eq!(
            failure_code("InvalidObjectState"),
            Some("S3InvalidObjectState")
        );
        assert_eq!(
            failure_code("PreconditionFailed"),
            Some("S3PreconditionFailed")
        );
        assert_eq!(
            failure_code("KMS.DisabledException"),
            Some("KmsDisabledException")
        );
        assert_eq!(
            failure_code("KMS.InvalidKeyUsageException"),
            Some("KmsInvalidKeyUsageException")
        );
        assert_eq!(
            failure_code("KMS.KMSInvalidStateException"),
            Some("KmsInvalidStateException")
        );
        assert_eq!(
            failure_code("KMS.NotFoundException"),
            Some("KmsNotFoundException")
        );
        assert_eq!(
            failure_code("AllAccessDisabled"),
            Some("S3AllAccessDisabled")
        );
        for code in SERVER_CREDENTIAL_CODES {
            assert_eq!(failure_code(code), None, "{code}");
            assert!(is_server_credential_code(Some(code)), "{code}");
        }
        assert!(!is_server_credential_code(Some("AllAccessDisabled")));
        assert!(!is_server_credential_code(None));
        for retried_or_unknown in [
            "RequestTimeout",
            "SlowDown",
            "Throttling",
            "KMS.Unheard",
            "",
        ] {
            assert_eq!(
                failure_code(retried_or_unknown),
                None,
                "{retried_or_unknown}"
            );
        }
    }
}

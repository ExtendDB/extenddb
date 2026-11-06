// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! S3 codes about the server's own signing identity, account or clock: the
//! server's failure on every call, with S3's code and message at ERROR.

use tracing::Level;

use super::logs::Capture;
use super::read::{LINES, open_with};
use super::*;
use crate::bulk_put::s3::client::Phase;
use crate::bulk_put::s3::error::Tail;
use crate::bulk_put::s3::list::check_bucket;
use crate::bulk_put::s3::probe::probe;
use crate::bulk_put::source::{InputCodec, OutputSse, SinkError, StoreUnavailable};

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

/// The server's failure, and not one the runner resumes after.
fn is_internal_source(error: &SourceError) -> bool {
    matches!(error, SourceError::Io(io)
        if io.kind() == std::io::ErrorKind::Other && !StoreUnavailable::of_io(io))
}

#[tokio::test]
async fn the_servers_own_credential_faults_are_the_servers_failure_on_every_call() {
    let (capture, _guard) = Capture::install();
    for &(status, code) in SERVER_FAULTS {
        let text = format!("S3 text for {code}.");
        let (reader, _replay) = open_with(
            vec![s3_error(status, code, &text)],
            16,
            LINES.len(),
            InputCodec::None,
        )
        .await;
        let error = reader.err().expect("refused");
        assert!(is_internal_source(&error), "read {code}: {error:?}");

        let (listing, _replay) = clients(vec![s3_error(status, code, &text)]);
        let error = check_bucket(&listing, &location("loads", None), "in/")
            .await
            .unwrap_err();
        assert!(is_internal_source(&error), "bucket check {code}: {error:?}");

        let (writing, _replay) = clients(vec![s3_error(status, code, &text)]);
        let error = probe(
            &writing,
            &location("reports", None),
            "run/AWSDynamoDB/BulkOperation/report-0.json.gz",
            &OutputSse::None,
            Tail::S3,
            Phase::Run,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&error, SinkError::Io(io)
                if io.kind() == std::io::ErrorKind::Other && !StoreUnavailable::of_io(io)),
            "write check {code}: {error:?}"
        );

        let lines: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|event| event.field("s3_code") == Some(code))
            .collect();
        assert_eq!(lines.len(), 3, "{code}: {lines:?}");
        for line in lines {
            assert_eq!(line.level, Level::ERROR, "{code}");
            assert_eq!(line.field("s3_message"), Some(text.as_str()), "{code}");
            assert!(
                line.message()
                    .contains("S3 refused the server's own credentials"),
                "{code}: {}",
                line.message()
            );
        }
    }
}

#[tokio::test]
async fn all_access_disabled_stays_the_callers_fault() {
    let text = "All access to this Amazon S3 resource has been disabled.";
    let (reader, _replay) = open_with(
        vec![s3_error(403, "AllAccessDisabled", text)],
        16,
        LINES.len(),
        InputCodec::None,
    )
    .await;
    let fault = fault(reader.err().expect("refused"));
    assert_eq!(fault.code, "S3AllAccessDisabled");
    assert_eq!(
        fault.message,
        format!("{text}{}", amazon_s3_tail(403, "AllAccessDisabled"))
    );
}

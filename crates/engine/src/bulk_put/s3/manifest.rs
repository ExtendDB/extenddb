// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The manifest on the S3 store: one PutObject of `manifest-files.json`
//! under the operation's key, one JSON object per line, with the expected
//! owner and the encryption fields (measured on the service: the manifest
//! is stored with the report's encryption settings). A failure carries the
//! tail the service renders for its manifest write.

use aws_sdk_s3::primitives::ByteStream;
use serde_json::Value;

use super::error::Tail;
use super::probe::{sink_error, sse_fields};
use super::{Location, S3Clients};
use crate::bulk_put::report;
use crate::bulk_put::source::{OutputSse, SinkError};

/// Write the manifest and return its key.
pub(super) async fn write_manifest(
    clients: &S3Clients,
    location: &Location,
    prefix: &str,
    operation_id: &str,
    sse: &OutputSse,
    lines: &[Value],
) -> Result<String, SinkError> {
    let key = report::manifest_key(prefix, &location.account, operation_id);
    let mut body = Vec::new();
    for line in lines {
        report::write_manifest_line(&mut body, line).map_err(SinkError::Io)?;
        body.push(b'\n');
    }
    let (encryption, kms_key) = sse_fields(sse);
    clients
        .call(&location.bucket, |client| {
            client
                .put_object()
                .bucket(&location.bucket)
                .key(&key)
                .body(ByteStream::from(body.clone()))
                .set_expected_bucket_owner(location.owner.clone())
                .set_server_side_encryption(encryption.clone())
                .set_ssekms_key_id(kms_key.clone())
                .send()
        })
        .await
        .map_err(|error| sink_error(&error, location, &key, Tail::AmazonS3, "manifest write"))?;
    tracing::info!(
        account_id = %location.account,
        operation = "StartBulkPutItems",
        object = %format!("s3://{}/{key}", location.bucket),
        "import/export file access"
    );
    Ok(key)
}

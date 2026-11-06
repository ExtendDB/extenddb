// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Request-to-echo resolution of the input and output configurations.

use super::*;

/// The input configuration as the description echoes it: an absent
/// `S3BucketOwner` resolves to the caller's account id and an absent
/// `InputCompressionType` to `NONE` (measured on the service). A
/// value the request supplied is left as submitted, having already passed
/// its own checks.
pub(super) fn resolved_input_configuration(
    submitted: &InputS3Configuration,
    account_id: &str,
) -> InputS3Configuration {
    InputS3Configuration {
        s3_bucket_owner: Some(
            submitted
                .s3_bucket_owner
                .clone()
                .unwrap_or_else(|| account_id.to_owned()),
        ),
        input_compression_type: Some(
            submitted
                .input_compression_type
                .unwrap_or(InputCompressionType::None),
        ),
        ..submitted.clone()
    }
}

/// The output configuration as the description echoes it: an absent
/// `S3BucketOwner` resolves to the caller's account id (measured
/// on both configurations). The output configuration
/// carries no compression member.
pub(super) fn resolved_output_configuration(
    submitted: &OutputS3Configuration,
    account_id: &str,
) -> OutputS3Configuration {
    OutputS3Configuration {
        s3_bucket_owner: Some(
            submitted
                .s3_bucket_owner
                .clone()
                .unwrap_or_else(|| account_id.to_owned()),
        ),
        ..submitted.clone()
    }
}

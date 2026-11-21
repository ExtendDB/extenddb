// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `S3KeyPrefix` checks of step 2: the service's length rule on both
//! prefixes, the path rules of the filesystem store, and the climb rule of
//! an S3-compatible endpoint.

use super::*;
use crate::bulk_put::source::climbs_above_start;

/// Check both prefixes. The service's length rule applies on every store.
/// The path rules (`validate_prefix`, `validate_output_prefix`) are the
/// filesystem store's, which joins a prefix onto a directory path: on the
/// S3 store every such prefix is a valid key prefix, used byte-wise in the
/// listing and verbatim in the report and manifest keys. A server with no
/// store applies them, so step 2 still answers before step 4 there.
///
/// An S3-compatible endpoint resolves a `..` segment against the segment
/// before it, so a prefix with more `..` segments than segments before them
/// names a key above the bucket. The permission checks evaluate the key
/// inside the bucket that resolution keeps, while the endpoint may resolve
/// the URL path into another bucket, so the prefix is refused there with
/// the path rules' text. AWS S3 stores such a key as given.
///
/// # Errors
///
/// The `ValidationException` of the first rule a prefix breaks.
pub(super) fn check_prefixes(
    ctx: &OperationContext,
    input: &StartBulkPutItemsInput,
) -> Result<(), DynamoDbError> {
    let path_rules = ctx
        .bulk
        .as_ref()
        .is_none_or(|store| store.filesystem_root().is_some());
    let climb_rule = ctx
        .bulk
        .as_ref()
        .is_some_and(|store| store.resolves_parent());
    if let Some(prefix) = &input.input_s3_configuration.s3_key_prefix {
        validate_prefix_length_service(prefix, "inputS3Configuration.s3KeyPrefix")?;
        if path_rules {
            validate_prefix(prefix)?;
        }
        if climb_rule {
            refuse_climb(prefix)?;
        }
    }
    if let Some(prefix) = &input.output_s3_configuration.s3_key_prefix {
        validate_prefix_length_service(prefix, "outputS3Configuration.s3KeyPrefix")?;
        if path_rules {
            validate_output_prefix(prefix)?;
        }
        if climb_rule {
            refuse_climb(prefix)?;
        }
    }
    Ok(())
}

fn refuse_climb(prefix: &str) -> Result<(), DynamoDbError> {
    if climbs_above_start(prefix) {
        return Err(DynamoDbError::ValidationException(
            "S3KeyPrefix contains an invalid path component".to_owned(),
        ));
    }
    Ok(())
}

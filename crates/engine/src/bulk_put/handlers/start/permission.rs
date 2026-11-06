// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The caller's `s3:ListBucket` on the input bucket, evaluated at Start
//! before the store's own bucket check.

use super::*;

use crate::bulk_put::authorize::{self, Decision, S3Action};
use crate::bulk_put::source::StoreFault;

/// The terminal-born fault for a caller who may not list the input bucket
/// under the request's prefix, or `None` when the listing is allowed. The
/// service accepts such a Start and fails the operation with
/// `S3AccessDenied` and S3's text naming `s3:ListBucket` on the bucket ARN,
/// with no byte members and no manifest (measured on the service); here the
/// row is born terminal with that shape, before the bucket check and before
/// enumeration. `s3:prefix` is the request's `S3KeyPrefix`, or the empty
/// string when absent, and then, when the store lists another prefix for it
/// (`BulkSource::listed_prefix`), that prefix: the listing is allowed only
/// when both are.
///
/// # Errors
///
/// `InternalServerError` when the decision cannot be made.
pub(super) async fn list_bucket_denial(
    ctx: &OperationContext,
    operation_id: &str,
    bucket: &str,
    prefix: &str,
) -> Result<Option<StoreFault>, DynamoDbError> {
    let listed = ctx
        .bulk
        .as_ref()
        .map(|store| store.source.listed_prefix(prefix))
        .filter(|listed| listed != prefix);
    let mut decision = Decision::Allowed;
    for prefix in std::iter::once(prefix).chain(listed.as_deref()) {
        decision = authorize::decide(
            ctx.bulk_authorizer.as_deref(),
            ctx.bulk_principal.as_ref(),
            S3Action::ListBucket,
            &authorize::bucket_arn(bucket),
            Some(prefix),
            &ctx.account_id,
            operation_id,
        )
        .await;
        if decision != Decision::Allowed {
            break;
        }
    }
    match decision {
        Decision::Allowed => Ok(None),
        Decision::Denied(message) => Ok(Some(StoreFault {
            code: "S3AccessDenied",
            message,
            after_first_byte: false,
        })),
        Decision::Unavailable => Err(DynamoDbError::InternalServerError(
            "Internal server error".to_owned(),
        )),
    }
}

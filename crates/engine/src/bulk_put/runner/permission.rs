// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The runner's checks of the S3 actions the run performs as the
//! operation's caller: `s3:GetObject` on an input object before it is opened
//! and at every checkpoint boundary while it is read, and `s3:PutObject` on a
//! report object before the first record or page that writes it and on the
//! manifest. In developer mode no check runs (`RunnerDeps::authorizer`).

use super::*;

/// The decision for `action` on `resource` for the operation's recorded
/// caller.
pub(super) async fn s3_decision(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
    action: S3Action,
    resource: &str,
) -> Decision {
    authorize::decide(
        deps.authorizer.as_deref(),
        config.caller.as_ref(),
        action,
        resource,
        None,
        &record.account_id,
        &record.operation_id,
    )
    .await
}

/// The failure a decision that is not `Allowed` ends the operation with.
pub(super) fn decision_failure(decision: Decision) -> Option<Failure> {
    match decision {
        Decision::Allowed => None,
        Decision::Denied(message) => Some(Failure {
            code: Cow::Borrowed("S3AccessDenied"),
            message,
        }),
        Decision::Unavailable => Some(Failure::internal()),
    }
}

/// The decision for `s3:PutObject` on the object `key` of the output
/// bucket. `key` is evaluated first and then, when the store writes the
/// object under another key (`BulkSink::stored_key`), that key: the write is
/// allowed only when both are, and a denial names the key whose evaluation
/// denied.
pub(super) async fn put_object_decision(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
    bucket: &str,
    key: &str,
) -> Decision {
    let stored = deps.sink.stored_key(key);
    let keys = std::iter::once(key).chain((stored != key).then_some(stored.as_str()));
    for key in keys {
        let resource = authorize::object_arn(bucket, key);
        let decision = s3_decision(deps, record, config, S3Action::PutObject, &resource).await;
        if decision != Decision::Allowed {
            return decision;
        }
    }
    Decision::Allowed
}

/// The failure for `s3:PutObject` on the object `key` of the output bucket,
/// or `None` when the caller may write it (`put_object_decision`).
pub(super) async fn put_object_failure(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
    bucket: &str,
    key: &str,
) -> Option<Failure> {
    decision_failure(put_object_decision(deps, record, config, bucket, key).await)
}

/// `s3:GetObject` on an input object, before the runner opens it, on a
/// resumed claim as well. A denial ends the operation `S3AccessDenied`;
/// rows already written stay written, and the manifest reports the file
/// `NOT_PROCESSED` when none of its rows was counted and `NOT_AVAILABLE`
/// otherwise (measured on the service).
pub(super) async fn check_get_at_open(
    ctx: &RunContext<'_>,
    st: &mut RunState,
    file: &InputFile,
    file_index: usize,
) -> Option<Flow> {
    let resource = authorize::object_arn(&ctx.config.input_bucket, &file.path);
    let decision = s3_decision(
        ctx.deps,
        ctx.record,
        ctx.config,
        S3Action::GetObject,
        &resource,
    )
    .await;
    if matches!(decision, Decision::Denied(_)) {
        st.unavailable_file = Some(file_index);
    }
    st.failure = Some(decision_failure(decision)?);
    Some(Flow::Finish)
}

/// `s3:GetObject` on the input object being read, at a checkpoint boundary
/// inside it, so a permission removed while the object is read takes effect
/// at the next boundary (measured on the service for a bucket policy changed
/// mid-object: the operation fails `S3AccessDenied` and the file is
/// `NOT_AVAILABLE`). A boundary at the end of a file checks nothing: the next
/// file is checked when it is opened.
pub(super) async fn check_get_at_boundary(ctx: &RunContext<'_>, st: &mut RunState) -> Option<Flow> {
    if !st.current_file_opened {
        return None;
    }
    let file = ctx.record.input_files.get(st.cursor_file)?;
    let file_index = st.cursor_file;
    check_get_at_open(ctx, st, file, file_index).await
}

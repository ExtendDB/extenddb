// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `StartBulkPutItems` handler: the seven observable validation steps,
//! the terminal-born input configuration path, and the start transaction.
//! The other three handlers, the renderers, and the field checks live
//! beside it in the parent module.

use super::*;

use crate::bulk_put::run_context::RecordedRunContext;

mod config;
use config::{resolved_input_configuration, resolved_output_configuration};
mod permission;
mod prefix;
mod replay;

/// An enumeration failure is server-side: the bucket name and prefix
/// passed their lexical checks, so a jail refusal or a read failure here
/// is not client input. Details go to the log, never to the client. Defined
/// here so the line is filed under the Start handler's target.
fn enumeration_error(error: &SourceError) -> DynamoDbError {
    tracing::error!(error = ?error, "bulk operation input enumeration failed");
    DynamoDbError::InternalServerError("Internal server error".to_owned())
}

/// Handle a `StartBulkPutItems` request.
///
/// # Errors
///
/// Returns the step's error per the validation order in the module doc.
#[allow(clippy::too_many_lines)] // the seven validation steps, kept in spec order in one place
pub async fn handle_start_bulk_put_items(
    body: Value,
    ctx: &OperationContext,
) -> Result<Value, DynamoDbError> {
    // The filesystem store's input and report path hardening is unix-only
    // (see the source module doc), so other targets refuse the operation on
    // that store before reading the body rather than running the
    // unhardened fallback. The S3 store has no filesystem code.
    if cfg!(not(unix))
        && ctx
            .bulk
            .as_ref()
            .is_some_and(|store| store.filesystem_root().is_some())
    {
        return Err(DynamoDbError::ValidationException(
            "Bulk operations are not supported on this platform".to_owned(),
        ));
    }

    // Step 1: deserialization and enum membership. A null or absent
    // required member is answered first, as the hosted ValidationException
    // for a null member rather than the serde SerializationException.
    if body
        .as_object()
        .is_some_and(|obj| is_absent_or_null(obj.get("TableName")))
    {
        return Err(DynamoDbError::ValidationException(
            TABLE_NAME_REQUIRED_MESSAGE.to_owned(),
        ));
    }
    require_members(&body, START_REQUIRED)?;
    // The tableName pass, on the raw body. The service judges TableName on
    // its own, before the other members: a bad TableName is answered alone
    // even when the configuration also carries enum or bucket faults
    // (measured with a bare `a*`, an own ARN `table/ab`, and an own index
    // ARN, each beside a bad compression type). The model types TableName
    // as TableArn, with a maximum of 1024 characters reported one-sided
    // (measured with 1025 characters); a bare name is then held to the
    // table-name rules (3 to 255 characters and the name pattern, every
    // failing rule in one envelope), and an ARN to the shared ARN rules,
    // which include the name rule on an own-account ARN. A foreign-account
    // ARN is denied here, before any configuration fault (measured).
    let table_name = match body.get("TableName") {
        Some(Value::String(value)) => {
            if value.chars().count() > MAX_TABLE_NAME_CHARS {
                return Err(DynamoDbError::ValidationException(format!(
                    "1 validation error detected: Value '{value}' at 'tableName' failed to \
                     satisfy constraint: Member must have length less than or equal to \
                     {MAX_TABLE_NAME_CHARS}"
                )));
            }
            if !value.starts_with("arn:") {
                validate_bare_table_name(value)?;
            }
            Some(arn::table_name_from_name_or_arn(
                value,
                &ctx.region,
                &ctx.account_id,
            )?)
        }
        _ => None,
    };
    // The model pass: every enum, pattern, length and range constraint of
    // the other members, answered together in one envelope in the
    // service's member order (measured), on the raw body so an enum
    // outside its set is answered with the full path and the service's set
    // order instead of the serde impls' bare name and declaration order.
    check_model_constraints(&body)?;
    let input: StartBulkPutItemsInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;
    // A TableName of the wrong JSON type is answered by deserialization
    // above, so the name is always resolved here.
    let table_name = match table_name {
        Some(name) => name,
        None => arn::table_name_from_name_or_arn(&input.table_name, &ctx.region, &ctx.account_id)?,
    };

    // Second pass: the service's own `Invalid Request:` checks, after every
    // model constraint above. Their order among themselves is kept as it
    // was, since it is unmeasured.
    if let Some(token) = &input.client_token {
        let cap = ctx.limits.max_bulk_client_token_bytes;
        if token.len() > cap {
            return Err(DynamoDbError::ValidationException(format!(
                "ClientToken exceeds max_bulk_client_token_bytes ({cap} bytes)"
            )));
        }
    }
    if input.output_s3_configuration.output_type.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "Invalid Request: OutputS3Configuration.OutputType cannot be empty".to_owned(),
        ));
    }
    validate_bucket_service(
        &input.input_s3_configuration.s3_bucket,
        "inputS3Configuration.s3Bucket",
        INPUT_BUCKET_MEMBER,
    )?;
    // The output bucket's presence is checked at Start: an output
    // configuration whose OutputType is set with no S3Bucket answers this
    // message (measured on the service). Its existence and its owner
    // are still decided when the first report record is produced. A
    // present bucket with no S3KeyPrefix is accepted.
    let Some(output_bucket) = &input.output_s3_configuration.s3_bucket else {
        return Err(bucket_is_invalid(OUTPUT_BUCKET_MEMBER));
    };
    validate_bucket_service(
        output_bucket,
        "outputS3Configuration.s3Bucket",
        OUTPUT_BUCKET_MEMBER,
    )?;
    prefix::check_prefixes(ctx, &input)?;
    let input_config = serialize_output(&input.input_s3_configuration)?;
    let output_config = serialize_output(&input.output_s3_configuration)?;
    // The description echoes the caller's account as the owner of both
    // buckets, and `NONE` as the input compression, when the request omits
    // them (measured on the service). The resolved values are what
    // the row stores, so every later Describe renders them without
    // re-deriving anything. The fingerprint below is taken from the
    // configurations as submitted, so omitting a member and supplying the
    // value it resolves to stay two different requests for idempotency.
    let stored_input_config = serialize_output(&resolved_input_configuration(
        &input.input_s3_configuration,
        &ctx.account_id,
    ))?;
    let stored_output_config = serialize_output(&resolved_output_configuration(
        &input.output_s3_configuration,
        &ctx.account_id,
    ))?;
    validate_configuration_size(&stored_input_config, &stored_output_config)?;
    validate_configuration_characters(&stored_input_config, &stored_output_config)?;

    // Step 3: modelled features this server does not implement.
    if input.input_s3_configuration.input_format == BulkOperationInputFormat::Ion {
        return Err(DynamoDbError::ValidationException(
            "InputFormat ION is not supported by this server".to_owned(),
        ));
    }

    // Step 4: the bulk store. One store serves inputs and reports; with no
    // [bulk] section the feature is disabled.
    let Some(store) = ctx.bulk.as_ref() else {
        return Err(DynamoDbError::ValidationException(
            "Bulk operations are disabled. Configure [bulk] in extenddb.toml to enable.".to_owned(),
        ));
    };

    // Step 5: table lookup. Not found and not-active reuse the existing
    // messages; the hosted service's documentation accepts ACTIVE and
    // UPDATING.
    let table = ctx
        .storage
        .describe_table(
            &ctx.account_id,
            DescribeTableInput {
                table_name: table_name.clone(),
            },
        )
        .await
        .map_err(|error| match bulk_storage_err(error) {
            // A table that does not exist answers the service's named
            // not-found text at Start (measured on the service): the
            // resource-not-found message carries the table name.
            DynamoDbError::ResourceNotFoundException(_) => {
                DynamoDbError::ResourceNotFoundException(format!(
                    "Requested resource not found: Table: {table_name} not found"
                ))
            }
            other => other,
        })?;
    // A table in DELETING answers a validation failure naming the table as
    // not found, measured both immediately after DeleteTable and three
    // seconds later. CREATING and UPDATING were not
    // measured that day: UPDATING is accepted and CREATING keeps the
    // in-use answer below.
    if table.table_status == TableStatus::Deleting {
        return Err(DynamoDbError::ValidationException(
            TABLE_DELETING_MESSAGE.to_owned(),
        ));
    }
    if !matches!(
        table.table_status,
        TableStatus::Active | TableStatus::Updating
    ) {
        return Err(DynamoDbError::ResourceInUseException(error_message(
            ErrorMessageKey::TableInUse,
            &[&table_name],
        )));
    }
    // A table with an index operation in progress refuses the start
    // (measured on the service: six attempts across
    // both creation phases of a global secondary index, every one
    // `ResourceInUseException` with this text, whether the table reported
    // `UPDATING` or `ACTIVE`). Every secondary and vector index must be
    // `ACTIVE`; on PostgreSQL a global secondary index is `CREATING` for
    // the whole backfill.
    if has_index_operation(&table) {
        return Err(DynamoDbError::ResourceInUseException(
            INDEX_OPERATION_MESSAGE.to_owned(),
        ));
    }
    // The accelerated tier is refused on a table with a vector index with
    // the measured text (a table carrying one active
    // vector index refuses ACCELERATED synchronously and accepts STANDARD).
    if input.performance_tier == Some(BulkOperationPerformanceTier::Accelerated)
        && table
            .vector_indexes
            .as_ref()
            .is_some_and(|indexes| !indexes.is_empty())
    {
        return Err(DynamoDbError::ValidationException(
            "Invalid Request: Value provided for parameter PerformanceTier is not supported for \
             tables with vector indexes."
                .to_owned(),
        ));
    }

    // The request identity for steps 6 and 7. The fingerprint hashes the
    // request as submitted, except for the service defaults a retry may add
    // or omit without changing the operation (measured): an absent
    // PerformanceTier and a STANDARD one hash alike, and
    // `strip_replayed_defaults` drops an InputCompressionType of NONE and an
    // own-account S3BucketOwner on either configuration. The stored record
    // still carries the resolved default. The file list is filled in after
    // enumeration.
    let requested_tier = input.performance_tier.map(tier_str);
    let fingerprint_tier = requested_tier.filter(|tier| *tier != "STANDARD");
    let fingerprint_input = fingerprint::strip_replayed_defaults(&input_config, &ctx.account_id);
    let fingerprint_output = fingerprint::strip_replayed_defaults(&output_config, &ctx.account_id);
    let request_fingerprint = fingerprint::request_fingerprint(
        &table.table_arn,
        &fingerprint_input,
        &fingerprint_output,
        input
            .max_write_request_units
            .and_then(|units| u64::try_from(units).ok()),
        fingerprint_tier,
    );
    let client_token_hash = input
        .client_token
        .as_ref()
        .map(|token| hex::encode(Sha256::digest(token.as_bytes())));
    let mut new = NewBulkOperation {
        operation_id: arn::generate_operation_id(),
        table_id: table.table_id.clone(),
        table_name,
        table_arn: table.table_arn,
        client_token: input.client_token,
        client_token_hash,
        request_fingerprint,
        input_config: stored_input_config,
        output_config: stored_output_config,
        input_files: Vec::new(),
        max_write_request_units: input.max_write_request_units,
        performance_tier: Some(requested_tier.unwrap_or("STANDARD").to_owned()),
        start_time_ms: epoch_millis(),
        // Set by enumeration below; a failure born before the walk leaves
        // it absent, as the hosted service does.
        total_input_bytes: None,
        initial_failure: None,
        run_context: Some(
            RecordedRunContext::at_start(store, ctx.bulk_principal.clone()).to_value(),
        ),
    };

    // A replayed token is answered from its row before any store call, the
    // listing, the byte bound and the failed-start cap (`replay`).
    if let Some(response) = replay::replayed(ctx, &new).await? {
        return Ok(response);
    }

    // The input bucket as the store sees it, for the bucket check and the
    // enumeration.
    let input_at = BucketRef::new(
        &ctx.account_id,
        &input.input_s3_configuration.s3_bucket,
        input.input_s3_configuration.s3_bucket_owner.as_deref(),
    );
    let prefix = input
        .input_s3_configuration
        .s3_key_prefix
        .clone()
        .unwrap_or_default();

    // A bad input configuration fails the operation rather than the
    // request: the row is inserted already terminal, the token consumed,
    // before any row is read. The store decides the
    // input bucket's owner, then its existence, and renders the fault (see
    // `BulkSource::check_bucket`); any other failure of the check is the
    // server's. Of the output configuration only the bucket member's
    // presence and name are checked at Start (measured on the service); its
    // existence and its owner are checked at the first report record. The
    // insert skips the account limit, which binds on non-terminal rows
    // only, so this path runs before the saturated-account check below. The
    // per-table guard still binds: on a table with an operation running the
    // start transaction answers busy and inserts nothing.
    // The caller's s3:ListBucket is decided first (`permission`).
    let denied =
        permission::list_bucket_denial(ctx, &new.operation_id, input_at.bucket, &prefix).await?;
    let initial_failure = match denied {
        Some(fault) => Some(fault),
        None => match store.source.check_bucket(&input_at, &prefix).await {
            Ok(()) => None,
            Err(SourceError::Store(fault)) => Some(fault),
            Err(error) => return Err(enumeration_error(&error)),
        },
    };
    // An SSE-KMS output key the store cannot use fails the operation the
    // same way, before the input walk and with both byte members absent
    // (measured on the service); a bucket or owner fault of the output is
    // left to the first report record. The key checked is report object 0
    // of the error report.
    let initial_failure = match initial_failure {
        Some(fault) => Some(fault),
        None => {
            let output_at = BucketRef::new(
                &ctx.account_id,
                output_bucket,
                input.output_s3_configuration.s3_bucket_owner.as_deref(),
            );
            let key = crate::bulk_put::report::report_key(
                input
                    .output_s3_configuration
                    .s3_key_prefix
                    .as_deref()
                    .unwrap_or_default(),
                &ctx.account_id,
                &new.operation_id,
                extenddb_storage::ReportKind::Error,
                0,
            );
            let sse = crate::bulk_put::source::OutputSse::of(&input.output_s3_configuration);
            match store.sink.check_encryption(&output_at, &key, &sse).await {
                Ok(()) => None,
                Err(crate::bulk_put::source::SinkError::Store(fault)) => Some(fault),
                Err(error) => {
                    tracing::error!(error = ?error, "bulk operation output encryption check failed");
                    return Err(DynamoDbError::InternalServerError(
                        "Internal server error".to_owned(),
                    ));
                }
            }
        }
    };
    if let Some(fault) = initial_failure {
        return start_terminal_born(ctx, new, fault).await;
    }

    // A saturated account is answered before the enumeration walk runs.
    // The start transaction keeps its own exact check under its locks;
    // this read only decides whether the walk runs. A count at or above
    // the limit goes straight to the transaction with a zero limit, which
    // can answer everything the limit does not outrank: a replayed token
    // is still answered from its existing row and a busy table still
    // answers busy, in the transaction's own order. A zero limit admits
    // nothing, so `Created` cannot arrive from this call.
    let account_limit = ctx.limits.max_concurrent_bulk_operations;
    let non_terminal = ctx
        .storage
        .count_non_terminal(&ctx.account_id)
        .await
        .map_err(bulk_storage_err)?;
    if non_terminal >= u64::from(account_limit) {
        let outcome = ctx
            .storage
            .start_bulk_operation(&ctx.account_id, new, 0)
            .await
            .map_err(bulk_storage_err)?;
        return start_response(outcome, &ctx.region, account_limit);
    }

    // Step 6: enumeration fixes the file list and the total. The count cap
    // binds during the walk as well as after it, then the two caps apply,
    // count before bytes.
    //
    // The object count quota is not a Start refusal: the walk stops one
    // object past the cap and returns what it collected, the record is
    // inserted with that list, and the runner ends the operation FAILED
    // with S3ObjectsCountLimitExceeded before reading a file (measured
    // ). The per-object size cap is the runner's as
    // well. Only the enumerated total stays a Start refusal; it
    // is ExtendDB's own bound.
    let max_files = ctx.limits.max_bulk_input_files;
    let files = match store.source.list(&input_at, &prefix, max_files).await {
        Ok(files) => files,
        // A fault the store rendered on a later page (the bucket removed or
        // access lost after the check) fails the operation as the check
        // would have.
        Err(SourceError::Store(fault)) => return start_terminal_born(ctx, new, fault).await,
        // Only the walk's visit ceiling (four times the cap in entries that
        // can hold a key under the prefix) still ends the enumeration here.
        Err(SourceError::TooManyFiles) => {
            return Err(DynamoDbError::ValidationException(format!(
                "Input exceeds max_bulk_input_files ({max_files} files)"
            )));
        }
        Err(other) => return Err(enumeration_error(&other)),
    };
    let total_input_bytes = files
        .iter()
        .fold(0_u64, |sum, file| sum.saturating_add(file.size));
    let max_bytes = ctx.limits.max_bulk_input_bytes;
    if total_input_bytes > max_bytes {
        return Err(DynamoDbError::ValidationException(format!(
            "Input exceeds max_bulk_input_bytes ({max_bytes} bytes)"
        )));
    }

    // Step 7: the start transaction, on the enumerated input.
    new.input_files = files
        .into_iter()
        .map(|file| InputFile {
            path: file.path,
            size: i64::try_from(file.size).unwrap_or(i64::MAX),
            rows: None,
            etag: file.etag,
            abandon: None,
        })
        .collect();
    new.total_input_bytes = Some(i64::try_from(total_input_bytes).unwrap_or(i64::MAX));
    let outcome = ctx
        .storage
        .start_bulk_operation(&ctx.account_id, new, account_limit)
        .await
        .map_err(bulk_storage_err)?;
    // Only the handler can tell a creation from an idempotent replay; the
    // wire answers are identical, so the metric and the creation log line
    // are recorded here.
    if let StartOutcome::Created(record) = &outcome {
        ctx.metrics.record(
            extenddb_core::metrics::MetricName::BulkOperationsStarted,
            1.0,
            Some(&record.table_name),
            None,
            None,
        );
        tracing::info!(
            account_id = %record.account_id,
            operation_id = %record.operation_id,
            table_name = %record.table_name,
            status = %record.status,
            processed_item_count = record.processed_item_count,
            failed_item_count = record.failed_item_count,
            "bulk operation created"
        );
    }
    start_response(outcome, &ctx.region, account_limit)
}

/// Insert the operation already terminal with a fault the store rendered:
/// a bad input bucket found by the check, or by a later page of the
/// enumeration (the bucket removed or access lost after the check). The
/// row is born `FAILED`, the token consumed, before any row is read. On a
/// table with an operation running nothing is inserted and the answer is
/// the busy one, as for any other start.
async fn start_terminal_born(
    ctx: &OperationContext,
    mut new: NewBulkOperation,
    fault: crate::bulk_put::source::StoreFault,
) -> Result<Value, DynamoDbError> {
    // Terminal-born rows are bounded: at or
    // above the cap of them born in the last hour, the request is
    // refused. This read is a cheap fast path that rejects an
    // obviously-over-cap caller before the start transaction; the
    // authoritative, exact check runs inside that transaction under
    // the per-account lock, so a concurrent burst that races past this
    // read is still refused. The cap and window travel with the failure
    // they govern. The floor keeps a zero configuration from refusing
    // every missing-bucket start.
    let cap = ctx.limits.max_bulk_failed_starts_per_hour.max(1);
    let since_ms = new.start_time_ms.saturating_sub(3_600_000);
    let born = ctx
        .storage
        .count_terminal_born_since(&ctx.account_id, since_ms)
        .await
        .map_err(bulk_storage_err)?;
    if born >= cap {
        // The transaction answers a busy table before the cap, so at the
        // cap the fast path refuses only a table with no operation
        // running and leaves a busy one to the transaction.
        let running = ctx
            .storage
            .count_non_terminal_for_table(&ctx.account_id, &new.table_id)
            .await
            .map_err(bulk_storage_err)?;
        if running == 0 {
            return Err(DynamoDbError::ThrottlingException(
                "Rate of requests exceeds the allowed throughput.".to_owned(),
            ));
        }
    }
    new.initial_failure = Some(extenddb_storage::InitialFailure {
        code: fault.code.to_owned(),
        message: fault.message,
        cap,
        window_start_ms: since_ms,
    });
    let outcome = ctx
        .storage
        .start_bulk_operation(
            &ctx.account_id,
            new,
            ctx.limits.max_concurrent_bulk_operations,
        )
        .await
        .map_err(bulk_storage_err)?;
    if let StartOutcome::Created(record) = &outcome {
        // Started and finished in the same instant: both metrics
        // count the operation, the finish carrying the FAILED status.
        // No worker sees the row, so only the handler can record the
        // finish.
        ctx.metrics.record(
            extenddb_core::metrics::MetricName::BulkOperationsStarted,
            1.0,
            Some(&record.table_name),
            None,
            None,
        );
        ctx.metrics.record(
            extenddb_core::metrics::MetricName::BulkOperationsFinished,
            1.0,
            Some(&record.table_name),
            None,
            Some("FAILED"),
        );
        tracing::info!(
            account_id = %record.account_id,
            operation_id = %record.operation_id,
            table_name = %record.table_name,
            status = %record.status,
            failure_code = record.failure_code.as_deref().unwrap_or(""),
            "bulk operation created terminal: the input configuration failed"
        );
    }
    start_response(
        outcome,
        &ctx.region,
        ctx.limits.max_concurrent_bulk_operations,
    )
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `StartBulkPutItems` handler: the observable validation steps and
//! the start transaction. The input location is checked and listed by the
//! runner on the operation's first claim (`runner::enumerate`). The other
//! three handlers, the renderers, and the field checks live beside it in
//! the parent module.

use super::*;

use crate::bulk_put::run_context::RecordedRunContext;

mod config;
use config::{resolved_input_configuration, resolved_output_configuration};
mod prefix;

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
    validate_output_encryption(&input.output_s3_configuration)?;
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

    // The request identity. The fingerprint hashes the
    // request as submitted, except for the service defaults a retry may add
    // or omit without changing the operation (measured): an absent
    // PerformanceTier and a STANDARD one hash alike, and
    // `strip_replayed_defaults` drops an InputCompressionType of NONE and an
    // own-account S3BucketOwner on either configuration. The stored record
    // still carries the resolved default.
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
    // The input location is not read here. The service answers a Start
    // naming a missing input bucket with HTTP 200 `IN_PROGRESS`, and the
    // operation then ends `FAILED` `S3NoSuchBucket`; its Start answer
    // carries no byte member (measured on the service). The runner checks
    // the caller's listing permission, the bucket, its owner and the output
    // key, then lists the input and records the files and their total, on
    // the operation's first claim (`runner::enumerate`).
    let new = NewBulkOperation {
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
        total_input_bytes: None,
        run_context: Some(
            RecordedRunContext::at_start(store, ctx.bulk_principal.clone()).to_value(),
        ),
    };

    // The start transaction answers a replayed token from its row before
    // the per-table guard and the account's limit in the request's
    // PerformanceTier.
    let account_limit = ctx.limits.max_concurrent_bulk_operations;
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
            Some(&record.account_id),
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

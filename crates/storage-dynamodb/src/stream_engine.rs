// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `StreamEngine` implementation for the DynamoDB-at-home backend — honest stubs.
//!
//! DynamoDB Streams is a separate API surface that this v1 backend does not
//! implement. Each method names the DynamoDB Streams call it would map to.
//! `cleanup_expired_stream_records` is a maintenance no-op (DynamoDB Streams
//! retains records for 24 hours and expires them automatically).

use futures::future::BoxFuture;

use extenddb_core::types::{DescribeStreamInput, StreamDescription, StreamRecord};
use extenddb_storage::error::StorageError;
use extenddb_storage::{StreamEngine, StreamListResult, StreamRecordsResult};

use crate::DynamoEngine;

impl StreamEngine for DynamoEngine {
    fn write_stream_record(
        &self,
        _account_id: &str,
        _record: &StreamRecord,
        _shard_id: &str,
        _table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        // DynamoDB produces stream records natively on every write; ExtendDB does
        // not insert them. This would map to DynamoDB Streams (records are produced
        // by DynamoDB itself, not via an explicit write API).
        Box::pin(async {
            Err(StorageError::Internal(
                "Streams are not implemented in the dynamodb backend (v1). \
                 DynamoDB produces stream records itself — there is no write API."
                    .into(),
            ))
        })
    }

    fn get_stream_records(
        &self,
        _shard_id: &str,
        _after_sequence: Option<&str>,
        _limit: i64,
    ) -> BoxFuture<'_, StreamRecordsResult> {
        // Maps to DynamoDB Streams GetShardIterator + GetRecords.
        Box::pin(async {
            Err(StorageError::Internal(
                "Streams are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB Streams GetShardIterator + GetRecords."
                    .into(),
            ))
        })
    }

    fn describe_stream(
        &self,
        _account_id: &str,
        _input: &DescribeStreamInput,
    ) -> BoxFuture<'_, Result<StreamDescription, StorageError>> {
        // Maps to DynamoDB Streams DescribeStream.
        Box::pin(async {
            Err(StorageError::Internal(
                "Streams are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB Streams DescribeStream."
                    .into(),
            ))
        })
    }

    fn list_streams(
        &self,
        _account_id: &str,
        _table_name: Option<&str>,
        _limit: i64,
        _exclusive_start_stream_arn: Option<&str>,
    ) -> BoxFuture<'_, StreamListResult> {
        // Maps to DynamoDB Streams ListStreams.
        Box::pin(async {
            Err(StorageError::Internal(
                "Streams are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB Streams ListStreams."
                    .into(),
            ))
        })
    }

    fn cleanup_expired_stream_records(
        &self,
        _retention_hours: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        // DynamoDB Streams retains records for 24 hours and expires them automatically.
        // No explicit cleanup is needed or possible via the API.
        Box::pin(async { Ok(0) })
    }

    fn assign_shard(
        &self,
        _account_id: &str,
        _table_name: &str,
        _partition_key: &str,
    ) -> BoxFuture<'_, Result<String, StorageError>> {
        // Maps to DynamoDB Streams shard management (shard assignment is implicit in the stream).
        Box::pin(async {
            Err(StorageError::Internal(
                "Streams are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB Streams shard management."
                    .into(),
            ))
        })
    }

    fn next_sequence_number(&self, _shard_id: &str) -> BoxFuture<'_, Result<String, StorageError>> {
        // Maps to DynamoDB Streams shard management (sequence numbers are assigned by DynamoDB).
        Box::pin(async {
            Err(StorageError::Internal(
                "Streams are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB Streams shard management."
                    .into(),
            ))
        })
    }

    fn validate_shard(
        &self,
        _account_id: &str,
        _stream_arn: &str,
        _shard_id: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        // Maps to DynamoDB Streams shard management (DescribeStream to verify shard membership).
        Box::pin(async {
            Err(StorageError::Internal(
                "Streams are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB Streams shard management (DescribeStream)."
                    .into(),
            ))
        })
    }

    fn latest_sequence_number(
        &self,
        _shard_id: &str,
    ) -> BoxFuture<'_, Result<Option<String>, StorageError>> {
        // Maps to DynamoDB Streams shard management (GetShardIterator with LATEST).
        Box::pin(async {
            Err(StorageError::Internal(
                "Streams are not implemented in the dynamodb backend (v1). \
                 Maps to DynamoDB Streams shard management (GetShardIterator LATEST)."
                    .into(),
            ))
        })
    }
}

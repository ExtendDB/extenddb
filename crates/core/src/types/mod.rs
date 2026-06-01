// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
mod attribute_value;
mod backup;
mod batch;
mod capacity;
mod import_export;
mod item;
mod key_schema;
mod query;
mod stream;
mod table;
mod transaction;

pub use attribute_value::AttributeValue;
pub use backup::{
    BackupDescription, BackupDetails, BackupSummary, ContinuousBackupsDescription,
    PointInTimeRecoveryDescription, SourceTableDetails,
};
pub use batch::{
    BatchGetItemInput, BatchGetItemOutput, BatchWriteItemInput, BatchWriteItemOutput,
    DeleteRequest, KeysAndAttributes, PutRequest, WriteRequest,
};
pub use capacity::{
    Capacity, ConsumedCapacity, ItemCollectionMetrics, ReturnConsumedCapacity,
    ReturnItemCollectionMetrics, ReturnValuesOnConditionCheckFailure,
};
pub use import_export::{
    CsvOptions, ExportDescription, ExportFormat, ExportStatus, ExportTableToPointInTimeInput,
    ExportTableToPointInTimeOutput, FileSource, ImportStatus, ImportTableDescription,
    ImportTableInput, ImportTableOutput, InputFormat, InputFormatOptions, TableCreationParameters,
};
pub use item::{
    AttributeValueUpdate, ConditionalOperator, DeleteItemInput, DeleteItemOutput,
    ExpectedAttributeValue, GetItemInput, GetItemOutput, Item, PutItemInput, PutItemOutput,
    ReturnValues, UpdateItemInput, UpdateItemOutput, attribute_value_size, extract_key,
    item_size_bytes,
};
pub use key_schema::{
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, KeyType, ScalarAttributeType,
    TableKeyInfo, TableReadInfo, hash_key_elements, is_multipart_key_schema, range_key_elements,
};
pub use query::{Condition, QueryInput, QueryOutput, ScanInput, ScanOutput, Select};
pub use stream::{
    DescribeStreamInput, DescribeStreamOutput, GetRecordsInput, GetRecordsOutput,
    GetShardIteratorInput, GetShardIteratorOutput, ListStreamsInput, ListStreamsOutput,
    SequenceNumberRange, Shard, ShardIteratorType, StreamDescription, StreamEventName,
    StreamRecord, StreamRecordData, StreamStatus, StreamSummary, UserIdentity,
};
pub use table::{
    BillingMode, BillingModeSummary, CreateGsiAction, CreateTableInput, CreateTableOutput,
    DeleteGsiAction, DeleteTableInput, DeleteTableOutput, DescribeLimitsOutput, DescribeTableInput,
    DescribeTableOutput, DescribeTimeToLiveInput, DescribeTimeToLiveOutput,
    GlobalSecondaryIndexUpdate, GsiDescription, GsiInput, ListTablesInput, ListTablesOutput,
    ListTagsOfResourceInput, ListTagsOfResourceOutput, LsiDescription, LsiInput, Projection,
    ProjectionType, ProvisionedThroughput, ProvisionedThroughputDescription, SseDescription,
    SseType, StreamSpecification, StreamViewType, TableDescription, TableStatus, Tag,
    TagResourceInput, TimeToLiveDescription, TimeToLiveSpecification,
    TimeToLiveSpecificationOutput, TimeToLiveStatus, UntagResourceInput, UpdateGsiAction,
    UpdateTableInput, UpdateTableOutput, UpdateTimeToLiveInput, UpdateTimeToLiveOutput,
};
pub use transaction::{
    CancellationReason, ItemResponse, TransactConditionCheck, TransactDelete, TransactGet,
    TransactGetItem, TransactGetItemsInput, TransactGetItemsOutput, TransactPut, TransactUpdate,
    TransactWriteItem, TransactWriteItemsInput, TransactWriteItemsOutput,
};

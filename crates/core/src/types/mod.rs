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
    ReturnItemCollectionMetrics, ReturnValuesOnConditionCheckFailure, VectorCapacity,
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
    AttributeDefinition, IndexInfo, IndexType, KeySchemaElement, KeyType, PartitionedIndexes,
    ScalarAttributeType, TableKeyInfo, VectorIndexKeyInfo, hash_key_elements,
    is_multipart_key_schema, partition_indexes, range_key_elements,
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
    DeleteGsiAction, DeleteTableInput, DeleteTableOutput, DeleteVectorIndexAction,
    DescribeLimitsOutput, DescribeTableInput, DescribeTableOutput, DescribeTimeToLiveInput,
    DescribeTimeToLiveOutput, DistanceFunction, GlobalSecondaryIndexUpdate, GsiDescription,
    GsiInput, IndexStatus, ListTablesInput, ListTablesOutput, ListTagsOfResourceInput,
    ListTagsOfResourceOutput, LsiDescription, LsiInput, MAX_VECTOR_INDEXES_PER_TABLE,
    OnDemandThroughput, Projection, ProjectionType, ProvisionedThroughput,
    ProvisionedThroughputDescription, RestoreSummary, SearchSchemaElement, SearchSchemaElementType,
    SseDescription, SseType, StreamSpecification, StreamViewType, TableDescription, TableStatus,
    Tag, TagResourceInput, TimeToLiveDescription, TimeToLiveSpecification,
    TimeToLiveSpecificationOutput, TimeToLiveStatus, UntagResourceInput, UpdateGsiAction,
    UpdateTableInput, UpdateTableOutput, UpdateTimeToLiveInput, UpdateTimeToLiveOutput,
    VECTOR_INDEX_ALREADY_EXISTS, VECTOR_INDEX_COUNT_LIMIT_CREATE, VECTOR_INDEX_COUNT_LIMIT_UPDATE,
    VECTOR_INDEX_CREATE_IN_USE_PREFIX, VECTOR_INDEX_REQUIRES_PAY_PER_REQUEST,
    VECTOR_SEARCH_SCHEMA_UNDECLARED, VectorAttribute, VectorIndexDescription,
    VectorIndexSpecification, VectorIndexUpdate, vector_attribute_conflicting_definition,
    vector_attribute_redefines_key,
};
pub use transaction::{
    CancellationReason, ItemResponse, TransactConditionCheck, TransactDelete, TransactGet,
    TransactGetItem, TransactGetItemsInput, TransactGetItemsOutput, TransactPut, TransactUpdate,
    TransactWriteItem, TransactWriteItemsInput, TransactWriteItemsOutput,
};

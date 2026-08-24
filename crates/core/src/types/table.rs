// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
use serde::{Deserialize, Serialize};

use super::key_schema::{AttributeDefinition, KeySchemaElement};

// --- Enums ---

/// Billing mode for a Virtual `DynamoDB` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BillingMode {
    /// Provisioned capacity with explicit RCU/WCU.
    Provisioned,
    /// On-demand capacity — pay per request.
    PayPerRequest,
}

/// Current status of a Virtual `DynamoDB` table.
///
/// `Creating` is the [`Default`] so that [`TableDescription`] can derive
/// [`Default`], which is what lets a storage backend build one with
/// `..Default::default()` and stay unaffected when a field is added to the
/// description. `Creating` rather than `Active` because a partially built
/// description has certainly not been confirmed ready, so if a default ever does
/// leak it understates rather than overstates readiness. Every real construction
/// site sets the status explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TableStatus {
    /// Table is being created.
    #[default]
    Creating,
    /// Table is ready for use.
    Active,
    /// Table is being deleted.
    Deleting,
    /// Table is being updated.
    Updating,
}

/// Projection type for a secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProjectionType {
    /// All attributes are projected.
    All,
    /// Only key attributes are projected.
    KeysOnly,
    /// Key attributes plus specified non-key attributes are projected.
    Include,
}

/// Distance function for a vector index. Selects the similarity metric used by
/// SearchVectors. Valid values: COSINE, EUCLIDEAN, DOT_PRODUCT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DistanceFunction {
    /// Cosine distance. SearchVectors reports the cosine distance.
    #[serde(rename = "COSINE")]
    Cosine,
    /// Euclidean (L2) distance. SearchVectors reports the L2 distance.
    #[serde(rename = "EUCLIDEAN")]
    Euclidean,
    /// Dot product (inner product). SearchVectors reports the inner product.
    #[serde(rename = "DOT_PRODUCT")]
    DotProduct,
}

impl DistanceFunction {
    /// Whether score `a` ranks ahead of score `b` under this distance function.
    ///
    /// Use this rather than comparing scores directly. Cosine and Euclidean are
    /// distances, so smaller wins; dot product is a similarity, so larger wins.
    /// Hand-rolled comparisons are the usual source of silently reversed
    /// rankings, and the direction is not uniform across the three.
    #[must_use]
    pub fn ranks_before(self, a: f64, b: f64) -> bool {
        match self {
            Self::Cosine | Self::Euclidean => a < b,
            Self::DotProduct => a > b,
        }
    }
}

impl<'de> serde::Deserialize<'de> for DistanceFunction {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "COSINE" => Ok(Self::Cosine),
            "EUCLIDEAN" => Ok(Self::Euclidean),
            "DOT_PRODUCT" => Ok(Self::DotProduct),
            // Enum order measured against the live service 2026-08-06, and it is
            // neither alphabetical nor this enum's declaration order:
            //   [DOT_PRODUCT, COSINE, EUCLIDEAN]
            // An earlier version guessed alphabetical and was wrong.
            //
            // KNOWN DIVERGENCE: the service reports the positional path
            // 'vectorIndexes.1.member.distanceFunction'; this says
            // 'distanceFunction'. A serde deserializer for the enum cannot know its
            // index within the request, so closing this means deserialising the
            // field permissively and validating it in `validate_one_vector_index`,
            // which already does exactly that for the required `Projection` and
            // knows the 1-based position. Deliberately left as a separate change.
            other => Err(serde::de::Error::custom(format!(
                "1 validation error detected: Value '{other}' at 'distanceFunction' \
                 failed to satisfy constraint: Member must satisfy enum value set: [DOT_PRODUCT, COSINE, EUCLIDEAN]"
            ))),
        }
    }
}

/// View type for Virtual `DynamoDB` Streams records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamViewType {
    /// Only key attributes.
    KeysOnly,
    /// The entire item after modification.
    NewImage,
    /// The entire item before modification.
    OldImage,
    /// Both old and new images.
    NewAndOldImages,
}

/// Server-side encryption type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SseType {
    /// Amazon S3-managed encryption.
    AES256,
    /// AWS KMS-managed encryption.
    KMS,
}

// --- Structs ---

/// Provisioned throughput settings for a table or GSI (input).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvisionedThroughput {
    #[serde(rename = "ReadCapacityUnits")]
    pub read_capacity_units: i64,
    #[serde(rename = "WriteCapacityUnits")]
    pub write_capacity_units: i64,
}

/// Provisioned throughput description returned in responses.
///
/// Derives [`Default`] so [`TableDescription`] can. All-zero is the value a
/// PAY_PER_REQUEST table reports, so the default is meaningful rather than
/// invented.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProvisionedThroughputDescription {
    #[serde(rename = "ReadCapacityUnits")]
    pub read_capacity_units: i64,
    #[serde(rename = "WriteCapacityUnits")]
    pub write_capacity_units: i64,
    #[serde(rename = "NumberOfDecreasesToday")]
    pub number_of_decreases_today: i64,
    #[serde(
        rename = "LastIncreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_increase_date_time: Option<f64>,
    #[serde(
        rename = "LastDecreaseDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_decrease_date_time: Option<f64>,
}

/// Index projection configuration — which attributes are copied into the index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Projection {
    #[serde(rename = "ProjectionType")]
    pub projection_type: ProjectionType,
    #[serde(rename = "NonKeyAttributes", skip_serializing_if = "Option::is_none")]
    pub non_key_attributes: Option<Vec<String>>,
}

/// Virtual `DynamoDB` Streams configuration for a table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSpecification {
    #[serde(rename = "StreamEnabled")]
    pub stream_enabled: bool,
    #[serde(rename = "StreamViewType", skip_serializing_if = "Option::is_none")]
    pub stream_view_type: Option<StreamViewType>,
}

/// Server-side encryption description.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SseDescription {
    #[serde(rename = "Status")]
    pub status: String,
    #[serde(rename = "SSEType", skip_serializing_if = "Option::is_none")]
    pub sse_type: Option<SseType>,
    #[serde(rename = "KMSMasterKeyArn", skip_serializing_if = "Option::is_none")]
    pub kms_master_key_arn: Option<String>,
}

/// On-demand throughput settings for a table or index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnDemandThroughput {
    #[serde(rename = "MaxReadRequestUnits")]
    pub max_read_request_units: Option<i64>,
    #[serde(rename = "MaxWriteRequestUnits")]
    pub max_write_request_units: Option<i64>,
}

/// Summary of the table's billing mode and last update timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BillingModeSummary {
    #[serde(rename = "BillingMode")]
    pub billing_mode: BillingMode,
    #[serde(
        rename = "LastUpdateToPayPerRequestDateTime",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_update_to_pay_per_request_date_time: Option<f64>,
}

/// A key-value tag attached to a Virtual `DynamoDB` resource.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tag {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Value")]
    pub value: String,
}

/// Global secondary index description returned in responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GsiDescription {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(rename = "IndexStatus")]
    pub index_status: String,
    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughputDescription>,
    #[serde(rename = "IndexSizeBytes")]
    pub index_size_bytes: i64,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "IndexArn")]
    pub index_arn: String,
}

/// Local secondary index description returned in responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LsiDescription {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(rename = "IndexSizeBytes")]
    pub index_size_bytes: i64,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "IndexArn")]
    pub index_arn: String,
}

/// Global secondary index definition for `CreateTable` requests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GsiInput {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

/// Local secondary index definition for `CreateTable` requests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LsiInput {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
}

/// The item attribute that holds the vector for a vector index.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorAttribute {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
}

/// Role of a vector-index search-schema element.
///
/// A search schema declares the scalar attributes that a vector search may
/// filter on: at most one partition key (`HASH`) and any number of
/// inline-filter keys (`INLINE_FILTER`).
///
/// The `HASH` element is OPTIONAL, measured against the live service. Declaring
/// one makes `SearchConditionExpression` required and scopes the search to a
/// partition; omitting it searches the whole table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchSchemaElementType {
    #[serde(rename = "HASH")]
    Hash,
    #[serde(rename = "INLINE_FILTER")]
    InlineFilter,
}

/// A single element of a vector index search schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchSchemaElement {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "SearchSchemaElementType")]
    pub element_type: SearchSchemaElementType,
}

impl VectorIndexSpecification {
    /// Collapse an empty `SearchSchema` to an absent one.
    ///
    /// A request may carry `SearchSchema: []`, and it means the same thing as
    /// omitting the member: the index is unscoped and a search spans the table.
    /// Amazon DynamoDB reports either an absent member or a populated one and
    /// never an empty list, so storing the empty list would create a third state
    /// that a describe echoes back and no client expects, and would let two
    /// backends answer the same request differently depending on whether each
    /// remembered to collapse it.
    ///
    /// Applied on the request paths, before any backend sees the specification,
    /// and again by the backends themselves so that a caller reaching the storage
    /// trait directly cannot store the third state either.
    pub fn normalize_search_schema(&mut self) {
        if self
            .search_schema
            .as_ref()
            .is_some_and(|elements| elements.is_empty())
        {
            self.search_schema = None;
        }
    }

    /// The search schema as it should be stored: absent when empty.
    ///
    /// The borrowing form of [`Self::normalize_search_schema`], for a caller that
    /// is serialising rather than holding a mutable specification.
    #[must_use]
    pub fn search_schema_for_storage(&self) -> Option<&[SearchSchemaElement]> {
        self.search_schema
            .as_deref()
            .filter(|elements| !elements.is_empty())
    }
}

/// Vector index definition for `CreateTable` requests.
///
/// A vector index is a specialized global secondary index that supports
/// similarity search over a vector-valued attribute via the SearchVectors API.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorIndexSpecification {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "Dimensions")]
    pub dimensions: u32,
    #[serde(rename = "DistanceFunction")]
    pub distance_function: DistanceFunction,
    #[serde(rename = "VectorAttribute")]
    pub vector_attribute: VectorAttribute,
    #[serde(
        rename = "SearchSchema",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub search_schema: Option<Vec<SearchSchemaElement>>,
    /// Required by the service, but deserialised as `Option` so that omitting it
    /// is reported with the service's own message rather than a serde failure.
    /// See `validate_vector_indexes`.
    #[serde(
        rename = "Projection",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub projection: Option<Projection>,
}

/// Lifecycle status of a secondary index.
///
/// The value set is the service's own: `CREATING`, `UPDATING`, `DELETING`,
/// `ACTIVE`, taken from the shared `IndexStatus` shape in the service model, so
/// it is not specific to vector indexes even though only they are typed here for
/// now. `GsiDescription` still carries a bare `String`; converting it would change
/// an existing wire-facing type and belongs in its own change.
///
/// `Unknown` catches a value the service adds later. Without it a new status would
/// make `DescribeTable` fail to parse, turning a forward-compatible response into
/// an outage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IndexStatus {
    /// Being created, and not yet able to serve.
    #[default]
    Creating,
    /// Being modified.
    Updating,
    /// Being deleted.
    Deleting,
    /// Able to serve complete results.
    Active,
    /// A status this build does not recognise.
    #[serde(other)]
    Unknown,
}

impl IndexStatus {
    /// Whether the index can serve complete results.
    #[must_use]
    pub fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Vector index description returned in responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorIndexDescription {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "VectorAttribute")]
    pub vector_attribute: VectorAttribute,
    #[serde(rename = "Dimensions")]
    pub dimensions: u32,
    #[serde(
        rename = "SearchSchema",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub search_schema: Option<Vec<SearchSchemaElement>>,
    #[serde(rename = "DistanceFunction")]
    pub distance_function: DistanceFunction,
    #[serde(rename = "IndexStatus")]
    pub index_status: IndexStatus,
    /// Whether the index is still being populated.
    ///
    /// Measured against the service on 2026-08-06 in us-east-1, by seeding 3000
    /// items of 1024 dimensions and then adding the index with `UpdateTable`, so
    /// the backfill was slow enough to observe. It took 8.5 minutes. Three
    /// distinct states appeared, in this order:
    ///
    /// | elapsed | `IndexStatus` | `Backfilling` |
    /// |---|---|---|
    /// | +0.01s | `CREATING` | present, `false` |
    /// | +30.8s | `CREATING` | present, `true` |
    /// | +511.5s | `ACTIVE` | absent |
    ///
    /// Two consequences for anyone implementing this. Present does not imply
    /// backfilling: the member appears as `false` first, meaning the index exists
    /// but its backfill has not started, so a client must read the value rather
    /// than test for presence. And the member is removed once the index is
    /// `ACTIVE` rather than reported as `false`, which matches the documented GSI
    /// behaviour, so `None` here must serialise as an absent member.
    ///
    /// An earlier probe saw only the absence-when-`ACTIVE` case and wrongly
    /// inferred the flag might never be set at all. It was measuring an index over
    /// a handful of tiny items, which finished backfilling before the first poll.
    #[serde(
        rename = "Backfilling",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub backfilling: Option<bool>,
    #[serde(rename = "IndexSizeBytes")]
    pub index_size_bytes: i64,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "IndexArn")]
    pub index_arn: String,
    /// Returned by the service on every vector index description.
    #[serde(
        rename = "Projection",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub projection: Option<Projection>,
}

/// A single change to a table's vector indexes, as carried by
/// `UpdateTable.VectorIndexUpdates`.
///
/// Exactly one action per element, mirroring `GlobalSecondaryIndexUpdate`. The
/// service accepts a list, so several changes may arrive in one request.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
pub struct VectorIndexUpdate {
    #[serde(rename = "Create", skip_serializing_if = "Option::is_none", default)]
    pub create: Option<VectorIndexSpecification>,
    #[serde(rename = "Delete", skip_serializing_if = "Option::is_none", default)]
    pub delete: Option<DeleteVectorIndexAction>,
}

/// Remove a vector index from an existing table.
///
/// Measured on 2026-08-06: the service accepts a delete of an index that is still
/// backfilling, and of one that is `ACTIVE`. Core therefore imposes no readiness
/// condition on deletion; an earlier instinct to forbid deleting a backfilling
/// index would have been stricter than the service.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct DeleteVectorIndexAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,
}

/// Message the service returns when a vector index is created whose name is taken
/// by an index that has finished building.
///
/// Measured on 2026-08-06, and paired with
/// [`VECTOR_INDEX_CREATE_IN_USE_PREFIX`]: the error *class* depends on the state
/// of the existing index, so a backend cannot report one message for both.
/// `ACTIVE` gives a `ValidationException` carrying this text.
/// Maximum vector indexes per table. Measured 2026-08-13: five are accepted,
/// six are refused, on both the create and the update path.
pub const MAX_VECTOR_INDEXES_PER_TABLE: usize = 5;

/// Vector indexes require on-demand billing. Measured 2026-08-13, identical on
/// `CreateTable` with `PROVISIONED` and on an `UpdateTable` that switches a
/// table holding vector indexes to `PROVISIONED`.
pub const VECTOR_INDEX_REQUIRES_PAY_PER_REQUEST: &str = "One or more parameter values were invalid: Vector indexes are only supported for \
     PAY_PER_REQUEST tables";

/// Per-table vector index limit exceeded on `CreateTable`.
///
/// The create and update paths differ in BOTH class and text for this one rule,
/// so they get separate constants rather than one shared message. Measured
/// 2026-08-13: `CreateTable` reports this as a `ValidationException`.
pub const VECTOR_INDEX_COUNT_LIMIT_CREATE: &str =
    "One or more parameter values were invalid: VectorIndex count exceeds the per-table limit of 5";

/// Per-table vector index limit exceeded on `UpdateTable`.
///
/// Measured 2026-08-13: reported as a `LimitExceededException`, not a
/// `ValidationException`, and with different wording from the create path.
pub const VECTOR_INDEX_COUNT_LIMIT_UPDATE: &str =
    "Subscriber limit exceeded: Number of vector secondary indexes exceeds per-table limit of 5";

/// A `SearchSchema` element names an attribute with no `AttributeDefinition`.
///
/// Measured 2026-08-13, same text on both paths. On `UpdateTable` the
/// definition must be present in THAT request, even when the attribute is
/// already declared on the table.
pub const VECTOR_SEARCH_SCHEMA_UNDECLARED: &str = "One or more parameter values were invalid: One element in SearchSchema is not defined in \
     attribute definitions";

/// The vector attribute must NOT appear in `AttributeDefinitions`.
///
/// The opposite of the rule for key attributes, and the reason a table
/// partition key cannot be used as the vector attribute on `CreateTable`: the
/// key must be declared, so declaring it trips this. Measured 2026-08-13.
#[must_use]
pub fn vector_attribute_conflicting_definition(attribute_name: &str) -> String {
    format!(
        "One or more parameter values were invalid: Conflicting attribute definition for \
         '{attribute_name}'. An attribute cannot be defined in AttributeDefinitions when used \
         as a VectorAttribute."
    )
}

/// The vector attribute collides with an existing key attribute.
///
/// Distinct from [`vector_attribute_conflicting_definition`]: seen on
/// `UpdateTable`, where the key is not re-declared in the request so the
/// conflicting-definition rule cannot fire first. The message embeds both
/// schemas, reporting the vector as type `L` with its dimension count.
/// Measured 2026-08-13.
#[must_use]
pub fn vector_attribute_redefines_key(
    attribute_name: &str,
    existing_type: &str,
    existing_key_type: &str,
    dimensions: u32,
) -> String {
    format!(
        "One or more parameter values were invalid: Attributes cannot be redefined. Please check \
         that your attribute has the same type as previously defined. Existing schema: \
         Schema:[SchemaElement: key{{{attribute_name}:{existing_type}:{existing_key_type}}}] \
         New schema: VectorIndexSchema:[VectorAttribute: key{{{attribute_name}:L:{dimensions}}}]"
    )
}

/// The message for an UpdateTable create whose vector attribute is already
/// used by an existing vector index with different `Dimensions`. Measured
/// live 2026-08-24 (us-east-1): `Create vConf(emb, 16)` against a table whose
/// `vidx0` declares `emb` at 4 answers exactly this, with both sides rendered
/// in the `VectorIndexSchema` shape (contrast the key-redefinition sibling
/// above, whose existing side is a key `Schema`). Same attribute with the
/// SAME dimensions is accepted (also measured).
#[must_use]
pub fn vector_attribute_redefines_vector(
    attribute_name: &str,
    existing_dimensions: u32,
    new_dimensions: u32,
) -> String {
    format!(
        "One or more parameter values were invalid: Attributes cannot be redefined. Please check \
         that your attribute has the same type as previously defined. Existing schema: \
         VectorIndexSchema:[VectorAttribute: key{{{attribute_name}:L:{existing_dimensions}}}] \
         New schema: VectorIndexSchema:[VectorAttribute: key{{{attribute_name}:L:{new_dimensions}}}]"
    )
}

pub const VECTOR_INDEX_ALREADY_EXISTS: &str = "Attempting to create an index which already exists";

/// Prefix of the message the service returns when the name is taken by an index
/// that is still being created. Measured on 2026-08-06; carried as a
/// `ResourceInUseException`, not a `ValidationException`, and continues
/// "  Table: {table} Index: {index}" with two spaces after the full stop.
pub const VECTOR_INDEX_CREATE_IN_USE_PREFIX: &str =
    "Attempt to change a resource which is still in use: Index is being created.";

/// Message the service returns when a vector index is deleted while its creation
/// is still in the resource-allocation phase.
///
/// Measured byte-exact on 2026-08-19 (probe P2), carried as a
/// `ResourceInUseException` with HTTP 400. Deleting a `CREATING` vector index is
/// phase-dependent: refused while the index reports `Backfilling: false`,
/// accepted once it reports `Backfilling: true`, which is why the text tells the
/// caller to retry rather than that the request was wrong.
///
/// A function rather than a bare constant because the service names both
/// resources, separated by a single space and with no comma:
/// "... is active. Table: t Index: i".
#[must_use]
pub fn vector_index_delete_in_allocation_phase(table_name: &str, index_name: &str) -> String {
    format!(
        "Attempt to change a resource which is still in use: Index creation is in resource \
         allocation phase. Retry deletion during backfilling phase or when the index is active. \
         Table: {table_name} Index: {index_name}"
    )
}

impl VectorIndexDescription {
    /// Reject a description whose reported state the service would never produce.
    ///
    /// Two rules, both measured rather than assumed (see
    /// [`VectorIndexDescription::backfilling`] for the observed lifecycle).
    ///
    /// `ACTIVE` with a backfill in flight is a contradiction. An index that has
    /// not finished being populated cannot answer a search completely, so
    /// reporting it ready makes a client's first search silently undercount, which
    /// is the failure RFC 236 guards against. `Backfilling: true` was only ever
    /// observed alongside `CREATING`.
    ///
    /// `ACTIVE` carrying the member at all, even `false`, is also rejected. The
    /// service removes it on completion rather than reporting `false`, matching the
    /// documented GSI behaviour, so a backend that keeps emitting it diverges on
    /// the wire from what a client is entitled to expect.
    ///
    /// A contradictory pair is a backend defect rather than anything the caller
    /// did, hence `InternalServerError`.
    ///
    /// # Errors
    /// Returns [`DynamoDbError::InternalServerError`](crate::error::DynamoDbError::InternalServerError)
    /// if the index is reported `ACTIVE` with any `backfilling` value present.
    pub fn validate_readiness(&self) -> Result<(), crate::error::DynamoDbError> {
        if !self.index_status.is_active() {
            return Ok(());
        }
        match self.backfilling {
            Some(true) => Err(crate::error::DynamoDbError::InternalServerError(format!(
                "vector index '{}' reported ACTIVE while still \
                 backfilling; it cannot serve complete results yet",
                self.index_name
            ))),
            Some(false) => Err(crate::error::DynamoDbError::InternalServerError(format!(
                "vector index '{}' reported ACTIVE with a \
                 Backfilling member; the service removes it once the index is \
                 active rather than reporting false",
                self.index_name
            ))),
            None => Ok(()),
        }
    }
}

impl TableDescription {
    /// Validate the reported readiness of every vector index on the table.
    ///
    /// Called on the paths that hand a description to a client, so a backend that
    /// reports a contradictory state is caught once, centrally, rather than each
    /// response path having to remember. No in-tree backend populates
    /// `vector_indexes` yet, so this guards the backend implementations to come
    /// rather than anything shipping today.
    ///
    /// # Errors
    /// Propagates the first failure from
    /// [`VectorIndexDescription::validate_readiness`].
    pub fn validate_vector_index_readiness(&self) -> Result<(), crate::error::DynamoDbError> {
        for index in self.vector_indexes.iter().flatten() {
            index.validate_readiness()?;
        }
        Ok(())
    }
}

/// Full description of a Virtual `DynamoDB` table, returned by `CreateTable`,
/// `DeleteTable`, and `DescribeTable`.
/// Derives [`Default`] deliberately: a storage backend assembles this and must
/// be able to write `..Default::default()` so that adding a field here, for a
/// feature that backend does not implement, does not break its build. Note that
/// `#[non_exhaustive]` would defeat that, since it forbids functional update
/// syntax from other crates entirely.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TableDescription {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Vec<AttributeDefinition>,
    #[serde(rename = "TableStatus")]
    pub table_status: TableStatus,
    #[serde(rename = "CreationDateTime")]
    pub creation_date_time: f64,
    #[serde(rename = "TableSizeBytes")]
    pub table_size_bytes: i64,
    #[serde(rename = "ItemCount")]
    pub item_count: i64,
    #[serde(rename = "TableArn")]
    pub table_arn: String,
    #[serde(rename = "TableId")]
    pub table_id: String,
    #[serde(rename = "ProvisionedThroughput")]
    pub provisioned_throughput: ProvisionedThroughputDescription,
    #[serde(rename = "BillingModeSummary", skip_serializing_if = "Option::is_none")]
    pub billing_mode_summary: Option<BillingModeSummary>,
    #[serde(
        rename = "GlobalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub global_secondary_indexes: Option<Vec<GsiDescription>>,
    #[serde(
        rename = "LocalSecondaryIndexes",
        skip_serializing_if = "Option::is_none"
    )]
    pub local_secondary_indexes: Option<Vec<LsiDescription>>,
    #[serde(rename = "VectorIndexes", skip_serializing_if = "Option::is_none")]
    pub vector_indexes: Option<Vec<VectorIndexDescription>>,
    #[serde(
        rename = "StreamSpecification",
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_specification: Option<StreamSpecification>,
    #[serde(rename = "LatestStreamArn", skip_serializing_if = "Option::is_none")]
    pub latest_stream_arn: Option<String>,
    #[serde(rename = "LatestStreamLabel", skip_serializing_if = "Option::is_none")]
    pub latest_stream_label: Option<String>,
    #[serde(rename = "DeletionProtectionEnabled")]
    pub deletion_protection_enabled: bool,
    #[serde(rename = "SSEDescription", skip_serializing_if = "Option::is_none")]
    pub sse_description: Option<SseDescription>,
    #[serde(rename = "TableClassSummary", skip_serializing_if = "Option::is_none")]
    pub table_class_summary: Option<serde_json::Value>,
    #[serde(rename = "OnDemandThroughput", skip_serializing_if = "Option::is_none")]
    pub on_demand_throughput: Option<OnDemandThroughput>,
    #[serde(rename = "RestoreSummary", skip_serializing_if = "Option::is_none")]
    pub restore_summary: Option<RestoreSummary>,
}

/// Details of a restore reported on the restored table's description.
///
/// `RestoreTableFromBackup` reports the source backup and an in-progress flag
/// on its response's `TableDescription`; the ground-truth runs of 2026-08-24
/// (us-east-1 and eu-west-2) pin `SourceBackupArn` and
/// `RestoreInProgress: true` there.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RestoreSummary {
    #[serde(rename = "SourceBackupArn", skip_serializing_if = "Option::is_none")]
    pub source_backup_arn: Option<String>,
    #[serde(rename = "RestoreDateTime")]
    pub restore_date_time: f64,
    #[serde(rename = "RestoreInProgress")]
    pub restore_in_progress: bool,
}

/// `CreateTable` request body.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub struct CreateTableInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Vec<AttributeDefinition>,
    #[serde(rename = "BillingMode")]
    pub billing_mode: Option<BillingMode>,
    #[serde(rename = "ProvisionedThroughput")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    #[serde(rename = "GlobalSecondaryIndexes")]
    pub global_secondary_indexes: Option<Vec<GsiInput>>,
    #[serde(rename = "LocalSecondaryIndexes")]
    pub local_secondary_indexes: Option<Vec<LsiInput>>,
    #[serde(rename = "VectorIndexes")]
    pub vector_indexes: Option<Vec<VectorIndexSpecification>>,
    #[serde(rename = "StreamSpecification")]
    pub stream_specification: Option<StreamSpecification>,
    #[serde(rename = "SSESpecification")]
    pub sse_specification: Option<serde_json::Value>,
    #[serde(rename = "Tags")]
    pub tags: Option<Vec<Tag>>,
    #[serde(rename = "DeletionProtectionEnabled")]
    pub deletion_protection_enabled: Option<bool>,
    #[serde(rename = "TableClass")]
    pub table_class: Option<String>,
    #[serde(rename = "OnDemandThroughput")]
    pub on_demand_throughput: Option<OnDemandThroughput>,
}

/// `CreateTable` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CreateTableOutput {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

/// `DeleteTable` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DeleteTableInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
}

/// `DeleteTable` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeleteTableOutput {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

/// `DescribeTable` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DescribeTableInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
}

/// `DescribeTable` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DescribeTableOutput {
    #[serde(rename = "Table")]
    pub table: TableDescription,
}

/// `ListTables` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ListTablesInput {
    #[serde(rename = "Limit")]
    pub limit: Option<i32>,
    #[serde(rename = "ExclusiveStartTableName")]
    pub exclusive_start_table_name: Option<String>,
}

/// `ListTables` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ListTablesOutput {
    #[serde(rename = "TableNames")]
    pub table_names: Vec<String>,
    #[serde(
        rename = "LastEvaluatedTableName",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_evaluated_table_name: Option<String>,
}

// --- UpdateTable ---

/// A single GSI update action within an `UpdateTable` request.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GlobalSecondaryIndexUpdate {
    #[serde(rename = "Create", skip_serializing_if = "Option::is_none")]
    pub create: Option<CreateGsiAction>,
    #[serde(rename = "Update", skip_serializing_if = "Option::is_none")]
    pub update: Option<UpdateGsiAction>,
    #[serde(rename = "Delete", skip_serializing_if = "Option::is_none")]
    pub delete: Option<DeleteGsiAction>,
}

/// Create a new GSI on an existing table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CreateGsiAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,
    #[serde(rename = "KeySchema")]
    pub key_schema: Vec<KeySchemaElement>,
    #[serde(rename = "Projection")]
    pub projection: Projection,
    #[serde(
        rename = "ProvisionedThroughput",
        skip_serializing_if = "Option::is_none"
    )]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
}

/// Delete an existing GSI from a table.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DeleteGsiAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,
}

/// Update provisioned throughput on an existing GSI.
///
/// Recognized by the deserializer but not yet implemented — the engine
/// returns a clear "not yet supported" error.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UpdateGsiAction {
    #[serde(rename = "IndexName")]
    pub index_name: String,
}

/// `UpdateTable` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UpdateTableInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "BillingMode")]
    pub billing_mode: Option<BillingMode>,
    #[serde(rename = "ProvisionedThroughput")]
    pub provisioned_throughput: Option<ProvisionedThroughput>,
    #[serde(rename = "DeletionProtectionEnabled")]
    pub deletion_protection_enabled: Option<bool>,
    #[serde(rename = "GlobalSecondaryIndexUpdates")]
    pub global_secondary_index_updates: Option<Vec<GlobalSecondaryIndexUpdate>>,
    #[serde(rename = "AttributeDefinitions")]
    pub attribute_definitions: Option<Vec<AttributeDefinition>>,
    #[serde(rename = "StreamSpecification")]
    pub stream_specification: Option<StreamSpecification>,
    #[serde(rename = "TableClass")]
    pub table_class: Option<String>,
    #[serde(rename = "OnDemandThroughput")]
    pub on_demand_throughput: Option<OnDemandThroughput>,
    /// Vector index changes. `None` and an empty list both mean no change, so an
    /// ordinary `UpdateTable` against a backend without vector support is
    /// unaffected.
    #[serde(rename = "VectorIndexUpdates", default)]
    pub vector_index_updates: Option<Vec<VectorIndexUpdate>>,
}

/// `UpdateTable` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UpdateTableOutput {
    #[serde(rename = "TableDescription")]
    pub table_description: TableDescription,
}

// --- TTL ---

/// TTL status for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeToLiveStatus {
    /// TTL is enabled.
    Enabled,
    /// TTL is disabled.
    Disabled,
}

/// TTL description returned by `DescribeTimeToLive` and `UpdateTimeToLive`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TimeToLiveDescription {
    #[serde(rename = "TimeToLiveStatus")]
    pub time_to_live_status: TimeToLiveStatus,
    #[serde(rename = "AttributeName", skip_serializing_if = "Option::is_none")]
    pub attribute_name: Option<String>,
}

/// `DescribeTimeToLive` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DescribeTimeToLiveInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
}

/// `DescribeTimeToLive` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DescribeTimeToLiveOutput {
    #[serde(rename = "TimeToLiveDescription")]
    pub time_to_live_description: TimeToLiveDescription,
}

/// `UpdateTimeToLive` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UpdateTimeToLiveInput {
    #[serde(rename = "TableName")]
    pub table_name: String,
    #[serde(rename = "TimeToLiveSpecification")]
    pub time_to_live_specification: TimeToLiveSpecification,
}

/// TTL specification for `UpdateTimeToLive`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TimeToLiveSpecification {
    #[serde(rename = "Enabled")]
    pub enabled: bool,
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
}

/// `UpdateTimeToLive` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UpdateTimeToLiveOutput {
    #[serde(rename = "TimeToLiveSpecification")]
    pub time_to_live_specification: TimeToLiveSpecificationOutput,
}

/// TTL specification in responses (uses status enum instead of bool).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TimeToLiveSpecificationOutput {
    #[serde(rename = "AttributeName")]
    pub attribute_name: String,
    #[serde(rename = "Enabled")]
    pub enabled: bool,
}

// --- Tags ---

/// `TagResource` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TagResourceInput {
    #[serde(rename = "ResourceArn")]
    pub resource_arn: String,
    #[serde(rename = "Tags")]
    pub tags: Vec<Tag>,
}

/// `UntagResource` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UntagResourceInput {
    #[serde(rename = "ResourceArn")]
    pub resource_arn: String,
    #[serde(rename = "TagKeys")]
    pub tag_keys: Vec<String>,
}

/// `ListTagsOfResource` request body.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ListTagsOfResourceInput {
    #[serde(rename = "ResourceArn")]
    pub resource_arn: String,
    #[serde(rename = "NextToken")]
    pub next_token: Option<String>,
}

/// `ListTagsOfResource` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ListTagsOfResourceOutput {
    #[serde(rename = "Tags")]
    pub tags: Vec<Tag>,
    #[serde(rename = "NextToken", skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

// --- DescribeLimits ---

/// `DescribeLimits` response body.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DescribeLimitsOutput {
    #[serde(rename = "AccountMaxReadCapacityUnits")]
    pub account_max_read_capacity_units: i64,
    #[serde(rename = "AccountMaxWriteCapacityUnits")]
    pub account_max_write_capacity_units: i64,
    #[serde(rename = "TableMaxReadCapacityUnits")]
    pub table_max_read_capacity_units: i64,
    #[serde(rename = "TableMaxWriteCapacityUnits")]
    pub table_max_write_capacity_units: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `TableThroughputMode` is not a member of DynamoDB's CreateTable request:
    /// the model has `BillingMode` only (verified against aws-sdk-dynamodb 1.119.0,
    /// where the field does not appear at all). An earlier version of this type
    /// accepted it as an alias, which meant a request that produced a
    /// PAY_PER_REQUEST table here would be ignored by AWS and produce a
    /// PROVISIONED table there: an accept-direction divergence, where code written
    /// against ExtendDB breaks against the real service. Under AWS JSON 1.0 an
    /// unknown member is ignored, which is what must happen here.
    #[test]
    fn create_table_ignores_the_unknown_table_throughput_mode_member() {
        let json = r#"{
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "TableThroughputMode": "PAY_PER_REQUEST"
        }"#;
        let input: CreateTableInput = serde_json::from_str(json).unwrap();
        assert_eq!(
            input.billing_mode, None,
            "an unknown member must be ignored, not treated as BillingMode"
        );
    }

    #[test]
    fn create_table_billing_mode_still_wins_when_present() {
        let json = r#"{
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }"#;
        let input: CreateTableInput = serde_json::from_str(json).unwrap();
        assert_eq!(input.billing_mode, Some(BillingMode::PayPerRequest));
    }

    #[test]
    fn update_table_ignores_the_unknown_table_throughput_mode_member() {
        let json = r#"{"TableName": "t", "TableThroughputMode": "PROVISIONED"}"#;
        let input: UpdateTableInput = serde_json::from_str(json).unwrap();
        assert_eq!(
            input.billing_mode, None,
            "an unknown member must be ignored"
        );
    }

    #[test]
    fn on_demand_throughput_round_trips_json() {
        let odt = OnDemandThroughput {
            max_read_request_units: Some(100),
            max_write_request_units: Some(50),
        };
        let json = serde_json::to_value(&odt).unwrap();
        assert_eq!(json["MaxReadRequestUnits"], 100);
        assert_eq!(json["MaxWriteRequestUnits"], 50);
        let parsed: OnDemandThroughput = serde_json::from_value(json).unwrap();
        assert_eq!(parsed, odt);
    }

    #[test]
    fn on_demand_throughput_deserializes_from_input() {
        let input_json = r#"{"MaxReadRequestUnits": 10, "MaxWriteRequestUnits": 5}"#;
        let odt: OnDemandThroughput = serde_json::from_str(input_json).unwrap();
        assert_eq!(odt.max_read_request_units, Some(10));
        assert_eq!(odt.max_write_request_units, Some(5));
    }

    #[test]
    fn sse_description_serializes_with_kms_arn() {
        let sse = SseDescription {
            status: "ENABLED".to_string(),
            sse_type: Some(SseType::KMS),
            kms_master_key_arn: Some("arn:aws:kms:us-east-1:123456789012:key/default".to_string()),
        };
        let json = serde_json::to_value(&sse).unwrap();
        assert_eq!(json["Status"], "ENABLED");
        assert_eq!(json["SSEType"], "KMS");
        assert_eq!(
            json["KMSMasterKeyArn"],
            "arn:aws:kms:us-east-1:123456789012:key/default"
        );
    }

    #[test]
    fn sse_description_omits_none_fields() {
        let sse = SseDescription {
            status: "ENABLED".to_string(),
            sse_type: None,
            kms_master_key_arn: None,
        };
        let json = serde_json::to_value(&sse).unwrap();
        assert_eq!(json["Status"], "ENABLED");
        assert!(json.get("SSEType").is_none());
        assert!(json.get("KMSMasterKeyArn").is_none());
    }

    #[test]
    fn create_table_input_deserializes_on_demand_throughput() {
        let json = r#"{
            "TableName": "T",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "OnDemandThroughput": {"MaxReadRequestUnits": 10, "MaxWriteRequestUnits": 5}
        }"#;
        let input: CreateTableInput = serde_json::from_str(json).unwrap();
        let odt = input.on_demand_throughput.unwrap();
        assert_eq!(odt.max_read_request_units, Some(10));
        assert_eq!(odt.max_write_request_units, Some(5));
    }
}

#[cfg(test)]
mod vector_search_schema_normalisation_tests {
    use super::{
        DistanceFunction, SearchSchemaElement, SearchSchemaElementType, VectorAttribute,
        VectorIndexSpecification,
    };

    fn spec(search_schema: Option<Vec<SearchSchemaElement>>) -> VectorIndexSpecification {
        VectorIndexSpecification {
            index_name: "vidx".to_owned(),
            dimensions: 4,
            distance_function: DistanceFunction::Cosine,
            vector_attribute: VectorAttribute {
                attribute_name: "emb".to_owned(),
            },
            search_schema,
            projection: None,
        }
    }

    fn hash() -> Vec<SearchSchemaElement> {
        vec![SearchSchemaElement {
            attribute_name: "tenant".to_owned(),
            element_type: SearchSchemaElementType::Hash,
        }]
    }

    /// An empty list and an absent member mean the same thing, so only one of them
    /// may reach storage. The service reports an absent member or a populated one
    /// and never an empty list, so storing `[]` would make DescribeTable echo a
    /// third state, and would let two backends differ on whether they collapsed it.
    #[test]
    fn an_empty_search_schema_becomes_absent() {
        let mut empty = spec(Some(Vec::new()));
        empty.normalize_search_schema();
        assert_eq!(empty.search_schema, None);
        assert_eq!(spec(Some(Vec::new())).search_schema_for_storage(), None);
    }

    #[test]
    fn a_populated_search_schema_is_left_alone() {
        let mut scoped = spec(Some(hash()));
        scoped.normalize_search_schema();
        assert_eq!(scoped.search_schema, Some(hash()));
        assert_eq!(
            spec(Some(hash())).search_schema_for_storage(),
            Some(hash().as_slice())
        );
    }

    #[test]
    fn an_absent_search_schema_stays_absent() {
        let mut unscoped = spec(None);
        unscoped.normalize_search_schema();
        assert_eq!(unscoped.search_schema, None);
        assert_eq!(spec(None).search_schema_for_storage(), None);
    }

    /// The two forms must agree, since one is applied on the request path and the
    /// other by the backends: a caller reaching storage directly must not be able
    /// to store a state the request path would have collapsed.
    #[test]
    fn the_owning_and_borrowing_forms_agree() {
        for schema in [None, Some(Vec::new()), Some(hash())] {
            let mut owned = spec(schema.clone());
            owned.normalize_search_schema();
            let borrowed = spec(schema).search_schema_for_storage().map(<[_]>::to_vec);
            assert_eq!(owned.search_schema, borrowed);
        }
    }
}

#[cfg(test)]
mod vector_index_readiness_tests {

    /// The enum must round-trip the service's own strings, and an unrecognised
    /// value must parse rather than fail: a new status appearing upstream should
    /// not turn DescribeTable into an outage.
    #[test]
    fn index_status_round_trips_the_service_values() {
        for (value, expected) in [
            ("\"CREATING\"", IndexStatus::Creating),
            ("\"UPDATING\"", IndexStatus::Updating),
            ("\"DELETING\"", IndexStatus::Deleting),
            ("\"ACTIVE\"", IndexStatus::Active),
        ] {
            let parsed: IndexStatus = serde_json::from_str(value).expect("parses");
            assert_eq!(parsed, expected, "{value}");
            assert_eq!(
                serde_json::to_string(&expected).expect("serialises"),
                value,
                "must serialise back to the service's spelling"
            );
        }
        let unknown: IndexStatus =
            serde_json::from_str("\"SOMETHING_NEW\"").expect("an unknown status must still parse");
        assert_eq!(unknown, IndexStatus::Unknown);
        assert!(!unknown.is_active(), "an unknown status is not serviceable");
    }
    use super::*;
    use crate::error::DynamoDbError;

    fn description(index_status: IndexStatus, backfilling: Option<bool>) -> VectorIndexDescription {
        VectorIndexDescription {
            index_name: "vidx".to_owned(),
            vector_attribute: VectorAttribute {
                attribute_name: "emb".to_owned(),
            },
            dimensions: 4,
            search_schema: None,
            distance_function: DistanceFunction::Cosine,
            index_status,
            backfilling,
            index_size_bytes: 0,
            item_count: 0,
            index_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/t/index/vidx".to_owned(),
            projection: Some(Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            }),
        }
    }

    /// The contradiction the invariant exists to catch.
    #[test]
    fn active_while_backfilling_is_rejected() {
        let err = description(IndexStatus::Active, Some(true))
            .validate_readiness()
            .expect_err("ACTIVE plus a backfill in flight must not be reportable");
        match err {
            DynamoDbError::InternalServerError(m) => {
                assert!(m.contains("vidx"), "should name the index: {m}");
                assert!(m.contains("backfilling"), "should say why: {m}");
            }
            other => panic!("expected InternalServerError, got {other:?}"),
        }
    }

    /// ACTIVE must not carry the member at all. The service removes it on
    /// completion rather than reporting false, so emitting false diverges on the
    /// wire from what a client is entitled to expect.
    #[test]
    fn active_carrying_the_member_at_all_is_rejected() {
        let err = description(IndexStatus::Active, Some(false))
            .validate_readiness()
            .expect_err("ACTIVE must not carry a Backfilling member");
        match err {
            DynamoDbError::InternalServerError(m) => {
                assert!(m.contains("removes it"), "should say why: {m}");
            }
            other => panic!("expected InternalServerError, got {other:?}"),
        }
    }

    /// The three states actually observed, in the order observed, must all be
    /// reportable. Measured on 2026-08-06 by adding an index to a table of 3000
    /// items with UpdateTable and polling through the 8.5 minute backfill.
    #[test]
    fn the_observed_lifecycle_is_reportable() {
        for (status, backfilling, when) in [
            (
                IndexStatus::Creating,
                Some(false),
                "t+0.01s, created, backfill not started",
            ),
            (IndexStatus::Creating, Some(true), "t+30.8s, backfilling"),
            (
                IndexStatus::Active,
                None,
                "t+511.5s, complete, member removed",
            ),
        ] {
            assert!(
                description(status, backfilling)
                    .validate_readiness()
                    .is_ok(),
                "observed state must be permitted ({when})"
            );
        }
    }

    /// Non-ACTIVE statuses are unconstrained: the rules are about not claiming
    /// readiness, so anything short of ACTIVE passes whatever the flag says.
    #[test]
    fn non_active_statuses_are_unconstrained() {
        for (status, backfilling) in [
            (IndexStatus::Creating, None),
            (IndexStatus::Deleting, Some(true)),
            (IndexStatus::Deleting, None),
            (IndexStatus::Updating, Some(false)),
        ] {
            assert!(
                description(status, backfilling)
                    .validate_readiness()
                    .is_ok(),
                "{status:?} with backfilling={backfilling:?} should be permitted"
            );
        }
    }

    /// The table-level check reports the offending index rather than passing
    /// because a sibling is fine.
    #[test]
    fn table_level_check_finds_a_bad_index_among_good_ones() {
        // Built the way a backend builds one, which is the pattern the contract
        // relies on for opt-out.
        let table = TableDescription {
            vector_indexes: Some(vec![
                description(IndexStatus::Active, None),
                description(IndexStatus::Active, Some(true)),
            ]),
            ..Default::default()
        };
        assert!(table.validate_vector_index_readiness().is_err());
    }

    #[test]
    fn a_table_with_no_vector_indexes_passes() {
        assert!(
            TableDescription::default()
                .validate_vector_index_readiness()
                .is_ok()
        );
        let table = TableDescription {
            vector_indexes: Some(Vec::new()),
            ..Default::default()
        };
        assert!(table.validate_vector_index_readiness().is_ok());
    }

    /// Wire shape. The service omitted `Backfilling` entirely for an ACTIVE index,
    /// so `None` must not serialise to `"Backfilling": null`, which a client would
    /// see as a different response.
    #[test]
    fn backfilling_is_omitted_from_the_wire_when_absent() {
        let json =
            serde_json::to_string(&description(IndexStatus::Active, None)).expect("serialises");
        assert!(
            !json.contains("Backfilling"),
            "absent must mean omitted, not null: {json}"
        );
        let json = serde_json::to_string(&description(IndexStatus::Creating, Some(true)))
            .expect("serialises");
        assert!(
            json.contains(r#""Backfilling":true"#),
            "present must serialise under the service's member name: {json}"
        );
    }

    /// A response without the member deserialises, which is the shape the service
    /// actually returned.
    #[test]
    fn a_response_without_backfilling_deserialises() {
        let json =
            serde_json::to_string(&description(IndexStatus::Active, None)).expect("serialises");
        let round_tripped: VectorIndexDescription =
            serde_json::from_str(&json).expect("deserialises without the member");
        assert_eq!(round_tripped.backfilling, None);
    }
}

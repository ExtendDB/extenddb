// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
pub mod number;
pub mod vector_item;

pub use vector_item::{
    MAX_HASH_KEY_SIZE, MAX_INLINE_FILTER_SIZE, validate_vector_write,
    validate_vector_write_changed, vector_components, vector_norm,
};

use crate::error::{DynamoDbError, ErrorMessageKey, error_message};
use crate::limits::LimitsConfig;
use crate::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteItemInput,
    GetItemInput, Item, KeySchemaElement, KeyType, MAX_VECTOR_INDEXES_PER_TABLE, PutItemInput,
    ReturnValues, ScalarAttributeType, Select, UpdateItemInput, item_size_bytes,
};

/// Validate a table name per Virtual `DynamoDB` rules.
/// REQ-LIM-020: 3-255 chars. REQ-LIM-021: [a-zA-Z0-9_.-]
pub fn validate_table_name(name: &str, limits: &LimitsConfig) -> Result<(), DynamoDbError> {
    if name.is_empty() {
        return Err(DynamoDbError::ValidationException(error_message(
            ErrorMessageKey::TableNameEmpty,
            &[],
        )));
    }
    if name.len() < limits.min_table_name_length {
        return Err(DynamoDbError::ValidationException(error_message(
            ErrorMessageKey::TableNameTooShort,
            &[name],
        )));
    }
    if name.len() > limits.max_table_name_length {
        return Err(DynamoDbError::ValidationException(error_message(
            ErrorMessageKey::TableNameTooLong,
            &[name],
        )));
    }
    validate_table_name_chars(name)?;
    Ok(())
}

/// Validate only the character set and max length of a table name.
///
/// Used for defense-in-depth on pagination tokens like `ExclusiveStartTableName`,
/// where real `DynamoDB` does not enforce the 3-character minimum but we still want
/// to ensure only safe characters reach storage.
pub fn validate_table_name_chars(name: &str) -> Result<(), DynamoDbError> {
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    {
        return Err(DynamoDbError::ValidationException(error_message(
            ErrorMessageKey::TableNameInvalidChars,
            &[name],
        )));
    }
    Ok(())
}

/// Validate an index name per `DynamoDB` rules: 3–255 chars, `[a-zA-Z0-9_.-]+`.
///
/// Same character rules as table names. Defense-in-depth: prevents SQL injection
/// via index names that are interpolated into DDL identifiers in storage-postgres.
///
/// # Errors
///
/// Returns `ValidationException` if the name is too short, too long, or contains
/// invalid characters.
pub fn validate_index_name(name: &str) -> Result<(), DynamoDbError> {
    if name.len() < 3 || name.len() > 255 {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{name}' at 'indexName' failed to satisfy constraint: \
             Member must have length greater than or equal to 3 and less than or equal to 255"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{name}' at 'indexName' failed to satisfy constraint: \
             Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
        )));
    }
    Ok(())
}

/// Validate a `CreateTable` request.
///
/// When `allow_multipart_table_keys` is `true`, base tables may have up to 4 HASH
/// and 4 RANGE key schema elements (preview extension). GSIs always allow multi-part
/// keys regardless of this flag.
pub fn validate_create_table(
    input: &CreateTableInput,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    validate_table_name(&input.table_name, limits)?;
    validate_key_schema(input, limits.allow_multipart_table_keys)?;
    validate_gsi_key_schemas(input)?;
    validate_lsi_key_schemas(input)?;
    validate_attribute_definitions(input)?;
    validate_provisioned_throughput(input)?;
    validate_gsi_provisioned_throughput(input)?;
    validate_gsi_count(input, limits)?;
    validate_lsi_count(input, limits)?;
    validate_lsi_requires_range_key(input)?;
    validate_unique_index_names(input)?;
    validate_index_projections(input)?;
    validate_vector_indexes(input)?;
    validate_stream_specification(input)?;
    Ok(())
}

#[cfg(test)]
mod search_schema_shape_tests {
    use super::{MAX_SEARCH_SCHEMA_INLINE_FILTERS, validate_search_schema_shape};
    use crate::types::{SearchSchemaElement, SearchSchemaElementType};

    fn element(name: &str, element_type: SearchSchemaElementType) -> SearchSchemaElement {
        SearchSchemaElement {
            attribute_name: name.to_owned(),
            element_type,
        }
    }

    fn hash(name: &str) -> SearchSchemaElement {
        element(name, SearchSchemaElementType::Hash)
    }

    fn filter(name: &str) -> SearchSchemaElement {
        element(name, SearchSchemaElementType::InlineFilter)
    }

    #[test]
    fn no_search_schema_is_allowed() {
        // The HASH element is optional: an index without one searches the table.
        validate_search_schema_shape(None).expect("absent schema is valid");
    }

    #[test]
    fn one_hash_is_allowed() {
        validate_search_schema_shape(Some(&[hash("t")])).expect("one HASH is valid");
    }

    /// The case that mattered: a two-HASH schema was accepted and then could not be
    /// honoured, because the query side requires a condition for every HASH while a
    /// backend resolves the scope from the first and demotes the rest.
    #[test]
    fn two_hash_elements_are_rejected_with_the_measured_message() {
        let err = validate_search_schema_shape(Some(&[hash("a"), hash("b")]))
            .expect_err("two HASH elements must be rejected");
        assert_eq!(
            format!("{err}"),
            "One or more parameter values were invalid: Value '2' at 'SearchSchema' \
             failed to satisfy constraint: Member must have HASH count less than or \
             equal to 1"
        );
    }

    #[test]
    fn the_inline_filter_cap_is_a_boundary_not_a_range() {
        let at_cap: Vec<_> = (0..MAX_SEARCH_SCHEMA_INLINE_FILTERS)
            .map(|i| filter(&format!("f{i}")))
            .collect();
        validate_search_schema_shape(Some(&at_cap)).expect("the cap itself is allowed");

        let over_cap: Vec<_> = (0..=MAX_SEARCH_SCHEMA_INLINE_FILTERS)
            .map(|i| filter(&format!("f{i}")))
            .collect();
        let err = validate_search_schema_shape(Some(&over_cap))
            .expect_err("one over the cap must be rejected");
        assert_eq!(
            format!("{err}"),
            "One or more parameter values were invalid: Value '19' at 'SearchSchema' \
             failed to satisfy constraint: Member must have INLINE_FILTER count less \
             than or equal to 18"
        );
    }

    /// Pins the measured number, because the obvious inference from the query-side
    /// cap gives twenty and is wrong. A future edit "tidying" this to match the
    /// query cap would break parity silently.
    #[test]
    fn the_inline_filter_cap_is_eighteen_as_measured() {
        assert_eq!(MAX_SEARCH_SCHEMA_INLINE_FILTERS, 18);
    }

    #[test]
    fn a_hash_plus_filters_is_allowed() {
        let mut elements = vec![hash("t")];
        elements.extend((0..MAX_SEARCH_SCHEMA_INLINE_FILTERS).map(|i| filter(&format!("f{i}"))));
        validate_search_schema_shape(Some(&elements))
            .expect("one HASH plus the filter cap is valid");
    }
}

/// Maximum `HASH` elements in a vector index search schema.
///
/// Measured against the live service 2026-08-06. Without this check the contract
/// accepted a schema it then could not honour: `validate_conditions_against_search_schema`
/// requires a condition for EVERY declared HASH, while a backend resolving the scope
/// takes the first HASH and demotes the rest to filters. So a two-HASH schema was
/// internally contradictory rather than merely unvalidated.
const MAX_SEARCH_SCHEMA_HASH: usize = 1;

/// Maximum `INLINE_FILTER` elements in a vector index search schema.
///
/// Measured against the live service 2026-08-06 as **18**, which is deliberately
/// recorded rather than derived: the obvious inference from the query-side cap
/// (`MAX_SEARCH_CONDITIONS`, one HASH plus twenty filters) gives twenty, and is
/// wrong. The schema cap and the per-query cap are different numbers.
const MAX_SEARCH_SCHEMA_INLINE_FILTERS: usize = 18;

/// Validate the shape of a vector index search schema.
///
/// Messages measured against the live service 2026-08-06 by signing raw requests,
/// since no published SDK models vector indexes:
///
/// ```text
/// One or more parameter values were invalid: Value '2' at 'SearchSchema' failed to
/// satisfy constraint: Member must have HASH count less than or equal to 1
/// ```
///
/// Note the field is `SearchSchema` in its request-shape capitalisation, not the
/// lower-camel positional path used by the projection message: the service is not
/// internally consistent here, so each message is reproduced as observed rather
/// than normalised.
fn validate_search_schema_shape(
    search_schema: Option<&[crate::types::SearchSchemaElement]>,
) -> Result<(), DynamoDbError> {
    let Some(elements) = search_schema else {
        return Ok(());
    };
    let hash_count = elements
        .iter()
        .filter(|e| e.element_type == crate::types::SearchSchemaElementType::Hash)
        .count();
    if hash_count > MAX_SEARCH_SCHEMA_HASH {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Value '{hash_count}' at \
             'SearchSchema' failed to satisfy constraint: Member must have HASH count \
             less than or equal to {MAX_SEARCH_SCHEMA_HASH}"
        )));
    }
    let filter_count = elements
        .iter()
        .filter(|e| e.element_type == crate::types::SearchSchemaElementType::InlineFilter)
        .count();
    if filter_count > MAX_SEARCH_SCHEMA_INLINE_FILTERS {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Value '{filter_count}' at \
             'SearchSchema' failed to satisfy constraint: Member must have \
             INLINE_FILTER count less than or equal to \
             {MAX_SEARCH_SCHEMA_INLINE_FILTERS}"
        )));
    }
    Ok(())
}

/// Rules that apply to one vector index specification, wherever it arrives from.
///
/// Each index needs a `Projection`, a well-formed name, `Dimensions` in `1..=4096`,
/// a non-empty vector attribute name, and a search schema within the measured HASH
/// and `INLINE_FILTER` caps. The distance function is enforced by the type system
/// through enum deserialization.
///
/// Multi-fault parity is deliberately not attempted: the service aggregates faults
/// (`"N validation errors detected"`) with its own field ordering, whereas this
/// returns the first fault it finds and hardcodes a count of one. Single-fault
/// wording is measured and exact, which is what a client parsing one error sees.
///
/// `CreateTable` and `UpdateTable`'s create action carry the same shape, so they
/// get the same rules: a malformed index is rejected identically whichever path
/// it arrives by, rather than each handler enforcing its own subset.
///
/// `position` numbers the element within its request list, from 1, because that is
/// how the service numbers it in the message below.
/// Attribute-definition rules for one vector index, applied on both paths.
///
/// Both are decidable from the request alone, so they belong here rather than
/// in a backend: the vector attribute must NOT be declared in
/// `AttributeDefinitions` (the opposite of the rule for key attributes), and
/// every `SearchSchema` element MUST be. Measured 2026-08-13; on `UpdateTable`
/// the search-schema definition must be present in that request even when the
/// attribute is already declared on the table.
fn validate_vector_index_attribute_definitions(
    vi: &crate::types::VectorIndexSpecification,
    attribute_definitions: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    let declared = |name: &str| {
        attribute_definitions
            .iter()
            .any(|ad| ad.attribute_name == name)
    };
    let vector_attr = &vi.vector_attribute.attribute_name;
    if declared(vector_attr) {
        return Err(DynamoDbError::ValidationException(
            crate::types::vector_attribute_conflicting_definition(vector_attr),
        ));
    }
    for element in vi.search_schema.iter().flatten() {
        if !declared(&element.attribute_name) {
            return Err(DynamoDbError::ValidationException(
                crate::types::VECTOR_SEARCH_SCHEMA_UNDECLARED.to_owned(),
            ));
        }
    }
    Ok(())
}

/// `member_path` is the request-model path to the index element, without the
/// trailing leaf: `vectorIndexes.{n}.member` on CreateTable and
/// `vectorIndexUpdates.{n}.member.create` on UpdateTable, both 1-based.
/// The `.create` segment on the update path is measured, not inferred:
/// probed live 2026-08-24 (us-east-1) with client-side validation disabled,
/// a `Create` with `Dimensions: 0` answers
/// `Value '0' at 'vectorIndexUpdates.1.member.create.dimensions' ...`.
fn validate_one_vector_index(
    vi: &crate::types::VectorIndexSpecification,
    member_path: &str,
) -> Result<(), DynamoDbError> {
    // Projection is required by the service. Reported with the service's own
    // message, which numbers the offending list element from 1, not 0. Measured
    // on 2026-08-06 by bypassing botocore's client-side check so the request
    // reached the service:
    //   Value null at 'vectorIndexes.1.member.projection' failed to satisfy
    //   constraint: Member must not be null
    // and re-measured on the update path 2026-08-24:
    //   Value null at 'vectorIndexUpdates.1.member.create.projection' ...
    if vi.projection.is_none() {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value null at \
             '{member_path}.projection' failed to satisfy constraint: \
             Member must not be null"
        )));
    }
    validate_index_name(&vi.index_name)?;
    validate_search_schema_shape(vi.search_schema.as_deref())?;
    // The two bounds are reported by DIFFERENT layers with different messages.
    // Dimensions below 1 never reaches the semantic check on the service: the
    // request model rejects it first, with the smithy-style single-fault
    // message. Measured 2026-08-24 in us-east-1 with botocore's client-side
    // validation disabled so Dimensions=0 reached the service:
    //   1 validation error detected: Value '0' at
    //   'vectorIndexes.1.member.dimensions' failed to satisfy constraint:
    //   Member must have value greater than or equal to 1
    if vi.dimensions < 1 {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '{}' at \
             '{member_path}.dimensions' failed to satisfy constraint: \
             Member must have value greater than or equal to 1",
            vi.dimensions
        )));
    }
    if vi.dimensions > 4096 {
        // Verified against the service 2026-08-05: Dimensions=4097 and 8192
        // both return exactly this message.
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Number of dimensions must be \
             between 1 and 4096 inclusive."
                .to_owned(),
        ));
    }
    if vi.vector_attribute.attribute_name.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "VectorAttribute.AttributeName must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn validate_vector_indexes(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    let Some(vis) = input.vector_indexes.as_ref() else {
        return Ok(());
    };

    // Per-index shape first, then the table-level constraints. Which the service
    // reports first is unobservable from outside, because botocore rejects a
    // malformed index client-side before the request is sent, so the order that
    // preserves the already-measured per-index messages is the right one to keep.
    for (position, vi) in vis.iter().enumerate() {
        validate_one_vector_index(vi, &format!("vectorIndexes.{}.member", position + 1))?;
    }

    // Vector indexes are supported only on on-demand tables. Documented under
    // "Requirements and limitations" and again in the quota table, which lists
    // vector index capacity mode as on-demand only. `BillingMode` defaults to
    // PROVISIONED when absent, so an omitted BillingMode is a rejection too.
    //
    // Wording measured against the service on 2026-08-11; the earlier text was a
    // reasonable paraphrase but not what the service says.
    if !vis.is_empty()
        && input.billing_mode.unwrap_or(BillingMode::Provisioned) != BillingMode::PayPerRequest
    {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Vector indexes are only supported \
             for PAY_PER_REQUEST tables"
                .to_owned(),
        ));
    }

    // Wording measured against the service on 2026-08-11. Note the service does not
    // echo the offending count here, unlike its SearchSchema messages, so neither
    // does this.
    if vis.len() > MAX_VECTOR_INDEXES_PER_TABLE {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: VectorIndex count exceeds the \
             per-table limit of {MAX_VECTOR_INDEXES_PER_TABLE}"
        )));
    }

    // Several indexes may share one vector attribute, but they must agree on its
    // Dimensions: the attribute stores one vector, so two indexes declaring
    // different sizes for it contradict each other. The service rejects the
    // create naming the attribute; wording pinned by the ground-truth runs of
    // 2026-08-24 (us-east-1 and eu-west-2 both answer exactly this).
    for (i, vi) in vis.iter().enumerate() {
        let conflicting = vis[..i].iter().any(|prior| {
            prior.vector_attribute.attribute_name == vi.vector_attribute.attribute_name
                && prior.dimensions != vi.dimensions
        });
        if conflicting {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: Conflicting attribute \
                 definition for '{}'. All VectorIndexes on the same vector attribute \
                 must use the same dimensions.",
                vi.vector_attribute.attribute_name
            )));
        }
    }

    Ok(())
}

/// Validate the vector index changes on an `UpdateTable` request.
///
/// Deliberately limited to what core can decide from the request alone. It does
/// not check whether a created name is already taken, or whether a deleted index
/// exists, because the service's error for a name clash changes CLASS with the
/// state of the existing index, `ValidationException` when it is ACTIVE and
/// `ResourceInUseException` while it is still creating, and `TableKeyInfo` does
/// not carry index status. Reporting the wrong class would be worse than leaving
/// it to the layer that knows. Those messages are recorded as
/// [`VECTOR_INDEX_ALREADY_EXISTS`](crate::types::VECTOR_INDEX_ALREADY_EXISTS) and
/// [`VECTOR_INDEX_CREATE_IN_USE_PREFIX`](crate::types::VECTOR_INDEX_CREATE_IN_USE_PREFIX)
/// so both backends produce identical text.
///
/// # Errors
/// Returns [`DynamoDbError::ValidationException`] if a create action carries a
/// malformed index.
pub fn validate_vector_index_updates(
    updates: Option<&Vec<crate::types::VectorIndexUpdate>>,
    attribute_definitions: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    let Some(updates) = updates else {
        return Ok(());
    };
    if updates.is_empty() {
        return Ok(());
    }
    // Per-element request-model validation FIRST, then the one-action rule.
    // The ordering is measured, not chosen: probed live 2026-08-24
    // (us-east-1), a two-action request carrying a malformed create (a
    // Dimensions of 0, or a missing Projection) answers the model-layer
    // ValidationException for the malformed element, not the subscriber
    // limit, so the model layer runs over every element before any online
    // rule. (The service aggregates multiple model faults into one "N
    // validation errors detected" message; single-fault reporting here is the
    // same deliberate divergence documented on validate_one_vector_index.)
    for (position, update) in updates.iter().enumerate() {
        if let Some(create) = update.create.as_ref() {
            validate_one_vector_index(
                create,
                &format!("vectorIndexUpdates.{}.member.create", position + 1),
            )?;
            validate_vector_index_attribute_definitions(create, attribute_definitions)?;
        }
        if let Some(delete) = update.delete.as_ref() {
            validate_index_name(&delete.index_name)?;
        }
    }
    // One online index action per call, enforced with the online-index
    // machinery's own error class and wording. Pinned by the ground-truth runs
    // of 2026-08-24 (us-east-1 and eu-west-2): two well-formed Create actions
    // in one UpdateTable answer LimitExceededException with exactly this
    // sentence. Counted over actions rather than elements, so an element
    // carrying both a Create and a Delete counts as two.
    let actions = updates
        .iter()
        .map(|u| usize::from(u.create.is_some()) + usize::from(u.delete.is_some()))
        .sum::<usize>();
    if actions > 1 {
        return Err(DynamoDbError::LimitExceededException(
            "Subscriber limit exceeded: Only 1 online index can be created or deleted \
             simultaneously per table"
                .to_owned(),
        ));
    }
    Ok(())
}

/// Validate that INCLUDE-projection secondary indexes specify `NonKeyAttributes`.
///
/// Real `DynamoDB` rejects a GSI or LSI whose `ProjectionType` is `INCLUDE`
/// without a non-empty `NonKeyAttributes` list, and conversely rejects a
/// `KEYS_ONLY` or `ALL` projection that carries `NonKeyAttributes`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for any such index.
fn validate_index_projections(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    use crate::types::{Projection, ProjectionType};

    // Validates a single index projection against real DynamoDB rules:
    // `INCLUDE` requires a non-empty `NonKeyAttributes`, while `KEYS_ONLY`
    // and `ALL` must not carry `NonKeyAttributes`.
    fn check(projection: &Projection) -> Result<(), DynamoDbError> {
        let has_attrs = projection
            .non_key_attributes
            .as_ref()
            .is_some_and(|attrs| !attrs.is_empty());
        let message = match projection.projection_type {
            ProjectionType::Include if !has_attrs => {
                Some("ProjectionType is INCLUDE, but NonKeyAttributes is not specified")
            }
            ProjectionType::KeysOnly if has_attrs => {
                Some("ProjectionType is KEYS_ONLY, but NonKeyAttributes is specified")
            }
            ProjectionType::All if has_attrs => {
                Some("ProjectionType is ALL, but NonKeyAttributes is specified")
            }
            _ => None,
        };
        if let Some(message) = message {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: {message}"
            )));
        }
        Ok(())
    }
    if let Some(gsis) = &input.global_secondary_indexes {
        for gsi in gsis {
            check(&gsi.projection)?;
        }
    }
    if let Some(lsis) = &input.local_secondary_indexes {
        for lsi in lsis {
            check(&lsi.projection)?;
        }
    }
    // Vector indexes were omitted from this function, so a vector index could
    // declare ProjectionType INCLUDE with no NonKeyAttributes and be accepted,
    // where the service refuses it with the message `check` already produces.
    // The rules are not vector-specific, so the fix is to iterate them here
    // rather than to restate the rules somewhere else.
    //
    // `projection` is optional on a vector index specification and its absence is
    // a separate fault reported by `validate_one_vector_index`, so it is skipped
    // here rather than being reported twice with different wording.
    if let Some(vis) = &input.vector_indexes {
        for vi in vis {
            if let Some(projection) = &vi.projection {
                check(projection)?;
            }
        }
    }
    Ok(())
}

/// Validate the `StreamSpecification`: a disabled stream must not also specify a
/// `StreamViewType`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when `StreamEnabled` is false
/// but a `StreamViewType` is present.
fn validate_stream_specification(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    if let Some(stream) = &input.stream_specification
        && !stream.stream_enabled
        && stream.stream_view_type.is_some()
    {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Table is being created with a stream disabled, UpdateViewType should not be specified".to_owned(),
        ));
    }
    Ok(())
}

/// Format `KeySchema` elements in `DynamoDB`'s Java-toString style for error messages.
fn format_key_schema_value(ks: &[KeySchemaElement]) -> String {
    let elements: Vec<String> = ks
        .iter()
        .map(|e| {
            let kt = match e.key_type {
                KeyType::Hash => "HASH",
                KeyType::Range => "RANGE",
            };
            format!(
                "KeySchemaElement(attributeName={}, keyType={})",
                e.attribute_name, kt
            )
        })
        .collect();
    format!("[{}]", elements.join(", "))
}

/// Maximum number of HASH or RANGE elements in a multi-part key schema.
const MAX_MULTIPART_KEY_ELEMENTS: usize = 4;

fn validate_key_schema(
    input: &CreateTableInput,
    allow_multipart: bool,
) -> Result<(), DynamoDbError> {
    if input.key_schema.is_empty() {
        return Err(DynamoDbError::ValidationException(error_message(
            ErrorMessageKey::KeySchemaTooMany,
            &[],
        )));
    }
    if input.key_schema[0].key_type != KeyType::Hash {
        return Err(DynamoDbError::ValidationException(error_message(
            ErrorMessageKey::KeySchemaFirstNotHash,
            &[],
        )));
    }

    if allow_multipart {
        validate_multipart_key_schema(&input.key_schema, "table")?;
    } else {
        // Standard DynamoDB: 1 HASH + optional 1 RANGE
        if input.key_schema.len() > 2 {
            let ks_repr = format_key_schema_value(&input.key_schema);
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '{ks_repr}' at 'keySchema' failed to satisfy constraint: \
                 Member must have length less than or equal to 2"
            )));
        }
        if input.key_schema.len() == 2 {
            if input.key_schema[1].key_type != KeyType::Range {
                return Err(DynamoDbError::ValidationException(
                    "Second KeySchemaElement is not a RANGE type".to_owned(),
                ));
            }
            if input.key_schema[0].attribute_name == input.key_schema[1].attribute_name {
                return Err(DynamoDbError::ValidationException(
                    "Invalid KeySchema: Some index key attribute have no definition".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

/// Validate a multi-part key schema: all HASH elements first, then all RANGE elements,
/// up to 4 of each type.
fn validate_multipart_key_schema(
    key_schema: &[KeySchemaElement],
    context: &str,
) -> Result<(), DynamoDbError> {
    let hash_count = key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Hash)
        .count();
    let range_count = key_schema
        .iter()
        .filter(|ks| ks.key_type == KeyType::Range)
        .count();

    if hash_count == 0 {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: {context} KeySchema must have at least one HASH key"
        )));
    }
    if hash_count > MAX_MULTIPART_KEY_ELEMENTS {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: {context} KeySchema exceeds maximum of {MAX_MULTIPART_KEY_ELEMENTS} HASH key attributes"
        )));
    }
    if range_count > MAX_MULTIPART_KEY_ELEMENTS {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: {context} KeySchema exceeds maximum of {MAX_MULTIPART_KEY_ELEMENTS} RANGE key attributes"
        )));
    }

    // HASH elements must come before RANGE elements
    let mut seen_range = false;
    for ks in key_schema {
        match ks.key_type {
            KeyType::Hash => {
                if seen_range {
                    return Err(DynamoDbError::ValidationException(format!(
                        "One or more parameter values were invalid: {context} KeySchema: HASH key attributes must precede RANGE key attributes"
                    )));
                }
            }
            KeyType::Range => {
                seen_range = true;
            }
        }
    }
    Ok(())
}

/// Validate GSI key schemas: 1–4 HASH elements followed by 0–4 RANGE elements.
/// Multi-part keys are always allowed on GSIs.
fn validate_gsi_key_schemas(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    let Some(gsis) = &input.global_secondary_indexes else {
        return Ok(());
    };
    for gsi in gsis {
        validate_index_name(&gsi.index_name)?;
        if gsi.key_schema.is_empty() {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: No defined key schema for index: {}",
                gsi.index_name
            )));
        }
        if gsi.key_schema[0].key_type != KeyType::Hash {
            return Err(DynamoDbError::ValidationException(
                "One or more parameter values were invalid: Index KeySchema: The first KeySchemaElement is not a HASH type".to_owned(),
            ));
        }
        validate_multipart_key_schema(&gsi.key_schema, &format!("Index {}", gsi.index_name))?;
    }
    Ok(())
}

/// Validate LSI key schemas: each must have exactly 2 elements, HASH key must match
/// the table's HASH key, second element must be RANGE.
/// LSIs do not support multi-part keys (same as real `DynamoDB`).
fn validate_lsi_key_schemas(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    let Some(lsis) = &input.local_secondary_indexes else {
        return Ok(());
    };
    let table_hash_key = &input.key_schema[0].attribute_name;
    for lsi in lsis {
        validate_index_name(&lsi.index_name)?;
        match lsi.key_schema.as_slice() {
            [hash, range] => {
                if hash.key_type != KeyType::Hash {
                    return Err(DynamoDbError::ValidationException(
                        "One or more parameter values were invalid: Index KeySchema: The first KeySchemaElement is not a HASH type".to_owned(),
                    ));
                }
                if hash.attribute_name != *table_hash_key {
                    return Err(DynamoDbError::ValidationException(
                        "One or more parameter values were invalid: Table KeySchema: The HASH key of a local secondary index must be the same as the HASH key of the table".to_owned(),
                    ));
                }
                if range.key_type != KeyType::Range {
                    return Err(DynamoDbError::ValidationException(
                        "One or more parameter values were invalid: Index KeySchema: The second KeySchemaElement is not a RANGE type".to_owned(),
                    ));
                }
            }
            [] | [_] => {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: No defined key schema for index: {}",
                    lsi.index_name
                )));
            }
            _ => {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: Too many KeySchema attributes for index: {}",
                    lsi.index_name
                )));
            }
        }
    }
    Ok(())
}

fn validate_attribute_definitions(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    // A vector attribute must NOT be declared in AttributeDefinitions, and this is
    // checked before anything else here because the attribute may simultaneously be
    // a legitimate key attribute: without this, naming the table's own partition key
    // as the VectorAttribute was accepted, since `pk` satisfied both the
    // definition-exists and the definition-is-used checks below.
    //
    // The rule follows from the shape: `VectorAttributeDefinition` carries only
    // `AttributeName` and no type, because a vector is not a scalar type that
    // AttributeDefinitions can express. Measured against the service on 2026-08-11.
    if let Some(vis) = &input.vector_indexes {
        for vi in vis {
            let vec_attr = vi.vector_attribute.attribute_name.as_str();
            if input
                .attribute_definitions
                .iter()
                .any(|ad| ad.attribute_name == vec_attr)
            {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: Conflicting attribute \
                     definition for '{vec_attr}'. An attribute cannot be defined in \
                     AttributeDefinitions when used as a VectorAttribute."
                )));
            }
        }
    }

    // Collect all key attribute names from table + GSIs + LSIs
    let mut key_attrs: Vec<&str> = input
        .key_schema
        .iter()
        .map(|ks| ks.attribute_name.as_str())
        .collect();

    if let Some(gsis) = &input.global_secondary_indexes {
        for gsi in gsis {
            for ks in &gsi.key_schema {
                if !key_attrs.contains(&ks.attribute_name.as_str()) {
                    key_attrs.push(&ks.attribute_name);
                }
            }
        }
    }
    if let Some(lsis) = &input.local_secondary_indexes {
        for lsi in lsis {
            for ks in &lsi.key_schema {
                if !key_attrs.contains(&ks.attribute_name.as_str()) {
                    key_attrs.push(&ks.attribute_name);
                }
            }
        }
    }
    // Vector-index search-schema attributes are declared in AttributeDefinitions
    // but are not part of the base or secondary-index key schema, so count them
    // as used to satisfy the definition/key correspondence check.
    //
    // They are checked for existence HERE, with their own message, rather than
    // being folded into `key_attrs` and reported by the loop below. The service
    // distinguishes the two cases and this previously reused the GSI key wording,
    // naming the attribute and listing every definition where the service says only
    // that one element is undefined. Measured on 2026-08-11.
    let def_names: Vec<&str> = input
        .attribute_definitions
        .iter()
        .map(|ad| ad.attribute_name.as_str())
        .collect();
    if let Some(vis) = &input.vector_indexes {
        for vi in vis {
            if let Some(schema) = &vi.search_schema {
                for element in schema {
                    if !def_names.contains(&element.attribute_name.as_str()) {
                        return Err(DynamoDbError::ValidationException(
                            "One or more parameter values were invalid: One element in \
                             SearchSchema is not defined in attribute definitions"
                                .to_owned(),
                        ));
                    }
                    if !key_attrs.contains(&element.attribute_name.as_str()) {
                        key_attrs.push(&element.attribute_name);
                    }
                }
            }
        }
    }

    // Every key attribute must have a definition
    for attr in &key_attrs {
        if !def_names.contains(attr) {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: Some index key attributes are not defined in AttributeDefinitions. Keys: [{attr}], AttributeDefinitions: [{}]",
                format_attr_defs(&input.attribute_definitions)
            )));
        }
    }

    // Every definition must be used by a key
    for def in &def_names {
        if !key_attrs.contains(def) {
            return Err(DynamoDbError::ValidationException(error_message(
                ErrorMessageKey::AttrDefNotInKey,
                &[&input.table_name],
            )));
        }
    }

    Ok(())
}

fn format_attr_defs(defs: &[AttributeDefinition]) -> String {
    defs.iter()
        .map(|d| d.attribute_name.clone())
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_provisioned_throughput(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    let billing = input.billing_mode.unwrap_or(BillingMode::Provisioned);
    match billing {
        BillingMode::Provisioned => {
            let Some(pt) = &input.provisioned_throughput else {
                return Err(DynamoDbError::ValidationException(
                    "No provisioned throughput specified for the table".to_owned(),
                ));
            };
            if pt.read_capacity_units < 1 || pt.write_capacity_units < 1 {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: ReadCapacityUnits and WriteCapacityUnits must both be greater than or equal to 1 for table".to_owned(),
                ));
            }
        }
        BillingMode::PayPerRequest => {
            if input.provisioned_throughput.is_some() {
                return Err(DynamoDbError::ValidationException(
                    "One or more parameter values were invalid: Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

/// Reject `ProvisionedThroughput` on GSIs when the table uses `PayPerRequest`.
/// Real `DynamoDB` returns: "One or more parameter values were invalid:
/// `ProvisionedThroughput` should not be specified for index: <name> when
/// `BillingMode` is `PAY_PER_REQUEST`"
fn validate_gsi_provisioned_throughput(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    let billing = input.billing_mode.unwrap_or(BillingMode::Provisioned);
    if billing != BillingMode::PayPerRequest {
        return Ok(());
    }
    if let Some(gsis) = &input.global_secondary_indexes {
        for gsi in gsis {
            if gsi.provisioned_throughput.is_some() {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: \
                     ProvisionedThroughput should not be specified for index: {} \
                     when BillingMode is PAY_PER_REQUEST",
                    gsi.index_name
                )));
            }
        }
    }
    Ok(())
}

fn validate_gsi_count(
    input: &CreateTableInput,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    if let Some(gsis) = &input.global_secondary_indexes
        && gsis.len() > limits.max_gsis_per_table
    {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: GlobalSecondaryIndexes count exceeds limit of {}",
            limits.max_gsis_per_table
        )));
    }
    Ok(())
}

fn validate_lsi_count(
    input: &CreateTableInput,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    if let Some(lsis) = &input.local_secondary_indexes
        && lsis.len() > limits.max_lsis_per_table
    {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: LocalSecondaryIndexes count exceeds limit of {}",
            limits.max_lsis_per_table
        )));
    }
    Ok(())
}

/// Validate a `PutItem` request.
///
/// Checks table name, item size, key presence/types, and `ReturnValues`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for invalid input.
pub fn validate_put_item(
    input: &PutItemInput,
    limits: &LimitsConfig,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    validate_table_name(&input.table_name, limits)?;

    // REQ-DATA-001: PutItem only supports NONE and ALL_OLD
    if !matches!(
        input.return_values,
        ReturnValues::None | ReturnValues::AllOld
    ) {
        return Err(DynamoDbError::ValidationException(
            "Return values set to invalid value".to_owned(),
        ));
    }

    validate_item_keys(&input.item, key_schema, attr_defs)?;
    validate_attribute_name_sizes(&input.item, limits)?;
    validate_item_numbers(&input.item)?;
    validate_item_nesting_depth(&input.item)?;

    let size = item_size_bytes(&input.item);
    if size > limits.max_item_size_bytes {
        return Err(DynamoDbError::ValidationException(
            "Item size has exceeded the maximum allowed size".to_owned(),
        ));
    }

    validate_key_sizes(&input.item, key_schema, limits)?;

    Ok(())
}

/// Validate a `GetItem` request.
///
/// Checks table name and key presence/types.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for invalid input.
pub fn validate_get_item(
    input: &GetItemInput,
    limits: &LimitsConfig,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    validate_table_name(&input.table_name, limits)?;
    validate_key_only(&input.key, key_schema, attr_defs)?;
    validate_key_sizes(&input.key, key_schema, limits)?;
    Ok(())
}

/// Validate a `DeleteItem` request.
///
/// Checks table name, key presence/types, and `ReturnValues`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for invalid input.
pub fn validate_delete_item(
    input: &DeleteItemInput,
    limits: &LimitsConfig,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    validate_table_name(&input.table_name, limits)?;

    // DeleteItem only supports NONE and ALL_OLD
    if !matches!(
        input.return_values,
        ReturnValues::None | ReturnValues::AllOld
    ) {
        return Err(DynamoDbError::ValidationException(
            "Return values set to invalid value".to_owned(),
        ));
    }

    validate_key_only(&input.key, key_schema, attr_defs)?;
    validate_key_sizes(&input.key, key_schema, limits)?;
    Ok(())
}

/// Validate an `UpdateItem` request.
///
/// Checks table name, key presence/types, and `ReturnValues`.
/// `UpdateExpression` parsing is handled separately by the expression engine.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for invalid input.
pub fn validate_update_item(
    input: &UpdateItemInput,
    limits: &LimitsConfig,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    validate_table_name(&input.table_name, limits)?;
    validate_key_only(&input.key, key_schema, attr_defs)?;
    validate_key_sizes(&input.key, key_schema, limits)?;

    // Validate number values supplied via ExpressionAttributeValues. Unlike a
    // PutItem item (validated by validate_item_numbers), these are checked here
    // because the AttributeValue deserializer intentionally stores malformed
    // numbers raw and defers rejection to the validation layer. Real DynamoDB
    // wraps the error as:
    //   "ExpressionAttributeValues contains invalid value: <inner> for key <k>"
    if let Some(values) = &input.expression_attribute_values {
        validate_expression_attribute_value_numbers(values)?;
    }

    if let Some(updates) = &input.attribute_updates {
        validate_attribute_values_nesting_depth(updates.values().filter_map(|u| u.value.as_ref()))?;
        // Legacy AttributeUpdates values carry the bare numeric-value message,
        // matching real DynamoDB (no ExpressionAttributeValues wrapper).
        for update in updates.values() {
            if let Some(v) = &update.value {
                validate_attribute_number(v)?;
            }
        }
    }

    Ok(())
}

/// Validate number values provided via `ExpressionAttributeValues`, matching the
/// service's wrapped message: `ExpressionAttributeValues contains invalid value:
/// <inner> for key <placeholder>`. Keys are visited in sorted order so the
/// reported placeholder is deterministic when several values are invalid.
fn validate_expression_attribute_value_numbers(
    values: &std::collections::HashMap<String, AttributeValue>,
) -> Result<(), DynamoDbError> {
    let mut keys: Vec<&String> = values.keys().collect();
    keys.sort();
    for key in keys {
        match validate_attribute_number(&values[key]) {
            Ok(()) => {}
            Err(DynamoDbError::ValidationException(inner)) => {
                return Err(DynamoDbError::ValidationException(format!(
                    "ExpressionAttributeValues contains invalid value: {inner} for key {key}"
                )));
            }
            Err(other) => return Err(other),
        }
    }
    Ok(())
}

/// Validate that no attribute name exceeds the maximum allowed size (REQ-LIM-004).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` if any attribute name exceeds the limit.
pub fn validate_attribute_name_sizes(
    item: &Item,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    for name in item.keys() {
        if name.len() > limits.max_attribute_name_bytes {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: Size of attribute name '{}' \
                 has exceeded the maximum size limit of {} bytes",
                truncate_for_error(name),
                limits.max_attribute_name_bytes
            )));
        }
    }
    Ok(())
}

/// Truncate a string for inclusion in error messages.
fn truncate_for_error(s: &str) -> &str {
    let end = s.char_indices().nth(64).map_or(s.len(), |(idx, _)| idx);
    &s[..end]
}

/// Validate that an item contains all required key attributes with correct types.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` if a key attribute is missing or has the wrong type.
pub fn validate_item_keys(
    item: &Item,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    for ks in key_schema {
        let value = item.get(&ks.attribute_name).ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: Missing the key {} in the item",
                ks.attribute_name
            ))
        })?;
        validate_key_attribute_type(&ks.attribute_name, value, attr_defs)?;
    }
    Ok(())
}

/// Validate that a key map contains exactly the key attributes and nothing else.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` if the key has extra/missing attributes or wrong types.
pub fn validate_key_only(
    key: &Item,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    // Must contain exactly the key attributes
    let expected_count = key_schema.len();
    if key.len() != expected_count {
        return Err(DynamoDbError::ValidationException(
            "The provided key element does not match the schema".to_owned(),
        ));
    }

    for ks in key_schema {
        let value = key.get(&ks.attribute_name).ok_or_else(|| {
            DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: Missing the key {} in the item",
                ks.attribute_name
            ))
        })?;
        validate_key_attribute_type(&ks.attribute_name, value, attr_defs)?;
        validate_no_empty_key_value(&ks.attribute_name, value)?;
    }
    Ok(())
}

/// Batch-specific key validation: uses `DynamoDB`'s batch error message for type mismatches.
///
/// Real `DynamoDB` returns "The provided key element does not match the schema" for
/// batch operations, not the single-item "Type mismatch" message.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` on key count, missing key, or type mismatch.
pub fn validate_batch_key_only(
    key: &Item,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    validate_key_only(key, key_schema, attr_defs).map_err(remap_key_type_mismatch)
}

/// Batch-specific item key validation: uses `DynamoDB`'s batch error message for type mismatches.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` on missing key or type mismatch.
pub fn validate_batch_item_keys(
    item: &Item,
    key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    validate_item_keys(item, key_schema, attr_defs).map_err(remap_key_type_mismatch)
}

/// Remap key type-mismatch errors to the batch/transaction-specific message.
///
/// Real `DynamoDB` uses "The provided key element does not match the schema"
/// for batch and transaction operations, not the single-item "Type mismatch"
/// message.
fn remap_key_type_mismatch(err: DynamoDbError) -> DynamoDbError {
    match &err {
        DynamoDbError::ValidationException(msg) if msg.contains("Type mismatch for key ") => {
            DynamoDbError::ValidationException(
                "The provided key element does not match the schema".to_owned(),
            )
        }
        _ => err,
    }
}

/// Validate that a key attribute value matches the expected scalar type from `AttributeDefinitions`.
fn validate_key_attribute_type(
    attr_name: &str,
    value: &AttributeValue,
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    let expected_type = attr_defs
        .iter()
        .find(|ad| ad.attribute_name == attr_name)
        .map(|ad| ad.attribute_type);

    let Some(expected) = expected_type else {
        return Ok(());
    };

    let matches = matches!(
        (expected, value),
        (ScalarAttributeType::S, AttributeValue::S(_))
            | (ScalarAttributeType::N, AttributeValue::N(_))
            | (ScalarAttributeType::B, AttributeValue::B(_))
    );

    if !matches {
        let type_char = match expected {
            ScalarAttributeType::S => "S",
            ScalarAttributeType::N => "N",
            ScalarAttributeType::B => "B",
        };
        let actual_tag = attribute_value_type_tag(value);
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Type mismatch for key {attr_name} expected: {type_char} actual: {actual_tag}"
        )));
    }

    Ok(())
}

/// Validate that a key attribute value is not empty.
///
/// `DynamoDB` rejects empty-string and empty-binary values in key positions,
/// returning a `ValidationException` with a type-specific error message.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` if `value` is an empty string
/// (`S("")`) or empty binary (`B(<empty>)`).
fn validate_no_empty_key_value(
    attr_name: &str,
    value: &AttributeValue,
) -> Result<(), DynamoDbError> {
    let kind = match value {
        AttributeValue::S(s) if s.is_empty() => Some("string"),
        AttributeValue::B(b) if b.is_empty() => Some("binary"),
        _ => None,
    };
    if let Some(kind) = kind {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values are not valid. \
             The AttributeValue for a key attribute cannot contain an empty {kind} value. \
             Key: {attr_name}"
        )));
    }
    Ok(())
}

/// A secondary index's name and key schema, for index-key validation.
///
/// Decouples [`validate_index_keys`] from the storage layer's index metadata
/// type: callers build these refs from whatever index representation they hold.
pub struct IndexKeyRef<'a> {
    pub index_name: &'a str,
    pub key_schema: &'a [KeySchemaElement],
}

/// The `DynamoDB` type tag for an `AttributeValue`, used in `Actual: <tag>`
/// error text (e.g. `N`, `L`, `BOOL`).
fn attribute_value_type_tag(v: &AttributeValue) -> &'static str {
    match v {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
    }
}

/// The scalar type tag (`S`, `N`, or `B`) for a declared attribute type.
fn scalar_type_tag(t: ScalarAttributeType) -> &'static str {
    match t {
        ScalarAttributeType::S => "S",
        ScalarAttributeType::N => "N",
        ScalarAttributeType::B => "B",
    }
}

/// Context for a secondary-index empty-key error, selecting the message form.
#[derive(Debug, Clone, Copy)]
pub enum SecondaryIndexEmptyContext {
    /// The value came directly from a written item (PutItem / Put transaction).
    Item,
    /// The value was produced by an UpdateExpression (Update transaction).
    UpdateExpression,
}

/// Validate that secondary-index key attributes present in `item` match their
/// declared scalar type.
///
/// Indexes are checked in name order, so when an attribute keys more than one
/// index the alphabetically-first index name is the one reported (matching
/// observed `DynamoDB` behaviour).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` on the first index key attribute
/// whose value type does not match its declared scalar type.
pub fn validate_index_key_types(
    item: &Item,
    indexes: &[IndexKeyRef<'_>],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    for idx in ordered_indexes(indexes) {
        for ks in idx.key_schema {
            let Some(value) = item.get(&ks.attribute_name) else {
                continue;
            };
            let Some(expected) = attr_defs
                .iter()
                .find(|ad| ad.attribute_name == ks.attribute_name)
                .map(|ad| ad.attribute_type)
            else {
                continue;
            };
            let matches = matches!(
                (expected, value),
                (ScalarAttributeType::S, AttributeValue::S(_))
                    | (ScalarAttributeType::N, AttributeValue::N(_))
                    | (ScalarAttributeType::B, AttributeValue::B(_))
            );
            if !matches {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: Type mismatch for Index Key {} Expected: {} Actual: {} IndexName: {}",
                    ks.attribute_name,
                    scalar_type_tag(expected),
                    attribute_value_type_tag(value),
                    idx.index_name
                )));
            }
        }
    }
    Ok(())
}

/// Validate that no secondary-index key attribute in `item` is an empty string
/// or empty binary value.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` naming the alphabetically-first
/// index for the first empty index key attribute found. The message form is
/// selected by `ctx`.
pub fn validate_index_key_not_empty(
    item: &Item,
    indexes: &[IndexKeyRef<'_>],
    ctx: SecondaryIndexEmptyContext,
) -> Result<(), DynamoDbError> {
    for idx in ordered_indexes(indexes) {
        for ks in idx.key_schema {
            let Some(value) = item.get(&ks.attribute_name) else {
                continue;
            };
            // The message names the offending value's kind: "string" for S,
            // "binary" for B, in both message forms (measured; the binary
            // wording was previously hardcoded to "string").
            let kind = match value {
                AttributeValue::S(s) if s.is_empty() => "string",
                AttributeValue::B(b) if b.is_empty() => "binary",
                _ => continue,
            };
            {
                let msg = match ctx {
                    SecondaryIndexEmptyContext::Item => format!(
                        "One or more parameter values are not valid. A value specified for a secondary index key is not supported. \
                         The AttributeValue for a key attribute cannot contain an empty {kind} value. IndexName: {}, IndexKey: {}",
                        idx.index_name, ks.attribute_name
                    ),
                    SecondaryIndexEmptyContext::UpdateExpression => format!(
                        "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. \
                         The AttributeValue for a key attribute cannot contain an empty {kind} value."
                    ),
                };
                return Err(DynamoDbError::ValidationException(msg));
            }
        }
    }
    Ok(())
}

/// Validate secondary-index key attributes for a written item: scalar type match
/// first, then non-empty. Convenience wrapper over [`validate_index_key_types`]
/// and [`validate_index_key_not_empty`] for the PutItem / BatchWriteItem paths,
/// where both faults are reported as a top-level `ValidationException`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` on the first offending index key.
pub fn validate_index_keys(
    item: &Item,
    indexes: &[IndexKeyRef<'_>],
    attr_defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    validate_index_key_types(item, indexes, attr_defs)?;
    validate_index_key_not_empty(item, indexes, SecondaryIndexEmptyContext::Item)
}

/// Indexes sorted by name, so the alphabetically-first index that uses a
/// violating attribute is the one reported.
fn ordered_indexes<'b, 'a>(indexes: &'b [IndexKeyRef<'a>]) -> Vec<&'b IndexKeyRef<'a>> {
    let mut ordered: Vec<&IndexKeyRef<'_>> = indexes.iter().collect();
    ordered.sort_by(|a, b| a.index_name.cmp(b.index_name));
    ordered
}

/// Readable flags for the `is_query` parameter of [`validate_select_projection`].
/// Real DynamoDB prepends `1 validation error detected: ` to some rejections for
/// Query but not for Scan, so callers pass one of these instead of a bare bool.
pub const IS_QUERY: bool = true;
pub const IS_SCAN: bool = false;

/// Validate the `Select` value against `ProjectionExpression` / `AttributesToGet`
/// presence and `IndexName`. Shared by Query and Scan so both reject the same
/// invalid combinations with the same messages.
///
/// Rules (matching real DynamoDB, in this order):
/// 1. A `ProjectionExpression` with `ALL_ATTRIBUTES`, `ALL_PROJECTED_ATTRIBUTES`,
///    or `COUNT` is rejected (reported before the `IndexName` requirement).
/// 2. `SPECIFIC_ATTRIBUTES` requires a `ProjectionExpression` (or legacy
///    `AttributesToGet`).
/// 3. `ALL_PROJECTED_ATTRIBUTES` requires an `IndexName`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` for any of the above.
pub fn validate_select_projection(
    select: Option<Select>,
    has_projection: bool,
    has_attributes_to_get: bool,
    has_index_name: bool,
    is_query: bool,
) -> Result<(), DynamoDbError> {
    if has_projection {
        let incompatible = match select {
            Some(Select::AllAttributes) => Some("ALL_ATTRIBUTES"),
            Some(Select::AllProjectedAttributes) => Some("ALL_PROJECTED_ATTRIBUTES"),
            Some(Select::Count) => Some("only the Count"),
            _ => None,
        };
        if let Some(what) = incompatible {
            // Real DynamoDB prepends "1 validation error detected: " to this
            // rejection for Query, but NOT for Scan.
            let body =
                format!("Cannot specify the ProjectionExpression when choosing to get {what}");
            let msg = if is_query {
                format!("1 validation error detected: {body}")
            } else {
                body
            };
            return Err(DynamoDbError::ValidationException(msg));
        }
    }
    if matches!(select, Some(Select::SpecificAttributes))
        && !has_projection
        && !has_attributes_to_get
    {
        // Same envelope split as the incompatible-Select rejection above:
        // Query carries the "1 validation error detected: " prefix, Scan does
        // not (both measured, 2026-08-24 ground-truth runs).
        let body = "Must specify the AttributesToGet or ProjectionExpression \
                    when choosing to get SPECIFIC_ATTRIBUTES";
        let msg = if is_query {
            format!("1 validation error detected: {body}")
        } else {
            body.to_owned()
        };
        return Err(DynamoDbError::ValidationException(msg));
    }
    if matches!(select, Some(Select::AllProjectedAttributes)) && !has_index_name {
        return Err(DynamoDbError::ValidationException(
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName".to_owned(),
        ));
    }
    Ok(())
}

/// Reject requests that mix legacy (non-expression) and expression parameters.
///
/// Each slice lists the API's parameters as `(name, present)` pairs in the
/// canonical order Amazon DynamoDB reports them. If at least one parameter is
/// present on each side, the error message lists every present parameter.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when both sides are non-empty.
pub fn validate_no_expression_param_mixing(
    non_expression: &[(&str, bool)],
    expression: &[(&str, bool)],
) -> Result<(), DynamoDbError> {
    // Happy path allocates nothing; the name lists are built only on error.
    let any_present = |params: &[(&str, bool)]| params.iter().any(|(_, p)| *p);
    if !any_present(non_expression) || !any_present(expression) {
        return Ok(());
    }
    fn present_names(params: &[(&str, bool)]) -> String {
        params
            .iter()
            .filter(|(_, p)| *p)
            .map(|(n, _)| *n)
            .collect::<Vec<_>>()
            .join(", ")
    }
    Err(DynamoDbError::ValidationException(format!(
        "Can not use both expression and non-expression parameters in the same request: \
         Non-expression parameters: {{{}}} Expression parameters: {{{}}}",
        present_names(non_expression),
        present_names(expression)
    )))
}

/// Validate `ConditionalOperator` accompanies a legacy Filter/Expected with
/// two or more conditions, matching Amazon DynamoDB.
///
/// `condition_count` is the number of entries in the request's legacy
/// `ScanFilter`/`QueryFilter`/`Expected` map (0 when absent or empty).
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when `ConditionalOperator` is
/// present with fewer than two legacy conditions.
pub fn validate_conditional_operator_usage(
    conditional_operator_present: bool,
    condition_count: usize,
) -> Result<(), DynamoDbError> {
    if !conditional_operator_present {
        return Ok(());
    }
    match condition_count {
        0 => Err(DynamoDbError::ValidationException(
            "ConditionalOperator cannot be used without Filter or Expected".to_owned(),
        )),
        1 => Err(DynamoDbError::ValidationException(
            "ConditionalOperator can only be used when Filter or Expected has two or more elements"
                .to_owned(),
        )),
        _ => Ok(()),
    }
}

/// Reject `Select=ALL_ATTRIBUTES` against a global secondary index whose
/// projection type is not `ALL` (such an index cannot serve every attribute).
/// Shared by Query and Scan so both reject with the identical message; a no-op
/// unless a GSI is targeted with `Select=ALL_ATTRIBUTES`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` when the combination is invalid.
pub fn validate_all_attributes_index_support(
    select: Option<Select>,
    is_gsi: bool,
    projection_is_all: bool,
    index_name: &str,
) -> Result<(), DynamoDbError> {
    if matches!(select, Some(Select::AllAttributes)) && is_gsi && !projection_is_all {
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not \
             supported for global secondary index {index_name} because its projection type is not ALL"
        )));
    }
    Ok(())
}

/// Validate partition key and sort key sizes against limits.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` if a key value exceeds its size limit.
pub fn validate_key_sizes(
    item: &Item,
    key_schema: &[KeySchemaElement],
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    for ks in key_schema {
        if let Some(value) = item.get(&ks.attribute_name) {
            validate_no_empty_key_value(&ks.attribute_name, value)?;
            check_key_size(ks, value, limits)?;
        }
    }
    Ok(())
}

/// Validate only the byte-size limit of primary-key values (no empty-value
/// check).
///
/// Used by the transaction path, which surfaces an oversized key as a per-item
/// `TransactionCanceledException` / `ValidationError` cancellation reason, while
/// an empty key value remains a top-level `ValidationException` — matching real
/// `DynamoDB`.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` if a key value exceeds its size limit.
pub fn validate_key_size_limits(
    item: &Item,
    key_schema: &[KeySchemaElement],
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    for ks in key_schema {
        if let Some(value) = item.get(&ks.attribute_name) {
            check_key_size(ks, value, limits)?;
        }
    }
    Ok(())
}

/// Validate only that primary-key values are non-empty (no size check).
///
/// The transaction path uses this to keep the empty-key rejection as a
/// top-level `ValidationException` (real `DynamoDB` behavior) while the size
/// limit is enforced separately as a per-item cancellation reason.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` if a key value is empty.
pub fn validate_key_not_empty(
    item: &Item,
    key_schema: &[KeySchemaElement],
) -> Result<(), DynamoDbError> {
    for ks in key_schema {
        if let Some(value) = item.get(&ks.attribute_name) {
            validate_no_empty_key_value(&ks.attribute_name, value)?;
        }
    }
    Ok(())
}

/// Check a single primary-key value against its size limit. Hash and range use
/// different wording, matching Amazon `DynamoDB` (the hash variant has no space
/// before the size).
fn check_key_size(
    ks: &KeySchemaElement,
    value: &AttributeValue,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    let size = key_value_byte_size(value);
    let max_size = match ks.key_type {
        KeyType::Hash => limits.max_partition_key_size_bytes,
        KeyType::Range => limits.max_sort_key_size_bytes,
    };
    if size > max_size {
        let msg = match ks.key_type {
            KeyType::Hash => format!(
                "One or more parameter values were invalid: \
                 Size of hashkey has exceeded the maximum size limit of{max_size} bytes"
            ),
            KeyType::Range => format!(
                "One or more parameter values were invalid: \
                 Aggregated size of all range keys has exceeded the size limit of {max_size} bytes"
            ),
        };
        return Err(DynamoDbError::ValidationException(msg));
    }
    Ok(())
}

/// Get the byte size of a key attribute value.
///
/// For `N` keys this uses the digit-string length. Amazon DynamoDB caps a
/// number at 38 significant digits, so the string length is always far below
/// the key-size limit and the proxy is exact for the rejection decision.
fn key_value_byte_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => n.len(),
        AttributeValue::B(b) => b.len(),
        _ => 0,
    }
}

/// Validate that an item does not exceed the maximum allowed size.
///
/// Called by the storage layer after applying update expressions to ensure
/// the resulting item is within the 400 KB limit.
///
/// # Errors
///
/// Returns `DynamoDbError::ValidationException` if the item exceeds the limit.
pub fn validate_item_size(item: &Item, max_bytes: usize) -> Result<(), DynamoDbError> {
    let size = item_size_bytes(item);
    if size > max_bytes {
        return Err(DynamoDbError::ValidationException(
            "Item size has exceeded the maximum allowed size".to_owned(),
        ));
    }
    Ok(())
}

/// Validate all number values in an item are within `DynamoDB` limits.
pub fn validate_item_numbers(item: &Item) -> Result<(), DynamoDbError> {
    for value in item.values() {
        validate_attribute_number(value)?;
    }
    Ok(())
}

fn validate_lsi_requires_range_key(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    let has_lsi = input
        .local_secondary_indexes
        .as_ref()
        .is_some_and(|v| !v.is_empty());
    if !has_lsi {
        return Ok(());
    }
    let has_range = input.key_schema.len() >= 2 && input.key_schema[1].key_type == KeyType::Range;
    if !has_range {
        return Err(DynamoDbError::ValidationException(
            "One or more parameter values were invalid: Table KeySchema does not have a range key, which is required when specifying a LocalSecondaryIndex".to_owned(),
        ));
    }
    Ok(())
}

fn validate_attribute_number(value: &AttributeValue) -> Result<(), DynamoDbError> {
    match value {
        AttributeValue::N(n) => {
            number::validate_and_normalize_number(n)?;
        }
        AttributeValue::NS(set) => {
            for n in set {
                number::validate_and_normalize_number(n)?;
            }
        }
        AttributeValue::L(list) => {
            for v in list {
                validate_attribute_number(v)?;
            }
        }
        AttributeValue::M(map) => {
            for v in map.values() {
                validate_attribute_number(v)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Maximum total nesting levels (M/L wrappers plus the leaf) `DynamoDB` allows.
pub(crate) const MAX_ITEM_NESTING_DEPTH: usize = 32;

/// Validate that no attribute value in `item` nests beyond `MAX_ITEM_NESTING_DEPTH`.
pub fn validate_item_nesting_depth(item: &Item) -> Result<(), DynamoDbError> {
    for value in item.values() {
        check_attribute_value_depth(value, 0)?;
    }
    Ok(())
}

/// Validate nesting depth on attribute values introduced outside of an `Item`
/// (`ExpressionAttributeValues`, `AttributeUpdates`, `Expected`).
pub fn validate_attribute_values_nesting_depth<'a, I>(values: I) -> Result<(), DynamoDbError>
where
    I: IntoIterator<Item = &'a AttributeValue>,
{
    for v in values {
        check_attribute_value_depth(v, 0)?;
    }
    Ok(())
}

fn check_attribute_value_depth(
    value: &AttributeValue,
    current_depth: usize,
) -> Result<(), DynamoDbError> {
    match value {
        AttributeValue::M(map) => {
            let next = current_depth + 1;
            if next >= MAX_ITEM_NESTING_DEPTH {
                return Err(nesting_depth_error());
            }
            for v in map.values() {
                check_attribute_value_depth(v, next)?;
            }
        }
        AttributeValue::L(list) => {
            let next = current_depth + 1;
            if next >= MAX_ITEM_NESTING_DEPTH {
                return Err(nesting_depth_error());
            }
            for v in list {
                check_attribute_value_depth(v, next)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn nesting_depth_error() -> DynamoDbError {
    DynamoDbError::ValidationException(
        "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels beyond supported limit".to_owned(),
    )
}

fn validate_unique_index_names(input: &CreateTableInput) -> Result<(), DynamoDbError> {
    let mut names = std::collections::HashSet::new();
    if let Some(gsis) = &input.global_secondary_indexes {
        for gsi in gsis {
            if !names.insert(&gsi.index_name) {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: Duplicate index name: {}",
                    gsi.index_name
                )));
            }
        }
    }
    if let Some(lsis) = &input.local_secondary_indexes {
        for lsi in lsis {
            if !names.insert(&lsi.index_name) {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: Duplicate index name: {}",
                    lsi.index_name
                )));
            }
        }
    }
    if let Some(vis) = &input.vector_indexes {
        for vi in vis {
            if !names.insert(&vi.index_name) {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values were invalid: Duplicate index name: {}",
                    vi.index_name
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    /// A create arriving by UpdateTable gets the same rules as one arriving by
    /// CreateTable, including the field name in the message path.
    #[test]
    fn update_table_creates_get_the_same_rules_as_create_table() {
        use crate::types::{DeleteVectorIndexAction, VectorIndexUpdate};
        let spec = |projection| VectorIndexSpecification {
            index_name: "vidx".to_owned(),
            dimensions: 4,
            distance_function: DistanceFunction::Cosine,
            vector_attribute: VectorAttribute {
                attribute_name: "emb".to_owned(),
            },
            search_schema: None,
            projection,
        };
        let all = || {
            Some(Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            })
        };

        // None and empty are not changes, so an ordinary UpdateTable is unaffected.
        assert!(validate_vector_index_updates(None, &[]).is_ok());
        assert!(validate_vector_index_updates(Some(&Vec::new()), &[]).is_ok());

        // Missing projection is caught here too, under this request's field name.
        let updates = vec![crate::types::VectorIndexUpdate {
            create: Some(spec(None)),
            delete: None,
        }];
        let err = validate_vector_index_updates(Some(&updates), &[])
            .expect_err("a malformed create must be rejected on this path as well");
        match err {
            DynamoDbError::ValidationException(m) => assert!(
                m.contains("vectorIndexUpdates.1.member.create.projection"),
                "should name this request's field and element: {m}"
            ),
            other => panic!("expected ValidationException, got {other:?}"),
        }

        // Dimensions bound applies here too.
        let updates = vec![crate::types::VectorIndexUpdate {
            create: Some(VectorIndexSpecification {
                dimensions: 4097,
                ..spec(all())
            }),
            delete: None,
        }];
        assert!(validate_vector_index_updates(Some(&updates), &[]).is_err());

        // A well-formed single create passes, and a single delete passes; two
        // actions in one request are refused by the one-online-action rule
        // with the online-index machinery's own error class (pinned by the
        // ground-truth runs of 2026-08-24).
        let updates = vec![VectorIndexUpdate {
            create: Some(spec(all())),
            delete: None,
        }];
        assert!(validate_vector_index_updates(Some(&updates), &[]).is_ok());
        let updates = vec![VectorIndexUpdate {
            create: None,
            delete: Some(DeleteVectorIndexAction {
                index_name: "other".to_owned(),
            }),
        }];
        assert!(validate_vector_index_updates(Some(&updates), &[]).is_ok());
        let updates = vec![
            VectorIndexUpdate {
                create: Some(spec(all())),
                delete: None,
            },
            VectorIndexUpdate {
                create: None,
                delete: Some(DeleteVectorIndexAction {
                    index_name: "other".to_owned(),
                }),
            },
        ];
        match validate_vector_index_updates(Some(&updates), &[])
            .expect_err("two online index actions in one call must be refused")
        {
            DynamoDbError::LimitExceededException(m) => assert_eq!(
                m,
                "Subscriber limit exceeded: Only 1 online index can be created or deleted \
                 simultaneously per table"
            ),
            other => panic!("expected LimitExceededException, got {other:?}"),
        }
    }

    use crate::types::{DistanceFunction, VectorAttribute, VectorIndexSpecification};

    /// The service requires `Projection` on every vector index, and numbers the
    /// offending element from 1. Measured on 2026-08-06 by bypassing botocore's
    /// client-side check so the request reached the service. Asserted exactly,
    /// because a client parsing the path would be misled by 0-based numbering.
    /// Builds a vector index spec for the update-path parity test below. The
    /// asserted messages were measured against the live service on 2026-08-13
    /// (us-east-1), so the test pins service wording, not our own.
    fn vi_spec_with_schema(name: &str, attr: &str, schema: &[&str]) -> VectorIndexSpecification {
        VectorIndexSpecification {
            index_name: name.to_owned(),
            vector_attribute: VectorAttribute {
                attribute_name: attr.to_owned(),
            },
            dimensions: 8,
            distance_function: DistanceFunction::Cosine,
            projection: Some(Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            }),
            search_schema: if schema.is_empty() {
                None
            } else {
                Some(
                    schema
                        .iter()
                        .map(|n| crate::types::SearchSchemaElement {
                            attribute_name: (*n).to_owned(),
                            element_type: crate::types::SearchSchemaElementType::Hash,
                        })
                        .collect(),
                )
            },
        }
    }
    /// The same two attribute-definition rules apply on the update path, against
    /// the definitions carried by THAT request. Measured: omitting the
    /// definition fails even when the attribute is already on the table.
    #[test]
    fn update_path_applies_the_attribute_definition_rules() {
        let create = vi_spec_with_schema("vidx", "emb", &["tenant"]);
        let updates = vec![crate::types::VectorIndexUpdate {
            create: Some(create),
            delete: None,
        }];
        let err =
            validate_vector_index_updates(Some(&updates), &[make_ad("pk", ScalarAttributeType::S)])
                .unwrap_err();
        assert_eq!(
            err.to_string(),
            crate::types::VECTOR_SEARCH_SCHEMA_UNDECLARED
        );
        validate_vector_index_updates(
            Some(&updates),
            &[
                make_ad("pk", ScalarAttributeType::S),
                make_ad("tenant", ScalarAttributeType::S),
            ],
        )
        .unwrap();

        let declared = vi_spec_with_schema("vidx", "emb", &[]);
        let updates = vec![crate::types::VectorIndexUpdate {
            create: Some(declared),
            delete: None,
        }];
        let err = validate_vector_index_updates(
            Some(&updates),
            &[make_ad("emb", ScalarAttributeType::S)],
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            crate::types::vector_attribute_conflicting_definition("emb")
        );
    }

    #[test]
    fn missing_vector_index_projection_matches_the_service_message() {
        fn spec(projection: Option<Projection>) -> VectorIndexSpecification {
            VectorIndexSpecification {
                index_name: "vidx".to_owned(),
                dimensions: 4,
                distance_function: DistanceFunction::Cosine,
                vector_attribute: VectorAttribute {
                    attribute_name: "emb".to_owned(),
                },
                search_schema: None,
                projection,
            }
        }
        let all = || {
            Some(Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            })
        };

        // First element omitted: reported as element 1.
        let input = CreateTableInput {
            table_name: "t".to_owned(),
            vector_indexes: Some(vec![spec(None)]),
            ..Default::default()
        };
        let err = validate_vector_indexes(&input).expect_err("must be rejected");
        assert!(
            format!("{err:?}").contains("vectorIndexes.1.member.projection"),
            "expected element 1 in the path: {err:?}"
        );

        // Second element omitted: reported as element 2, not 1.
        let input = CreateTableInput {
            table_name: "t".to_owned(),
            vector_indexes: Some(vec![spec(all()), spec(None)]),
            ..Default::default()
        };
        let err = validate_vector_indexes(&input).expect_err("must be rejected");
        match err {
            DynamoDbError::ValidationException(m) => assert_eq!(
                m,
                "1 validation error detected: Value null at \
                 'vectorIndexes.2.member.projection' failed to satisfy constraint: \
                 Member must not be null"
            ),
            other => panic!("expected ValidationException, got {other:?}"),
        }

        // Present on every element: accepted. PAY_PER_REQUEST is required because a
        // vector index is only valid on an on-demand table, which this function now
        // enforces; the projection is what this case is about.
        let input = CreateTableInput {
            table_name: "t".to_owned(),
            billing_mode: Some(BillingMode::PayPerRequest),
            vector_indexes: Some(vec![spec(all()), spec(all())]),
            ..Default::default()
        };
        assert!(validate_vector_indexes(&input).is_ok());
    }

    use super::*;
    use crate::types::{GsiInput, Projection, ProjectionType};

    fn make_ks(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        }
    }

    fn make_ad(name: &str, attr_type: ScalarAttributeType) -> AttributeDefinition {
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: attr_type,
        }
    }

    fn base_input(
        key_schema: Vec<KeySchemaElement>,
        attr_defs: Vec<AttributeDefinition>,
    ) -> CreateTableInput {
        CreateTableInput {
            table_name: "TestTable".to_owned(),
            key_schema,
            attribute_definitions: attr_defs,
            billing_mode: Some(BillingMode::PayPerRequest),
            provisioned_throughput: None,
            global_secondary_indexes: None,
            local_secondary_indexes: None,
            vector_indexes: None,
            stream_specification: None,
            sse_specification: None,
            tags: None,
            deletion_protection_enabled: None,
            table_class: None,
            on_demand_throughput: None,
        }
    }

    /// Helper: a minimal well-formed vector index specification for these tests.
    fn vi_spec(name: &str) -> crate::types::VectorIndexSpecification {
        crate::types::VectorIndexSpecification {
            index_name: name.to_owned(),
            vector_attribute: crate::types::VectorAttribute {
                attribute_name: "emb".to_owned(),
            },
            dimensions: 8,
            distance_function: crate::types::DistanceFunction::Cosine,
            search_schema: None,
            projection: Some(Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            }),
        }
    }

    /// The vector attribute must not be declared in `AttributeDefinitions`.
    ///
    /// Measured against the service on 2026-08-11. Before this, naming the table's
    /// own partition key as the vector attribute was ACCEPTED, because `pk`
    /// satisfied both the definition-exists and definition-is-used checks, so the
    /// conflict was invisible to every rule that ran.
    ///
    /// Both shapes are asserted: an ordinary extra definition, and the key case,
    /// because only the latter passes the surrounding checks and so is the one that
    /// actually got through.
    #[test]
    fn a_vector_attribute_must_not_be_declared_in_attribute_definitions() {
        let expected = "One or more parameter values were invalid: Conflicting attribute \
                        definition for 'emb'. An attribute cannot be defined in \
                        AttributeDefinitions when used as a VectorAttribute.";
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![
                make_ad("pk", ScalarAttributeType::S),
                make_ad("emb", ScalarAttributeType::S),
            ],
        );
        input.vector_indexes = Some(vec![vi_spec("vidx")]);
        let err = validate_attribute_definitions(&input)
            .expect_err("a declared vector attribute must be refused");
        assert_eq!(format!("{err}"), expected);

        // The key case: the attribute is legitimately a key AND the vector
        // attribute, which is what previously slipped through.
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![make_ad("pk", ScalarAttributeType::S)],
        );
        let mut spec = vi_spec("vidx");
        spec.vector_attribute.attribute_name = "pk".to_owned();
        input.vector_indexes = Some(vec![spec]);
        let err = validate_attribute_definitions(&input)
            .expect_err("the partition key must not double as the vector attribute");
        assert!(
            format!("{err}").contains("Conflicting attribute definition for 'pk'"),
            "{err}"
        );
    }

    /// A vector index's projection is subject to the same INCLUDE rule as a GSI's.
    ///
    /// `validate_index_projections` iterated GSIs and LSIs only, so a vector index
    /// could declare INCLUDE with no NonKeyAttributes and be accepted where the
    /// service refuses it. The message already existed and was already correct;
    /// only the iteration was missing, so this asserts the message to pin that the
    /// shared rule is what is being applied rather than a restatement of it.
    #[test]
    fn a_vector_index_projection_obeys_the_include_rule() {
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![make_ad("pk", ScalarAttributeType::S)],
        );
        let mut spec = vi_spec("vidx");
        spec.projection = Some(Projection {
            projection_type: ProjectionType::Include,
            non_key_attributes: None,
        });
        input.vector_indexes = Some(vec![spec]);
        let err = validate_index_projections(&input)
            .expect_err("INCLUDE without NonKeyAttributes must be refused");
        assert_eq!(
            format!("{err}"),
            "One or more parameter values were invalid: ProjectionType is INCLUDE, but \
             NonKeyAttributes is not specified"
        );

        // An absent projection is a different fault, reported elsewhere, and must
        // not be reported twice with different wording.
        let mut spec = vi_spec("vidx");
        spec.projection = None;
        input.vector_indexes = Some(vec![spec]);
        validate_index_projections(&input)
            .expect("an absent projection is validate_one_vector_index's fault to report");
    }

    /// A SearchSchema attribute with no definition gets the service's own wording,
    /// which differs from the GSI key-attribute message this previously reused.
    ///
    /// The distinction matters because the two messages carry different amounts of
    /// detail: the GSI one names the attribute and lists every definition, while
    /// the service says only that one element is undefined.
    #[test]
    fn an_undefined_search_schema_attribute_uses_its_own_message() {
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![make_ad("pk", ScalarAttributeType::S)],
        );
        let mut spec = vi_spec("vidx");
        spec.search_schema = Some(vec![crate::types::SearchSchemaElement {
            attribute_name: "cat".to_owned(),
            element_type: crate::types::SearchSchemaElementType::Hash,
        }]);
        input.vector_indexes = Some(vec![spec]);
        let err = validate_attribute_definitions(&input)
            .expect_err("an undefined SearchSchema attribute must be refused");
        assert_eq!(
            format!("{err}"),
            "One or more parameter values were invalid: One element in SearchSchema is not \
             defined in attribute definitions"
        );
    }

    /// Both table-level vector messages, pinned to what the service returns.
    #[test]
    fn the_table_level_vector_messages_match_the_service() {
        // Billing mode. PROVISIONED is the default when BillingMode is absent, so
        // the omitted case is asserted too.
        for billing in [Some(BillingMode::Provisioned), None] {
            let mut input = base_input(
                vec![make_ks("pk", KeyType::Hash)],
                vec![make_ad("pk", ScalarAttributeType::S)],
            );
            input.billing_mode = billing;
            input.vector_indexes = Some(vec![vi_spec("vidx")]);
            let err = validate_vector_indexes(&input)
                .expect_err("a vector index requires PAY_PER_REQUEST");
            assert_eq!(
                format!("{err}"),
                "One or more parameter values were invalid: Vector indexes are only \
                 supported for PAY_PER_REQUEST tables"
            );
        }

        // The per-table cap, asserted as a boundary so an off-by-one cannot hide.
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![make_ad("pk", ScalarAttributeType::S)],
        );
        let at_cap: Vec<_> = (0..MAX_VECTOR_INDEXES_PER_TABLE)
            .map(|i| vi_spec(&format!("idx{i}")))
            .collect();
        input.vector_indexes = Some(at_cap);
        validate_vector_indexes(&input).expect("the cap itself is allowed");

        let over_cap: Vec<_> = (0..=MAX_VECTOR_INDEXES_PER_TABLE)
            .map(|i| vi_spec(&format!("idx{i}")))
            .collect();
        input.vector_indexes = Some(over_cap);
        let err = validate_vector_indexes(&input).expect_err("one over the cap is refused");
        assert_eq!(
            format!("{err}"),
            format!(
                "One or more parameter values were invalid: VectorIndex count exceeds the \
                 per-table limit of {MAX_VECTOR_INDEXES_PER_TABLE}"
            )
        );
    }

    #[test]
    fn standard_table_rejects_multipart_keys() {
        let limits = LimitsConfig::default(); // allow_multipart_table_keys = false
        let input = base_input(
            vec![make_ks("pk1", KeyType::Hash), make_ks("pk2", KeyType::Hash)],
            vec![
                make_ad("pk1", ScalarAttributeType::S),
                make_ad("pk2", ScalarAttributeType::S),
            ],
        );
        assert!(validate_create_table(&input, &limits).is_err());
    }

    #[test]
    fn multipart_table_keys_allowed_when_enabled() {
        let limits = LimitsConfig {
            allow_multipart_table_keys: true,
            ..Default::default()
        };
        let input = base_input(
            vec![
                make_ks("pk1", KeyType::Hash),
                make_ks("pk2", KeyType::Hash),
                make_ks("sk1", KeyType::Range),
            ],
            vec![
                make_ad("pk1", ScalarAttributeType::S),
                make_ad("pk2", ScalarAttributeType::S),
                make_ad("sk1", ScalarAttributeType::S),
            ],
        );
        assert!(validate_create_table(&input, &limits).is_ok());
    }

    #[test]
    fn gsi_multipart_keys_always_allowed() {
        let limits = LimitsConfig::default(); // allow_multipart_table_keys = false
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![
                make_ad("pk", ScalarAttributeType::S),
                make_ad("gsi_pk1", ScalarAttributeType::S),
                make_ad("gsi_pk2", ScalarAttributeType::S),
                make_ad("gsi_sk", ScalarAttributeType::N),
            ],
        );
        input.global_secondary_indexes = Some(vec![GsiInput {
            index_name: "my-gsi".to_owned(),
            key_schema: vec![
                make_ks("gsi_pk1", KeyType::Hash),
                make_ks("gsi_pk2", KeyType::Hash),
                make_ks("gsi_sk", KeyType::Range),
            ],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]);
        assert!(validate_create_table(&input, &limits).is_ok());
    }

    #[test]
    fn gsi_rejects_more_than_4_hash_keys() {
        let limits = LimitsConfig::default();
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![
                make_ad("pk", ScalarAttributeType::S),
                make_ad("a", ScalarAttributeType::S),
                make_ad("b", ScalarAttributeType::S),
                make_ad("c", ScalarAttributeType::S),
                make_ad("d", ScalarAttributeType::S),
                make_ad("e", ScalarAttributeType::S),
            ],
        );
        input.global_secondary_indexes = Some(vec![GsiInput {
            index_name: "my-gsi".to_owned(),
            key_schema: vec![
                make_ks("a", KeyType::Hash),
                make_ks("b", KeyType::Hash),
                make_ks("c", KeyType::Hash),
                make_ks("d", KeyType::Hash),
                make_ks("e", KeyType::Hash),
            ],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]);
        assert!(validate_create_table(&input, &limits).is_err());
    }

    #[test]
    fn gsi_rejects_hash_after_range() {
        let limits = LimitsConfig::default();
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![
                make_ad("pk", ScalarAttributeType::S),
                make_ad("a", ScalarAttributeType::S),
                make_ad("b", ScalarAttributeType::S),
                make_ad("c", ScalarAttributeType::S),
            ],
        );
        input.global_secondary_indexes = Some(vec![GsiInput {
            index_name: "my-gsi".to_owned(),
            key_schema: vec![
                make_ks("a", KeyType::Hash),
                make_ks("b", KeyType::Range),
                make_ks("c", KeyType::Hash), // HASH after RANGE — invalid
            ],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]);
        assert!(validate_create_table(&input, &limits).is_err());
    }

    #[test]
    fn attribute_name_within_limit_passes() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        item.insert("ok_name".to_owned(), AttributeValue::S("v".to_owned()));
        assert!(validate_attribute_name_sizes(&item, &limits).is_ok());
    }

    #[test]
    fn gsi_provisioned_throughput_rejected_on_pay_per_request() {
        let limits = LimitsConfig::default();
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![
                make_ad("pk", ScalarAttributeType::S),
                make_ad("gsi_pk", ScalarAttributeType::S),
            ],
        );
        input.billing_mode = Some(BillingMode::PayPerRequest);
        input.global_secondary_indexes = Some(vec![GsiInput {
            index_name: "my-gsi".to_owned(),
            key_schema: vec![make_ks("gsi_pk", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: Some(crate::types::ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5,
            }),
        }]);
        let err = validate_create_table(&input, &limits).unwrap_err();
        assert!(
            err.to_string()
                .contains("ProvisionedThroughput should not be specified for index: my-gsi"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn gsi_without_provisioned_throughput_accepted_on_pay_per_request() {
        let limits = LimitsConfig::default();
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![
                make_ad("pk", ScalarAttributeType::S),
                make_ad("gsi_pk", ScalarAttributeType::S),
            ],
        );
        input.billing_mode = Some(BillingMode::PayPerRequest);
        input.global_secondary_indexes = Some(vec![GsiInput {
            index_name: "my-gsi".to_owned(),
            key_schema: vec![make_ks("gsi_pk", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]);
        assert!(validate_create_table(&input, &limits).is_ok());
    }

    #[test]
    fn attribute_name_exceeding_limit_rejected() {
        let limits = LimitsConfig {
            max_attribute_name_bytes: 10,
            ..Default::default()
        };
        let mut item = Item::new();
        item.insert("a".repeat(11), AttributeValue::S("v".to_owned()));
        let err = validate_attribute_name_sizes(&item, &limits).unwrap_err();
        assert!(
            err.to_string().contains("Size of attribute name"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_key_sizes_rejects_empty_binary_partition_key() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::B(Vec::new()));
        let err = validate_key_sizes(&item, &[make_ks("pk", KeyType::Hash)], &limits).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty binary value"), "got: {msg}");
        assert!(msg.contains("Key: pk"), "got: {msg}");
    }

    #[test]
    fn validate_key_sizes_still_rejects_empty_string_partition_key() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S(String::new()));
        let err = validate_key_sizes(&item, &[make_ks("pk", KeyType::Hash)], &limits).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty string value"), "got: {msg}");
        assert!(msg.contains("Key: pk"), "got: {msg}");
    }

    #[test]
    fn validate_key_sizes_accepts_non_empty_binary_partition_key() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::B(vec![0x00]));
        assert!(validate_key_sizes(&item, &[make_ks("pk", KeyType::Hash)], &limits).is_ok());
    }

    #[test]
    fn validate_key_sizes_rejects_oversized_hash_key() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        let big = "a".repeat(limits.max_partition_key_size_bytes + 1);
        item.insert("pk".to_owned(), AttributeValue::S(big));
        let err = validate_key_sizes(&item, &[make_ks("pk", KeyType::Hash)], &limits).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Size of hashkey has exceeded the maximum size limit"),
            "got: {msg}"
        );
    }

    #[test]
    fn validate_key_sizes_rejects_oversized_range_key() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        let big = "b".repeat(limits.max_sort_key_size_bytes + 1);
        item.insert("sk".to_owned(), AttributeValue::S(big));
        let err = validate_key_sizes(&item, &[make_ks("sk", KeyType::Range)], &limits).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Aggregated size of all range keys has exceeded the size limit"),
            "got: {msg}"
        );
    }

    #[test]
    fn validate_key_sizes_accepts_key_at_limit() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        item.insert(
            "pk".to_owned(),
            AttributeValue::S("a".repeat(limits.max_partition_key_size_bytes)),
        );
        assert!(validate_key_sizes(&item, &[make_ks("pk", KeyType::Hash)], &limits).is_ok());
    }

    #[test]
    fn validate_key_size_limits_rejects_oversized_but_ignores_empty() {
        // Size-only helper: oversized hash key rejected with the exact message,
        // but an empty key value is NOT rejected here (that stays a separate,
        // top-level check for the transaction path).
        let limits = LimitsConfig::default();
        let mut big = Item::new();
        big.insert(
            "pk".to_owned(),
            AttributeValue::S("a".repeat(limits.max_partition_key_size_bytes + 1)),
        );
        let err =
            validate_key_size_limits(&big, &[make_ks("pk", KeyType::Hash)], &limits).unwrap_err();
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: \
             Size of hashkey has exceeded the maximum size limit of2048 bytes"
        );

        let mut empty = Item::new();
        empty.insert("pk".to_owned(), AttributeValue::S(String::new()));
        assert!(
            validate_key_size_limits(&empty, &[make_ks("pk", KeyType::Hash)], &limits).is_ok(),
            "size-only check must ignore empty key values"
        );
    }

    #[test]
    fn validate_key_size_limits_range_message_matches_amazon_dynamodb() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        item.insert(
            "sk".to_owned(),
            AttributeValue::S("b".repeat(limits.max_sort_key_size_bytes + 1)),
        );
        let err =
            validate_key_size_limits(&item, &[make_ks("sk", KeyType::Range)], &limits).unwrap_err();
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: \
             Aggregated size of all range keys has exceeded the size limit of 1024 bytes"
        );
    }

    #[test]
    fn validate_key_not_empty_rejects_empty_but_ignores_oversized() {
        // Empty-only helper: rejects an empty key value, but a merely-oversized
        // (non-empty) key passes (size is enforced separately).
        let limits = LimitsConfig::default();
        let mut empty = Item::new();
        empty.insert("pk".to_owned(), AttributeValue::S(String::new()));
        assert!(validate_key_not_empty(&empty, &[make_ks("pk", KeyType::Hash)]).is_err());

        let mut big = Item::new();
        big.insert(
            "pk".to_owned(),
            AttributeValue::S("a".repeat(limits.max_partition_key_size_bytes + 1)),
        );
        assert!(
            validate_key_not_empty(&big, &[make_ks("pk", KeyType::Hash)]).is_ok(),
            "empty-only check must ignore oversized (non-empty) key values"
        );
    }

    #[test]
    fn validate_key_sizes_hash_message_matches_amazon_dynamodb() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        item.insert(
            "pk".to_owned(),
            AttributeValue::S("a".repeat(limits.max_partition_key_size_bytes + 1)),
        );
        let err = validate_key_sizes(&item, &[make_ks("pk", KeyType::Hash)], &limits).unwrap_err();
        // Exact wording, including Amazon DynamoDB's missing space before the size.
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: \
             Size of hashkey has exceeded the maximum size limit of2048 bytes"
        );
    }

    #[test]
    fn validate_key_sizes_range_message_matches_amazon_dynamodb() {
        let limits = LimitsConfig::default();
        let mut item = Item::new();
        item.insert(
            "sk".to_owned(),
            AttributeValue::S("b".repeat(limits.max_sort_key_size_bytes + 1)),
        );
        let err = validate_key_sizes(&item, &[make_ks("sk", KeyType::Range)], &limits).unwrap_err();
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: \
             Aggregated size of all range keys has exceeded the size limit of 1024 bytes"
        );
    }

    #[test]
    fn validate_key_only_rejects_empty_binary_key() {
        let mut key = Item::new();
        key.insert("pk".to_owned(), AttributeValue::B(Vec::new()));
        let err = validate_key_only(
            &key,
            &[make_ks("pk", KeyType::Hash)],
            &[make_ad("pk", ScalarAttributeType::B)],
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty binary value"), "got: {msg}");
        assert!(msg.contains("Key: pk"), "got: {msg}");
    }

    #[test]
    fn validate_key_only_rejects_empty_string_key() {
        let mut key = Item::new();
        key.insert("pk".to_owned(), AttributeValue::S(String::new()));
        let err = validate_key_only(
            &key,
            &[make_ks("pk", KeyType::Hash)],
            &[make_ad("pk", ScalarAttributeType::S)],
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("empty string value"), "got: {msg}");
        assert!(msg.contains("Key: pk"), "got: {msg}");
    }

    #[test]
    fn validate_key_only_accepts_non_empty_binary_key() {
        let mut key = Item::new();
        key.insert("pk".to_owned(), AttributeValue::B(vec![0xff]));
        assert!(
            validate_key_only(
                &key,
                &[make_ks("pk", KeyType::Hash)],
                &[make_ad("pk", ScalarAttributeType::B)],
            )
            .is_ok()
        );
    }

    fn update_input_no_directives() -> UpdateItemInput {
        UpdateItemInput {
            table_name: "TestTable".to_owned(),
            key: {
                let mut k = Item::new();
                k.insert("pk".to_owned(), AttributeValue::S("p".to_owned()));
                k
            },
            update_expression: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values: ReturnValues::None,
            expected: None,
            conditional_operator: None,
            attribute_updates: None,
            return_values_on_condition_check_failure: Default::default(),
            return_consumed_capacity: Default::default(),
            return_item_collection_metrics: Default::default(),
        }
    }

    #[test]
    fn update_item_no_update_expression_or_attribute_updates_accepted() {
        // DynamoDB treats UpdateItem with only TableName + Key as a no-op
        // upsert. Validation must not reject it.
        let limits = LimitsConfig::default();
        let key_schema = vec![make_ks("pk", KeyType::Hash)];
        let attr_defs = vec![make_ad("pk", ScalarAttributeType::S)];
        let input = update_input_no_directives();
        assert!(validate_update_item(&input, &limits, &key_schema, &attr_defs).is_ok());
    }

    #[test]
    fn update_item_empty_attribute_updates_map_accepted() {
        // An empty AttributeUpdates map is equivalent to no directives.
        let limits = LimitsConfig::default();
        let key_schema = vec![make_ks("pk", KeyType::Hash)];
        let attr_defs = vec![make_ad("pk", ScalarAttributeType::S)];
        let mut input = update_input_no_directives();
        input.attribute_updates = Some(std::collections::HashMap::new());
        assert!(validate_update_item(&input, &limits, &key_schema, &attr_defs).is_ok());
    }

    #[test]
    fn update_item_empty_string_update_expression_passes_validation() {
        // Validation must let Some("") through so the engine's tokenize_for
        // produces the DynamoDB-compatible "The expression can not be empty;"
        // message. PR #24 (ef8b94f) protects this routing; we keep it.
        let limits = LimitsConfig::default();
        let key_schema = vec![make_ks("pk", KeyType::Hash)];
        let attr_defs = vec![make_ad("pk", ScalarAttributeType::S)];
        let mut input = update_input_no_directives();
        input.update_expression = Some(String::new());
        assert!(validate_update_item(&input, &limits, &key_schema, &attr_defs).is_ok());
    }

    fn eav_update_input(key: &str, value: AttributeValue) -> UpdateItemInput {
        let mut input = update_input_no_directives();
        input.update_expression = Some(format!("SET bad = {key}"));
        let mut m = std::collections::HashMap::new();
        m.insert(key.to_owned(), value);
        input.expression_attribute_values = Some(m);
        input
    }

    #[test]
    fn update_item_rejects_malformed_number_in_eav() {
        // The AttributeValue deserializer stores malformed numbers raw and
        // defers rejection here. Matches real DynamoDB's wrapped message.
        let limits = LimitsConfig::default();
        let key_schema = vec![make_ks("pk", KeyType::Hash)];
        let attr_defs = vec![make_ad("pk", ScalarAttributeType::S)];
        let input = eav_update_input(":v", AttributeValue::N("12e".to_owned()));
        let err = validate_update_item(&input, &limits, &key_schema, &attr_defs).unwrap_err();
        assert_eq!(
            err.to_string(),
            "ExpressionAttributeValues contains invalid value: \
             The parameter cannot be converted to a numeric value: 12e for key :v"
        );
    }

    #[test]
    fn update_item_rejects_number_overflow_in_eav() {
        let limits = LimitsConfig::default();
        let key_schema = vec![make_ks("pk", KeyType::Hash)];
        let attr_defs = vec![make_ad("pk", ScalarAttributeType::S)];
        let big = format!("1{}", "0".repeat(200));
        let input = eav_update_input(":v", AttributeValue::N(big));
        let err = validate_update_item(&input, &limits, &key_schema, &attr_defs).unwrap_err();
        assert_eq!(
            err.to_string(),
            "ExpressionAttributeValues contains invalid value: \
             Number overflow. Attempting to store a number with magnitude larger than \
             supported range for key :v"
        );
    }

    #[test]
    fn update_item_rejects_invalid_number_set_member_in_eav() {
        let limits = LimitsConfig::default();
        let key_schema = vec![make_ks("pk", KeyType::Hash)];
        let attr_defs = vec![make_ad("pk", ScalarAttributeType::S)];
        let ns: std::collections::BTreeSet<String> =
            ["1".to_owned(), "abc".to_owned()].into_iter().collect();
        let input = eav_update_input(":v", AttributeValue::NS(ns));
        let err = validate_update_item(&input, &limits, &key_schema, &attr_defs).unwrap_err();
        assert_eq!(
            err.to_string(),
            "ExpressionAttributeValues contains invalid value: \
             The parameter cannot be converted to a numeric value: abc for key :v"
        );
    }

    #[test]
    fn update_item_accepts_valid_number_in_eav() {
        let limits = LimitsConfig::default();
        let key_schema = vec![make_ks("pk", KeyType::Hash)];
        let attr_defs = vec![make_ad("pk", ScalarAttributeType::S)];
        let input = eav_update_input(":v", AttributeValue::N("42".to_owned()));
        assert!(validate_update_item(&input, &limits, &key_schema, &attr_defs).is_ok());
    }

    #[test]
    fn update_item_rejects_bad_number_in_attribute_updates() {
        // Legacy AttributeUpdates path: bare numeric-value message, no
        // ExpressionAttributeValues wrapper (matches real DynamoDB).
        let limits = LimitsConfig::default();
        let key_schema = vec![make_ks("pk", KeyType::Hash)];
        let attr_defs = vec![make_ad("pk", ScalarAttributeType::S)];
        let mut input = update_input_no_directives();
        let mut updates = std::collections::HashMap::new();
        updates.insert(
            "bad".to_owned(),
            crate::types::AttributeValueUpdate {
                value: Some(AttributeValue::N("not_a_num".to_owned())),
                action: "PUT".to_owned(),
            },
        );
        input.attribute_updates = Some(updates);
        let err = validate_update_item(&input, &limits, &key_schema, &attr_defs).unwrap_err();
        assert_eq!(
            err.to_string(),
            "The parameter cannot be converted to a numeric value: not_a_num"
        );
    }

    fn nested_map(depth: usize) -> AttributeValue {
        let mut leaf = AttributeValue::S("leaf".to_owned());
        for _ in 0..depth {
            let mut m = std::collections::BTreeMap::new();
            m.insert("a".to_owned(), leaf);
            leaf = AttributeValue::M(m);
        }
        leaf
    }

    fn nested_list(depth: usize) -> AttributeValue {
        let mut leaf = AttributeValue::S("leaf".to_owned());
        for _ in 0..depth {
            leaf = AttributeValue::L(vec![leaf]);
        }
        leaf
    }

    #[test]
    fn nesting_depth_at_limit_accepted() {
        // 31 wrappers + leaf = 32 total levels, DynamoDB's hard cap.
        let mut item = Item::new();
        item.insert("deep".to_owned(), nested_map(MAX_ITEM_NESTING_DEPTH - 1));
        validate_item_nesting_depth(&item).expect("32 total levels must be accepted");
    }

    #[test]
    fn nesting_depth_one_over_limit_rejected_for_map() {
        let mut item = Item::new();
        item.insert("deep".to_owned(), nested_map(MAX_ITEM_NESTING_DEPTH));
        let err = validate_item_nesting_depth(&item).unwrap_err();
        assert!(
            err.to_string()
                .contains("Nesting Levels have exceeded supported limits"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nesting_depth_one_over_limit_rejected_for_list() {
        let mut item = Item::new();
        item.insert("deep".to_owned(), nested_list(MAX_ITEM_NESTING_DEPTH));
        let err = validate_item_nesting_depth(&item).unwrap_err();
        assert!(
            err.to_string()
                .contains("Nesting Levels have exceeded supported limits"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nesting_depth_mixed_map_and_list_counted_together() {
        let mut leaf = AttributeValue::S("leaf".to_owned());
        for i in 0..MAX_ITEM_NESTING_DEPTH {
            leaf = if i % 2 == 0 {
                AttributeValue::L(vec![leaf])
            } else {
                let mut m = std::collections::BTreeMap::new();
                m.insert("a".to_owned(), leaf);
                AttributeValue::M(m)
            };
        }
        let mut item = Item::new();
        item.insert("deep".to_owned(), leaf);
        let err = validate_item_nesting_depth(&item).unwrap_err();
        assert!(
            err.to_string()
                .contains("Nesting Levels have exceeded supported limits"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nesting_depth_attribute_values_iterator_at_limit_accepted() {
        let v = nested_map(MAX_ITEM_NESTING_DEPTH - 1);
        validate_attribute_values_nesting_depth(std::iter::once(&v))
            .expect("32 total levels via iterator must be accepted");
    }

    #[test]
    fn nesting_depth_attribute_values_iterator_one_over_rejected() {
        let v = nested_map(MAX_ITEM_NESTING_DEPTH);
        let err = validate_attribute_values_nesting_depth(std::iter::once(&v)).unwrap_err();
        assert!(
            err.to_string()
                .contains("Nesting Levels have exceeded supported limits"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nesting_depth_visits_all_top_level_attributes() {
        // Only one of three top-level attributes is over the limit. The
        // recursion must inspect every attribute and reject.
        let mut item = Item::new();
        item.insert("shallow_a".to_owned(), AttributeValue::S("a".to_owned()));
        item.insert("deep".to_owned(), nested_map(MAX_ITEM_NESTING_DEPTH));
        item.insert("shallow_b".to_owned(), AttributeValue::N("42".to_owned()));
        let err = validate_item_nesting_depth(&item).unwrap_err();
        assert!(
            err.to_string()
                .contains("Nesting Levels have exceeded supported limits"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nesting_depth_visits_all_map_children() {
        // A wide Map: many children, only one is over the limit.
        let mut wide = std::collections::BTreeMap::new();
        wide.insert("a".to_owned(), AttributeValue::S("x".to_owned()));
        wide.insert("b".to_owned(), nested_map(MAX_ITEM_NESTING_DEPTH - 1));
        wide.insert("c".to_owned(), nested_map(MAX_ITEM_NESTING_DEPTH));
        wide.insert("d".to_owned(), AttributeValue::N("1".to_owned()));
        let mut item = Item::new();
        item.insert("wide".to_owned(), AttributeValue::M(wide));
        let err = validate_item_nesting_depth(&item).unwrap_err();
        assert!(
            err.to_string()
                .contains("Nesting Levels have exceeded supported limits"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nesting_depth_visits_all_list_elements() {
        // A wide List: many elements, only one element is over the limit.
        let wide = vec![
            AttributeValue::S("x".to_owned()),
            nested_map(MAX_ITEM_NESTING_DEPTH - 1),
            AttributeValue::Bool(true),
            nested_map(MAX_ITEM_NESTING_DEPTH),
            AttributeValue::N("3".to_owned()),
        ];
        let mut item = Item::new();
        item.insert("wide".to_owned(), AttributeValue::L(wide));
        let err = validate_item_nesting_depth(&item).unwrap_err();
        assert!(
            err.to_string()
                .contains("Nesting Levels have exceeded supported limits"),
            "unexpected error: {err}"
        );
    }

    fn idx<'a>(name: &'a str, attr: &'a str) -> (String, Vec<KeySchemaElement>) {
        (name.to_owned(), vec![make_ks(attr, KeyType::Hash)])
    }

    #[test]
    fn index_key_type_mismatch_names_alphabetically_first_index() {
        // lsi1sk keys both gsi1 and lsi1; declared S but written as N.
        let owned = [idx("lsi1", "lsi1sk"), idx("gsi1", "lsi1sk")];
        let refs: Vec<IndexKeyRef<'_>> = owned
            .iter()
            .map(|(n, ks)| IndexKeyRef {
                index_name: n,
                key_schema: ks,
            })
            .collect();
        let attr_defs = vec![make_ad("lsi1sk", ScalarAttributeType::S)];
        let mut item = Item::new();
        item.insert("lsi1sk".to_owned(), AttributeValue::N("5".to_owned()));

        let err = validate_index_key_types(&item, &refs, &attr_defs).unwrap_err();
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: Type mismatch for Index Key lsi1sk Expected: S Actual: N IndexName: gsi1"
        );
    }

    #[test]
    fn select_projection_incompatible_messages() {
        let cases = [
            (Select::AllAttributes, "ALL_ATTRIBUTES"),
            (Select::AllProjectedAttributes, "ALL_PROJECTED_ATTRIBUTES"),
            (Select::Count, "only the Count"),
        ];
        for (select, what) in cases {
            let body =
                format!("Cannot specify the ProjectionExpression when choosing to get {what}");
            // Query prepends the "1 validation error detected: " prefix.
            let q =
                validate_select_projection(Some(select), true, false, true, IS_QUERY).unwrap_err();
            assert_eq!(
                q.to_string(),
                format!("1 validation error detected: {body}")
            );
            // Scan does NOT prepend the prefix (matches real DynamoDB).
            let s =
                validate_select_projection(Some(select), true, false, true, IS_SCAN).unwrap_err();
            assert_eq!(s.to_string(), body);
        }
    }

    #[test]
    fn select_specific_attributes_requires_projection() {
        // No projection and no AttributesToGet -> rejected.
        assert!(
            validate_select_projection(
                Some(Select::SpecificAttributes),
                false,
                false,
                false,
                IS_QUERY
            )
            .is_err()
        );
        // A projection satisfies it.
        assert!(
            validate_select_projection(
                Some(Select::SpecificAttributes),
                true,
                false,
                false,
                IS_QUERY
            )
            .is_ok()
        );
        // Legacy AttributesToGet satisfies it.
        assert!(
            validate_select_projection(
                Some(Select::SpecificAttributes),
                false,
                true,
                false,
                IS_QUERY
            )
            .is_ok()
        );
    }

    #[test]
    fn select_all_projected_requires_index() {
        let err = validate_select_projection(
            Some(Select::AllProjectedAttributes),
            false,
            false,
            false,
            IS_QUERY,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName"
        );
        assert!(
            validate_select_projection(
                Some(Select::AllProjectedAttributes),
                false,
                false,
                true,
                IS_QUERY
            )
            .is_ok()
        );
    }

    #[test]
    fn index_key_non_scalar_reports_actual_l() {
        let owned = [idx("gsi1", "lsi1sk")];
        let refs: Vec<IndexKeyRef<'_>> = owned
            .iter()
            .map(|(n, ks)| IndexKeyRef {
                index_name: n,
                key_schema: ks,
            })
            .collect();
        let attr_defs = vec![make_ad("lsi1sk", ScalarAttributeType::S)];
        let mut item = Item::new();
        item.insert("lsi1sk".to_owned(), AttributeValue::L(vec![]));
        let err = validate_index_key_types(&item, &refs, &attr_defs).unwrap_err();
        assert!(err.to_string().contains("Actual: L"), "got: {err}");
    }

    #[test]
    fn index_key_empty_messages_by_context() {
        let owned = [idx("gsi1", "lsi1sk")];
        let refs: Vec<IndexKeyRef<'_>> = owned
            .iter()
            .map(|(n, ks)| IndexKeyRef {
                index_name: n,
                key_schema: ks,
            })
            .collect();
        let mut item = Item::new();
        item.insert("lsi1sk".to_owned(), AttributeValue::S(String::new()));

        let put_err = validate_index_key_not_empty(&item, &refs, SecondaryIndexEmptyContext::Item)
            .unwrap_err();
        assert_eq!(
            put_err.to_string(),
            "One or more parameter values are not valid. A value specified for a secondary index key is not supported. \
             The AttributeValue for a key attribute cannot contain an empty string value. IndexName: gsi1, IndexKey: lsi1sk"
        );

        let upd_err = validate_index_key_not_empty(
            &item,
            &refs,
            SecondaryIndexEmptyContext::UpdateExpression,
        )
        .unwrap_err();
        assert_eq!(
            upd_err.to_string(),
            "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. \
             The AttributeValue for a key attribute cannot contain an empty string value."
        );
    }

    #[test]
    fn valid_index_key_passes() {
        let owned = [idx("gsi1", "lsi1sk")];
        let refs: Vec<IndexKeyRef<'_>> = owned
            .iter()
            .map(|(n, ks)| IndexKeyRef {
                index_name: n,
                key_schema: ks,
            })
            .collect();
        let attr_defs = vec![make_ad("lsi1sk", ScalarAttributeType::S)];
        let mut item = Item::new();
        item.insert("lsi1sk".to_owned(), AttributeValue::S("ok".to_owned()));
        assert!(validate_index_keys(&item, &refs, &attr_defs).is_ok());
    }

    #[test]
    fn select_projection_rule_precedes_index_rule() {
        // ALL_PROJECTED_ATTRIBUTES + ProjectionExpression + no IndexName: the
        // ProjectionExpression rule is reported, not the IndexName one.
        let err = validate_select_projection(
            Some(Select::AllProjectedAttributes),
            true,
            false,
            false,
            IS_QUERY,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("ALL_PROJECTED_ATTRIBUTES")
                && err
                    .to_string()
                    .contains("Cannot specify the ProjectionExpression"),
            "got: {err}"
        );
    }

    #[test]
    fn gsi_include_projection_requires_non_key_attributes() {
        use crate::types::{GsiInput, Projection, ProjectionType};
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![
                make_ad("pk", ScalarAttributeType::S),
                make_ad("gsipk", ScalarAttributeType::S),
            ],
        );
        let gsi = |non_key: Option<Vec<String>>| GsiInput {
            index_name: "gsi_inc".to_owned(),
            key_schema: vec![make_ks("gsipk", KeyType::Hash)],
            projection: Projection {
                projection_type: ProjectionType::Include,
                non_key_attributes: non_key,
            },
            provisioned_throughput: None,
        };

        input.global_secondary_indexes = Some(vec![gsi(None)]);
        let err = validate_create_table(&input, &LimitsConfig::default()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: ProjectionType is INCLUDE, but NonKeyAttributes is not specified"
        );

        // Empty NonKeyAttributes is also rejected.
        input.global_secondary_indexes = Some(vec![gsi(Some(vec![]))]);
        assert!(validate_create_table(&input, &LimitsConfig::default()).is_err());

        // A non-empty NonKeyAttributes list is accepted.
        input.global_secondary_indexes = Some(vec![gsi(Some(vec!["extra".to_owned()]))]);
        assert!(validate_create_table(&input, &LimitsConfig::default()).is_ok());
    }

    #[test]
    fn keys_only_and_all_projections_reject_non_key_attributes() {
        use crate::types::{GsiInput, LsiInput, Projection, ProjectionType};

        // GSI cases: table keyed on `pk`, GSI keyed on `gsipk`.
        let gsi_input = |ptype: ProjectionType, non_key: Option<Vec<String>>| {
            let mut input = base_input(
                vec![make_ks("pk", KeyType::Hash)],
                vec![
                    make_ad("pk", ScalarAttributeType::S),
                    make_ad("gsipk", ScalarAttributeType::S),
                ],
            );
            input.global_secondary_indexes = Some(vec![GsiInput {
                index_name: "gsi1".to_owned(),
                key_schema: vec![make_ks("gsipk", KeyType::Hash)],
                projection: Projection {
                    projection_type: ptype,
                    non_key_attributes: non_key,
                },
                provisioned_throughput: None,
            }]);
            input
        };

        // LSI case: table keyed on `pk`+`sk`, LSI alternate sort key `lsisk`.
        let lsi_input = |ptype: ProjectionType, non_key: Option<Vec<String>>| {
            let mut input = base_input(
                vec![make_ks("pk", KeyType::Hash), make_ks("sk", KeyType::Range)],
                vec![
                    make_ad("pk", ScalarAttributeType::S),
                    make_ad("sk", ScalarAttributeType::S),
                    make_ad("lsisk", ScalarAttributeType::S),
                ],
            );
            input.local_secondary_indexes = Some(vec![LsiInput {
                index_name: "lsi1".to_owned(),
                key_schema: vec![
                    make_ks("pk", KeyType::Hash),
                    make_ks("lsisk", KeyType::Range),
                ],
                projection: Projection {
                    projection_type: ptype,
                    non_key_attributes: non_key,
                },
            }]);
            input
        };

        // GSI KEYS_ONLY + NonKeyAttributes -> rejected.
        let err = validate_create_table(
            &gsi_input(ProjectionType::KeysOnly, Some(vec!["x".to_owned()])),
            &LimitsConfig::default(),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: ProjectionType is KEYS_ONLY, but NonKeyAttributes is specified"
        );

        // GSI ALL + NonKeyAttributes -> rejected.
        let err = validate_create_table(
            &gsi_input(ProjectionType::All, Some(vec!["x".to_owned()])),
            &LimitsConfig::default(),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: ProjectionType is ALL, but NonKeyAttributes is specified"
        );

        // LSI KEYS_ONLY + NonKeyAttributes -> rejected.
        let err = validate_create_table(
            &lsi_input(ProjectionType::KeysOnly, Some(vec!["x".to_owned()])),
            &LimitsConfig::default(),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: ProjectionType is KEYS_ONLY, but NonKeyAttributes is specified"
        );

        // KEYS_ONLY / ALL without NonKeyAttributes are accepted.
        assert!(
            validate_create_table(
                &gsi_input(ProjectionType::KeysOnly, None),
                &LimitsConfig::default()
            )
            .is_ok()
        );
        assert!(
            validate_create_table(
                &gsi_input(ProjectionType::All, Some(vec![])),
                &LimitsConfig::default()
            )
            .is_ok()
        );
    }

    #[test]
    fn stream_disabled_with_view_type_is_rejected() {
        use crate::types::{StreamSpecification, StreamViewType};
        let mut input = base_input(
            vec![make_ks("pk", KeyType::Hash)],
            vec![make_ad("pk", ScalarAttributeType::S)],
        );
        input.stream_specification = Some(StreamSpecification {
            stream_enabled: false,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        });
        let err = validate_create_table(&input, &LimitsConfig::default()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "One or more parameter values were invalid: Table is being created with a stream disabled, UpdateViewType should not be specified"
        );

        // Disabled with no view type is fine.
        input.stream_specification = Some(StreamSpecification {
            stream_enabled: false,
            stream_view_type: None,
        });
        assert!(validate_create_table(&input, &LimitsConfig::default()).is_ok());

        // Enabled with a view type is fine.
        input.stream_specification = Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewImage),
        });
        assert!(validate_create_table(&input, &LimitsConfig::default()).is_ok());
    }

    #[test]
    fn test_expression_param_mixing_lists_present_params_in_order() {
        let err = validate_no_expression_param_mixing(
            &[
                ("AttributesToGet", true),
                ("ScanFilter", true),
                ("ConditionalOperator", true),
            ],
            &[("ProjectionExpression", true), ("FilterExpression", true)],
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {AttributesToGet, ScanFilter, ConditionalOperator} \
             Expression parameters: {ProjectionExpression, FilterExpression}"
        );
    }

    #[test]
    fn test_expression_param_mixing_skips_absent_params() {
        let err = validate_no_expression_param_mixing(
            &[("ScanFilter", false), ("ConditionalOperator", true)],
            &[("ProjectionExpression", false), ("FilterExpression", true)],
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {ConditionalOperator} \
             Expression parameters: {FilterExpression}"
        );
    }

    #[test]
    fn test_expression_param_mixing_allows_one_side_only() {
        assert!(
            validate_no_expression_param_mixing(
                &[("ScanFilter", true), ("ConditionalOperator", true)],
                &[("FilterExpression", false)],
            )
            .is_ok()
        );
        assert!(
            validate_no_expression_param_mixing(
                &[("ScanFilter", false)],
                &[("FilterExpression", true), ("ProjectionExpression", true)],
            )
            .is_ok()
        );
        assert!(validate_no_expression_param_mixing(&[], &[]).is_ok());
    }

    #[test]
    fn test_conditional_operator_requires_conditions() {
        let err = validate_conditional_operator_usage(true, 0).unwrap_err();
        assert_eq!(
            err.to_string(),
            "ConditionalOperator cannot be used without Filter or Expected"
        );

        let err = validate_conditional_operator_usage(true, 1).unwrap_err();
        assert_eq!(
            err.to_string(),
            "ConditionalOperator can only be used when Filter or Expected has two or more elements"
        );

        assert!(validate_conditional_operator_usage(true, 2).is_ok());
        assert!(validate_conditional_operator_usage(true, 5).is_ok());
    }

    #[test]
    fn test_conditional_operator_absent_is_ok() {
        assert!(validate_conditional_operator_usage(false, 0).is_ok());
        assert!(validate_conditional_operator_usage(false, 1).is_ok());
    }
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
pub mod number;

use crate::error::{DynamoDbError, ErrorMessageKey, error_message};
use crate::limits::LimitsConfig;
use crate::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteItemInput,
    GetItemInput, Item, KeySchemaElement, KeyType, PutItemInput, ReturnValues, ScalarAttributeType,
    UpdateItemInput, item_size_bytes,
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
    Ok(())
}

/// Format KeySchema elements in DynamoDB's Java-toString style for error messages.
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
/// LSIs do not support multi-part keys (same as real DynamoDB).
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

    // Every key attribute must have a definition
    let def_names: Vec<&str> = input
        .attribute_definitions
        .iter()
        .map(|ad| ad.attribute_name.as_str())
        .collect();

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
/// Real DynamoDB returns: "One or more parameter values were invalid:
/// ProvisionedThroughput should not be specified for index: <name> when
/// BillingMode is PAY_PER_REQUEST"
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
    if let Some(gsis) = &input.global_secondary_indexes {
        if gsis.len() > limits.max_gsis_per_table {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: GlobalSecondaryIndexes count exceeds limit of {}",
                limits.max_gsis_per_table
            )));
        }
    }
    Ok(())
}

fn validate_lsi_count(
    input: &CreateTableInput,
    limits: &LimitsConfig,
) -> Result<(), DynamoDbError> {
    if let Some(lsis) = &input.local_secondary_indexes {
        if lsis.len() > limits.max_lsis_per_table {
            return Err(DynamoDbError::ValidationException(format!(
                "One or more parameter values were invalid: LocalSecondaryIndexes count exceeds limit of {}",
                limits.max_lsis_per_table
            )));
        }
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

    if let Some(updates) = &input.attribute_updates {
        validate_attribute_values_nesting_depth(updates.values().filter_map(|u| u.value.as_ref()))?;
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
/// Real DynamoDB uses "The provided key element does not match the schema"
/// for batch and transaction operations, not the single-item "Type mismatch"
/// message.
fn remap_key_type_mismatch(err: DynamoDbError) -> DynamoDbError {
    match &err {
        DynamoDbError::ValidationException(msg)
            if msg.contains("Type mismatch for key attribute") =>
        {
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
        return Err(DynamoDbError::ValidationException(format!(
            "One or more parameter values were invalid: Type mismatch for key attribute {attr_name}: expected: {type_char}"
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
            let size = key_value_byte_size(value);
            let (max_size, key_label) = match ks.key_type {
                KeyType::Hash => (limits.max_partition_key_size_bytes, "partition key"),
                KeyType::Range => (limits.max_sort_key_size_bytes, "sort key"),
            };
            if size > max_size {
                return Err(DynamoDbError::ValidationException(format!(
                    "One or more parameter values are not valid. \
                     The {key_label} size must be between 1 and {max_size} bytes"
                )));
            }
        }
    }
    Ok(())
}

/// Get the byte size of a key attribute value.
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

/// Validate all number values in an item are within DynamoDB limits.
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

/// Maximum total nesting levels (M/L wrappers plus the leaf) DynamoDB allows.
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
    Ok(())
}

#[cfg(test)]
mod tests {
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
            stream_specification: None,
            sse_specification: None,
            tags: None,
            deletion_protection_enabled: None,
            table_class: None,
        }
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
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row admission: deciding whether one input row may be written, in the
//! order `handle_put_item` applies its checks, and the item-level fault
//! record shape (message and newImage) for a refused row.

use super::*;

/// `newImage` for an item-level fault record. Measured present on the
/// missing-key class and on the secondary-index key classes (wrong type,
/// empty string), on the nesting-depth class and
/// on a base key holding an empty string, absent on
/// base-key type-mismatch and oversized records; the image is
/// attached only for those kinds and when the submitted item is within the
/// item size limit, so one record is bounded by the item limit plus the
/// fixed fields. A base-key type-mismatch fault carries no image even when
/// a key attribute is also missing, because the schema walk reports the
/// mismatch it reaches first, not the absence.
pub(super) fn item_fault_new_image(
    kind: PutPayloadFault,
    item: &Item,
    max_item_size_bytes: usize,
) -> Option<&Item> {
    if item_size_bytes(item) > max_item_size_bytes {
        return None;
    }
    matches!(
        kind,
        PutPayloadFault::MissingKey
            | PutPayloadFault::IndexKey
            | PutPayloadFault::NestingDepth
            | PutPayloadFault::KeyEmptyValue
    )
    .then_some(item)
}

/// The record message for an item-level fault: the `PutItem` message,
/// except for the nesting-depth class, whose bulk record carries the short
/// form "Nesting Levels have exceeded supported limits" (measured
/// at 32, 33 and 63 levels; the long `PutItem` form with its
/// explanatory clause is the parse-level abandon message at 64 levels and
/// beyond, see `rows::NESTING_ABANDON_MESSAGE`).
pub(super) fn item_fault_message(kind: Option<PutPayloadFault>, message: String) -> String {
    match kind {
        Some(PutPayloadFault::NestingDepth) => NESTING_ITEM_MESSAGE.to_owned(),
        _ => message,
    }
}

/// The item-level nesting-depth record message (measured on the service).
const NESTING_ITEM_MESSAGE: &str = "Nesting Levels have exceeded supported limits";

/// The record message for a condition fault: the `PutItem` message without
/// the per-parameter prefix the single-item path puts on it. Measured on
/// the service: `Invalid ConditionExpression: ` is dropped on the operator
/// count, the 4 KB size bound and a syntax error;
/// `ExpressionAttributeNames contains invalid key: ` is dropped on the
/// placeholder key length fault. The unused-placeholder texts carry no
/// prefix on either path. The shared parser does
/// not raise the key length fault on this path today; the second prefix is
/// stripped so the rule holds when it does.
pub(super) fn condition_fault_message(message: String) -> String {
    for prefix in CONDITION_PREFIXES {
        if let Some(rest) = message.strip_prefix(prefix) {
            return rest.to_owned();
        }
    }
    message
}

const CONDITION_PREFIXES: [&str; 2] = [
    "Invalid ConditionExpression: ",
    "ExpressionAttributeNames contains invalid key: ",
];

/// One row's admission through the shared write core, in the order
/// `handle_put_item` applies the checks: the condition fields first, then
/// the item. A row carrying both an unparseable condition and a bad item
/// therefore reports the condition message, as `PutItem` answers for the
/// same item and expression.
pub(super) enum Admission {
    /// The row may be written; the parsed condition when the row has one.
    Write(Option<(Expr, ExpressionMaps)>),
    /// The row fails an item-level check: the record message and, for item
    /// faults, the fired check deciding the image rule. Condition faults
    /// carry no kind and no image.
    Fault {
        message: String,
        kind: Option<PutPayloadFault>,
    },
}

pub(super) fn admit_row(row: &Row, key_info: &TableKeyInfo, limits: &LimitsConfig) -> Admission {
    let condition = match &row.condition {
        Some(condition) => match parse_row_condition(condition, limits) {
            Ok(parsed) => Some(parsed),
            Err(fault) => {
                return Admission::Fault {
                    message: condition_fault_message(fault.to_string()),
                    kind: None,
                };
            }
        },
        None => None,
    };
    if let Err(fault) = validate_put_payload(&row.item, key_info, &row.item_limits(limits)) {
        return Admission::Fault {
            message: fault.error.to_string(),
            kind: Some(fault.kind),
        };
    }
    // Secondary-index key attributes present in the row must match their
    // declared type and must not be empty. The storage backends run the same
    // check on every row of a batch before writing, but there it is a batch
    // error that fails the operation; here it is this row's fault, reported
    // with the `PutItem` message and no image, and the file continues.
    let index_refs: Vec<extenddb_core::validation::IndexKeyRef<'_>> = key_info
        .global_secondary_indexes
        .iter()
        .chain(key_info.local_secondary_indexes.iter())
        .map(|idx| extenddb_core::validation::IndexKeyRef {
            index_name: &idx.index_name,
            key_schema: &idx.key_schema,
        })
        .collect();
    if let Err(error) = extenddb_core::validation::validate_index_keys(
        &row.item,
        &index_refs,
        &key_info.attribute_definitions,
    ) {
        return Admission::Fault {
            message: error.to_string(),
            kind: Some(PutPayloadFault::IndexKey),
        };
    }
    Admission::Write(condition)
}

/// Parse one row's condition through the shared core `handle_put_item`
/// uses. The attribute name sizes in both expression maps are checked
/// first, as `handle_put_item` checks them ahead of the condition, with
/// the standalone request's wording; `condition_fault_message` leaves that
/// text as `PutItem` words it, because its `1 validation error detected: `
/// opening is not one of the prefixes it strips. Then
/// `resolve_put_condition` runs `resolve_condition`, the unused-attribute
/// check, and the nesting-depth check on the expression attribute values,
/// in that order.
fn parse_row_condition(
    condition: &RowCondition,
    limits: &LimitsConfig,
) -> Result<(Expr, ExpressionMaps), DynamoDbError> {
    extenddb_core::validation::validate_expression_attribute_name_sizes(
        condition.names.as_ref(),
        condition.values.as_ref(),
        limits,
        extenddb_core::validation::AttributeNameRequestForm::SingleItem,
    )?;
    let (parsed, maps) = resolve_put_condition(
        Some(&condition.expression),
        condition.names.as_ref(),
        condition.values.as_ref(),
        None,
        None,
        limits,
    )?;
    // parse_row filters empty expressions, so this branch is not reached;
    // the message is the tokenizer's for an empty condition expression.
    let Some(expr) = parsed else {
        return Err(DynamoDbError::ValidationException(
            "Invalid ConditionExpression: The expression can not be empty;".to_owned(),
        ));
    };
    Ok((expr, maps))
}

/// Whether a read fault inside the resume skip region abandons the file.
/// A line over the line limit or one that is not valid UTF-8 means the
/// file changed since a committed checkpoint counted the region: the
/// earlier run abandoned the file at any such line, so its cursor cannot
/// lie past one (end of file is the other changed-file signal and is
/// handled before this). An I/O fault, a jail refusal, or a non-regular
/// file does not imply a changed file and fails the operation exactly as
/// it would on a first read, so a resume can never complete an operation
/// that a crash-free run would have failed.
pub(super) fn skip_fault_abandons(error: &SourceError) -> bool {
    matches!(
        error,
        SourceError::LineTooLong
            | SourceError::InvalidUtf8
            | SourceError::Corrupt
            | SourceError::DecodeFailed
    )
}

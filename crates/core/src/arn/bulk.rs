// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bulk operation ARN parsing and the own-table ARN resolution.

use crate::error::DynamoDbError;

/// Layer 1 of the bulk operation ARN validation: the string is not a bulk
/// operation ARN at all.
const ARN_SHAPE_MESSAGE: &str = "Invalid Bulk operation ARN";

/// Layer 2: a bulk operation ARN whose partition is not `aws`, whose
/// service is not `dynamodb`, or whose operation id has the wrong shape.
const ARN_ID_MESSAGE: &str = "Invalid Request: Bulk operation ARN is invalid.";

/// Why a bulk operation ARN did not resolve for the caller who sent it.
///
/// The variants are the layers in the order the hosted service applies
/// them (measured on the service): `Shape` first, then the account
/// and region comparisons, and only then the partition, service, and
/// operation id. A foreign-account ARN carrying an id of the wrong shape
/// is answered as a foreign account, not as a malformed id, so the caller
/// learns nothing about the shape of another account's ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BulkOperationArnError {
    /// The string is not a bulk operation ARN at all.
    Shape,
    /// A bulk operation ARN naming another account.
    ForeignAccount,
    /// The caller's account, another region.
    ForeignRegion,
    /// The caller's account and region, with a partition, a service, or an
    /// operation id the parse refuses.
    InvalidId,
}

/// Why a table ARN spelling did not resolve to a bare name.
///
/// `ForeignAccount` is a well-formed table ARN naming another account:
/// both callers answer it `AccessDeniedException`, `Access is denied`
/// (measured), before any policy is fetched and before the handler runs.
/// `ForeignRegion` is a well-formed table ARN in the caller's account
/// naming another region: it answers `ValidationException`, `1 validation
/// error detected: Invalid AWS region in '{arn}'` (measured), at
/// validation step 2. The account check wins when both differ, matching
/// the authorization layer running first. `Invalid` is every other
/// non-resolving spelling, answered with the `Invalid table ARN` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableArnError {
    ForeignAccount,
    ForeignRegion,
    Invalid,
}

/// The bare table name of a value spelled as a table ARN in the caller's
/// own account and region.
///
/// A string resolves only when every one of these holds: it starts with
/// `arn:`, has exactly six colon-separated parts, the partition is `aws`,
/// the service is `dynamodb`, the region and account equal the caller's,
/// the resource starts with `table/`, and the remainder is a non-empty
/// name containing neither `/` nor `:`. The `/` rule rejects index and
/// backup ARNs; the `:` rule rejects an ARN with a seventh
/// colon-separated field, which `splitn` would otherwise fold into the
/// name. A name never resolves across account, region, or partition
/// boundaries.
///
/// A non-resolving spelling is classified by [`TableArnError`]. Both
/// callers read the accept rules and the classification from this one
/// function, so the name a policy is evaluated against, the name the
/// handler serves, and the error a rejected spelling answers cannot
/// drift apart.
///
/// # Errors
///
/// Returns the [`TableArnError`] classification; the callers map it to
/// their wire errors.
pub fn own_table_arn_name(
    value: &str,
    region: &str,
    account: &str,
) -> Result<String, TableArnError> {
    if !value.starts_with("arn:") {
        return Err(TableArnError::Invalid);
    }
    let parts: Vec<&str> = value.splitn(6, ':').collect();
    if parts.len() < 6 || parts[1] != "aws" || parts[2] != "dynamodb" {
        return Err(TableArnError::Invalid);
    }
    let Some(name) = parts[5].strip_prefix("table/") else {
        return Err(TableArnError::Invalid);
    };
    if name.is_empty() || name.contains('/') || name.contains(':') {
        return Err(TableArnError::Invalid);
    }
    if parts[4] != account {
        return Err(TableArnError::ForeignAccount);
    }
    if parts[3] != region {
        return Err(TableArnError::ForeignRegion);
    }
    Ok(name.to_owned())
}

/// The parsed parts of a bulk operation ARN.
///
/// [`parse_bulk_operation_arn`] reads a string with no caller to compare
/// against, so a foreign ARN parses successfully there and the caller
/// decides what to answer. [`own_bulk_operation_arn`] takes the caller's
/// region and account and classifies a foreign ARN itself, which is what
/// puts the account comparison ahead of the operation id check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkOperationArn {
    pub region: String,
    pub account: String,
    pub operation_id: String,
}

/// The first-layer answer for a bulk operation ARN that is not an ARN at
/// all, including an absent one (measured on the service:
/// `DescribeBulkOperation` and `CancelBulkOperation` with no
/// `BulkOperationArn` answer this).
#[must_use]
pub fn bulk_operation_arn_shape_error() -> DynamoDbError {
    DynamoDbError::ValidationException(ARN_SHAPE_MESSAGE.to_owned())
}

/// The layer 2 answer: a bulk operation ARN whose partition, service, or
/// operation id the parse refuses.
#[must_use]
pub fn bulk_operation_arn_id_error() -> DynamoDbError {
    DynamoDbError::ValidationException(ARN_ID_MESSAGE.to_owned())
}

/// The colon-separated fields of a string that passed layer 1, with
/// nothing read yet. One home for the split, so the two entry points
/// below cannot disagree about what a bulk operation ARN is made of.
struct BulkArnFields<'a> {
    partition: &'a str,
    service: &'a str,
    region: &'a str,
    account: &'a str,
    operation_id: &'a str,
}

impl BulkArnFields<'_> {
    /// Whether the partition and service are the two this server serves
    /// (both measured). Policy resource matching is literal, so a
    /// respelling no Deny on the canonical spelling could match is
    /// refused rather than served.
    fn names_this_service(&self) -> bool {
        self.partition == "aws" && self.service == "dynamodb"
    }

    /// Whether the operation id has the shape ids are minted with.
    fn id_is_well_formed(&self) -> bool {
        is_valid_operation_id(self.operation_id)
    }

    fn into_arn(self) -> BulkOperationArn {
        BulkOperationArn {
            region: self.region.to_owned(),
            account: self.account.to_owned(),
            operation_id: self.operation_id.to_owned(),
        }
    }
}

/// Layer 1: split a string that is a bulk operation ARN into its fields.
/// `None` for a string that does not start with `arn:`, falls outside the
/// model's length bounds, has fewer than six colon-separated parts, or
/// whose resource part is not `bulk-operation/{id}`. Nothing else is read
/// here.
fn split_bulk_operation_arn(arn: &str) -> Option<BulkArnFields<'_>> {
    if !arn.starts_with("arn:") || arn.len() < 37 || arn.len() > 1024 {
        return None;
    }
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() < 6 {
        return None;
    }
    let operation_id = parts[5].strip_prefix("bulk-operation/")?;
    Some(BulkArnFields {
        partition: parts[1],
        service: parts[2],
        region: parts[3],
        account: parts[4],
        operation_id,
    })
}

/// Read a bulk operation ARN on behalf of the caller who sent it, in the
/// order the hosted service applies the layers.
///
/// Layer 1 is the shape. The account and region comparisons come next, and
/// the partition, service, and operation id last: a foreign-account ARN
/// whose id is malformed is `ForeignAccount` (measured on the service:
/// a foreign account with the id `nope` is denied, while the caller's
/// own account with the same id answers the layer 2 validation message).
/// Both foreign answers are decided here, from the string alone, so
/// neither depends on whether the foreign operation exists.
///
/// The engine's handlers and the server's authorization layer both read an
/// ARN through this function, so the resource a policy is evaluated
/// against and the resource a handler serves cannot drift apart, and
/// neither can the order in which a request wrong at two layers is
/// answered.
///
/// # Errors
///
/// Returns the [`BulkOperationArnError`] classification; the callers map
/// it to their wire errors.
pub fn own_bulk_operation_arn(
    arn: &str,
    region: &str,
    account: &str,
) -> Result<BulkOperationArn, BulkOperationArnError> {
    let fields = split_bulk_operation_arn(arn).ok_or(BulkOperationArnError::Shape)?;
    if fields.account != account {
        return Err(BulkOperationArnError::ForeignAccount);
    }
    if fields.region != region {
        return Err(BulkOperationArnError::ForeignRegion);
    }
    if !fields.names_this_service() || !fields.id_is_well_formed() {
        return Err(BulkOperationArnError::InvalidId);
    }
    Ok(fields.into_arn())
}

/// Validate and split a bulk operation ARN without a caller's account or
/// region to compare against: layer 1, then the partition, service, and
/// operation id of layer 2. A caller that knows its own account reads an
/// ARN through [`own_bulk_operation_arn`] instead, because the hosted
/// service compares the account before it reads the id.
///
/// Layer 1 rejects a string that is not a bulk operation ARN: it does not
/// start with `arn:`, has fewer than six colon-separated parts, is shorter
/// than 37 or longer than 1024 characters (the model's bounds), or its
/// resource part is not `bulk-operation/{id}`. Layer 2 rejects a partition
/// that is not `aws`, a service that is not `dynamodb` (both measured),
/// or an id that does not match `[0-9]{14}-[0-9a-f]{8}`.
/// `own_table_arn_name` requires the same partition and service of a table
/// ARN.
///
/// # Errors
///
/// Returns `ValidationException` with the layer's own message.
pub fn parse_bulk_operation_arn(arn: &str) -> Result<BulkOperationArn, DynamoDbError> {
    let fields = split_bulk_operation_arn(arn).ok_or_else(bulk_operation_arn_shape_error)?;
    if !fields.names_this_service() || !fields.id_is_well_formed() {
        return Err(bulk_operation_arn_id_error());
    }
    Ok(fields.into_arn())
}

/// Layer 3 of the bulk operation ARN validation: a well-formed ARN that
/// names no operation in the caller's account, or an operation in another
/// region. The engine's handlers and the server's authorization layer
/// answer a foreign-region ARN with this one error, decided without a
/// storage read, so the caller cannot learn whether the foreign operation
/// exists. A foreign-account ARN is denied instead (measured), also
/// without a storage read.
#[must_use]
pub fn bulk_operation_does_not_exist(arn: &str) -> DynamoDbError {
    DynamoDbError::ResourceNotFoundException(format!(
        "Bulk operation with arn: {arn} does not exist"
    ))
}

/// True when `id` matches `[0-9]{14}-[0-9a-f]{8}` exactly.
fn is_valid_operation_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    bytes.len() == 23
        && bytes[..14].iter().all(u8::is_ascii_digit)
        && bytes[14] == b'-'
        && bytes[15..]
            .iter()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod bulk_arn_tests {
    use super::*;

    const REGION: &str = "us-east-1";
    const ACCOUNT: &str = "123456789012";

    #[test]
    fn only_an_own_account_and_region_table_arn_resolves() {
        assert_eq!(
            own_table_arn_name(
                &format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/orders"),
                REGION,
                ACCOUNT
            ),
            Ok("orders".to_owned())
        );
        // A well-formed table ARN of another account is denied; one of the
        // caller's account in another region is a region validation
        // failure. The account check wins when both differ, matching the
        // authorization layer running first.
        assert_eq!(
            own_table_arn_name(
                &format!("arn:aws:dynamodb:{REGION}:999999999999:table/orders"),
                REGION,
                ACCOUNT
            ),
            Err(TableArnError::ForeignAccount),
        );
        assert_eq!(
            own_table_arn_name(
                &format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:table/orders"),
                REGION,
                ACCOUNT
            ),
            Err(TableArnError::ForeignRegion),
        );
        assert_eq!(
            own_table_arn_name(
                "arn:aws:dynamodb:eu-west-1:999999999999:table/orders",
                REGION,
                ACCOUNT
            ),
            Err(TableArnError::ForeignAccount),
        );
        // Everything else is Invalid: bare names, another partition,
        // another service, another resource type, an index path, an empty
        // name, and a name that keeps ARN separators.
        for invalid in [
            "orders".to_owned(),
            format!("arn:aws-cn:dynamodb:{REGION}:{ACCOUNT}:table/orders"),
            format!("arn:aws:s3:{REGION}:{ACCOUNT}:table/orders"),
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:backup/orders"),
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/orders/index/gsi1"),
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/"),
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/orders:extra"),
            format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}"),
        ] {
            assert_eq!(
                own_table_arn_name(&invalid, REGION, ACCOUNT),
                Err(TableArnError::Invalid),
                "{invalid} must classify as invalid"
            );
        }
    }

    fn parse_message(input: &str) -> String {
        match parse_bulk_operation_arn(input).expect_err("this ARN must be rejected") {
            DynamoDbError::ValidationException(message) => message,
            other => panic!("expected ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn each_parse_layer_answers_its_own_message() {
        // Layer 1: not a bulk operation ARN at all.
        assert_eq!(parse_message("orders"), ARN_SHAPE_MESSAGE);
        assert_eq!(
            parse_message("arn:aws:dynamodb:us-east-1:123456789012:table/x"),
            ARN_SHAPE_MESSAGE
        );
        // Layer 2: a partition or service respelling (both measured).
        // Policy resource matching is literal, so no Deny written against
        // the documented ARN shape could match either string; accepting
        // one would let it evade an explicit Deny on the canonical
        // spelling. The parse still refuses both, with the layer 2
        // message.
        assert_eq!(
            parse_message(
                "arn:aws-cn:dynamodb:us-east-1:123456789012:bulk-operation/01789060589767-da9ddd4f"
            ),
            ARN_ID_MESSAGE
        );
        assert_eq!(
            parse_message(
                "arn:aws:s3:us-east-1:123456789012:bulk-operation/01789060589767-da9ddd4f"
            ),
            ARN_ID_MESSAGE
        );
        // Layer 2: the right resource type with the wrong id shape.
        assert_eq!(
            parse_message(
                "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/not-a-valid-id-here"
            ),
            ARN_ID_MESSAGE
        );
    }

    #[test]
    fn a_valid_arn_parses_into_its_parts() {
        let parsed = parse_bulk_operation_arn(
            "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/01789060589767-da9ddd4f",
        )
        .expect("this ARN parses");
        assert_eq!(
            parsed,
            BulkOperationArn {
                region: "us-east-1".to_owned(),
                account: "123456789012".to_owned(),
                operation_id: "01789060589767-da9ddd4f".to_owned(),
            }
        );
    }

    #[test]
    fn the_caller_aware_read_compares_the_account_before_the_id() {
        let valid_id = "01789060589767-da9ddd4f";
        // Measured on the service: with a malformed id, a foreign
        // account is denied and the caller's own account answers the
        // layer two validation message.
        assert_eq!(
            own_bulk_operation_arn(
                "arn:aws:dynamodb:us-east-1:999999999999:bulk-operation/nope",
                REGION,
                ACCOUNT
            ),
            Err(BulkOperationArnError::ForeignAccount)
        );
        assert_eq!(
            own_bulk_operation_arn(
                &format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/nope"),
                REGION,
                ACCOUNT
            ),
            Err(BulkOperationArnError::InvalidId)
        );
        // The account comparison outranks the region comparison, and both
        // outrank the id, so a foreign account in a foreign region with a
        // malformed id is still the account answer.
        assert_eq!(
            own_bulk_operation_arn(
                "arn:aws:dynamodb:eu-west-1:999999999999:bulk-operation/nope",
                REGION,
                ACCOUNT
            ),
            Err(BulkOperationArnError::ForeignAccount)
        );
        assert_eq!(
            own_bulk_operation_arn(
                &format!("arn:aws:dynamodb:eu-west-1:{ACCOUNT}:bulk-operation/nope"),
                REGION,
                ACCOUNT
            ),
            Err(BulkOperationArnError::ForeignRegion)
        );
        // Layer 1 still runs first: a string that is not a bulk operation
        // ARN is the shape answer whatever account it names.
        assert_eq!(
            own_bulk_operation_arn("orders", REGION, ACCOUNT),
            Err(BulkOperationArnError::Shape)
        );
        assert_eq!(
            own_bulk_operation_arn(
                "arn:aws:dynamodb:us-east-1:999999999999:table/orders-with-a-long-name",
                REGION,
                ACCOUNT
            ),
            Err(BulkOperationArnError::Shape)
        );
        // A partition or service respelling of the caller's own ARN is the
        // id answer, as it is for the no-caller-context parse.
        for respelled in [
            format!("arn:aws-cn:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/{valid_id}"),
            format!("arn:aws:s3:{REGION}:{ACCOUNT}:bulk-operation/{valid_id}"),
        ] {
            assert_eq!(
                own_bulk_operation_arn(&respelled, REGION, ACCOUNT),
                Err(BulkOperationArnError::InvalidId),
                "{respelled} must answer the id layer"
            );
        }
        // The caller's own canonical ARN resolves to the same parts the
        // no-caller-context parse gives.
        let canonical = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:bulk-operation/{valid_id}");
        assert_eq!(
            own_bulk_operation_arn(&canonical, REGION, ACCOUNT),
            Ok(BulkOperationArn {
                region: REGION.to_owned(),
                account: ACCOUNT.to_owned(),
                operation_id: valid_id.to_owned(),
            })
        );
        assert_eq!(
            parse_bulk_operation_arn(&canonical).ok(),
            own_bulk_operation_arn(&canonical, REGION, ACCOUNT).ok()
        );
    }

    #[test]
    fn the_not_found_error_names_the_arn() {
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:bulk-operation/01789060589767-da9ddd4f";
        match bulk_operation_does_not_exist(arn) {
            DynamoDbError::ResourceNotFoundException(message) => {
                assert_eq!(
                    message,
                    format!("Bulk operation with arn: {arn} does not exist")
                );
            }
            other => panic!("expected ResourceNotFoundException, got {other:?}"),
        }
    }
}

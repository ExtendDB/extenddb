// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The measured `WarmThroughput` request rules (Amazon DynamoDB, us-east-1,
//! 2026-10-23). Every refusal is a `ValidationException` whose text is the
//! service's own.
//!
//! - On-demand, `CreateTable`: a side below the on-demand default (12000 read,
//!   4000 write) is refused with the "initial throughput for OnDemand"
//!   sentence. Read is checked before write (a 0/0 request reports the read
//!   message).
//! - Provisioned, `CreateTable`: a side below the resource's provisioned units
//!   is refused with the "lower than ReadCapacityUnits of
//!   ProvisionedThroughput" sentence (both member forms measured; a value
//!   equal to the units is accepted).
//! - `UpdateTable`, either billing mode: a side below the CURRENT warm
//!   throughput is refused with the "decreasing WarmThroughput is not
//!   supported" sentence. The same values as current are accepted.
//! - Every sentence names its subject: `for table` on the table, `for index
//!   <IndexName>` on a global secondary index (measured on create and update,
//!   both members).
//! - `UpdateTable`: a table-level `WarmThroughput` is an operation that must
//!   stand alone, checked before the value rules. Measured per companion:
//!   `SSESpecification`, `TableClass` and `DeletionProtectionEnabled` each
//!   answer their own member's "must be the only operation" sentence (those
//!   members have the same rule on their own); `OnDemandThroughput`, with or
//!   without `BillingMode`, answers its own "can only be combined with
//!   BillingMode" sentence; a `GlobalSecondaryIndexUpdates` `Delete` answers
//!   its own sentence; `StreamSpecification`, `BillingMode` and
//!   `ProvisionedThroughput` answer the WarmThroughput sentence;
//!   a `GlobalSecondaryIndexUpdates` `Update` is accepted beside it (table and
//!   index both go `UPDATING`). See [`validate_warm_throughput_is_only_operation`].

use crate::error::DynamoDbError;
use crate::types::{
    BillingMode, CreateGsiAction, CreateTableInput, GsiInput, ON_DEMAND_WARM_READ_UNITS,
    ON_DEMAND_WARM_WRITE_UNITS, ProvisionedThroughput, UpdateTableInput, WarmThroughput,
};

const PREFIX: &str = "One or more parameter values were invalid: Requested ";

const ON_DEMAND_MINIMUM_SUFFIX: &str = " is lower than initial throughput for OnDemand. See: \
     https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/\
     on-demand-capacity-mode.html#on-demand-capacity-mode-initial";

const DECREASE_SUFFIX: &str = " is lower than current WarmThroughput, decreasing \
     WarmThroughput is not supported";

/// The measured refusal for a table-level `WarmThroughput` sent beside a
/// `StreamSpecification`, `BillingMode` or `ProvisionedThroughput` change.
pub const WARM_THROUGHPUT_MUST_BE_ONLY_OPERATION: &str = "One or more parameter values were \
     invalid: WarmThroughput must be the only operation in the request";

/// The measured refusal for `TableClass` beside another operation.
pub const TABLE_CLASS_MUST_BE_ONLY_OPERATION: &str = "One or more parameter values were \
     invalid: TableClass modification must be the only operation in the request";

/// The measured refusal for `DeletionProtectionEnabled` beside another
/// operation.
pub const DELETION_PROTECTION_MUST_BE_ONLY_OPERATION: &str = "One or more parameter values \
     were invalid: DeletionProtection modification must be the only operation in the request";

/// The measured refusal for `OnDemandThroughput` beside a table
/// `WarmThroughput`, with or without `BillingMode` (rows T4a, T4d).
pub const ON_DEMAND_THROUGHPUT_NEEDS_BILLING_MODE: &str = "One or more parameter values were \
     invalid: OnDemandThroughput can only be combined with BillingMode in an UpdateTable request";

/// The measured refusal for `SSESpecification` beside another operation
/// (rows S1a to S1c).
pub const SSE_MUST_BE_ONLY_OPERATION: &str = "One or more parameter values were invalid: \
     Server-Side Encryption modification must be the only operation in the request";

/// The measured refusal for a `GlobalSecondaryIndexUpdates` `Delete` beside a
/// table `WarmThroughput` (row S9a; the index stays).
pub const DELETE_GSI_WITH_WARM_THROUGHPUT: &str = "One or more parameter values were invalid: \
     Delete global secondary index cannot be specified when updating WarmThroughput";

/// The measured refusal for a `GlobalSecondaryIndexUpdates` `Delete` beside a
/// table `WarmThroughput` and a `StreamSpecification` (row V3a).
pub const INDEX_CHANGE_WITH_STREAM_CHANGE: &str = "One or more parameter values were invalid: \
     You cannot create or delete index while changing stream status";

/// The resource a `WarmThroughput` sentence is about: the service writes
/// `for table` on the table and `for index <IndexName>` on a global secondary
/// index (measured on create and update, both members).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmThroughputSubject<'a> {
    Table,
    Index(&'a str),
}

impl std::fmt::Display for WarmThroughputSubject<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table => f.write_str("table"),
            Self::Index(name) => write!(f, "index {name}"),
        }
    }
}

/// The on-demand minimum refusal for `member` (`ReadUnitsPerSecond` or
/// `WriteUnitsPerSecond`) on `subject`. Measured byte for byte on both
/// members, on a table and on an index.
#[must_use]
pub fn warm_throughput_below_on_demand_minimum(
    member: &str,
    subject: WarmThroughputSubject<'_>,
) -> String {
    format!("{PREFIX}{member} for WarmThroughput for {subject}{ON_DEMAND_MINIMUM_SUFFIX}")
}

/// The provisioned-units refusal for `member` and its provisioned
/// counterpart (`ReadCapacityUnits` or `WriteCapacityUnits`) on `subject`.
/// Measured on both members on a table and on the read member on an index.
#[must_use]
pub fn warm_throughput_below_provisioned(
    member: &str,
    capacity_member: &str,
    subject: WarmThroughputSubject<'_>,
) -> String {
    format!(
        "{PREFIX}{member} for WarmThroughput for {subject} is lower than {capacity_member} of \
         ProvisionedThroughput"
    )
}

/// The decrease refusal for `member` on `subject`. Measured on both members
/// on a table (either billing mode) and on an index.
#[must_use]
pub fn warm_throughput_decrease(member: &str, subject: WarmThroughputSubject<'_>) -> String {
    format!("{PREFIX}{member} for WarmThroughput for {subject}{DECREASE_SUFFIX}")
}

/// The `CreateTable` rule for one resource (the table or one index) against
/// the billing mode it is created under and, when provisioned, its units.
///
/// # Errors
///
/// `ValidationException` with the measured sentence for the first side that
/// is too low, read before write.
pub fn validate_warm_throughput_on_create(
    spec: &WarmThroughput,
    subject: WarmThroughputSubject<'_>,
    billing_mode: BillingMode,
    provisioned: Option<&ProvisionedThroughput>,
) -> Result<(), DynamoDbError> {
    match billing_mode {
        BillingMode::PayPerRequest => {
            if spec
                .read_units_per_second
                .is_some_and(|r| r < ON_DEMAND_WARM_READ_UNITS)
            {
                return Err(DynamoDbError::ValidationException(
                    warm_throughput_below_on_demand_minimum("ReadUnitsPerSecond", subject),
                ));
            }
            if spec
                .write_units_per_second
                .is_some_and(|w| w < ON_DEMAND_WARM_WRITE_UNITS)
            {
                return Err(DynamoDbError::ValidationException(
                    warm_throughput_below_on_demand_minimum("WriteUnitsPerSecond", subject),
                ));
            }
        }
        BillingMode::Provisioned => {
            // A provisioned resource without units is refused by the throughput
            // rules before this one runs; nothing to compare against here.
            let Some(units) = provisioned else {
                return Ok(());
            };
            if spec
                .read_units_per_second
                .is_some_and(|r| r < units.read_capacity_units)
            {
                return Err(DynamoDbError::ValidationException(
                    warm_throughput_below_provisioned(
                        "ReadUnitsPerSecond",
                        "ReadCapacityUnits",
                        subject,
                    ),
                ));
            }
            if spec
                .write_units_per_second
                .is_some_and(|w| w < units.write_capacity_units)
            {
                return Err(DynamoDbError::ValidationException(
                    warm_throughput_below_provisioned(
                        "WriteUnitsPerSecond",
                        "WriteCapacityUnits",
                        subject,
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// The `UpdateTable` rule for one resource against its current effective warm
/// throughput: no side may go below the current value. Equal is accepted.
///
/// # Errors
///
/// `ValidationException` with the measured decrease sentence, read before write.
pub fn validate_warm_throughput_on_update(
    spec: &WarmThroughput,
    subject: WarmThroughputSubject<'_>,
    current: (i64, i64),
) -> Result<(), DynamoDbError> {
    if spec.read_units_per_second.is_some_and(|r| r < current.0) {
        return Err(DynamoDbError::ValidationException(
            warm_throughput_decrease("ReadUnitsPerSecond", subject),
        ));
    }
    if spec.write_units_per_second.is_some_and(|w| w < current.1) {
        return Err(DynamoDbError::ValidationException(
            warm_throughput_decrease("WriteUnitsPerSecond", subject),
        ));
    }
    Ok(())
}

/// The `UpdateTable` rules for a request carrying a table-level
/// `WarmThroughput`, checked before the value rules. Measured per companion
/// on an on-demand table with one GSI, all values otherwise valid
/// (2026-10-23; row labels are those of the live runs):
///
/// | beside `WarmThroughput`                   | refusal                          | rows |
/// |-------------------------------------------|----------------------------------|------|
/// | `SSESpecification`                        | Server-Side Encryption must be the only operation | S1a, S1b, S1c |
/// | `TableClass`                              | TableClass must be the only operation | V3b, V4, S10b |
/// | `DeletionProtectionEnabled`               | DeletionProtection must be the only operation | S1d, T4c, T5d |
/// | `OnDemandThroughput`, with or without `BillingMode` | OnDemandThroughput can only be combined with BillingMode | T4a, T4d |
/// | `GlobalSecondaryIndexUpdates` with a `Delete` | Delete global secondary index cannot be specified when updating WarmThroughput | S9a |
/// | `GlobalSecondaryIndexUpdates` with a `Delete` and a `StreamSpecification` | You cannot create or delete index while changing stream status | V3a |
/// | `StreamSpecification`                     | WarmThroughput must be the only operation | T5b |
/// | `ProvisionedThroughput`, no `BillingMode` | WarmThroughput must be the only operation | V2 |
/// | `BillingMode` and `ProvisionedThroughput` | WarmThroughput must be the only operation | UT2 |
/// | `GlobalSecondaryIndexUpdates` with an `Update` | accepted; table and index go UPDATING | |
///
/// Order, as measured: `SSESpecification`, then `TableClass`, then
/// `DeletionProtectionEnabled`, then `OnDemandThroughput`, then the index
/// `Delete`, then the WarmThroughput rule. SSE outranks TableClass,
/// DeletionProtection and Streams (S1a, S1b, S1c); TableClass outranks
/// DeletionProtection and Streams, and outranks a `Delete` (V3b); the triple
/// TableClass, DeletionProtection and Streams answers the TableClass sentence
/// (V4); DeletionProtection outranks Streams (S1d); TableClass and
/// DeletionProtection outrank `OnDemandThroughput` (T4b, T4c). The position of
/// the index `Delete` relative to `SSESpecification`, `DeletionProtectionEnabled`
/// and `OnDemandThroughput` was not measured; it sits after them, where the
/// measured pairs allow. A missing table with a companion answers the
/// companion's sentence (S10b TableClass, T5b Streams, T5d DeletionProtection)
/// because this function runs before the table is read; a bad value alone, or
/// an index `Update` alone, reaches the table lookup and answers
/// `ResourceNotFoundException` (S10, T5a, T5c).
///
/// TableClass, DeletionProtection and SSE have the same "only operation" rule
/// on their own, without WarmThroughput (measured), but ExtendDB does not apply
/// it elsewhere yet; here the companion's own sentence is used only when
/// WarmThroughput is present, so a request without WarmThroughput is never
/// refused by this function. `VectorIndexUpdates` is an ExtendDB extension and
/// is grouped with the WarmThroughput sentence, unmeasured. `AttributeDefinitions`
/// is not an operation.
///
/// # Errors
///
/// `ValidationException` with the measured sentence for the companion.
pub fn validate_warm_throughput_is_only_operation(
    input: &UpdateTableInput,
) -> Result<(), DynamoDbError> {
    if input.warm_throughput.is_none() {
        return Ok(());
    }
    let refuse = |sentence: &str| Err(DynamoDbError::ValidationException(sentence.to_owned()));
    if input.sse_specification.is_some() {
        return refuse(SSE_MUST_BE_ONLY_OPERATION);
    }
    if input.table_class.is_some() {
        return refuse(TABLE_CLASS_MUST_BE_ONLY_OPERATION);
    }
    if input.deletion_protection_enabled.is_some() {
        return refuse(DELETION_PROTECTION_MUST_BE_ONLY_OPERATION);
    }
    if input.on_demand_throughput.is_some() {
        return refuse(ON_DEMAND_THROUGHPUT_NEEDS_BILLING_MODE);
    }
    if input
        .global_secondary_index_updates
        .iter()
        .flatten()
        .any(|u| u.delete.is_some())
    {
        return refuse(if input.stream_specification.is_some() {
            INDEX_CHANGE_WITH_STREAM_CHANGE
        } else {
            DELETE_GSI_WITH_WARM_THROUGHPUT
        });
    }
    let warm_rule = input.billing_mode.is_some()
        || input.table_throughput_mode.is_some()
        || input.provisioned_throughput.is_some()
        || input.stream_specification.is_some()
        || input
            .vector_index_updates
            .as_ref()
            .is_some_and(|u| !u.is_empty());
    if warm_rule {
        return refuse(WARM_THROUGHPUT_MUST_BE_ONLY_OPERATION);
    }
    Ok(())
}

/// `validate_warm_throughput_on_create` for the table and each of its global
/// secondary indexes in a `CreateTable` request.
///
/// # Errors
///
/// The first refusal, table first, then indexes in request order.
pub fn validate_create_table_warm_throughput(
    input: &CreateTableInput,
) -> Result<(), DynamoDbError> {
    let billing_mode = input.billing_mode.unwrap_or(BillingMode::Provisioned);
    if let Some(spec) = &input.warm_throughput {
        validate_warm_throughput_on_create(
            spec,
            WarmThroughputSubject::Table,
            billing_mode,
            input.provisioned_throughput.as_ref(),
        )?;
    }
    for gsi in input.global_secondary_indexes.iter().flatten() {
        validate_gsi_warm_throughput_on_create(gsi, billing_mode)?;
    }
    Ok(())
}

/// The create rule for one `GlobalSecondaryIndexes` element.
///
/// # Errors
///
/// See [`validate_warm_throughput_on_create`].
pub fn validate_gsi_warm_throughput_on_create(
    gsi: &GsiInput,
    billing_mode: BillingMode,
) -> Result<(), DynamoDbError> {
    match &gsi.warm_throughput {
        Some(spec) => validate_warm_throughput_on_create(
            spec,
            WarmThroughputSubject::Index(&gsi.index_name),
            billing_mode,
            gsi.provisioned_throughput.as_ref(),
        ),
        None => Ok(()),
    }
}

/// The create rule for a `GlobalSecondaryIndexUpdates` `Create` action, under
/// the table's billing mode.
///
/// # Errors
///
/// See [`validate_warm_throughput_on_create`].
pub fn validate_created_gsi_warm_throughput(
    create: &CreateGsiAction,
    billing_mode: BillingMode,
) -> Result<(), DynamoDbError> {
    match &create.warm_throughput {
        Some(spec) => validate_warm_throughput_on_create(
            spec,
            WarmThroughputSubject::Index(&create.index_name),
            billing_mode,
            create.provisioned_throughput.as_ref(),
        ),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests;

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The refusal of a request that names a resource in another account.
//!
//! ExtendDB has no resource-based policies, so a resource ARN in another
//! account is always refused, before the IAM decision. DynamoDB gives a caller
//! allowed `dynamodb:*` on `*` and a caller with no policy the same answer,
//! and the answer depends on the operation and on whether the account exists
//! (measured 2026-11-03 in us-east-1, with an account that exists and with
//! `999999999999`, which does not):
//!
//! | Operations | Account exists | Account does not exist |
//! |---|---|---|
//! | the tag operations, `DescribeTable`, `UpdateTable`, `BatchWriteItem`, `TransactGetItems`, `TransactWriteItems`, `DescribeStream`, `GetShardIterator` | [`resource_policy_denial`] | "Access is denied" |
//! | `GetItem`, `PutItem`, `UpdateItem`, `Query`, `Scan`, `BatchGetItem` | [`resource_policy_denial`] | "The caller is not authorized to access the specified resource." |
//! | `CreateTable`, `DescribeTimeToLive`, `UpdateTimeToLive`, `DescribeContinuousBackups`, `UpdateContinuousBackups`, `CreateBackup`, `ListBackups`, `DescribeBackup`, `RestoreTableFromBackup`, `ExportTableToPointInTime` | ValidationException [`OWNER_ONLY`] | "Access is denied" |
//! | `RestoreTableToPointInTime` | ValidationException [`OWNER_ONLY`] | the same |
//!
//! `DeleteTable`, `DeleteItem` and `DeleteBackup` were not sent to the
//! service; they take the answers of `UpdateTable`, `UpdateItem` and
//! `DescribeBackup`. `SearchVectors` has no service counterpart and takes the
//! answers of `DescribeTable`.
//!
//! On the service a `StreamArn` that is not a stream ARN (a table, index or
//! backup ARN included), or has an empty partition, service or account field,
//! is ValidationException "Invalid StreamArn" before any of these answers, in
//! every account (measured 2026-11-05); `authorize_request` checks it before
//! this refusal ([`super::validate_stream_arn_kind`]). The other ARN shape
//! checks sit between the two answers: an index or backup ARN in
//! `ResourceArn`, or a `StreamArn` with another partition, service or region,
//! an empty region included, is "Access is denied" in an account that does
//! not exist and gets the shape ValidationException in one that does
//! (measured 2026-11-03 and 2026-11-05). ExtendDB runs its own shape checks at
//! that point ([`answer`]). A table ARN in `TableName` with another partition,
//! service or region, or naming an index or backup, gets the answers above,
//! where the service answers a ValidationException (#794). A stream ARN with
//! an empty region in a tag operation's `ResourceArn`, in an account that
//! exists, gets [`resource_policy_denial`], where the service's denial ends
//! after the resource ARN, with no reason clause (measured 2026-11-05). A
//! batch or transaction whose tables are in more than one account, the
//! caller's included, is refused with [`MIXED_ACCOUNTS`] whichever accounts
//! they are.
//!
//! An account exists here when this deployment's catalog holds it. Fields the
//! service reads as a name rather than an ARN (`ListStreams` `TableName`,
//! `RestoreTableToPointInTime` `SourceTableName`, the `ImportTable` table name,
//! a restore's `TargetTableName`) are not checked.

use std::collections::{BTreeSet, HashMap};

use extenddb_auth::AuthIdentity;
use extenddb_core::error::DynamoDbError;
use serde_json::Value;

use crate::AppState;

/// The ValidationException message for an operation only the resource owner's
/// account may call, as the service writes it (with U+2019 in "owner's").
pub(crate) const OWNER_ONLY: &str =
    "This action is only supported by accounts that match the resource owner\u{2019}s account.";

/// The ValidationException message for a batch or transaction whose tables
/// are in more than one account.
pub(crate) const MIXED_ACCOUNTS: &str =
    "Invalid request! All tables in the request must be from a single account.";

const ACCESS_IS_DENIED: &str = "Access is denied";
const CALLER_NOT_AUTHORIZED: &str =
    "The caller is not authorized to access the specified resource.";

/// Another account's resource named by a request.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum OtherAccount {
    /// The first such resource, with the action and resource a denial names.
    Resource {
        action: String,
        resource: String,
        account: String,
    },
    /// A batch or transaction whose tables are in more than one account.
    Mixed,
}

/// The account of a `DynamoDB` resource ARN (`table/...`), or `None` for a
/// bare name or any other value.
fn arn_account(value: &str) -> Option<String> {
    extenddb_core::arn::parse(value).map(|r| r.account)
}

/// The first resource `input` names by an ARN whose account is not
/// `account_id`, or `Mixed` for a batch or transaction across accounts.
pub(crate) fn find(operation: &str, input: &Value, account_id: &str) -> Option<OtherAccount> {
    let field = |key: &str| input.get(key).and_then(Value::as_str);
    let other = |value: &str, resource: String| {
        let account = arn_account(value)?;
        (account != account_id).then(|| OtherAccount::Resource {
            action: operation.to_owned(),
            resource,
            account,
        })
    };
    let named = |key: &str| field(key).and_then(|v| other(v, v.to_owned()));
    match operation {
        "TagResource" | "UntagResource" | "ListTagsOfResource" => named("ResourceArn"),
        "DescribeStream" | "GetShardIterator" => named("StreamArn"),
        // The engine trims a BackupArn before it resolves it, so the account
        // is read from the trimmed value too.
        "DescribeBackup" | "DeleteBackup" | "RestoreTableFromBackup" => field("BackupArn")
            .map(extenddb_core::backup_format::trim_backup_arn)
            .and_then(|v| other(v, v.to_owned())),
        "ExportTableToPointInTime" => named("TableArn"),
        "RestoreTableToPointInTime" => named("SourceTableArn"),
        "BatchGetItem" | "BatchWriteItem" | "TransactGetItems" | "TransactWriteItems" => {
            batch_or_transaction(operation, input, account_id)
        }
        "ListStreams" | "ImportTable" | "ListTables" | "DescribeLimits" | "DescribeEndpoints"
        | "GetRecords" => None,
        // An index query names the index ARN, built from the table ARN as
        // written (measured: `.../table/<T>/index/<I>`).
        "Query" | "Scan" => {
            let table = field("TableName")?;
            let resource = match field("IndexName") {
                Some(index)
                    if !index.is_empty()
                        && extenddb_core::arn::parse(table).is_some_and(|r| r.sub.is_none()) =>
                {
                    format!("{table}/index/{index}")
                }
                _ => table.to_owned(),
            };
            other(table, resource)
        }
        _ => named("TableName"),
    }
}

/// The tables of a batch or transaction, in request order, with the action
/// each is authorized for: `Mixed` when they are in more than one account
/// (a bare name is in the caller's), else the first in another account.
fn batch_or_transaction(operation: &str, input: &Value, account_id: &str) -> Option<OtherAccount> {
    let targets = crate::leading_keys::batch_transact_targets(operation, input, &HashMap::new())?;
    let mut accounts = BTreeSet::new();
    let mut first = None;
    for target in targets {
        let account = arn_account(&target.table).unwrap_or_else(|| account_id.to_owned());
        if first.is_none() && account != account_id {
            first = Some(OtherAccount::Resource {
                action: target.action,
                resource: target.table,
                account: account.clone(),
            });
        }
        accounts.insert(account);
    }
    if accounts.len() > 1 {
        return Some(OtherAccount::Mixed);
    }
    first
}

/// The denial for a resource in an account that exists, which has no
/// resource-based policy granting the caller anything. `action` is the bare
/// operation; `resource` is the ARN as the caller wrote it.
pub(crate) fn resource_policy_denial(principal: &str, action: &str, resource: &str) -> String {
    format!(
        "User: {principal} is not authorized to perform: dynamodb:{action} on resource: \
         {resource} because no resource-based policy allows the dynamodb:{action} action"
    )
}

/// The error for `operation` naming `resource` in another account, by whether
/// the catalog holds that account (`known`). See the module docs.
pub(crate) fn refusal(
    operation: &str,
    action: &str,
    resource: &str,
    known: bool,
    principal: &str,
) -> DynamoDbError {
    let access_denied = |m: &str| DynamoDbError::AccessDeniedException(m.to_owned());
    match (operation, known) {
        ("RestoreTableToPointInTime", _)
        | (
            "CreateTable"
            | "DescribeTimeToLive"
            | "UpdateTimeToLive"
            | "DescribeContinuousBackups"
            | "UpdateContinuousBackups"
            | "CreateBackup"
            | "ListBackups"
            | "DescribeBackup"
            | "DeleteBackup"
            | "RestoreTableFromBackup"
            | "ExportTableToPointInTime",
            true,
        ) => DynamoDbError::ValidationException(OWNER_ONLY.to_owned()),
        (_, true) => access_denied(&resource_policy_denial(principal, action, resource)),
        (
            "GetItem" | "PutItem" | "UpdateItem" | "DeleteItem" | "Query" | "Scan" | "BatchGetItem",
            false,
        ) => access_denied(CALLER_NOT_AUTHORIZED),
        (_, false) => access_denied(ACCESS_IS_DENIED),
    }
}

/// True when this deployment's catalog holds `account`. A deployment without
/// a catalog store knows no other account. A lookup failure fails the request
/// (it is refused either way) with the authorization layer's internal error.
///
/// An empty account field names no account the catalog can hold, so it is
/// answered without a lookup. Cassandra refuses an empty partition key, and
/// the lookup would fail the request with that internal error where every
/// other backend answers that the account is not held.
async fn account_in_catalog(state: &AppState, account: &str) -> Result<bool, DynamoDbError> {
    if account.is_empty() {
        return Ok(false);
    }
    let Some(store) = state.catalog_store.as_ref() else {
        return Ok(false);
    };
    match store.list_accounts_for(account).await {
        Ok(rows) => Ok(rows.iter().any(|(id, _)| id == account)),
        Err(e) => {
            tracing::error!("Authorization: account lookup failed: {e:?}");
            Err(DynamoDbError::InternalServerError(
                "Internal error during authorization".to_owned(),
            ))
        }
    }
}

/// The principal a denial names, in the form the service writes it.
fn principal_arn(identity: &AuthIdentity) -> String {
    match identity {
        AuthIdentity::User {
            account_id,
            user_name,
        } => format!("arn:aws:iam::{account_id}:user/{user_name}"),
        AuthIdentity::RoleSession {
            account_id,
            role_name,
            session_name,
        } => format!("arn:aws:sts::{account_id}:assumed-role/{role_name}/{session_name}"),
    }
}

/// The error for `operation` naming `resource` in another account. For an
/// account that exists ExtendDB's ARN shape checks run first, and one that
/// does not is refused before them, as on the service (measured 2026-11-03
/// with an index or backup ARN in `ResourceArn`, and 2026-11-05 with a
/// `StreamArn` in another partition, service or region, an empty region
/// included). For a `StreamArn` the shape check compares the partition,
/// service and region with this deployment's (`region` is the server's), so
/// such a `StreamArn` in an account that exists is "Invalid StreamArn". A
/// `StreamArn` that is not a stream ARN, or has an empty partition, service or
/// account field, does not reach this point: `authorize_request` refuses it
/// first, in every account (measured 2026-11-05).
fn answer(
    operation: &str,
    input: &Value,
    action: &str,
    resource: &str,
    known: bool,
    principal: &str,
    region: &str,
) -> DynamoDbError {
    if known {
        let shape = if matches!(
            operation,
            "TagResource" | "UntagResource" | "ListTagsOfResource"
        ) {
            super::validate_tag_resource_arn_shape(resource)
        } else {
            super::validate_operation_arns(operation, input, region)
        };
        if let Err(e) = shape {
            return e;
        }
    }
    refusal(operation, action, resource, known, principal)
}

/// Refuse a request that names a resource in another account, before the IAM
/// decision, with the answer in the module docs. `Ok` when every resource the
/// request names by ARN is in `account_id`.
///
/// A batch or transaction outside its count limits gets the count error
/// first, and the ARN shape checks run as [`answer`] describes.
pub(crate) async fn refuse(
    state: &AppState,
    identity: &AuthIdentity,
    operation: &str,
    input: &Value,
    account_id: &str,
) -> Result<(), DynamoDbError> {
    let Some(other) = find(operation, input, account_id) else {
        return Ok(());
    };
    extenddb_engine::check_request_item_counts(operation, input)?;
    let OtherAccount::Resource {
        action,
        resource,
        account,
    } = other
    else {
        return Err(DynamoDbError::ValidationException(
            MIXED_ACCOUNTS.to_owned(),
        ));
    };
    let known = account_in_catalog(state, &account).await?;
    Err(answer(
        operation,
        input,
        &action,
        &resource,
        known,
        &principal_arn(identity),
        &state.region,
    ))
}

#[cfg(test)]
mod tests;

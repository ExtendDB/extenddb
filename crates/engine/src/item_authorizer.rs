// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The authorization of item reads and writes that a request names inside
//! its body.
//!
//! The server authorizes most operations before dispatch, from the request
//! fields that name the table they read or write. An operation that names its
//! tables inside its body, as the PartiQL operations do in their statements,
//! asks the [`ItemAuthorizer`] the server puts in its
//! [`OperationContext`](crate::OperationContext) to allow each read or write
//! before it makes it. A context without one refuses every such read and
//! write.

use std::future::Future;
use std::pin::Pin;

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::AttributeValue;

/// One read or write of items, as the authorizer evaluates it: the action,
/// the resource, and the item-level condition keys the request carries for
/// it. A field that is `None` leaves its condition key absent.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ItemAccess {
    /// The action, without the `dynamodb:` prefix (`PartiQLSelect`).
    pub action: &'static str,
    /// The table read or written, by name.
    pub table: String,
    /// The index read, for a read of an index: the resource is then the
    /// index, not the table.
    pub index: Option<String>,
    /// The partition key values read or written (`dynamodb:LeadingKeys`).
    pub partition_keys: Option<Vec<AttributeValue>>,
    /// The top-level attribute names (`dynamodb:Attributes`).
    pub attributes: Option<Vec<String>>,
    /// `dynamodb:Select`.
    pub select: Option<&'static str>,
    /// `dynamodb:ReturnValues`.
    pub return_values: Option<&'static str>,
    /// `dynamodb:FullTableScan`.
    pub full_table_scan: Option<bool>,
    /// `dynamodb:EnclosingOperation`.
    pub enclosing_operation: Option<&'static str>,
}

/// The answer an [`ItemAuthorizer`] gives.
pub type ItemAuthorization<'a> =
    Pin<Box<dyn Future<Output = Result<(), DynamoDbError>> + Send + 'a>>;

/// Allows or refuses the item reads and writes of one request, for the
/// identity that signed it.
pub trait ItemAuthorizer: Send + Sync {
    /// `Ok` when `access` is allowed. A refusal is the error the request
    /// answers with: `AccessDeniedException` with the IAM message.
    fn authorize<'a>(&'a self, access: &'a ItemAccess) -> ItemAuthorization<'a>;
}

// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A predicate that a vector search applies to each candidate item.
//!
//! The engine parses and validates a search condition, and a backend only
//! evaluates it. A backend that serves vector search calls
//! [`VectorItemFilter::matches`] on candidates and keeps only the items that
//! match, so the `top_k` cut applies to matching items only. SQLite calls it
//! on each candidate before scoring it. PostgreSQL ranks the candidates in
//! SQL and calls it on the rows in score order, page by page, until `top_k`
//! rows match.
//!
//! The engine sets [`crate::VectorSearch::item_filter`] only for a condition
//! that the equality pairs of [`crate::VectorSearch::filters`] cannot express.
//! Otherwise the field is `None`, and a backend behaves exactly as it does
//! without it.

use extenddb_core::types::Item;

use crate::error::StorageError;

/// A condition on the attributes of one item.
pub trait VectorItemFilter: Send + Sync {
    /// Whether `item` satisfies the condition.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Internal`] when the condition cannot be
    /// evaluated. The engine validates the condition before the search, so an
    /// error here is a defect, not a caller mistake.
    fn matches(&self, item: &Item) -> Result<bool, StorageError>;
}

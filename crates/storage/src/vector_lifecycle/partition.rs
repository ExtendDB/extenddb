// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Partition scoping for vector index rows.
//!
//! Moved verbatim from the SQLite backend when the lifecycle was extracted
//! (ADR-0005): the encoding must stay byte-identical across backends, because a
//! value written by one backend's write path and a value derived from a search
//! request must compare equal.

use extenddb_core::types::AttributeValue;

use crate::error::StorageError;
use crate::util::pk_to_text;

/// Partition value for an index that declares no HASH element.
///
/// Such an index searches the whole table, so every row shares one partition
/// rather than the scan needing a second code path.
///
/// What makes this safe is **not** that the value is unguessable. `pk_to_text`
/// stores an `S` attribute verbatim, so a caller could supply this exact string as
/// a partition key. The guarantee is structural instead: the partition is chosen
/// from the *index's* schema, not from the item, and each index has its own data
/// table. So within one table either every row is keyed by a real hash value (the
/// index declares a HASH element) or every row uses this sentinel (it does not).
/// The two never coexist, so there is nothing for a collision to leak into.
///
/// The leading NUL is defence in depth for the day that invariant changes, for
/// instance if several indexes ever shared one table. It is deliberately not what
/// correctness rests on, because a reader who believed it was would then feel free
/// to weaken it.
pub const UNSCOPED_PARTITION: &str = "\u{0}all";

/// The partition column value for a vector row.
///
/// Uses the same `pk_to_text` encoding as item partition keys, so a value written
/// by the write path and a value derived from a search request are byte-identical.
/// Getting this wrong would not fail loudly: it would silently return no hits.
///
/// # Errors
/// Fails when the attribute value cannot be encoded as a partition key text.
pub fn partition_value(hash_key: Option<(&str, &AttributeValue)>) -> Result<String, StorageError> {
    match hash_key {
        Some((_, value)) => Ok(pk_to_text(value)?.into_owned()),
        None => Ok(UNSCOPED_PARTITION.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The partition is chosen from the index's schema, never from the item, which
    /// is the invariant that makes the sentinel safe. Asserted as the property
    /// rather than against one example: the previous version compared the sentinel
    /// to `pk_to_text(S("all"))` only, and passed unchanged when the sentinel was
    /// weakened to the ordinary string `"unscoped"`.
    #[test]
    fn the_partition_comes_from_the_index_schema_not_the_item() {
        // No HASH element declared: the sentinel, whatever the item holds.
        assert_eq!(partition_value(None).unwrap(), UNSCOPED_PARTITION);

        // A HASH element declared: the item's value, verbatim for S.
        for value in ["all", "unscoped", UNSCOPED_PARTITION, ""] {
            let scoped =
                partition_value(Some(("pk", &AttributeValue::S(value.to_owned())))).unwrap();
            assert_eq!(
                scoped, value,
                "a scoped partition must be the attribute value itself"
            );
        }
    }

    /// The sentinel keeps a leading NUL as defence in depth. Not what correctness
    /// rests on (see the constant's documentation), but weakening it to an ordinary
    /// string should break a test rather than pass silently.
    #[test]
    fn the_unscoped_sentinel_keeps_its_unusual_prefix() {
        assert!(
            UNSCOPED_PARTITION.starts_with('\0'),
            "sentinel must keep its NUL prefix: {UNSCOPED_PARTITION:?}"
        );
    }
}

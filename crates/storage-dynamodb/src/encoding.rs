// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Real, round-trip-tested marshalling between ExtendDB's internal item type and
//! the AWS SDK's. This is, structurally, the identity function: ExtendDB already
//! speaks DynamoDB's type system, so every value maps to its exact namesake.
//! The only reason this file is not empty is that ExtendDB's in-memory Rust enum
//! and the SDK's `AttributeValue` enum are *different Rust types* holding the
//! same data. We translate DynamoDB into DynamoDB. It round-trips because of
//! course it does.

use std::collections::HashMap;

use aws_sdk_dynamodb::primitives::Blob;
use extenddb_core::types::AttributeValue as CoreAttributeValue;
use extenddb_core::types::Item as CoreItem;

/// Convert an ExtendDB `AttributeValue` to the AWS SDK `AttributeValue`.
pub fn to_sdk(v: &CoreAttributeValue) -> aws_sdk_dynamodb::types::AttributeValue {
    use aws_sdk_dynamodb::types::AttributeValue as Sdk;
    match v {
        CoreAttributeValue::S(s) => Sdk::S(s.clone()),
        CoreAttributeValue::N(n) => Sdk::N(n.clone()),
        CoreAttributeValue::B(bytes) => Sdk::B(Blob::new(bytes.clone())),
        CoreAttributeValue::SS(set) => Sdk::Ss(set.iter().cloned().collect()),
        CoreAttributeValue::NS(set) => Sdk::Ns(set.iter().cloned().collect()),
        CoreAttributeValue::BS(set) => Sdk::Bs(set.iter().map(|b| Blob::new(b.clone())).collect()),
        CoreAttributeValue::Bool(b) => Sdk::Bool(*b),
        CoreAttributeValue::Null => Sdk::Null(true),
        CoreAttributeValue::L(list) => Sdk::L(list.iter().map(to_sdk).collect()),
        CoreAttributeValue::M(map) => {
            Sdk::M(map.iter().map(|(k, v)| (k.clone(), to_sdk(v))).collect())
        }
    }
}

/// Convert an AWS SDK `AttributeValue` to an ExtendDB `AttributeValue`.
///
/// Unknown / future SDK variants (the `#[non_exhaustive]` catch-all) map to
/// `AttributeValue::Null` and emit a `tracing::warn!`.
pub fn from_sdk(v: &aws_sdk_dynamodb::types::AttributeValue) -> CoreAttributeValue {
    use aws_sdk_dynamodb::types::AttributeValue as Sdk;
    use std::collections::{BTreeMap, BTreeSet};
    match v {
        Sdk::S(s) => CoreAttributeValue::S(s.clone()),
        Sdk::N(n) => CoreAttributeValue::N(n.clone()),
        Sdk::B(blob) => CoreAttributeValue::B(blob.as_ref().to_vec()),
        Sdk::Ss(vec) => CoreAttributeValue::SS(vec.iter().cloned().collect::<BTreeSet<_>>()),
        Sdk::Ns(vec) => CoreAttributeValue::NS(vec.iter().cloned().collect::<BTreeSet<_>>()),
        Sdk::Bs(blobs) => CoreAttributeValue::BS(
            blobs
                .iter()
                .map(|b| b.as_ref().to_vec())
                .collect::<BTreeSet<_>>(),
        ),
        Sdk::Bool(b) => CoreAttributeValue::Bool(*b),
        Sdk::Null(_) => CoreAttributeValue::Null,
        Sdk::L(list) => CoreAttributeValue::L(list.iter().map(from_sdk).collect()),
        Sdk::M(map) => CoreAttributeValue::M(
            map.iter()
                .map(|(k, v)| (k.clone(), from_sdk(v)))
                .collect::<BTreeMap<_, _>>(),
        ),
        _ => {
            tracing::warn!("encountered unknown SDK AttributeValue variant; mapping to Null");
            CoreAttributeValue::Null
        }
    }
}

/// Convert an ExtendDB `Item` to a DynamoDB SDK item (HashMap).
pub fn item_to_sdk(item: &CoreItem) -> HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
    item.iter().map(|(k, v)| (k.clone(), to_sdk(v))).collect()
}

/// Convert a DynamoDB SDK item (HashMap) to an ExtendDB `Item`.
pub fn item_from_sdk(item: HashMap<String, aws_sdk_dynamodb::types::AttributeValue>) -> CoreItem {
    item.into_iter().map(|(k, v)| (k, from_sdk(&v))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::types::AttributeValue as Core;
    use std::collections::{BTreeMap, BTreeSet};

    fn round_trip(v: Core) {
        let sdk = to_sdk(&v);
        let back = from_sdk(&sdk);
        assert_eq!(back, v, "round-trip mismatch");
    }

    #[test]
    fn rt_string() {
        round_trip(Core::S("hello".into()));
    }

    #[test]
    fn rt_number() {
        round_trip(Core::N("123.45".into()));
    }

    #[test]
    fn rt_bool() {
        round_trip(Core::Bool(true));
    }

    #[test]
    fn rt_null() {
        round_trip(Core::Null);
    }

    #[test]
    fn rt_binary() {
        round_trip(Core::B(vec![0u8, 1, 2, 255]));
    }

    #[test]
    fn rt_string_set() {
        round_trip(Core::SS(BTreeSet::from(["a".to_string(), "b".to_string()])));
    }

    #[test]
    fn rt_number_set() {
        round_trip(Core::NS(BTreeSet::from(["1".to_string(), "2".to_string()])));
    }

    #[test]
    fn rt_binary_set() {
        round_trip(Core::BS(BTreeSet::from([vec![1u8, 2], vec![3u8, 4]])));
    }

    #[test]
    fn rt_list_and_map_nested() {
        let mut m = BTreeMap::new();
        m.insert("k".to_string(), Core::S("v".into()));
        round_trip(Core::L(vec![Core::N("1".into()), Core::M(m)]));
    }

    #[test]
    fn item_round_trips() {
        let mut item: BTreeMap<String, Core> = BTreeMap::new();
        item.insert("pk".to_string(), Core::S("u#1".into()));
        item.insert("n".to_string(), Core::N("42".into()));
        let sdk = item_to_sdk(&item);
        assert_eq!(item_from_sdk(sdk), item);
    }
}

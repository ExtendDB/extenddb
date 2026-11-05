// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Item deserialization that refuses a repeated top-level attribute name.
//!
//! `Item` is a map, so serde's own map deserialization keeps the last of two
//! entries with one name silently, while `ImportTable` refuses the same bytes
//! with `ItemValidationError` (measured) and a reader in another language
//! might keep the first. The format refuses them wherever it reads an item:
//! a data file line ([`super::DataFileDecoder`]) and a key-range endpoint
//! ([`super::KeyRange`]). The engine's `ImportTable` reader is a separate,
//! laxer reader of the same lines that keeps the last (the `data_file`
//! module docs list how it differs).
//!
//! Only the top level is checked here. A nested `M` value is read by the
//! shared wire `AttributeValue` deserializer, which the request path uses as
//! well; this module leaves it alone, so a repeated name inside a nested map
//! is kept last-wins on both paths alike. The type descriptor of a
//! top-level value is read from the document text on both readers here, so
//! a value with a second descriptor (`{"S":"0","S":"A"}`) is refused on
//! both: by the `AttributeValue` deserializer on a data file line, and by
//! [`item_without_duplicates_raw`] on a key-range endpoint.

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, MapAccess};

use super::error::truncated;
use crate::types::AttributeValue;

/// `deserialize_with` target: an [`Item`](crate::types::Item) whose
/// top-level attribute names are distinct.
pub(super) fn item_without_duplicates<'de, D>(
    deserializer: D,
) -> Result<crate::types::Item, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_map(Visitor::<AttributeValue>(PhantomData))
}

/// One top-level attribute value as written: its type descriptor and the
/// descriptor's JSON payload, before the `AttributeValue` deserializer reads
/// (and, for `N`, rewrites) the payload.
pub(super) type WrittenValue = (String, serde_json::Value);

/// The same check, keeping each attribute's value as it was written: for a
/// reader that must look at a value before the `AttributeValue` deserializer
/// rewrites it (a `key_range` endpoint). Each value must be a JSON object
/// with exactly one member, its type descriptor; an empty object or a second
/// member (a different descriptor or the same one again) is refused while
/// the text is read, as the `AttributeValue` deserializer refuses it, rather
/// than collapsed by a JSON map that keeps the last of two equal names.
pub(super) fn item_without_duplicates_raw<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, WrittenValue>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_map(Visitor::<WrittenValue>(PhantomData))
}

/// How one attribute's value is read, given the attribute's name.
trait ReadValue<'de>: Sized {
    fn read<A: MapAccess<'de>>(map: &mut A, name: &str) -> Result<Self, A::Error>;
}

impl<'de> ReadValue<'de> for AttributeValue {
    fn read<A: MapAccess<'de>>(map: &mut A, _name: &str) -> Result<Self, A::Error> {
        map.next_value()
    }
}

impl<'de> ReadValue<'de> for WrittenValue {
    fn read<A: MapAccess<'de>>(map: &mut A, name: &str) -> Result<Self, A::Error> {
        map.next_value_seed(OneDescriptor { name })
    }
}

/// The members of a map, refusing a repeated name before its value is read.
struct Visitor<V>(PhantomData<V>);

impl<'de, V: ReadValue<'de>> serde::de::Visitor<'de> for Visitor<V> {
    type Value = BTreeMap<String, V>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an item")
    }
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut entries = BTreeMap::new();
        while let Some(name) = map.next_key::<String>()? {
            if entries.contains_key(&name) {
                return Err(serde::de::Error::custom(format!(
                    "item names attribute {:?} more than once",
                    truncated(&name)
                )));
            }
            let value = V::read(&mut map, &name)?;
            entries.insert(name, value);
        }
        Ok(entries)
    }
}

/// Reads one attribute value as its one type descriptor and payload.
struct OneDescriptor<'a> {
    name: &'a str,
}

impl<'de> DeserializeSeed<'de> for OneDescriptor<'_> {
    type Value = WrittenValue;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> serde::de::Visitor<'de> for OneDescriptor<'_> {
    type Value = WrittenValue;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an attribute value with exactly one type descriptor")
    }
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let Some((descriptor, payload)) = map.next_entry::<String, serde_json::Value>()? else {
            return Err(serde::de::Error::custom(format!(
                "attribute {:?} value has no type descriptor",
                truncated(self.name)
            )));
        };
        if let Some(second) = map.next_key::<String>()? {
            return Err(serde::de::Error::custom(format!(
                "attribute {:?} value has more than one type descriptor ({:?}, then {:?})",
                truncated(self.name),
                truncated(&descriptor),
                truncated(&second)
            )));
        }
        Ok((descriptor, payload))
    }
}

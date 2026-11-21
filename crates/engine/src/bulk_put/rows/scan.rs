// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The structure of one record's `Item`, read before the attribute values
//! are decoded.
//!
//! The line is parsed into [`Node`], a JSON tree that keeps repeated object
//! members rather than collapsing them to the last one, and the walk over
//! it follows the item's own shape: the attribute map holds typed values, an
//! `M` holds another attribute map, an `L` holds typed values. Faults the
//! shared deserializer reports with a different class, or does not report at
//! all, are reported here with the measured text for the position they sit
//! in.

use std::collections::HashSet;
use std::fmt;

use extenddb_core::validation::number::validate_and_normalize_number;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::{
    EXPECTED_ARRAY_MESSAGE, INVALID_NUMERIC_MESSAGE, REPEATED_MEMBER_MESSAGE, RowError,
    VALUE_RULE_PREFIX,
};

/// The prefix of the number validator's message for text that is not a
/// number at all, which is the only number fault the record parser reports:
/// a magnitude or digit-count fault is the row's own, through the item
/// validators the runner runs.
const NUMERIC_CONVERSION_PREFIX: &str = "The parameter cannot be converted to a numeric value";

/// One JSON value of an input line, with repeated object members kept in
/// the order they were read.
pub(super) enum Node {
    Object(Vec<(String, Node)>),
    Array(Vec<Node>),
    Leaf(serde_json::Value),
}

impl Node {
    /// This value as a `serde_json::Value`, which collapses a repeated
    /// member to the last one, the way the record shape and the attribute
    /// deserializers read it.
    pub(super) fn to_value(&self) -> serde_json::Value {
        match self {
            Self::Object(members) => serde_json::Value::Object(
                members
                    .iter()
                    .map(|(name, node)| (name.clone(), node.to_value()))
                    .collect(),
            ),
            Self::Array(elements) => {
                serde_json::Value::Array(elements.iter().map(Self::to_value).collect())
            }
            Self::Leaf(value) => value.clone(),
        }
    }

    /// The member under `name`, the last one when it is repeated, matching
    /// which of them [`Node::to_value`] keeps.
    fn member(&self, name: &str) -> Option<&Node> {
        let Self::Object(members) = self else {
            return None;
        };
        members
            .iter()
            .rev()
            .find(|(member, _)| member == name)
            .map(|(_, node)| node)
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(NodeVisitor)
    }
}

struct NodeVisitor;

impl<'de> Visitor<'de> for NodeVisitor {
    type Value = Node;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Node, A::Error> {
        let mut members = Vec::new();
        while let Some(member) = access.next_entry::<String, Node>()? {
            members.push(member);
        }
        Ok(Node::Object(members))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Node, A::Error> {
        let mut elements = Vec::new();
        while let Some(element) = access.next_element::<Node>()? {
            elements.push(element);
        }
        Ok(Node::Array(elements))
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Node, E> {
        Ok(Node::Leaf(value.into()))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Node, E> {
        Ok(Node::Leaf(value.into()))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Node, E> {
        Ok(Node::Leaf(value.into()))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Node, E> {
        Ok(Node::Leaf(
            serde_json::Number::from_f64(value).map_or(serde_json::Value::Null, Into::into),
        ))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Node, E> {
        Ok(Node::Leaf(value.into()))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Node, E> {
        Ok(Node::Leaf(serde_json::Value::Null))
    }

    fn visit_none<E: de::Error>(self) -> Result<Node, E> {
        Ok(Node::Leaf(serde_json::Value::Null))
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Node, D::Error> {
        Node::deserialize(deserializer)
    }
}

/// Check the record's own members.
///
/// A top-level member that appears twice abandons the file, even when both
/// carry the same value (measured on the service on `Item`;
/// a repeat of one of the other three members was not measured and is read
/// the same way).
///
/// # Errors
///
/// Returns [`RowError::Parse`] with [`REPEATED_MEMBER_MESSAGE`].
pub(super) fn check_record_members(record: &Node) -> Result<(), RowError> {
    let Node::Object(members) = record else {
        return Ok(());
    };
    let mut seen: HashSet<&str> = HashSet::with_capacity(members.len());
    for (name, _) in members {
        if !seen.insert(name) {
            return Err(RowError::Parse(REPEATED_MEMBER_MESSAGE.to_owned()));
        }
    }
    Ok(())
}

/// Check the structure of the record's `Item`.
///
/// A record with no `Item`, or one that is not an object, answers its own
/// message before this runs, so either shape returns without a fault here.
///
/// # Errors
///
/// Returns the measured fault for the position it is found in. Faults are
/// reported in the order the members were read, so a record carrying two of
/// them answers the first; no record with two was measured.
pub(super) fn check_item(record: &Node) -> Result<(), RowError> {
    match record.member("Item") {
        Some(item) => check_attribute_map(item, &Path::Item),
        None => Ok(()),
    }
}

/// Where a value sits in the item: the steps from `Item` down to it. The
/// walk hands it down by reference and spells it out only for a fault that
/// names it, so no value's path is copied once for every value beneath it.
enum Path<'a> {
    /// The item's own attribute map.
    Item,
    /// The value of the named attribute in the map at the parent path.
    Attribute(&'a Path<'a>, &'a str),
    /// The attribute map of the `M` value at the parent path.
    Map(&'a Path<'a>),
    /// One element of the `L` value at the parent path.
    Element(&'a Path<'a>, usize),
}

impl fmt::Display for Path<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Item => formatter.write_str("Item"),
            Self::Attribute(parent, name) => write!(formatter, "{parent}.{name}"),
            Self::Map(parent) => write!(formatter, "{parent}.M"),
            Self::Element(parent, index) => write!(formatter, "{parent}.L[{index}]"),
        }
    }
}

/// One attribute map: the item's own, or the value of an `M`. A name is
/// mapped to a typed value.
///
/// A name that appears twice in the same map is the row's own fault
/// (measured on the service): the path in the message spells
/// the typed step, so a repeat of member `k` of map attribute `m` reads
/// `Item.m.M.k`. The repeat is found when its second occurrence is read, so
/// a fault in an earlier member of the same map is reported first; no
/// record carrying both was measured.
fn check_attribute_map(map: &Node, path: &Path<'_>) -> Result<(), RowError> {
    let Node::Object(members) = map else {
        // A non-object here is the deserializer's to refuse.
        return Ok(());
    };
    let mut seen: HashSet<&str> = HashSet::with_capacity(members.len());
    for (name, typed) in members {
        if !seen.insert(name) {
            return Err(RowError::Item(format!(
                "{VALUE_RULE_PREFIX} Value for {path}.{name} is ambiguous"
            )));
        }
        check_typed_value(typed, &Path::Attribute(path, name))?;
    }
    Ok(())
}

/// One typed attribute value: a single-member object whose name is the type
/// tag.
///
/// More than one member abandons the file, whether the tag is repeated or
/// two different tags are given (measured on the service); a
/// value with no member at all is the deserializer's to refuse, which was
/// not measured.
fn check_typed_value(typed: &Node, path: &Path<'_>) -> Result<(), RowError> {
    let Node::Object(members) = typed else {
        return Ok(());
    };
    if members.len() > 1 {
        return Err(RowError::Parse(REPEATED_MEMBER_MESSAGE.to_owned()));
    }
    let Some((tag, inner)) = members.first() else {
        // A value with no type tag is the deserializer's to refuse.
        return Ok(());
    };
    match tag.as_str() {
        // An `N` whose text is not a number abandons the file (measured
        // on a sort key); the item validators answer
        // for it as a row fault otherwise. `NS` members were not measured
        // and are left to those validators.
        "N" => {
            if let Node::Leaf(serde_json::Value::String(text)) = inner
                && is_not_numeric(text)
            {
                return Err(RowError::Parse(INVALID_NUMERIC_MESSAGE.to_owned()));
            }
            Ok(())
        }
        // A set tag whose value is not an array abandons the file with the
        // message naming the character it expected (measured on the service on
        // `SS`). An `L` or an `M` of the wrong JSON type was not
        // measured and is left to the deserializer.
        "SS" | "NS" | "BS" => {
            if matches!(inner, Node::Array(_)) {
                Ok(())
            } else {
                Err(RowError::Parse(EXPECTED_ARRAY_MESSAGE.to_owned()))
            }
        }
        "M" => check_attribute_map(inner, &Path::Map(path)),
        "L" => {
            let Node::Array(elements) = inner else {
                return Ok(());
            };
            for (index, element) in elements.iter().enumerate() {
                check_typed_value(element, &Path::Element(path, index))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Whether the text is not a number at all, as against a number outside the
/// supported magnitude or digit count, which is a row fault the runner's
/// validators report.
fn is_not_numeric(text: &str) -> bool {
    match validate_and_normalize_number(text) {
        Ok(_) => false,
        Err(error) => error.to_string().starts_with(NUMERIC_CONVERSION_PREFIX),
    }
}

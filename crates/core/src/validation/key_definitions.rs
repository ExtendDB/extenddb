// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Checks that a key schema's attributes are defined in `AttributeDefinitions`,
//! shared by `CreateTable` and an `UpdateTable` GSI `Create`.
//!
//! The service's rules, measured on 2026-10-25, 2026-10-26 and 2026-11-25
//! (us-east-1), in the order the service checks them:
//!
//! 1. A key schema with more keys than there are definitions answers
//!    `Invalid KeySchema: Some index key attribute have no definition`.
//! 2. A key schema whose HASH and RANGE elements name one attribute answers
//!    `Both the Hash Key and the Range Key element in the KeySchema have the
//!    same name`.
//! 3. A name defined twice answers `Attribute Name is duplicated: <name>`,
//!    naming the name whose second definition comes first in the list.
//! 4. A key with no definition answers `Some index key attributes are not
//!    defined in AttributeDefinitions`, listing every key of the schema and
//!    every definition, each list in the order of [`listed`].
//!
//! An `UpdateTable` GSI `Create` runs the four in that order. `CreateTable`
//! runs the first three on the table's key schema, after the table billing
//! rules and before the key schema shape checks. After every shape check it
//! runs rule 4 on the table's key schema, then rules 1 and 4 on each LSI's and
//! each GSI's in turn. The service checks each index's definitions before that
//! index's shape (measured 2026-11-25), so a request with both faults in one
//! index gets the shape sentence here.

use std::collections::HashSet;

use crate::error::DynamoDbError;
use crate::types::{AttributeDefinition, KeySchemaElement, KeyType};

/// Rule 1's sentence.
const KEY_SCHEMA_WITHOUT_DEFINITION: &str =
    "Invalid KeySchema: Some index key attribute have no definition";

/// Rule 1: refuse a key schema with more keys than `defs` has entries.
///
/// # Errors
///
/// `ValidationException` `Invalid KeySchema: Some index key attribute have no
/// definition`.
pub fn validate_definition_count(
    key_schema: &[KeySchemaElement],
    defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    if defs.len() < key_schema.len() {
        return Err(DynamoDbError::ValidationException(
            KEY_SCHEMA_WITHOUT_DEFINITION.to_owned(),
        ));
    }
    Ok(())
}

/// Rule 2: refuse a two-element key schema whose HASH and RANGE elements name
/// one attribute.
///
/// # Errors
///
/// `ValidationException` `Both the Hash Key and the Range Key element in the
/// KeySchema have the same name`.
pub fn validate_hash_and_range_differ(
    key_schema: &[KeySchemaElement],
) -> Result<(), DynamoDbError> {
    if let [hash, range] = key_schema
        && hash.key_type == KeyType::Hash
        && range.key_type == KeyType::Range
        && hash.attribute_name == range.attribute_name
    {
        return Err(DynamoDbError::ValidationException(
            "Both the Hash Key and the Range Key element in the KeySchema have the same name"
                .to_owned(),
        ));
    }
    Ok(())
}

/// Rule 3: refuse a name defined twice in `defs`.
///
/// # Errors
///
/// `ValidationException` `Attribute Name is duplicated: <name>`.
pub fn validate_unique_attribute_definitions(
    defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    let mut seen = HashSet::with_capacity(defs.len());
    for def in defs {
        if !seen.insert(def.attribute_name.as_str()) {
            return Err(DynamoDbError::ValidationException(format!(
                "Attribute Name is duplicated: {}",
                def.attribute_name
            )));
        }
    }
    Ok(())
}

/// Rule 4: refuse a key schema with a key that has no definition in `defs`.
///
/// # Errors
///
/// `ValidationException` `Some index key attributes are not defined in
/// AttributeDefinitions`, with every key and every definition.
pub fn validate_keys_defined(
    key_schema: &[KeySchemaElement],
    defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    if key_schema
        .iter()
        .all(|k| defs.iter().any(|d| d.attribute_name == k.attribute_name))
    {
        return Ok(());
    }
    let keys = listed(key_schema.iter().map(|k| k.attribute_name.as_str()));
    let names = listed(defs.iter().map(|d| d.attribute_name.as_str()));
    Err(DynamoDbError::ValidationException(format!(
        "One or more parameter values were invalid: Some index key attributes are not defined in \
         AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
        keys.join(", "),
        names.join(", ")
    )))
}

/// The order in which the service lists the names of a key schema or of
/// `AttributeDefinitions`: the iteration order of a `java.util.HashMap`
/// created with 4 buckets and load factor 0.75, into which the names go in
/// request order. A name's bucket is `spread & (buckets - 1)`, where `spread`
/// is the Java `String.hashCode` of the name over its UTF-16 code units,
/// combined by XOR with itself shifted right by 16 bits. The table doubles
/// when it holds more names than three quarters of its buckets, and when a
/// bucket that holds eight names takes a ninth while the table has fewer than
/// 64 buckets. The names come out bucket by bucket, in insertion order within a
/// bucket (measured 2026-11-25, with lists of up to nine names in one bucket).
/// A repeated name keeps its first position, as `HashMap::put` does. No
/// measured list repeats a name.
///
/// Not modelled: a bucket that takes its ninth name in a table of 64 or more
/// buckets. `HashMap` turns that bucket into a tree, which it iterates in
/// another order. That needs at least 11 names, nine or more of them in one
/// bucket.
fn listed<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    let mut seen = HashSet::new();
    let mut entries: Vec<(&str, u32)> = Vec::new();
    let mut buckets: u32 = 4;
    for name in names {
        if !seen.insert(name) {
            continue;
        }
        let hash = name.encode_utf16().fold(0_u32, |h, unit| {
            h.wrapping_mul(31).wrapping_add(u32::from(unit))
        });
        let spread = hash ^ (hash >> 16);
        let mask = buckets - 1;
        let crowded = buckets < 64
            && entries
                .iter()
                .filter(|&&(_, s)| s & mask == spread & mask)
                .count()
                >= 8;
        entries.push((name, spread));
        if crowded || entries.len() > (buckets / 4 * 3) as usize {
            buckets *= 2;
        }
    }
    entries.sort_by_key(|&(_, s)| s & (buckets - 1));
    entries.into_iter().map(|(name, _)| name).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ScalarAttributeType;

    fn defs(names: &[&str]) -> Vec<AttributeDefinition> {
        names
            .iter()
            .map(|n| AttributeDefinition {
                attribute_name: (*n).to_owned(),
                attribute_type: ScalarAttributeType::S,
            })
            .collect()
    }

    fn ks(hash: &str, range: Option<&str>) -> Vec<KeySchemaElement> {
        let mut k = vec![KeySchemaElement {
            attribute_name: hash.to_owned(),
            key_type: KeyType::Hash,
        }];
        if let Some(r) = range {
            k.push(KeySchemaElement {
                attribute_name: r.to_owned(),
                key_type: KeyType::Range,
            });
        }
        k
    }

    fn message(r: Result<(), DynamoDbError>) -> String {
        match r {
            Err(DynamoDbError::ValidationException(m)) => m,
            other => panic!("expected a ValidationException, got {other:?}"),
        }
    }

    #[test]
    fn the_first_repeated_name_is_named() {
        assert_eq!(
            message(validate_unique_attribute_definitions(&defs(&[
                "b", "a", "b", "a"
            ]))),
            "Attribute Name is duplicated: b"
        );
        assert!(validate_unique_attribute_definitions(&defs(&["a", "b"])).is_ok());
    }

    #[test]
    fn fewer_definitions_than_keys_is_the_key_schema_sentence() {
        for (schema, names) in [(ks("g", Some("r")), &["pk"][..]), (ks("g", None), &[][..])] {
            assert_eq!(
                message(validate_definition_count(&schema, &defs(names))),
                "Invalid KeySchema: Some index key attribute have no definition"
            );
        }
        assert!(validate_definition_count(&ks("g", Some("r")), &defs(&["pk", "x"])).is_ok());
    }

    #[test]
    fn a_hash_and_a_range_on_one_name_is_refused() {
        assert_eq!(
            message(validate_hash_and_range_differ(&ks("g", Some("g")))),
            "Both the Hash Key and the Range Key element in the KeySchema have the same name"
        );
        assert!(validate_hash_and_range_differ(&ks("g", Some("r"))).is_ok());
        assert!(validate_hash_and_range_differ(&ks("g", None)).is_ok());
    }

    /// The lists of measured answers (2026-10-25 and 2026-11-25).
    #[test]
    fn a_missing_key_lists_every_key_and_every_definition_in_the_service_order() {
        for (schema, names, expected) in [
            (
                ks("g", Some("r")),
                &["pk", "g"][..],
                "Keys: [r, g], AttributeDefinitions: [pk, g]",
            ),
            (
                ks("g", Some("r")),
                &["pk", "z"][..],
                "Keys: [r, g], AttributeDefinitions: [z, pk]",
            ),
            (
                ks("pk", Some("sk")),
                &["pk", "x"][..],
                "Keys: [sk, pk], AttributeDefinitions: [x, pk]",
            ),
        ] {
            assert_eq!(
                message(validate_keys_defined(&schema, &defs(names))),
                format!(
                    "One or more parameter values were invalid: Some index key attributes are \
                     not defined in AttributeDefinitions. {expected}"
                )
            );
        }
        assert!(validate_keys_defined(&ks("g", Some("r")), &defs(&["r", "g"])).is_ok());
    }

    /// Request order in, the service's order out (measured 2026-11-25). The
    /// last two lists put nine and eight names in one bucket of a 16-bucket
    /// table: the ninth makes the table double, the eighth does not.
    #[test]
    fn lists_follow_the_measured_order() {
        for (request, service) in [
            (
                "alpha beta gamma delta epsilon zeta eta theta",
                "epsilon zeta eta alpha delta theta beta gamma",
            ),
            ("Zed apple Mango kiwi Ünï", "Zed Mango apple kiwi Ünï"),
            ("pk id userId createdAt", "createdAt pk id userId"),
            ("gsiPk gsiSk", "gsiSk gsiPk"),
            ("pk sk g", "sk pk g"),
            ("pk g h", "h pk g"),
            ("pk n b", "n b pk"),
            (
                "kaaa kaaq kabb kabr kacc kacs kadd kadt kaee",
                "kaaa kabb kacc kadd kaee kaaq kabr kacs kadt",
            ),
            (
                "kaaa kaaq kabb kabr kacc kacs kadd kadt",
                "kaaa kaaq kabb kabr kacc kacs kadd kadt",
            ),
        ] {
            assert_eq!(listed(request.split(' ')).join(" "), service, "{request}");
        }
    }
}

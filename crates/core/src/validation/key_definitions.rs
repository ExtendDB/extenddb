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
//!    every definition.
//!
//! An `UpdateTable` GSI `Create` runs the four in that order. `CreateTable`
//! runs the first three on the table's key schema, after the table billing
//! rules and before the key schema shape checks. After every shape check it
//! runs rule 4 on the table's key schema, then rules 1 and 4 on each LSI's and
//! each GSI's in turn. The service checks each index's definitions before that
//! index's shape (measured 2026-11-25), so a request with both faults in one
//! index gets the shape sentence here.
//!
//! The service lists the keys and definitions in an order that follows neither
//! the request nor the alphabet; ExtendDB lists them in request order.

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
    let keys: Vec<&str> = key_schema
        .iter()
        .map(|k| k.attribute_name.as_str())
        .collect();
    let names: Vec<&str> = defs.iter().map(|d| d.attribute_name.as_str()).collect();
    Err(DynamoDbError::ValidationException(format!(
        "One or more parameter values were invalid: Some index key attributes are not defined in \
         AttributeDefinitions. Keys: [{}], AttributeDefinitions: [{}]",
        keys.join(", "),
        names.join(", ")
    )))
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

    #[test]
    fn a_missing_key_lists_every_key_and_every_definition() {
        assert_eq!(
            message(validate_keys_defined(
                &ks("g", Some("r")),
                &defs(&["pk", "g"])
            )),
            "One or more parameter values were invalid: Some index key attributes are not defined \
             in AttributeDefinitions. Keys: [g, r], AttributeDefinitions: [pk, g]"
        );
        // As many definitions as keys, none of them a key.
        assert_eq!(
            message(validate_keys_defined(
                &ks("g", Some("r")),
                &defs(&["pk", "z"])
            )),
            "One or more parameter values were invalid: Some index key attributes are not defined \
             in AttributeDefinitions. Keys: [g, r], AttributeDefinitions: [pk, z]"
        );
        assert!(validate_keys_defined(&ks("g", Some("r")), &defs(&["r", "g"])).is_ok());
    }
}

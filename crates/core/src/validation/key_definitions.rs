// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Checks that a key schema's attributes are defined in `AttributeDefinitions`,
//! shared by `CreateTable` and an `UpdateTable` GSI `Create`.
//!
//! The service's rules, measured on 2026-10-25 and 2026-10-26 (us-east-1):
//!
//! - A name defined twice answers `Attribute Name is duplicated: <name>`,
//!   naming the first name whose second definition comes first in the list.
//!   This comes before every key check.
//! - Each key schema is then checked in turn: the table's, each LSI's, each
//!   GSI's. A schema with more keys than there are definitions answers
//!   `Invalid KeySchema: Some index key attribute have no definition`. A
//!   schema with a key that has no definition answers `Some index key
//!   attributes are not defined in AttributeDefinitions`, listing every key of
//!   that schema and every definition.
//!
//! The service lists the keys and definitions in an order that follows neither
//! the request nor the alphabet; ExtendDB lists them in request order.

use crate::error::DynamoDbError;
use crate::types::{AttributeDefinition, KeySchemaElement};

/// Refuse a name defined twice in `defs`.
///
/// # Errors
///
/// `ValidationException` `Attribute Name is duplicated: <name>`.
pub fn validate_unique_attribute_definitions(
    defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    for (i, def) in defs.iter().enumerate() {
        if defs[..i]
            .iter()
            .any(|earlier| earlier.attribute_name == def.attribute_name)
        {
            return Err(DynamoDbError::ValidationException(format!(
                "Attribute Name is duplicated: {}",
                def.attribute_name
            )));
        }
    }
    Ok(())
}

/// Refuse a key schema whose keys are not all defined in `defs`.
///
/// # Errors
///
/// `ValidationException` with the `Invalid KeySchema` sentence when the schema
/// has more keys than `defs` has entries, otherwise with the `Some index key
/// attributes are not defined` sentence when any key has no definition.
pub fn validate_key_schema_defined(
    key_schema: &[KeySchemaElement],
    defs: &[AttributeDefinition],
) -> Result<(), DynamoDbError> {
    if defs.len() < key_schema.len() {
        return Err(DynamoDbError::ValidationException(
            "Invalid KeySchema: Some index key attribute have no definition".to_owned(),
        ));
    }
    let defined = |name: &str| defs.iter().any(|d| d.attribute_name == name);
    if key_schema.iter().all(|k| defined(&k.attribute_name)) {
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
    use crate::types::{KeyType, ScalarAttributeType};

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

    const INVALID: &str = "Invalid KeySchema: Some index key attribute have no definition";

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
        assert_eq!(
            message(validate_key_schema_defined(
                &ks("g", Some("r")),
                &defs(&["pk"])
            )),
            INVALID
        );
        assert_eq!(
            message(validate_key_schema_defined(&ks("g", None), &[])),
            INVALID
        );
    }

    #[test]
    fn a_missing_key_lists_every_key_and_every_definition() {
        assert_eq!(
            message(validate_key_schema_defined(
                &ks("g", Some("r")),
                &defs(&["pk", "g"])
            )),
            "One or more parameter values were invalid: Some index key attributes are not defined \
             in AttributeDefinitions. Keys: [g, r], AttributeDefinitions: [pk, g]"
        );
        // As many definitions as keys, none of them a key.
        assert_eq!(
            message(validate_key_schema_defined(
                &ks("g", Some("r")),
                &defs(&["pk", "z"])
            )),
            "One or more parameter values were invalid: Some index key attributes are not defined \
             in AttributeDefinitions. Keys: [g, r], AttributeDefinitions: [pk, z]"
        );
        assert!(validate_key_schema_defined(&ks("g", Some("r")), &defs(&["r", "g"])).is_ok());
    }
}

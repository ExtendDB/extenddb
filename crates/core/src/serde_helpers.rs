// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Custom serde deserializers for DynamoDB input validation.

use std::collections::HashMap;
use std::fmt;

use serde::de::{self, Deserializer, MapAccess, Visitor};

use crate::types::AttributeValue;

/// Deserialize `ExpressionAttributeNames` with key prefix validation.
/// Keys must start with `#` and the map must not be empty.
pub fn deserialize_expression_names<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct NamesVisitor;

    impl<'de> Visitor<'de> for NamesVisitor {
        type Value = Option<HashMap<String, String>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a map of expression attribute names")
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            let map: HashMap<String, String> = de::Deserialize::deserialize(d)?;
            if map.is_empty() {
                return Err(de::Error::custom(
                    "ExpressionAttributeNames must not be empty",
                ));
            }
            for key in map.keys() {
                if !key.starts_with('#') {
                    return Err(de::Error::custom(format!(
                        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"{key}\""
                    )));
                }
            }
            Ok(Some(map))
        }

        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            let m: HashMap<String, String> =
                de::Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))?;
            if m.is_empty() {
                return Err(de::Error::custom(
                    "ExpressionAttributeNames must not be empty",
                ));
            }
            for key in m.keys() {
                if !key.starts_with('#') {
                    return Err(de::Error::custom(format!(
                        "ExpressionAttributeNames contains invalid key: Syntax error; key: \"{key}\""
                    )));
                }
            }
            Ok(Some(m))
        }
    }

    deserializer.deserialize_option(NamesVisitor)
}

/// Deserialize `ExpressionAttributeValues` with key prefix validation.
/// Keys must start with `:` and the map must not be empty.
pub fn deserialize_expression_values<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, AttributeValue>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ValuesVisitor;

    impl<'de> Visitor<'de> for ValuesVisitor {
        type Value = Option<HashMap<String, AttributeValue>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a map of expression attribute values")
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
            let map: HashMap<String, AttributeValue> = de::Deserialize::deserialize(d)?;
            if map.is_empty() {
                return Err(de::Error::custom(
                    "ExpressionAttributeValues must not be empty",
                ));
            }
            for key in map.keys() {
                if !key.starts_with(':') {
                    return Err(de::Error::custom(format!(
                        "ExpressionAttributeValues contains invalid key: Syntax error; key: \"{key}\""
                    )));
                }
            }
            Ok(Some(map))
        }

        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            let m: HashMap<String, AttributeValue> =
                de::Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))?;
            if m.is_empty() {
                return Err(de::Error::custom(
                    "ExpressionAttributeValues must not be empty",
                ));
            }
            for key in m.keys() {
                if !key.starts_with(':') {
                    return Err(de::Error::custom(format!(
                        "ExpressionAttributeValues contains invalid key: Syntax error; key: \"{key}\""
                    )));
                }
            }
            Ok(Some(m))
        }
    }

    deserializer.deserialize_option(ValuesVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::HashMap;

    #[derive(Debug, Deserialize)]
    struct TestNames {
        #[serde(default, deserialize_with = "deserialize_expression_names")]
        names: Option<HashMap<String, String>>,
    }

    #[derive(Debug, Deserialize)]
    struct TestValues {
        #[serde(default, deserialize_with = "deserialize_expression_values")]
        values: Option<HashMap<String, AttributeValue>>,
    }

    #[test]
    fn names_missing_hash_rejected() {
        let json = r#"{"names":{"a":"real"}}"#;
        let result: Result<TestNames, _> = serde_json::from_str(json);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Syntax error; key")
        );
    }

    #[test]
    fn names_with_hash_accepted() {
        let json = "{\"names\":{\"#a\":\"real\"}}";
        let result: Result<TestNames, _> = serde_json::from_str(json);
        assert!(result.is_ok());
        assert!(result.unwrap().names.unwrap().contains_key("#a"));
    }

    #[test]
    fn names_empty_rejected() {
        let json = r#"{"names":{}}"#;
        let result: Result<TestNames, _> = serde_json::from_str(json);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must not be empty")
        );
    }

    #[test]
    fn values_missing_colon_rejected() {
        let json = r#"{"values":{"v":{"S":"x"}}}"#;
        let result: Result<TestValues, _> = serde_json::from_str(json);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Syntax error; key")
        );
    }

    #[test]
    fn values_with_colon_accepted() {
        let json = r#"{"values":{":v":{"S":"x"}}}"#;
        let result: Result<TestValues, _> = serde_json::from_str(json);
        let parsed = result.unwrap();
        assert!(parsed.values.is_some());
    }
}

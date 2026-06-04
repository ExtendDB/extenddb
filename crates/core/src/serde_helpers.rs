// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Custom serde deserializers for DynamoDB input validation.

use std::collections::HashMap;
use std::fmt;

use serde::de::{self, Deserializer, MapAccess, Visitor};

use crate::types::AttributeValue;

macro_rules! prefixed_map_deserializer {
    ($fn_name:ident, $value_type:ty, $prefix:expr, $field_name:expr, $expecting:expr) => {
        pub fn $fn_name<'de, D>(
            deserializer: D,
        ) -> Result<Option<HashMap<String, $value_type>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct V;

            fn check_keys(map: &HashMap<String, $value_type>) -> Result<(), String> {
                if map.is_empty() {
                    return Err(concat!($field_name, " must not be empty").to_owned());
                }
                for key in map.keys() {
                    if !key.starts_with($prefix) {
                        return Err(format!(
                            concat!(
                                $field_name,
                                " contains invalid key: Syntax error; key: \"{}\""
                            ),
                            key
                        ));
                    }
                }
                Ok(())
            }

            impl<'de> Visitor<'de> for V {
                type Value = Option<HashMap<String, $value_type>>;

                fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str($expecting)
                }

                fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                    Ok(None)
                }

                fn visit_some<D2: Deserializer<'de>>(
                    self,
                    d: D2,
                ) -> Result<Self::Value, D2::Error> {
                    let map: HashMap<String, $value_type> = de::Deserialize::deserialize(d)?;
                    check_keys(&map).map_err(de::Error::custom)?;
                    Ok(Some(map))
                }

                fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                    let m: HashMap<String, $value_type> =
                        de::Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))?;
                    check_keys(&m).map_err(de::Error::custom)?;
                    Ok(Some(m))
                }
            }

            deserializer.deserialize_option(V)
        }
    };
}

// Deserialize `ExpressionAttributeNames` — keys must start with `#`, map must not be empty.
prefixed_map_deserializer!(
    deserialize_expression_names,
    String,
    '#',
    "ExpressionAttributeNames",
    "a map of expression attribute names"
);

// Deserialize `ExpressionAttributeValues` — keys must start with `:`, map must not be empty.
prefixed_map_deserializer!(
    deserialize_expression_values,
    AttributeValue,
    ':',
    "ExpressionAttributeValues",
    "a map of expression attribute values"
);

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

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

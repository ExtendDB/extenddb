// SPDX-License-Identifier: Apache-2.0

//! The tag condition keys a `DynamoDB` request populates.
//!
//! Three operations carry a tag set, and the AWS service authorization
//! reference (`dynamodb` v1.5) lists the condition keys each one populates:
//!
//! | operation | `aws:RequestTag/${TagKey}` | `aws:TagKeys` |
//! |---|---|---|
//! | `TagResource` | yes, from `Tags` | yes, from the `Tags` keys |
//! | `CreateTable` | yes, from `Tags` | yes, from the `Tags` keys |
//! | `UntagResource` | no | yes, from `TagKeys` |
//!
//! `UntagResource` names keys and no values, so it populates `aws:TagKeys`
//! alone; a statement on `aws:RequestTag/<key>` reads the key as absent for it.
//! Every other operation populates neither, including `ListTagsOfResource`,
//! which the reference gives no tag condition key.

use std::collections::HashMap;

use serde_json::Value;

/// The tag condition keys `operation` populates from `input`.
///
/// Returns `(request_tags, request_tag_keys)`: the tags for
/// `aws:RequestTag/<key>` and the tag keys for `aws:TagKeys`. Either is `None`
/// when the operation does not populate that key, which leaves it absent in
/// policy evaluation.
///
/// A duplicated tag key takes the LAST value in the request, which is the value
/// the request would store: every backend's `tag_resource` walks the tag list
/// in request order and upserts each key, so the last occurrence wins. Reading
/// it the same way keeps authorization and the engine on one value.
///
/// `Tags` is read as the engine reads it, as a `Vec<Tag>`. serde accepts a tag
/// as an object (`{"Key": "k", "Value": "v"}`) and as a two-element array
/// (`["k", "v"]`), and the engine stores both, so both populate the keys. A
/// `Tags` value that does not parse as `Vec<Tag>` (an entry without a `Key` or
/// a `Value`, a non-string member, a wrong-length array) populates neither key:
/// the engine answers it with a `SerializationException` before it writes
/// anything.
pub(crate) fn tag_condition_keys(
    operation: &str,
    input: &Value,
) -> (Option<HashMap<String, String>>, Option<Vec<String>>) {
    match operation {
        "TagResource" | "CreateTable" => {
            let Some(raw) = input.get("Tags") else {
                return (None, None);
            };
            // The type the engine deserializes `Tags` into, so both readers
            // accept the same shapes. Reading through the reference leaves the
            // array in `input` uncopied.
            let Ok(tags) = <Vec<extenddb_core::types::Tag> as serde::Deserialize>::deserialize(raw)
            else {
                return (None, None);
            };
            let mut values: HashMap<String, String> = HashMap::with_capacity(tags.len());
            let mut keys: Vec<String> = Vec::with_capacity(tags.len());
            for tag in tags {
                keys.push(tag.key.clone());
                values.insert(tag.key, tag.value);
            }
            (Some(values), Some(keys))
        }
        "UntagResource" => {
            let Some(list) = input.get("TagKeys").and_then(Value::as_array) else {
                return (None, None);
            };
            let keys: Vec<String> = list
                .iter()
                .filter_map(|k| k.as_str().map(ToOwned::to_owned))
                .collect();
            (None, Some(keys))
        }
        _ => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tag_resource_populates_both_keys() {
        let (tags, keys) = tag_condition_keys(
            "TagResource",
            &json!({"ResourceArn": "arn", "Tags": [{"Key": "env", "Value": "prod"}]}),
        );
        assert_eq!(
            tags.expect("TagResource carries tag values").get("env"),
            Some(&"prod".to_owned())
        );
        assert_eq!(keys.expect("TagResource names tag keys"), vec!["env"]);
    }

    #[test]
    fn create_table_populates_both_keys() {
        let (tags, keys) = tag_condition_keys(
            "CreateTable",
            &json!({"TableName": "t", "Tags": [{"Key": "env", "Value": "prod"}]}),
        );
        assert_eq!(
            tags.expect("CreateTable with Tags carries tag values")
                .get("env"),
            Some(&"prod".to_owned())
        );
        assert_eq!(keys.expect("CreateTable with Tags names keys"), vec!["env"]);
    }

    #[test]
    fn create_table_without_tags_populates_neither_key() {
        let out = tag_condition_keys("CreateTable", &json!({"TableName": "t"}));
        assert!(
            out.0.is_none() && out.1.is_none(),
            "CreateTable with no Tags leaves both keys absent"
        );
    }

    #[test]
    fn untag_resource_populates_tag_keys_only() {
        let (tags, keys) = tag_condition_keys(
            "UntagResource",
            &json!({"ResourceArn": "arn", "TagKeys": ["env", "owner"]}),
        );
        assert!(
            tags.is_none(),
            "UntagResource names no tag value, so aws:RequestTag stays absent"
        );
        assert_eq!(
            keys.expect("UntagResource names keys"),
            vec!["env", "owner"]
        );
    }

    #[test]
    fn list_tags_of_resource_populates_neither_key() {
        let out = tag_condition_keys("ListTagsOfResource", &json!({"ResourceArn": "arn"}));
        assert!(out.0.is_none() && out.1.is_none());
    }

    #[test]
    fn unrelated_operation_populates_neither_key() {
        let out = tag_condition_keys(
            "PutItem",
            &json!({"TableName": "t", "Tags": [{"Key": "env", "Value": "prod"}]}),
        );
        assert!(
            out.0.is_none() && out.1.is_none(),
            "an operation the reference gives no tag condition key populates neither, \
             whatever the body carries"
        );
    }

    #[test]
    fn duplicate_tag_key_takes_the_last_value_and_keeps_both_keys() {
        let (tags, keys) = tag_condition_keys(
            "TagResource",
            &json!({"Tags": [{"Key": "env", "Value": "dev"}, {"Key": "env", "Value": "prod"}]}),
        );
        assert_eq!(
            tags.expect("tags present").get("env"),
            Some(&"prod".to_owned()),
            "the last occurrence is the value the upsert stores"
        );
        assert_eq!(
            keys.expect("keys present"),
            vec!["env", "env"],
            "aws:TagKeys keeps the keys the request named"
        );
    }

    #[test]
    fn case_variant_keys_are_separate_entries() {
        let (tags, keys) = tag_condition_keys(
            "TagResource",
            &json!({"Tags": [{"Key": "Env", "Value": "a"}, {"Key": "env", "Value": "b"}]}),
        );
        let tags = tags.expect("tags present");
        assert_eq!(tags.get("Env"), Some(&"a".to_owned()));
        assert_eq!(tags.get("env"), Some(&"b".to_owned()));
        assert_eq!(keys.expect("keys present"), vec!["Env", "env"]);
    }

    #[test]
    fn empty_tag_value_is_kept() {
        let (tags, _) = tag_condition_keys(
            "TagResource",
            &json!({"Tags": [{"Key": "env", "Value": ""}]}),
        );
        assert_eq!(
            tags.expect("tags present").get("env"),
            Some(&String::new()),
            "the empty tag value is a value, not an absent key"
        );
    }

    #[test]
    fn array_form_tag_populates_both_keys() {
        // serde reads a two-element array as a Tag, and the engine stores it, so
        // authorization has to read it too. A Deny on either key must apply.
        for operation in ["TagResource", "CreateTable"] {
            let (tags, keys) = tag_condition_keys(operation, &json!({"Tags": [["secret", "x"]]}));
            assert_eq!(
                tags.expect("an array-form tag carries a value")
                    .get("secret"),
                Some(&"x".to_owned()),
                "{operation}: aws:RequestTag/secret reads the array-form value"
            );
            assert_eq!(
                keys.expect("an array-form tag names a key"),
                vec!["secret"],
                "{operation}: aws:TagKeys lists the array-form key"
            );
        }
    }

    #[test]
    fn array_and_object_entries_in_one_list_both_count() {
        let (tags, keys) = tag_condition_keys(
            "TagResource",
            &json!({"Tags": [{"Key": "colour", "Value": "red"}, ["secret", "x"]]}),
        );
        let tags = tags.expect("tags present");
        assert_eq!(tags.get("colour"), Some(&"red".to_owned()));
        assert_eq!(tags.get("secret"), Some(&"x".to_owned()));
        assert_eq!(keys.expect("keys present"), vec!["colour", "secret"]);
    }

    #[test]
    fn malformed_tag_entry_populates_neither_key() {
        // The engine answers each of these with a SerializationException before
        // it writes anything, so there is no tag for a condition to read. The
        // valid entry beside the malformed one does not populate the keys either:
        // the whole list is refused.
        let valid = json!({"Key": "env", "Value": "prod"});
        for bad in [
            json!({"Value": "prod"}),
            json!({"Key": "env"}),
            json!({"Key": "env", "Value": null}),
            json!({"Key": 7, "Value": "prod"}),
            json!({"Key": "env", "Value": 7}),
            json!(["env"]),
            json!(["env", "prod", "extra"]),
            json!("env"),
            json!(null),
        ] {
            for operation in ["TagResource", "CreateTable"] {
                let out =
                    tag_condition_keys(operation, &json!({"Tags": [valid.clone(), bad.clone()]}));
                assert!(
                    out.0.is_none() && out.1.is_none(),
                    "{operation} with {bad} populated a key"
                );
            }
        }
    }

    #[test]
    fn empty_tag_list_yields_an_empty_set() {
        // RequestContext::build turns an empty set into an absent key; this
        // function reports what the request carried.
        let (tags, keys) = tag_condition_keys("TagResource", &json!({"Tags": []}));
        assert!(tags.expect("an empty object, not None").is_empty());
        assert!(keys.expect("an empty list, not None").is_empty());
    }

    #[test]
    fn non_array_tags_field_populates_neither_key() {
        let out = tag_condition_keys("TagResource", &json!({"Tags": "env=prod"}));
        assert!(out.0.is_none() && out.1.is_none());
    }

    #[test]
    fn non_string_tag_key_in_untag_resource_is_skipped() {
        let (_, keys) = tag_condition_keys("UntagResource", &json!({"TagKeys": [7, "env"]}));
        assert_eq!(keys.expect("keys present"), vec!["env"]);
    }

    /// For a body the engine stores, authorization reads the same tags in the
    /// same order, whichever shape the body uses. A `Tags` value the engine
    /// refuses populates neither key.
    #[test]
    fn authorization_reads_the_tags_the_engine_stores() {
        use extenddb_core::types::{CreateTableInput, Tag, TagResourceInput};

        let check =
            |operation: &str, body: &Value, stored: Result<Option<Vec<Tag>>, serde_json::Error>| {
                let (values, keys) = tag_condition_keys(operation, body);
                match stored {
                    Ok(Some(tags)) => {
                        let named: Vec<String> = tags.iter().map(|t| t.key.clone()).collect();
                        let last: HashMap<String, String> =
                            tags.into_iter().map(|t| (t.key, t.value)).collect();
                        assert_eq!(keys, Some(named), "{operation} keys for {body}");
                        assert_eq!(values, Some(last), "{operation} values for {body}");
                    }
                    Ok(None) | Err(_) => assert!(
                        values.is_none() && keys.is_none(),
                        "{operation} populated a key for {body}"
                    ),
                }
            };
        let create_table = json!({
            "TableName": "t",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        });
        for tags in [
            json!([{"Key": "a", "Value": "1"}]),
            json!([["a", "1"]]),
            json!([{"Key": "a", "Value": "1"}, ["b", "2"], ["a", "3"]]),
            json!([{"Key": "a", "Value": "1", "Extra": true}]),
            json!([]),
            json!([{"Key": "a"}]),
            json!([["a"]]),
            json!([["a", "1", "2"]]),
            json!(["a"]),
            json!("a"),
            json!(null),
        ] {
            let body = json!({"ResourceArn": "arn", "Tags": tags});
            let stored = serde_json::from_value::<TagResourceInput>(body.clone());
            check("TagResource", &body, stored.map(|input| Some(input.tags)));

            let mut body = create_table.clone();
            body["Tags"] = tags;
            let stored = serde_json::from_value::<CreateTableInput>(body.clone());
            check("CreateTable", &body, stored.map(|input| input.tags));
        }
    }

    /// `UntagResource` names keys only. For a body the engine accepts,
    /// authorization reads the same keys. For a body it refuses, which removes
    /// nothing, authorization still reads the string elements, so a neighbour
    /// that is not a string cannot hide a key from a Deny.
    #[test]
    fn authorization_reads_the_keys_the_engine_removes() {
        use extenddb_core::types::UntagResourceInput;

        for tag_keys in [
            json!(["a", "b"]),
            json!([]),
            json!(["a", 7]),
            json!([null, "a"]),
            json!([["a"]]),
            json!("a"),
            json!(null),
        ] {
            let body = json!({"ResourceArn": "arn", "TagKeys": tag_keys});
            let (_, keys) = tag_condition_keys("UntagResource", &body);
            let named: Vec<String> =
                match serde_json::from_value::<UntagResourceInput>(body.clone()) {
                    Ok(input) => input.tag_keys,
                    Err(_) => tag_keys
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|k| k.as_str().map(ToOwned::to_owned))
                        .collect(),
                };
            assert_eq!(
                keys.unwrap_or_default(),
                named,
                "UntagResource keys for {body}"
            );
        }
    }

    #[test]
    fn aws_prefixed_and_long_tag_keys_are_carried_verbatim() {
        let long = "k".repeat(128);
        let (tags, keys) = tag_condition_keys(
            "TagResource",
            &json!({"Tags": [
                {"Key": "aws:cloudformation:stack-name", "Value": "s"},
                {"Key": long, "Value": "v"}
            ]}),
        );
        let tags = tags.expect("tags present");
        assert_eq!(
            tags.get("aws:cloudformation:stack-name"),
            Some(&"s".to_owned())
        );
        assert_eq!(tags.get(&long), Some(&"v".to_owned()));
        assert_eq!(keys.expect("keys present").len(), 2);
    }
}

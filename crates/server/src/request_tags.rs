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
/// A tag entry with no string `Key` contributes nothing. The engine rejects
/// such a body with `ValidationException` after authorization, so nothing is
/// written for the entry either way.
pub(crate) fn tag_condition_keys(
    operation: &str,
    input: &Value,
) -> (Option<HashMap<String, String>>, Option<Vec<String>>) {
    match operation {
        "TagResource" | "CreateTable" => {
            let Some(tags) = input.get("Tags").and_then(Value::as_array) else {
                return (None, None);
            };
            let mut values: HashMap<String, String> = HashMap::with_capacity(tags.len());
            let mut keys: Vec<String> = Vec::with_capacity(tags.len());
            for tag in tags {
                let Some(key) = tag.get("Key").and_then(Value::as_str) else {
                    continue;
                };
                // A missing or non-string Value is read as the empty string:
                // the empty tag value is one the service accepts, and a body
                // that carries a non-string there is rejected by the engine
                // after authorization.
                let value = tag.get("Value").and_then(Value::as_str).unwrap_or_default();
                keys.push(key.to_owned());
                values.insert(key.to_owned(), value.to_owned());
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

    fn keys_of(out: &(Option<HashMap<String, String>>, Option<Vec<String>>)) -> Vec<String> {
        out.1.clone().unwrap_or_default()
    }

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
    fn missing_value_field_reads_as_empty() {
        let (tags, keys) = tag_condition_keys("TagResource", &json!({"Tags": [{"Key": "env"}]}));
        assert_eq!(tags.expect("tags present").get("env"), Some(&String::new()));
        assert_eq!(keys.expect("keys present"), vec!["env"]);
    }

    #[test]
    fn entry_without_a_key_contributes_nothing() {
        let out = tag_condition_keys(
            "TagResource",
            &json!({"Tags": [{"Value": "prod"}, {"Key": "env", "Value": "prod"}]}),
        );
        assert_eq!(keys_of(&out), vec!["env"]);
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

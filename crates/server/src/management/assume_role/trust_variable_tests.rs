// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A trust policy write refuses a condition value with a `${...}` reference IAM refuses
//! in the place IAM refuses it (issue #834). Measured with
//! `UpdateAssumeRolePolicy` and `CreateRole` on 2026-11-09, two samples each.

use serde_json::{Value, json};

use super::validate_trust_policy;
use extenddb_auth::policy::variable_syntax::TRUST_INVALID_VARIABLES;

const ARN_TYPE: &str = "value used for ARN condition is incorrect type";

/// One statement: an `Allow sts:AssumeRole` from the account root holding
/// `condition`, with the fields of `changes` set (or removed when null).
fn trust(version: Option<&str>, condition: &Value, changes: &Value) -> Value {
    let mut stmt = json!({
        "Effect": "Allow",
        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
        "Action": "sts:AssumeRole",
        "Condition": condition,
    });
    let obj = stmt.as_object_mut().expect("object");
    for (k, v) in changes.as_object().expect("changes object") {
        if v.is_null() {
            obj.remove(k);
        } else {
            obj.insert(k.clone(), v.clone());
        }
    }
    let mut doc = json!({"Statement": [stmt]});
    if let Some(v) = version {
        doc["Version"] = json!(v);
    }
    doc
}

fn error(doc: &Value) -> String {
    validate_trust_policy(doc).expect_err("refused")
}

fn string_value() -> Value {
    json!({"StringEquals": {"sts:ExternalId": "a${"}})
}

fn arn_value() -> Value {
    json!({"ArnLike": {"aws:PrincipalArn": "arn:aws:iam::123456789012:user/a${"}})
}

/// Faults IAM reports before the variable error, under every Version.
fn structural_faults() -> Vec<(Value, &'static str)> {
    vec![
        (json!({"Effect": "Maybe"}), "invalid Effect: Maybe"),
        (
            json!({"Resource": "*"}),
            "Resource is not allowed in a trust policy",
        ),
        (json!({"Action": null}), "statement is missing Action"),
        (
            json!({"NotAction": "sts:TagSession"}),
            "statement has both Action and NotAction",
        ),
        (json!({"Principal": null}), "statement is missing Principal"),
    ]
}

#[test]
fn version_2012_refuses_after_structural_faults() {
    let v = Some("2012-10-17");
    assert!(error(&trust(v, &string_value(), &json!({}))).starts_with(TRUST_INVALID_VARIABLES));
    for (change, expected) in structural_faults() {
        assert_eq!(
            error(&trust(v, &string_value(), &change)),
            expected,
            "{change}"
        );
    }
    let mut cond = string_value();
    cond["Bogus"] = json!({"k": "v"});
    assert_eq!(
        error(&trust(v, &cond, &json!({}))),
        "invalid Condition operator: Bogus"
    );
}

#[test]
fn version_2012_refuses_before_sid_principal_and_action_values() {
    let v = Some("2012-10-17");
    for change in [
        json!({"Sid": "bad-sid"}),
        json!({"Action": "sts:Nope"}),
        json!({"Principal": "*"}),
        json!({"Principal": {"AWS": "arn:aws:iam::123456789012:user/*"}}),
        json!({"Principal": null, "NotPrincipal": {"AWS": "arn:aws:iam::123456789012:root"}}),
    ] {
        let e = error(&trust(v, &string_value(), &change));
        assert!(e.starts_with(TRUST_INVALID_VARIABLES), "{change}: {e}");
    }
}

#[test]
fn older_versions_store_string_values() {
    for version in [Some("2008-10-17"), None] {
        assert!(validate_trust_policy(&trust(version, &string_value(), &json!({}))).is_ok());
    }
}

#[test]
fn older_versions_refuse_arn_values_after_structure_not_principal_and_sid() {
    for version in [Some("2008-10-17"), None] {
        assert!(error(&trust(version, &arn_value(), &json!({}))).contains(ARN_TYPE));
        for (change, expected) in structural_faults() {
            assert_eq!(
                error(&trust(version, &arn_value(), &change)),
                expected,
                "{change}"
            );
        }
        let not_principal =
            json!({"Principal": null, "NotPrincipal": {"AWS": "arn:aws:iam::123456789012:root"}});
        assert_eq!(
            error(&trust(version, &arn_value(), &not_principal)),
            "NotPrincipal is not allowed in a trust policy"
        );
        assert!(error(&trust(version, &arn_value(), &json!({"Sid": "bad-sid"}))).contains("Sid"));
        for change in [
            json!({"Action": "sts:Nope"}),
            json!({"Principal": "*"}),
            json!({"Principal": {"AWS": "arn:aws:iam::123456789012:user/*"}}),
        ] {
            let e = error(&trust(version, &arn_value(), &change));
            assert!(e.contains(ARN_TYPE), "{change}: {e}");
        }
    }
}

/// `NotResource` is not one of the checks IAM runs first, and a structural
/// fault in another statement still comes first.
#[test]
fn not_resource_comes_after_and_other_statements_count() {
    for (version, cond, refused) in [
        (Some("2012-10-17"), string_value(), TRUST_INVALID_VARIABLES),
        (Some("2008-10-17"), arn_value(), ARN_TYPE),
        (None, arn_value(), ARN_TYPE),
    ] {
        let e = error(&trust(version, &cond, &json!({"NotResource": "*"})));
        assert!(e.contains(refused), "{version:?}: {e}");

        let mut doc = trust(version, &cond, &json!({}));
        let second = json!({
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
            "Action": "sts:AssumeRole",
            "Resource": "*",
        });
        doc["Statement"].as_array_mut().expect("array").push(second);
        assert_eq!(
            error(&doc),
            "Resource is not allowed in a trust policy",
            "{version:?}"
        );
    }
}

/// Under `2008-10-17` and with no `Version`, the structural checks of every
/// statement run before the `NotPrincipal` check of any statement.
#[test]
fn older_versions_check_every_statement_structure_before_not_principal() {
    for version in [Some("2008-10-17"), None] {
        let not_principal =
            json!({"Principal": null, "NotPrincipal": {"AWS": "arn:aws:iam::123456789012:root"}});
        let mut doc = trust(version, &arn_value(), &not_principal);
        let second = json!({
            "Effect": "Maybe",
            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
            "Action": "sts:AssumeRole",
        });
        doc["Statement"].as_array_mut().expect("array").push(second);
        assert_eq!(error(&doc), "invalid Effect: Maybe", "{version:?}");
    }
}

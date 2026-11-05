// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scope checks for stream ARNs (issues #450 and #451): a `StreamArn` must be a
//! stream ARN whose partition, service and region are this deployment's. The
//! account of a `StreamArn`, and of a stream ARN in a tag operation's
//! `ResourceArn`, is checked by the cross-account refusal
//! ([`super::cross_account`]); a `GetRecords` iterator's stream ARN is held to
//! the caller's account here ([`stream_arn_in_scope`]). See the parent module
//! docs for where each check runs relative to the IAM decision.

use extenddb_core::error::DynamoDbError;

use super::{INVALID_STREAM_ARN_MESSAGE, ResourceId, Sub, parse_resource_identity};

fn invalid_stream_arn() -> DynamoDbError {
    DynamoDbError::ValidationException(INVALID_STREAM_ARN_MESSAGE.to_owned())
}

/// Refuse a `StreamArn` that is not a stream ARN, or whose partition, service
/// or account field is empty, with `Invalid StreamArn`.
///
/// The service gives this answer to every principal before it looks at the
/// account: in the caller's account, in an account that exists and in one
/// that does not. Measured live for a table, index or backup ARN (2026-11-05),
/// an empty partition or service field (2026-10-28 and 2026-11-05), and an
/// empty account field, an extra or missing field and a trailing slash
/// (2026-10-23), on DescribeStream and GetShardIterator; the tables are in the
/// module docstring of `tests/test_stream_arn_scope.py`. So
/// [`super::validate_stream_arn_kind`] runs this before the cross-account
/// refusal, and [`check_stream_arn_scope`] runs it before comparing the
/// partition, service and region.
pub(super) fn check_stream_arn_kind(arn: &str) -> Result<(), DynamoDbError> {
    let is_stream = matches!(
        parse_resource_identity(arn),
        Some(ResourceId {
            sub: Some(Sub::Stream(_)),
            ..
        })
    );
    match extenddb_core::arn::fields(arn) {
        Some(f)
            if is_stream
                && !f.partition.is_empty()
                && !f.service.is_empty()
                && !f.account.is_empty() =>
        {
            Ok(())
        }
        _ => Err(invalid_stream_arn()),
    }
}

/// Refuse a `StreamArn` that fails [`check_stream_arn_kind`], or whose
/// partition, service or region is not this deployment's, with
/// `Invalid StreamArn`: a partition other than `aws` (`aws-cn`, `aws-us-gov`),
/// a service other than `dynamodb` (`DynamoDB`, `DYNAMODB`, `s3`), or a region
/// other than this server's (empty, `us-west-2`, `US-EAST-1`). This is the rule
/// the engine applies to a tag operation's `ResourceArn`, through the one
/// predicate [`extenddb_core::arn::ArnFields::in_deployment`].
///
/// The account is not compared here. The service gives this answer before the
/// IAM decision in the caller's account (measured live 2026-10-23, identical
/// for an admin, a no-permissions user, a user allowed the exact stream ARN and
/// a user with a Deny on it) and in another account that exists (measured
/// 2026-11-05, an admin and a no-policy caller). So it runs for the caller's
/// account in [`super::validate_operation_arns`] and for an account the catalog
/// holds in the cross-account answer. An account the catalog does not hold is
/// "Access is denied" whatever the partition, service and region are.
///
/// The stream engine resolves a stream by table name and label within the
/// caller's account, so a stream ARN in the caller's account that passes here
/// names exactly the stream the engine reads, and authorization evaluates that
/// same ARN. Before this check existed, a respelled ARN was canonicalized for
/// authorization and then served as the caller's own stream (issues #450 and
/// #451).
pub(super) fn check_stream_arn_scope(arn: &str, region: &str) -> Result<(), DynamoDbError> {
    check_stream_arn_kind(arn)?;
    match extenddb_core::arn::fields(arn) {
        Some(f) if f.in_deployment(region) => Ok(()),
        _ => Err(invalid_stream_arn()),
    }
}

/// True when every field of this stream ARN is this deployment's and the
/// caller's. GetShardIterator refuses a `StreamArn` for which this is false,
/// so a shard iterator carrying one was hand-built, or issued by an earlier
/// build or under another `server.region` ([`super::getrecords_resource`]).
/// A `GetRecords` request names its stream only inside the iterator, which the
/// cross-account refusal does not read, so the account is compared here.
pub(super) fn stream_arn_in_scope(arn: &str, region: &str, account_id: &str) -> bool {
    check_stream_arn_scope(arn, region).is_ok()
        && extenddb_core::arn::fields(arn).is_some_and(|f| f.account == account_id)
}

#[cfg(test)]
mod tests {
    use super::super::{
        GetRecordsResource, authz_targets, getrecords_resource, validate_operation_arns,
        validate_stream_arn_kind, validate_tag_resource_arn_shape,
    };
    use super::*;
    use serde_json::{Value, json};

    const R: &str = "us-east-1";
    const A: &str = "444455556666";
    const OTHER: &str = "111111111111";

    const STREAM: &str =
        "arn:aws:dynamodb:us-east-1:444455556666:table/T/stream/2026-10-20T16:17:40.188";

    const STREAM_OPS: [&str; 2] = ["DescribeStream", "GetShardIterator"];

    /// The resource a single-target operation authorizes, and the ARN its
    /// policies are matched against.
    fn target(op: &str, input: &Value) -> (String, String) {
        let t = authz_targets(op, input, R, A);
        assert_eq!(t.len(), 1, "expected one target for {op}: {t:?}");
        (t[0].resource.clone(), t[0].policy_resource().to_owned())
    }

    fn stream_input(arn: &str) -> Value {
        json!({ "StreamArn": arn })
    }

    /// Names the result's variant for a failure message.
    fn variant(r: &Result<(), DynamoDbError>) -> &'static str {
        match r {
            Ok(()) => "Ok",
            Err(DynamoDbError::ValidationException(_)) => "ValidationException",
            Err(DynamoDbError::AccessDeniedException(_)) => "AccessDeniedException",
            Err(_) => "another error",
        }
    }

    fn assert_invalid_stream_arn(r: Result<(), DynamoDbError>, what: &str, arn: &str) {
        match r {
            Err(DynamoDbError::ValidationException(m)) => {
                assert_eq!(m, "Invalid StreamArn", "{what} {arn}");
            }
            other => panic!(
                "{what} {arn}: expected Invalid StreamArn, got {}",
                variant(&other)
            ),
        }
    }

    fn respellings() -> [String; 8] {
        [
            STREAM.replace("arn:aws:", "arn:aws-cn:"),
            STREAM.replace("arn:aws:", "arn:aws-us-gov:"),
            STREAM.replace(":dynamodb:", ":DYNAMODB:"),
            STREAM.replace(":dynamodb:", ":DynamoDB:"),
            STREAM.replace(":dynamodb:", ":s3:"),
            STREAM.replace("us-east-1", "us-west-2"),
            STREAM.replace("us-east-1", "US-EAST-1"),
            STREAM.replace("us-east-1", ""),
        ]
    }

    /// A `StreamArn` that is not a stream ARN (a table, index or backup ARN
    /// included), or has an empty partition, service or account field, is
    /// "Invalid StreamArn" in the caller's account and in another, before the
    /// account is looked at (measured 2026-10-23, 2026-10-28 and 2026-11-05).
    #[test]
    fn stream_arn_kind_is_refused_in_every_account() {
        let table = STREAM.split("/stream/").next().unwrap_or_default();
        for account in [A, OTHER] {
            let mut arns: Vec<String> = [
                table.to_owned(),
                format!("{table}/index/gsi1"),
                format!("{table}/backup/01791976431438-73d8d5bc"),
                STREAM.replace("arn:aws:", "arn::"),
                STREAM.replace(":dynamodb:", "::"),
                STREAM.replace(":444455556666:", ":444455556666:x:"),
                STREAM.replace(":us-east-1:", ":"),
                format!("{STREAM}/"),
                STREAM.replace("arn:", "xrn:"),
            ]
            .iter()
            .map(|arn| arn.replace(A, account))
            .collect();
            arns.push(STREAM.replace(A, ""));
            for arn in &arns {
                for op in STREAM_OPS {
                    assert_invalid_stream_arn(
                        validate_stream_arn_kind(op, &stream_input(arn)),
                        op,
                        arn,
                    );
                    assert_invalid_stream_arn(
                        validate_operation_arns(op, &stream_input(arn), R),
                        op,
                        arn,
                    );
                }
            }
        }
    }

    /// A stream ARN with non-empty partition, service and account fields
    /// passes the kind step whatever those fields and the region hold: the
    /// cross-account refusal checks the account, and
    /// [`check_stream_arn_scope`] checks the partition, service and region.
    #[test]
    fn a_stream_arn_passes_the_kind_step_in_any_account_and_spelling() {
        let mut arns = respellings().to_vec();
        arns.extend([
            STREAM.to_owned(),
            STREAM.replace(A, OTHER),
            STREAM.replace(A, "abc"),
            STREAM.replace("arn:aws:", "arn:aws-cn:").replace(A, OTHER),
        ]);
        for arn in &arns {
            for op in STREAM_OPS {
                assert!(
                    validate_stream_arn_kind(op, &stream_input(arn)).is_ok(),
                    "{op} {arn}"
                );
            }
        }
        // Only the stream operations take this step; an absent or empty field
        // is left to the normal path.
        assert!(validate_stream_arn_kind("DescribeTable", &stream_input("x")).is_ok());
        assert!(validate_stream_arn_kind("DescribeStream", &json!({})).is_ok());
        assert!(validate_stream_arn_kind("DescribeStream", &stream_input("")).is_ok());
    }

    /// In the caller's account the service answers another partition, region
    /// or service spelling of a `StreamArn` with ValidationException "Invalid
    /// StreamArn" before the IAM decision (measured 2026-10-23 for aws-cn,
    /// aws-us-gov, DynamoDB, DYNAMODB, us-west-2 and US-EAST-1, and 2026-10-28
    /// for an empty region; `s3` was measured on 2026-11-05, in an account that
    /// exists). On a tag `ResourceArn` the same spellings pass the
    /// pre-authorization check and are authorized as sent; the engine refuses
    /// them after the IAM decision.
    #[test]
    fn stream_arn_with_other_partition_region_or_service_is_invalid() {
        for respelled in &respellings() {
            for op in STREAM_OPS {
                assert_invalid_stream_arn(
                    validate_operation_arns(op, &stream_input(respelled), R),
                    op,
                    respelled,
                );
            }
            assert!(
                validate_tag_resource_arn_shape(respelled).is_ok(),
                "tag {respelled}"
            );
            assert_eq!(
                target("TagResource", &json!({ "ResourceArn": respelled })),
                (respelled.clone(), respelled.clone()),
                "a tag ResourceArn is authorized as sent"
            );
        }
        // The canonical spelling passes both checks and authorizes verbatim.
        assert!(validate_operation_arns("DescribeStream", &stream_input(STREAM), R).is_ok());
        assert!(validate_tag_resource_arn_shape(STREAM).is_ok());
        assert_eq!(
            target("DescribeStream", &stream_input(STREAM)),
            (STREAM.to_owned(), STREAM.to_owned())
        );
        assert_eq!(
            target("TagResource", &json!({ "ResourceArn": STREAM })),
            (STREAM.to_owned(), STREAM.to_owned())
        );
    }

    /// [`check_stream_arn_scope`] does not compare the account: the
    /// cross-account answer runs it for an account the catalog holds, where the
    /// service answers a respelled stream ARN with "Invalid StreamArn" and the
    /// canonical one with its resource-policy denial (measured 2026-11-05).
    #[test]
    fn stream_arn_scope_does_not_compare_the_account() {
        assert!(check_stream_arn_scope(&STREAM.replace(A, OTHER), R).is_ok());
        for respelled in &respellings() {
            let arn = respelled.replace(A, OTHER);
            assert_invalid_stream_arn(check_stream_arn_scope(&arn, R), "scope", &arn);
        }
    }

    /// The `StreamArn` scope check refuses exactly the stream ARNs the shared
    /// predicate puts outside this deployment, the predicate the engine tag
    /// handler applies to a tag `ResourceArn` (`deployment_scope_error` in
    /// `crates/engine/src/tagging.rs`).
    #[test]
    fn stream_arn_scope_uses_the_shared_predicate() {
        let mut arns = respellings().to_vec();
        arns.push(STREAM.to_owned());
        for arn in &arns {
            let f = extenddb_core::arn::fields(arn).unwrap();
            assert_eq!(
                check_stream_arn_scope(arn, R).is_ok(),
                f.in_deployment(R),
                "{arn}"
            );
        }
    }

    /// A tag `ResourceArn` holding a stream ARN passes the shape check in any
    /// account and spelling: another account gets the cross-account answer
    /// before this check, and the engine checks the partition, service and
    /// region after the IAM decision.
    #[test]
    fn a_tag_stream_arn_passes_the_shape_check_in_any_account() {
        for arn in [
            STREAM.replace(A, OTHER),
            STREAM.replace(A, "abc"),
            STREAM.replace(A, ""),
            STREAM.replace("us-east-1", "us-west-2").replace(A, OTHER),
        ] {
            assert!(validate_tag_resource_arn_shape(&arn).is_ok(), "{arn}");
        }
    }

    /// A `GetRecords` iterator carrying a stream ARN outside this deployment's
    /// scope, or in another account, is Invalid and is not canonicalized.
    /// GetShardIterator issues no such iterator.
    #[test]
    fn getrecords_iterator_with_respelled_stream_arn_is_invalid() {
        let token = |arn: &str| {
            json!({ "ShardIterator": extenddb_engine::encode_shard_iterator(
                "shardId-1", "AFTER_SEQUENCE_NUMBER", "", 1_000, arn) })
        };
        for respelled in [
            STREAM.replace(A, OTHER),
            STREAM.replace("arn:aws:", "arn:aws-cn:"),
            STREAM.replace(":dynamodb:", ":DYNAMODB:"),
            STREAM.replace("us-east-1", "us-west-2"),
        ] {
            assert!(
                matches!(
                    getrecords_resource(&token(&respelled), R, A),
                    GetRecordsResource::Invalid
                ),
                "{respelled}"
            );
        }
        match getrecords_resource(&token(STREAM), R, A) {
            GetRecordsResource::Stream(arn) => assert_eq!(arn, STREAM),
            _ => panic!("canonical iterator must authorize its stream"),
        }
    }
}

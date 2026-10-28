// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scope check for stream ARNs (issues #450 and #451): the account must be the
//! caller's, and the partition, service and region this deployment's. See the
//! parent module docs for where each check runs relative to the IAM decision.

use extenddb_core::error::DynamoDbError;

use super::{
    INVALID_STREAM_ARN_MESSAGE, PARTITION, ResourceId, SERVICE, Sub, parse_resource_identity,
};

/// Refuse a stream ARN whose account is not the caller's with
/// `AccessDeniedException` "Access is denied". The service answers this before
/// the IAM decision, for every principal, on `StreamArn` (DescribeStream,
/// GetShardIterator) and on a tag operation's `ResourceArn` alike, and before
/// it compares the partition, region or label (measured live 2026-10-23, in
/// the module docstring of `tests/test_stream_arn_scope.py`: another 12-digit
/// account, `abc`, and another account combined with `aws-cn`, `us-west-2` or
/// an unknown label). On `StreamArn` an empty partition is refused before this
/// check ([`check_stream_arn_scope`]).
pub(super) fn check_stream_arn_account(arn: &str, account_id: &str) -> Result<(), DynamoDbError> {
    let Some(f) = extenddb_core::arn::fields(arn) else {
        return Err(DynamoDbError::ValidationException(
            INVALID_STREAM_ARN_MESSAGE.to_owned(),
        ));
    };
    if f.account != account_id {
        return Err(DynamoDbError::AccessDeniedException(
            "Access is denied".to_owned(),
        ));
    }
    Ok(())
}

/// Refuse a `StreamArn` that is not a stream ARN in this deployment's scope,
/// before the IAM decision, as the service does for DescribeStream and
/// GetShardIterator (measured live 2026-10-23, identical for an admin, a
/// no-permissions user, a user allowed the exact stream ARN and a user with a
/// Deny on it). The checks run in the service's order, measured as an admin on
/// DescribeStream and GetShardIterator on 2026-10-28; both measurements are
/// tabled in the module docstring of `tests/test_stream_arn_scope.py`:
///
/// 1. not a stream ARN (a table ARN, an extra or missing field, a trailing
///    slash), or an empty partition, service or account field:
///    `ValidationException` `Invalid StreamArn`, for any account;
/// 2. an account other than the caller's: `AccessDeniedException`
///    ([`check_stream_arn_account`]), whatever the region field holds;
/// 3. a partition other than `aws` (`aws-cn`, `aws-us-gov`), a service other
///    than `dynamodb` (`DynamoDB`, `DYNAMODB`), or a region other than this
///    server's (empty, `us-west-2`, `US-EAST-1`): `ValidationException`
///    `Invalid StreamArn`.
///
/// The order was measured on DescribeStream from 20:47 to 20:48 UTC and on
/// GetShardIterator at 21:51 UTC. An empty service field, measured on both
/// operations at 21:51 UTC, is step 1's refusal in another account too.
///
/// The stream engine resolves a stream by table name and label within the
/// caller's account, so a stream ARN that passes here names exactly the stream
/// the engine reads, and authorization evaluates that same ARN. Before this
/// check existed, a respelled ARN was canonicalized for authorization and then
/// served as the caller's own stream (issues #450 and #451).
///
/// A tag operation's `ResourceArn` does not take this check: the service runs
/// the IAM decision on the ARN as sent and refuses the partition, service and
/// region after it (`validate_resource_arn` in the engine answers
/// `Invalid StreamArn` there).
pub(super) fn check_stream_arn_scope(
    arn: &str,
    region: &str,
    account_id: &str,
) -> Result<(), DynamoDbError> {
    let invalid = || {
        Err(DynamoDbError::ValidationException(
            INVALID_STREAM_ARN_MESSAGE.to_owned(),
        ))
    };
    let is_stream = matches!(
        parse_resource_identity(arn),
        Some(ResourceId {
            sub: Some(Sub::Stream(_)),
            ..
        })
    );
    let Some(f) = extenddb_core::arn::fields(arn).filter(|_| is_stream) else {
        return invalid();
    };
    if f.partition.is_empty() || f.service.is_empty() || f.account.is_empty() {
        return invalid();
    }
    check_stream_arn_account(arn, account_id)?;
    if f.partition != PARTITION || f.service != SERVICE || f.region != region {
        return invalid();
    }
    Ok(())
}

/// True when every field of this stream ARN is this deployment's and the
/// caller's. GetShardIterator refuses a `StreamArn` for which this is false, so
/// a shard iterator carrying one was hand-built, or issued by an earlier build
/// or under another `server.region` ([`super::getrecords_resource`]).
pub(super) fn stream_arn_in_scope(arn: &str, region: &str, account_id: &str) -> bool {
    check_stream_arn_scope(arn, region, account_id).is_ok()
}

#[cfg(test)]
mod tests {
    use super::super::{
        GetRecordsResource, authz_targets, getrecords_resource, validate_operation_arns,
        validate_tag_resource_arn_shape,
    };
    use super::*;
    use serde_json::{Value, json};

    const R: &str = "us-east-1";
    const A: &str = "964157134968";

    const STREAM: &str =
        "arn:aws:dynamodb:us-east-1:964157134968:table/T/stream/2026-10-20T16:17:40.188";

    fn only(op: &str, input: Value) -> String {
        let t = authz_targets(op, &input, R, A);
        assert_eq!(t.len(), 1, "expected one target for {op}: {t:?}");
        t[0].1.clone()
    }

    fn stream_arn_check(op: &str, arn: &str) -> Result<(), DynamoDbError> {
        validate_operation_arns(op, &json!({ "StreamArn": arn }), R, A)
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

    fn assert_access_denied(r: Result<(), DynamoDbError>, what: &str, arn: &str) {
        match r {
            Err(DynamoDbError::AccessDeniedException(m)) => {
                assert_eq!(m, "Access is denied", "{what} {arn}");
            }
            other => panic!(
                "{what} {arn}: expected Access is denied, got {}",
                variant(&other)
            ),
        }
    }

    fn respellings() -> [String; 7] {
        [
            STREAM.replace("arn:aws:", "arn:aws-cn:"),
            STREAM.replace("arn:aws:", "arn:aws-us-gov:"),
            STREAM.replace(":dynamodb:", ":DYNAMODB:"),
            STREAM.replace(":dynamodb:", ":DynamoDB:"),
            STREAM.replace(":dynamodb:", ":s3:"),
            STREAM.replace("us-east-1", "us-west-2"),
            STREAM.replace("us-east-1", "US-EAST-1"),
        ]
    }

    type Check = fn(Result<(), DynamoDbError>, &str, &str);

    /// The nine StreamArn cells measured live on 2026-10-28 (the order table in
    /// the module docstring of `tests/test_stream_arn_scope.py`), each with the
    /// answer it expects: an empty partition or service is "Invalid StreamArn"
    /// even in another account, another account is "Access is denied" whatever
    /// the region field holds, and the caller's account with an empty or
    /// another region is "Invalid StreamArn".
    fn measured_order() -> [(String, Check); 9] {
        let other = |arn: String| arn.replace(A, "111111111111");
        let no_partition = STREAM.replace("arn:aws:", "arn::");
        let no_service = STREAM.replace(":dynamodb:", "::");
        let no_region = STREAM.replace("us-east-1", "");
        let west = STREAM.replace("us-east-1", "us-west-2");
        [
            (other(no_partition.clone()), assert_invalid_stream_arn),
            (no_partition, assert_invalid_stream_arn),
            (other(no_service.clone()), assert_invalid_stream_arn),
            (no_service, assert_invalid_stream_arn),
            (other(no_region.clone()), assert_access_denied),
            (other(west.clone()), assert_access_denied),
            (other(STREAM.to_owned()), assert_access_denied),
            (no_region, assert_invalid_stream_arn),
            (west, assert_invalid_stream_arn),
        ]
    }

    /// The StreamArn checks run in the measured order on DescribeStream and
    /// GetShardIterator: an empty partition or service field, then the
    /// account, then the region (measured on both operations on 2026-10-28,
    /// GetShardIterator and the empty service at 21:51 UTC).
    #[test]
    fn stream_arn_checks_follow_the_measured_order() {
        for (arn, check) in measured_order() {
            for op in ["DescribeStream", "GetShardIterator"] {
                check(stream_arn_check(op, &arn), op, &arn);
            }
        }
    }

    /// The service answers a stream ARN in another account with
    /// AccessDeniedException "Access is denied" before the IAM decision, on
    /// StreamArn and on a tag ResourceArn (measured 2026-10-23).
    #[test]
    fn stream_arn_in_another_account_is_denied_before_authz() {
        for respelled in [
            STREAM.replace(A, "111111111111"),
            STREAM.replace(A, "111122223333"),
            STREAM.replace(A, "abc"),
        ] {
            for op in ["DescribeStream", "GetShardIterator"] {
                assert_access_denied(stream_arn_check(op, &respelled), op, &respelled);
            }
            assert_access_denied(
                validate_tag_resource_arn_shape(&respelled, A),
                "tag",
                &respelled,
            );
        }
        // An empty account is "Invalid StreamArn" on StreamArn (the kind check
        // requires every field) and "Access is denied" on a tag ResourceArn.
        let empty = STREAM.replace(A, "");
        for op in ["DescribeStream", "GetShardIterator"] {
            assert_invalid_stream_arn(stream_arn_check(op, &empty), op, &empty);
        }
        assert_access_denied(validate_tag_resource_arn_shape(&empty, A), "tag", &empty);
    }

    /// The service answers another partition, region or service spelling of a
    /// StreamArn with ValidationException "Invalid StreamArn" before the IAM
    /// decision (measured 2026-10-23 for aws-cn, aws-us-gov, DynamoDB,
    /// DYNAMODB, us-west-2 and US-EAST-1). On a tag ResourceArn the same
    /// spellings pass the pre-authorization check and are authorized as sent;
    /// the engine refuses them after the IAM decision.
    #[test]
    fn stream_arn_with_other_partition_region_or_service_is_invalid() {
        for respelled in &respellings() {
            for op in ["DescribeStream", "GetShardIterator"] {
                assert_invalid_stream_arn(stream_arn_check(op, respelled), op, respelled);
            }
            assert!(
                validate_tag_resource_arn_shape(respelled, A).is_ok(),
                "tag {respelled}"
            );
            assert_eq!(
                only("TagResource", json!({ "ResourceArn": respelled })),
                *respelled,
                "a tag ResourceArn is authorized as sent"
            );
        }
        // The canonical spelling passes both checks and authorizes verbatim.
        assert!(stream_arn_check("DescribeStream", STREAM).is_ok());
        assert!(validate_tag_resource_arn_shape(STREAM, A).is_ok());
        assert_eq!(
            only("DescribeStream", json!({ "StreamArn": STREAM })),
            STREAM
        );
        assert_eq!(
            only("TagResource", json!({ "ResourceArn": STREAM })),
            STREAM
        );
    }

    /// Account is checked before the partition, service and region (measured
    /// 2026-10-23: another account combined with aws-cn or us-west-2 is
    /// "Access is denied" on every operation).
    #[test]
    fn stream_arn_account_is_checked_before_partition() {
        for both in [
            STREAM
                .replace("arn:aws:", "arn:aws-cn:")
                .replace(A, "111111111111"),
            STREAM
                .replace("us-east-1", "us-west-2")
                .replace(A, "111111111111"),
        ] {
            for op in ["DescribeStream", "GetShardIterator"] {
                assert_access_denied(stream_arn_check(op, &both), op, &both);
            }
            assert_access_denied(validate_tag_resource_arn_shape(&both, A), "tag", &both);
        }
    }

    /// A GetRecords iterator carrying a stream ARN outside this deployment's
    /// scope is Invalid, never canonicalized. GetShardIterator issues no such
    /// iterator.
    #[test]
    fn getrecords_iterator_with_respelled_stream_arn_is_invalid() {
        let token = |arn: &str| {
            json!({ "ShardIterator": extenddb_engine::encode_shard_iterator(
                "shardId-1", "AFTER_SEQUENCE_NUMBER", "", 1_000, arn) })
        };
        for respelled in [
            STREAM.replace(A, "111111111111"),
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

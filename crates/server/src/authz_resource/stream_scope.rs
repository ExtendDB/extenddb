// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scope check for stream ARNs (issues #450 and #451): the account must be the
//! caller's, and the partition, service and region this deployment's. See the
//! parent module docs for where each check runs relative to the IAM decision.

use extenddb_core::error::DynamoDbError;

use super::{INVALID_STREAM_ARN_MESSAGE, PARTITION, SERVICE};

/// Refuse a stream ARN whose account is not the caller's with
/// `AccessDeniedException` "Access is denied". The service answers this before
/// the IAM decision, for every principal, on `StreamArn` (DescribeStream,
/// GetShardIterator) and on a tag operation's `ResourceArn` alike, and before
/// it looks at the partition, region or label (measured live 2026-10-23,
/// probe F2-1: another 12-digit account, `abc`, and another account combined
/// with `aws-cn`, `us-west-2` or an unknown label).
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

/// Refuse a well-formed `StreamArn` that names a stream outside this
/// deployment's scope, before the IAM decision, as the service does for
/// DescribeStream and GetShardIterator (measured live 2026-10-23, probe F2-1,
/// identical for an admin, a no-permissions user, a user allowed the exact
/// stream ARN and a user with a Deny on it):
///
/// - an account other than the caller's: `AccessDeniedException`
///   ([`check_stream_arn_account`]), checked first;
/// - a partition other than `aws` (`aws-cn`, `aws-us-gov`), a service other
///   than `dynamodb` (`DynamoDB`, `DYNAMODB`), or a region other than this
///   server's (`us-west-2`, `US-EAST-1`): `ValidationException`
///   `Invalid StreamArn`.
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
    check_stream_arn_account(arn, account_id)?;
    let Some(f) = extenddb_core::arn::fields(arn) else {
        return Err(DynamoDbError::ValidationException(
            INVALID_STREAM_ARN_MESSAGE.to_owned(),
        ));
    };
    if f.partition != PARTITION || f.service != SERVICE || f.region != region {
        return Err(DynamoDbError::ValidationException(
            INVALID_STREAM_ARN_MESSAGE.to_owned(),
        ));
    }
    Ok(())
}

/// True when every field of this stream ARN is this deployment's and the
/// caller's. The server issues only such ARNs, so a shard iterator carrying any
/// other spelling was not issued by this server.
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

    /// Names the result's variant for a failure message without printing the
    /// error's text, which carries the account id from the checked ARN.
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
    /// scope was not issued by this server and is Invalid, never canonicalized.
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

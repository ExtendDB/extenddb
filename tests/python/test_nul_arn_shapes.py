# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""ARN-shaped values with U+0000 in `TableName`, in nested table names, and in
the stream, backup and tag ARN fields.

Measured against the service on 2026-10-24 with SDK parameter validation off;
every row passed against the service again on 2026-11-04 (us-east-1).
A plain table ARN whose table segment fails the pattern gets the `Table name
of ARN` sentence; an ARN that is not a plain table ARN gets the `Valid ARN
format is ...` frame, with the constraint replaced by `Invalid subscriber ID`
or `Invalid AWS region` when that segment is the bad one. The ARNs below are
built from a table the test creates, so the account and region are the
caller's on either target.

The account segment rows (`TestAccountSegment...`) and the `ListStreams`
`ExclusiveStartStreamArn` rows were measured against the service on
2026-11-06 (us-east-1). An account segment with U+0000, a letter, 11 or 13
digits gets the answer for an account that does not exist in every ARN field
the cross-account check reads, and in `TableName` of the control-plane
operations; the item operations, the transaction members and `BatchGetItem`
refuse it as a validation error.
"""

import time

import pytest
from botocore.exceptions import ClientError

from helpers import assert_validation, unique_name, wait_for_active, wait_for_deleted

NUL = "\u0000"
FRAME = (
    "Valid ARN format is "
    "'arn:<awsPartition>:<vendor>:<region>:<subscriber>:resourceType/resourceName', "
    "where Resource name must have length less than or equal to 255, "
    "Resource name must have length greater than or equal to 3, "
    "Resource name must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
)
TABLE_OF_ARN = "Table name of ARN must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
MAP_KEYS_ARN = (
    "Map keys must satisfy constraint: [Valid resource name or resource arn must be given. "
    "Valid resource arn format is "
    "'arn:<awsPartition>:<vendor>:<region>:<subscriber>:resourceType/resourceName', "
    "where Resource name must have length less than or equal to 255, "
    "Resource name must have length greater than or equal to 3, "
    "Resource name must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]"
)


def _at_table_name(exc: ClientError, value: str, constraint: str) -> None:
    assert_validation(
        exc,
        f"1 validation error detected: Value '{value}' at 'tableName' failed to satisfy "
        f"constraint: {constraint}",
    )


@pytest.fixture()
def arns(raw_client):
    """The created table's name and the parts of its ARN."""
    name = unique_name("nula")
    raw_client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
    )
    wait_for_active(raw_client, name)
    arn = raw_client.describe_table(TableName=name)["Table"]["TableArn"]
    # arn:aws:dynamodb:<region>:<account>:table/<name>
    _, partition, service, region, account, _ = arn.split(":", 5)
    yield {
        "name": name,
        "prefix": f"arn:{partition}:{service}:{region}:{account}:table/",
        "region": region,
        "account": account,
        "bad_table": f"arn:{partition}:{service}:{region}:{account}:table/t367{NUL}name",
    }
    try:
        raw_client.delete_table(TableName=name)
        wait_for_deleted(raw_client, name)
    except Exception:
        pass


class TestArnInTableName:
    def test_plain_table_arn_with_nul_segment(self, raw_client, arns):
        bad = arns["bad_table"]
        for call in [
            lambda: raw_client.describe_table(TableName=bad),
            lambda: raw_client.put_item(TableName=bad, Item={"pk": {"S": "a"}}),
        ]:
            with pytest.raises(ClientError) as exc:
                call()
            _at_table_name(exc.value, bad, TABLE_OF_ARN)

    def test_foreign_service_arn_on_item_operations_gets_the_frame(self, raw_client):
        arn = f"arn:aws:s3:::t367{NUL}name"
        with pytest.raises(ClientError) as exc:
            raw_client.put_item(TableName=arn, Item={"pk": {"S": "a"}})
        _at_table_name(exc.value, arn, FRAME)
        with pytest.raises(ClientError) as exc:
            raw_client.get_item(TableName=arn, Key={"pk": {"S": "a"}})
        _at_table_name(exc.value, arn, FRAME)
        # DescribeTable answers AccessDeniedException on the service (measured
        # 2026-10-24) and the engine's own ValidationException on ExtendDB;
        # neither is a 500, and the row is not asserted beyond that.
        with pytest.raises(ClientError) as exc:
            raw_client.describe_table(TableName=arn)
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    def test_bad_account_and_region_segments_name_their_constraint(self, raw_client, arns):
        acct = arns["account"]
        region = arns["region"]
        arn = f"arn:aws:dynamodb:{region}:{acct[:4]}{NUL}{acct[4:]}:table/t367name"
        with pytest.raises(ClientError) as exc:
            raw_client.put_item(TableName=arn, Item={"pk": {"S": "a"}})
        _at_table_name(exc.value, arn, "Invalid subscriber ID")
        arn = f"arn:aws:dynamodb:{region[:5]}{NUL}{region[5:]}:{acct}:table/t367name"
        with pytest.raises(ClientError) as exc:
            raw_client.put_item(TableName=arn, Item={"pk": {"S": "a"}})
        _at_table_name(exc.value, arn, "Invalid AWS region")

    def test_index_and_stream_arns_get_the_frame(self, raw_client, arns):
        index = f"{arns['prefix']}{arns['name']}/index/i{NUL}x"
        with pytest.raises(ClientError) as exc:
            raw_client.query(
                TableName=index,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "a"}},
            )
        _at_table_name(exc.value, index, FRAME)
        for arn in [
            f"{arns['bad_table']}/index/ix",
            f"{arns['bad_table']}/stream/2026-10-24T00:00:00.000",
        ]:
            with pytest.raises(ClientError) as exc:
                raw_client.describe_table(TableName=arn)
            _at_table_name(exc.value, arn, FRAME)


class TestNestedArns:
    def test_batch_request_items_key_as_arn_with_nul_segment(self, raw_client, arns):
        bad = arns["bad_table"]
        for call in [
            lambda: raw_client.batch_write_item(
                RequestItems={bad: [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]}
            ),
            lambda: raw_client.batch_get_item(RequestItems={bad: {"Keys": [{"pk": {"S": "a"}}]}}),
        ]:
            with pytest.raises(ClientError) as exc:
                call()
            resp = exc.value.response
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400
            assert resp["Error"]["Code"] == "ValidationException"
            msg = resp["Error"]["Message"]
            # The service renders its own dump of the whole map inside
            # Value '...'; the sentence from `at 'requestItems'` onward is
            # compared byte for byte and the key must appear in the value.
            assert msg.startswith("1 validation error detected: Value '"), msg
            assert msg.endswith(
                f"' at 'requestItems' failed to satisfy constraint: {MAP_KEYS_ARN}"
            ), msg
            value = msg[len("1 validation error detected: Value '") : msg.rindex("' at 'requestItems'")]
            assert bad.replace(NUL, "") in value.replace(NUL, "").replace("\\u0000", ""), msg

    def test_transact_write_items_put_table_name_as_arn(self, raw_client, arns):
        bad = arns["bad_table"]
        with pytest.raises(ClientError) as exc:
            raw_client.transact_write_items(
                TransactItems=[{"Put": {"TableName": bad, "Item": {"pk": {"S": "a"}}}}]
            )
        assert_validation(
            exc.value,
            f"1 validation error detected: Value '{bad}' at "
            f"'transactItems.1.member.put.tableName' failed to satisfy constraint: {TABLE_OF_ARN}",
        )


BAD_ACCOUNTS = ["1234" + NUL, "12345678901x", "12345678901", "1234567890123"]
BAD_IDS = ["nul", "letter", "eleven", "thirteen"]
FOREIGN = "999999999999"
OWNER_ONLY = (
    "This action is only supported by accounts that match the resource owner\u2019s account."
)
SHARD = "shardId-00000001700000000000-abcdef12"
LABEL = "2026-11-06T00:00:00.000"


@pytest.fixture(scope="module")
def caller(raw_client):
    """The partition, region and account of the caller, read from a table the
    module creates once."""
    name = unique_name("nulc")
    raw_client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
    )
    wait_for_active(raw_client, name)
    arn = raw_client.describe_table(TableName=name)["Table"]["TableArn"]
    _, partition, _, region, account, _ = arn.split(":", 5)
    yield {"name": name, "partition": partition, "region": region, "account": account}
    try:
        raw_client.delete_table(TableName=name)
        wait_for_deleted(raw_client, name)
    except Exception:
        pass


def _table_arn(caller, account, table="t830name", region=None):
    region = caller["region"] if region is None else region
    return f"arn:{caller['partition']}:dynamodb:{region}:{account}:table/{table}"


def _assert_denied(exc: ClientError, message: str = "Access is denied") -> None:
    resp = exc.response
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400, resp
    assert resp["Error"]["Code"] == "AccessDeniedException", resp["Error"]
    assert resp["Error"]["Message"] == message, resp["Error"]["Message"]


def _raises(call) -> ClientError:
    with pytest.raises(ClientError) as exc:
        call()
    return exc.value


class TestAccountSegmentInArnFields:
    """The fields the cross-account check reads. On PostgreSQL an account with
    U+0000 answered 500 `Internal error during authorization` before the
    account was answered without a catalog lookup."""

    @pytest.mark.parametrize(
        "account",
        [BAD_ACCOUNTS[0], FOREIGN, *BAD_ACCOUNTS[1:]],
        ids=["nul", "foreign", "letter", "eleven", "thirteen"],
    )
    def test_export_and_point_in_time_restore(self, raw_client, caller, account):
        arn = _table_arn(caller, account)
        _assert_denied(
            _raises(lambda: raw_client.export_table_to_point_in_time(TableArn=arn, S3Bucket="bucket1"))
        )
        exc = _raises(
            lambda: raw_client.restore_table_to_point_in_time(
                SourceTableArn=arn, TargetTableName="t830target"
            )
        )
        assert_validation(exc, OWNER_ONLY)

    @pytest.mark.parametrize("account", BAD_ACCOUNTS, ids=BAD_IDS)
    def test_tag_stream_and_backup_arn_fields(self, raw_client, raw_streams_client, caller, account):
        table = _table_arn(caller, account)
        stream = f"{table}/stream/{LABEL}"
        backup = f"{table}/backup/01700000000000-abcdef12"
        for call in [
            lambda: raw_client.tag_resource(ResourceArn=table, Tags=[{"Key": "k", "Value": "v"}]),
            lambda: raw_client.untag_resource(ResourceArn=table, TagKeys=["k"]),
            lambda: raw_client.list_tags_of_resource(ResourceArn=table),
            lambda: raw_client.list_tags_of_resource(ResourceArn=stream),
            lambda: raw_streams_client.describe_stream(StreamArn=stream),
            lambda: raw_streams_client.get_shard_iterator(
                StreamArn=stream, ShardId=SHARD, ShardIteratorType="TRIM_HORIZON"
            ),
            lambda: raw_client.describe_backup(BackupArn=backup),
            lambda: raw_client.restore_table_from_backup(
                BackupArn=backup, TargetTableName="t830target"
            ),
        ]:
            _assert_denied(_raises(call))


class TestAccountSegmentInTableName:
    """A table ARN in `TableName`, `RequestItems` and `TransactItems` whose
    account segment is not 12 digits."""

    @pytest.mark.parametrize("account", BAD_ACCOUNTS, ids=BAD_IDS)
    def test_control_plane_operations_answer_as_for_an_unknown_account(
        self, raw_client, caller, account
    ):
        arn = _table_arn(caller, account)
        for call in [
            lambda: raw_client.describe_table(TableName=arn),
            lambda: raw_client.update_table(TableName=arn, BillingMode="PAY_PER_REQUEST"),
            lambda: raw_client.describe_time_to_live(TableName=arn),
            lambda: raw_client.update_time_to_live(
                TableName=arn,
                TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
            ),
            lambda: raw_client.describe_continuous_backups(TableName=arn),
            lambda: raw_client.update_continuous_backups(
                TableName=arn,
                PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": False},
            ),
            lambda: raw_client.create_backup(TableName=arn, BackupName="b830"),
            lambda: raw_client.list_backups(TableName=arn),
            lambda: raw_client.create_table(
                TableName=arn,
                BillingMode="PAY_PER_REQUEST",
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            ),
        ]:
            _assert_denied(_raises(call))

    @pytest.mark.parametrize("account", BAD_ACCOUNTS, ids=BAD_IDS)
    def test_item_operations_and_transaction_members_answer_invalid_subscriber_id(
        self, raw_client, caller, account
    ):
        arn = _table_arn(caller, account)
        key = {"pk": {"S": "a"}}
        for call in [
            lambda: raw_client.get_item(TableName=arn, Key=key),
            lambda: raw_client.put_item(TableName=arn, Item=key),
            lambda: raw_client.update_item(TableName=arn, Key=key),
            lambda: raw_client.query(
                TableName=arn,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "a"}},
            ),
            lambda: raw_client.scan(TableName=arn),
        ]:
            _at_table_name(_raises(call), arn, "Invalid subscriber ID")
        for op, call in [
            ("get", lambda: raw_client.transact_get_items(
                TransactItems=[{"Get": {"TableName": arn, "Key": key}}])),
            ("put", lambda: raw_client.transact_write_items(
                TransactItems=[{"Put": {"TableName": arn, "Item": key}}])),
        ]:
            assert_validation(
                _raises(call),
                f"1 validation error detected: Value '{arn}' at "
                f"'transactItems.1.member.{op}.tableName' failed to satisfy constraint: "
                "Invalid subscriber ID",
            )

    @pytest.mark.parametrize("account", BAD_ACCOUNTS, ids=BAD_IDS)
    def test_batch_keys(self, raw_client, caller, account):
        arn = _table_arn(caller, account)
        key = {"pk": {"S": "a"}}
        calls = {
            "BatchGetItem": lambda: raw_client.batch_get_item(RequestItems={arn: {"Keys": [key]}}),
            "BatchWriteItem": lambda: raw_client.batch_write_item(
                RequestItems={arn: [{"PutRequest": {"Item": key}}]}
            ),
        }
        for op, call in calls.items():
            if op == "BatchWriteItem" and account in (BAD_ACCOUNTS[0], BAD_ACCOUNTS[2]):
                # The service answered U+0000 and 11 digits here with either
                # `Access is denied` or the map-key constraint from one request
                # to the next (2026-11-06); only a 400 is asserted.
                exc = _raises(call)
                assert exc.response["ResponseMetadata"]["HTTPStatusCode"] == 400
                continue
            exc = _raises(call)
            if op == "BatchWriteItem":
                _assert_denied(exc)
                continue
            resp = exc.response
            assert resp["Error"]["Code"] == "ValidationException", (op, resp["Error"])
            assert resp["Error"]["Message"].endswith(
                f"' at 'requestItems' failed to satisfy constraint: {MAP_KEYS_ARN}"
            ), (op, resp["Error"]["Message"])

    def test_control_plane_region_fault_has_no_location(self, raw_client, caller):
        arn = _table_arn(
            caller, caller["account"], table=caller["name"], region="us-ea" + NUL + "st-1"
        )
        for call in [
            lambda: raw_client.describe_table(TableName=arn),
            lambda: raw_client.update_table(TableName=arn, BillingMode="PAY_PER_REQUEST"),
            lambda: raw_client.describe_time_to_live(TableName=arn),
            lambda: raw_client.describe_continuous_backups(TableName=arn),
            lambda: raw_client.list_backups(TableName=arn),
        ]:
            assert_validation(
                _raises(call), f"1 validation error detected: Invalid AWS region in '{arn}'"
            )


def _list_streams(client, **params):
    """ListStreams, sent again after a ThrottlingException (the service's rate
    for this operation is low)."""
    for attempt in range(5):
        try:
            return client.list_streams(**params)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ThrottlingException" or attempt == 4:
                raise
            time.sleep(2 * (attempt + 1))
    raise AssertionError("unreachable")


class TestListStreamsExclusiveStartStreamArn:
    """Storage reads the start ARN's table segment and label. U+0000 in either
    answered 500 `Internal server error` on PostgreSQL."""

    def test_nul_in_the_label_or_table_segment_is_invalid_stream_arn(
        self, raw_streams_client, caller
    ):
        own = _table_arn(caller, caller["account"], table=caller["name"])
        for start in [
            f"{own}/stream/2026{NUL}",
            f"{_table_arn(caller, caller['account'], table='t830' + NUL + 'name')}/stream/{LABEL}",
        ]:
            for extra in [{}, {"TableName": caller["name"]}]:
                with pytest.raises(ClientError) as exc:
                    _list_streams(raw_streams_client, ExclusiveStartStreamArn=start, **extra)
                assert_validation(exc.value, "Invalid StreamArn")

    def test_nul_in_the_account_or_region_is_accepted(self, raw_streams_client, caller):
        for start in [
            f"{_table_arn(caller, '1234' + NUL, table=caller['name'])}/stream/{LABEL}",
            f"{_table_arn(caller, caller['account'], table=caller['name'], region='us-ea' + NUL + 'st-1')}"
            f"/stream/{LABEL}",
        ]:
            resp = _list_streams(raw_streams_client, ExclusiveStartStreamArn=start, Limit=1)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Streams" in resp

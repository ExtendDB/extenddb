"""ARN-shaped values with U+0000 in `TableName`, in nested table names, and in
the stream, backup and tag ARN fields.

Measured against the service on 2026-10-24 with SDK parameter validation off.
A plain table ARN whose table segment fails the pattern gets the `Table name
of ARN` sentence; an ARN that is not a plain table ARN gets the `Valid ARN
format is ...` frame, with the constraint replaced by `Invalid subscriber ID`
or `Invalid AWS region` when that segment is the bad one. The ARNs below are
built from a table the test creates, so the account and region are the
caller's on either target.
"""

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
        # DescribeTable answers AccessDeniedException on the service and the
        # engine's own ValidationException on ExtendDB; neither is a 500, and
        # the row is not asserted beyond that.
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

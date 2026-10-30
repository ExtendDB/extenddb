"""U+0000 in CreateTable names: key attribute names, index names, table names.

DynamoDB accepts U+0000 inside a key schema attribute name (the table's or a
secondary index's) and DescribeTable returns the name byte-identical. It
refuses U+0000 inside an index name or a table name with the member pattern
ValidationException. Measured against the service on 2026-09-18 with SDK
parameter validation off, so the service, not the SDK, decided.

The table-name row is checked on every operation that carries a bare
TableName, since the name is used before any handler runs.

Order against authorization (measured 2026-10-24, not encoded as a test): a
principal with no DynamoDB permission gets the same tableName pattern
ValidationException for DescribeTable, PutItem and Scan with a U+0000 name
(and the requestItems sentence for BatchWriteItem), and AccessDeniedException
for DescribeTable on a real table. The pattern check precedes the IAM decision.
This suite has no denied-principal fixture against the service, so the row is
recorded here as a comment; the server places the check before authorization
(crates/server/src/table_name_gate.rs).
"""

import pytest
from botocore.exceptions import ClientError

from helpers import assert_member_pattern, unique_name, wait_for_active, wait_for_deleted

NUL = "\u0000"
PATTERN = "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
MAP_KEYS = (
    "Map keys must satisfy constraint: [Member must have length less than or equal to 255, "
    "Member must have length greater than or equal to 3, "
    "Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+]"
)


@pytest.fixture()
def raw_table_factory(raw_client):
    created: list[str] = []

    def _create(**kwargs) -> str:
        name = kwargs.pop("TableName", None) or unique_name("nul")
        raw_client.create_table(TableName=name, BillingMode="PAY_PER_REQUEST", **kwargs)
        created.append(name)
        wait_for_active(raw_client, name)
        return name

    yield _create

    for name in created:
        try:
            raw_client.delete_table(TableName=name)
            wait_for_deleted(raw_client, name)
        except Exception:
            pass


def _validation_error(exc: ClientError, location: str, value: str) -> None:
    assert_member_pattern(exc, location, value)


class TestNulInKeyAttributeNames:
    def test_partition_key_attribute_name_with_nul_is_stored_and_returned(
        self, raw_client, raw_table_factory
    ):
        pk = f"p{NUL}k"
        name = raw_table_factory(
            KeySchema=[{"AttributeName": pk, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": pk, "AttributeType": "S"}],
        )
        table = raw_client.describe_table(TableName=name)["Table"]
        assert table["TableStatus"] == "ACTIVE"
        assert table["KeySchema"] == [{"AttributeName": pk, "KeyType": "HASH"}]
        assert table["AttributeDefinitions"] == [{"AttributeName": pk, "AttributeType": "S"}]

    def test_gsi_key_attribute_name_with_nul_is_stored_and_returned(
        self, raw_client, raw_table_factory
    ):
        gk = f"g{NUL}k"
        name = raw_table_factory(
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": gk, "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": gk, "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        table = raw_client.describe_table(TableName=name)["Table"]
        gsi = table["GlobalSecondaryIndexes"][0]
        assert gsi["IndexName"] == "gsi1"
        assert gsi["KeySchema"] == [{"AttributeName": gk, "KeyType": "HASH"}]
        defs = {d["AttributeName"]: d["AttributeType"] for d in table["AttributeDefinitions"]}
        assert defs == {"pk": "S", gk: "S"}

    def test_gsi_on_nul_attribute_can_be_removed_and_added_again(
        self, raw_client, raw_table_factory
    ):
        gk = f"g{NUL}k"
        name = raw_table_factory(
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": gk, "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": gk, "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        raw_client.update_table(
            TableName=name,
            GlobalSecondaryIndexUpdates=[{"Delete": {"IndexName": "gsi1"}}],
        )
        wait_for_active(raw_client, name)
        table = raw_client.describe_table(TableName=name)["Table"]
        assert not table.get("GlobalSecondaryIndexes")
        raw_client.update_table(
            TableName=name,
            AttributeDefinitions=[{"AttributeName": gk, "AttributeType": "S"}],
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": "gsi2",
                        "KeySchema": [{"AttributeName": gk, "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                }
            ],
        )
        wait_for_active(raw_client, name)
        table = raw_client.describe_table(TableName=name)["Table"]
        gsi = table["GlobalSecondaryIndexes"][0]
        assert gsi["IndexName"] == "gsi2"
        assert gsi["KeySchema"] == [{"AttributeName": gk, "KeyType": "HASH"}]
        defs = {d["AttributeName"]: d["AttributeType"] for d in table["AttributeDefinitions"]}
        assert defs == {"pk": "S", gk: "S"}

    def test_lsi_key_and_gsi_projected_attribute_names_with_nul_are_stored_and_returned(
        self, raw_client, raw_table_factory
    ):
        """An LSI keyed on a U+0000 attribute and a GSI whose NonKeyAttributes
        entry contains U+0000: 200, both returned byte-identical (measured
        2026-10-24)."""
        lk = f"l{NUL}k"
        nk = f"n{NUL}k"
        name = raw_table_factory(
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gk", "AttributeType": "S"},
                {"AttributeName": lk, "AttributeType": "S"},
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "lsi1",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": lk, "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": [nk]},
                }
            ],
        )
        table = raw_client.describe_table(TableName=name)["Table"]
        lsi = table["LocalSecondaryIndexes"][0]
        assert lsi["KeySchema"] == [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": lk, "KeyType": "RANGE"},
        ]
        gsi = table["GlobalSecondaryIndexes"][0]
        assert gsi["Projection"] == {"ProjectionType": "INCLUDE", "NonKeyAttributes": [nk]}
        defs = {d["AttributeName"]: d["AttributeType"] for d in table["AttributeDefinitions"]}
        assert defs == {"pk": "S", "sk": "S", "gk": "S", lk: "S"}


class TestNulInNames:
    def test_gsi_name_with_nul_is_refused(self, raw_client):
        gsi_name = f"gsi{NUL}1"
        with pytest.raises(ClientError) as exc:
            raw_client.create_table(
                TableName=unique_name("nul"),
                BillingMode="PAY_PER_REQUEST",
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "gk", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": gsi_name,
                        "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                ],
            )
        resp = exc.value.response
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert resp["Error"]["Code"] == "ValidationException"
        assert "indexName" in resp["Error"]["Message"]
        assert PATTERN in resp["Error"]["Message"]

    def test_create_table_with_nul_in_table_name_is_refused(self, raw_client):
        bad = f"nul{NUL}table"
        with pytest.raises(ClientError) as exc:
            raw_client.create_table(
                TableName=bad,
                BillingMode="PAY_PER_REQUEST",
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            )
        _validation_error(exc.value, "tableName", bad)

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(lambda c, t: c.describe_table(TableName=t), id="DescribeTable"),
            pytest.param(lambda c, t: c.delete_table(TableName=t), id="DeleteTable"),
            pytest.param(
                lambda c, t: c.update_table(TableName=t, BillingMode="PAY_PER_REQUEST"),
                id="UpdateTable",
            ),
            pytest.param(
                lambda c, t: c.put_item(TableName=t, Item={"pk": {"S": "a"}}), id="PutItem"
            ),
            pytest.param(
                lambda c, t: c.get_item(TableName=t, Key={"pk": {"S": "a"}}), id="GetItem"
            ),
            pytest.param(
                lambda c, t: c.update_item(
                    TableName=t,
                    Key={"pk": {"S": "a"}},
                    UpdateExpression="SET v = :v",
                    ExpressionAttributeValues={":v": {"S": "a"}},
                ),
                id="UpdateItem",
            ),
            pytest.param(
                lambda c, t: c.delete_item(TableName=t, Key={"pk": {"S": "a"}}),
                id="DeleteItem",
            ),
            pytest.param(
                lambda c, t: c.query(
                    TableName=t,
                    KeyConditionExpression="pk = :v",
                    ExpressionAttributeValues={":v": {"S": "a"}},
                ),
                id="Query",
            ),
            pytest.param(lambda c, t: c.scan(TableName=t), id="Scan"),
        ],
    )
    def test_operations_with_nul_in_table_name_are_refused(self, raw_client, call):
        bad = f"nul{NUL}table"
        with pytest.raises(ClientError) as exc:
            call(raw_client, bad)
        _validation_error(exc.value, "tableName", bad)

    @pytest.mark.parametrize("name", ["a b"], ids=["space"])
    def test_query_and_scan_with_other_bad_characters_are_refused(self, raw_client, name):
        """Same sentence for any character outside the pattern (measured with a space)."""
        with pytest.raises(ClientError) as exc:
            raw_client.scan(TableName=name)
        _validation_error(exc.value, "tableName", name)
        with pytest.raises(ClientError) as exc:
            raw_client.query(
                TableName=name,
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "a"}},
            )
        _validation_error(exc.value, "tableName", name)


class TestNulInNestedTableNames:
    """Table names carried inside RequestItems or TransactItems.

    Measured 2026-10-24. For the batch operations the service renders its own
    dump of the whole RequestItems map inside ``Value '...'`` (a Java-style
    object dump for BatchWriteItem, the JSON of the map for BatchGetItem), so
    the value is not compared byte for byte: the sentence from
    ``at 'requestItems'`` onward is, and the offending key must appear in the
    value (with the U+0000 normalised, since the BatchGetItem dump carries it as
    the six characters ``\\u0000``). ExtendDB renders the offending key alone.
    """

    @staticmethod
    def _request_items_error(exc: ClientError, key: str) -> None:
        resp = exc.response
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert resp["Error"]["Code"] == "ValidationException"
        msg = resp["Error"]["Message"]
        assert msg.startswith("1 validation error detected: Value '"), msg
        assert msg.endswith(f"' at 'requestItems' failed to satisfy constraint: {MAP_KEYS}"), msg
        value = msg[len("1 validation error detected: Value '") : msg.rindex("' at 'requestItems'")]
        assert key.replace(NUL, "") in value.replace(NUL, "").replace("\\u0000", ""), msg

    def test_batch_write_item_with_nul_request_items_key_is_refused(self, raw_client):
        bad = f"nul{NUL}table"
        with pytest.raises(ClientError) as exc:
            raw_client.batch_write_item(
                RequestItems={bad: [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]}
            )
        self._request_items_error(exc.value, bad)

    def test_batch_get_item_with_nul_request_items_key_is_refused(self, raw_client):
        bad = f"nul{NUL}table"
        with pytest.raises(ClientError) as exc:
            raw_client.batch_get_item(RequestItems={bad: {"Keys": [{"pk": {"S": "a"}}]}})
        self._request_items_error(exc.value, bad)

    def test_transact_write_items_with_nul_table_name_is_refused(self, raw_client):
        bad = f"nul{NUL}table"
        with pytest.raises(ClientError) as exc:
            raw_client.transact_write_items(
                TransactItems=[{"Put": {"TableName": bad, "Item": {"pk": {"S": "a"}}}}]
            )
        _validation_error(exc.value, "transactItems.1.member.put.tableName", bad)

    def test_transact_get_items_with_nul_table_name_is_refused(self, raw_client):
        bad = f"nul{NUL}table"
        with pytest.raises(ClientError) as exc:
            raw_client.transact_get_items(
                TransactItems=[{"Get": {"TableName": bad, "Key": {"pk": {"S": "a"}}}}]
            )
        _validation_error(exc.value, "transactItems.1.member.get.tableName", bad)


class TestNulInTableArn:
    """A TableName or ResourceArn that is a table ARN whose table segment
    contains U+0000 (measured 2026-10-24). The ARN is built in the caller's
    own account and region, read from a table the test creates."""

    @pytest.fixture()
    def nul_arn(self, raw_client, raw_table_factory):
        name = raw_table_factory(
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        )
        real = raw_client.describe_table(TableName=name)["Table"]["TableArn"]
        prefix = real[: real.rindex("table/") + len("table/")]
        return f"{prefix}nul{NUL}table"

    @staticmethod
    def _arn_table_name_error(exc: ClientError, arn: str) -> None:
        resp = exc.response
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert resp["Error"]["Code"] == "ValidationException"
        assert resp["Error"]["Message"] == (
            f"1 validation error detected: Value '{arn}' at 'tableName' failed to satisfy "
            "constraint: Table name of ARN must satisfy regular expression pattern: "
            "[a-zA-Z0-9_.-]+"
        )

    def test_describe_table_with_nul_arn_segment_is_refused(self, raw_client, nul_arn):
        with pytest.raises(ClientError) as exc:
            raw_client.describe_table(TableName=nul_arn)
        self._arn_table_name_error(exc.value, nul_arn)

    def test_put_item_with_nul_arn_segment_is_refused(self, raw_client, nul_arn):
        with pytest.raises(ClientError) as exc:
            raw_client.put_item(TableName=nul_arn, Item={"pk": {"S": "a"}})
        self._arn_table_name_error(exc.value, nul_arn)

    @pytest.mark.parametrize(
        "call",
        [
            pytest.param(
                lambda c, a: c.tag_resource(ResourceArn=a, Tags=[{"Key": "k", "Value": "v"}]),
                id="TagResource",
            ),
            pytest.param(lambda c, a: c.untag_resource(ResourceArn=a, TagKeys=["k"]), id="UntagResource"),
            pytest.param(lambda c, a: c.list_tags_of_resource(ResourceArn=a), id="ListTagsOfResource"),
        ],
    )
    def test_tag_operations_with_nul_arn_segment_are_refused(self, raw_client, nul_arn, call):
        """Measured on TagResource; UntagResource and ListTagsOfResource share
        the ResourceArn shape check and are given the same sentence."""
        with pytest.raises(ClientError) as exc:
            call(raw_client, nul_arn)
        resp = exc.value.response
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert resp["Error"]["Code"] == "ValidationException"
        assert resp["Error"]["Message"] == (
            f"Invalid TableArn: Invalid ResourceArn provided as input {nul_arn}"
        )

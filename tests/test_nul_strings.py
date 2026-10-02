# SPDX-License-Identifier: Apache-2.0
"""U+0000 and U+0001 inside strings: keys, index keys, values, and names.

DynamoDB places no character restriction on strings. Measured 2026-09-18
against the service on a scratch table: PutItem accepts the NUL character
inside a partition key, a sort key, a GSI key, a non-key value, a string in a
list, a map key, and a top-level attribute name, and GetItem returns every one
byte-identical. NUL sorts as the byte 0x00, before every other character; it is
a different character from the six-character text ``\\u0000``; range and prefix
conditions treat it as an ordinary byte; the GSI holds the item; DeleteItem on
the key removes it. These tests pin exactly that, so every backend has to store
the character rather than its escape text and has to keep it out of the way of
its own storage encoding.

U+0001 is included because a storage encoding that handles NUL has to leave the
next code point alone as well.
"""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table, wait_for_gsi_items

NUL = "\u0000"
SOH = "\u0001"
ESCAPE_TEXT = "\\u0000"  # backslash, u, 0, 0, 0, 0: six characters, not a NUL

PK = f"a{NUL}b"
PK_WITHOUT_NUL = "ab"
PK_SOH = f"a{SOH}b"

# One partition, five sort keys whose byte order is: NUL, U+0001, space, the
# escape text (starts with 0x5c), then "a" (0x61).
SORT_KEYS = [NUL, SOH, " ", ESCAPE_TEXT, "a"]

# The item under the NUL sort key carries the character in every kind of
# position the item model has.
RICH_ITEM = {
    "pk": {"S": PK},
    "sk": {"S": NUL},
    "g": {"S": f"g{NUL}"},
    "v": {"S": f"v{NUL}"},
    f"n{NUL}n": {"S": "x"},
    "m": {"M": {f"k{NUL}": {"S": f"{NUL}"}, f"k{SOH}{SOH}": {"N": "1"}}},
    "l": {"L": [{"S": NUL}, {"S": f"{SOH}{NUL}{SOH}"}]},
    "ss": {"SS": [NUL, "a"]},
}


def _query(client, table, **kwargs):
    return client.query(
        TableName=table,
        KeyConditionExpression=kwargs.pop("cond", "pk = :p"),
        ExpressionAttributeValues={":p": {"S": PK}, **kwargs.pop("values", {})},
        **kwargs,
    )


def _sks(resp):
    return [i["sk"]["S"] for i in resp["Items"]]


@pytest.fixture(scope="class")
def nul_table(dynamodb_client):
    """(S, S) table with GSI gsi1 on g; the partition PK holds SORT_KEYS, plus
    a control item in the partition without the NUL and one in a partition
    keyed with U+0001."""
    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    ) as name:
        dynamodb_client.put_item(TableName=name, Item=RICH_ITEM)
        for sk in SORT_KEYS[1:]:
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": PK}, "sk": {"S": sk}, "g": {"S": "plain"}}
            )
        dynamodb_client.put_item(
            TableName=name, Item={"pk": {"S": PK_WITHOUT_NUL}, "sk": {"S": "x"}, "g": {"S": "plain"}}
        )
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": PK_SOH}, "sk": {"S": SOH}, "g": {"S": f"g{SOH}"}, "v": {"S": SOH}},
        )
        yield name


class TestNulInStrings:
    def test_item_with_nul_everywhere_round_trips(self, dynamodb_client, nul_table):
        resp = dynamodb_client.get_item(
            TableName=nul_table, Key={"pk": {"S": PK}, "sk": {"S": NUL}}, ConsistentRead=True
        )
        assert resp["Item"] == RICH_ITEM

    def test_soh_item_round_trips(self, dynamodb_client, nul_table):
        resp = dynamodb_client.get_item(
            TableName=nul_table, Key={"pk": {"S": PK_SOH}, "sk": {"S": SOH}}, ConsistentRead=True
        )
        assert resp["Item"] == {
            "pk": {"S": PK_SOH},
            "sk": {"S": SOH},
            "g": {"S": f"g{SOH}"},
            "v": {"S": SOH},
        }

    def test_partition_order_is_byte_order_with_nul_first(self, dynamodb_client, nul_table):
        assert _sks(_query(dynamodb_client, nul_table, ConsistentRead=True)) == SORT_KEYS
        assert (
            _sks(_query(dynamodb_client, nul_table, ScanIndexForward=False, ConsistentRead=True))
            == SORT_KEYS[::-1]
        )

    def test_nul_and_its_escape_text_are_different_keys(self, dynamodb_client, nul_table):
        by_nul = dynamodb_client.get_item(
            TableName=nul_table, Key={"pk": {"S": PK}, "sk": {"S": NUL}}, ConsistentRead=True
        )["Item"]
        by_text = dynamodb_client.get_item(
            TableName=nul_table, Key={"pk": {"S": PK}, "sk": {"S": ESCAPE_TEXT}}, ConsistentRead=True
        )["Item"]
        assert by_nul["sk"]["S"] == NUL and len(by_nul["sk"]["S"]) == 1
        assert by_text["sk"]["S"] == ESCAPE_TEXT and len(by_text["sk"]["S"]) == 6

    def test_partition_key_with_nul_is_distinct_from_the_key_without_it(
        self, dynamodb_client, nul_table
    ):
        resp = dynamodb_client.get_item(
            TableName=nul_table, Key={"pk": {"S": PK_WITHOUT_NUL}, "sk": {"S": NUL}}, ConsistentRead=True
        )
        assert "Item" not in resp
        resp = _query(dynamodb_client, nul_table, values={":p": {"S": PK_WITHOUT_NUL}}, ConsistentRead=True)
        assert _sks(resp) == ["x"]

    def test_range_conditions_treat_nul_as_a_byte(self, dynamodb_client, nul_table):
        below_space = _query(
            dynamodb_client, nul_table, cond="pk = :p AND sk < :s", values={":s": {"S": " "}},
            ConsistentRead=True,
        )
        assert _sks(below_space) == [NUL, SOH]
        between = _query(
            dynamodb_client, nul_table, cond="pk = :p AND sk BETWEEN :lo AND :hi",
            values={":lo": {"S": NUL}, ":hi": {"S": SOH}}, ConsistentRead=True,
        )
        assert _sks(between) == [NUL, SOH]
        prefix = _query(
            dynamodb_client, nul_table, cond="pk = :p AND begins_with(sk, :pre)",
            values={":pre": {"S": NUL}}, ConsistentRead=True,
        )
        assert _sks(prefix) == [NUL]
        text_prefix = _query(
            dynamodb_client, nul_table, cond="pk = :p AND begins_with(sk, :pre)",
            values={":pre": {"S": "\\"}}, ConsistentRead=True,
        )
        assert _sks(text_prefix) == [ESCAPE_TEXT]

    def test_pages_of_one_visit_every_key_once(self, dynamodb_client, nul_table):
        """The cursor for a NUL key travels through LastEvaluatedKey and back
        through ExclusiveStartKey; the walk must neither skip nor repeat."""
        seen: list[str] = []
        kwargs: dict = {"Limit": 1, "ConsistentRead": True}
        while True:
            resp = _query(dynamodb_client, nul_table, **kwargs)
            seen += _sks(resp)
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        assert seen == SORT_KEYS

    def test_gsi_holds_the_item_with_a_nul_key(self, dynamodb_client, nul_table):
        def walk():
            return dynamodb_client.query(
                TableName=nul_table,
                IndexName="gsi1",
                KeyConditionExpression="g = :g",
                ExpressionAttributeValues={":g": {"S": f"g{NUL}"}},
            )["Items"]

        items = wait_for_gsi_items(walk, 1)
        assert items == [RICH_ITEM]

    def test_filter_on_a_nul_named_attribute(self, dynamodb_client, nul_table):
        resp = dynamodb_client.scan(
            TableName=nul_table,
            FilterExpression="#n = :x",
            ExpressionAttributeNames={"#n": f"n{NUL}n"},
            ExpressionAttributeValues={":x": {"S": "x"}},
            ConsistentRead=True,
        )
        assert resp["Items"] == [RICH_ITEM]
        resp = _query(
            dynamodb_client, nul_table, FilterExpression="v = :v",
            values={":v": {"S": f"v{NUL}"}}, ConsistentRead=True,
        )
        assert resp["Items"] == [RICH_ITEM]

    def test_condition_expression_sees_the_nul_named_attribute(self, dynamodb_client, nul_table):
        # Exists on the rich item: the conditional write goes through.
        dynamodb_client.update_item(
            TableName=nul_table,
            Key={"pk": {"S": PK}, "sk": {"S": NUL}},
            UpdateExpression="SET touched = :t",
            ConditionExpression="attribute_exists(#n)",
            ExpressionAttributeNames={"#n": f"n{NUL}n"},
            ExpressionAttributeValues={":t": {"BOOL": True}},
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_item(
                TableName=nul_table,
                Key={"pk": {"S": PK}, "sk": {"S": "a"}},
                UpdateExpression="SET touched = :t",
                ConditionExpression="attribute_exists(#n)",
                ExpressionAttributeNames={"#n": f"n{NUL}n"},
                ExpressionAttributeValues={":t": {"BOOL": True}},
            )
        assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
        dynamodb_client.update_item(
            TableName=nul_table,
            Key={"pk": {"S": PK}, "sk": {"S": NUL}},
            UpdateExpression="REMOVE touched",
        )

    def test_update_item_sets_nul_named_attribute_and_nul_value(self, dynamodb_client, nul_table):
        key = {"pk": {"S": PK}, "sk": {"S": " "}}
        resp = dynamodb_client.update_item(
            TableName=nul_table,
            Key=key,
            UpdateExpression="SET #a = :v, m2 = :m",
            ExpressionAttributeNames={"#a": f"u{NUL}"},
            ExpressionAttributeValues={
                ":v": {"S": f"{NUL}z"},
                ":m": {"M": {f"{NUL}": {"L": [{"S": SOH}]}}},
            },
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"][f"u{NUL}"] == {"S": f"{NUL}z"}
        assert resp["Attributes"]["m2"] == {"M": {f"{NUL}": {"L": [{"S": SOH}]}}}
        got = dynamodb_client.get_item(TableName=nul_table, Key=key, ConsistentRead=True)["Item"]
        assert got[f"u{NUL}"] == {"S": f"{NUL}z"}
        assert got["m2"] == {"M": {f"{NUL}": {"L": [{"S": SOH}]}}}
        resp = dynamodb_client.update_item(
            TableName=nul_table,
            Key=key,
            UpdateExpression="REMOVE #a, m2",
            ExpressionAttributeNames={"#a": f"u{NUL}"},
            ReturnValues="ALL_NEW",
        )
        assert f"u{NUL}" not in resp["Attributes"] and "m2" not in resp["Attributes"]

    def test_batch_and_transact_writes_and_reads(self, dynamodb_client, nul_table):
        b1 = {"pk": {"S": f"batch{NUL}"}, "sk": {"S": NUL}, "g": {"S": "plain"}, f"{NUL}": {"S": NUL}}
        t1 = {"pk": {"S": f"tx{NUL}"}, "sk": {"S": f"{NUL}{NUL}"}, "g": {"S": "plain"}}
        dynamodb_client.batch_write_item(RequestItems={nul_table: [{"PutRequest": {"Item": b1}}]})
        dynamodb_client.transact_write_items(
            TransactItems=[{"Put": {"TableName": nul_table, "Item": t1}}]
        )
        got = dynamodb_client.batch_get_item(
            RequestItems={
                nul_table: {
                    "Keys": [{"pk": b1["pk"], "sk": b1["sk"]}, {"pk": t1["pk"], "sk": t1["sk"]}],
                    "ConsistentRead": True,
                }
            }
        )["Responses"][nul_table]
        assert sorted(got, key=lambda i: i["pk"]["S"]) == sorted([b1, t1], key=lambda i: i["pk"]["S"])
        tg = dynamodb_client.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": nul_table, "Key": {"pk": b1["pk"], "sk": b1["sk"]}}},
                {"Get": {"TableName": nul_table, "Key": {"pk": t1["pk"], "sk": t1["sk"]}}},
            ]
        )["Responses"]
        assert [r["Item"] for r in tg] == [b1, t1]
        dynamodb_client.batch_write_item(
            RequestItems={
                nul_table: [
                    {"DeleteRequest": {"Key": {"pk": b1["pk"], "sk": b1["sk"]}}},
                    {"DeleteRequest": {"Key": {"pk": t1["pk"], "sk": t1["sk"]}}},
                ]
            }
        )

    def test_scan_counts_every_item(self, dynamodb_client, nul_table):
        resp = dynamodb_client.scan(TableName=nul_table, Select="COUNT", ConsistentRead=True)
        assert resp["Count"] == len(SORT_KEYS) + 2

    def test_delete_item_on_a_nul_key(self, dynamodb_client, nul_table):
        key = {"pk": {"S": f"del{NUL}"}, "sk": {"S": NUL}}
        dynamodb_client.put_item(TableName=nul_table, Item={**key, "g": {"S": "plain"}})
        resp = dynamodb_client.delete_item(TableName=nul_table, Key=key, ReturnValues="ALL_OLD")
        assert resp["Attributes"] == {**key, "g": {"S": "plain"}}
        assert "Item" not in dynamodb_client.get_item(TableName=nul_table, Key=key, ConsistentRead=True)


@pytest.fixture(scope="module")
def nul_streams_client(endpoint_url):
    """Streams client on the ExtendDB endpoint. The service routes Streams to a
    separate endpoint boto3 resolves on its own, so this test is ExtendDB only,
    as the stream tests in ``test_streams.py`` are."""
    if not endpoint_url:
        pytest.skip("stream record fidelity is checked against ExtendDB servers only")
    import os

    import boto3

    kwargs: dict = dict(
        service_name="dynamodbstreams",
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        endpoint_url=endpoint_url,
    )
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def test_stream_records_carry_nul_keys_names_and_values(dynamodb_client, nul_streams_client):
    """INSERT, MODIFY, and REMOVE records for an item whose key, attribute
    names, map keys, and values hold U+0000 and U+0001 come back byte-identical
    in Keys, NewImage, and OldImage."""
    from test_streams import _drain_all_shards

    with scoped_table(
        dynamodb_client,
        attribute_definitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        key_schema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    ) as name:
        stream_arn = dynamodb_client.describe_table(TableName=name)["Table"]["LatestStreamArn"]
        key = {"pk": {"S": PK}, "sk": {"S": NUL}}
        first = {**RICH_ITEM}
        dynamodb_client.put_item(TableName=name, Item=first)
        second = dynamodb_client.update_item(
            TableName=name,
            Key=key,
            UpdateExpression="SET #a = :v REMOVE #n",
            ExpressionAttributeNames={"#a": f"u{NUL}", "#n": f"n{NUL}n"},
            ExpressionAttributeValues={":v": {"S": f"{SOH}{NUL}"}},
            ReturnValues="ALL_NEW",
        )["Attributes"]
        dynamodb_client.delete_item(TableName=name, Key=key)

        deadline = time.monotonic() + 30
        records: list[dict] = []
        while time.monotonic() < deadline:
            records = _drain_all_shards(nul_streams_client, stream_arn)
            if len(records) >= 3:
                break
            time.sleep(0.5)
        assert [r["eventName"] for r in records] == ["INSERT", "MODIFY", "REMOVE"]
        for r in records:
            assert r["dynamodb"]["Keys"] == key
        assert records[0]["dynamodb"]["NewImage"] == first
        assert "OldImage" not in records[0]["dynamodb"]
        assert records[1]["dynamodb"]["OldImage"] == first
        assert records[1]["dynamodb"]["NewImage"] == second
        assert records[2]["dynamodb"]["OldImage"] == second
        assert "NewImage" not in records[2]["dynamodb"]

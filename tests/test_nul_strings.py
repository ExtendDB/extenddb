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

The PostgreSQL backend's key columns cannot hold NUL. It stores NUL in values,
attribute names and map keys, and refuses a key value that holds NUL with a
ValidationException (see docs/differences-from-dynamodb.md): the tests that
put NUL in a key skip there, and ``test_postgresql_refuses_nul_in_a_key_value``
pins that answer.

U+0001 is included because a storage encoding that handles NUL has to leave the
next code point alone as well.
"""

from __future__ import annotations

import random
import string
import time

import pytest
from botocore.exceptions import ClientError

from conftest import configured_backend, scoped_table, wait_for_gsi_items

NUL = "\u0000"
SOH = "\u0001"
ESCAPE_TEXT = "\\u0000"  # backslash, u, 0, 0, 0, 0: six characters, not a NUL

PK = f"a{NUL}b"
PK_WITHOUT_NUL = "ab"
PK_SOH = f"a{SOH}b"

# One partition, five sort keys whose byte order is: NUL, U+0001, space, the
# escape text (starts with 0x5c), then "a" (0x61).
SORT_KEYS = [NUL, SOH, " ", ESCAPE_TEXT, "a"]

ON_POSTGRESQL = configured_backend() == "postgres"
nul_keys = pytest.mark.skipif(
    ON_POSTGRESQL,
    reason="the PostgreSQL backend refuses NUL in a key value; see "
    "docs/differences-from-dynamodb.md",
)
# The two key characters the item tests run with: NUL where keys can hold it.
KEY_CHARS = [pytest.param(NUL, id="nul", marks=nul_keys), pytest.param(SOH, id="soh")]

NUL_IN_KEY = (
    "Key attribute values cannot contain the character U+0000 on the PostgreSQL "
    "storage backend, whose text columns cannot hold the byte 0x00. Remove the "
    "character from the key value, or use another storage backend."
)

# NUL in every kind of position the item model has outside the keys, which
# hold U+0001.
RICH_ITEM = {
    "pk": {"S": f"r{SOH}"},
    "sk": {"S": f"{SOH}{SOH}"},
    "g": {"S": f"g{SOH}{SOH}"},
    "v": {"S": f"v{NUL}"},
    f"n{NUL}n": {"S": "x"},
    "m": {"M": {f"k{NUL}": {"S": f"{NUL}"}, f"k{SOH}{SOH}": {"N": "1"}}},
    "l": {"L": [{"S": NUL}, {"S": f"{SOH}{NUL}{SOH}"}]},
    "ss": {"SS": [NUL, "a"]},
}
RICH_KEY = {"pk": RICH_ITEM["pk"], "sk": RICH_ITEM["sk"]}

# The item under the NUL sort key, with NUL in its GSI key and a value.
NUL_KEY_ITEM = {"pk": {"S": PK}, "sk": {"S": NUL}, "g": {"S": f"g{NUL}"}, "v": {"S": f"v{NUL}"}}

UPDATE_KEY = {"pk": {"S": PK_SOH}, "sk": {"S": " "}}


def _query(client, table, **kwargs):
    return client.query(
        TableName=table,
        KeyConditionExpression=kwargs.pop("cond", "pk = :p"),
        ExpressionAttributeValues={":p": {"S": PK}, **kwargs.pop("values", {})},
        **kwargs,
    )


def _sks(resp):
    return [i["sk"]["S"] for i in resp["Items"]]


def _error(exc) -> tuple[int, str, str]:
    err = exc.value.response
    return (err["ResponseMetadata"]["HTTPStatusCode"], err["Error"]["Code"], err["Error"]["Message"])


TABLE_SCHEMA = dict(
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
)


@pytest.fixture(scope="class")
def nul_table(dynamodb_client):
    """(S, S) table with GSI gsi1 on g. It holds RICH_ITEM, an item keyed with
    U+0001, and the item UPDATE_KEY names; where keys can hold NUL, also the
    partition PK with SORT_KEYS (NUL_KEY_ITEM under the NUL sort key) and a
    control item in the partition without the NUL. Yields the table name and
    its item count."""
    with scoped_table(dynamodb_client, **TABLE_SCHEMA) as name:
        items = [
            RICH_ITEM,
            {"pk": {"S": PK_SOH}, "sk": {"S": SOH}, "g": {"S": f"g{SOH}"}, "v": {"S": SOH}},
            {**UPDATE_KEY, "g": {"S": "plain"}},
        ]
        if not ON_POSTGRESQL:
            items.append(NUL_KEY_ITEM)
            items += [{"pk": {"S": PK}, "sk": {"S": sk}, "g": {"S": "plain"}} for sk in SORT_KEYS[1:]]
            items.append({"pk": {"S": PK_WITHOUT_NUL}, "sk": {"S": "x"}, "g": {"S": "plain"}})
        for item in items:
            dynamodb_client.put_item(TableName=name, Item=item)
        yield name, len(items)


class TestNulInStrings:
    def test_item_with_nul_outside_its_keys_round_trips(self, dynamodb_client, nul_table):
        table, _ = nul_table
        resp = dynamodb_client.get_item(TableName=table, Key=RICH_KEY, ConsistentRead=True)
        assert resp["Item"] == RICH_ITEM

    @nul_keys
    def test_item_with_nul_in_its_keys_round_trips(self, dynamodb_client, nul_table):
        table, _ = nul_table
        resp = dynamodb_client.get_item(
            TableName=table, Key={"pk": {"S": PK}, "sk": {"S": NUL}}, ConsistentRead=True
        )
        assert resp["Item"] == NUL_KEY_ITEM

    def test_soh_item_round_trips(self, dynamodb_client, nul_table):
        table, _ = nul_table
        resp = dynamodb_client.get_item(
            TableName=table, Key={"pk": {"S": PK_SOH}, "sk": {"S": SOH}}, ConsistentRead=True
        )
        assert resp["Item"] == {
            "pk": {"S": PK_SOH},
            "sk": {"S": SOH},
            "g": {"S": f"g{SOH}"},
            "v": {"S": SOH},
        }

    @nul_keys
    def test_partition_order_is_byte_order_with_nul_first(self, dynamodb_client, nul_table):
        table, _ = nul_table
        assert _sks(_query(dynamodb_client, table, ConsistentRead=True)) == SORT_KEYS
        assert (
            _sks(_query(dynamodb_client, table, ScanIndexForward=False, ConsistentRead=True))
            == SORT_KEYS[::-1]
        )

    @nul_keys
    def test_nul_and_its_escape_text_are_different_keys(self, dynamodb_client, nul_table):
        table, _ = nul_table
        by_nul = dynamodb_client.get_item(
            TableName=table, Key={"pk": {"S": PK}, "sk": {"S": NUL}}, ConsistentRead=True
        )["Item"]
        by_text = dynamodb_client.get_item(
            TableName=table, Key={"pk": {"S": PK}, "sk": {"S": ESCAPE_TEXT}}, ConsistentRead=True
        )["Item"]
        assert by_nul["sk"]["S"] == NUL and len(by_nul["sk"]["S"]) == 1
        assert by_text["sk"]["S"] == ESCAPE_TEXT and len(by_text["sk"]["S"]) == 6

    @nul_keys
    def test_partition_key_with_nul_is_distinct_from_the_key_without_it(
        self, dynamodb_client, nul_table
    ):
        table, _ = nul_table
        resp = dynamodb_client.get_item(
            TableName=table, Key={"pk": {"S": PK_WITHOUT_NUL}, "sk": {"S": NUL}}, ConsistentRead=True
        )
        assert "Item" not in resp
        resp = _query(dynamodb_client, table, values={":p": {"S": PK_WITHOUT_NUL}}, ConsistentRead=True)
        assert _sks(resp) == ["x"]

    @nul_keys
    def test_range_conditions_treat_nul_as_a_byte(self, dynamodb_client, nul_table):
        table, _ = nul_table
        below_space = _query(
            dynamodb_client, table, cond="pk = :p AND sk < :s", values={":s": {"S": " "}},
            ConsistentRead=True,
        )
        assert _sks(below_space) == [NUL, SOH]
        between = _query(
            dynamodb_client, table, cond="pk = :p AND sk BETWEEN :lo AND :hi",
            values={":lo": {"S": NUL}, ":hi": {"S": SOH}}, ConsistentRead=True,
        )
        assert _sks(between) == [NUL, SOH]
        prefix = _query(
            dynamodb_client, table, cond="pk = :p AND begins_with(sk, :pre)",
            values={":pre": {"S": NUL}}, ConsistentRead=True,
        )
        assert _sks(prefix) == [NUL]
        text_prefix = _query(
            dynamodb_client, table, cond="pk = :p AND begins_with(sk, :pre)",
            values={":pre": {"S": "\\"}}, ConsistentRead=True,
        )
        assert _sks(text_prefix) == [ESCAPE_TEXT]

    @nul_keys
    def test_pages_of_one_visit_every_key_once(self, dynamodb_client, nul_table):
        """The cursor for a NUL key travels through LastEvaluatedKey and back
        through ExclusiveStartKey; the walk must neither skip nor repeat."""
        table, _ = nul_table
        seen: list[str] = []
        kwargs: dict = {"Limit": 1, "ConsistentRead": True}
        while True:
            resp = _query(dynamodb_client, table, **kwargs)
            seen += _sks(resp)
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        assert seen == SORT_KEYS

    @pytest.mark.parametrize(
        "gsi_key,want",
        [
            pytest.param(f"g{NUL}", NUL_KEY_ITEM, id="nul", marks=nul_keys),
            pytest.param(f"g{SOH}{SOH}", RICH_ITEM, id="soh"),
        ],
    )
    def test_gsi_holds_the_item(self, dynamodb_client, nul_table, gsi_key, want):
        table, _ = nul_table

        def walk():
            return dynamodb_client.query(
                TableName=table,
                IndexName="gsi1",
                KeyConditionExpression="g = :g",
                ExpressionAttributeValues={":g": {"S": gsi_key}},
            )["Items"]

        assert wait_for_gsi_items(walk, 1) == [want]

    def test_filter_on_a_nul_named_attribute(self, dynamodb_client, nul_table):
        table, _ = nul_table
        resp = dynamodb_client.scan(
            TableName=table,
            FilterExpression="#n = :x",
            ExpressionAttributeNames={"#n": f"n{NUL}n"},
            ExpressionAttributeValues={":x": {"S": "x"}},
            ConsistentRead=True,
        )
        assert resp["Items"] == [RICH_ITEM]
        resp = _query(
            dynamodb_client, table, FilterExpression="v = :v",
            values={":p": RICH_ITEM["pk"], ":v": {"S": f"v{NUL}"}}, ConsistentRead=True,
        )
        assert resp["Items"] == [RICH_ITEM]

    def test_condition_expression_sees_the_nul_named_attribute(self, dynamodb_client, nul_table):
        table, _ = nul_table
        # Exists on the rich item: the conditional write goes through.
        dynamodb_client.update_item(
            TableName=table,
            Key=RICH_KEY,
            UpdateExpression="SET touched = :t",
            ConditionExpression="attribute_exists(#n)",
            ExpressionAttributeNames={"#n": f"n{NUL}n"},
            ExpressionAttributeValues={":t": {"BOOL": True}},
        )
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_item(
                TableName=table,
                Key=UPDATE_KEY,
                UpdateExpression="SET touched = :t",
                ConditionExpression="attribute_exists(#n)",
                ExpressionAttributeNames={"#n": f"n{NUL}n"},
                ExpressionAttributeValues={":t": {"BOOL": True}},
            )
        assert _error(exc) == (400, "ConditionalCheckFailedException", "The conditional request failed")
        dynamodb_client.update_item(TableName=table, Key=RICH_KEY, UpdateExpression="REMOVE touched")

    def test_update_item_sets_nul_named_attribute_and_nul_value(self, dynamodb_client, nul_table):
        table, _ = nul_table
        resp = dynamodb_client.update_item(
            TableName=table,
            Key=UPDATE_KEY,
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
        got = dynamodb_client.get_item(TableName=table, Key=UPDATE_KEY, ConsistentRead=True)["Item"]
        assert got[f"u{NUL}"] == {"S": f"{NUL}z"}
        assert got["m2"] == {"M": {f"{NUL}": {"L": [{"S": SOH}]}}}
        resp = dynamodb_client.update_item(
            TableName=table,
            Key=UPDATE_KEY,
            UpdateExpression="REMOVE #a, m2",
            ExpressionAttributeNames={"#a": f"u{NUL}"},
            ReturnValues="ALL_NEW",
        )
        assert f"u{NUL}" not in resp["Attributes"] and "m2" not in resp["Attributes"]

    @pytest.mark.parametrize("c", KEY_CHARS)
    def test_batch_and_transact_writes_and_reads(self, dynamodb_client, nul_table, c):
        table, _ = nul_table
        b1 = {"pk": {"S": f"batch{c}"}, "sk": {"S": c}, "g": {"S": "plain"}, f"{NUL}": {"S": NUL}}
        t1 = {"pk": {"S": f"tx{c}"}, "sk": {"S": f"{c}{c}"}, "g": {"S": "plain"}, "v": {"S": NUL}}
        dynamodb_client.batch_write_item(RequestItems={table: [{"PutRequest": {"Item": b1}}]})
        dynamodb_client.transact_write_items(
            TransactItems=[{"Put": {"TableName": table, "Item": t1}}]
        )
        got = dynamodb_client.batch_get_item(
            RequestItems={
                table: {
                    "Keys": [{"pk": b1["pk"], "sk": b1["sk"]}, {"pk": t1["pk"], "sk": t1["sk"]}],
                    "ConsistentRead": True,
                }
            }
        )["Responses"][table]
        assert sorted(got, key=lambda i: i["pk"]["S"]) == sorted([b1, t1], key=lambda i: i["pk"]["S"])
        tg = dynamodb_client.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": table, "Key": {"pk": b1["pk"], "sk": b1["sk"]}}},
                {"Get": {"TableName": table, "Key": {"pk": t1["pk"], "sk": t1["sk"]}}},
            ]
        )["Responses"]
        assert [r["Item"] for r in tg] == [b1, t1]
        dynamodb_client.batch_write_item(
            RequestItems={
                table: [
                    {"DeleteRequest": {"Key": {"pk": b1["pk"], "sk": b1["sk"]}}},
                    {"DeleteRequest": {"Key": {"pk": t1["pk"], "sk": t1["sk"]}}},
                ]
            }
        )

    def test_scan_counts_every_item(self, dynamodb_client, nul_table):
        table, count = nul_table
        resp = dynamodb_client.scan(TableName=table, Select="COUNT", ConsistentRead=True)
        assert resp["Count"] == count

    @pytest.mark.parametrize("c", KEY_CHARS)
    def test_delete_item_on_the_key(self, dynamodb_client, nul_table, c):
        table, _ = nul_table
        key = {"pk": {"S": f"del{c}"}, "sk": {"S": c}}
        dynamodb_client.put_item(TableName=table, Item={**key, "v": {"S": NUL}})
        resp = dynamodb_client.delete_item(TableName=table, Key=key, ReturnValues="ALL_OLD")
        assert resp["Attributes"] == {**key, "v": {"S": NUL}}
        assert "Item" not in dynamodb_client.get_item(TableName=table, Key=key, ConsistentRead=True)


def _long_key() -> str:
    """2046 bytes: 1364 random letters and digits with U+0001 after every
    second one, 682 in all. The service allows a partition key value of 2048
    bytes. PostgreSQL compresses a btree entry over about 2 KB when it can,
    and a key of letters alone compresses enough to fit the 2704-byte entry
    limit even with each U+0001 stored as two bytes; letters and digits do
    not."""
    rng = random.Random(368)
    key = "".join(
        rng.choice(string.ascii_letters + string.digits) + ("" if i % 2 == 0 else SOH)
        for i in range(1364)
    )
    assert (len(key.encode()), key.count(SOH)) == (2046, 682)
    return key


def test_a_long_key_holding_u0001_is_stored(dynamodb_client):
    """A 2046-byte partition key with 682 U+0001, and a GSI partition key of
    the same shape, are stored and read back. Each U+0001 stays one byte in
    the key, as every earlier release stored it."""
    key = _long_key()
    with scoped_table(dynamodb_client, **TABLE_SCHEMA) as table:
        base = {"pk": {"S": key}, "sk": {"S": "s"}}
        indexed = {"pk": {"S": "short-key1"}, "sk": {"S": "s"}, "g": {"S": key}}
        for item in (base, indexed):
            dynamodb_client.put_item(TableName=table, Item=item)
        got = dynamodb_client.get_item(TableName=table, Key=base, ConsistentRead=True)
        assert got["Item"] == base

        def walk():
            return dynamodb_client.query(
                TableName=table,
                IndexName="gsi1",
                KeyConditionExpression="g = :g",
                ExpressionAttributeValues={":g": {"S": key}},
            )["Items"]

        assert wait_for_gsi_items(walk, 1) == [indexed]


# Requests whose key value holds NUL, each answered by the PostgreSQL backend
# with NUL_IN_KEY, and none of which stores an item.
NUL_KEY = {"pk": {"S": f"a{NUL}"}, "sk": {"S": "s"}}
REFUSED = [
    ("PutItem, partition key", "put_item", lambda t: dict(TableName=t, Item=NUL_KEY)),
    (
        "PutItem, sort key",
        "put_item",
        lambda t: dict(TableName=t, Item={"pk": {"S": "p"}, "sk": {"S": NUL}}),
    ),
    (
        "PutItem, GSI key",
        "put_item",
        lambda t: dict(TableName=t, Item={"pk": {"S": "p"}, "sk": {"S": "s"}, "g": {"S": f"g{NUL}"}}),
    ),
    (
        "UpdateItem, a GSI key the update sets",
        "update_item",
        lambda t: dict(
            TableName=t,
            Key={"pk": {"S": "p"}, "sk": {"S": "s"}},
            UpdateExpression="SET g = :g",
            ExpressionAttributeValues={":g": {"S": f"g{NUL}"}},
        ),
    ),
    ("GetItem", "get_item", lambda t: dict(TableName=t, Key=NUL_KEY)),
    ("DeleteItem", "delete_item", lambda t: dict(TableName=t, Key=NUL_KEY)),
    (
        "Query, partition key",
        "query",
        lambda t: dict(
            TableName=t,
            KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": NUL_KEY["pk"]},
        ),
    ),
    (
        "Query, sort key bound",
        "query",
        lambda t: dict(
            TableName=t,
            KeyConditionExpression="pk = :p AND sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={":p": {"S": "p"}, ":lo": {"S": NUL}, ":hi": {"S": "z"}},
        ),
    ),
    (
        "Query, GSI key",
        "query",
        lambda t: dict(
            TableName=t,
            IndexName="gsi1",
            KeyConditionExpression="g = :g",
            ExpressionAttributeValues={":g": {"S": f"g{NUL}"}},
        ),
    ),
    (
        "BatchWriteItem",
        "batch_write_item",
        lambda t: dict(RequestItems={t: [{"PutRequest": {"Item": NUL_KEY}}]}),
    ),
    ("BatchGetItem", "batch_get_item", lambda t: dict(RequestItems={t: {"Keys": [NUL_KEY]}})),
    (
        "TransactWriteItems",
        "transact_write_items",
        lambda t: dict(TransactItems=[{"Put": {"TableName": t, "Item": NUL_KEY}}]),
    ),
    (
        "TransactGetItems",
        "transact_get_items",
        lambda t: dict(TransactItems=[{"Get": {"TableName": t, "Key": NUL_KEY}}]),
    ),
]


@pytest.mark.skipif(not ON_POSTGRESQL, reason="the refusal is the PostgreSQL backend's")
def test_postgresql_refuses_nul_in_a_key_value(dynamodb_client):
    with scoped_table(dynamodb_client, **TABLE_SCHEMA) as table:
        for label, operation, request in REFUSED:
            with pytest.raises(ClientError) as exc:
                getattr(dynamodb_client, operation)(**request(table))
            assert _error(exc) == (400, "ValidationException", NUL_IN_KEY), label
        # An Update member that computes such a GSI key cancels the
        # transaction with the same message.
        with pytest.raises(ClientError) as exc:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": table,
                            "Key": {"pk": {"S": "p"}, "sk": {"S": "s"}},
                            "UpdateExpression": "SET g = :g",
                            "ExpressionAttributeValues": {":g": {"S": f"g{NUL}"}},
                        }
                    }
                ]
            )
        assert _error(exc) == (
            400,
            "TransactionCanceledException",
            "Transaction cancelled, please refer cancellation reasons for specific reasons "
            "[ValidationError]",
        )
        assert exc.value.response["CancellationReasons"] == [
            {"Code": "ValidationError", "Message": NUL_IN_KEY}
        ]
        resp = dynamodb_client.scan(TableName=table, Select="COUNT", ConsistentRead=True)
        assert resp["Count"] == 0, "a refused write stored an item"


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


def test_stream_records_carry_nul_names_and_values(dynamodb_client, nul_streams_client):
    """INSERT, MODIFY, and REMOVE records for an item whose key holds U+0001
    and whose attribute names, map keys, and values hold U+0000 and U+0001
    come back byte-identical in Keys, NewImage, and OldImage."""
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
        first = {**RICH_ITEM}
        dynamodb_client.put_item(TableName=name, Item=first)
        second = dynamodb_client.update_item(
            TableName=name,
            Key=RICH_KEY,
            UpdateExpression="SET #a = :v REMOVE #n",
            ExpressionAttributeNames={"#a": f"u{NUL}", "#n": f"n{NUL}n"},
            ExpressionAttributeValues={":v": {"S": f"{SOH}{NUL}"}},
            ReturnValues="ALL_NEW",
        )["Attributes"]
        dynamodb_client.delete_item(TableName=name, Key=RICH_KEY)

        deadline = time.monotonic() + 30
        records: list[dict] = []
        while time.monotonic() < deadline:
            records = _drain_all_shards(nul_streams_client, stream_arn)
            if len(records) >= 3:
                break
            time.sleep(0.5)
        assert [r["eventName"] for r in records] == ["INSERT", "MODIFY", "REMOVE"]
        for r in records:
            assert r["dynamodb"]["Keys"] == RICH_KEY
        assert records[0]["dynamodb"]["NewImage"] == first
        assert "OldImage" not in records[0]["dynamodb"]
        assert records[1]["dynamodb"]["OldImage"] == first
        assert records[1]["dynamodb"]["NewImage"] == second
        assert records[2]["dynamodb"]["OldImage"] == second
        assert "NewImage" not in records[2]["dynamodb"]

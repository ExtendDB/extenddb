# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Dual-target wire tests for PartiQL SELECT (ExecuteStatement).

Runs against real DynamoDB (EXTENDDB_TEST_ENDPOINT unset, AWS creds) and against
an extenddb server (EXTENDDB_TEST_ENDPOINT set). Identical assertions on both,
so the service is the oracle for every case. Every expected value and error
string was measured on the live service; see conf-run/partiql/char-select.md.

The table is created with a unique ``extenddb-measure-2a-`` prefix and deleted in
a finally so nothing is left in the live account.
"""

from __future__ import annotations

import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError


# ---- local polling helpers (independent of conftest internals) -------------

def _is_service() -> bool:
    import os
    return not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()


def _interval() -> float:
    return 0.3 if _is_service() else 0.03


def _wait_active(client, name: str, timeout: float = 180.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        d = client.describe_table(TableName=name)["Table"]
        if d["TableStatus"] == "ACTIVE":
            gsis = d.get("GlobalSecondaryIndexes", [])
            if all(g.get("IndexStatus") == "ACTIVE" for g in gsis):
                return
        time.sleep(_interval())
    raise TimeoutError(f"{name} not ACTIVE within {timeout}s")


def _wait_deleted(client, name: str, timeout: float = 180.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client.describe_table(TableName=name)
        except client.exceptions.ResourceNotFoundException:
            return
        time.sleep(_interval())


def _items(resp) -> list:
    return resp.get("Items", [])


def _poll_statement(client, timeout: float = 20.0, want: int = 0, **kwargs) -> list:
    """Run an ExecuteStatement, retrying until it yields >= ``want`` items.

    A GSI is eventually consistent (on the service and on extenddb), so a query
    right after the seed can be short until propagation completes.
    """
    deadline = time.monotonic() + timeout
    items = _items(client.execute_statement(**kwargs))
    while len(items) < want and time.monotonic() < deadline:
        time.sleep(_interval())
        items = _items(client.execute_statement(**kwargs))
    return items


# ---- seeded table fixture --------------------------------------------------

@pytest.fixture(scope="module")
def table(dynamodb_client):
    name = f"extenddb-measure-2a-{uuid.uuid4().hex[:12]}"
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
            {"AttributeName": "gsi_sk", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [
                    {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi_sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    try:
        _wait_active(dynamodb_client, name)
        items = [
            {
                "pk": {"S": "user1"}, "sk": {"S": "profile"},
                "name": {"S": "Alice"}, "age": {"N": "30"},
                "addr": {"M": {"city": {"S": "NYC"}, "zip": {"S": "10001"}}},
                "tags": {"L": [{"S": "a"}, {"S": "b"}]},
                "Comment": {"S": "hello"},
                "gsi_pk": {"S": "g1"}, "gsi_sk": {"N": "1"},
            },
            {
                "pk": {"S": "user1"}, "sk": {"S": "post#1"},
                "name": {"S": "Post One"}, "age": {"N": "5"},
                "gsi_pk": {"S": "g1"}, "gsi_sk": {"N": "2"},
            },
            {
                "pk": {"S": "user1"}, "sk": {"S": "post#2"},
                "name": {"S": "Post Two"}, "age": {"N": "25"},
                "gsi_pk": {"S": "g1"}, "gsi_sk": {"N": "3"},
            },
            {
                "pk": {"S": "user1"}, "sk": {"S": "post#3"},
                "name": {"S": "Post Three"}, "age": {"N": "50"},
            },
            {"pk": {"S": "user2"}, "sk": {"S": "profile"}, "name": {"S": "Bob"}, "age": {"N": "40"}},
            {"pk": {"S": "user3"}, "sk": {"S": "profile"}, "name": {"S": "Carol"}, "age": {"N": "10"}},
        ]
        for it in items:
            dynamodb_client.put_item(TableName=name, Item=it)
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
            _wait_deleted(dynamodb_client, name)
        except ClientError:
            pass


def q(client, table, where="", proj="*", **kwargs):
    stmt = f'SELECT {proj} FROM "{table}"'
    if where:
        stmt += f" WHERE {where}"
    return client.execute_statement(Statement=stmt, **kwargs)


def sks(items) -> list:
    return [i["sk"]["S"] for i in items]


# ---- planning: point read / Query / Scan -----------------------------------

def test_full_key_point_read(dynamodb_client, table):
    items = _items(q(dynamodb_client, table, "pk='user1' AND sk='profile'"))
    assert len(items) == 1
    assert items[0]["sk"]["S"] == "profile"


def test_point_read_miss_is_empty(dynamodb_client, table):
    assert _items(q(dynamodb_client, table, "pk='nope' AND sk='nope'")) == []


def test_partition_only_query_ascending(dynamodb_client, table):
    assert sks(_items(q(dynamodb_client, table, "pk='user1'"))) == [
        "post#1", "post#2", "post#3", "profile",
    ]


def test_sort_range_query(dynamodb_client, table):
    assert sks(_items(q(dynamodb_client, table, "pk='user1' AND sk > 'post#0'"))) == [
        "post#1", "post#2", "post#3", "profile",
    ]


def test_begins_with_sort_key(dynamodb_client, table):
    assert sks(_items(q(dynamodb_client, table, "pk='user1' AND begins_with(\"sk\", 'post#')"))) == [
        "post#1", "post#2", "post#3",
    ]


def test_between_sort_key(dynamodb_client, table):
    assert sks(_items(q(dynamodb_client, table, "pk='user1' AND sk BETWEEN 'post#1' AND 'post#3'"))) == [
        "post#1", "post#2", "post#3",
    ]


def test_no_where_scans_all(dynamodb_client, table):
    assert len(_items(q(dynamodb_client, table))) == 6


def test_sort_only_is_scan_filter(dynamodb_client, table):
    pks = sorted(i["pk"]["S"] for i in _items(q(dynamodb_client, table, "sk='profile'")))
    assert pks == ["user1", "user2", "user3"]


def test_non_key_filter_scan(dynamodb_client, table):
    assert len(_items(q(dynamodb_client, table, "age > 20"))) == 4


def test_pk_eq_plus_non_key_filter_is_query(dynamodb_client, table):
    assert sorted(sks(_items(q(dynamodb_client, table, "pk='user1' AND age > 20")))) == [
        "post#2", "post#3", "profile",
    ]


def test_in_on_sort_key(dynamodb_client, table):
    assert sorted(sks(_items(q(dynamodb_client, table, "pk='user1' AND sk IN ['post#1','post#2']")))) == [
        "post#1", "post#2",
    ]


def test_order_by_desc(dynamodb_client, table):
    stmt = f'SELECT * FROM "{table}" WHERE pk=\'user1\' ORDER BY sk DESC'
    assert sks(_items(dynamodb_client.execute_statement(Statement=stmt))) == [
        "profile", "post#3", "post#2", "post#1",
    ]


# ---- projection ------------------------------------------------------------

def test_projection_subset(dynamodb_client, table):
    items = _items(q(dynamodb_client, table, "pk='user1' AND sk='profile'", proj="pk, sk, name"))
    assert set(items[0].keys()) == {"pk", "sk", "name"}


def test_projection_nested_path_keys_on_leaf(dynamodb_client, table):
    items = _items(q(dynamodb_client, table, "pk='user1' AND sk='profile'", proj="addr.city"))
    assert items[0] == {"city": {"S": "NYC"}}


def test_projection_list_index_keys_on_literal_text(dynamodb_client, table):
    items = _items(q(dynamodb_client, table, "pk='user1' AND sk='profile'", proj="tags[0]"))
    assert items[0] == {"tags[0]": {"S": "a"}}


def test_projection_reserved_word_attribute(dynamodb_client, table):
    items = _items(q(dynamodb_client, table, "pk='user1' AND sk='profile'", proj="Comment"))
    assert items[0] == {"Comment": {"S": "hello"}}


# ---- parameters ------------------------------------------------------------

def test_positional_parameters(dynamodb_client, table):
    stmt = f'SELECT * FROM "{table}" WHERE pk=? AND sk=?'
    items = _items(dynamodb_client.execute_statement(
        Statement=stmt, Parameters=[{"S": "user1"}, {"S": "profile"}]))
    assert len(items) == 1


def test_too_few_parameters(dynamodb_client, table):
    stmt = f'SELECT * FROM "{table}" WHERE pk=? AND sk=?'
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(Statement=stmt, Parameters=[{"S": "user1"}])
    assert e.value.response["Error"]["Code"] == "ValidationException"
    assert e.value.response["Error"]["Message"] == (
        "Number of parameters in request and statement don't match."
    )


# ---- pagination: Limit + NextToken -----------------------------------------

def test_limit_and_next_token_round_trip(dynamodb_client, table):
    stmt = f'SELECT * FROM "{table}" WHERE pk=\'user1\''
    collected: list = []
    token = None
    for _ in range(6):
        kwargs = {"Statement": stmt, "Limit": 2}
        if token:
            kwargs["NextToken"] = token
        resp = dynamodb_client.execute_statement(**kwargs)
        collected.extend(sks(_items(resp)))
        token = resp.get("NextToken")
        if not token:
            break
    assert collected == ["post#1", "post#2", "post#3", "profile"]


def test_next_token_bound_to_statement(dynamodb_client, table):
    first = dynamodb_client.execute_statement(
        Statement=f'SELECT * FROM "{table}" WHERE pk=\'user1\'', Limit=2)
    token = first["NextToken"]
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(
            Statement=f'SELECT * FROM "{table}" WHERE pk=\'user1\' AND sk > \'post#0\'',
            Limit=2, NextToken=token)
    assert e.value.response["Error"]["Code"] == "ValidationException"
    assert e.value.response["Error"]["Message"] == "NextToken does not match request"


# ---- index targeting -------------------------------------------------------

def test_gsi_query(dynamodb_client, table):
    stmt = f'SELECT * FROM "{table}"."gsi1" WHERE gsi_pk=\'g1\''
    items = _poll_statement(dynamodb_client, want=3, Statement=stmt)
    assert [i["gsi_sk"]["N"] for i in items] == ["1", "2", "3"]


def test_gsi_strong_read_rejected(dynamodb_client, table):
    stmt = f'SELECT * FROM "{table}"."gsi1" WHERE gsi_pk=\'g1\''
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(Statement=stmt, ConsistentRead=True)
    assert e.value.response["Error"]["Code"] == "ValidationException"
    assert e.value.response["Error"]["Message"] == (
        "Strongly consistent read is not supported on Global Secondary Indexes"
    )


def test_missing_index(dynamodb_client, table):
    stmt = f'SELECT * FROM "{table}"."nope" WHERE gsi_pk=\'g1\''
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(Statement=stmt)
    assert e.value.response["Error"]["Code"] == "ValidationException"
    assert e.value.response["Error"]["Message"] == "The table does not have the specified index"


# ---- errors ----------------------------------------------------------------

def test_missing_table(dynamodb_client):
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(
            Statement='SELECT * FROM "extenddb-measure-2a-does-not-exist-xyz" WHERE pk=\'x\'')
    assert e.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert e.value.response["Error"]["Message"] == "Requested resource not found"


def test_aliasing_rejected(dynamodb_client, table):
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(
            Statement=f'SELECT name AS n FROM "{table}" WHERE pk=\'user1\' AND sk=\'profile\'')
    assert e.value.response["Error"]["Code"] == "ValidationException"
    assert e.value.response["Error"]["Message"] == "Aliasing is not supported"


def test_empty_in_list_rejected(dynamodb_client, table):
    with pytest.raises(ClientError) as e:
        dynamodb_client.execute_statement(
            Statement=f'SELECT * FROM "{table}" WHERE pk=\'user1\' AND sk IN []')
    assert e.value.response["Error"]["Code"] == "ValidationException"
    assert e.value.response["Error"]["Message"] == (
        "IN operator must have a left hand argument of type Variable Reference "
        "and right hand argument of type Seq with at least one member"
    )

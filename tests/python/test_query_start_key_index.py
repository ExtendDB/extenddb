# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query ExclusiveStartKey with a sort key condition on a GSI or LSI (issue #440).

Every row here was measured on Amazon DynamoDB (2026-10-23 and 2026-10-24,
each row twice with identical results). On an index a repeat of the start
key's sort value counts as a later key, because index keys repeat: a start
key at the bound is accepted for `<=`, reverse `>=`, BETWEEN at both ends,
reverse `begins_with` and `=` with the start key itself. Strict operators
still refuse the bound. A start key from another partition with a sort key
condition present gets "The query can return at most one row and cannot be
restarted".
"""

from __future__ import annotations

import time

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

RANGE_PREDICATE = "The provided starting key does not match the range key predicate"
AT_MOST_ONE_ROW = "The query can return at most one row and cannot be restarted"


def _assert_validation(exc_info, message):
    err = exc_info.value.response["Error"]
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert err["Code"] == "ValidationException"
    assert err["Message"] == message


def _run(client, expected, **query):
    """Run a Query; `expected` is a sentence (refused) or an item count (accepted)."""
    if isinstance(expected, str):
        with pytest.raises(ClientError) as exc_info:
            client.query(**query)
        _assert_validation(exc_info, expected)
        return
    resp = client.query(**query)
    assert resp["Count"] == expected


def _wait_for_index(client, table, index, key, value, count):
    """GSI writes propagate asynchronously; wait until the index holds them."""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        resp = client.query(
            TableName=table,
            IndexName=index,
            KeyConditionExpression=f"{key} = :v",
            ExpressionAttributeValues={":v": {"S": value}},
        )
        if resp["Count"] == count:
            return
        time.sleep(0.5)
    raise TimeoutError(f"index {index} did not reach {count} items")


@pytest.fixture(scope="module")
def index_range_table(dynamodb_client):
    """pk+sk table with a GSI (g, gs) and an LSI (ls); partition alice with sk
    a, aa, ab, abz, m, z (gs = "g" + sk, ls = "l" + sk), plus bob/a."""
    name = unique_name("esk-index-range")
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "g", "AttributeType": "S"},
            {"AttributeName": "gs", "AttributeType": "S"},
            {"AttributeName": "ls", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-g",
                "KeySchema": [
                    {"AttributeName": "g", "KeyType": "HASH"},
                    {"AttributeName": "gs", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "by-l",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "ls", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    wait_for_active(dynamodb_client, name)
    for pk, sk, g in (
        ("alice", "a", "one"),
        ("alice", "aa", "one"),
        ("alice", "ab", "one"),
        ("alice", "abz", "one"),
        ("alice", "m", "one"),
        ("alice", "z", "one"),
        ("bob", "a", "two"),
    ):
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": pk},
                "sk": {"S": sk},
                "g": {"S": g},
                "gs": {"S": "g" + sk},
                "ls": {"S": "l" + sk},
            },
        )
    _wait_for_index(dynamodb_client, name, "by-g", "g", "one", 6)
    yield name
    try:
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client, name)
    except Exception:
        pass


GS_GE = "g = :g AND gs >= :v"
GS_GT = "g = :g AND gs > :v"
GS_LE = "g = :g AND gs <= :v"
GS_LT = "g = :g AND gs < :v"
GS_EQ = "g = :g AND gs = :v"
GS_BETWEEN = "g = :g AND gs BETWEEN :lo AND :hi"
GS_BEGINS = "g = :g AND begins_with(gs, :v)"

LS_GE = "pk = :p AND ls >= :v"
LS_GT = "pk = :p AND ls > :v"
LS_LE = "pk = :p AND ls <= :v"
LS_EQ = "pk = :p AND ls = :v"
LS_BETWEEN = "pk = :p AND ls BETWEEN :lo AND :hi"
LS_BEGINS = "pk = :p AND begins_with(ls, :v)"


def _gsi(table, kce, values, esk, forward=True):
    """esk is (pk, sk, g, gs)."""
    return {
        "TableName": table,
        "IndexName": "by-g",
        "KeyConditionExpression": kce,
        "ExpressionAttributeValues": {
            ":g": {"S": "one"},
            **{k: {"S": v} for k, v in values.items()},
        },
        "ExclusiveStartKey": {
            "pk": {"S": esk[0]},
            "sk": {"S": esk[1]},
            "g": {"S": esk[2]},
            "gs": {"S": esk[3]},
        },
        "ScanIndexForward": forward,
    }


def _lsi(table, kce, values, esk, forward=True):
    """esk is (pk, sk, ls)."""
    return {
        "TableName": table,
        "IndexName": "by-l",
        "KeyConditionExpression": kce,
        "ExpressionAttributeValues": {
            ":p": {"S": "alice"},
            **{k: {"S": v} for k, v in values.items()},
        },
        "ExclusiveStartKey": {
            "pk": {"S": esk[0]},
            "sk": {"S": esk[1]},
            "ls": {"S": esk[2]},
        },
        "ScanIndexForward": forward,
    }


M = {":v": "gm"}
A_M = {":lo": "ga", ":hi": "gm"}
ALICE_M = ("alice", "m", "one", "gm")

# (kce, bound values, start key (pk, sk, g, gs), forward, expected)
GSI_ROWS = [
    # Below the bound: the range sentence.
    (GS_GE, M, ("alice", "a", "one", "ga"), True, RANGE_PREDICATE),
    # At the bound: accepted, because a repeat of the sort value is a later key.
    (GS_LE, M, ALICE_M, True, 0),
    (GS_GE, M, ALICE_M, False, 0),
    (GS_BETWEEN, A_M, ALICE_M, True, 0),
    (GS_BETWEEN, A_M, ("alice", "a", "one", "ga"), False, 0),
    (GS_BEGINS, M, ALICE_M, False, 0),
    (GS_EQ, M, ALICE_M, True, 0),
    (GS_EQ, M, ALICE_M, False, 0),
    # Strict operators still refuse the bound.
    (GS_GT, M, ALICE_M, True, RANGE_PREDICATE),
    (GS_LT, M, ALICE_M, True, RANGE_PREDICATE),
    # Another index partition with a range condition: the fifth sentence.
    (GS_GE, {":v": "ga"}, ("bob", "m", "two", "gm"), True, AT_MOST_ONE_ROW),
    (GS_EQ, M, ("bob", "m", "two", "gm"), True, AT_MOST_ONE_ROW),
    (GS_EQ, M, ("bob", "m", "two", "gm"), False, AT_MOST_ONE_ROW),
    (
        GS_BETWEEN,
        {":lo": "ga", ":hi": "gz"},
        ("bob", "m", "two", "gm"),
        True,
        AT_MOST_ONE_ROW,
    ),
]


@pytest.mark.parametrize(
    "row",
    GSI_ROWS,
    ids=[
        f"{r[0].split('AND ', 1)[1]}|{','.join(r[1].values())}|esk={r[2][2]}/{r[2][3]}|{'fwd' if r[3] else 'rev'}"
        for r in GSI_ROWS
    ],
)
def test_gsi_start_key(index_range_table, dynamodb_client, row):
    kce, values, esk, forward, expected = row
    _run(dynamodb_client, expected, **_gsi(index_range_table, kce, values, esk, forward))


LM = {":v": "lm"}
LA_LM = {":lo": "la", ":hi": "lm"}
ALICE_LM = ("alice", "m", "lm")

# (kce, bound values, start key (pk, sk, ls), forward, expected)
LSI_ROWS = [
    (LS_GE, LM, ("alice", "a", "la"), True, RANGE_PREDICATE),
    (LS_LE, LM, ALICE_LM, True, 0),
    (LS_BETWEEN, LA_LM, ALICE_LM, True, 0),
    (LS_GE, LM, ALICE_LM, False, 0),
    (LS_EQ, LM, ALICE_LM, True, 0),
    (LS_BEGINS, {":v": "la"}, ("alice", "a", "la"), False, 0),
    (LS_GT, LM, ALICE_LM, True, RANGE_PREDICATE),
    # Another partition: outside the range wins; inside gets the fifth sentence.
    (LS_GE, LM, ("bob", "a", "la"), True, RANGE_PREDICATE),
    (LS_GE, {":v": "la"}, ("bob", "m", "lm"), True, AT_MOST_ONE_ROW),
    (LS_EQ, LM, ("bob", "m", "lm"), True, AT_MOST_ONE_ROW),
]


@pytest.mark.parametrize(
    "row",
    LSI_ROWS,
    ids=[
        f"{r[0].split('AND ', 1)[1]}|{','.join(r[1].values())}|esk={r[2][0]}/{r[2][2]}|{'fwd' if r[3] else 'rev'}"
        for r in LSI_ROWS
    ],
)
def test_lsi_start_key(index_range_table, dynamodb_client, row):
    kce, values, esk, forward, expected = row
    _run(dynamodb_client, expected, **_lsi(index_range_table, kce, values, esk, forward))


class TestIndexKeysThatRepeat:
    """An index key is not unique, so a key equal to the bound can have later
    keys that differ only in the base key."""

    @pytest.fixture()
    def table(self, dynamodb_client):
        name = unique_name("esk-dup")
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"},
                {"AttributeName": "gs", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "by-g",
                    "KeySchema": [
                        {"AttributeName": "g", "KeyType": "HASH"},
                        {"AttributeName": "gs", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        try:
            wait_for_active(dynamodb_client, name)
            for pk in ("d1", "d2", "d3"):
                dynamodb_client.put_item(
                    TableName=name,
                    Item={"pk": {"S": pk}, "g": {"S": "one"}, "gs": {"S": "dup"}},
                )
            _wait_for_index(dynamodb_client, name, "by-g", "g", "one", 3)
            yield name
        finally:
            try:
                dynamodb_client.delete_table(TableName=name)
                wait_for_deleted(dynamodb_client, name)
            except Exception:
                pass

    @pytest.mark.parametrize("forward", [True, False])
    @pytest.mark.parametrize("kce", ["g = :g AND gs = :v", "g = :g AND gs <= :v"])
    def test_walk_over_identical_index_keys_is_accepted_back(
        self, table, dynamodb_client, kce, forward
    ):
        seen, start, pages = [], None, 0
        while True:
            query = {
                "TableName": table,
                "IndexName": "by-g",
                "KeyConditionExpression": kce,
                "ExpressionAttributeValues": {":g": {"S": "one"}, ":v": {"S": "dup"}},
                "Limit": 1,
                "ScanIndexForward": forward,
            }
            if start:
                query["ExclusiveStartKey"] = start
            resp = dynamodb_client.query(**query)
            seen.extend(item["pk"]["S"] for item in resp["Items"])
            start = resp.get("LastEvaluatedKey")
            pages += 1
            assert pages < 10
            if not start:
                break
        assert sorted(seen) == ["d1", "d2", "d3"]

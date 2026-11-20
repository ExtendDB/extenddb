# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Long and deeply nested condition expressions on UpdateItem and DeleteItem.

Amazon DynamoDB evaluates a condition whose AND, OR and NOT chains run 150
terms long or nest 140 levels deep, and a legacy ``Expected`` map of 120
entries joined by ``ConditionalOperator``: it answers
ConditionalCheckFailedException when the condition fails and applies the
write when it holds. Measured on 2026-10-25 at 18:34 and 19:53 UTC
(us-east-1), each failing case twice.

On MongoDB such a condition on a table with no index and no stream used to be
compiled to one filter nested one level per operator, which MongoDB refused,
and the request answered InternalServerError. The server takes that path for
60 seconds after a write records that the table has no index, so each test
gets a table of its own and writes an item to it before the conditional
write.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

NAMES = {"#v": "v", "#u": "u"}
VALUES = {":v": {"S": "x"}, ":w": {"S": "y"}, ":x": {"S": "z"}}
UPDATE = "SET #u = :x"


def or_chain(n: int, term: str = "#v = :w") -> str:
    return " OR ".join([term] * n)


def and_chain(n: int, term: str = "attribute_exists(pk)") -> str:
    return " AND ".join([term] * n)


def alternating(depth: int) -> str:
    """``#v = :w OR (#v = :w AND (#v = :w OR (...)))``, ``depth`` operators deep."""
    expr = "#v = :w"
    for i in range(depth):
        expr = f"#v = :w {'OR' if i % 2 == 0 else 'AND'} ({expr})"
    return expr


def _maps(*exprs: str) -> dict:
    text = " ".join(exprs)
    out = {}
    names = {k: v for k, v in NAMES.items() if k in text}
    values = {k: v for k, v in VALUES.items() if k in text}
    if names:
        out["ExpressionAttributeNames"] = names
    if values:
        out["ExpressionAttributeValues"] = values
    return out


def update(client, table, key, condition):
    return client.update_item(
        TableName=table,
        Key=key,
        UpdateExpression=UPDATE,
        ConditionExpression=condition,
        **_maps(UPDATE, condition),
    )


def delete(client, table, key, condition):
    return client.delete_item(
        TableName=table, Key=key, ConditionExpression=condition, **_maps(condition)
    )


@pytest.fixture
def table(dynamodb_client):
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
    ) as name:
        yield name


@pytest.fixture
def warm(dynamodb_client, table):
    dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "warm"}, "sk": {"S": "warm"}})


@pytest.fixture
def live_key(dynamodb_client, table, warm):
    key = {"pk": {"S": f"live-{uuid.uuid4().hex[:8]}"}, "sk": {"S": "live"}}
    dynamodb_client.put_item(TableName=table, Item={**key, "v": {"S": "x"}})
    return key


def _get(client, table, key):
    return client.get_item(TableName=table, Key=key, ConsistentRead=True).get("Item")


@pytest.mark.parametrize(
    "write, condition",
    [
        (update, or_chain(150)),
        (delete, or_chain(150)),
        (update, and_chain(150)),
        (delete, and_chain(150)),
        (update, alternating(100)),
        (delete, alternating(140)),
        (update, f"attribute_exists(pk) AND ({or_chain(120)})"),
    ],
    ids=[
        "update-or-150",
        "delete-or-150",
        "update-and-150",
        "delete-and-150",
        "update-alternating-100",
        "delete-alternating-140",
        "update-or-120-inside-and",
    ],
)
def test_failing_condition_is_evaluated(dynamodb_client, table, warm, write, condition):
    key = {"pk": {"S": f"absent-{uuid.uuid4().hex[:8]}"}, "sk": {"S": "absent"}}
    with pytest.raises(ClientError) as exc_info:
        write(dynamodb_client, table, key, condition)
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ConditionalCheckFailedException", err
    assert _get(dynamodb_client, table, key) is None


@pytest.mark.parametrize(
    "condition",
    [
        or_chain(149) + " OR #v = :v",
        and_chain(150),
        "#v = :v OR (" + alternating(99) + ")",
    ],
    ids=["or-150-last-holds", "and-150", "alternating-100-outermost-holds"],
)
def test_update_with_holding_condition_applies(dynamodb_client, table, live_key, condition):
    update(dynamodb_client, table, live_key, condition)
    assert _get(dynamodb_client, table, live_key)["u"] == {"S": "z"}


@pytest.mark.parametrize(
    "condition",
    [
        or_chain(149) + " OR #v = :v",
        and_chain(150),
        "#v = :v OR (" + alternating(139) + ")",
    ],
    ids=["or-150-last-holds", "and-150", "alternating-140-outermost-holds"],
)
def test_delete_with_holding_condition_applies(dynamodb_client, table, live_key, condition):
    delete(dynamodb_client, table, live_key, condition)
    assert _get(dynamodb_client, table, live_key) is None


def expected(n: int, last_holds: bool = False) -> dict:
    """``n`` legacy ``Expected`` entries ``a<i> EQ "no"``; the last one ``EQ "x"``."""
    entries = {
        f"a{i}": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": "no"}]}
        for i in range(n)
    }
    if last_holds:
        entries[f"a{n - 1}"]["AttributeValueList"] = [{"S": "x"}]
    return entries


ATTRIBUTE_UPDATES = {"u": {"Action": "PUT", "Value": {"S": "z"}}}


@pytest.mark.parametrize("operation", ["update_item", "delete_item"])
def test_legacy_expected_failing_is_evaluated(dynamodb_client, table, warm, operation):
    key = {"pk": {"S": f"absent-{uuid.uuid4().hex[:8]}"}, "sk": {"S": "absent"}}
    extra = {"AttributeUpdates": ATTRIBUTE_UPDATES} if operation == "update_item" else {}
    with pytest.raises(ClientError) as exc_info:
        getattr(dynamodb_client, operation)(
            TableName=table, Key=key, Expected=expected(120), ConditionalOperator="OR", **extra
        )
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ConditionalCheckFailedException", err
    assert _get(dynamodb_client, table, key) is None


@pytest.mark.parametrize("operation", ["update_item", "delete_item"])
def test_legacy_expected_holding_applies(dynamodb_client, table, warm, operation):
    key = {"pk": {"S": f"live-{uuid.uuid4().hex[:8]}"}, "sk": {"S": "live"}}
    dynamodb_client.put_item(TableName=table, Item={**key, "a119": {"S": "x"}})
    extra = {"AttributeUpdates": ATTRIBUTE_UPDATES} if operation == "update_item" else {}
    getattr(dynamodb_client, operation)(
        TableName=table,
        Key=key,
        Expected=expected(120, last_holds=True),
        ConditionalOperator="OR",
        **extra,
    )
    item = _get(dynamodb_client, table, key)
    if operation == "update_item":
        assert item["u"] == {"S": "z"}
    else:
        assert item is None

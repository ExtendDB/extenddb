# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""List indexes in the document paths of each expression.

Amazon DynamoDB takes a list index from 0 to 2147483647. It refuses a larger
index with the index as the request wrote it, the first one of the
expression, and it refuses an index of two or more digits that starts with 0
as a syntax error. A syntax error anywhere in the expression comes first. In
the update and condition expressions of UpdateItem, PutItem and DeleteItem the
refusal starts with "1 validation error detected: ". In the expressions of
TransactWriteItems and of the reads it does not.

Every expected answer was measured on Amazon DynamoDB in us-east-1 on 13, 14
and 17 Nov 2026, each request sent twice. Dual-target.
"""

from __future__ import annotations

from functools import partial

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

KEY = {"pk": {"S": "k"}}
NO_ITEM = {"pk": {"S": "missing"}}
ITEM = {"pk": {"S": "k"}, "l": {"L": [{"S": "a"}, {"S": "b"}]}}
V = {":v": {"S": "c"}}
COUNT = "1 validation error detected: "


def _range(index: str) -> str:
    return f"List index is not within the allowable range; index: [{index}]"


def _syntax(token: str, near: str) -> str:
    return f'Syntax error; token: "{token}", near: "{near}"'


def _req(
    operation: str, key: str, text: str, values: dict | None = None, **extra
) -> tuple[str, dict]:
    """`operation` with the expression `text` under `key`, first in its parameters."""
    params = {key: text, **extra}
    if values:
        params["ExpressionAttributeValues"] = values
    return (operation, params)


def _transact(
    action: str, key: str, text: str, values: dict | None = None, **extra
) -> tuple[str, dict]:
    """A TransactWriteItems request of one `action`, built as `_req` builds it."""
    _, params = _req(action, key, text, values, **extra)
    return ("transact_write_items", {"TransactItems": [{action: params}]})


_get = partial(_req, "get_item", "ProjectionExpression", Key=NO_ITEM)
_update = partial(_req, "update_item", "UpdateExpression", Key=KEY)
_put = partial(_req, "put_item", "ConditionExpression", Item=ITEM)
_delete = partial(_req, "delete_item", "ConditionExpression", Key=KEY)
_scan = partial(_req, "scan", "FilterExpression", values=V)
_query = partial(_req, "query", "KeyConditionExpression", values={":p": {"S": "k"}, **V})

PROJECTION = "Invalid ProjectionExpression: "
UPDATE = COUNT + "Invalid UpdateExpression: "
CONDITION = COUNT + "Invalid ConditionExpression: "
FILTER = "Invalid FilterExpression: "
KEY_CONDITION = "Invalid KeyConditionExpression: "
TRANSACT_UPDATE = "Invalid UpdateExpression: "
TRANSACT_CONDITION = "Invalid ConditionExpression: "
N = {":n": {"N": "1"}}
SS = {":ss": {"SS": ["x"]}}

REFUSED = [
    (_get("l[4294967294]", Key=KEY), PROJECTION + _range("4294967294")),
    (_get("l[4294967295]", Key=KEY), PROJECTION + _range("4294967295")),
    (_get("l[4294967296]", Key=KEY), PROJECTION + _range("4294967296")),
    (_get("l[2147483648]"), PROJECTION + _range("2147483648")),
    (_get("l[99999999999]"), PROJECTION + _range("99999999999")),
    (_get("l[99999999999999999999]"), PROJECTION + _range("99999999999999999999")),
    (_get("a[1], l[99999999999], m[99999999999999]"), PROJECTION + _range("99999999999")),
    (_get("l[0][99999999999]"), PROJECTION + _range("99999999999")),
    (_get("l, l[99999999999]"), PROJECTION + _range("99999999999")),
    (_get("l[01]"), PROJECTION + _syntax("1", "01]")),
    (_get("l[0000000001]"), PROJECTION + _syntax("0", "000")),
    (_get("l[00]", Key=KEY), PROJECTION + _syntax("0", "00]")),
    (_get("l[012]"), PROJECTION + _syntax("12", "012]")),
    (_get("l[ 01]"), PROJECTION + _syntax("1", "01]")),
    (_get("l[01].x"), PROJECTION + _syntax("1", "01]")),
    (_get("l[02147483648]"), PROJECTION + _syntax("2147483648", "02147483648]")),
    (_get("l[99999999999], l[01]"), PROJECTION + _syntax("1", "01]")),
    (_get("l[01], l[99999999999]"), PROJECTION + _syntax("1", "01]")),
    (_update("SET l[2147483648] = :v", V), UPDATE + _range("2147483648")),
    (_update("SET l[4294967294] = :v", V), UPDATE + _range("4294967294")),
    (_update("SET l[4294967295] = :v", V), UPDATE + _range("4294967295")),
    (_update("SET l[99999999999999999999] = :v", V), UPDATE + _range("99999999999999999999")),
    (_update("SET x = l[2147483648]"), UPDATE + _range("2147483648")),
    (
        _update("SET a[1] = :v, l[99999999999] = :v, m[99999999999999] = :v", V),
        UPDATE + _range("99999999999"),
    ),
    (_update("SET l[2147483648] = :w"), UPDATE + _range("2147483648")),
    (_update("REMOVE l[2147483648]"), UPDATE + _range("2147483648")),
    (_update("REMOVE l[99999999999999999999]"), UPDATE + _range("99999999999999999999")),
    (_update("ADD l[2147483648] :n", N), UPDATE + _range("2147483648")),
    (_update("DELETE l[2147483648] :ss", SS), UPDATE + _range("2147483648")),
    (_update("SET l[01] = :v", V), UPDATE + _syntax("1", "01]")),
    (_update("SET l[00] = :v", V), UPDATE + _syntax("0", "00]")),
    (_update("SET x = l[01]"), UPDATE + _syntax("1", "01]")),
    (_update("SET x = l[00]"), UPDATE + _syntax("0", "00]")),
    (_update("REMOVE l[01]"), UPDATE + _syntax("1", "01]")),
    (_update("REMOVE l[00]"), UPDATE + _syntax("0", "00]")),
    (_update("ADD l[01] :n", N), UPDATE + _syntax("1", "01]")),
    (_update("DELETE l[01] :ss", SS), UPDATE + _syntax("1", "01]")),
    (
        _req("update_item", "ConditionExpression", "attribute_exists(l[2147483648])", V,
             Key=KEY, UpdateExpression="SET x = :v"),
        CONDITION + _range("2147483648"),
    ),
    (_put("l[2147483648] = :v", V), CONDITION + _range("2147483648")),
    (_put("l[99999999999999999999] = :v", V), CONDITION + _range("99999999999999999999")),
    (_put("attribute_exists(l[2147483648])"), CONDITION + _range("2147483648")),
    (_put("l[01] = :v", V), CONDITION + _syntax("1", "01]")),
    (_put("l[00] = :v", V), CONDITION + _syntax("0", "00]")),
    (_put("attribute_exists(l[01])"), CONDITION + _syntax("1", "01]")),
    (_delete("attribute_exists(l[2147483648])"), CONDITION + _range("2147483648")),
    (_scan("l[2147483648] = :v"), FILTER + _range("2147483648")),
    (_scan("l[99999999999999999999] = :v"), FILTER + _range("99999999999999999999")),
    (_scan("l[01] = :v"), FILTER + _syntax("1", "01]")),
    (_scan("l[00] = :v"), FILTER + _syntax("0", "00]")),
    (_query("pk = :p AND l[2147483648] = :v"), KEY_CONDITION + _range("2147483648")),
    (
        _query("pk = :p AND l[99999999999999999999] = :v"),
        KEY_CONDITION + _range("99999999999999999999"),
    ),
    (_query("pk = :p AND l[01] = :v"), KEY_CONDITION + _syntax("1", "01]")),
    (_query("pk = :p AND l[00] = :v"), KEY_CONDITION + _syntax("0", "00]")),
    (
        _transact("Update", "UpdateExpression", "SET l[2147483648] = :v", V, Key=KEY),
        TRANSACT_UPDATE + _range("2147483648"),
    ),
    (
        _transact("Update", "UpdateExpression", "SET l[01] = :v", V, Key=KEY),
        TRANSACT_UPDATE + _syntax("1", "01]"),
    ),
    (
        _transact(
            "ConditionCheck", "ConditionExpression", "attribute_exists(l[2147483648])", Key=KEY
        ),
        TRANSACT_CONDITION + _range("2147483648"),
    ),
    (
        _transact("ConditionCheck", "ConditionExpression", "attribute_exists(l[01])", Key=KEY),
        TRANSACT_CONDITION + _syntax("1", "01]"),
    ),
    (
        _transact("Put", "ConditionExpression", "l[2147483648] = :v", V, Item=ITEM),
        TRANSACT_CONDITION + _range("2147483648"),
    ),
    (
        _transact("Delete", "ConditionExpression", "attribute_exists(l[2147483648])", Key=KEY),
        TRANSACT_CONDITION + _range("2147483648"),
    ),
    (
        _transact("Update", "ConditionExpression", "attribute_exists(l[2147483648])", V,
                  Key=KEY, UpdateExpression="SET x = :v"),
        TRANSACT_CONDITION + _range("2147483648"),
    ),
]


def _label(operation: str, params: dict) -> str:
    """The operation, the action of a transaction, and the expression `_req` put first."""
    if operation == "transact_write_items":
        ((action, params),) = params["TransactItems"][0].items()
        operation = f"{operation}:{action}"
    return f"{operation}:{next(iter(params.values()))}"


def _send(client, table: str, operation: str, params: dict):
    """Send the request to `table`, named at the top or in each action of a transaction."""
    if operation == "transact_write_items":
        items = [
            {a: {"TableName": table, **p}} for e in params["TransactItems"] for a, p in e.items()
        ]
        return client.transact_write_items(TransactItems=items)
    return getattr(client, operation)(TableName=table, **params)


@pytest.fixture(scope="module")
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


@pytest.fixture()
def item(dynamodb_client, table):
    """The table with the item `l` = ["a", "b"] under the key `k`."""
    dynamodb_client.put_item(TableName=table, Item=ITEM)
    return table


def _stored(client, table: str) -> dict | None:
    return client.get_item(TableName=table, Key=KEY, ConsistentRead=True).get("Item")


@pytest.mark.parametrize(
    ("request_", "message"), REFUSED, ids=[_label(*request) for request, _ in REFUSED]
)
def test_the_index_is_refused(dynamodb_client, item, request_, message):
    operation, params = request_
    with pytest.raises(ClientError) as exc_info:
        _send(dynamodb_client, item, operation, params)
    error = exc_info.value.response["Error"]
    assert (error["Code"], error["Message"]) == ("ValidationException", message)
    assert _stored(dynamodb_client, item) == ITEM


def test_index_2147483647_is_taken(dynamodb_client, item):
    got = dynamodb_client.get_item(TableName=item, Key=KEY, ProjectionExpression="l[2147483647]")
    assert got["Item"] == {}
    dynamodb_client.update_item(TableName=item, Key=KEY, UpdateExpression="REMOVE l[2147483647]")
    assert _stored(dynamodb_client, item) == ITEM
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.put_item(
            TableName=item,
            Item=ITEM,
            ConditionExpression="l[2147483647] = :v",
            ExpressionAttributeValues=V,
        )
    assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
    dynamodb_client.update_item(
        TableName=item,
        Key=KEY,
        UpdateExpression="SET l[2147483647] = :v",
        ExpressionAttributeValues=V,
    )
    assert _stored(dynamodb_client, item)["l"] == {"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]}

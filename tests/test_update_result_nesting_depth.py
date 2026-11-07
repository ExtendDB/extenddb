# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Nesting depth of the item an UpdateItem or a TransactWriteItems Update produces.

Amazon DynamoDB refuses an update whose result nests an attribute value more
than 32 levels (31 M or L wrappers around a scalar) and writes nothing:
UpdateItem answers ValidationException ``Nesting Levels have exceeded
supported limits``, and a TransactWriteItems Update is cancelled with a
``ValidationError`` reason carrying the same text. A failing condition is
reported first. Measured on 2026-10-25 at 19:53 UTC and 2026-10-26 at 00:55
UTC (us-east-1), each case twice.

Depth below is counted in wrappers: a value with w wrappers has w + 1 levels.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

MESSAGE = "Nesting Levels have exceeded supported limits"
NAMES = {"#m": "m", "#k": "k"}


def nmap(wrappers: int, leaf: dict | None = None) -> dict:
    value = leaf or {"S": "leaf"}
    for _ in range(wrappers):
        value = {"M": {"k": value}}
    return value


def nlist(wrappers: int) -> dict:
    value = {"S": "leaf"}
    for _ in range(wrappers):
        value = {"L": [value]}
    return value


def wrappers(value: dict) -> int:
    """Most wrappers on any branch; an empty map or list adds none."""
    children = list(value.get("M", {}).values()) or value.get("L", [])
    return 1 + max(wrappers(c) for c in children) if children else 0


def path(j: int) -> str:
    return "#m" + ".#k" * j


@pytest.fixture(scope="module")
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


def _put(client, table, sk, m):
    key = {"pk": {"S": "nest"}, "sk": {"S": sk}}
    item = {**key, "x": {"S": "x"}}
    if m is not None:
        item["m"] = m
    client.put_item(TableName=table, Item=item)
    return key


def _stored_wrappers(client, table, key):
    item = client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"]
    return wrappers(item["m"]) if "m" in item else None


# (case, base value of m, update expression, names, values, wrappers stored after)
ACCEPTED = [
    ("j29_w2", nmap(30), f"SET {path(29)} = :v", NAMES, {":v": nmap(2)}, 31),
    ("j30_w1", nmap(30), f"SET {path(30)} = :v", NAMES, {":v": nmap(1)}, 31),
    ("j1_w30", nmap(30), f"SET {path(1)} = :v", NAMES, {":v": nmap(30)}, 31),
    ("j15_w16", nmap(30), f"SET {path(15)} = :v", NAMES, {":v": nmap(16)}, 31),
    ("top_w31", None, "SET #m = :v", {"#m": "m"}, {":v": nmap(31)}, 31),
    ("new_j1_w30", {"M": {}}, f"SET {path(1)} = :v", NAMES, {":v": nmap(30)}, 31),
    ("list_j30_w1", nlist(30), "SET #m" + "[0]" * 30 + " = :v", {"#m": "m"}, {":v": nlist(1)}, 31),
    ("if_not_exists_kept", nmap(30), f"SET {path(30)} = if_not_exists({path(30)}, :v)", NAMES,
     {":v": nmap(2)}, 30),
    ("at_limit_scalar", nmap(31), f"SET {path(31)} = :v", NAMES, {":v": {"S": "y"}}, 31),
]

REFUSED = [
    ("j29_w3", nmap(30), f"SET {path(29)} = :v", NAMES, {":v": nmap(3)}, 30),
    ("j30_w2", nmap(30), f"SET {path(30)} = :v", NAMES, {":v": nmap(2)}, 30),
    ("j1_w31", nmap(30), f"SET {path(1)} = :v", NAMES, {":v": nmap(31)}, 30),
    ("j15_w17", nmap(30), f"SET {path(15)} = :v", NAMES, {":v": nmap(17)}, 30),
    ("new_j1_w31", {"M": {}}, f"SET {path(1)} = :v", NAMES, {":v": nmap(31)}, 0),
    ("list_j30_w2", nlist(30), "SET #m" + "[0]" * 30 + " = :v", {"#m": "m"}, {":v": nlist(2)}, 30),
    ("list_append", nmap(29, {"L": [{"S": "a"}]}), f"SET {path(29)} = list_append({path(29)}, :v)", NAMES,
     {":v": {"L": [nmap(2)]}}, 30),
    ("if_not_exists_default", nmap(29, {"M": {}}), f"SET {path(30)} = if_not_exists(#m.#z, :v)",
     {**NAMES, "#z": "z"}, {":v": nmap(2)}, 29),
]


@pytest.mark.parametrize(("case", "base", "expr", "names", "values", "after"), ACCEPTED,
                         ids=[c[0] for c in ACCEPTED])
def test_update_up_to_the_limit_is_applied(dynamodb_client, table, case, base, expr, names, values, after):
    key = _put(dynamodb_client, table, f"ok-{case}", base)
    dynamodb_client.update_item(
        TableName=table, Key=key, UpdateExpression=expr,
        ExpressionAttributeNames=names, ExpressionAttributeValues=values,
    )
    assert _stored_wrappers(dynamodb_client, table, key) == after


@pytest.mark.parametrize(("case", "base", "expr", "names", "values", "after"), REFUSED,
                         ids=[c[0] for c in REFUSED])
def test_update_past_the_limit_is_refused(dynamodb_client, table, case, base, expr, names, values, after):
    key = _put(dynamodb_client, table, f"no-{case}", base)
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.update_item(
            TableName=table, Key=key, UpdateExpression=expr,
            ExpressionAttributeNames=names, ExpressionAttributeValues=values,
        )
    err = exc_info.value.response["Error"]
    assert (err["Code"], err["Message"]) == ("ValidationException", MESSAGE)
    assert _stored_wrappers(dynamodb_client, table, key) == after


def test_an_add_that_would_create_maps_past_the_limit_is_refused(dynamodb_client, table):
    # The service refuses this path for its 33 elements (measured 2026-10-26
    # at 01:26 UTC) and ExtendDB for the depth of the maps ADD would create,
    # so only the code and the stored item are compared.
    key = _put(dynamodb_client, table, "add-deep", nmap(30))
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.update_item(
            TableName=table, Key=key, UpdateExpression=f"ADD {path(29)}.#a.#b.#c :one",
            ExpressionAttributeNames={**NAMES, "#a": "a", "#b": "b", "#c": "c"},
            ExpressionAttributeValues={":one": {"N": "1"}},
        )
    assert exc_info.value.response["Error"]["Code"] == "ValidationException"
    assert _stored_wrappers(dynamodb_client, table, key) == 30


def test_a_failing_condition_is_reported_before_the_depth(dynamodb_client, table):
    key = _put(dynamodb_client, table, "condfail", nmap(30))
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.update_item(
            TableName=table, Key=key, UpdateExpression=f"SET {path(30)} = :v",
            ConditionExpression="#x = :no",
            ExpressionAttributeNames={**NAMES, "#x": "x"},
            ExpressionAttributeValues={":v": nmap(2), ":no": {"S": "no"}},
        )
    assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert _stored_wrappers(dynamodb_client, table, key) == 30


def test_an_item_at_the_limit_takes_an_update_of_another_attribute(dynamodb_client, table):
    key = _put(dynamodb_client, table, "other-attr", nmap(31))
    dynamodb_client.update_item(
        TableName=table, Key=key, UpdateExpression="SET #x = :v",
        ExpressionAttributeNames={"#x": "x"}, ExpressionAttributeValues={":v": {"S": "y"}},
    )
    assert _stored_wrappers(dynamodb_client, table, key) == 31


def test_transact_update_past_the_limit_is_cancelled(dynamodb_client, table):
    key = _put(dynamodb_client, table, "tx-no", nmap(30))
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(TransactItems=[{"Update": {
            "TableName": table, "Key": key, "UpdateExpression": f"SET {path(30)} = :v",
            "ExpressionAttributeNames": NAMES, "ExpressionAttributeValues": {":v": nmap(2)},
        }}])
    resp = exc_info.value.response
    assert resp["Error"]["Code"] == "TransactionCanceledException"
    reasons = [(r.get("Code"), r.get("Message")) for r in resp["CancellationReasons"]]
    assert reasons == [("ValidationError", MESSAGE)]
    assert _stored_wrappers(dynamodb_client, table, key) == 30


def test_transact_update_up_to_the_limit_is_applied(dynamodb_client, table):
    key = _put(dynamodb_client, table, "tx-ok", nmap(30))
    dynamodb_client.transact_write_items(TransactItems=[{"Update": {
        "TableName": table, "Key": key, "UpdateExpression": f"SET {path(29)} = :v",
        "ExpressionAttributeNames": NAMES, "ExpressionAttributeValues": {":v": nmap(2)},
    }}])
    assert _stored_wrappers(dynamodb_client, table, key) == 31

# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""UpdateExpression ADD on a nested path whose parent is missing.

Amazon DynamoDB adds to a member of a map that exists, but creates no missing
map on the way: every element above the leaf must exist and be the container
the next element needs (a map for a name, a list for an index). Otherwise the
update answers ValidationException ``The document path provided in the update
expression is invalid for update`` and writes nothing, the other actions of
the request included. A failing condition is reported first. Measured on
2026-10-26 at 02:38 and 02:54 UTC (us-east-1), each case twice.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import re

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

INVALID = "The document path provided in the update expression is invalid for update"
ONE = {":one": {"N": "1"}}
SS = {":ss": {"SS": ["a"]}}


def names_of(*texts: str) -> dict:
    return {t: t[1:] for text in texts for t in re.findall(r"#\w+", text)}


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


def _key(case: str) -> dict:
    return {"pk": {"S": "add-parent"}, "sk": {"S": case}}


def _put(client, table, case, attrs):
    key = _key(case)
    if attrs is not None:
        client.put_item(TableName=table, Item={**key, "x": {"S": "x"}, **attrs})
    return key


def _stored(client, table, key):
    item = client.get_item(TableName=table, Key=key, ConsistentRead=True).get("Item")
    return None if item is None else {k: v for k, v in item.items() if k not in ("pk", "sk")}


def _update(client, table, key, expr, values=ONE, **kw):
    return client.update_item(
        TableName=table, Key=key, UpdateExpression=expr,
        ExpressionAttributeNames=names_of(expr, kw.get("ConditionExpression", "")),
        ExpressionAttributeValues=values, **kw,
    )


EMPTY_MAP = {"m": {"M": {}}}
LIST_OF_ONE = {"l": {"L": [{"N": "1"}]}}

# (case, item attributes or None for no item, update expression, values)
REFUSED = [
    ("two_missing_maps", EMPTY_MAP, "ADD #m.#a.#b :one", ONE),
    ("missing_top", {}, "ADD #z.#a :one", ONE),
    ("missing_top_set", {}, "ADD #z.#a :ss", SS),
    ("absent_item", None, "ADD #z.#a :one", ONE),
    ("parent_is_string", {"s": {"S": "str"}}, "ADD #s.#a :one", ONE),
    ("parent_is_number", {"s": {"N": "1"}}, "ADD #s.#a :one", ONE),
    ("parent_is_list", LIST_OF_ONE, "ADD #l.#a :one", ONE),
    ("map_by_index", {"m": {"M": {"a": {"N": "1"}}}}, "ADD #m[0] :one", ONE),
    ("index_of_string", {"s": {"S": "str"}}, "ADD #s[0] :one", ONE),
    ("index_of_missing", {}, "ADD #z[0] :one", ONE),
    ("under_index_past_end", LIST_OF_ONE, "ADD #l[5].#a :one", ONE),
    ("under_list_member_missing_map", {"l": {"L": [{"M": {}}]}}, "ADD #l[0].#a.#b :one", ONE),
    ("with_set_before", {}, "SET #x = :one ADD #z.#a :one", ONE),
    ("with_set_after", {}, "ADD #z.#a :one SET #x = :one", ONE),
    ("second_of_two", {"x": {"N": "1"}}, "ADD #x :one, #z.#a :one", ONE),
]


@pytest.mark.parametrize(("case", "attrs", "expr", "values"), REFUSED, ids=[c[0] for c in REFUSED])
def test_add_under_a_missing_parent_is_refused(dynamodb_client, table, case, attrs, expr, values):
    key = _put(dynamodb_client, table, f"no-{case}", attrs)
    before = _stored(dynamodb_client, table, key)
    with pytest.raises(ClientError) as exc_info:
        _update(dynamodb_client, table, key, expr, values)
    err = exc_info.value.response["Error"]
    assert (err["Code"], err["Message"]) == ("ValidationException", INVALID)
    assert _stored(dynamodb_client, table, key) == before


# (case, item attributes, update expression, values, attribute changed, its value after)
APPLIED = [
    ("member_of_existing_map", EMPTY_MAP, "ADD #m.#a :one", ONE, "m", {"M": {"a": {"N": "1"}}}),
    ("set_member_of_existing_map", EMPTY_MAP, "ADD #m.#a :ss", SS, "m", {"M": {"a": {"SS": ["a"]}}}),
    ("member_of_map_in_list", {"l": {"L": [{"M": {}}]}}, "ADD #l[0].#a :one", ONE,
     "l", {"L": [{"M": {"a": {"N": "1"}}}]}),
]


@pytest.mark.parametrize(("case", "attrs", "expr", "values", "name", "after"), APPLIED,
                         ids=[c[0] for c in APPLIED])
def test_add_under_an_existing_map_is_applied(dynamodb_client, table, case, attrs, expr, values, name, after):
    key = _put(dynamodb_client, table, f"ok-{case}", attrs)
    _update(dynamodb_client, table, key, expr, values)
    assert _stored(dynamodb_client, table, key)[name] == after


def test_a_failing_condition_is_reported_first(dynamodb_client, table):
    key = _put(dynamodb_client, table, "cond-fails", {})
    with pytest.raises(ClientError) as exc_info:
        _update(dynamodb_client, table, key, "ADD #z.#a :one", ConditionExpression="attribute_exists(#b)")
    assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_a_passing_condition_does_not_change_the_answer(dynamodb_client, table):
    key = _put(dynamodb_client, table, "cond-passes", {})
    with pytest.raises(ClientError) as exc_info:
        _update(dynamodb_client, table, key, "ADD #z.#a :one", ConditionExpression="attribute_exists(#x)")
    err = exc_info.value.response["Error"]
    assert (err["Code"], err["Message"]) == ("ValidationException", INVALID)
    assert _stored(dynamodb_client, table, key) == {"x": {"S": "x"}}


def test_transact_write_items_update_is_cancelled(dynamodb_client, table):
    key = _put(dynamodb_client, table, "tx", {})
    with pytest.raises(ClientError) as exc_info:
        dynamodb_client.transact_write_items(TransactItems=[{"Update": {
            "TableName": table, "Key": key, "UpdateExpression": "ADD #z.#a :one",
            "ExpressionAttributeNames": {"#z": "z", "#a": "a"}, "ExpressionAttributeValues": ONE,
        }}])
    response = exc_info.value.response
    assert response["Error"]["Code"] == "TransactionCanceledException"
    assert [(r["Code"], r.get("Message")) for r in response["CancellationReasons"]] == [
        ("ValidationError", INVALID)]
    assert _stored(dynamodb_client, table, key) == {"x": {"S": "x"}}

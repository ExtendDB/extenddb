# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""UpdateItem SET of values that hold `$` strings or `$` member names.

Amazon DynamoDB stores every SET value as sent, with or without a numeric ADD
in the same update: strings such as ``$item_data``, ``$$ROOT``, ``$`` and
``$$REMOVE``, string sets and lists holding them, maps whose values hold
them, maps with members named ``$k`` or ``$literal``, and maps with members
whose names hold ``.`` (measured with the ADD on 2026-10-25 at 23:15 UTC and without
it on 2026-10-26 at 00:55 UTC, us-east-1, each case twice). A SET over an
attribute the item already holds replaces it, and a REMOVE in the same update
as the ADD removes its attribute (2026-10-26 at 00:55 UTC).

On MongoDB an unconditional update on a table with no index and no stream is
built as a native update while the server records that the table has no
index (60 seconds after a write). With a numeric ADD it is an aggregation
pipeline, where a `$` string reads as a field path. Each test gets a table of
its own and writes an item before its update, so the update runs on that
path.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest

from conftest import scoped_table

CASES = {
    "s_dollar_field": {"S": "$item_data"},
    "s_dollar_dollar_root": {"S": "$$ROOT"},
    "s_dollar_only": {"S": "$"},
    "s_dollar_remove": {"S": "$$REMOVE"},
    "s_literal_text": {"S": '{"$literal": 1}'},
    "ss_dollar": {"SS": ["$a", "$$ROOT", "b"]},
    "l_dollar": {"L": [{"S": "$a"}, {"N": "1"}, {"M": {"k": {"S": "$b"}}}]},
    "m_dollar_value": {"M": {"k": {"S": "$item_data.pk"}}},
    "m_dollar_member_name": {"M": {"$k": {"S": "v"}}},
    "m_member_named_literal": {"M": {"$literal": {"S": "v"}}},
    "m_dotted_member_names": {
        "M": {"a.b": {"S": "v"}, "$c.d": {"S": "$e"}, "f": {"M": {"g.h": {"N": "2"}}}}
    },
    "n_value": {"N": "5"},
    "bool_value": {"BOOL": True},
    "null_value": {"NULL": True},
    "b_value": {"B": b"$x"},
}


@pytest.fixture
def table(dynamodb_client):
    with scoped_table(dynamodb_client) as name:
        yield name


def _normalize(value):
    """String sets come back in no fixed order."""
    if "SS" in value:
        return {"SS": sorted(value["SS"])}
    return value


@pytest.mark.parametrize("with_add", [True, False], ids=["set_and_add", "set_alone"])
@pytest.mark.parametrize("case", sorted(CASES))
def test_set_stores_the_value_as_sent(dynamodb_client, table, case, with_add):
    value = CASES[case]
    key = {"pk": {"S": case}}
    dynamodb_client.put_item(TableName=table, Item={**key, "v": {"S": "x"}})
    names = {"#s": "s"}
    values = {":s": value}
    expression = "SET #s = :s"
    if with_add:
        names["#n"] = "n"
        values[":one"] = {"N": "1"}
        expression += " ADD #n :one"
    dynamodb_client.update_item(
        TableName=table,
        Key=key,
        UpdateExpression=expression,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )
    item = dynamodb_client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"]
    want = {**key, "v": {"S": "x"}, "s": value}
    if with_add:
        want["n"] = {"N": "1"}
    assert {k: _normalize(v) for k, v in item.items()} == {
        k: _normalize(v) for k, v in want.items()
    }


# A SET over an attribute the item already holds replaces it, with or without a
# numeric ADD in the same update (measured on 2026-10-26 at 00:55 UTC). In a
# MongoDB pipeline a document value merges into the embedded document it
# replaces unless it is a constant.
OVER = {
    "over_s_to_n": ({"S": "x"}, {"N": "5"}),
    "over_m_to_m": ({"M": {"a": {"S": "1"}}}, {"M": {"b": {"S": "2"}}}),
    "over_m_nested": (
        {"M": {"a": {"M": {"x": {"S": "1"}}}}},
        {"M": {"a": {"M": {"y": {"S": "2"}}}}},
    ),
    "over_l_to_m": ({"L": [{"S": "1"}]}, {"M": {"b": {"S": "2"}}}),
}


@pytest.mark.parametrize("with_add", [True, False], ids=["set_and_add", "set_alone"])
@pytest.mark.parametrize("case", sorted(OVER))
def test_set_replaces_the_value_the_item_holds(dynamodb_client, table, case, with_add):
    old, new = OVER[case]
    key = {"pk": {"S": case}}
    dynamodb_client.put_item(TableName=table, Item={**key, "s": old})
    names = {"#s": "s"}
    values = {":s": new}
    expression = "SET #s = :s"
    if with_add:
        names["#n"] = "n"
        values[":one"] = {"N": "1"}
        expression += " ADD #n :one"
    dynamodb_client.update_item(
        TableName=table,
        Key=key,
        UpdateExpression=expression,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )
    item = dynamodb_client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"]
    want = {**key, "s": new}
    if with_add:
        want["n"] = {"N": "1"}
    assert item == want


def test_remove_in_the_same_update_as_add_removes_the_attribute(dynamodb_client, table):
    """Measured on 2026-10-26 at 00:55 UTC: r is removed, s and n are written."""
    key = {"pk": {"S": "remove_with_add"}}
    dynamodb_client.put_item(TableName=table, Item={**key, "r": {"S": "gone"}})
    dynamodb_client.update_item(
        TableName=table,
        Key=key,
        UpdateExpression="SET #s = :s REMOVE #r ADD #n :one",
        ExpressionAttributeNames={"#s": "s", "#r": "r", "#n": "n"},
        ExpressionAttributeValues={":s": {"S": "$$REMOVE"}, ":one": {"N": "1"}},
    )
    item = dynamodb_client.get_item(TableName=table, Key=key, ConsistentRead=True)["Item"]
    assert item == {**key, "s": {"S": "$$REMOVE"}, "n": {"N": "1"}}

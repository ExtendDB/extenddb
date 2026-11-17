# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Legacy ``Expected`` entries whose ``ComparisonOperator`` gets the wrong
number of values, or both or neither of ``Value`` and ``AttributeValueList``.

Amazon DynamoDB refuses them with a ``ValidationException`` whose text starts
with ``1 validation error detected: `` on PutItem, UpdateItem and DeleteItem,
and leaves the item as it was. Measured in us-east-1 on 13 and 14 November
2026 (EQ, NE, IN, BETWEEN, the two-entry request, and the entry with neither
field) and on 17 November 2026 (LE, LT, GE, GT, BEGINS_WITH, CONTAINS,
NOT_CONTAINS, BETWEEN with three values, NULL and NOT_NULL with a value, and
the entry with both fields), each request sent twice.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

PREFIX = "1 validation error detected: One or more parameter values were invalid: "
BOTH_MSG = PREFIX + "Value and AttributeValueList cannot be used together for Attribute: tested"
NEITHER_MSG = (
    PREFIX + "Value or AttributeValueList must be used with ComparisonOperator: EQ "
    "for Attribute: tested"
)
BASE_ITEM = {"pk": {"S": "k"}, "tested": {"S": "READY"}}
KEY = {"pk": {"S": "k"}}
READY = {"S": "READY"}
TWO = [{"S": "READY"}, {"S": "OTHER"}]
OPERATIONS = ("put_item", "update_item", "delete_item")


def _count_msg(op: str) -> str:
    return f"{PREFIX}Invalid number of argument(s) for the {op} ComparisonOperator"


def _entry(op: str, values: list | None = None, value: dict | None = None) -> dict:
    entry: dict = {"ComparisonOperator": op}
    if values is not None:
        entry["AttributeValueList"] = values
    if value is not None:
        entry["Value"] = value
    return entry


# (entry, message): one row per measured refusal.
REFUSED_ROWS = [
    (_entry("EQ", []), _count_msg("EQ")),
    (_entry("EQ", TWO), _count_msg("EQ")),
    (_entry("NE", TWO), _count_msg("NE")),
    (_entry("IN", []), _count_msg("IN")),
    (_entry("BETWEEN", value=READY), _count_msg("BETWEEN")),
    (_entry("BETWEEN", [{"S": "A"}, {"S": "M"}, {"S": "Z"}]), _count_msg("BETWEEN")),
    *[
        (_entry(op, values), _count_msg(op))
        for op in ("LE", "LT", "GE", "GT", "BEGINS_WITH", "CONTAINS", "NOT_CONTAINS")
        for values in ([], TWO)
    ],
    (_entry("NULL", value=READY), _count_msg("NULL")),
    (_entry("NULL", [READY]), _count_msg("NULL")),
    (_entry("NOT_NULL", value=READY), _count_msg("NOT_NULL")),
    (_entry("EQ", [READY], value=READY), BOTH_MSG),
    (_entry("EQ", [], value=READY), BOTH_MSG),
    (_entry("NOT_NULL", [READY], value=READY), BOTH_MSG),
    (_entry("EQ"), NEITHER_MSG),
]

# Entries the service accepts; each condition holds for BASE_ITEM.
ACCEPTED_ROWS = [
    _entry("EQ", value=READY),
    _entry("NE", value={"S": "OTHER"}),
    _entry("IN", value=READY),
    _entry("BETWEEN", [{"S": "A"}, {"S": "Z"}]),
    _entry("NOT_NULL", []),
]


def _row_id(entry: dict) -> str:
    parts = [entry["ComparisonOperator"]]
    if "AttributeValueList" in entry:
        parts.append(f"list{len(entry['AttributeValueList'])}")
    if "Value" in entry:
        parts.append("value")
    return "-".join(parts)


REFUSED_IDS = [_row_id(entry) for entry, _ in REFUSED_ROWS]
ACCEPTED_IDS = [_row_id(entry) for entry in ACCEPTED_ROWS]


def _send(client, table: str, operation: str, expected: dict) -> None:
    if operation == "put_item":
        client.put_item(TableName=table, Item={**BASE_ITEM, "marker": {"S": "after"}}, Expected=expected)
    elif operation == "update_item":
        client.update_item(
            TableName=table,
            Key=KEY,
            AttributeUpdates={"marker": {"Action": "PUT", "Value": {"S": "after"}}},
            Expected=expected,
        )
    else:
        client.delete_item(TableName=table, Key=KEY, Expected=expected)


def _read(client, table: str) -> dict | None:
    return client.get_item(TableName=table, Key=KEY, ConsistentRead=True).get("Item")


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """Hash-only table with the measured item, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(TableName=name, Item=BASE_ITEM)
        yield name


class TestExpectedValueCount:
    @pytest.mark.parametrize("operation", OPERATIONS)
    @pytest.mark.parametrize("entry,message", REFUSED_ROWS, ids=REFUSED_IDS)
    def test_refused_entry_gets_the_measured_text(
        self, dynamodb_client, table, operation, entry, message
    ):
        with pytest.raises(ClientError) as exc_info:
            _send(dynamodb_client, table, operation, {"tested": entry})
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == message
        assert _read(dynamodb_client, table) == BASE_ITEM

    def test_wrong_entry_beside_a_valid_one(self, dynamodb_client, table):
        # Two entries and no ConditionalOperator: the BETWEEN entry is refused.
        with pytest.raises(ClientError) as exc_info:
            _send(
                dynamodb_client,
                table,
                "put_item",
                {"a": _entry("BETWEEN", [{"S": "x"}]), "b": _entry("EQ", [{"S": "y"}])},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == _count_msg("BETWEEN")
        assert _read(dynamodb_client, table) == BASE_ITEM

    @pytest.mark.parametrize("operation", OPERATIONS)
    @pytest.mark.parametrize("entry", ACCEPTED_ROWS, ids=ACCEPTED_IDS)
    def test_accepted_entry_applies_the_write(self, dynamodb_client, table, operation, entry):
        _send(dynamodb_client, table, operation, {"tested": entry})
        item = _read(dynamodb_client, table)
        if operation == "delete_item":
            assert item is None
        else:
            assert item == {**BASE_ITEM, "marker": {"S": "after"}}
        dynamodb_client.put_item(TableName=table, Item=BASE_ITEM)

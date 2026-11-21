# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The text of a refused ConditionalOperator, per operation.

Amazon DynamoDB refuses a ConditionalOperator outside [AND, OR] with a
ValidationException on the five operations that take one. Query and Scan
send the enum clause alone. PutItem, UpdateItem and DeleteItem send it
inside the "1 validation error detected: " envelope. On Query and Scan the
clause comes ahead of the operand count of a BETWEEN in the legacy filter.
Measured 2026-11-13 and 2026-11-14 (us-east-1).

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest

from conftest import scoped_table
from test_conditional_operator import _cond, _expect_validation

ENVELOPE = "1 validation error detected: "
VALUES = ["XOR", "and", "", "ALL_OLD", "ALL"]
READS = ("query", "scan")
WRITES = ("put_item", "update_item", "delete_item")
BETWEEN_ONE = {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"S": "x"}]}


def _clause(value: str) -> str:
    return (
        f"Value '{value}' at 'conditionalOperator' failed to satisfy constraint: "
        "Member must satisfy enum value set: [AND, OR]"
    )


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """A table keyed on pk and sk (both S), deleted on teardown."""
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


def _request(operation: str, table: str, op: str, first: dict) -> dict:
    """The request of `operation` with `op` as its ConditionalOperator and
    `first` as the `a` entry of its two-entry filter or Expected."""
    two = {"a": first, "b": _cond("y")}
    key = {"pk": {"S": "k"}, "sk": {"S": "s"}}
    shapes = {
        "query": {"KeyConditions": {"pk": _cond("k")}, "QueryFilter": two},
        "scan": {"ScanFilter": two},
        "put_item": {"Item": key, "Expected": two},
        "update_item": {
            "Key": key,
            "Expected": two,
            "AttributeUpdates": {"c": {"Value": {"S": "v"}}},
        },
        "delete_item": {"Key": key, "Expected": two},
    }
    return {"TableName": table, "ConditionalOperator": op, **shapes[operation]}


def _row(operation: str, op: str, first: dict, expected: str):
    shape = "between" if first is BETWEEN_ONE else "eq"
    return pytest.param(
        operation, op, first, expected, id=f"{operation}-{op or 'empty'}-{shape}"
    )


# One row per measured request: the operation, its ConditionalOperator, the
# `a` entry of its filter or Expected, and the message the service sends.
ROWS = [
    *[_row(o, v, _cond("x"), _clause(v)) for o in READS for v in VALUES],
    *[_row(o, "XOR", BETWEEN_ONE, _clause("XOR")) for o in READS],
    *[_row(o, v, _cond("x"), ENVELOPE + _clause(v)) for o in WRITES for v in VALUES],
]


@pytest.mark.parametrize(("operation", "op", "first", "expected"), ROWS)
def test_refused_conditional_operator_text(
    dynamodb_client_no_validation, table, operation, op, first, expected
):
    call = getattr(dynamodb_client_no_validation, operation)
    _expect_validation(lambda: call(**_request(operation, table, op, first)), expected)

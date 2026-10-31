# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Request-stage checks of Query, Scan and UpdateItem, with the service's text.

Each row below was measured against Amazon DynamoDB on 2026-10-27 (two
samples, identical bytes). These checks run before the table is read, so the
tables here are only needed for the cases that name one.

Dual-target: runs against the service and against ExtendDB on every backend.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

S = {"S": "s"}
UNDEF_NAME = "An expression attribute name used in the document path is not defined; attribute name: {}"
UNDEF_VALUE = "An expression attribute value used in expression is not defined; attribute value: {}"
KEY_CONDITION = "Invalid KeyConditionExpression: "


@pytest.fixture(scope="module")
def table(dynamodb_client):
    """Composite-key table (pk S, sk S) with one item."""
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
        dynamodb_client.put_item(TableName=name, Item={"pk": {"S": "p"}, "sk": {"S": "s"}, "n": {"N": "1"}})
        yield name


@pytest.fixture(scope="module")
def missing_table() -> str:
    return f"extenddb-missing-{uuid.uuid4().hex[:12]}"


def _refused(func, message: str):
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException", err
    assert err["Message"] == message, err


class TestKeyConditionReferenceOrder:
    """An undefined `#name` or `:value` is named in the order the text has them."""

    @pytest.mark.parametrize(
        ("expression", "values", "undefined"),
        [
            ("#b = :p AND #a > :q", {":p": S, ":q": S}, UNDEF_NAME.format("#b")),
            ("#a = :p AND #b > :q", {":p": S, ":q": S}, UNDEF_NAME.format("#a")),
            ("pk = :q AND #n = :v", {":v": S}, UNDEF_VALUE.format(":q")),
            ("pk = :z AND sk > :y", None, UNDEF_VALUE.format(":z")),
            ("pk = :y AND sk > :z", None, UNDEF_VALUE.format(":y")),
        ],
        ids=["names-b-a", "names-a-b", "value-before-name", "values-z-y", "values-y-z"],
    )
    def test_the_first_undefined_reference_in_the_text_is_named(
        self, dynamodb_client, table, expression, values, undefined
    ):
        extra = {} if values is None else {"ExpressionAttributeValues": values}
        _refused(
            lambda: dynamodb_client.query(TableName=table, KeyConditionExpression=expression, **extra),
            KEY_CONDITION + undefined,
        )

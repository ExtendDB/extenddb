# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A KeyConditionExpression carries one condition per key attribute.

Amazon DynamoDB rejects a key condition that names the same key attribute twice,
on the partition key and on the sort key alike, whatever the operators are:

    ValidationException: Invalid KeyConditionExpression: KeyConditionExpressions
    must only contain one condition per key

Measured 2026-10-24T00:27:56Z and 00:30:39Z in us-east-1 on a table with
pk (S) HASH and sk (S) RANGE. Issue #430.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

ONE_PER_KEY = (
    "Invalid KeyConditionExpression: "
    "KeyConditionExpressions must only contain one condition per key"
)
NESTED = (
    "Invalid KeyConditionExpression: "
    "KeyConditionExpressions cannot have conditions on nested attributes"
)
REDUNDANT_PARENS = (
    "Invalid KeyConditionExpression: The expression has redundant parentheses;"
)
UNDEFINED_NAME = (
    "An expression attribute name used in the document path is not defined; "
    "attribute name: #u"
)

A = {"S": "alice"}
B = {"S": "bob"}
C = {"S": "carol"}


@pytest.fixture(scope="class")
def range_table(dynamodb_client):
    """pk (S) HASH, sk (S) RANGE, holding alice/bob and alice/carol."""
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
        for sk in ("bob", "carol"):
            dynamodb_client.put_item(
                TableName=name, Item={"pk": {"S": "alice"}, "sk": {"S": sk}}
            )
        yield name


def _error(func) -> tuple[str, str]:
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    return err["Code"], err["Message"]


class TestOneConditionPerKey:
    """Every measured duplicate shape, and the shapes that keep their own error."""

    @pytest.mark.parametrize(
        ("expr", "values"),
        [
            # The shape in the issue: a second equality on the partition key is
            # served on extenddb before the fix, reading :b as a sort key value.
            ("pk = :a AND pk = :b", {":a": A, ":b": B}),
            ("pk = :a AND pk = :a", {":a": A}),
            ("pk = :a AND pk = :b AND sk = :c", {":a": A, ":b": B, ":c": C}),
            ("pk = :a AND pk > :b", {":a": A, ":b": B}),
            ("pk > :b AND pk = :a", {":a": A, ":b": B}),
            (":a = pk AND pk = :b", {":a": A, ":b": B}),
            ("(pk = :a) AND (pk = :b)", {":a": A, ":b": B}),
        ],
    )
    def test_duplicate_partition_key_condition(
        self, dynamodb_client, range_table, expr, values
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression=expr,
                ExpressionAttributeValues=values,
            )
        )
        assert code == "ValidationException"
        assert msg == ONE_PER_KEY

    @pytest.mark.parametrize(
        ("expr", "values"),
        [
            ("pk = :a AND sk = :b AND sk = :c", {":a": A, ":b": B, ":c": C}),
            ("pk = :a AND sk = :b AND sk = :b", {":a": A, ":b": B}),
            (
                "pk = :a AND sk BETWEEN :b AND :c AND sk = :b",
                {":a": A, ":b": B, ":c": C},
            ),
            (
                "pk = :a AND sk BETWEEN :b AND :c AND begins_with(sk, :b)",
                {":a": A, ":b": B, ":c": C},
            ),
            ("pk = :a AND begins_with(sk, :b) AND sk = :c", {":a": A, ":b": B, ":c": C}),
            ("pk = :a AND sk > :b AND sk < :c", {":a": A, ":b": B, ":c": C}),
            (
                "pk = :a AND begins_with(sk, :b) AND begins_with(sk, :c)",
                {":a": A, ":b": B, ":c": C},
            ),
            ("pk = :a AND pk = :b AND sk = :b AND sk = :c", {":a": A, ":b": B, ":c": C}),
            # No partition key condition at all: the duplicate is reported
            # ahead of the missing key schema element.
            ("sk = :b AND sk = :c", {":b": B, ":c": C}),
        ],
    )
    def test_duplicate_sort_key_condition(
        self, dynamodb_client, range_table, expr, values
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression=expr,
                ExpressionAttributeValues=values,
            )
        )
        assert code == "ValidationException"
        assert msg == ONE_PER_KEY

    @pytest.mark.parametrize(
        ("expr", "values", "names"),
        [
            ("#p = :a AND pk = :b", {":a": A, ":b": B}, {"#p": "pk"}),
            ("#p = :a AND #q = :b", {":a": A, ":b": B}, {"#p": "pk", "#q": "pk"}),
            (
                "pk = :a AND #s = :b AND sk = :c",
                {":a": A, ":b": B, ":c": C},
                {"#s": "sk"},
            ),
        ],
    )
    def test_duplicate_through_name_reference(
        self, dynamodb_client, range_table, expr, values, names
    ):
        """An #name alias resolving to the same attribute is a duplicate."""
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression=expr,
                ExpressionAttributeValues=values,
                ExpressionAttributeNames=names,
            )
        )
        assert code == "ValidationException"
        assert msg == ONE_PER_KEY

    def test_distinct_key_conditions_still_served(self, dynamodb_client, range_table):
        """One condition per key attribute, in either order, returns rows."""
        pk_only = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression="pk = :a",
            ExpressionAttributeValues={":a": A},
        )
        assert pk_only["Count"] == 2

        pk_and_sk = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression="pk = :a AND sk = :b",
            ExpressionAttributeValues={":a": A, ":b": B},
        )
        assert pk_and_sk["Count"] == 1

        sk_first = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression="sk = :b AND pk = :a",
            ExpressionAttributeValues={":a": A, ":b": B},
        )
        assert sk_first["Count"] == 1

        sk_range = dynamodb_client.query(
            TableName=range_table,
            KeyConditionExpression="pk = :a AND sk BETWEEN :b AND :c",
            ExpressionAttributeValues={":a": A, ":b": B, ":c": C},
        )
        assert sk_range["Count"] == 2

    def test_nested_path_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="pk = :a AND pk.foo = :b",
                ExpressionAttributeValues={":a": A, ":b": B},
            )
        )
        assert code == "ValidationException"
        assert msg == NESTED

    def test_redundant_parens_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table
    ):
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="((pk = :a)) AND (pk = :b)",
                ExpressionAttributeValues={":a": A, ":b": B},
            )
        )
        assert code == "ValidationException"
        assert msg == REDUNDANT_PARENS

    def test_undefined_name_reported_ahead_of_duplicate(
        self, dynamodb_client, range_table
    ):
        """Two conditions on an undefined #name: the name is reported."""
        code, msg = _error(
            lambda: dynamodb_client.query(
                TableName=range_table,
                KeyConditionExpression="#u = :a AND #u = :b",
                ExpressionAttributeValues={":a": A, ":b": B},
            )
        )
        assert code == "ValidationException"
        assert UNDEFINED_NAME in msg

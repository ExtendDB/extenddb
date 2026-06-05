# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Expression-attribute validation and parse-error labeling for transactions.

TransactWriteItems and TransactGetItems differ from the single-item APIs:

  - They do NOT reject ExpressionAttributeNames/Values supplied with no
    expression. Amazon DynamoDB accepts a Put/Delete/Get sub-operation that
    declares names or values but has no condition/update/projection that
    references them. (The single-item APIs reject this as
    "can only be specified when using expressions"; transactions do not.)

  - The "unused in expressions" rejection still applies when an expression IS
    present and a name/value goes unreferenced.

  - Parse errors carry the per-parameter prefix
    (Invalid UpdateExpression: / Invalid ConditionExpression:), not the generic
    Invalid expression:.

Every expected message was captured from Amazon DynamoDB. Dual-target.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

RESERVED_UPDATE = (
    "Invalid UpdateExpression: Attribute name is a reserved keyword; "
    "reserved keyword: status"
)
RESERVED_CONDITION = (
    "Invalid ConditionExpression: Attribute name is a reserved keyword; "
    "reserved keyword: status"
)
REDUNDANT_PARENS_CONDITION = (
    "Invalid ConditionExpression: The expression has redundant parentheses;"
)
UNUSED_NAME_MSG = (
    "Value provided in ExpressionAttributeNames unused in expressions: keys: {#n}"
)
UNUSED_VALUE_MSG = (
    "Value provided in ExpressionAttributeValues unused in expressions: keys: {:v}"
)


@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """Hash-only table (pk) with one item, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "foo": {"S": "a"}},
        )
        yield name


def _expect_validation(func, expected_message: str):
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert err["Message"] == expected_message


class TestTransactNamesValuesWithoutExpression:
    """Transactions accept names/values that have no referencing expression."""

    def test_twi_put_names_no_condition_accepted(self, dynamodb_client, hash_table):
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": hash_table,
                        "Item": {"pk": {"S": "p-names"}},
                        "ExpressionAttributeNames": {"#n": "foo"},
                    }
                }
            ]
        )
        got = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "p-names"}})
        assert got["Item"]["pk"]["S"] == "p-names"

    def test_twi_put_values_no_condition_accepted(self, dynamodb_client, hash_table):
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": hash_table,
                        "Item": {"pk": {"S": "p-values"}},
                        "ExpressionAttributeValues": {":v": {"N": "1"}},
                    }
                }
            ]
        )
        got = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "p-values"}})
        assert got["Item"]["pk"]["S"] == "p-values"

    def test_twi_delete_names_no_condition_accepted(self, dynamodb_client, hash_table):
        dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "d-names"}})
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Delete": {
                        "TableName": hash_table,
                        "Key": {"pk": {"S": "d-names"}},
                        "ExpressionAttributeNames": {"#n": "foo"},
                    }
                }
            ]
        )
        got = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "d-names"}})
        assert "Item" not in got

    def test_twi_delete_values_no_condition_accepted(self, dynamodb_client, hash_table):
        dynamodb_client.put_item(TableName=hash_table, Item={"pk": {"S": "d-values"}})
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Delete": {
                        "TableName": hash_table,
                        "Key": {"pk": {"S": "d-values"}},
                        "ExpressionAttributeValues": {":v": {"N": "1"}},
                    }
                }
            ]
        )
        got = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "d-values"}})
        assert "Item" not in got

    def test_tgi_get_names_no_projection_accepted(self, dynamodb_client, hash_table):
        resp = dynamodb_client.transact_get_items(
            TransactItems=[
                {
                    "Get": {
                        "TableName": hash_table,
                        "Key": {"pk": {"S": "k1"}},
                        "ExpressionAttributeNames": {"#n": "foo"},
                    }
                }
            ]
        )
        assert resp["Responses"][0]["Item"]["pk"]["S"] == "k1"


class TestTransactReferencedExpressionAccepted:
    """Names/values that ARE referenced by an expression are accepted."""

    def test_twi_put_name_referenced_by_condition_accepted(
        self, dynamodb_client, hash_table
    ):
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": hash_table,
                        "Item": {"pk": {"S": "p-ref"}},
                        "ConditionExpression": "attribute_not_exists(#n)",
                        "ExpressionAttributeNames": {"#n": "pk"},
                    }
                }
            ]
        )
        got = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "p-ref"}})
        assert got["Item"]["pk"]["S"] == "p-ref"

    def test_twi_update_value_referenced_accepted(self, dynamodb_client, hash_table):
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": hash_table,
                        "Key": {"pk": {"S": "k1"}},
                        "UpdateExpression": "SET foo = :v",
                        "ExpressionAttributeValues": {":v": {"S": "set"}},
                    }
                }
            ]
        )
        got = dynamodb_client.get_item(TableName=hash_table, Key={"pk": {"S": "k1"}})
        assert got["Item"]["foo"]["S"] == "set"


class TestTransactUnusedWithExpression:
    """With an expression present, unused names/values are still rejected."""

    def test_twi_put_unused_name_with_condition_rejected(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": hash_table,
                            "Item": {"pk": {"S": "p-unused"}},
                            "ConditionExpression": "attribute_not_exists(pk)",
                            "ExpressionAttributeNames": {"#n": "foo"},
                        }
                    }
                ]
            ),
            UNUSED_NAME_MSG,
        )

    def test_twi_delete_unused_value_with_condition_rejected(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Delete": {
                            "TableName": hash_table,
                            "Key": {"pk": {"S": "k1"}},
                            "ConditionExpression": "attribute_exists(pk)",
                            "ExpressionAttributeValues": {":v": {"N": "1"}},
                        }
                    }
                ]
            ),
            UNUSED_VALUE_MSG,
        )

    def test_twi_condition_check_unused_name_rejected(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": hash_table,
                            "Key": {"pk": {"S": "k1"}},
                            "ConditionExpression": "attribute_exists(pk)",
                            "ExpressionAttributeNames": {"#n": "foo"},
                        }
                    }
                ]
            ),
            UNUSED_NAME_MSG,
        )

    def test_tgi_get_unused_name_with_projection_rejected(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.transact_get_items(
                TransactItems=[
                    {
                        "Get": {
                            "TableName": hash_table,
                            "Key": {"pk": {"S": "k1"}},
                            "ProjectionExpression": "pk",
                            "ExpressionAttributeNames": {"#n": "foo"},
                        }
                    }
                ]
            ),
            UNUSED_NAME_MSG,
        )


class TestTransactParseErrorPrefix:
    """Parse errors carry the per-parameter prefix, not Invalid expression:."""

    def test_twi_update_reserved_word_prefix(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": hash_table,
                            "Key": {"pk": {"S": "k1"}},
                            "UpdateExpression": "SET status = :v",
                            "ExpressionAttributeValues": {":v": {"N": "1"}},
                        }
                    }
                ]
            ),
            RESERVED_UPDATE,
        )

    def test_twi_condition_check_reserved_word_prefix(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": hash_table,
                            "Key": {"pk": {"S": "k1"}},
                            "ConditionExpression": "status > :v",
                            "ExpressionAttributeValues": {":v": {"N": "0"}},
                        }
                    }
                ]
            ),
            RESERVED_CONDITION,
        )

    def test_twi_put_condition_reserved_word_prefix(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": hash_table,
                            "Item": {"pk": {"S": "p-cond"}},
                            "ConditionExpression": "status = :v",
                            "ExpressionAttributeValues": {":v": {"N": "1"}},
                        }
                    }
                ]
            ),
            RESERVED_CONDITION,
        )

    def test_twi_delete_condition_reserved_word_prefix(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Delete": {
                            "TableName": hash_table,
                            "Key": {"pk": {"S": "k1"}},
                            "ConditionExpression": "status = :v",
                            "ExpressionAttributeValues": {":v": {"N": "1"}},
                        }
                    }
                ]
            ),
            RESERVED_CONDITION,
        )

    def test_twi_update_syntax_error_prefix(self, dynamodb_client, hash_table):
        # Syntax error also carries the UpdateExpression prefix.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": hash_table,
                            "Key": {"pk": {"S": "k1"}},
                            "UpdateExpression": "SET foo = ",
                            "ExpressionAttributeValues": {":v": {"N": "1"}},
                        }
                    }
                ]
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"].startswith("Invalid UpdateExpression:")

    def test_twi_condition_check_redundant_parens_prefix(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.transact_write_items(
                TransactItems=[
                    {
                        "ConditionCheck": {
                            "TableName": hash_table,
                            "Key": {"pk": {"S": "k1"}},
                            "ConditionExpression": "((pk = :v))",
                            "ExpressionAttributeValues": {":v": {"S": "k1"}},
                        }
                    }
                ]
            ),
            REDUNDANT_PARENS_CONDITION,
        )

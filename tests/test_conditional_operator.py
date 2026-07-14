# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Legacy ConditionalOperator validation and expression-parameter mixing.

Amazon DynamoDB rejects ConditionalOperator unless it accompanies a legacy
Filter (ScanFilter/QueryFilter) or Expected with two or more conditions.
ConditionalOperator also counts as a non-expression parameter, so combining
it with any expression parameter raises the mixing error, whose message
lists every present parameter on each side in a fixed per-API order.

KeyConditions is exempt from the mixing check unless KeyConditionExpression
is also present: legacy KeyConditions combined with FilterExpression or
ProjectionExpression is accepted.

Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

WITHOUT_FILTER_MSG = "ConditionalOperator cannot be used without Filter or Expected"
TWO_OR_MORE_MSG = (
    "ConditionalOperator can only be used when Filter or Expected has two or more elements"
)
MIXING_PREFIX = (
    "Can not use both expression and non-expression parameters in the same request: "
)


def _mixing_msg(non_expression: str, expression: str) -> str:
    return (
        f"{MIXING_PREFIX}Non-expression parameters: {{{non_expression}}} "
        f"Expression parameters: {{{expression}}}"
    )


def _cond(value: str = "x") -> dict:
    return {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": value}]}


@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """Hash-only table for the class, with one item, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "a": {"S": "x"}, "b": {"S": "y"}},
        )
        yield name


def _expect_validation(func, expected_message: str):
    with pytest.raises(ClientError) as exc_info:
        func()
    err = exc_info.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert err["Message"] == expected_message


class TestConditionalOperatorRequiresConditions:
    """ConditionalOperator without a 2+ element Filter/Expected is rejected."""

    def test_scan_condop_without_filter(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table, ConditionalOperator="AND"
            ),
            WITHOUT_FILTER_MSG,
        )

    def test_scan_condop_or_without_filter(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table, ConditionalOperator="OR"
            ),
            WITHOUT_FILTER_MSG,
        )

    def test_scan_condop_empty_scan_filter(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table, ConditionalOperator="AND", ScanFilter={}
            ),
            WITHOUT_FILTER_MSG,
        )

    def test_scan_condop_single_condition_filter(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                ConditionalOperator="AND",
                ScanFilter={"a": _cond()},
            ),
            TWO_OR_MORE_MSG,
        )

    def test_scan_condop_with_attributes_to_get_only(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                ConditionalOperator="AND",
                AttributesToGet=["a", "b"],
            ),
            WITHOUT_FILTER_MSG,
        )

    def test_query_condop_without_filter(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.query(
                TableName=hash_table,
                ConditionalOperator="AND",
                KeyConditions={"pk": _cond("k1")},
            ),
            WITHOUT_FILTER_MSG,
        )

    def test_query_condop_without_any_key_condition(self, dynamodb_client, hash_table):
        # The ConditionalOperator rule fires before the missing
        # KeyConditions/KeyConditionExpression error.
        _expect_validation(
            lambda: dynamodb_client.query(
                TableName=hash_table,
                ConditionalOperator="AND",
            ),
            WITHOUT_FILTER_MSG,
        )

    def test_query_condop_single_condition_filter(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.query(
                TableName=hash_table,
                ConditionalOperator="AND",
                KeyConditions={"pk": _cond("k1")},
                QueryFilter={"a": _cond()},
            ),
            TWO_OR_MORE_MSG,
        )

    def test_put_item_condop_without_expected(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": "k2"}},
                ConditionalOperator="AND",
            ),
            WITHOUT_FILTER_MSG,
        )

    def test_put_item_condop_single_condition_expected(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": "k2"}},
                ConditionalOperator="AND",
                Expected={"a": _cond()},
            ),
            TWO_OR_MORE_MSG,
        )

    def test_delete_item_condop_without_expected(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.delete_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ConditionalOperator="AND",
            ),
            WITHOUT_FILTER_MSG,
        )

    def test_delete_item_condop_single_condition_expected(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.delete_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ConditionalOperator="AND",
                Expected={"a": _cond()},
            ),
            TWO_OR_MORE_MSG,
        )

    def test_update_item_condop_without_expected(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.update_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ConditionalOperator="AND",
                AttributeUpdates={"a": {"Value": {"S": "v"}, "Action": "PUT"}},
            ),
            WITHOUT_FILTER_MSG,
        )

    def test_update_item_condop_no_updates_at_all(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.update_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ConditionalOperator="AND",
            ),
            WITHOUT_FILTER_MSG,
        )


class TestConditionalOperatorMixing:
    """ConditionalOperator with any expression parameter raises the mixing error."""

    def test_scan_condop_with_filter_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                ConditionalOperator="AND",
                FilterExpression="a = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            _mixing_msg("ConditionalOperator", "FilterExpression"),
        )

    def test_scan_condop_with_projection_expression(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                ConditionalOperator="AND",
                ProjectionExpression="a",
            ),
            _mixing_msg("ConditionalOperator", "ProjectionExpression"),
        )

    def test_scan_full_sets_ordering(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                AttributesToGet=["a"],
                ScanFilter={"a": _cond()},
                ConditionalOperator="AND",
                FilterExpression="a = :v",
                ProjectionExpression="b",
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            _mixing_msg(
                "AttributesToGet, ScanFilter, ConditionalOperator",
                "ProjectionExpression, FilterExpression",
            ),
        )

    def test_query_condop_with_key_condition_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.query(
                TableName=hash_table,
                ConditionalOperator="AND",
                KeyConditionExpression="pk = :p",
                ExpressionAttributeValues={":p": {"S": "k1"}},
            ),
            _mixing_msg("ConditionalOperator", "KeyConditionExpression"),
        )

    def test_query_full_sets_ordering(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.query(
                TableName=hash_table,
                KeyConditions={"pk": _cond("k1")},
                QueryFilter={"a": _cond()},
                ConditionalOperator="AND",
                KeyConditionExpression="pk = :p",
                FilterExpression="a = :v",
                ProjectionExpression="b",
                ExpressionAttributeValues={":p": {"S": "k1"}, ":v": {"S": "x"}},
            ),
            _mixing_msg(
                "QueryFilter, ConditionalOperator, KeyConditions",
                "ProjectionExpression, FilterExpression, KeyConditionExpression",
            ),
        )

    def test_put_item_condop_with_condition_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": "k2"}},
                ConditionalOperator="AND",
                ConditionExpression="attribute_exists(pk)",
            ),
            _mixing_msg("ConditionalOperator", "ConditionExpression"),
        )

    def test_put_item_expected_condop_with_condition_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": "k2"}},
                Expected={"a": _cond()},
                ConditionalOperator="AND",
                ConditionExpression="attribute_exists(pk)",
            ),
            _mixing_msg("Expected, ConditionalOperator", "ConditionExpression"),
        )

    def test_delete_item_condop_with_condition_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.delete_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ConditionalOperator="AND",
                ConditionExpression="attribute_exists(pk)",
            ),
            _mixing_msg("ConditionalOperator", "ConditionExpression"),
        )

    def test_update_item_condop_with_update_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.update_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ConditionalOperator="AND",
                UpdateExpression="SET a = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            _mixing_msg("ConditionalOperator", "UpdateExpression"),
        )

    def test_update_item_full_sets_ordering(self, dynamodb_client, hash_table):
        _expect_validation(
            lambda: dynamodb_client.update_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                AttributeUpdates={"a": {"Value": {"S": "v"}, "Action": "PUT"}},
                Expected={"a": _cond()},
                ConditionalOperator="AND",
                UpdateExpression="SET a = :v",
                ConditionExpression="attribute_exists(pk)",
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            _mixing_msg(
                "AttributeUpdates, Expected, ConditionalOperator",
                "UpdateExpression, ConditionExpression",
            ),
        )

    def test_scan_condop_mixing_beats_unused_values(self, dynamodb_client, hash_table):
        # The mixing error fires before the unused-ExpressionAttributeValues check.
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                ConditionalOperator="AND",
                FilterExpression="a = :v",
                ExpressionAttributeValues={":v": {"S": "x"}, ":unused": {"S": "y"}},
            ),
            _mixing_msg("ConditionalOperator", "FilterExpression"),
        )


class TestTableNameValidationPrecedence:
    """An invalid TableName wins over the ConditionalOperator and mixing errors."""

    @staticmethod
    def _expect_table_name_error(func):
        with pytest.raises(ClientError) as exc_info:
            func()
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "at 'tableName' failed to satisfy constraint" in err["Message"]

    def test_put_item_short_table_name_with_condop_mixing(
        self, dynamodb_client_no_validation
    ):
        self._expect_table_name_error(
            lambda: dynamodb_client_no_validation.put_item(
                TableName="aa",
                Item={"pk": {"S": "x"}},
                ConditionalOperator="AND",
                ConditionExpression="attribute_exists(pk)",
            )
        )

    def test_update_item_short_table_name_with_condop_mixing(
        self, dynamodb_client_no_validation
    ):
        self._expect_table_name_error(
            lambda: dynamodb_client_no_validation.update_item(
                TableName="aa",
                Key={"pk": {"S": "x"}},
                ConditionalOperator="AND",
                UpdateExpression="SET a = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        )

    def test_delete_item_short_table_name_with_condop(
        self, dynamodb_client_no_validation
    ):
        self._expect_table_name_error(
            lambda: dynamodb_client_no_validation.delete_item(
                TableName="aa",
                Key={"pk": {"S": "x"}},
                ConditionalOperator="AND",
            )
        )


class TestCrossAspectMixing:
    """Legacy params mix with expression params of a different aspect too."""

    def test_scan_scan_filter_with_projection_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                ScanFilter={"a": _cond()},
                ProjectionExpression="b",
            ),
            _mixing_msg("ScanFilter", "ProjectionExpression"),
        )

    def test_scan_attributes_to_get_with_filter_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.scan(
                TableName=hash_table,
                AttributesToGet=["a"],
                FilterExpression="a = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            _mixing_msg("AttributesToGet", "FilterExpression"),
        )

    def test_query_attributes_to_get_with_key_condition_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.query(
                TableName=hash_table,
                AttributesToGet=["a"],
                KeyConditionExpression="pk = :p",
                ExpressionAttributeValues={":p": {"S": "k1"}},
            ),
            _mixing_msg("AttributesToGet", "KeyConditionExpression"),
        )

    def test_query_query_filter_with_key_condition_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.query(
                TableName=hash_table,
                QueryFilter={"a": _cond()},
                KeyConditionExpression="pk = :p",
                ExpressionAttributeValues={":p": {"S": "k1"}},
            ),
            _mixing_msg("QueryFilter", "KeyConditionExpression"),
        )

    def test_update_item_attribute_updates_with_condition_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.update_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                AttributeUpdates={"a": {"Value": {"S": "v"}, "Action": "PUT"}},
                ConditionExpression="attribute_exists(pk)",
            ),
            _mixing_msg("AttributeUpdates", "ConditionExpression"),
        )

    def test_update_item_expected_with_update_expression(
        self, dynamodb_client, hash_table
    ):
        _expect_validation(
            lambda: dynamodb_client.update_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                Expected={"a": _cond(), "b": _cond("y")},
                UpdateExpression="SET c = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            ),
            _mixing_msg("Expected", "UpdateExpression"),
        )


class TestAcceptedCombinations:
    """Valid combinations must not be over-rejected by the fix."""

    def test_scan_condop_two_condition_filter(self, dynamodb_client, hash_table):
        resp = dynamodb_client.scan(
            TableName=hash_table,
            ConditionalOperator="AND",
            ScanFilter={"a": _cond("x"), "b": _cond("y")},
        )
        assert resp["Count"] == 1

    def test_scan_single_condition_filter_no_condop(self, dynamodb_client, hash_table):
        resp = dynamodb_client.scan(TableName=hash_table, ScanFilter={"a": _cond("x")})
        assert resp["Count"] == 1

    def test_query_condop_two_condition_filter(self, dynamodb_client, hash_table):
        resp = dynamodb_client.query(
            TableName=hash_table,
            KeyConditions={"pk": _cond("k1")},
            ConditionalOperator="OR",
            QueryFilter={"a": _cond("x"), "b": _cond("nope")},
        )
        assert resp["Count"] == 1

    def test_query_key_conditions_with_filter_expression(
        self, dynamodb_client, hash_table
    ):
        # KeyConditions is exempt from the mixing check when
        # KeyConditionExpression is absent.
        resp = dynamodb_client.query(
            TableName=hash_table,
            KeyConditions={"pk": _cond("k1")},
            FilterExpression="a = :v",
            ExpressionAttributeValues={":v": {"S": "x"}},
        )
        assert resp["Count"] == 1

    def test_query_key_conditions_with_projection_expression(
        self, dynamodb_client, hash_table
    ):
        resp = dynamodb_client.query(
            TableName=hash_table,
            KeyConditions={"pk": _cond("k1")},
            ProjectionExpression="a",
        )
        assert resp["Count"] == 1
        assert resp["Items"][0] == {"a": {"S": "x"}}

    def test_query_key_conditions_with_attributes_to_get(
        self, dynamodb_client, hash_table
    ):
        resp = dynamodb_client.query(
            TableName=hash_table,
            KeyConditions={"pk": _cond("k1")},
            AttributesToGet=["a"],
        )
        assert resp["Count"] == 1

    def test_put_item_condop_two_condition_expected(self, dynamodb_client, hash_table):
        resp = dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "k1"}, "a": {"S": "x"}, "b": {"S": "y"}},
            ConditionalOperator="AND",
            Expected={"a": _cond("x"), "b": _cond("y")},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_item_condop_two_condition_expected(
        self, dynamodb_client, hash_table
    ):
        dynamodb_client.put_item(
            TableName=hash_table,
            Item={"pk": {"S": "k3"}, "a": {"S": "x"}, "b": {"S": "y"}},
        )
        resp = dynamodb_client.delete_item(
            TableName=hash_table,
            Key={"pk": {"S": "k3"}},
            ConditionalOperator="OR",
            Expected={"a": _cond("x"), "b": _cond("zzz")},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

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
N = {"N": "1"}
KEY = {"pk": {"S": "p"}, "sk": {"S": "s"}}
UNDEF_NAME = "An expression attribute name used in the document path is not defined; attribute name: {}"
UNDEF_VALUE = "An expression attribute value used in expression is not defined; attribute value: {}"
KEY_CONDITION = "Invalid KeyConditionExpression: "
LIMIT = (
    "1 validation error detected: Value at 'Limit' failed to satisfy constraint: "
    "Member must have value greater than or equal to 1"
)


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


class TestKeyConditionFunctionNames:
    """A function in a key condition: the text depends on the name as written."""

    NOT_AN_OPERATOR = KEY_CONDITION + "Invalid operator used in KeyConditionExpression"
    NESTED = KEY_CONDITION + "KeyConditionExpressions cannot contain nested operations"

    @pytest.mark.parametrize(
        ("expression", "values", "message"),
        [
            ("pk = :p AND contains(sk, :v)", {":p": S, ":v": S}, NOT_AN_OPERATOR),
            ("pk = :p AND attribute_exists(sk)", {":p": S}, NOT_AN_OPERATOR),
            ("pk = :p AND attribute_not_exists(sk)", {":p": S}, NOT_AN_OPERATOR),
            ("pk = :p AND attribute_type(sk, :t)", {":p": S, ":t": S}, NOT_AN_OPERATOR),
            ("contains(pk, :v) AND sk = :s", {":v": S, ":s": S}, NOT_AN_OPERATOR),
            ("pk = :p AND size(sk) > :n", {":p": S, ":n": N}, NESTED),
            ("pk = :p AND foo(sk, :v)", {":p": S, ":v": S}, KEY_CONDITION + "Invalid function name; function: foo"),
            (
                "pk = :p AND Begins_With(sk, :v)",
                {":p": S, ":v": S},
                KEY_CONDITION + "Invalid function name; function: Begins_With",
            ),
        ],
        ids=["contains", "attribute_exists", "attribute_not_exists", "attribute_type", "contains-on-pk",
             "size", "unknown", "miscased"],
    )
    def test_the_refusal_follows_the_name(self, dynamodb_client, table, expression, values, message):
        _refused(
            lambda: dynamodb_client.query(
                TableName=table, KeyConditionExpression=expression, ExpressionAttributeValues=values
            ),
            message,
        )

    def test_begins_with_is_accepted(self, dynamodb_client, table):
        response = dynamodb_client.query(
            TableName=table,
            KeyConditionExpression="pk = :p AND begins_with(sk, :v)",
            ExpressionAttributeValues={":p": {"S": "p"}, ":v": {"S": "s"}},
        )
        assert response["Count"] == 1


class TestLimitBelowOne:
    """Query and Scan refuse a Limit under 1 with one text, ahead of the table and of every expression."""

    @pytest.mark.parametrize("limit", [0, -1])
    def test_the_text_is_the_same_on_an_existing_and_a_missing_table(
        self, dynamodb_client_no_validation, table, missing_table, limit
    ):
        client = dynamodb_client_no_validation
        for name in (table, missing_table):
            _refused(
                lambda: client.query(
                    TableName=name, KeyConditionExpression="pk = :p", ExpressionAttributeValues={":p": S}, Limit=limit
                ),
                LIMIT,
            )
            _refused(lambda: client.scan(TableName=name, Limit=limit), LIMIT)

    def test_it_comes_before_every_expression_check(self, dynamodb_client_no_validation, table):
        client = dynamodb_client_no_validation
        _refused(lambda: client.query(TableName=table, KeyConditionExpression="pk = :q", Limit=0), LIMIT)
        _refused(
            lambda: client.query(
                TableName=table,
                KeyConditionExpression="pk = :p",
                FilterExpression="n = ",
                ExpressionAttributeValues={":p": S},
                Limit=0,
            ),
            LIMIT,
        )
        _refused(lambda: client.scan(TableName=table, FilterExpression="n = ", Limit=0), LIMIT)


class TestLegacyAttributeUpdates:
    """The value type of an ADD or DELETE is refused with the legacy text, ahead of the table and the key."""

    ADD = "1 validation error detected: One or more parameter values were invalid: ADD action is not supported for the type {}"
    DELETE = (
        "1 validation error detected: One or more parameter values were invalid: "
        "DELETE action with value is not supported for the type {}"
    )

    @pytest.mark.parametrize(
        ("action", "value", "message"),
        [
            ("ADD", {"S": "x"}, ADD.format("S")),
            ("ADD", {"M": {}}, ADD.format("M")),
            ("ADD", {"BOOL": True}, ADD.format("BOOL")),
            ("DELETE", {"N": "1"}, DELETE.format("N")),
            ("DELETE", {"S": "x"}, DELETE.format("S")),
            ("DELETE", {"L": []}, DELETE.format("L")),
        ],
        ids=["add-S", "add-M", "add-BOOL", "delete-N", "delete-S", "delete-L"],
    )
    def test_the_value_type_is_refused(self, dynamodb_client, table, action, value, message):
        _refused(
            lambda: dynamodb_client.update_item(
                TableName=table, Key=KEY, AttributeUpdates={"a": {"Action": action, "Value": value}}
            ),
            message,
        )

    def test_the_check_comes_before_the_table_and_the_key(self, dynamodb_client, table, missing_table):
        refusal = self.ADD.format("S")
        update = {"a": {"Action": "ADD", "Value": {"S": "x"}}}
        for name, key in (
            (missing_table, KEY),
            (table, {"pk": {"N": "1"}, "sk": {"S": "s"}}),
            (table, {"pk": {"S": "p"}}),
        ):
            _refused(
                lambda: dynamodb_client.update_item(TableName=name, Key=key, AttributeUpdates=update), refusal
            )


class TestEmptyExpressionStrings:
    """An empty expression string is refused as empty."""

    def test_a_condition_check_with_an_empty_condition(self, dynamodb_client_no_validation, table):
        _refused(
            lambda: dynamodb_client_no_validation.transact_write_items(
                TransactItems=[{"ConditionCheck": {"TableName": table, "Key": KEY, "ConditionExpression": ""}}]
            ),
            "Invalid ConditionExpression: The expression can not be empty;",
        )

    def test_an_update_with_an_empty_update_expression(self, dynamodb_client_no_validation, table):
        client = dynamodb_client_no_validation
        _refused(
            lambda: client.transact_write_items(
                TransactItems=[{"Update": {"TableName": table, "Key": KEY, "UpdateExpression": ""}}]
            ),
            "Invalid UpdateExpression: The expression can not be empty;",
        )
        _refused(
            lambda: client.update_item(TableName=table, Key=KEY, UpdateExpression=""),
            "1 validation error detected: Invalid UpdateExpression: The expression can not be empty;",
        )

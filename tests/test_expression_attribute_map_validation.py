# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""ExpressionAttributeNames / ExpressionAttributeValues map-entry validation.

Distinct from the "unused" and "can only be specified when using expressions"
checks: this validates the map entries themselves. Amazon DynamoDB rejects:

  - a placeholder key that is not ``<prefix><ident>`` (prefix-only ``#`` / ``:``,
    or characters outside ``[A-Za-z0-9_]``) with a Syntax error,
  - a placeholder key longer than 255 bytes (including the prefix),
  - an ExpressionAttributeNames mapped value that is the empty string,
  - an ExpressionAttributeValues AttributeValue that carries no datatype.

Key-length is checked before key-syntax (a too-long key with an invalid
character reports the length error).

Messages captured directly from Amazon DynamoDB (asomasun-admin, us-east-1).
Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table


@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """Hash-only table for the class, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "foo": {"S": "a"}, "bar": {"S": "b"}},
        )
        yield name


class TestExpressionAttributeNamesEntries:
    """ExpressionAttributeNames map entries must be well-formed."""

    def test_name_key_prefix_only(self, dynamodb_client_no_validation, hash_table):
        # "#" alone is not a valid placeholder.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="#",
                ExpressionAttributeNames={"#": "foo"},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            'ExpressionAttributeNames contains invalid key: Syntax error; key: "#"'
        )

    def test_name_key_invalid_char(self, dynamodb_client_no_validation, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="#a",
                ExpressionAttributeNames={"#a-b": "foo"},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            'ExpressionAttributeNames contains invalid key: Syntax error; key: "#a-b"'
        )

    def test_name_key_too_long(self, dynamodb_client_no_validation, hash_table):
        # 256 bytes including the '#' prefix => too long (255 is the max).
        key = "#" + "a" * 255
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="#a",
                ExpressionAttributeNames={key: "foo"},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "ExpressionAttributeNames contains invalid key: The expression "
            "attribute map contains a key that is too long; size of key: 256"
        )

    def test_name_key_length_before_syntax(self, dynamodb_client_no_validation, hash_table):
        # A key that is both too long and has an invalid char reports length.
        key = "#" + "a" * 255 + "-x"
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="#a",
                ExpressionAttributeNames={key: "foo"},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "too long" in err["Message"]
        assert "size of key: 258" in err["Message"]

    def test_name_empty_mapped_value(self, dynamodb_client_no_validation, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="#a",
                ExpressionAttributeNames={"#a": ""},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "ExpressionAttributeNames contains invalid value: "
            "Empty attribute name for key #a"
        )

    def test_name_valid_entry_succeeds(self, dynamodb_client, hash_table):
        # Positive control: digit-bearing ident and underscore are valid.
        resp = dynamodb_client.get_item(
            TableName=hash_table,
            Key={"pk": {"S": "k1"}},
            ProjectionExpression="#a_1",
            ExpressionAttributeNames={"#a_1": "foo"},
        )
        assert resp["Item"] == {"foo": {"S": "a"}}

    def test_name_non_string_value_is_serialization_error(
        self, dynamodb_client_no_validation, hash_table
    ):
        # Non-string value is a wire type mismatch: Amazon DynamoDB returns
        # SerializationException ("NUMBER_VALUE cannot be converted to String").
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="#a",
                ExpressionAttributeNames={"#a": 5},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "SerializationException"
        assert "Empty attribute name" not in err["Message"]


class TestExpressionAttributeValuesEntries:
    """ExpressionAttributeValues map entries must be well-formed."""

    def test_value_key_prefix_only(self, dynamodb_client_no_validation, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.scan(
                TableName=hash_table,
                FilterExpression="foo = :",
                ExpressionAttributeValues={":": {"S": "x"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            'ExpressionAttributeValues contains invalid key: Syntax error; key: ":"'
        )

    def test_value_key_invalid_char(self, dynamodb_client_no_validation, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.scan(
                TableName=hash_table,
                FilterExpression="foo = :v",
                ExpressionAttributeValues={":a-b": {"S": "x"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            'ExpressionAttributeValues contains invalid key: Syntax error; key: ":a-b"'
        )

    def test_value_key_too_long(self, dynamodb_client_no_validation, hash_table):
        key = ":" + "a" * 255
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.scan(
                TableName=hash_table,
                FilterExpression="foo = :v",
                ExpressionAttributeValues={key: {"S": "x"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        # Quirk: unlike ExpressionAttributeNames, the values variant omits the
        # "; size of key: N" suffix.
        assert err["Message"] == (
            "ExpressionAttributeValues contains invalid key: The expression "
            "attribute map contains a key that is too long;"
        )

    def test_value_empty_attribute_value(self, dynamodb_client_no_validation, hash_table):
        # An AttributeValue with no datatype member set.
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client_no_validation.scan(
                TableName=hash_table,
                FilterExpression="foo = :v",
                ExpressionAttributeValues={":v": {}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "ExpressionAttributeValues contains invalid value: Supplied "
            "AttributeValue is empty, must contain exactly one of the "
            "supported datatypes for key :v"
        )

    def test_value_empty_string_is_accepted(self, dynamodb_client, hash_table):
        # Positive control: an empty *string* value is valid (no match here).
        resp = dynamodb_client.scan(
            TableName=hash_table,
            FilterExpression="foo = :v",
            ExpressionAttributeValues={":v": {"S": ""}},
        )
        assert resp["Count"] == 0

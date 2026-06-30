# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""KeyConditionExpression rejects document-path (nested) access on key attributes.

Amazon DynamoDB rejects any KeyConditionExpression clause that uses a document
path (`attr.sub` or `attr[i]`) on the keyed attribute, with:

    Invalid KeyConditionExpression: KeyConditionExpressions cannot have
    conditions on nested attributes

The check applies to both the partition and sort key clauses, to `.` and `[]`
access, and to paths supplied through ExpressionAttributeNames. It fires after
the reserved-keyword check but before the key-schema-element check.

Dual-target: runs against ExtendDB and Amazon DynamoDB unchanged.
"""

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

NESTED_MSG = "cannot have conditions on nested attributes"


@pytest.fixture(scope="class")
def kc_table(dynamodb_client):
    """Composite (S,S) table with one seeded item."""
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
        dynamodb_client.put_item(
            TableName=name,
            Item={
                "pk": {"S": "p1"},
                "sk": {"S": "s1"},
                "info": {"M": {"foo": {"S": "bar"}}},
            },
        )
        yield name


def _query(dynamodb_client, table, kce, values, names=None):
    kwargs = {
        "TableName": table,
        "KeyConditionExpression": kce,
        "ExpressionAttributeValues": values,
    }
    if names is not None:
        kwargs["ExpressionAttributeNames"] = names
    return dynamodb_client.query(**kwargs)


class TestKeyConditionNestedAttributes:
    """A nested path on a key attribute in a KeyConditionExpression is rejected."""

    def test_pk_dot_path_rejected(self, dynamodb_client, kc_table):
        with pytest.raises(ClientError) as exc:
            _query(dynamodb_client, kc_table, "pk.foo = :v", {":v": {"S": "p1"}})
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_pk_index_path_rejected(self, dynamodb_client, kc_table):
        with pytest.raises(ClientError) as exc:
            _query(dynamodb_client, kc_table, "pk[0] = :v", {":v": {"S": "p1"}})
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_sk_dot_path_rejected(self, dynamodb_client, kc_table):
        with pytest.raises(ClientError) as exc:
            _query(
                dynamodb_client,
                kc_table,
                "pk = :p AND sk.foo = :v",
                {":p": {"S": "p1"}, ":v": {"S": "s1"}},
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_sk_index_path_rejected(self, dynamodb_client, kc_table):
        with pytest.raises(ClientError) as exc:
            _query(
                dynamodb_client,
                kc_table,
                "pk = :p AND sk[0] = :v",
                {":p": {"S": "p1"}, ":v": {"S": "s1"}},
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_sk_nested_in_begins_with_rejected(self, dynamodb_client, kc_table):
        with pytest.raises(ClientError) as exc:
            _query(
                dynamodb_client,
                kc_table,
                "pk = :p AND begins_with(sk.foo, :v)",
                {":p": {"S": "p1"}, ":v": {"S": "s"}},
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_sk_nested_in_between_rejected(self, dynamodb_client, kc_table):
        with pytest.raises(ClientError) as exc:
            _query(
                dynamodb_client,
                kc_table,
                "pk = :p AND sk.foo BETWEEN :a AND :b",
                {":p": {"S": "p1"}, ":a": {"S": "a"}, ":b": {"S": "z"}},
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_sk_index_in_begins_with_rejected(self, dynamodb_client, kc_table):
        with pytest.raises(ClientError) as exc:
            _query(
                dynamodb_client,
                kc_table,
                "pk = :p AND begins_with(sk[0], :v)",
                {":p": {"S": "p1"}, ":v": {"S": "s"}},
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_reversed_value_first_nested_sk_rejected(self, dynamodb_client, kc_table):
        # Value-first comparison form (`:v < sk.foo`) is still rejected.
        with pytest.raises(ClientError) as exc:
            _query(
                dynamodb_client,
                kc_table,
                "pk = :p AND :v < sk.foo",
                {":p": {"S": "p1"}, ":v": {"S": "a"}},
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_single_clause_non_eq_nested_pk_rejected(self, dynamodb_client, kc_table):
        # The nested error fires before the "partition key must use equality" error.
        with pytest.raises(ClientError) as exc:
            _query(dynamodb_client, kc_table, "pk.foo > :v", {":v": {"S": "a"}})
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_nested_via_expression_attribute_name_rejected(
        self, dynamodb_client, kc_table
    ):
        with pytest.raises(ClientError) as exc:
            _query(
                dynamodb_client,
                kc_table,
                "#d.foo = :v",
                {":v": {"S": "p1"}},
                names={"#d": "pk"},
            )
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    def test_nested_non_key_attribute_rejected_as_nested(
        self, dynamodb_client, kc_table
    ):
        # Precedence: a nested path on a non-key, non-reserved attribute is
        # rejected as nested, NOT as a missed key-schema element.
        with pytest.raises(ClientError) as exc:
            _query(dynamodb_client, kc_table, "foo.bar = :v", {":v": {"S": "x"}})
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert NESTED_MSG in err["Message"]

    # --- precedence / control cases (must NOT report the nested error) ---

    def test_reserved_keyword_beats_nested(self, dynamodb_client, kc_table):
        # `data` is a reserved word; the reserved-keyword check fires first.
        with pytest.raises(ClientError) as exc:
            _query(dynamodb_client, kc_table, "data.foo = :v", {":v": {"S": "x"}})
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "reserved keyword" in err["Message"]
        assert NESTED_MSG not in err["Message"]

    def test_top_level_non_key_is_schema_missed(self, dynamodb_client, kc_table):
        with pytest.raises(ClientError) as exc:
            _query(dynamodb_client, kc_table, "foo = :v", {":v": {"S": "x"}})
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "missed key schema element" in err["Message"]
        assert NESTED_MSG not in err["Message"]

    def test_top_level_key_paths_accepted(self, dynamodb_client, kc_table):
        resp = _query(
            dynamodb_client,
            kc_table,
            "pk = :p AND sk = :s",
            {":p": {"S": "p1"}, ":s": {"S": "s1"}},
        )
        assert resp["Count"] == 1

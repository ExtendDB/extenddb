# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Overlapping ProjectionExpression paths (C9).

Amazon DynamoDB rejects a ProjectionExpression in which one path is a prefix
of another (for example ``a`` and ``a.b``, or ``a[0]`` and ``a[0].b``, or a
duplicate ``a`` and ``a``) with a ValidationException. Sibling paths such as
``a.b`` and ``a.c`` are accepted. The rejection happens during request
validation, so it fires even when the item does not exist, and it applies to
every read API that takes a ProjectionExpression. All messages here were
captured directly from Amazon DynamoDB via the AWS CLI.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

OVERLAP = (
    "Invalid ProjectionExpression: Two document paths overlap with each other; "
    "must remove or rewrite one of these paths; path one: {one}, path two: {two}"
)


def _validation_message(excinfo) -> str:
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ValidationException"
    return err["Message"]


class TestProjectionOverlap:
    @pytest.fixture(scope="class")
    def overlap_table(self, dynamodb_client):
        with scoped_table(dynamodb_client) as name:
            dynamodb_client.put_item(
                TableName=name,
                Item={
                    "pk": {"S": "k1"},
                    "a": {"M": {"b": {"S": "bv"}, "c": {"S": "cv"}}},
                    "x": {"S": "xv"},
                    "alist": {"L": [{"M": {"b": {"S": "lb"}}}]},
                },
            )
            yield name

    def test_get_item_parent_then_child(self, dynamodb_client, overlap_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.get_item(
                TableName=overlap_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="a, a.b",
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a]", two="[a, b]")

    def test_get_item_child_then_parent(self, dynamodb_client, overlap_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.get_item(
                TableName=overlap_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="a.b, a",
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a, b]", two="[a]")

    def test_get_item_exact_duplicate(self, dynamodb_client, overlap_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.get_item(
                TableName=overlap_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="a, a",
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a]", two="[a]")

    def test_get_item_missing_item_still_validates(self, dynamodb_client, overlap_table):
        # Validation runs before the lookup: a nonexistent key still errors.
        with pytest.raises(ClientError) as ei:
            dynamodb_client.get_item(
                TableName=overlap_table,
                Key={"pk": {"S": "does-not-exist"}},
                ProjectionExpression="a, a.b",
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a]", two="[a, b]")

    def test_get_item_index_path_rendering(self, dynamodb_client, overlap_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.get_item(
                TableName=overlap_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="alist[0], alist",
            )
        assert _validation_message(ei) == OVERLAP.format(one="[alist, [0]]", two="[alist]")

    def test_get_item_resolved_names(self, dynamodb_client, overlap_table):
        # The message reports resolved attribute names, not the #placeholders.
        with pytest.raises(ClientError) as ei:
            dynamodb_client.get_item(
                TableName=overlap_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="#p, #p.#q",
                ExpressionAttributeNames={"#p": "a", "#q": "b"},
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a]", two="[a, b]")

    def test_get_item_first_pair_in_order(self, dynamodb_client, overlap_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.get_item(
                TableName=overlap_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression="x, a.b, a",
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a, b]", two="[a]")

    def test_batch_get_item_overlap(self, dynamodb_client, overlap_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.batch_get_item(
                RequestItems={
                    overlap_table: {
                        "Keys": [{"pk": {"S": "k1"}}],
                        "ProjectionExpression": "a, a.b",
                    }
                }
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a]", two="[a, b]")

    def test_query_overlap(self, dynamodb_client, overlap_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.query(
                TableName=overlap_table,
                KeyConditionExpression="pk = :p",
                ExpressionAttributeValues={":p": {"S": "k1"}},
                ProjectionExpression="a, a.b",
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a]", two="[a, b]")

    def test_scan_overlap(self, dynamodb_client, overlap_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.scan(
                TableName=overlap_table,
                ProjectionExpression="a, a.b",
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a]", two="[a, b]")

    def test_transact_get_items_overlap(self, dynamodb_client, overlap_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.transact_get_items(
                TransactItems=[
                    {
                        "Get": {
                            "TableName": overlap_table,
                            "Key": {"pk": {"S": "k1"}},
                            "ProjectionExpression": "a, a.b",
                        }
                    }
                ]
            )
        assert _validation_message(ei) == OVERLAP.format(one="[a]", two="[a, b]")

    def test_siblings_accepted(self, dynamodb_client, overlap_table):
        # Non-overlapping sibling paths must still work.
        resp = dynamodb_client.get_item(
            TableName=overlap_table,
            Key={"pk": {"S": "k1"}},
            ProjectionExpression="a.b, a.c",
        )
        assert resp["Item"]["a"]["M"].keys() == {"b", "c"}

# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Expression size-limit validation.

Amazon DynamoDB caps each expression string at 4096 bytes, measured on the
raw expression text (before #name / :value substitution). Over the limit it
returns ``Invalid <Param>: Expression size has exceeded the maximum allowed
size;``.

A captured quirk: FilterExpression and ConditionExpression append
`` expression size: <N>`` (N = raw byte length); ProjectionExpression,
UpdateExpression, and KeyConditionExpression do not.

Messages captured directly from Amazon DynamoDB (asomasun-admin, us-east-1).
Dual-target against Amazon DynamoDB and extenddb.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from conftest import scoped_table

MAX = 4096
PREFIX = "Expression size has exceeded the maximum allowed size;"


@pytest.fixture(scope="class")
def hash_table(dynamodb_client):
    """Hash-only table for the class, deleted on teardown."""
    with scoped_table(dynamodb_client) as name:
        dynamodb_client.put_item(
            TableName=name,
            Item={"pk": {"S": "k1"}, "foo": {"S": "a"}},
        )
        yield name


def _name_of_len(total: int) -> str:
    """A single valid attribute-name path of exactly ``total`` bytes."""
    return "a" + "z" * (total - 1)


class TestExpressionSizeLimit:
    """Each expression string is capped at 4096 raw bytes."""

    def test_projection_over_limit(self, dynamodb_client, hash_table):
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.get_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                ProjectionExpression=_name_of_len(MAX + 1),
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        # Projection has no "expression size: N" suffix.
        assert err["Message"] == f"Invalid ProjectionExpression: {PREFIX}"

    def test_projection_at_limit_ok(self, dynamodb_client, hash_table):
        # Exactly 4096 bytes is accepted (resolves to a missing attribute).
        resp = dynamodb_client.get_item(
            TableName=hash_table,
            Key={"pk": {"S": "k1"}},
            ProjectionExpression=_name_of_len(MAX),
        )
        assert resp.get("Item", {}) == {}

    def test_filter_over_limit_has_size_suffix(self, dynamodb_client, hash_table):
        path = _name_of_len(MAX + 1)
        expr = f"{path} = :v"
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.scan(
                TableName=hash_table,
                FilterExpression=expr,
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        # Filter appends the raw byte length.
        assert err["Message"] == (
            f"Invalid FilterExpression: {PREFIX} expression size: {len(expr)}"
        )

    def test_condition_over_limit_has_size_suffix(self, dynamodb_client, hash_table):
        path = _name_of_len(MAX + 1)
        expr = f"attribute_not_exists({path})"
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.put_item(
                TableName=hash_table,
                Item={"pk": {"S": "sztest"}},
                ConditionExpression=expr,
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            f"Invalid ConditionExpression: {PREFIX} expression size: {len(expr)}"
        )

    def test_key_condition_over_limit_no_suffix(self, dynamodb_client, hash_table):
        path = _name_of_len(MAX + 1)
        expr = f"pk = :p AND {path} = :q"
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.query(
                TableName=hash_table,
                KeyConditionExpression=expr,
                ExpressionAttributeValues={":p": {"S": "k1"}, ":q": {"S": "x"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == f"Invalid KeyConditionExpression: {PREFIX}"

    def test_update_over_limit_no_suffix(self, dynamodb_client, hash_table):
        path = _name_of_len(MAX + 1)
        expr = f"SET {path} = :v"
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.update_item(
                TableName=hash_table,
                Key={"pk": {"S": "k1"}},
                UpdateExpression=expr,
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        err = exc_info.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == f"Invalid UpdateExpression: {PREFIX}"

    def test_size_measured_on_raw_text_not_substituted(self, dynamodb_client, hash_table):
        # 686 short #aN placeholders => raw ~4005 bytes (under limit) even though
        # the substituted form would be larger. Must be accepted.
        names = {f"#a{i}": f"attribute{i}" for i in range(686)}
        proj = ",".join(names.keys())
        assert len(proj) <= MAX
        resp = dynamodb_client.get_item(
            TableName=hash_table,
            Key={"pk": {"S": "k1"}},
            ProjectionExpression=proj,
            ExpressionAttributeNames=names,
        )
        assert resp.get("Item", {}) == {}

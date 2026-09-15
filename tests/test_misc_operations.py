# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for operations with zero prior coverage.

Covers: TagResource, UntagResource, ListTagsOfResource,
        DescribeEndpoints, DescribeLimits.
"""

from __future__ import annotations

import uuid

import pytest
from botocore.exceptions import ClientError
from conftest import wait_for_active, wait_for_deleted
@pytest.fixture()
def tagged_table(dynamodb_client):
    """Create a table for tagging tests, yield (table_name, table_arn), cleanup."""
    table_name = f"extenddb-tag-{uuid.uuid4().hex[:8]}"
    resp = dynamodb_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, table_name)
    table_arn = resp["TableDescription"]["TableArn"]
    yield table_name, table_arn
    try:
        dynamodb_client.delete_table(TableName=table_name)
        wait_for_deleted(dynamodb_client, table_name)
    except Exception:
        pass
class TestTagResource:
    """TagResource operation tests."""

    def test_tags_do_not_survive_table_recreation(
        self, dynamodb_client, create_and_cleanup_table, unique_table_name
    ):
        """Deleting and recreating a table with the same name starts untagged."""
        first = create_and_cleanup_table(unique_table_name)
        first_arn = first["TableDescription"]["TableArn"]
        dynamodb_client.tag_resource(
            ResourceArn=first_arn,
            Tags=[{"Key": "Lifecycle", "Value": "stale-must-not-survive"}],
        )

        dynamodb_client.delete_table(TableName=unique_table_name)
        wait_for_deleted(dynamodb_client, unique_table_name)

        second = create_and_cleanup_table(unique_table_name)
        second_arn = second["TableDescription"]["TableArn"]
        assert second_arn == first_arn
        assert dynamodb_client.list_tags_of_resource(ResourceArn=second_arn)["Tags"] == []

    def test_tag_and_list(self, dynamodb_client, tagged_table):
        _table_name, table_arn = tagged_table
        dynamodb_client.tag_resource(
            ResourceArn=table_arn,
            Tags=[
                {"Key": "Environment", "Value": "Test"},
                {"Key": "Project", "Value": "extenddb"},
            ],
        )
        resp = dynamodb_client.list_tags_of_resource(ResourceArn=table_arn)
        tags = {t["Key"]: t["Value"] for t in resp.get("Tags", [])}
        assert tags["Environment"] == "Test"
        assert tags["Project"] == "extenddb"

    def test_tag_overwrite(self, dynamodb_client, tagged_table):
        _table_name, table_arn = tagged_table
        dynamodb_client.tag_resource(
            ResourceArn=table_arn,
            Tags=[{"Key": "Env", "Value": "v1"}],
        )
        dynamodb_client.tag_resource(
            ResourceArn=table_arn,
            Tags=[{"Key": "Env", "Value": "v2"}],
        )
        resp = dynamodb_client.list_tags_of_resource(ResourceArn=table_arn)
        tags = {t["Key"]: t["Value"] for t in resp.get("Tags", [])}
        assert tags["Env"] == "v2"

    def test_tag_nonexistent_resource(self, dynamodb_client):
        """TagResource on a nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.tag_resource(
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/NoSuchTable",
                Tags=[{"Key": "k", "Value": "v"}],
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
class TestUntagResource:
    """UntagResource operation tests."""

    def test_untag(self, dynamodb_client, tagged_table):
        _table_name, table_arn = tagged_table
        dynamodb_client.tag_resource(
            ResourceArn=table_arn,
            Tags=[
                {"Key": "A", "Value": "1"},
                {"Key": "B", "Value": "2"},
                {"Key": "C", "Value": "3"},
            ],
        )
        dynamodb_client.untag_resource(
            ResourceArn=table_arn,
            TagKeys=["B"],
        )
        resp = dynamodb_client.list_tags_of_resource(ResourceArn=table_arn)
        keys = [t["Key"] for t in resp.get("Tags", [])]
        assert "B" not in keys
        assert "A" in keys
        assert "C" in keys

    def test_untag_nonexistent_key(self, dynamodb_client, tagged_table):
        """Untagging a key that doesn't exist should not error."""
        _table_name, table_arn = tagged_table
        # Should not raise.
        dynamodb_client.untag_resource(
            ResourceArn=table_arn,
            TagKeys=["NonexistentKey"],
        )

    def test_untag_nonexistent_resource(self, dynamodb_client):
        """UntagResource on a nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.untag_resource(
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/NoSuchTable",
                TagKeys=["k"],
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
class TestListTagsOfResource:
    """ListTagsOfResource operation tests."""

    def test_empty_tags(self, dynamodb_client, tagged_table):
        """A new table with no tags returns an empty list."""
        _table_name, table_arn = tagged_table
        # Remove all tags first.
        resp = dynamodb_client.list_tags_of_resource(ResourceArn=table_arn)
        existing_keys = [t["Key"] for t in resp.get("Tags", [])]
        if existing_keys:
            dynamodb_client.untag_resource(
                ResourceArn=table_arn, TagKeys=existing_keys,
            )
        resp = dynamodb_client.list_tags_of_resource(ResourceArn=table_arn)
        assert resp.get("Tags", []) == []

    def test_list_tags_nonexistent_resource(self, dynamodb_client):
        """ListTagsOfResource on a nonexistent table returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb_client.list_tags_of_resource(
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/NoSuchTable",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
class TestDescribeEndpoints:
    """DescribeEndpoints operation tests."""

    def test_describe_endpoints(self, dynamodb_client):
        resp = dynamodb_client.describe_endpoints()
        endpoints = resp.get("Endpoints", [])
        assert len(endpoints) >= 1
        ep = endpoints[0]
        assert "Address" in ep
        assert "CachePeriodInMinutes" in ep
        assert isinstance(ep["CachePeriodInMinutes"], int)
class TestDescribeLimits:
    """DescribeLimits operation tests."""

    def test_describe_limits(self, dynamodb_client):
        resp = dynamodb_client.describe_limits()
        assert "AccountMaxReadCapacityUnits" in resp
        assert "AccountMaxWriteCapacityUnits" in resp
        assert "TableMaxReadCapacityUnits" in resp
        assert "TableMaxWriteCapacityUnits" in resp
        # All should be positive integers.
        assert resp["AccountMaxReadCapacityUnits"] > 0
        assert resp["AccountMaxWriteCapacityUnits"] > 0
        assert resp["TableMaxReadCapacityUnits"] > 0
        assert resp["TableMaxWriteCapacityUnits"] > 0

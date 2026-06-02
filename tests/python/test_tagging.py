# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tagging tests: TagResource, UntagResource, ListTagsOfResource.

Covers add/remove/list tags, overwrite, error paths, and empty tags.
"""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError


class TestTagging:
    """TagResource, UntagResource, ListTagsOfResource API tests."""

    def _get_table_arn(self, dynamodb_client, table_name: str) -> str:
        resp = dynamodb_client.describe_table(TableName=table_name)
        return resp["Table"]["TableArn"]

    def test_tag_and_list(self, table_factory, dynamodb_client):
        """Tag a table and list the tags."""
        name = table_factory()
        arn = self._get_table_arn(dynamodb_client, name)
        dynamodb_client.tag_resource(
            ResourceArn=arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        resp = dynamodb_client.list_tags_of_resource(ResourceArn=arn)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

    def test_tag_overwrite(self, table_factory, dynamodb_client):
        """Tagging with an existing key overwrites the value."""
        name = table_factory()
        arn = self._get_table_arn(dynamodb_client, name)
        dynamodb_client.tag_resource(
            ResourceArn=arn, Tags=[{"Key": "env", "Value": "dev"}]
        )
        dynamodb_client.tag_resource(
            ResourceArn=arn, Tags=[{"Key": "env", "Value": "prod"}]
        )
        resp = dynamodb_client.list_tags_of_resource(ResourceArn=arn)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "prod"

    def test_untag(self, table_factory, dynamodb_client):
        """Remove a tag from a table."""
        name = table_factory()
        arn = self._get_table_arn(dynamodb_client, name)
        dynamodb_client.tag_resource(
            ResourceArn=arn,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "x"}],
        )
        dynamodb_client.untag_resource(ResourceArn=arn, TagKeys=["env"])
        resp = dynamodb_client.list_tags_of_resource(ResourceArn=arn)
        keys = [t["Key"] for t in resp["Tags"]]
        assert "env" not in keys
        assert "team" in keys

    def test_untag_nonexistent_key(self, table_factory, dynamodb_client):
        """Untagging a nonexistent key succeeds silently."""
        name = table_factory()
        arn = self._get_table_arn(dynamodb_client, name)
        # Should not raise
        dynamodb_client.untag_resource(ResourceArn=arn, TagKeys=["no-such-key"])

    def test_list_tags_empty(self, table_factory, dynamodb_client):
        """ListTagsOfResource on a table with no tags returns empty list."""
        name = table_factory()
        arn = self._get_table_arn(dynamodb_client, name)
        resp = dynamodb_client.list_tags_of_resource(ResourceArn=arn)
        assert resp["Tags"] == []

    def test_tag_nonexistent_resource(self, table_factory, dynamodb_client):
        """Tagging a nonexistent resource fails."""
        # Use a real table's ARN to extract the correct account/region prefix,
        # then reference a table that doesn't exist.
        name = table_factory()
        real_arn = self._get_table_arn(dynamodb_client, name)
        fake_arn = real_arn.rsplit("/", 1)[0] + "/nonexistent-xyz-99"
        with pytest.raises(ClientError) as exc:
            dynamodb_client.tag_resource(
                ResourceArn=fake_arn, Tags=[{"Key": "k", "Value": "v"}]
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_tags_nonexistent_resource(self, table_factory, dynamodb_client):
        """ListTagsOfResource on a nonexistent resource fails."""
        name = table_factory()
        real_arn = self._get_table_arn(dynamodb_client, name)
        fake_arn = real_arn.rsplit("/", 1)[0] + "/nonexistent-xyz-99"
        with pytest.raises(ClientError) as exc:
            dynamodb_client.list_tags_of_resource(ResourceArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

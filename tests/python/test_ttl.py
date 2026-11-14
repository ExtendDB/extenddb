# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""TTL (Time to Live) tests.

Covers UpdateTimeToLive, DescribeTimeToLive, enable/disable,
and error paths.
"""

from __future__ import annotations

import os
import time

import pytest
from botocore.exceptions import ClientError

_extenddb_only = pytest.mark.skipif(
    os.environ.get("EXTENDDB_VALIDATION_MODE", "").lower() == "true",
    reason="extenddb_only: skipped in validation mode",
)


class TestTimeToLive:
    """UpdateTimeToLive and DescribeTimeToLive API tests."""

    def test_describe_ttl_disabled(self, table_factory, dynamodb_client):
        """DescribeTimeToLive on a table with TTL disabled."""
        name = table_factory()
        resp = dynamodb_client.describe_time_to_live(TableName=name)
        ttl = resp["TimeToLiveDescription"]
        assert ttl["TimeToLiveStatus"] in ("DISABLED", "DISABLING")

    def test_enable_ttl(self, table_factory, dynamodb_client):
        """Enable TTL on a table."""
        name = table_factory()
        dynamodb_client.update_time_to_live(
            TableName=name,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "expires_at",
            },
        )
        resp = dynamodb_client.describe_time_to_live(TableName=name)
        ttl = resp["TimeToLiveDescription"]
        assert ttl["TimeToLiveStatus"] in ("ENABLED", "ENABLING")
        assert ttl["AttributeName"] == "expires_at"

    @_extenddb_only
    def test_disable_ttl(self, table_factory, dynamodb_client):
        """Disable TTL after enabling it.

        Real DynamoDB enforces a ~1-hour cooldown between enable and disable.
        This test only runs against extenddb.
        """
        name = table_factory()
        dynamodb_client.update_time_to_live(
            TableName=name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
        )
        # Enable returns during ENABLING now (the backfill is detached, as in
        # DynamoDB) and updates are rejected until the transition settles, so
        # wait for ENABLED like a real client must.
        for _ in range(120):
            resp = dynamodb_client.describe_time_to_live(TableName=name)
            if resp["TimeToLiveDescription"]["TimeToLiveStatus"] == "ENABLED":
                break
            time.sleep(0.25)
        else:
            pytest.fail("TTL enable did not settle within 30s")
        dynamodb_client.update_time_to_live(
            TableName=name,
            TimeToLiveSpecification={"Enabled": False, "AttributeName": "ttl"},
        )
        resp = dynamodb_client.describe_time_to_live(TableName=name)
        ttl = resp["TimeToLiveDescription"]
        assert ttl["TimeToLiveStatus"] in ("DISABLED", "DISABLING")

    def test_enable_ttl_nonexistent_table(self, dynamodb_client):
        """Enable TTL on a nonexistent table fails."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.update_time_to_live(
                TableName="nonexistent-table-xyz",
                TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_ttl_nonexistent_table(self, dynamodb_client):
        """DescribeTimeToLive on a nonexistent table fails."""
        with pytest.raises(ClientError) as exc:
            dynamodb_client.describe_time_to_live(TableName="nonexistent-table-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_ttl_enabled(self, table_factory, dynamodb_client):
        """DescribeTimeToLive returns correct attribute name when enabled."""
        name = table_factory()
        dynamodb_client.update_time_to_live(
            TableName=name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "my_ttl"},
        )
        resp = dynamodb_client.describe_time_to_live(TableName=name)
        assert resp["TimeToLiveDescription"]["AttributeName"] == "my_ttl"

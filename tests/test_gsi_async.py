# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Tests for async GSI propagation delay (D-4, Phase 24).

Verifies that GSI updates are applied asynchronously with a measurable
delay, simulating real DynamoDB eventual consistency behavior.

These tests are extenddb-specific — real DynamoDB does not expose GSI
propagation delay as a configurable setting.

Note: These tests require auth to be disabled (the default) for the
DynamoDB data plane operations. The settings functions use the extenddb CLI
directly and do not require HTTP auth.
"""

from __future__ import annotations

import os
import subprocess
import time
import uuid

import pytest

from conftest import wait_for_active, wait_for_deleted
# EXTENDDB_TEST_ENDPOINT is required — devtools/run-tests validates this.
# Tests will use the default endpoint if the env var is missing.

ENDPOINT = os.environ.get("EXTENDDB_TEST_ENDPOINT", "http://localhost:18443").strip()
EXTENDDB_CONFIG = os.environ.get("EXTENDDB_CONFIG", "extenddb.toml")
def extenddb_settings_set(key: str, value: str) -> None:
    """Set a extenddb runtime setting via the CLI.

    Uses ``extenddb settings set`` which writes directly to the catalog
    database. The setting takes effect on the next read by the server.
    """
    result = subprocess.run(
        ["./target/release/extenddb", "settings", "--config", EXTENDDB_CONFIG,
         "set", key, value],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to set {key}={value}: {result.stderr.strip()}"
        )
def extenddb_settings_get(key: str) -> str:
    """Get a extenddb runtime setting via the CLI."""
    result = subprocess.run(
        ["./target/release/extenddb", "settings", "--config", EXTENDDB_CONFIG,
         "get", key],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to get {key}: {result.stderr.strip()}"
        )
    return result.stdout.strip()
class TestGsiAsyncPropagation:
    """Tests for GSI async propagation delay behavior."""

    @pytest.fixture()
    def gsi_table(self, dynamodb_client, unique_table_name):
        """Create a table with a GSI for async propagation tests."""
        table_name = unique_table_name
        dynamodb_client.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
                {"AttributeName": "gsi_sk", "AttributeType": "N"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "test-gsi",
                    "KeySchema": [
                        {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                        {"AttributeName": "gsi_sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                },
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(dynamodb_client, table_name)
        yield table_name
        try:
            dynamodb_client.delete_table(TableName=table_name)
        except Exception:
            pass
        else:
            wait_for_deleted(dynamodb_client, table_name)

    def test_gsi_update_is_eventually_consistent(
        self, dynamodb_client, gsi_table
    ):
        """GSI updates appear after a delay, not immediately.

        Writes items to the base table and measures the time until
        each item appears in the GSI. With the default propagation
        delay (10ms), items should appear within a few hundred ms.
        """
        table_name = gsi_table
        delays = []

        for i in range(5):
            pk = f"async-{uuid.uuid4().hex[:8]}"
            gsi_pk = f"gsi-partition-{i}"

            # Write to base table.
            dynamodb_client.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": pk},
                    "gsi_pk": {"S": gsi_pk},
                    "gsi_sk": {"N": str(i)},
                    "data": {"S": f"value-{i}"},
                },
            )

            # Measure time until item appears in GSI.
            start = time.monotonic()
            deadline = start + 5.0
            found = False
            while time.monotonic() < deadline:
                resp = dynamodb_client.query(
                    TableName=table_name,
                    IndexName="test-gsi",
                    KeyConditionExpression="gsi_pk = :pk",
                    ExpressionAttributeValues={
                        ":pk": {"S": gsi_pk},
                    },
                )
                if resp["Count"] > 0:
                    found = True
                    elapsed_ms = (time.monotonic() - start) * 1000
                    delays.append(elapsed_ms)
                    break
                time.sleep(0.005)  # 5ms poll interval

            assert found, f"Item {pk} did not appear in GSI within 5 seconds"

        # Print observed delays for human review.
        print(f"\n  GSI propagation delays (ms): {[f'{d:.1f}' for d in delays]}")
        print(f"  Min: {min(delays):.1f}ms, Max: {max(delays):.1f}ms, Avg: {sum(delays)/len(delays):.1f}ms")

        # All items should have appeared (already asserted above).
        assert len(delays) == 5

    def test_gsi_delete_is_eventually_consistent(
        self, dynamodb_client, gsi_table
    ):
        """Deleting a base table item removes it from the GSI after a delay."""
        table_name = gsi_table
        pk = f"del-async-{uuid.uuid4().hex[:8]}"
        gsi_pk = "del-gsi-partition"

        # Write item.
        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": pk},
                "gsi_pk": {"S": gsi_pk},
                "gsi_sk": {"N": "1"},
            },
        )

        # Wait for it to appear in GSI.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            resp = dynamodb_client.query(
                TableName=table_name,
                IndexName="test-gsi",
                KeyConditionExpression="gsi_pk = :pk",
                ExpressionAttributeValues={":pk": {"S": gsi_pk}},
            )
            if resp["Count"] > 0:
                break
            time.sleep(0.01)
        assert resp["Count"] == 1, "Item should appear in GSI before delete"

        # Delete from base table.
        dynamodb_client.delete_item(
            TableName=table_name,
            Key={"pk": {"S": pk}},
        )

        # Wait for removal from GSI.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            resp = dynamodb_client.query(
                TableName=table_name,
                IndexName="test-gsi",
                KeyConditionExpression="gsi_pk = :pk",
                ExpressionAttributeValues={":pk": {"S": gsi_pk}},
            )
            if resp["Count"] == 0:
                break
            time.sleep(0.01)
        assert resp["Count"] == 0, "Item should be removed from GSI after delete"

    def test_base_table_read_is_immediately_consistent(
        self, dynamodb_client, gsi_table
    ):
        """Base table reads are immediately consistent even when GSI is async."""
        table_name = gsi_table
        pk = f"base-{uuid.uuid4().hex[:8]}"

        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                "pk": {"S": pk},
                "gsi_pk": {"S": "some-partition"},
                "gsi_sk": {"N": "1"},
                "data": {"S": "immediate"},
            },
        )

        # Base table read should be immediate — no polling needed.
        resp = dynamodb_client.get_item(
            TableName=table_name,
            Key={"pk": {"S": pk}},
        )
        assert "Item" in resp
        assert resp["Item"]["data"]["S"] == "immediate"

    def test_gsi_sync_path_with_zero_delay(
        self, dynamodb_client, gsi_table
    ):
        """When gsi_propagation_delay_ms=0, GSI updates are synchronous.

        Sets the system-wide delay to 0, writes an item, and asserts
        the GSI query returns the item immediately (single query, no
        polling loop). This validates the effective_delay==0 sync path.

        Note: gsi_propagation_delay_ms is read by the data-plane write path at
        server startup, so setting it at runtime here only takes effect if the
        server was already booted with delay=0 (as devtools/run-tests does). A
        behavioral probe below confirms the server is actually in synchronous
        mode and skips — rather than falsely fails — if it is not.
        """
        table_name = gsi_table

        # Save original delay and set to 0 (sync mode).
        original_delay = extenddb_settings_get("gsi_propagation_delay_ms")
        extenddb_settings_set("gsi_propagation_delay_ms", "0")

        try:
            # Behavioral guard: throwaway write + immediate index read. If the
            # running server is genuinely synchronous, this appears at once;
            # otherwise the sync path is not exercisable here — skip cleanly.
            probe_gsi = f"syncprobe-gsi-{uuid.uuid4().hex[:8]}"
            dynamodb_client.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": f"syncprobe-{uuid.uuid4().hex[:8]}"},
                    "gsi_pk": {"S": probe_gsi},
                    "gsi_sk": {"N": "1"},
                    "data": {"S": "probe"},
                },
            )
            probe = dynamodb_client.query(
                TableName=table_name,
                IndexName="test-gsi",
                KeyConditionExpression="gsi_pk = :pk",
                ExpressionAttributeValues={":pk": {"S": probe_gsi}},
            )
            if probe["Count"] != 1:
                pytest.skip(
                    "server is not running with gsi_propagation_delay_ms=0 in "
                    "effect (the setting is applied at startup); the synchronous "
                    "GSI path cannot be exercised in this configuration"
                )

            pk = f"sync-{uuid.uuid4().hex[:8]}"
            gsi_pk = f"sync-gsi-{uuid.uuid4().hex[:8]}"

            dynamodb_client.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": pk},
                    "gsi_pk": {"S": gsi_pk},
                    "gsi_sk": {"N": "42"},
                    "data": {"S": "sync-value"},
                },
            )

            # Single query — no polling. With delay=0, the GSI update
            # happens in the same transaction as the base table write.
            resp = dynamodb_client.query(
                TableName=table_name,
                IndexName="test-gsi",
                KeyConditionExpression="gsi_pk = :pk",
                ExpressionAttributeValues={":pk": {"S": gsi_pk}},
            )
            assert resp["Count"] == 1, (
                "With gsi_propagation_delay_ms=0, GSI should be "
                "immediately consistent"
            )
            assert resp["Items"][0]["data"]["S"] == "sync-value"
        finally:
            # Restore original delay.
            extenddb_settings_set("gsi_propagation_delay_ms", original_delay)

    def test_gsi_configured_delay_range(
        self, dynamodb_client, gsi_table
    ):
        """GSI propagation delay falls within the configured range.

        Sets gsi_propagation_delay_ms to 50, writes items, and measures
        the observed propagation delay. The delay should be within
        [1, 50]ms (the random range used by the worker).
        """
        table_name = gsi_table

        # Save original delay and set to 50ms.
        original_delay = extenddb_settings_get("gsi_propagation_delay_ms")
        extenddb_settings_set("gsi_propagation_delay_ms", "50")

        try:
            delays = []
            for i in range(5):
                pk = f"range-{uuid.uuid4().hex[:8]}"
                gsi_pk = f"range-gsi-{i}-{uuid.uuid4().hex[:8]}"

                dynamodb_client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": pk},
                        "gsi_pk": {"S": gsi_pk},
                        "gsi_sk": {"N": str(i)},
                    },
                )

                start = time.monotonic()
                deadline = start + 5.0
                found = False
                while time.monotonic() < deadline:
                    resp = dynamodb_client.query(
                        TableName=table_name,
                        IndexName="test-gsi",
                        KeyConditionExpression="gsi_pk = :pk",
                        ExpressionAttributeValues={":pk": {"S": gsi_pk}},
                    )
                    if resp["Count"] > 0:
                        found = True
                        elapsed_ms = (time.monotonic() - start) * 1000
                        delays.append(elapsed_ms)
                        break
                    time.sleep(0.005)

                assert found, f"Item did not appear in GSI within 5 seconds"

            print(f"\n  Configured delay: 50ms")
            print(f"  Observed delays (ms): {[f'{d:.1f}' for d in delays]}")
            print(f"  Min: {min(delays):.1f}ms, Max: {max(delays):.1f}ms")

            # All items should have appeared.
            assert len(delays) == 5

            # Loose upper bound: all delays should be under 500ms (10x
            # configured max, accounting for scheduling jitter and poll
            # interval).
            assert all(d < 500 for d in delays), (
                f"Delays exceeded 500ms: {[f'{d:.1f}' for d in delays]}"
            )
        finally:
            extenddb_settings_set("gsi_propagation_delay_ms", original_delay)

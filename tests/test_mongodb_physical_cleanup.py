# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""MongoDB-specific checks for physical collection lifecycle cleanup.

The lifecycle operation is always exercised through the DynamoDB API. The
follow-up assertion uses ``mongosh`` outside ExtendDB to inspect the MongoDB
data database directly. The MongoDB runner supplies the container name; the
tests are skipped for PostgreSQL and real-DynamoDB runs.
"""

from __future__ import annotations

import json
import os
import subprocess
import time

import pytest
import requests

from conftest import wait_for_active, wait_for_deleted

GSI_BACKFILL_TEST_GATE = "gsi_backfill_test_gate"


@pytest.fixture()
def mongodb_container() -> str:
    container = os.environ.get("EXTENDDB_TEST_MONGODB_CONTAINER", "").strip()
    if not container:
        pytest.skip("requires devtools/run-mongodb-tests")
    return container


def _mongo_eval(container: str, javascript: str) -> dict:
    result = subprocess.run(
        ["docker", "exec", container, "mongosh", "--quiet", "--eval", javascript],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(f"mongosh inspection failed: {result.stderr.strip()}")
    output = result.stdout.strip()
    if not output:
        pytest.fail("mongosh inspection returned no result")
    try:
        return json.loads(output.splitlines()[-1])
    except json.JSONDecodeError as exc:
        pytest.fail(f"mongosh inspection returned invalid JSON: {output!r}: {exc}")


def _physical_ids(container: str, table_name: str, index_name: str) -> dict:
    table_literal = json.dumps(table_name)
    index_literal = json.dumps(index_name)
    javascript = f"""
const catalog = db.getSiblingDB("extenddb_catalog");
const data = db.getSiblingDB("extenddb_data");
const table = catalog.tables.findOne({{"_id.table_name": {table_literal}}});
const index = table && catalog.indexes.findOne({{
  "_id.table_id": table.table_id,
  "_id.index_name": {index_literal}
}});
if (!table || !index) {{
  print(JSON.stringify({{error: "catalog entry not found"}}));
}} else {{
  print(JSON.stringify({{
    tableId: table.table_id,
    indexId: index.index_id,
    tableCollectionExists: data.getCollectionNames().includes("_ddb_" + table.table_id),
    indexCollectionExists: data.getCollectionNames().includes("_ddb_" + index.index_id)
  }}));
}}
"""
    result = _mongo_eval(container, javascript)
    if "error" in result:
        pytest.fail(f"could not find MongoDB catalog entries: {result['error']}")
    return result


def _physical_collections_exist(container: str, *ids: str) -> dict[str, bool]:
    ids_literal = json.dumps([f"_ddb_{item_id}" for item_id in ids])
    javascript = f"""
const data = db.getSiblingDB("extenddb_data");
const names = data.getCollectionNames();
const requested = {ids_literal};
const result = {{}};
for (const name of requested) {{
  result[name] = names.includes(name);
}}
print(JSON.stringify(result));
"""
    return _mongo_eval(container, javascript)


def _backfill_gate_url(table_name: str) -> str:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    if not endpoint:
        pytest.skip("requires EXTENDDB_TEST_ENDPOINT")
    gate_key = f"{GSI_BACKFILL_TEST_GATE}:{table_name}"
    return f"{endpoint.rstrip('/')}/management/settings/{gate_key}"


def _set_backfill_gate(table_name: str, value: str) -> None:
    user = os.environ.get("EXTENDDB_ADMIN_USER", "admin")
    password = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not password:
        pytest.fail("EXTENDDB_ADMIN_PASSWORD is required for the backfill gate")
    response = requests.put(
        _backfill_gate_url(table_name),
        auth=(user, password),
        json={"value": value},
        timeout=30,
        verify=False,
    )
    if not response.ok:
        pytest.fail(f"setting GSI backfill gate failed: {response.status_code}: {response.text}")


def _wait_for_backfill_gate(table_name: str, value: str, timeout: float = 30.0) -> bool:
    user = os.environ.get("EXTENDDB_ADMIN_USER", "admin")
    password = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = requests.get(
            _backfill_gate_url(table_name),
            auth=(user, password),
            timeout=30,
            verify=False,
        )
        if response.ok and response.json().get("value") == value:
            return True
        time.sleep(0.1)
    return False


def _wait_for_physical_collection_absent(
    container: str, index_id: str, timeout: float = 30.0
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        present = _physical_collections_exist(container, index_id)
        if not present[f"_ddb_{index_id}"]:
            return
        time.sleep(0.1)
    pytest.fail(f"physical index collection _ddb_{index_id} was not removed")


def _create_gsi_table(client, table_name: str) -> None:
    client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            }
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )


def _create_hash_only_table(client, table_name: str) -> None:
    client.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )


def _cleanup_table(client, table_name: str) -> None:
    try:
        client.delete_table(TableName=table_name)
    except client.exceptions.ResourceNotFoundException:
        return
    wait_for_deleted(client, table_name)


def test_update_table_gsi_delete_drops_physical_collection(
    dynamodb_client, unique_table_name, mongodb_container
):
    """Delete a GSI through UpdateTable and verify its Mongo collection is gone."""
    _create_gsi_table(dynamodb_client, unique_table_name)
    wait_for_active(dynamodb_client, unique_table_name)

    try:
        physical = _physical_ids(mongodb_container, unique_table_name, "gsi1")
        assert physical["tableCollectionExists"]
        assert physical["indexCollectionExists"]

        dynamodb_client.update_table(
            TableName=unique_table_name,
            GlobalSecondaryIndexUpdates=[{"Delete": {"IndexName": "gsi1"}}],
        )

        remaining = _physical_collections_exist(mongodb_container, physical["indexId"])
        assert remaining[f"_ddb_{physical['indexId']}"] is False
    finally:
        _cleanup_table(dynamodb_client, unique_table_name)


def test_deleting_gsi_during_backfill_drops_orphaned_collection(
    dynamodb_client, unique_table_name, mongodb_container
):
    """Deleting a backfilling GSI must not leave its physical collection behind."""
    if os.environ.get("EXTENDDB_TEST_MONGODB_TEST_HOOKS") != "1":
        pytest.skip("requires the MongoDB test-hook build")

    _create_hash_only_table(dynamodb_client, unique_table_name)
    wait_for_active(dynamodb_client, unique_table_name)
    gate_armed = False

    try:
        dynamodb_client.put_item(
            TableName=unique_table_name,
            Item={"pk": {"S": "item-1"}, "gsi_pk": {"S": "value-1"}},
        )
        _set_backfill_gate(unique_table_name, "armed")
        gate_armed = True

        dynamodb_client.update_table(
            TableName=unique_table_name,
            AttributeDefinitions=[
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": "gsi1",
                        "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    }
                }
            ],
        )

        assert _wait_for_backfill_gate(unique_table_name, "paused"), (
            "backfill did not reach its deterministic pause"
        )
        physical = _physical_ids(mongodb_container, unique_table_name, "gsi1")
        assert physical["indexCollectionExists"]

        dynamodb_client.update_table(
            TableName=unique_table_name,
            GlobalSecondaryIndexUpdates=[{"Delete": {"IndexName": "gsi1"}}],
        )

        # Releasing the worker lets it finish the batch. Its catalog cursor
        # update must observe that the index document was deleted, then remove
        # the collection that the batch upsert may have recreated.
        _set_backfill_gate(unique_table_name, "release")
        gate_armed = False

        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            description = dynamodb_client.describe_table(TableName=unique_table_name)
            if not description["Table"].get("GlobalSecondaryIndexes"):
                break
            time.sleep(0.1)
        else:
            pytest.fail("deleted Global Secondary Index remained in the table description")

        _wait_for_physical_collection_absent(mongodb_container, physical["indexId"])
    finally:
        if gate_armed:
            _set_backfill_gate(unique_table_name, "release")
        _cleanup_table(dynamodb_client, unique_table_name)


def test_delete_table_drops_physical_table_and_index_collections(
    dynamodb_client, unique_table_name, mongodb_container
):
    """Delete a table through DeleteTable and verify all Mongo collections are gone."""
    _create_gsi_table(dynamodb_client, unique_table_name)
    wait_for_active(dynamodb_client, unique_table_name)

    physical = _physical_ids(mongodb_container, unique_table_name, "gsi1")
    assert physical["tableCollectionExists"]
    assert physical["indexCollectionExists"]

    dynamodb_client.delete_table(TableName=unique_table_name)
    wait_for_deleted(dynamodb_client, unique_table_name)

    remaining = _physical_collections_exist(
        mongodb_container, physical["tableId"], physical["indexId"]
    )
    assert remaining[f"_ddb_{physical['tableId']}"] is False
    assert remaining[f"_ddb_{physical['indexId']}"] is False

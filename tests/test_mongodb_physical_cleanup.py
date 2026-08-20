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

import pytest

from conftest import wait_for_active, wait_for_deleted


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

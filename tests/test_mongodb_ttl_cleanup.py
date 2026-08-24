# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""MongoDB-specific checks for TTL index lifecycle cleanup."""

from __future__ import annotations

import json
import os
import subprocess

import pytest

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


def _ttl_index_state(container: str, table_name: str, index_name: str) -> dict:
    table_literal = json.dumps(table_name)
    index_literal = json.dumps(index_name)
    javascript = f"""
const catalog = db.getSiblingDB("extenddb_catalog");
const data = db.getSiblingDB("extenddb_data");
const table = catalog.tables.findOne({{"_id.table_name": {table_literal}}});
if (!table) {{
  print(JSON.stringify({{error: "catalog table not found"}}));
}} else {{
  const collection = data.getCollection("_ddb_" + table.table_id);
  const indexNames = collection.getIndexes().map(index => index.name);
  print(JSON.stringify({{
    ttlIndexReady: table.ttl_index_ready === true,
    indexExists: indexNames.includes({index_literal})
  }}));
}}
"""
    result = _mongo_eval(container, javascript)
    if "error" in result:
        pytest.fail(f"could not find MongoDB catalog entries: {result['error']}")
    return result


def test_disable_ttl_drops_physical_index(
    dynamodb_client, create_and_cleanup_table, mongodb_container
):
    """Disabling TTL through DynamoDB must remove its MongoDB index."""
    table_name = create_and_cleanup_table()["TableDescription"]["TableName"]
    dynamodb_client.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )

    before = _ttl_index_state(mongodb_container, table_name, "idx_ttl_expires_at")
    assert before["ttlIndexReady"] is True
    assert before["indexExists"] is True

    dynamodb_client.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={"Enabled": False, "AttributeName": "expires_at"},
    )

    after = _ttl_index_state(mongodb_container, table_name, "idx_ttl_expires_at")
    assert after["ttlIndexReady"] is False
    assert after["indexExists"] is False


def test_dotted_ttl_does_not_create_physical_index(
    dynamodb_client, create_and_cleanup_table, mongodb_container
):
    """Dotted TTL attributes use the expression sweep without a Mongo index."""
    table_name = create_and_cleanup_table()["TableDescription"]["TableName"]
    index_name = "idx_ttl_expires.at"

    dynamodb_client.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires.at"},
    )

    enabled = _ttl_index_state(mongodb_container, table_name, index_name)
    assert enabled["ttlIndexReady"] is True
    assert enabled["indexExists"] is False

    dynamodb_client.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={"Enabled": False, "AttributeName": "expires.at"},
    )

    disabled = _ttl_index_state(mongodb_container, table_name, index_name)
    assert disabled["ttlIndexReady"] is False
    assert disabled["indexExists"] is False

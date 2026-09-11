# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""MongoDB-specific check: a committed write whose commit outcome is
reported as unknown must be applied exactly once.

MongoDB signals a lost commitTransaction reply with the
UnknownTransactionCommitResult error label. The commit may already have
applied, so the server must retry the commit itself (idempotent) and must
not re-run the transaction body; re-running the body duplicates a
non-idempotent write such as ADD. The window is a network fault in the
instant between sending the commit and reading its reply, which a test
cannot hit on purpose, so a test-hook gate makes the next UpdateItem
commit on an armed table succeed and then report the unknown label once.

Requires a server built with mongodb-test-hooks; the tests self-skip when
not run under devtools/run-mongodb-tests.
"""

from __future__ import annotations

import os

import pytest
import requests

from conftest import wait_for_active, wait_for_deleted

UNKNOWN_COMMIT_TEST_GATE = "unknown_commit_test_gate"


@pytest.fixture()
def mongodb_container() -> str:
    container = os.environ.get("EXTENDDB_TEST_MONGODB_CONTAINER", "").strip()
    if not container:
        pytest.skip("requires devtools/run-mongodb-tests")
    return container


def _gate_url(table_name: str) -> str:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    if not endpoint:
        pytest.skip("requires EXTENDDB_TEST_ENDPOINT")
    gate_key = f"{UNKNOWN_COMMIT_TEST_GATE}:{table_name}"
    return f"{endpoint.rstrip('/')}/management/settings/{gate_key}"


def _admin_auth() -> tuple[str, str]:
    user = os.environ.get("EXTENDDB_ADMIN_USER", "admin")
    password = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not password:
        pytest.fail("EXTENDDB_ADMIN_PASSWORD is required for the commit gate")
    return user, password


def _set_gate(table_name: str, value: str) -> None:
    response = requests.put(
        _gate_url(table_name),
        auth=_admin_auth(),
        json={"value": value},
        timeout=30,
        verify=False,
    )
    if not response.ok:
        pytest.fail(
            f"setting unknown-commit gate failed: {response.status_code}: {response.text}"
        )


def _get_gate(table_name: str) -> str:
    response = requests.get(
        _gate_url(table_name),
        auth=_admin_auth(),
        timeout=30,
        verify=False,
    )
    if not response.ok:
        pytest.fail(
            f"reading unknown-commit gate failed: {response.status_code}: {response.text}"
        )
    return response.json().get("value", "")


def _cleanup_table(client, table_name: str) -> None:
    try:
        client.delete_table(TableName=table_name)
    except client.exceptions.ResourceNotFoundException:
        return
    wait_for_deleted(client, table_name)


def test_update_item_applied_once_when_commit_outcome_unknown(
    dynamodb_client, unique_table_name, mongodb_container
):
    """One successful ADD must increment by exactly one, even when the
    first commit's outcome is reported as unknown after it applied."""
    dynamodb_client.create_table(
        TableName=unique_table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, unique_table_name)

    try:
        _set_gate(unique_table_name, "armed")

        # ReturnValues forces the transactional path; the no-transaction
        # fast path for unconditional updates never reaches a commit.
        response = dynamodb_client.update_item(
            TableName=unique_table_name,
            Key={"pk": {"S": "counter"}},
            UpdateExpression="ADD #c :one",
            ExpressionAttributeNames={"#c": "count"},
            ExpressionAttributeValues={":one": {"N": "1"}},
            ReturnValues="ALL_OLD",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        # The gate must have been consumed, otherwise the update never
        # took the guarded commit path and the test proves nothing.
        assert _get_gate(unique_table_name) == "idle"

        item = dynamodb_client.get_item(
            TableName=unique_table_name,
            Key={"pk": {"S": "counter"}},
            ConsistentRead=True,
        )["Item"]
        assert item["count"]["N"] == "1", (
            f"counter is {item['count']['N']}, the increment was applied "
            "more than once after a commit whose outcome was reported unknown"
        )
    finally:
        _cleanup_table(dynamodb_client, unique_table_name)

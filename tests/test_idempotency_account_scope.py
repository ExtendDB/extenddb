# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Idempotency token (ClientRequestToken) scoping for TransactWriteItems.

Amazon DynamoDB scopes a ClientRequestToken's uniqueness per account (and
region), not globally. Two behaviors follow:

  * Single account: replaying the same token with an identical request is
    idempotent (applied at most once), and reusing the same token with a
    different request is rejected with IdempotentParameterMismatchException.
  * Two accounts: the same token value used by different accounts must not
    collide. One account's token can neither suppress (idempotent replay)
    nor reject (parameter mismatch) another account's transaction.

The single-account cases run against both Amazon DynamoDB and ExtendDB. The
cross-account isolation case needs two accounts provisioned through the
management API, so it runs against ExtendDB only (a single AWS profile is one
account and cannot exercise a cross-account collision).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted
from management_helpers import ManagementClient


# ---------------------------------------------------------------------------
# Single-account behavior (dual-target: Amazon DynamoDB + ExtendDB)
# ---------------------------------------------------------------------------


def _simple_table(client, table_name: str) -> None:
    client.create_table(
        TableName=table_name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(client, table_name)


def _add_one(client, table_name: str, token: str, key: str = "k", amount: int = 1):
    """TransactWriteItems that increments a counter by ``amount`` (non-idempotent
    op, so a wrongly-replayed transaction is observable as a double count)."""
    return client.transact_write_items(
        ClientRequestToken=token,
        TransactItems=[
            {
                "Update": {
                    "TableName": table_name,
                    "Key": {"pk": {"S": key}},
                    "UpdateExpression": "ADD #c :n",
                    "ExpressionAttributeNames": {"#c": "count"},
                    "ExpressionAttributeValues": {":n": {"N": str(amount)}},
                }
            }
        ],
    )


def _count(client, table_name: str, key: str = "k") -> int:
    item = client.get_item(
        TableName=table_name, Key={"pk": {"S": key}}, ConsistentRead=True
    ).get("Item")
    return int(item["count"]["N"]) if item and "count" in item else 0


def test_replay_same_token_same_payload_is_idempotent(dynamodb_client, unique_table_name):
    """Same token + identical request applies exactly once (no double count)."""
    _simple_table(dynamodb_client, unique_table_name)
    try:
        token = f"tok-{uuid.uuid4().hex[:12]}"
        _add_one(dynamodb_client, unique_table_name, token)
        assert _count(dynamodb_client, unique_table_name) == 1
        # Replaying the exact same request is a no-op that returns success.
        _add_one(dynamodb_client, unique_table_name, token)
        assert _count(dynamodb_client, unique_table_name) == 1
    finally:
        dynamodb_client.delete_table(TableName=unique_table_name)
        wait_for_deleted(dynamodb_client, unique_table_name)


def test_same_token_different_payload_is_rejected(dynamodb_client, unique_table_name):
    """Reusing a token with a different request is rejected as a mismatch."""
    _simple_table(dynamodb_client, unique_table_name)
    try:
        token = f"tok-{uuid.uuid4().hex[:12]}"
        _add_one(dynamodb_client, unique_table_name, token, amount=1)
        with pytest.raises(ClientError) as exc:
            _add_one(dynamodb_client, unique_table_name, token, amount=5)
        assert (
            exc.value.response["Error"]["Code"]
            == "IdempotentParameterMismatchException"
        )
        # The rejected transaction must not have applied.
        assert _count(dynamodb_client, unique_table_name) == 1
    finally:
        dynamodb_client.delete_table(TableName=unique_table_name)
        wait_for_deleted(dynamodb_client, unique_table_name)


def test_different_token_writes_independently(dynamodb_client, unique_table_name):
    """A distinct token applies its own transaction (token-scoped dedup)."""
    _simple_table(dynamodb_client, unique_table_name)
    try:
        _add_one(dynamodb_client, unique_table_name, f"tok-{uuid.uuid4().hex[:12]}")
        _add_one(dynamodb_client, unique_table_name, f"tok-{uuid.uuid4().hex[:12]}")
        assert _count(dynamodb_client, unique_table_name) == 2
    finally:
        dynamodb_client.delete_table(TableName=unique_table_name)
        wait_for_deleted(dynamodb_client, unique_table_name)


# ---------------------------------------------------------------------------
# Cross-account isolation (ExtendDB only: needs two accounts via management API)
# ---------------------------------------------------------------------------


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint:
        pytest.skip("cross-account isolation runs against ExtendDB only")
    if not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: cross-account tests require EXTENDDB_ADMIN_USER and "
            "EXTENDDB_ADMIN_PASSWORD (set by devtools/run-tests)."
        )
    return endpoint, admin_user, admin_pass


def _make_client(endpoint_url: str, access_key: str, secret_key: str, region: str) -> Any:
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _full_access_policy() -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
    }


@pytest.fixture()
def two_account_clients():
    """Provision two independent accounts, each with a full-access user.

    Yields ``(client_a, client_b)`` bound to distinct accounts. Both accounts
    create an identically-named table so that a shared ClientRequestToken with
    an identical payload produces an identical server-side fingerprint, which
    is exactly the condition that made a global token keyspace collide.
    """
    endpoint, admin_user, admin_pass = _require_auth_env()
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    mgmt = ManagementClient(endpoint, admin_user, admin_pass)

    made: list[tuple[str, str]] = []

    def provision(user: str, password: str):
        acct_id = f"{uuid.uuid4().int % 10**12:012d}"
        assert mgmt.create_account(acct_id, f"{user}-{acct_id}").status_code == 201
        assert mgmt.create_user(acct_id, user, password).status_code == 201
        made.append((acct_id, user))
        resp = mgmt.create_access_key(acct_id, user)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        assert mgmt.put_user_policy(acct_id, user, "full", _full_access_policy()).status_code == 204
        return _make_client(endpoint, creds["access_key_id"], creds["secret_access_key"], region)

    client_a = provision("acct-a", "AcctAPass123!")
    client_b = provision("acct-b", "AcctBPass456!")
    try:
        yield client_a, client_b
    finally:
        for acct_id, user in made:
            try:
                mgmt.delete_user(acct_id, user)
                mgmt.delete_account(acct_id)
            except Exception:
                pass


def _put(client, table_name: str, token: str, value: str):
    return client.transact_write_items(
        ClientRequestToken=token,
        TransactItems=[
            {"Put": {"TableName": table_name, "Item": {"pk": {"S": "k"}, "v": {"S": value}}}}
        ],
    )


def test_cross_account_same_token_same_payload_not_deduped(two_account_clients):
    """A token used by account A must not suppress account B's identical write.

    Both accounts use the same table name and the same payload, so the token
    and the request fingerprint match exactly. That is the mode-1 collision: a
    global token keyspace treats B's request as an idempotent replay of A's and
    returns success without applying it, silently dropping B's write. Account
    scoping must let B's write land in B's own table.
    """
    client_a, client_b = two_account_clients
    table = f"idem-{uuid.uuid4().hex[:8]}"
    token = f"tok-{uuid.uuid4().hex[:12]}"
    _simple_table(client_a, table)
    _simple_table(client_b, table)
    try:
        # Identical value under both accounts -> identical fingerprint.
        _put(client_a, table, token, "shared")
        _put(client_b, table, token, "shared")
        # B's write must have landed in B's own table (not dropped as a replay).
        b_item = client_b.get_item(
            TableName=table, Key={"pk": {"S": "k"}}, ConsistentRead=True
        ).get("Item")
        assert b_item is not None, (
            "account B's identical-payload write was dropped as a cross-account replay"
        )
        assert b_item["v"]["S"] == "shared"
        # A's data is present and unchanged.
        a_item = client_a.get_item(
            TableName=table, Key={"pk": {"S": "k"}}, ConsistentRead=True
        )["Item"]
        assert a_item["v"]["S"] == "shared"
    finally:
        for c in (client_a, client_b):
            try:
                c.delete_table(TableName=table)
                wait_for_deleted(c, table)
            except Exception:
                pass


def test_cross_account_same_token_different_payload_not_rejected(two_account_clients):
    """A token used by account A must not reject account B's different write.

    A global token keyspace would raise IdempotentParameterMismatchException
    for B because A already stored the token with a different fingerprint.
    """
    client_a, client_b = two_account_clients
    table = f"idem-{uuid.uuid4().hex[:8]}"
    token = f"tok-{uuid.uuid4().hex[:12]}"
    _simple_table(client_a, table)
    _simple_table(client_b, table)
    try:
        _put(client_a, table, token, "payload-a")
        # Different payload under the same token, but a different account.
        _put(client_b, table, token, "payload-b")
        b_item = client_b.get_item(
            TableName=table, Key={"pk": {"S": "k"}}, ConsistentRead=True
        ).get("Item")
        assert b_item is not None and b_item["v"]["S"] == "payload-b"
    finally:
        for c in (client_a, client_b):
            try:
                c.delete_table(TableName=table)
                wait_for_deleted(c, table)
            except Exception:
                pass

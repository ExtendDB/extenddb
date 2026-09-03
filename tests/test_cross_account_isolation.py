# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Cross-account isolation tests — verify data plane and console isolation.

Two accounts, two users (one per account), each with full DynamoDB access.
Verifies that tables, items, and console views are isolated between accounts.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars
  - `extenddb init` has been run (encryption key + admin user exist)

Run:
  EXTENDDB_TEST_ENDPOINT=http://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin \\
  EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_cross_account_isolation.py -v

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted
from management_helpers import ManagementClient
def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: Cross-account tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass
def _make_dynamodb_client(endpoint_url: str, access_key: str, secret_key: str,
                          region: str) -> Any:
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)
def _full_access_policy() -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "dynamodb:*",
            "Resource": "*",
        }],
    }


class TestAccountLifecycle:
    """Account deletion must respect table ownership dependencies."""

    def test_delete_account_rejects_existing_table(self, alice_env, mgmt):
        client, account_id = alice_env
        table_name = f"account-delete-{uuid.uuid4().hex[:8]}"
        client.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, table_name)

        try:
            response = mgmt.delete_account(account_id)
            assert response.status_code == 409, response.text
            assert "existing tables" in response.text.lower()
        finally:
            client.delete_table(TableName=table_name)
            wait_for_deleted(client, table_name)


@pytest.fixture(scope="module")
def auth_env():
    return _require_auth_env()
@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    endpoint, admin_user, admin_pass = auth_env
    return ManagementClient(endpoint, admin_user, admin_pass)
@pytest.fixture(scope="module")
def region() -> str:
    return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
@pytest.fixture(scope="module")
def alice_env(auth_env, mgmt, region):
    """Create account + user 'alice' with full DynamoDB access. Return (client, account_id)."""
    endpoint = auth_env[0]
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"alice-acct-{acct_id}")
    assert resp.status_code == 201, resp.text

    resp = mgmt.create_user(acct_id, "alice", "AlicePass123!")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(acct_id, "alice")
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    resp = mgmt.put_user_policy(acct_id, "alice", "full", _full_access_policy())
    assert resp.status_code == 204, resp.text

    client = _make_dynamodb_client(endpoint, creds["access_key_id"],
                                   creds["secret_access_key"], region)
    yield client, acct_id

    mgmt.delete_user(acct_id, "alice")
    mgmt.delete_account(acct_id)
@pytest.fixture(scope="module")
def bob_env(auth_env, mgmt, region):
    """Create account + user 'bob' with full DynamoDB access. Return (client, account_id)."""
    endpoint = auth_env[0]
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"bob-acct-{acct_id}")
    assert resp.status_code == 201, resp.text

    resp = mgmt.create_user(acct_id, "bob", "BobPass456!")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(acct_id, "bob")
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    resp = mgmt.put_user_policy(acct_id, "bob", "full", _full_access_policy())
    assert resp.status_code == 204, resp.text

    client = _make_dynamodb_client(endpoint, creds["access_key_id"],
                                   creds["secret_access_key"], region)
    yield client, acct_id

    mgmt.delete_user(acct_id, "bob")
    mgmt.delete_account(acct_id)
# ---------------------------------------------------------------------------
# Data Plane Isolation
# ---------------------------------------------------------------------------

class TestDataPlaneIsolation:
    """Tables and items are isolated between accounts."""

    @pytest.fixture(autouse=True)
    def setup(self, alice_env, bob_env):
        self.alice, self.alice_acct = alice_env
        self.bob, self.bob_acct = bob_env

    def test_alice_tables_invisible_to_bob(self):
        """Tables created by alice do not appear in bob's ListTables."""
        table = f"iso-{uuid.uuid4().hex[:8]}"
        try:
            self.alice.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.alice, table)

            bob_tables = self.bob.list_tables()["TableNames"]
            assert table not in bob_tables
        finally:
            try:
                self.alice.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(self.alice, table)

    def test_same_name_tables_coexist_independently(self):
        """Alice and bob can both create a table with the same name."""
        table = f"shared-{uuid.uuid4().hex[:8]}"
        try:
            self.alice.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.alice, table)

            self.bob.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.bob, table)

            # Both should see the table in their own listing.
            assert table in self.alice.list_tables()["TableNames"]
            assert table in self.bob.list_tables()["TableNames"]

            # Write different data to each.
            self.alice.put_item(
                TableName=table,
                Item={"pk": {"S": "key1"}, "owner": {"S": "alice"}},
            )
            self.bob.put_item(
                TableName=table,
                Item={"pk": {"S": "key1"}, "owner": {"S": "bob"}},
            )

            # Each sees their own data.
            alice_item = self.alice.get_item(
                TableName=table, Key={"pk": {"S": "key1"}}
            )["Item"]
            assert alice_item["owner"]["S"] == "alice"

            bob_item = self.bob.get_item(
                TableName=table, Key={"pk": {"S": "key1"}}
            )["Item"]
            assert bob_item["owner"]["S"] == "bob"
        finally:
            for client in (self.alice, self.bob):
                try:
                    client.delete_table(TableName=table)
                except Exception:
                    pass
                else:
                    wait_for_deleted(client, table)

    def test_alice_cannot_read_bob_items(self):
        """Alice cannot read items from bob's table (table doesn't exist in her namespace)."""
        table = f"bob-only-{uuid.uuid4().hex[:8]}"
        try:
            self.bob.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.bob, table)
            self.bob.put_item(
                TableName=table,
                Item={"pk": {"S": "secret"}, "data": {"S": "bob-private"}},
            )

            # Alice tries to read from the same table name — should fail
            # because the table doesn't exist in her account.
            with pytest.raises(ClientError) as exc_info:
                self.alice.get_item(
                    TableName=table, Key={"pk": {"S": "secret"}}
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            try:
                self.bob.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(self.bob, table)

    def test_alice_cannot_write_to_bob_table(self):
        """Alice cannot write to a table that only exists in bob's account."""
        table = f"bob-write-{uuid.uuid4().hex[:8]}"
        try:
            self.bob.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.bob, table)

            with pytest.raises(ClientError) as exc_info:
                self.alice.put_item(
                    TableName=table,
                    Item={"pk": {"S": "intruder"}, "data": {"S": "hacked"}},
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            try:
                self.bob.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(self.bob, table)

    def test_delete_in_one_account_does_not_affect_other(self):
        """Deleting a same-name table in alice's account doesn't affect bob's."""
        table = f"del-iso-{uuid.uuid4().hex[:8]}"
        try:
            for client in (self.alice, self.bob):
                client.create_table(
                    TableName=table,
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    BillingMode="PAY_PER_REQUEST",
                )
                wait_for_active(client, table)

            self.bob.put_item(
                TableName=table,
                Item={"pk": {"S": "k1"}, "v": {"S": "bob-data"}},
            )

            # Alice deletes her copy.
            self.alice.delete_table(TableName=table)
            wait_for_deleted(self.alice, table)

            # Bob's table and data should be unaffected.
            assert table in self.bob.list_tables()["TableNames"]
            item = self.bob.get_item(
                TableName=table, Key={"pk": {"S": "k1"}}
            )["Item"]
            assert item["v"]["S"] == "bob-data"
        finally:
            try:
                self.bob.delete_table(TableName=table)
            except Exception:
                pass
            else:
                wait_for_deleted(self.bob, table)
# ---------------------------------------------------------------------------
# Console Isolation
# ---------------------------------------------------------------------------

class TestConsoleIsolation:
    """IAM users in different accounts see only their own account's entities."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, alice_env, bob_env):
        self.endpoint = auth_env[0]
        self.alice_acct = alice_env[1]
        self.bob_acct = bob_env[1]

    def _console_login(self, account_id: str, user_name: str,
                       password: str) -> requests.Session:
        """Login to console as IAM user, return session with cookie."""
        session = requests.Session()
        # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
        if self.endpoint.startswith("https://"):
            session.verify = False
        resp = session.post(
            f"{self.endpoint}/console/login",
            data={"username": f"{account_id}/{user_name}", "password": password},
            allow_redirects=False,
            timeout=30,
        )
        assert resp.status_code == 303, f"Console login failed: {resp.status_code}"
        return session

    def test_alice_console_does_not_show_bob_account(self):
        """Alice's console session does not list bob's account."""
        session = self._console_login(self.alice_acct, "alice", "AlicePass123!")
        resp = session.get(
            f"{self.endpoint}/console/accounts",
            allow_redirects=False,
            timeout=30,
        )
        assert resp.status_code == 200
        assert self.alice_acct in resp.text
        assert self.bob_acct not in resp.text

    def test_bob_console_does_not_show_alice_account(self):
        """Bob's console session does not list alice's account."""
        session = self._console_login(self.bob_acct, "bob", "BobPass456!")
        resp = session.get(
            f"{self.endpoint}/console/accounts",
            allow_redirects=False,
            timeout=30,
        )
        assert resp.status_code == 200
        assert self.bob_acct in resp.text
        assert self.alice_acct not in resp.text


# ---------------------------------------------------------------------------
# Stream record account scoping
# ---------------------------------------------------------------------------
import base64
import time


def _make_streams_client(endpoint_url: str, access_key: str, secret_key: str,
                         region: str) -> Any:
    kwargs: dict = dict(
        service_name="dynamodbstreams",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _stream_arn(ddb: Any, table: str) -> str:
    return ddb.describe_table(TableName=table)["Table"]["LatestStreamArn"]


@pytest.fixture(scope="module")
def stream_accounts(auth_env, mgmt, region):
    """Two accounts, each with a DynamoDB and a DynamoDB Streams client."""
    endpoint = auth_env[0]
    accts: dict[str, dict] = {}
    for name, passwd in (("accta", "AcctaPass123!"), ("acctb", "AcctbPass456!")):
        acct_id = f"{uuid.uuid4().int % 10**12:012d}"
        assert mgmt.create_account(acct_id, f"{name}-acct-{acct_id}").status_code == 201
        assert mgmt.create_user(acct_id, name, passwd).status_code == 201
        creds = mgmt.create_access_key(acct_id, name).json()
        assert mgmt.put_user_policy(acct_id, name, "full",
                                    _full_access_policy()).status_code == 204
        ak, sk = creds["access_key_id"], creds["secret_access_key"]
        accts[name] = {
            "acct": acct_id,
            "ddb": _make_dynamodb_client(endpoint, ak, sk, region),
            "streams": _make_streams_client(endpoint, ak, sk, region),
        }
    yield accts
    for name in ("accta", "acctb"):
        mgmt.delete_user(accts[name]["acct"], name)
        mgmt.delete_account(accts[name]["acct"])


class TestStreamAccountScoping:
    """GetRecords returns only the records of the shard's owning account."""

    def test_shard_iterator_only_returns_owning_account_records(self, stream_accounts):
        owner = stream_accounts["accta"]
        other = stream_accounts["acctb"]
        table = f"accta-stream-{uuid.uuid4().hex[:8]}"

        owner["ddb"].create_table(
            TableName=table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={"StreamEnabled": True,
                                 "StreamViewType": "NEW_AND_OLD_IMAGES"},
        )
        wait_for_active(owner["ddb"], table)
        owner["ddb"].put_item(
            TableName=table,
            Item={"pk": {"S": "item1"}, "data": {"S": "owner-value"}},
        )

        arn = _stream_arn(owner["ddb"], table)
        shards = owner["streams"].describe_stream(StreamArn=arn)[
            "StreamDescription"]["Shards"]

        def drain(client: Any, shard_id: str) -> list:
            it = client.get_shard_iterator(
                StreamArn=arn, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON",
            )["ShardIterator"]
            recs: list = []
            for _ in range(10):
                resp = client.get_records(ShardIterator=it, Limit=100)
                recs.extend(resp["Records"])
                it = resp.get("NextShardIterator")
                if recs or not it:
                    break
                time.sleep(0.5)
            return recs

        # Positive control: the owner finds its record in whichever shard holds
        # it (the item's key hashes to one of the stream's shards).
        drained = {sh["ShardId"]: drain(owner["streams"], sh["ShardId"]) for sh in shards}
        target_shard = next(
            (
                sid
                for sid, recs in drained.items()
                if any(
                    r["dynamodb"].get("NewImage", {}).get("data", {}).get("S")
                    == "owner-value"
                    for r in recs
                )
            ),
            None,
        )
        assert target_shard is not None, (
            "owner should be able to read its own stream records; drained counts="
            + str({sid: len(recs) for sid, recs in drained.items()})
            + " sample="
            + str(drained)[:600]
        )

        # Construct a shard-iterator token for the shard holding account A's
        # record and call GetRecords with account B's own credentials. The
        # token is "<shard_id>|AFTER_SEQUENCE_NUMBER|<seq>|<ts>" in plain base64.
        other_iterator = base64.b64encode(
            f"{target_shard}|AFTER_SEQUENCE_NUMBER||{int(time.time())}".encode()
        ).decode()
        # Real DynamoDB rejects a GetRecords iterator it did not issue with
        # ValidationException("Invalid ShardIterator") -- and does not
        # distinguish "exists but not yours" from "does not exist", so the
        # response cannot be used to probe another account's shard existence.
        with pytest.raises(ClientError) as exc:
            other["streams"].get_records(ShardIterator=other_iterator, Limit=100)
        err = exc.value.response["Error"]
        assert err["Code"] == "ValidationException", (
            "a shard iterator for another account's shard must be rejected as "
            f"ValidationException, got {err}"
        )
        assert err["Message"] == "Invalid ShardIterator", (
            "expected DynamoDB-verbatim 'Invalid ShardIterator', got "
            f"{err['Message']!r}"
        )

    def test_metrics_endpoint_requires_admin_auth(self, auth_env):
        endpoint, admin_user, admin_pass = auth_env

        # An unauthenticated request is rejected outright. Deterministic --
        # no dependency on the async metrics flush.
        r = requests.get(f"{endpoint}/metrics", verify=False, timeout=10)
        assert r.status_code == 401, (
            f"unauthenticated /metrics returned {r.status_code}, "
            "expected 401 (must require admin auth)"
        )

        # With admin Basic auth the endpoint is reachable and returns a metrics
        # document. (Per-table dimension *retention* is covered deterministically
        # by the aggregate_rows Rust unit test, not by polling the 60s flush.)
        r = requests.get(
            f"{endpoint}/metrics",
            auth=(admin_user, admin_pass),
            verify=False,
            timeout=10,
        )
        assert r.status_code == 200, (
            f"admin /metrics returned {r.status_code}, expected 200"
        )
        body = r.json()
        assert isinstance(body, dict) and ("metrics" in body or "buckets" in body), (
            "admin /metrics should return a MetricsResponse JSON body"
        )


    def test_console_metrics_data_requires_session(self, auth_env):
        endpoint, admin_user, admin_pass = auth_env

        # 1. Without a console session, the data route must not serve
        #    metrics -- it redirects to the console login.
        r = requests.get(
            f"{endpoint}/console/metrics-data?window=Last5Minutes",
            verify=False,
            timeout=10,
            allow_redirects=False,
        )
        assert r.status_code in (302, 303), (
            f"unauthenticated /console/metrics-data returned {r.status_code}, "
            "expected a redirect to login"
        )
        assert "/console/login" in r.headers.get("Location", ""), (
            "unauthenticated /console/metrics-data should redirect to console login"
        )

        # 2. With a valid console session, the route returns metrics JSON
        #    (per-table dimensions), gated by the session cookie rather than
        #    admin Basic auth.
        s = requests.Session()
        # The harness sets REQUESTS_CA_BUNDLE/SSL_CERT_FILE, which overrides
        # session.verify; disable env trust and pass verify=False per request so
        # the self-signed dev cert is accepted (same as the direct calls above).
        s.trust_env = False
        s.verify = False
        login = s.post(
            f"{endpoint}/console/login",
            data={"username": admin_user, "password": admin_pass},
            timeout=10,
            allow_redirects=False,
            verify=False,
        )
        assert login.status_code in (302, 303), (
            f"console login failed: {login.status_code}"
        )
        r = s.get(
            f"{endpoint}/console/metrics-data?window=Last5Minutes",
            timeout=10,
            verify=False,
        )
        assert r.status_code == 200, (
            f"authenticated /console/metrics-data returned {r.status_code}, "
            "expected 200"
        )
        body = r.json()
        assert isinstance(body, dict) and ("metrics" in body or "buckets" in body), (
            "authenticated /console/metrics-data should return a MetricsResponse "
            "JSON body"
        )

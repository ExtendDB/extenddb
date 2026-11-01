# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The console metrics dashboard and its data feed serve admins only.

The metrics table behind the dashboard has no account column, so its feed
carries the table names and traffic of every account. An IAM user who logs in
to the console gets neither the page nor the feed. An admin gets both, with
every account's tables.

Two accounts each create a table and send it traffic. The tests wait until
the metrics flush has written both tables' rows, as seen through the admin's
feed, and then check what an IAM user of the second account receives.

Prerequisites match tests/test_console_integration.py:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from typing import Any

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig

from conftest import wait_for_active, wait_for_deleted
from management_helpers import ManagementClient

# The flush worker persists a data point once it is older than the worker's
# 60-second interval, at the next tick, so a point can take up to two
# intervals to reach the metrics table.
FLUSH_DEADLINE_SECONDS = 180
POLL_SECONDS = 5

# The module fixture waits for the metrics flush.
pytestmark = pytest.mark.slow


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: console metrics tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass


class Console:
    """One browser: a cookie jar talking to the server's console."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = requests.Session()
        # Self-signed certs from ``extenddb init``. The harness sets
        # REQUESTS_CA_BUNDLE, which overrides a session-level ``verify``, so
        # the environment is ignored and ``verify`` is passed per request.
        self.http.trust_env = False
        self.verify = not self.base_url.startswith("https://")

    def login(self, username: str, password: str) -> None:
        resp = self.http.post(
            f"{self.base_url}/console/login",
            data={"username": username, "password": password},
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )
        assert resp.status_code == 303, (
            f"login as {username}: {resp.status_code} {resp.text[:200]}"
        )

    def get(self, path: str, **params: str) -> requests.Response:
        return self.http.get(
            f"{self.base_url}/console{path}",
            params=params,
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )


@dataclass
class Account:
    account_id: str
    user_name: str
    password: str
    access_key_id: str
    ddb: Any
    table: str


def _ddb_client(endpoint: str, access_key_id: str, secret_access_key: str) -> Any:
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    if endpoint.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _create_account(mgmt: ManagementClient, endpoint: str, label: str) -> Account:
    """An account with one IAM user (console password, access key, full
    DynamoDB access) and one table that has seen reads and writes."""
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    user_name, password = "metrics-viewer", uuid.uuid4().hex
    resp = mgmt.create_account(account_id, f"metrics-{label}-{account_id}")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_user(account_id, user_name, password)
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(account_id, user_name)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
    }
    resp = mgmt.put_user_policy(account_id, user_name, "full", policy)
    assert resp.status_code == 204, resp.text

    ddb = _ddb_client(endpoint, creds["access_key_id"], creds["secret_access_key"])
    table = f"metrics-scope-{label}-{uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=table,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(ddb, table)
    for i in range(3):
        ddb.put_item(TableName=table, Item={"pk": {"S": f"item-{i}"}})
        ddb.get_item(TableName=table, Key={"pk": {"S": f"item-{i}"}})
    return Account(account_id, user_name, password, creds["access_key_id"], ddb, table)


def _delete_account(mgmt: ManagementClient, account: Account) -> None:
    """Best-effort cleanup; a failure here must not hide a test result."""
    try:
        account.ddb.delete_table(TableName=account.table)
        wait_for_deleted(account.ddb, account.table)
    except Exception:
        pass
    mgmt.delete_user_policy(account.account_id, account.user_name, "full")
    mgmt.delete_access_key(account.account_id, account.user_name, account.access_key_id)
    mgmt.delete_user(account.account_id, account.user_name)
    mgmt.delete_account(account.account_id)


def _table_names(feed: dict) -> set[str]:
    names = set()
    for metric in feed.get("metrics", []):
        for dimension in metric.get("dimensions", []):
            if "TableName" in dimension:
                names.add(dimension["TableName"])
    return names


@dataclass
class Flushed:
    admin: Console
    user_b: Console
    tables: tuple[str, str]


@pytest.fixture(scope="module")
def flushed():
    """Two accounts with traffic, persisted by the metrics flush, an admin
    console session and a console session for the IAM user of account B."""
    endpoint, admin_user, admin_pass = _require_auth_env()
    mgmt = ManagementClient(endpoint, admin_user, admin_pass)
    accounts: list[Account] = []
    try:
        accounts.append(_create_account(mgmt, endpoint, "a"))
        accounts.append(_create_account(mgmt, endpoint, "b"))
        account_a, account_b = accounts
        admin = Console(endpoint)
        admin.login(admin_user, admin_pass)
        tables = (account_a.table, account_b.table)

        deadline = time.monotonic() + FLUSH_DEADLINE_SECONDS
        seen: set[str] = set()
        while True:
            resp = admin.get("/metrics-data", window="LastHour")
            assert resp.status_code == 200, f"admin feed: {resp.status_code} {resp.text[:200]}"
            seen = _table_names(resp.json())
            if set(tables) <= seen or time.monotonic() > deadline:
                break
            time.sleep(POLL_SECONDS)
        assert set(tables) <= seen, (
            f"the metrics flush did not persist {tables} within "
            f"{FLUSH_DEADLINE_SECONDS}s; the admin feed holds {sorted(seen)}"
        )

        user_b = Console(endpoint)
        user_b.login(f"{account_b.account_id}/{account_b.user_name}", account_b.password)
        yield Flushed(admin, user_b, tables)
    finally:
        for account in accounts:
            _delete_account(mgmt, account)


# Query strings for the feed, built from the two accounts' table names.
FEED_QUERIES = {
    "default": lambda tables: {},
    "last-hour": lambda tables: {"window": "LastHour"},
    "all-time": lambda tables: {"window": "AllTime"},
    "account-a-table": lambda tables: {"table_name": tables[0]},
}


@pytest.mark.parametrize("query", sorted(FEED_QUERIES))
def test_iam_user_gets_403_and_no_metrics_from_the_feed(flushed: Flushed, query: str) -> None:
    params = FEED_QUERIES[query](flushed.tables)
    resp = flushed.user_b.get("/metrics-data", **params)
    assert resp.status_code == 403, f"{resp.status_code} {resp.text[:300]}"
    assert "TableName" not in resp.text, resp.text
    for table in flushed.tables:
        assert table not in resp.text, resp.text


def test_iam_user_is_redirected_from_the_metrics_page(flushed: Flushed) -> None:
    resp = flushed.user_b.get("/metrics")
    assert resp.status_code == 303, f"{resp.status_code} {resp.text[:300]}"
    assert resp.headers.get("location") == "/console"


def test_navigation_links_the_metrics_page_for_admins_only(flushed: Flushed) -> None:
    link = 'href="/console/metrics"'
    user_dashboard = flushed.user_b.get("")
    assert user_dashboard.status_code == 200, user_dashboard.status_code
    assert link not in user_dashboard.text
    admin_dashboard = flushed.admin.get("")
    assert admin_dashboard.status_code == 200, admin_dashboard.status_code
    assert link in admin_dashboard.text


def test_admin_gets_the_page_and_every_accounts_tables(flushed: Flushed) -> None:
    page = flushed.admin.get("/metrics")
    assert page.status_code == 200, page.status_code
    assert "<h1>Metrics</h1>" in page.text
    assert "accountFilter" not in page.text

    resp = flushed.admin.get("/metrics-data", window="LastHour")
    assert resp.status_code == 200, resp.status_code
    seen = _table_names(resp.json())
    assert set(flushed.tables) <= seen, sorted(seen)

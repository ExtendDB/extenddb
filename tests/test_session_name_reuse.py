# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Role sessions that share a session name each keep their own session data.

AssumeRole stores a new session for every call, so a role can hold several
sessions with one session name at the same time. A request signed with a
session's credentials is authorized with that session's own session policy,
session tags and issue time (`aws:TokenIssueTime`), whatever other sessions
of the role are called.

Each test assumes one role twice under one session name, with different
session data on the two calls, and checks the answer to a request of each
session. A server that read one session's data for both sessions answers one
of the two requests wrongly, whichever session's data it read.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT,
    on the clock of the host that runs the tests (the issue time test compares
    the two)
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

Run:
  EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:18443 \
  EXTENDDB_ADMIN_USER=admin \
  EXTENDDB_ADMIN_PASSWORD=<password> \
  pytest tests/test_session_name_reuse.py -v
"""

from __future__ import annotations

import datetime
import os
import time
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient

ALLOW_ALL = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
}

# The session name every session of a test is assumed under.
SESSION_NAME = "shared-name"

# The time between the issue of each session and the cut-off the issue time
# test puts between them, which covers a clock difference between this host
# and the server.
CLOCK_MARGIN = datetime.timedelta(seconds=1.2)


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: these tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER and EXTENDDB_ADMIN_PASSWORD, which "
            "devtools/run-tests sets."
        )
    return endpoint, admin_user, admin_pass


def _make_client(
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str,
    session_token: str | None = None,
) -> Any:
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    if session_token:
        kwargs["aws_session_token"] = session_token
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _policy(statements: list[dict]) -> dict:
    return {"Version": "2012-10-17", "Statement": statements}


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
def account_id(mgmt) -> str:
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"sessname-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


class TestSessionNameReuse:
    """Two sessions of one role under one session name, each judged by its own
    session data."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region
        self.caller = f"caller-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, self.caller, "TestPass123!")
        assert resp.status_code == 201, resp.text
        resp = self.mgmt.put_user_policy(self.account_id, self.caller, "full", ALLOW_ALL)
        assert resp.status_code == 204, resp.text
        resp = self.mgmt.create_access_key(self.account_id, self.caller)
        assert resp.status_code == 201, resp.text
        keys = resp.json()
        self.caller_client = _make_client(
            self.endpoint, keys["access_key_id"], keys["secret_access_key"], self.region
        )
        self.caller_arn = f"arn:aws:iam::{self.account_id}:user/{self.caller}"
        self.table = f"sn-{uuid.uuid4().hex[:8]}"
        self.caller_client.create_table(
            TableName=self.table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(self.caller_client, self.table)
        self.role = f"sn-role-{uuid.uuid4().hex[:8]}"
        trust = _policy(
            [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": self.caller_arn},
                    "Action": ["sts:AssumeRole", "sts:TagSession"],
                }
            ]
        )
        resp = self.mgmt.create_role(self.account_id, self.role, trust)
        assert resp.status_code == 201, resp.text
        yield
        self.mgmt.delete_role(self.account_id, self.role)
        try:
            self.caller_client.delete_table(TableName=self.table)
        except ClientError:
            pass
        self.mgmt.delete_user(self.account_id, self.caller)

    def _put_role_policy(self, policy: dict) -> None:
        resp = self.mgmt.put_role_policy(self.account_id, self.role, "p", policy)
        assert resp.status_code == 204, resp.text

    def _assume(self, **kwargs: Any) -> Any:
        """A client signed with the credentials of a new session of the role,
        assumed under `SESSION_NAME`."""
        resp = self.mgmt.assume_role(
            self.account_id, self.role, self.caller_arn, SESSION_NAME, **kwargs
        )
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        return _make_client(
            self.endpoint,
            creds["access_key_id"],
            creds["secret_access_key"],
            self.region,
            session_token=creds["session_token"],
        )

    def _outcomes(self, client: Any) -> dict[str, str]:
        """ALLOW or DENY for a GetItem and a PutItem on the table."""
        key = {"pk": {"S": "k1"}}
        calls = {
            "GetItem": lambda: client.get_item(TableName=self.table, Key=key),
            "PutItem": lambda: client.put_item(TableName=self.table, Item=key),
        }
        outcomes = {}
        for op, call in calls.items():
            try:
                call()
                outcomes[op] = "ALLOW"
            except ClientError as exc:
                assert exc.response["Error"]["Code"] == "AccessDeniedException", (
                    op,
                    exc.response["Error"],
                )
                outcomes[op] = "DENY"
        return outcomes

    def test_each_session_is_judged_by_its_own_session_policy(self):
        self._put_role_policy(
            _policy(
                [
                    {
                        "Effect": "Allow",
                        "Action": ["dynamodb:GetItem", "dynamodb:PutItem"],
                        "Resource": "*",
                    }
                ]
            )
        )
        reader = self._assume(
            session_policy=_policy(
                [{"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"}]
            )
        )
        writer = self._assume(
            session_policy=_policy(
                [{"Effect": "Allow", "Action": "dynamodb:PutItem", "Resource": "*"}]
            )
        )
        assert self._outcomes(reader) == {"GetItem": "ALLOW", "PutItem": "DENY"}
        assert self._outcomes(writer) == {"GetItem": "DENY", "PutItem": "ALLOW"}

    def test_each_session_is_judged_by_its_own_session_tags(self):
        self._put_role_policy(
            _policy(
                [
                    {
                        "Effect": "Allow",
                        "Action": "dynamodb:GetItem",
                        "Resource": "*",
                        "Condition": {"StringEquals": {"aws:PrincipalTag/team": "readers"}},
                    },
                    {
                        "Effect": "Allow",
                        "Action": "dynamodb:PutItem",
                        "Resource": "*",
                        "Condition": {"StringEquals": {"aws:PrincipalTag/team": "writers"}},
                    },
                ]
            )
        )
        reader = self._assume(session_tags={"team": "readers"})
        writer = self._assume(session_tags={"team": "writers"})
        assert self._outcomes(reader) == {"GetItem": "ALLOW", "PutItem": "DENY"}
        assert self._outcomes(writer) == {"GetItem": "DENY", "PutItem": "ALLOW"}

    def test_each_session_is_judged_by_its_own_issue_time(self):
        # A Deny of every action to sessions issued before a cut-off that
        # falls between the two sessions' issue times.
        older = self._assume()
        time.sleep(CLOCK_MARGIN.total_seconds())
        cut = datetime.datetime.now(datetime.timezone.utc).replace(
            microsecond=0
        ) + datetime.timedelta(seconds=1)
        while datetime.datetime.now(datetime.timezone.utc) < cut + CLOCK_MARGIN:
            time.sleep(0.1)
        newer = self._assume()
        self._put_role_policy(
            _policy(
                [
                    {
                        "Effect": "Allow",
                        "Action": ["dynamodb:GetItem", "dynamodb:PutItem"],
                        "Resource": "*",
                    },
                    {
                        "Effect": "Deny",
                        "Action": "*",
                        "Resource": "*",
                        "Condition": {
                            "DateLessThan": {
                                "aws:TokenIssueTime": cut.strftime("%Y-%m-%dT%H:%M:%SZ")
                            }
                        },
                    },
                ]
            )
        )
        assert self._outcomes(older) == {"GetItem": "DENY", "PutItem": "DENY"}
        assert self._outcomes(newer) == {"GetItem": "ALLOW", "PutItem": "ALLOW"}

# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A tag key repeated within one TagResource request reads its last value.

ExtendDB accepts a request that names a tag key twice and stores the last value
on SQLite, PostgreSQL and MongoDB: TagResource walks the list in request order
and upserts each key. Cassandra was not run; it writes one INSERT per tag, and
the order of two writes to one key was not checked.
Authorization reads the same value, so a Deny on `aws:RequestTag/<key>` applies
when the last value is the one it names and does not apply when only an earlier
value is. The hosted service refuses a repeated key with `ValidationException`
("Duplicate Tag Keys provided as input"), measured on 2026-10-27, and ExtendDB
does not; that difference is outside these tests, which pin that authorization
and the engine read one value.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

Run:
  EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:18443 \
  EXTENDDB_ADMIN_USER=admin \
  EXTENDDB_ADMIN_PASSWORD=<password> \
  pytest tests/test_tag_request_repeated_keys.py -v
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient
from test_tag_request_conditions import (
    ALLOW_ALL,
    _deny,
    _error_code,
    _make_client,
    _require_auth_env,
    _user_with_key,
)

DENY_PROD = {"StringEquals": {"aws:RequestTag/env": "prod"}}


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
    resp = mgmt.create_account(acct_id, f"reqtag-dup-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


class TestRepeatedTagKey:
    """Authorization reads the value TagResource stores for a repeated key."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region
        self.admin_name = f"adm-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(self.mgmt, self.account_id, self.admin_name)
        resp = self.mgmt.put_user_policy(self.account_id, self.admin_name, "full", ALLOW_ALL)
        assert resp.status_code == 204, resp.text
        self.admin_client = _make_client(self.endpoint, ak, sk, self.region)
        self.table = f"rtd-{uuid.uuid4().hex[:8]}"
        self.admin_client.create_table(
            TableName=self.table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(self.admin_client, self.table)
        self.arn = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/{self.table}"
        yield
        try:
            self.admin_client.delete_table(TableName=self.table)
        except ClientError:
            pass
        self.mgmt.delete_user(self.account_id, self.admin_name)

    def _restricted(self, policy: dict) -> Any:
        """A client signed for a user holding `policy`."""
        name = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(self.mgmt, self.account_id, name)
        resp = self.mgmt.put_user_policy(self.account_id, name, "p", policy)
        assert resp.status_code == 204, resp.text
        return _make_client(self.endpoint, ak, sk, self.region)

    def _stored_tags(self) -> dict[str, str]:
        resp = self.admin_client.list_tags_of_resource(ResourceArn=self.arn)
        return {t["Key"]: t["Value"] for t in resp.get("Tags", [])}

    def test_deny_on_an_earlier_value_does_not_apply(self):
        client = self._restricted(_deny(DENY_PROD))
        client.tag_resource(
            ResourceArn=self.arn,
            Tags=[{"Key": "env", "Value": "prod"}, {"Key": "env", "Value": "dev"}],
        )
        assert self._stored_tags() == {"env": "dev"}

    def test_deny_on_the_last_value_applies(self):
        client = self._restricted(_deny(DENY_PROD))
        with pytest.raises(ClientError) as exc:
            client.tag_resource(
                ResourceArn=self.arn,
                Tags=[{"Key": "env", "Value": "dev"}, {"Key": "env", "Value": "prod"}],
            )
        assert _error_code(exc.value) == "AccessDeniedException", exc.value.response
        assert self._stored_tags() == {}

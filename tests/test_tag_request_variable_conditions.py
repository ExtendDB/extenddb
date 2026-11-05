# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A policy variable that names `aws:TagKeys` stays literal text.

`aws:TagKeys` is the set of tag keys a request names, so it has no single value
to put where `${aws:TagKeys}` stands in a condition value. The value keeps its
text, and no tag key equals that text.

The expected answers come from two measurements on 2026-10-27:

  - `iam simulate-custom-policy`, a Deny on `TagResource` whose condition
    compares `aws:ResourceTag/team` (or `aws:RequestTag/env`) with
    `${aws:TagKeys}`: not applied for any order of the request's tag keys. The
    same policy with `StringNotEquals` is applied for every order, which is the
    comparison against the literal text.
  - a resource policy carrying the same Deny on a DynamoDB table, with
    `TagResource` requests listing the tag keys in both orders: not refused.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

Run:
  EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:18443 \
  EXTENDDB_ADMIN_USER=admin \
  EXTENDDB_ADMIN_PASSWORD=<password> \
  pytest tests/test_tag_request_variable_conditions.py -v
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

TAG_ORDERS = [
    pytest.param(["env", "owner"], id="env-first"),
    pytest.param(["owner", "env"], id="env-second"),
]

# A Deny on TagResource that compares a tag with the variable. The table carries
# team=env, and each request tag has its key as its value, so `aws:ResourceTag/team`
# and `aws:RequestTag/env` both read `env`, the first tag key of one order.
VARIABLE_DENIES = [
    pytest.param(
        {"StringEquals": {"aws:ResourceTag/team": "${aws:TagKeys}"}}, id="resource-tag"
    ),
    pytest.param(
        {"StringEquals": {"aws:RequestTag/env": "${aws:TagKeys}"}}, id="request-tag"
    ),
]


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
    resp = mgmt.create_account(acct_id, f"reqtag-var-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


def _tags(keys: list[str]) -> list[dict[str, str]]:
    return [{"Key": key, "Value": key} for key in keys]


class TestTagKeysPolicyVariable:
    """A condition value `${aws:TagKeys}` is compared as text."""

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
        self.table = f"rtv-{uuid.uuid4().hex[:8]}"
        self.admin_client.create_table(
            TableName=self.table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": "team", "Value": "env"}],
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

    @pytest.mark.parametrize("condition", VARIABLE_DENIES)
    @pytest.mark.parametrize("keys", TAG_ORDERS)
    def test_variable_naming_tagkeys_applies_for_no_tag_order(self, condition, keys):
        client = self._restricted(_deny(condition))
        client.tag_resource(ResourceArn=self.arn, Tags=_tags(keys))
        assert self._stored_tags() == {"team": "env", **{key: key for key in keys}}

    def test_literal_value_on_the_same_tag_applies(self):
        # The same Deny with the text `env` for the variable refuses the request,
        # so the table's `team` tag is read and a Deny on it can fire.
        client = self._restricted(_deny({"StringEquals": {"aws:ResourceTag/team": "env"}}))
        with pytest.raises(ClientError) as exc:
            client.tag_resource(ResourceArn=self.arn, Tags=_tags(["colour"]))
        assert _error_code(exc.value) == "AccessDeniedException", exc.value.response
        assert self._stored_tags() == {"team": "env"}

    @pytest.mark.parametrize("keys", [*TAG_ORDERS, pytest.param(["colour"], id="other-key")])
    def test_negated_comparison_with_the_variable_applies_for_every_tag_order(self, keys):
        # `env` is not equal to the text `${aws:TagKeys}`, so the negated
        # comparison is true whatever the request's tag keys are.
        client = self._restricted(
            _deny({"StringNotEquals": {"aws:ResourceTag/team": "${aws:TagKeys}"}})
        )
        with pytest.raises(ClientError) as exc:
            client.tag_resource(ResourceArn=self.arn, Tags=_tags(keys))
        assert _error_code(exc.value) == "AccessDeniedException", exc.value.response
        assert self._stored_tags() == {"team": "env"}

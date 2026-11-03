# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A tag sent as a two-element array reaches the tag condition keys.

`TagResource` and `CreateTable` read each tag as an object
(`{"Key": "k", "Value": "v"}`) or as a two-element array (`["k", "v"]`) and
store both. No SDK sends the array form, so these tests sign the request body
themselves and post it unchanged (`raw_signed_request.signed_post`). A Deny on
`aws:TagKeys` or `aws:RequestTag/<key>` has to refuse a request in the array form
as it refuses the object form, and a refused request stores nothing.

A `Tags` value the engine cannot parse populates neither key. The engine refuses
the body before it writes anything. The last tests in each group assert that the
body is refused and nothing is stored, and leave the error name unpinned.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

Run:
  EXTENDDB_TEST_ENDPOINT=https://127.0.0.1:18443 \
  EXTENDDB_ADMIN_USER=admin \
  EXTENDDB_ADMIN_PASSWORD=<password> \
  pytest tests/test_tag_request_array_conditions.py -v
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient
from raw_signed_request import signed_post
from test_tag_request_conditions import (
    ALLOW_ALL,
    _deny,
    _error_code,
    _make_client,
    _require_auth_env,
    _user_with_key,
)

TAG_KEYS_DENY = {"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}}
REQUEST_TAG_DENY = {"StringEquals": {"aws:RequestTag/env": "prod"}}

# (condition, Tags) pairs the Deny has to refuse.
REFUSED = [
    pytest.param(TAG_KEYS_DENY, [["secret", "x"]], id="tag-keys-array"),
    pytest.param(
        TAG_KEYS_DENY,
        [{"Key": "colour", "Value": "red"}, ["secret", "x"]],
        id="tag-keys-object-then-array",
    ),
    pytest.param(
        TAG_KEYS_DENY,
        [["secret", "x"], {"Key": "colour", "Value": "red"}],
        id="tag-keys-array-then-object",
    ),
    pytest.param(REQUEST_TAG_DENY, [["env", "prod"]], id="request-tag-array"),
]

# (condition, Tags, stored tags) the same Deny has to allow.
ALLOWED = [
    pytest.param(TAG_KEYS_DENY, [["colour", "red"]], {"colour": "red"}, id="tag-keys-other-key"),
    pytest.param(REQUEST_TAG_DENY, [["env", "dev"]], {"env": "dev"}, id="request-tag-other-value"),
]

# `Tags` values the engine refuses.
MALFORMED = [
    pytest.param([{"Key": "secret"}], id="no-value"),
    pytest.param([{"Key": "secret", "Value": None}], id="null-value"),
    pytest.param([["secret"]], id="one-element-array"),
    pytest.param([{"Key": "colour", "Value": "red"}, {"Key": "secret"}], id="valid-then-no-value"),
]


def _create_table_body(name: str, tags: Any) -> dict:
    return {
        "TableName": name,
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "Tags": tags,
    }


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
    resp = mgmt.create_account(acct_id, f"reqtag-array-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


class TestArrayTagConditions:
    """A Deny on a tag condition key applies to a tag sent as an array."""

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
        self.table = f"rta-{uuid.uuid4().hex[:8]}"
        self.admin_client.create_table(
            TableName=self.table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(self.admin_client, self.table)
        self.arn = self._table_arn(self.table)
        yield
        try:
            self.admin_client.delete_table(TableName=self.table)
        except ClientError:
            pass
        self.mgmt.delete_user(self.account_id, self.admin_name)

    def _table_arn(self, table: str) -> str:
        return f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/{table}"

    def _restricted(self, policy: dict) -> tuple[str, str]:
        """The access key and secret of a user holding `policy`."""
        name = f"u-{uuid.uuid4().hex[:8]}"
        creds = _user_with_key(self.mgmt, self.account_id, name)
        resp = self.mgmt.put_user_policy(self.account_id, name, "p", policy)
        assert resp.status_code == 204, resp.text
        return creds

    def _post(self, creds: tuple[str, str], operation: str, body: Any) -> tuple[int, str]:
        """Sign `body` as it is, post it, and return the status and error name."""
        resp = signed_post(self.endpoint, creds[0], creds[1], self.region, operation, body)
        try:
            code = resp.json().get("__type", "").rsplit("#", 1)[-1]
        except ValueError:
            code = ""
        return resp.status_code, code

    def _stored_tags(self, arn: str | None = None) -> dict[str, str]:
        resp = self.admin_client.list_tags_of_resource(ResourceArn=arn or self.arn)
        return {t["Key"]: t["Value"] for t in resp.get("Tags", [])}

    def _drop_table(self, name: str) -> None:
        try:
            wait_for_active(self.admin_client, name)
            self.admin_client.delete_table(TableName=name)
        except ClientError:
            pass

    # -- TagResource ------------------------------------------------------

    @pytest.mark.parametrize("condition,tags", REFUSED)
    def test_tag_resource_refuses_the_array_form(self, condition, tags):
        creds = self._restricted(_deny(condition))
        status, code = self._post(creds, "TagResource", {"ResourceArn": self.arn, "Tags": tags})
        assert (status, code) == (400, "AccessDeniedException")
        assert self._stored_tags() == {}, "a refused TagResource stores nothing"

    @pytest.mark.parametrize("condition,tags,stored", ALLOWED)
    def test_tag_resource_allows_an_array_tag_the_deny_does_not_name(
        self, condition, tags, stored
    ):
        creds = self._restricted(_deny(condition))
        status, code = self._post(creds, "TagResource", {"ResourceArn": self.arn, "Tags": tags})
        assert status == 200, code
        assert self._stored_tags() == stored

    @pytest.mark.parametrize("tags", MALFORMED)
    def test_tag_resource_refuses_a_malformed_entry_and_stores_nothing(self, tags):
        creds = self._restricted(_deny(TAG_KEYS_DENY))
        status, _ = self._post(creds, "TagResource", {"ResourceArn": self.arn, "Tags": tags})
        assert status == 400
        assert self._stored_tags() == {}

    # -- CreateTable ------------------------------------------------------

    @pytest.mark.parametrize("condition,tags", REFUSED)
    def test_create_table_refuses_the_array_form(self, condition, tags):
        creds = self._restricted(_deny(condition, actions="dynamodb:CreateTable"))
        name = f"ct-{uuid.uuid4().hex[:8]}"
        try:
            status, code = self._post(creds, "CreateTable", _create_table_body(name, tags))
            assert (status, code) == (400, "AccessDeniedException")
            with pytest.raises(ClientError) as exc:
                self.admin_client.describe_table(TableName=name)
            assert _error_code(exc.value) == "ResourceNotFoundException"
        finally:
            self._drop_table(name)

    @pytest.mark.parametrize("condition,tags,stored", ALLOWED)
    def test_create_table_allows_an_array_tag_the_deny_does_not_name(
        self, condition, tags, stored
    ):
        creds = self._restricted(_deny(condition, actions="dynamodb:CreateTable"))
        name = f"ct-{uuid.uuid4().hex[:8]}"
        try:
            status, code = self._post(creds, "CreateTable", _create_table_body(name, tags))
            assert status == 200, code
            wait_for_active(self.admin_client, name)
            assert self._stored_tags(self._table_arn(name)) == stored
        finally:
            self._drop_table(name)

    @pytest.mark.parametrize("tags", MALFORMED)
    def test_create_table_refuses_a_malformed_entry_and_creates_nothing(self, tags):
        creds = self._restricted(_deny(TAG_KEYS_DENY, actions="dynamodb:CreateTable"))
        name = f"ct-{uuid.uuid4().hex[:8]}"
        try:
            status, _ = self._post(creds, "CreateTable", _create_table_body(name, tags))
            assert status == 400
            with pytest.raises(ClientError) as exc:
                self.admin_client.describe_table(TableName=name)
            assert _error_code(exc.value) == "ResourceNotFoundException"
        finally:
            self._drop_table(name)

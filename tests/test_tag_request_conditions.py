# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Condition keys resolved from the tags a request carries.

`aws:TagKeys` and `aws:RequestTag/<key>` on the operations the AWS service
authorization reference (dynamodb v1.5) lists them on: `TagResource` and
`CreateTable` populate both, `UntagResource` populates `aws:TagKeys`, and every
other operation populates neither.

The operator semantics come from `iam simulate-custom-policy` runs on
2026-10-24: which operator forms a Deny fires under, which context values match,
and what happens when the key is absent. Three tests encode ExtendDB's own
behaviour where no IAM measurement exists. A tag key repeated in one request
reads its last value, which TagResource stores
(`test_request_tag_duplicate_key_reads_its_last_value`). A request that names one
tag under two spellings with differing values fails closed
(`test_request_tag_case_variant_keys_with_differing_values_are_refused` and
`test_ambiguous_request_tag_as_a_policy_variable_is_refused`), because IAM
refuses a simulated context of that shape.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

Run:
  EXTENDDB_TEST_ENDPOINT=http://localhost:18646 \
  EXTENDDB_ADMIN_USER=admin \
  EXTENDDB_ADMIN_PASSWORD=<password> \
  pytest tests/test_tag_request_conditions.py -v
"""

from __future__ import annotations

import os
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


def _deny(cond: dict, actions: Any = "dynamodb:TagResource") -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
            {
                "Sid": "DenyByRequestTags",
                "Effect": "Deny",
                "Action": actions,
                "Resource": "*",
                "Condition": cond,
            },
        ],
    }


def _error_code(exc: ClientError) -> str:
    return exc.response["Error"]["Code"]


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
    resp = mgmt.create_account(acct_id, f"reqtag-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


def _user_with_key(mgmt: ManagementClient, account_id: str, name: str) -> tuple[str, str]:
    resp = mgmt.create_user(account_id, name, "TestPass123!")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(account_id, name)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    return creds["access_key_id"], creds["secret_access_key"]


class TestTagRequestConditions:
    """A Deny gated on the tags a request carries applies to the request."""

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
        self.table = f"rt-{uuid.uuid4().hex[:8]}"
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
        """A user holding `policy`, with a client signed for it."""
        name = f"u-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(self.mgmt, self.account_id, name)
        resp = self.mgmt.put_user_policy(self.account_id, name, "p", policy)
        assert resp.status_code == 204, resp.text
        return _make_client(self.endpoint, ak, sk, self.region), name

    def _stored_tags(self) -> dict[str, str]:
        resp = self.admin_client.list_tags_of_resource(ResourceArn=self.arn)
        return {t["Key"]: t["Value"] for t in resp.get("Tags", [])}

    def _assert_denied(self, call, *args, **kwargs):
        with pytest.raises(ClientError) as exc:
            call(*args, **kwargs)
        assert _error_code(exc.value) == "AccessDeniedException", exc.value.response

    # -- aws:TagKeys on TagResource -------------------------------------

    def test_tagkeys_deny_refuses_the_named_key(self):
        client, _ = self._restricted(
            _deny({"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}})
        )
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "secret", "Value": "x"}],
        )
        assert "secret" not in self._stored_tags(), "a refused TagResource stores nothing"

    def test_tagkeys_deny_allows_another_key(self):
        client, _ = self._restricted(
            _deny({"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}})
        )
        client.tag_resource(ResourceArn=self.arn, Tags=[{"Key": "colour", "Value": "red"}])
        assert self._stored_tags()["colour"] == "red"

    def test_tagkeys_deny_refuses_a_set_containing_the_named_key(self):
        client, _ = self._restricted(
            _deny({"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}})
        )
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "colour", "Value": "red"}, {"Key": "secret", "Value": "x"}],
        )
        assert self._stored_tags() == {}, "neither tag of a refused request is stored"

    def test_tagkeys_values_compare_case_sensitively(self):
        # IAM case A-deny-ForAnyValue-StringEquals-ctx-Secret-upper: allowed.
        client, _ = self._restricted(
            _deny({"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}})
        )
        client.tag_resource(ResourceArn=self.arn, Tags=[{"Key": "Secret", "Value": "x"}])
        assert "Secret" in self._stored_tags()

    def test_tagkeys_ignore_case_operator_refuses_the_case_variant(self):
        # IAM case A-deny-ForAnyValue-StringEqualsIgnoreCase-ctx-Secret-upper:
        # explicitDeny.
        client, _ = self._restricted(
            _deny({"ForAnyValue:StringEqualsIgnoreCase": {"aws:TagKeys": "secret"}})
        )
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "Secret", "Value": "x"}],
        )

    def test_tagkeys_deny_refuses_a_duplicated_named_key(self):
        # IAM case A-deny-ForAnyValue-StringEquals-ctx-dup-secret: explicitDeny.
        client, _ = self._restricted(
            _deny({"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}})
        )
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "secret", "Value": "a"}, {"Key": "secret", "Value": "b"}],
        )

    def test_tagkeys_for_all_values_allows_a_set_outside_the_list(self):
        # IAM case A-deny-ForAllValues-StringEquals-ctx-both: allowed, because one
        # key is outside the policy list.
        client, _ = self._restricted(
            _deny({"ForAllValues:StringEquals": {"aws:TagKeys": ["colour"]}})
        )
        client.tag_resource(
            ResourceArn=self.arn,
            Tags=[{"Key": "colour", "Value": "red"}, {"Key": "shape", "Value": "round"}],
        )
        assert set(self._stored_tags()) == {"colour", "shape"}

    def test_tagkeys_for_all_values_refuses_a_set_inside_the_list(self):
        client, _ = self._restricted(
            _deny({"ForAllValues:StringEquals": {"aws:TagKeys": ["colour"]}})
        )
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "colour", "Value": "red"}],
        )

    def test_bare_operator_on_a_present_tagkeys_does_not_refuse(self):
        # IAM case G-aws-TagKeys-bare-StringNotEquals-present and
        # IAM case A-deny-plain-StringEquals-ctx-secret: both allowed. An
        # unqualified operator on a present multivalued key never matches.
        for cond in [
            {"StringEquals": {"aws:TagKeys": "secret"}},
            {"StringNotEquals": {"aws:TagKeys": "secret"}},
        ]:
            client, _ = self._restricted(_deny(cond))
            client.tag_resource(
                ResourceArn=self.arn, Tags=[{"Key": "secret", "Value": "x"}]
            )
            self.admin_client.untag_resource(ResourceArn=self.arn, TagKeys=["secret"])

    # -- aws:RequestTag/<key> on TagResource -----------------------------

    def test_request_tag_deny_refuses_the_named_value(self):
        client, _ = self._restricted(_deny({"StringEquals": {"aws:RequestTag/env": "prod"}}))
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        assert self._stored_tags() == {}

    def test_request_tag_deny_allows_another_value(self):
        client, _ = self._restricted(_deny({"StringEquals": {"aws:RequestTag/env": "prod"}}))
        client.tag_resource(ResourceArn=self.arn, Tags=[{"Key": "env", "Value": "dev"}])
        assert self._stored_tags()["env"] == "dev"

    def test_request_tag_values_compare_case_sensitively(self):
        # IAM case B-deny-StringEquals-ctx-Prod-upper: allowed.
        client, _ = self._restricted(_deny({"StringEquals": {"aws:RequestTag/env": "prod"}}))
        client.tag_resource(ResourceArn=self.arn, Tags=[{"Key": "env", "Value": "Prod"}])
        assert self._stored_tags()["env"] == "Prod"

    def test_request_tag_sub_key_matches_case_insensitively(self):
        # IAM case C-subkey-policy-Env-ctx-env and C-subkey-policy-env-ctx-Env:
        # both explicitDeny. The tag name in the condition key is matched
        # without regard to case.
        client, _ = self._restricted(_deny({"StringEquals": {"aws:RequestTag/Env": "prod"}}))
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "env", "Value": "prod"}],
        )

    def test_request_tag_deny_allows_a_request_naming_another_tag(self):
        # IAM case B-deny-StringEquals-ctx-absent: allowed. A request that does not
        # carry the key leaves it absent, and a positive operator on an absent
        # key does not match.
        client, _ = self._restricted(_deny({"StringEquals": {"aws:RequestTag/env": "prod"}}))
        client.tag_resource(ResourceArn=self.arn, Tags=[{"Key": "owner", "Value": "prod"}])
        assert self._stored_tags()["owner"] == "prod"

    def test_request_tag_duplicate_key_reads_its_last_value(self):
        # TagResource upserts in request order on SQLite, PostgreSQL and MongoDB,
        # so the last value is the one stored; authorization reads the same one.
        client, _ = self._restricted(_deny({"StringEquals": {"aws:RequestTag/env": "prod"}}))
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "env", "Value": "dev"}, {"Key": "env", "Value": "prod"}],
        )

    def test_request_tag_case_variant_keys_with_differing_values_are_refused(self):
        # The service (measured on 2026-10-24) and the engine both store
        # Env=prod and env=dev as two tags, so a request naming the tag under two
        # spellings with differing values has no single value for
        # aws:RequestTag/env. The key then fails closed: the Deny blocks in
        # either order and nothing is stored. IAM refuses a simulated context of
        # this shape, so the service's own answer for it is unmeasured.
        client, _ = self._restricted(_deny({"StringEquals": {"aws:RequestTag/env": "prod"}}))
        for tags in (
            [{"Key": "Env", "Value": "prod"}, {"Key": "env", "Value": "dev"}],
            [{"Key": "env", "Value": "dev"}, {"Key": "Env", "Value": "prod"}],
        ):
            self._assert_denied(client.tag_resource, ResourceArn=self.arn, Tags=tags)
            assert self._stored_tags() == {}, "a refused request stores neither spelling"
        # The same two spellings carrying one value read as that value.
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "Env", "Value": "prod"}, {"Key": "ENV", "Value": "prod"}],
        )
        client.tag_resource(
            ResourceArn=self.arn,
            Tags=[{"Key": "Env", "Value": "dev"}, {"Key": "ENV", "Value": "dev"}],
        )
        assert self._stored_tags() == {"Env": "dev", "ENV": "dev"}

    def test_request_tag_negated_deny_refuses_a_request_without_the_key(self):
        # IAM case B-deny-StringNotEquals-ctx-absent: explicitDeny. The absent-key
        # rule for a negated operator on a single-valued key.
        client, _ = self._restricted(
            _deny({"StringNotEquals": {"aws:RequestTag/env": "prod"}})
        )
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "owner", "Value": "x"}],
        )

    def test_request_tag_is_absent_on_untag_resource(self):
        # UntagResource names keys and no values, so the reference gives it
        # aws:TagKeys only. A Deny on aws:RequestTag/<key> with a positive
        # operator does not apply to it.
        self.admin_client.tag_resource(
            ResourceArn=self.arn, Tags=[{"Key": "env", "Value": "prod"}]
        )
        client, _ = self._restricted(
            _deny({"StringEquals": {"aws:RequestTag/env": "prod"}}, actions="dynamodb:*")
        )
        client.untag_resource(ResourceArn=self.arn, TagKeys=["env"])
        assert "env" not in self._stored_tags()

    # -- aws:TagKeys on UntagResource ------------------------------------

    def test_untag_resource_tagkeys_deny_refuses_the_named_key(self):
        self.admin_client.tag_resource(
            ResourceArn=self.arn, Tags=[{"Key": "secret", "Value": "x"}]
        )
        client, _ = self._restricted(
            _deny(
                {"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}},
                actions="dynamodb:UntagResource",
            )
        )
        self._assert_denied(client.untag_resource, ResourceArn=self.arn, TagKeys=["secret"])
        assert self._stored_tags()["secret"] == "x", "a refused UntagResource removes nothing"

    def test_untag_resource_tagkeys_deny_allows_another_key(self):
        self.admin_client.tag_resource(
            ResourceArn=self.arn, Tags=[{"Key": "colour", "Value": "red"}]
        )
        client, _ = self._restricted(
            _deny(
                {"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}},
                actions="dynamodb:UntagResource",
            )
        )
        client.untag_resource(ResourceArn=self.arn, TagKeys=["colour"])
        assert "colour" not in self._stored_tags()

    # -- CreateTable ------------------------------------------------------

    def test_create_table_tagkeys_deny_refuses_the_named_key(self):
        client, _ = self._restricted(
            _deny(
                {"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}},
                actions="dynamodb:CreateTable",
            )
        )
        name = f"ct-{uuid.uuid4().hex[:8]}"
        self._assert_denied(
            client.create_table,
            TableName=name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": "secret", "Value": "x"}],
        )
        with pytest.raises(ClientError) as exc:
            self.admin_client.describe_table(TableName=name)
        assert _error_code(exc.value) == "ResourceNotFoundException"

    def test_create_table_without_tags_is_allowed_under_the_same_deny(self):
        # IAM case F-deny-tagkeys-ctx-absent-CreateTable: allowed. CreateTable with
        # no Tags leaves aws:TagKeys absent.
        client, _ = self._restricted(
            _deny(
                {"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}},
                actions="dynamodb:CreateTable",
            )
        )
        name = f"ct-{uuid.uuid4().hex[:8]}"
        try:
            client.create_table(
                TableName=name,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(self.admin_client, name)
        finally:
            try:
                self.admin_client.delete_table(TableName=name)
            except ClientError:
                pass

    def test_create_table_request_tag_deny_refuses_the_named_value(self):
        client, _ = self._restricted(
            _deny(
                {"StringEquals": {"aws:RequestTag/env": "prod"}},
                actions="dynamodb:CreateTable",
            )
        )
        name = f"ct-{uuid.uuid4().hex[:8]}"
        self._assert_denied(
            client.create_table,
            TableName=name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": "env", "Value": "prod"}],
        )

    # -- the keys are absent on operations that carry no tag set ----------

    def test_tagkeys_is_absent_on_an_item_operation(self):
        # IAM case G-aws-TagKeys-bare-StringNotEquals-absent: explicitDeny, and
        # A-deny-ForAnyValue-StringEquals-ctx-absent: allowed. GetItem carries
        # no tag set, so aws:TagKeys is absent for it.
        allow_client, _ = self._restricted(
            _deny(
                {"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}},
                actions="dynamodb:GetItem",
            )
        )
        allow_client.get_item(TableName=self.table, Key={"pk": {"S": "k"}})

        deny_client, _ = self._restricted(
            _deny({"Null": {"aws:TagKeys": "true"}}, actions="dynamodb:GetItem")
        )
        self._assert_denied(
            deny_client.get_item, TableName=self.table, Key={"pk": {"S": "k"}}
        )

    def test_bare_negated_tagkeys_deny_refuses_an_operation_with_no_tags(self):
        # IAM case G-aws-TagKeys-bare-StringNotEquals-absent: explicitDeny.
        client, _ = self._restricted(
            _deny({"StringNotEquals": {"aws:TagKeys": "secret"}}, actions="dynamodb:GetItem")
        )
        self._assert_denied(client.get_item, TableName=self.table, Key={"pk": {"S": "k"}})

    def test_for_all_values_tagkeys_deny_refuses_an_operation_with_no_tags(self):
        # IAM case A-deny-ForAllValues-StringEquals-ctx-absent: explicitDeny.
        # ForAllValues on an absent key holds vacuously.
        client, _ = self._restricted(
            _deny(
                {"ForAllValues:StringEquals": {"aws:TagKeys": "secret"}},
                actions="dynamodb:GetItem",
            )
        )
        self._assert_denied(client.get_item, TableName=self.table, Key={"pk": {"S": "k"}})

    def test_null_false_tagkeys_deny_refuses_a_tagged_request(self):
        # IAM case A-deny-Null-false-ctx-secret: explicitDeny.
        client, _ = self._restricted(_deny({"Null": {"aws:TagKeys": "false"}}))
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "colour", "Value": "red"}],
        )

    # -- the policy types that carry the condition ------------------------

    def test_permissions_boundary_deny_refuses_the_named_key(self):
        # IAM case E-boundary-deny-tagkeys-ctx-secret: explicitDeny.
        name = f"ub-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(self.mgmt, self.account_id, name)
        resp = self.mgmt.put_user_policy(self.account_id, name, "p", ALLOW_ALL)
        assert resp.status_code == 204, resp.text
        resp = self.mgmt.set_user_boundary(
            self.account_id,
            name,
            _deny({"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}}),
        )
        assert resp.status_code == 204, resp.text
        client = _make_client(self.endpoint, ak, sk, self.region)
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "secret", "Value": "x"}],
        )
        client.tag_resource(ResourceArn=self.arn, Tags=[{"Key": "colour", "Value": "red"}])

    def test_role_policy_deny_refuses_the_named_key(self):
        role = f"r-{uuid.uuid4().hex[:8]}"
        caller = f"arn:aws:iam::{self.account_id}:user/{self.admin_name}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": caller},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        assert self.mgmt.create_role(self.account_id, role, trust).status_code == 201
        try:
            resp = self.mgmt.put_role_policy(
                self.account_id,
                role,
                "p",
                _deny({"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}}),
            )
            assert resp.status_code == 204, resp.text
            resp = self.mgmt.assume_role(self.account_id, role, caller, "sess")
            assert resp.status_code == 201, resp.text
            creds = resp.json()
            client = _make_client(
                self.endpoint,
                creds["access_key_id"],
                creds["secret_access_key"],
                self.region,
                session_token=creds["session_token"],
            )
            self._assert_denied(
                client.tag_resource,
                ResourceArn=self.arn,
                Tags=[{"Key": "secret", "Value": "x"}],
            )
            client.tag_resource(
                ResourceArn=self.arn, Tags=[{"Key": "colour", "Value": "red"}]
            )
        finally:
            self.mgmt.delete_role(self.account_id, role)

    def test_session_policy_deny_refuses_the_named_key(self):
        role = f"r-{uuid.uuid4().hex[:8]}"
        caller = f"arn:aws:iam::{self.account_id}:user/{self.admin_name}"
        trust = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": caller},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        assert self.mgmt.create_role(self.account_id, role, trust).status_code == 201
        try:
            assert (
                self.mgmt.put_role_policy(
                    self.account_id, role, "full", ALLOW_ALL
                ).status_code
                == 204
            )
            resp = self.mgmt.assume_role(
                self.account_id,
                role,
                caller,
                "sess",
                session_policy=_deny(
                    {"ForAnyValue:StringEquals": {"aws:TagKeys": "secret"}}
                ),
            )
            assert resp.status_code == 201, resp.text
            creds = resp.json()
            client = _make_client(
                self.endpoint,
                creds["access_key_id"],
                creds["secret_access_key"],
                self.region,
                session_token=creds["session_token"],
            )
            self._assert_denied(
                client.tag_resource,
                ResourceArn=self.arn,
                Tags=[{"Key": "secret", "Value": "x"}],
            )
        finally:
            self.mgmt.delete_role(self.account_id, role)

    # -- policy variables -------------------------------------------------

    def test_request_tag_expands_as_a_policy_variable(self):
        # IAM case D-var-principaltag-eq-requesttag-match: allowed, -differ:
        # implicitDeny. An Allow that holds only when the request tag equals
        # another resolved key.
        name = f"pv-{uuid.uuid4().hex[:8]}"
        ak, sk = _user_with_key(self.mgmt, self.account_id, name)
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
                {
                    "Sid": "DenyWhenOwnerIsNotTheCaller",
                    "Effect": "Deny",
                    "Action": "dynamodb:TagResource",
                    "Resource": "*",
                    "Condition": {
                        "StringNotEquals": {"aws:RequestTag/owner": "${aws:username}"}
                    },
                },
            ],
        }
        assert (
            self.mgmt.put_user_policy(self.account_id, name, "p", policy).status_code == 204
        )
        client = _make_client(self.endpoint, ak, sk, self.region)
        client.tag_resource(ResourceArn=self.arn, Tags=[{"Key": "owner", "Value": name}])
        assert self._stored_tags()["owner"] == name
        self._assert_denied(
            client.tag_resource,
            ResourceArn=self.arn,
            Tags=[{"Key": "owner", "Value": "someone-else"}],
        )

    def test_ambiguous_request_tag_as_a_policy_variable_is_refused(self):
        # IAM case V (2026-10-24): a Deny `StringEquals aws:ResourceTag/env
        # ${aws:RequestTag/env}` refuses env=prod on a table tagged env=prod and
        # does not apply when the request carries no env tag. A request naming
        # the tag under two spellings with differing values has no value to
        # expand the variable to, so the statement fails closed as it does when
        # the key is the condition key: refused in either order, nothing stored.
        self.admin_client.tag_resource(
            ResourceArn=self.arn, Tags=[{"Key": "env", "Value": "prod"}]
        )
        client, _ = self._restricted(
            _deny({"StringEquals": {"aws:ResourceTag/env": "${aws:RequestTag/env}"}})
        )
        self._assert_denied(
            client.tag_resource, ResourceArn=self.arn, Tags=[{"Key": "env", "Value": "prod"}]
        )
        for tags in (
            [{"Key": "Env", "Value": "prod"}, {"Key": "env", "Value": "dev"}],
            [{"Key": "env", "Value": "dev"}, {"Key": "Env", "Value": "prod"}],
        ):
            self._assert_denied(client.tag_resource, ResourceArn=self.arn, Tags=tags)
            assert self._stored_tags() == {"env": "prod"}, tags
        client.tag_resource(ResourceArn=self.arn, Tags=[{"Key": "team", "Value": "x"}])
        assert self._stored_tags() == {"env": "prod", "team": "x"}

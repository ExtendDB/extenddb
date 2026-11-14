# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""dynamodb:Attributes on writes, reads, document paths, conditions, filters,
legacy parameters, and batch and transaction requests.

Every request shape here was sent to live DynamoDB on 2026-10-31 or
2026-11-01 under a session policy of the same shape as the user policy the
test attaches, twice, with the same answer both times. The policies:

  * deny(X): Allow dynamodb:* on the table, and Deny dynamodb:* on the table
    with ForAnyValue:StringEquals {"dynamodb:Attributes": [X]}.
  * allow-only(action, condition): the only statement allows the action on
    the table under the condition: ForAllValues:StringEquals over a list of
    names, or a bare StringNotEquals {"dynamodb:Attributes": "ssn"}.
  * deny-get-when-absent: Allow dynamodb:* on the table, and Deny
    dynamodb:GetItem with Null {"dynamodb:Attributes": "true"}.

Under deny(ssn) the service refused every request that names ssn as an item
attribute, the target of a SET, REMOVE, ADD or DELETE action, an attribute a
condition, filter, Expected, AttributeUpdates or ScanFilter reads, or an entry
of AttributesToGet or ProjectionExpression, in the single, batch and
transaction forms measured. A document path is matched by its top-level name.
It authorized an UpdateItem whose only mention of ssn is in a SET value, which
copies ssn into another attribute. It allowed a DeleteItem of an item holding
ssn and reads with no projection, which returned whole items: the key holds
the names the request gives, and on GetItem the table's key attribute too, so
an Allow for all of ["name"] refused a GetItem of name and deny-get-when-absent
did not refuse a GetItem with no projection. A bare StringNotEquals granted no
Scan, whether the Scan named an attribute or not.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars

Run:
  EXTENDDB_TEST_ENDPOINT=https://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin \\
  EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_dynamodb_attributes_condition.py -v

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Any, Callable

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active
from management_helpers import ManagementClient

# The measured item, and four items holding ssn for the DeleteItem shape.
ITEM_A = {
    "pk": {"S": "a"},
    "ssn": {"S": "123-45-6789"},
    "name": {"S": "alice"},
    "secret": {"M": {"a": {"S": "1"}, "b": {"S": "2"}}},
}
DELETABLE = ["d1", "d2", "d3", "d4"]
KEY_A = {"pk": {"S": "a"}}

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
            "MISCONFIGURED: dynamodb:Attributes tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass


def _make_client(endpoint_url: str, access_key: str, secret_key: str,
                 region: str) -> Any:
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    # Self-signed certs from ``extenddb init``: disable SSL verification.
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _create_user_with_key(mgmt: ManagementClient, account_id: str,
                          user_name: str) -> tuple[str, str]:
    resp = mgmt.create_user(account_id, user_name, "TestPass123!")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(account_id, user_name)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    return creds["access_key_id"], creds["secret_access_key"]


def _deny(arn: str, attribute: str) -> dict:
    """deny(X): everything on the table except a request naming `attribute`."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": arn},
            {
                "Effect": "Deny",
                "Action": "dynamodb:*",
                "Resource": arn,
                "Condition": {
                    "ForAnyValue:StringEquals": {"dynamodb:Attributes": [attribute]}
                },
            },
        ],
    }


def _allow_only(arn: str, action: str, condition: dict) -> dict:
    """A policy whose only statement allows `action` on the table under `condition`."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": f"dynamodb:{action}",
                "Resource": arn,
                "Condition": condition,
            }
        ],
    }


def _for_all(attributes: list[str]) -> dict:
    return {"ForAllValues:StringEquals": {"dynamodb:Attributes": attributes}}


def _deny_get_when_absent(arn: str) -> dict:
    """Everything on the table, except a GetItem whose key is absent."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": arn},
            {
                "Effect": "Deny",
                "Action": "dynamodb:GetItem",
                "Resource": arn,
                "Condition": {"Null": {"dynamodb:Attributes": "true"}},
            },
        ],
    }


@dataclass
class Env:
    admin: Any
    table: str
    deny_ssn: Any
    deny_secret: Any
    allow_put_pk: Any
    allow_scan_unless_ssn: Any
    deny_get_when_absent: Any
    allow_get_name: Any
    allow_get_pk_name: Any


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
    resp = mgmt.create_account(acct_id, f"attrs-test-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


@pytest.fixture(scope="module")
def env(auth_env, mgmt, account_id, region) -> Env:
    endpoint = auth_env[0]
    owner = f"owner-{uuid.uuid4().hex[:8]}"
    ak, sk = _create_user_with_key(mgmt, account_id, owner)
    resp = mgmt.put_user_policy(account_id, owner, "full", ALLOW_ALL)
    assert resp.status_code == 204, resp.text
    admin = _make_client(endpoint, ak, sk, region)

    table = f"attrs-{uuid.uuid4().hex[:8]}"
    admin.create_table(
        TableName=table,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(admin, table)
    admin.put_item(TableName=table, Item=ITEM_A)
    for pk in DELETABLE:
        admin.put_item(
            TableName=table,
            Item={"pk": {"S": pk}, "name": {"S": "x"}, "ssn": {"S": "1"}},
        )
    arn = f"arn:aws:dynamodb:{region}:{account_id}:table/{table}"

    def client_with(policy: dict) -> Any:
        user = f"u-{uuid.uuid4().hex[:8]}"
        uak, usk = _create_user_with_key(mgmt, account_id, user)
        resp = mgmt.put_user_policy(account_id, user, "p", policy)
        assert resp.status_code == 204, resp.text
        return _make_client(endpoint, uak, usk, region)

    yield Env(
        admin=admin,
        table=table,
        deny_ssn=client_with(_deny(arn, "ssn")),
        deny_secret=client_with(_deny(arn, "secret")),
        allow_put_pk=client_with(_allow_only(arn, "PutItem", _for_all(["pk"]))),
        allow_scan_unless_ssn=client_with(
            _allow_only(arn, "Scan", {"StringNotEquals": {"dynamodb:Attributes": "ssn"}})
        ),
        deny_get_when_absent=client_with(_deny_get_when_absent(arn)),
        allow_get_name=client_with(_allow_only(arn, "GetItem", _for_all(["name"]))),
        allow_get_pk_name=client_with(_allow_only(arn, "GetItem", _for_all(["pk", "name"]))),
    )
    try:
        admin.delete_table(TableName=table)
    except ClientError:
        pass


def _refusal(call: Callable[[], Any], action: str) -> str:
    """The message of the AccessDeniedException `call` raises for `action`."""
    with pytest.raises(ClientError) as exc:
        call()
    err = exc.value.response["Error"]
    assert err["Code"] == "AccessDeniedException", exc.value.response
    assert f"perform: dynamodb:{action} on resource:" in err["Message"], err
    return err["Message"]


def _assert_denied(call: Callable[[], Any], action: str) -> None:
    msg = _refusal(call, action)
    assert "with an explicit deny in an identity-based policy" in msg, msg


def _new_key() -> dict:
    return {"pk": {"S": f"k-{uuid.uuid4().hex[:8]}"}}


def _stored(env: Env, key: dict) -> dict | None:
    return env.admin.get_item(TableName=env.table, Key=key, ConsistentRead=True).get("Item")


# -- writes ------------------------------------------------------------------


def test_put_item_of_an_item_holding_a_denied_attribute_is_denied(env: Env):
    key = _new_key()
    _assert_denied(
        lambda: env.deny_ssn.put_item(TableName=env.table, Item={**key, "ssn": {"S": "1"}}),
        "PutItem",
    )
    assert _stored(env, key) is None, "a refused write stores nothing"


def test_update_item_setting_a_denied_attribute_is_denied(env: Env):
    key = _new_key()
    _assert_denied(
        lambda: env.deny_ssn.update_item(
            TableName=env.table,
            Key=key,
            UpdateExpression="SET ssn = :v",
            ExpressionAttributeValues={":v": {"S": "1"}},
        ),
        "UpdateItem",
    )
    assert _stored(env, key) is None, "a refused write stores nothing"


def test_put_item_without_the_denied_attribute_is_allowed(env: Env):
    key = _new_key()
    env.deny_ssn.put_item(TableName=env.table, Item={**key, "name": {"S": "n"}})
    assert _stored(env, key) == {**key, "name": {"S": "n"}}


def test_update_item_setting_another_attribute_through_a_placeholder_is_allowed(env: Env):
    key = _new_key()
    env.deny_ssn.update_item(
        TableName=env.table,
        Key=key,
        UpdateExpression="SET #n = :v",
        ExpressionAttributeNames={"#n": "name"},
        ExpressionAttributeValues={":v": {"S": "n"}},
    )
    assert _stored(env, key) == {**key, "name": {"S": "n"}}


def test_delete_item_of_an_item_holding_the_denied_attribute_is_allowed(env: Env):
    key = {"pk": {"S": DELETABLE[0]}}
    assert "ssn" in _stored(env, key)
    env.deny_ssn.delete_item(TableName=env.table, Key=key)
    assert _stored(env, key) is None


def test_for_all_values_allow_refuses_a_put_of_an_unlisted_attribute(env: Env):
    key = _new_key()
    msg = _refusal(
        lambda: env.allow_put_pk.put_item(
            TableName=env.table, Item={**key, "ssn": {"S": "1"}}
        ),
        "PutItem",
    )
    assert "because no identity-based policy allows the dynamodb:PutItem action" in msg, msg
    assert _stored(env, key) is None, "a refused write stores nothing"


def test_for_all_values_allow_admits_a_put_of_listed_attributes_only(env: Env):
    key = _new_key()
    env.allow_put_pk.put_item(TableName=env.table, Item=key)
    assert _stored(env, key) == key


# -- reads -------------------------------------------------------------------


def test_get_item_attributes_to_get_naming_a_denied_attribute_is_denied(env: Env):
    _assert_denied(
        lambda: env.deny_ssn.get_item(
            TableName=env.table, Key=KEY_A, AttributesToGet=["ssn"]
        ),
        "GetItem",
    )


def test_scan_attributes_to_get_naming_a_denied_attribute_is_denied(env: Env):
    _assert_denied(
        lambda: env.deny_ssn.scan(TableName=env.table, AttributesToGet=["ssn"]),
        "Scan",
    )


def test_get_item_projection_naming_a_denied_attribute_is_denied(env: Env):
    _assert_denied(
        lambda: env.deny_ssn.get_item(
            TableName=env.table, Key=KEY_A, ProjectionExpression="ssn"
        ),
        "GetItem",
    )


def test_get_item_attributes_to_get_of_another_attribute_returns_only_it(env: Env):
    resp = env.deny_ssn.get_item(TableName=env.table, Key=KEY_A, AttributesToGet=["name"])
    assert resp["Item"] == {"name": {"S": "alice"}}


def test_get_item_projection_of_a_nested_path_is_matched_by_its_top_level_name(env: Env):
    _assert_denied(
        lambda: env.deny_secret.get_item(
            TableName=env.table, Key=KEY_A, ProjectionExpression="secret.a"
        ),
        "GetItem",
    )


def test_get_item_projection_of_a_placeholder_path_is_matched_by_its_top_level_name(
    env: Env,
):
    _assert_denied(
        lambda: env.deny_secret.get_item(
            TableName=env.table,
            Key=KEY_A,
            ProjectionExpression="#s.a",
            ExpressionAttributeNames={"#s": "secret"},
        ),
        "GetItem",
    )


def test_get_item_with_no_projection_returns_the_whole_item(env: Env):
    resp = env.deny_ssn.get_item(TableName=env.table, Key=KEY_A)
    assert resp["Item"] == ITEM_A


def test_scan_with_no_projection_returns_whole_items(env: Env):
    resp = env.deny_ssn.scan(TableName=env.table, Limit=2)
    assert resp["Count"] == 2
    for item in resp["Items"]:
        assert item == _stored(env, {"pk": item["pk"]})


# -- batch and transaction requests ------------------------------------------


def test_batch_get_item_projection_naming_a_denied_attribute_is_denied(env: Env):
    _assert_denied(
        lambda: env.deny_ssn.batch_get_item(
            RequestItems={env.table: {"Keys": [KEY_A], "ProjectionExpression": "ssn"}}
        ),
        "BatchGetItem",
    )


def test_transact_get_items_projection_naming_a_denied_attribute_is_denied(env: Env):
    _assert_denied(
        lambda: env.deny_ssn.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": env.table, "Key": KEY_A, "ProjectionExpression": "ssn"}}
            ]
        ),
        "GetItem",
    )


def test_batch_write_item_put_of_an_item_holding_a_denied_attribute_is_denied(env: Env):
    key = _new_key()
    _assert_denied(
        lambda: env.deny_ssn.batch_write_item(
            RequestItems={env.table: [{"PutRequest": {"Item": {**key, "ssn": {"S": "1"}}}}]}
        ),
        "BatchWriteItem",
    )
    assert _stored(env, key) is None, "a refused write stores nothing"


def test_transact_write_items_put_of_an_item_holding_a_denied_attribute_is_denied(
    env: Env,
):
    key = _new_key()
    _assert_denied(
        lambda: env.deny_ssn.transact_write_items(
            TransactItems=[
                {"Put": {"TableName": env.table, "Item": {**key, "ssn": {"S": "1"}}}}
            ]
        ),
        "PutItem",
    )
    assert _stored(env, key) is None, "a refused write stores nothing"


def test_batch_get_item_with_no_projection_returns_the_whole_item(env: Env):
    resp = env.deny_ssn.batch_get_item(RequestItems={env.table: {"Keys": [KEY_A]}})
    assert resp["Responses"][env.table] == [ITEM_A]

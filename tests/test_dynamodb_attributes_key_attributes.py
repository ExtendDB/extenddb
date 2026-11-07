# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""dynamodb:Attributes and the key attributes of the table and of a queried
index: Allows over ForAllValues lists on Query, UpdateItem, DeleteItem,
BatchWriteItem and Scan, a Deny over the key attribute on Scan, Null in a
Deny and an Allow on Scan, and the evaluation of a BatchWriteItem once per
table.

Unless its docstring says otherwise, every request shape here was sent to live
DynamoDB on 2026-11-01 under a session policy of the same shape as the user
policy the test attaches, twice, with the same answer both times. The fixtures
are those of test_dynamodb_attributes_condition.py; its docstring describes the
policies.

Prerequisites and run command: as for test_dynamodb_attributes_condition.py.

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Callable

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active

# The fixtures are imported so this module gets its own account and table.
from test_dynamodb_attributes_condition import (  # noqa: F401
    KEY_A,
    Env,
    _allow_only,
    _assert_denied,
    _create_user_with_key,
    _deny,
    _for_all,
    _make_client,
    _new_key,
    _refusal,
    _stored,
    account_id,
    auth_env,
    env,
    mgmt,
    region,
)

NULL_KEY = {"Null": {"dynamodb:Attributes": "true"}}


def _arn(region: str, account_id: str, table: str) -> str:
    return f"arn:aws:dynamodb:{region}:{account_id}:table/{table}"


def _deny_when_absent(arn: str, action: str) -> dict:
    """Everything on the table, except an `action` request whose key is absent."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": arn},
            {
                "Effect": "Deny",
                "Action": f"dynamodb:{action}",
                "Resource": arn,
                "Condition": NULL_KEY,
            },
        ],
    }


@pytest.fixture(scope="module")
def table_arn(env: Env, account_id: str, region: str) -> str:
    return _arn(region, account_id, env.table)


@pytest.fixture(scope="module")
def client_for(auth_env, mgmt, account_id, region) -> Callable[[dict], Any]:
    """A client for a new user whose only policy is the one given, made once
    per policy."""
    endpoint = auth_env[0]
    made: dict[str, Any] = {}

    def make(policy: dict) -> Any:
        key = json.dumps(policy, sort_keys=True)
        if key not in made:
            user = f"u-{uuid.uuid4().hex[:8]}"
            ak, sk = _create_user_with_key(mgmt, account_id, user)
            resp = mgmt.put_user_policy(account_id, user, "p", policy)
            assert resp.status_code == 204, resp.text
            made[key] = _make_client(endpoint, ak, sk, region)
        return made[key]

    return make


def _assert_not_allowed(call: Callable[[], Any], action: str) -> None:
    msg = _refusal(call, action)
    assert f"because no identity-based policy allows the dynamodb:{action} action" in msg, msg


# -- the table's key attributes on other operations --------------------------


def _naming_name_and_the_key(table: str) -> dict[str, Callable[[Any], Any]]:
    """Per action, a request that names name, the table's key attribute, or
    both."""
    return {
        "Query": lambda c: c.query(
            TableName=table, KeyConditionExpression="pk = :p", ProjectionExpression="#n",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":p": KEY_A["pk"]}),
        "UpdateItem": lambda c: c.update_item(
            TableName=table, Key=_new_key(), UpdateExpression="SET #n = :v",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":v": {"S": "n"}}),
        "DeleteItem": lambda c: c.delete_item(TableName=table, Key=_new_key()),
        "BatchWriteItem": lambda c: c.batch_write_item(
            RequestItems={table: [{"DeleteRequest": {"Key": _new_key()}}]}),
    }


KEY_ATTRIBUTE_ACTIONS = ["Query", "UpdateItem", "DeleteItem", "BatchWriteItem"]


@pytest.mark.parametrize("action", KEY_ATTRIBUTE_ACTIONS)
def test_for_all_values_allow_without_the_key_attribute_refuses(
    env: Env, client_for, table_arn: str, action: str
):
    """An Allow of the action alone with ForAllValues:StringEquals
    over ["name"] refuses a Query projecting name, an UpdateItem setting name,
    a DeleteItem and a BatchWriteItem DeleteRequest, because the key holds pk."""
    client = client_for(_allow_only(table_arn, action, _for_all(["name"])))
    _assert_not_allowed(lambda: _naming_name_and_the_key(env.table)[action](client), action)


@pytest.mark.parametrize("action", KEY_ATTRIBUTE_ACTIONS)
def test_for_all_values_allow_with_the_key_attribute_admits(
    env: Env, client_for, table_arn: str, action: str
):
    """The same Allow over ["pk", "name"] admits each request."""
    client = client_for(_allow_only(table_arn, action, _for_all(["pk", "name"])))
    _naming_name_and_the_key(env.table)[action](client)


# -- Scan holds the table's key attributes -----------------------------------


def _scans(table: str) -> dict[str, Callable[[Any], Any]]:
    return {
        "plain": lambda c: c.scan(TableName=table, Limit=2),
        "projected": lambda c: c.scan(
            TableName=table, Limit=2, ProjectionExpression="#n",
            ExpressionAttributeNames={"#n": "name"}),
        "count": lambda c: c.scan(TableName=table, Select="COUNT"),
    }


def _assert_whole_items(env: Env, resp: dict) -> None:
    assert resp["Count"] == 2
    for item in resp["Items"]:
        assert item == _stored(env, {"pk": item["pk"]})


def test_a_deny_on_the_key_attribute_refuses_a_scan_with_no_projection(
    env: Env, client_for, table_arn: str
):
    """A Deny over pk refuses a Scan that names no attribute."""
    client = client_for(_deny(table_arn, "pk"))
    _assert_denied(lambda: client.scan(TableName=env.table, Limit=1), "Scan")


def test_a_deny_on_the_key_attribute_refuses_a_scan_from_an_exclusive_start_key(
    env: Env, client_for, table_arn: str
):
    """A Deny over pk refuses a Scan whose only mention of pk is
    ExclusiveStartKey. The Scan holds pk without the start key too, so whether
    ExclusiveStartKey adds names of its own cannot be told on the service."""
    client = client_for(_deny(table_arn, "pk"))
    _assert_denied(lambda: client.scan(TableName=env.table, ExclusiveStartKey=KEY_A), "Scan")


@pytest.mark.parametrize("shape", ["plain", "projected", "count"])
def test_for_all_values_allow_without_the_key_attribute_refuses_a_scan(
    env: Env, client_for, table_arn: str, shape: str
):
    """An Allow of Scan alone with ForAllValues:StringEquals over
    ["name"] refuses a Scan with no projection, a Scan projecting name and a
    Select COUNT Scan."""
    client = client_for(_allow_only(table_arn, "Scan", _for_all(["name"])))
    _assert_not_allowed(lambda: _scans(env.table)[shape](client), "Scan")


def test_for_all_values_allow_with_the_key_attribute_admits_a_projected_scan(
    env: Env, client_for, table_arn: str
):
    """The same Allow over ["pk", "name"] admits a Scan projecting
    name."""
    client = client_for(_allow_only(table_arn, "Scan", _for_all(["pk", "name"])))
    assert "Items" in _scans(env.table)["projected"](client)


def test_for_all_values_allow_of_the_key_attribute_admits_a_scan_of_whole_items(
    env: Env, client_for, table_arn: str
):
    """An Allow over ["pk"] admits a Scan with no projection, which
    returns whole items."""
    client = client_for(_allow_only(table_arn, "Scan", _for_all(["pk"])))
    _assert_whole_items(env, _scans(env.table)["plain"](client))


def test_a_null_deny_does_not_refuse_a_scan_with_no_projection(
    env: Env, client_for, table_arn: str
):
    """A Deny of Scan gated on Null true does not refuse a Scan that
    names no attribute, because the key is present; the Scan returns whole
    items."""
    client = client_for(_deny_when_absent(table_arn, "Scan"))
    _assert_whole_items(env, _scans(env.table)["plain"](client))


def test_a_null_deny_does_not_refuse_a_projected_scan(env: Env, client_for, table_arn: str):
    """The same Deny does not refuse a Scan projecting name."""
    client = client_for(_deny_when_absent(table_arn, "Scan"))
    assert "Items" in _scans(env.table)["projected"](client)


@pytest.mark.parametrize("shape", ["plain", "count"])
def test_a_null_allow_grants_no_scan(env: Env, client_for, table_arn: str, shape: str):
    """An Allow of Scan alone gated on Null true grants neither a Scan
    with no projection nor a Select COUNT Scan."""
    client = client_for(_allow_only(table_arn, "Scan", NULL_KEY))
    _assert_not_allowed(lambda: _scans(env.table)[shape](client), "Scan")


# -- the queried index's key attributes --------------------------------------


@pytest.fixture(scope="module")
def indexed(env: Env, account_id: str, region: str):
    """A table keyed on pk with a global secondary index by-gk keyed on gk,
    and the table's ARN."""
    table = f"attrs-ix-{uuid.uuid4().hex[:8]}"
    env.admin.create_table(
        TableName=table,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gk", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[{
            "IndexName": "by-gk",
            "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(env.admin, table)
    env.admin.put_item(TableName=table, Item={"pk": {"S": "a"}, "gk": {"S": "g"}})
    yield table, _arn(region, account_id, table)
    try:
        env.admin.delete_table(TableName=table)
    except ClientError:
        pass


def _allow_query_on_indexes(arn: str, attributes: list[str]) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "dynamodb:Query",
            "Resource": [arn, f"{arn}/index/*"],
            "Condition": _for_all(attributes),
        }],
    }


def _query_by_gk(client: Any, table: str) -> Any:
    return client.query(
        TableName=table, IndexName="by-gk", KeyConditionExpression="gk = :g",
        ExpressionAttributeValues={":g": {"S": "g"}},
    )


def test_for_all_values_allow_without_the_index_key_refuses_a_query_on_the_index(
    indexed, client_for
):
    """Follows the IAM guide's rule, not measured: a policy on
    dynamodb:Attributes lists the key attributes of the table and of every
    index it covers. An Allow of Query with ForAllValues:StringEquals over
    ["pk"] refuses a Query on the index keyed on gk."""
    table, arn = indexed
    client = client_for(_allow_query_on_indexes(arn, ["pk"]))
    _assert_not_allowed(lambda: _query_by_gk(client, table), "Query")


def test_for_all_values_allow_with_the_index_key_admits_a_query_on_the_index(
    indexed, client_for
):
    """Follows the IAM guide's rule, not measured: the same Allow over
    ["pk", "gk"] admits the Query on the index."""
    table, arn = indexed
    client = client_for(_allow_query_on_indexes(arn, ["pk", "gk"]))
    assert "Items" in _query_by_gk(client, table)


# -- a BatchWriteItem is evaluated once per table ----------------------------


def _allow_batch_write_naming_name(table_arn: str) -> dict:
    return _allow_only(
        table_arn,
        "BatchWriteItem",
        {"ForAnyValue:StringEquals": {"dynamodb:Attributes": ["name"]}},
    )


def test_a_batch_write_is_evaluated_once_per_table_with_the_names_of_all_its_items(
    env: Env, client_for, table_arn: str
):
    """An Allow of BatchWriteItem alone with ForAnyValue:StringEquals
    over ["name"] admits a batch that puts {pk, name} and {pk, ssn} on one
    table, and both items are written."""
    client = client_for(_allow_batch_write_naming_name(table_arn))
    named, other = _new_key(), _new_key()
    resp = client.batch_write_item(RequestItems={env.table: [
        {"PutRequest": {"Item": {**named, "name": {"S": "n"}}}},
        {"PutRequest": {"Item": {**other, "ssn": {"S": "1"}}}},
    ]})
    assert resp["UnprocessedItems"] == {}
    assert _stored(env, named) == {**named, "name": {"S": "n"}}
    assert _stored(env, other) == {**other, "ssn": {"S": "1"}}


def test_a_batch_write_of_an_item_naming_a_listed_attribute_is_allowed(
    env: Env, client_for, table_arn: str
):
    """The same Allow admits a batch that puts {pk, name} alone."""
    client = client_for(_allow_batch_write_naming_name(table_arn))
    key = _new_key()
    client.batch_write_item(
        RequestItems={env.table: [{"PutRequest": {"Item": {**key, "name": {"S": "n"}}}}]}
    )
    assert _stored(env, key) == {**key, "name": {"S": "n"}}


def test_a_batch_write_of_an_item_naming_no_listed_attribute_is_refused(
    env: Env, client_for, table_arn: str
):
    """The same Allow refuses a batch that puts {pk, ssn} alone."""
    client = client_for(_allow_batch_write_naming_name(table_arn))
    key = _new_key()
    _assert_not_allowed(
        lambda: client.batch_write_item(
            RequestItems={env.table: [{"PutRequest": {"Item": {**key, "ssn": {"S": "1"}}}}]}
        ),
        "BatchWriteItem",
    )
    assert _stored(env, key) is None, "a refused write stores nothing"

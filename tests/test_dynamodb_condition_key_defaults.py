# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""dynamodb:Select, dynamodb:ReturnValues and dynamodb:ReturnConsumedCapacity
when a request leaves the parameter out.

DynamoDB evaluates the value the operation uses when the request carries no
parameter, so the key is present whether or not the caller spells it out:

  * dynamodb:Select is SPECIFIC_ATTRIBUTES on a GetItem, Query or Scan with a
    projection and ALL_ATTRIBUTES on one without, and the same on each table
    of a BatchGetItem and on the Get of a TransactGetItems. A Select the
    request gives is used as given.
  * dynamodb:ReturnValues is NONE on a PutItem, UpdateItem or DeleteItem
    without ReturnValues and on the Put, Update and Delete items of a
    TransactWriteItems. A BatchWriteItem leaves the key absent.
  * dynamodb:ReturnConsumedCapacity is NONE on a GetItem, a BatchWriteItem and
    the Get of a TransactGetItems without ReturnConsumedCapacity.

Unless its docstring says otherwise, every request shape here was sent to live
DynamoDB on 2026-11-01 under a session policy of the same shape as the user
policy the test attaches, twice, with the same answer both times. The fixtures
are those of test_dynamodb_attributes_condition.py, plus a table keyed on
(pk, sk) with a global secondary index for the two rows that were not
measured.

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
    ITEM_A,
    KEY_A,
    Env,
    _allow_only,
    _assert_denied,
    _create_user_with_key,
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

RV_NONE = {"StringEquals": {"dynamodb:ReturnValues": "NONE"}}
RCC_NONE = {"StringEquals": {"dynamodb:ReturnConsumedCapacity": "NONE"}}
NAME = {"ProjectionExpression": "#n", "ExpressionAttributeNames": {"#n": "name"}}


def _deny_select(resources: list[str], actions: list[str], operator: str,
                 value: str) -> dict:
    """Everything on the resources, except `actions` when `operator` holds
    over dynamodb:Select and `value`."""
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": resources},
            {
                "Effect": "Deny",
                "Action": [f"dynamodb:{a}" for a in actions],
                "Resource": resources,
                "Condition": {operator: {"dynamodb:Select": value}},
            },
        ],
    }


@pytest.fixture(scope="module")
def table_arn(env: Env, account_id: str, region: str) -> str:
    return f"arn:aws:dynamodb:{region}:{account_id}:table/{env.table}"


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


@pytest.fixture(scope="module")
def deny_all_attributes(client_for, table_arn: str) -> Callable[[list[str]], Any]:
    """A client denied `actions` with StringEquals {"dynamodb:Select":
    "ALL_ATTRIBUTES"} and allowed everything else on the table."""
    return lambda actions: client_for(
        _deny_select([table_arn], actions, "StringEquals", "ALL_ATTRIBUTES"))


def _assert_not_allowed(call: Callable[[], Any], action: str) -> None:
    msg = _refusal(call, action)
    assert f"because no identity-based policy allows the dynamodb:{action} action" in msg, msg


def _named_item(env: Env) -> dict:
    """A new item holding name, stored by the owner."""
    item = {**_new_key(), "name": {"S": "n"}}
    env.admin.put_item(TableName=env.table, Item=item)
    return item


# -- dynamodb:Select ----------------------------------------------------------


@pytest.mark.parametrize("action", ["GetItem", "Query"])
def test_projected_read_passes_a_deny_unless_specific_attributes(
    env: Env, client_for, table_arn: str, action: str
):
    """A Deny of the action with StringNotEqualsIfExists {"dynamodb:Select":
    "SPECIFIC_ATTRIBUTES"} lets a read with a projection and no Select
    through: its key holds SPECIFIC_ATTRIBUTES."""
    client = client_for(_deny_select(
        [table_arn], [action], "StringNotEqualsIfExists", "SPECIFIC_ATTRIBUTES"))
    if action == "GetItem":
        resp = client.get_item(TableName=env.table, Key=KEY_A, **NAME)
        assert resp["Item"] == {"name": ITEM_A["name"]}
    else:
        resp = client.query(
            TableName=env.table, KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": KEY_A["pk"]}, **NAME)
        assert resp["Items"] == [{"name": ITEM_A["name"]}]


@pytest.mark.parametrize("action", ["GetItem", "Query"])
def test_unprojected_read_is_denied_unless_specific_attributes(
    env: Env, client_for, table_arn: str, action: str
):
    """The same Deny refuses a read with no projection and no Select: its key
    holds ALL_ATTRIBUTES."""
    client = client_for(_deny_select(
        [table_arn], [action], "StringNotEqualsIfExists", "SPECIFIC_ATTRIBUTES"))
    if action == "GetItem":
        call = lambda: client.get_item(TableName=env.table, Key=KEY_A)
    else:
        call = lambda: client.query(
            TableName=env.table, KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": KEY_A["pk"]})
    _assert_denied(call, action)


@pytest.mark.parametrize("action", ["GetItem", "Query", "Scan"])
def test_unprojected_read_is_denied_on_all_attributes(
    env: Env, deny_all_attributes, action: str
):
    """A Deny of GetItem, Query and Scan with StringEquals {"dynamodb:Select":
    "ALL_ATTRIBUTES"} refuses a read with no projection and no Select."""
    client = deny_all_attributes(["GetItem", "Query", "Scan"])
    calls = {
        "GetItem": lambda: client.get_item(TableName=env.table, Key=KEY_A),
        "Query": lambda: client.query(
            TableName=env.table, KeyConditionExpression="pk = :p",
            ExpressionAttributeValues={":p": KEY_A["pk"]}),
        "Scan": lambda: client.scan(TableName=env.table),
    }
    _assert_denied(calls[action], action)


def test_projected_get_item_passes_a_deny_on_all_attributes(env: Env, deny_all_attributes):
    client = deny_all_attributes(["GetItem", "Query", "Scan"])
    resp = client.get_item(TableName=env.table, Key=KEY_A, **NAME)
    assert resp["Item"] == {"name": ITEM_A["name"]}


def test_scan_with_select_count_passes_a_deny_on_all_attributes(env: Env, deny_all_attributes):
    """A Select the request gives is used as given."""
    client = deny_all_attributes(["GetItem", "Query", "Scan"])
    resp = client.scan(TableName=env.table, Select="COUNT")
    assert resp["Count"] >= 1
    assert "Items" not in resp


def test_query_with_select_count_passes_a_deny_on_all_attributes(env: Env, deny_all_attributes):
    client = deny_all_attributes(["Query"])
    resp = client.query(
        TableName=env.table, KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": KEY_A["pk"]}, Select="COUNT")
    assert resp["Count"] == 1


def test_unprojected_batch_get_item_is_denied_on_all_attributes(env: Env, deny_all_attributes):
    client = deny_all_attributes(["BatchGetItem"])
    _assert_denied(
        lambda: client.batch_get_item(RequestItems={env.table: {"Keys": [KEY_A]}}),
        "BatchGetItem",
    )


def test_projected_batch_get_item_passes_a_deny_on_all_attributes(env: Env, deny_all_attributes):
    client = deny_all_attributes(["BatchGetItem"])
    resp = client.batch_get_item(RequestItems={env.table: {"Keys": [KEY_A], **NAME}})
    assert resp["Responses"][env.table] == [{"name": ITEM_A["name"]}]


def test_projected_batch_get_item_is_granted_specific_attributes(
    env: Env, client_for, table_arn: str
):
    """An Allow of BatchGetItem alone with StringEquals {"dynamodb:Select":
    "SPECIFIC_ATTRIBUTES"} grants a BatchGetItem with a projection."""
    client = client_for(_allow_only(
        table_arn, "BatchGetItem",
        {"StringEquals": {"dynamodb:Select": "SPECIFIC_ATTRIBUTES"}}))
    resp = client.batch_get_item(RequestItems={env.table: {"Keys": [KEY_A], **NAME}})
    assert resp["Responses"][env.table] == [{"name": ITEM_A["name"]}]


def test_unprojected_transaction_get_is_denied_on_all_attributes(env: Env, deny_all_attributes):
    """The Get of a TransactGetItems is authorized as GetItem, with the key
    set as on a GetItem."""
    client = deny_all_attributes(["GetItem"])
    _assert_denied(
        lambda: client.transact_get_items(
            TransactItems=[{"Get": {"TableName": env.table, "Key": KEY_A}}]),
        "GetItem",
    )


def test_projected_transaction_get_passes_a_deny_on_all_attributes(
    env: Env, deny_all_attributes
):
    client = deny_all_attributes(["GetItem"])
    resp = client.transact_get_items(
        TransactItems=[{"Get": {"TableName": env.table, "Key": KEY_A, **NAME}}])
    assert resp["Responses"] == [{"Item": {"name": ITEM_A["name"]}}]


# -- dynamodb:ReturnValues ----------------------------------------------------


def test_put_item_without_return_values_is_granted_return_values_none(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "PutItem", RV_NONE))
    item = {**_new_key(), "name": {"S": "n"}}
    client.put_item(TableName=env.table, Item=item)
    assert _stored(env, {"pk": item["pk"]}) == item


def test_put_item_with_return_values_all_old_is_not_granted(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "PutItem", RV_NONE))
    item = {**_new_key(), "name": {"S": "n"}}
    _assert_not_allowed(
        lambda: client.put_item(TableName=env.table, Item=item, ReturnValues="ALL_OLD"),
        "PutItem",
    )
    assert _stored(env, {"pk": item["pk"]}) is None, "a refused write stores nothing"


def test_update_item_without_return_values_is_granted_return_values_none(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "UpdateItem", RV_NONE))
    item = _named_item(env)
    key = {"pk": item["pk"]}
    client.update_item(
        TableName=env.table, Key=key, UpdateExpression="SET #n = :v",
        ExpressionAttributeNames={"#n": "name"}, ExpressionAttributeValues={":v": {"S": "u"}})
    assert _stored(env, key) == {**key, "name": {"S": "u"}}


def test_update_item_with_return_values_all_new_is_not_granted(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "UpdateItem", RV_NONE))
    item = _named_item(env)
    key = {"pk": item["pk"]}
    _assert_not_allowed(
        lambda: client.update_item(
            TableName=env.table, Key=key, UpdateExpression="SET #n = :v",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":v": {"S": "u"}}, ReturnValues="ALL_NEW"),
        "UpdateItem",
    )
    assert _stored(env, key) == item, "a refused write changes nothing"


def test_delete_item_without_return_values_is_granted_return_values_none(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "DeleteItem", RV_NONE))
    item = _named_item(env)
    key = {"pk": item["pk"]}
    client.delete_item(TableName=env.table, Key=key)
    assert _stored(env, key) is None


def test_delete_item_with_return_values_all_old_is_not_granted(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "DeleteItem", RV_NONE))
    item = _named_item(env)
    key = {"pk": item["pk"]}
    _assert_not_allowed(
        lambda: client.delete_item(TableName=env.table, Key=key, ReturnValues="ALL_OLD"),
        "DeleteItem",
    )
    assert _stored(env, key) == item, "a refused write changes nothing"


def test_batch_write_item_is_not_granted_return_values_none(
    env: Env, client_for, table_arn: str
):
    """A BatchWriteItem leaves dynamodb:ReturnValues absent, so an Allow gated
    on StringEquals NONE does not grant it."""
    client = client_for(_allow_only(table_arn, "BatchWriteItem", RV_NONE))
    item = {**_new_key(), "name": {"S": "n"}}
    _assert_not_allowed(
        lambda: client.batch_write_item(
            RequestItems={env.table: [{"PutRequest": {"Item": item}}]}),
        "BatchWriteItem",
    )
    assert _stored(env, {"pk": item["pk"]}) is None, "a refused write stores nothing"


@pytest.mark.parametrize("action", ["PutItem", "UpdateItem", "DeleteItem"])
def test_transaction_write_is_granted_return_values_none(
    env: Env, client_for, table_arn: str, action: str
):
    """Each Put, Update and Delete item of a TransactWriteItems is authorized
    as PutItem, UpdateItem and DeleteItem with dynamodb:ReturnValues NONE.
    The Delete names a key that holds no item, as on the service."""
    client = client_for(_allow_only(table_arn, action, RV_NONE))
    key = _new_key()
    item = {
        "PutItem": {"Put": {"TableName": env.table, "Item": {**key, "name": {"S": "t"}}}},
        "UpdateItem": {"Update": {
            "TableName": env.table, "Key": key, "UpdateExpression": "SET #n = :v",
            "ExpressionAttributeNames": {"#n": "name"},
            "ExpressionAttributeValues": {":v": {"S": "t"}}}},
        "DeleteItem": {"Delete": {"TableName": env.table, "Key": key}},
    }[action]
    client.transact_write_items(TransactItems=[item])
    expected = None if action == "DeleteItem" else {**key, "name": {"S": "t"}}
    assert _stored(env, key) == expected


# -- dynamodb:ReturnConsumedCapacity -----------------------------------------


def test_get_item_without_return_consumed_capacity_is_granted_none(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "GetItem", RCC_NONE))
    resp = client.get_item(TableName=env.table, Key=KEY_A)
    assert resp["Item"] == ITEM_A


def test_get_item_with_return_consumed_capacity_total_is_not_granted(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "GetItem", RCC_NONE))
    _assert_not_allowed(
        lambda: client.get_item(TableName=env.table, Key=KEY_A, ReturnConsumedCapacity="TOTAL"),
        "GetItem",
    )


def test_batch_write_item_without_return_consumed_capacity_is_granted_none(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "BatchWriteItem", RCC_NONE))
    item = {**_new_key(), "name": {"S": "n"}}
    resp = client.batch_write_item(RequestItems={env.table: [{"PutRequest": {"Item": item}}]})
    assert resp["UnprocessedItems"] == {}
    assert _stored(env, {"pk": item["pk"]}) == item


def test_transaction_get_without_return_consumed_capacity_is_granted_none(
    env: Env, client_for, table_arn: str
):
    client = client_for(_allow_only(table_arn, "GetItem", RCC_NONE))
    resp = client.transact_get_items(
        TransactItems=[{"Get": {"TableName": env.table, "Key": KEY_A}}])
    assert resp["Responses"] == [{"Item": ITEM_A}]


# -- rows that were not measured ---------------------------------------------


@pytest.fixture(scope="module")
def indexed(env: Env, account_id: str, region: str) -> tuple[str, list[str]]:
    """A table keyed on (pk, sk), with a global secondary index on gpk that
    projects every attribute, holding items (a, 1) and (a, 2). Yields the
    table name and the resources a policy on it and its indexes lists."""
    table = f"attrs-ix-{uuid.uuid4().hex[:8]}"
    env.admin.create_table(
        TableName=table,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(env.admin, table)
    for sk in ["1", "2"]:
        env.admin.put_item(TableName=table, Item={
            "pk": {"S": "a"}, "sk": {"S": sk}, "gpk": {"S": "g"}, "ssn": {"S": "1"}})
    arn = f"arn:aws:dynamodb:{region}:{account_id}:table/{table}"
    yield table, [arn, f"{arn}/index/*"]
    try:
        env.admin.delete_table(TableName=table)
    except ClientError:
        pass


def _index_query(client: Any, table: str, **extra: Any) -> Any:
    return client.query(
        TableName=table, IndexName="gsi", KeyConditionExpression="gpk = :g",
        ExpressionAttributeValues={":g": {"S": "g"}}, **extra)


def test_unprojected_index_query_holds_all_projected_attributes(indexed, client_for):
    """Not measured: follows the API reference, which gives ALL_PROJECTED_ATTRIBUTES
    as the default for a Query or Scan of an index with no projection and no
    Select. A Deny on ALL_PROJECTED_ATTRIBUTES refuses such a Query, and a
    Deny on ALL_ATTRIBUTES does not, though the index projects every
    attribute; with Select ALL_ATTRIBUTES given, the Deny on ALL_ATTRIBUTES
    refuses it."""
    table, resources = indexed
    deny_projected = client_for(_deny_select(
        resources, ["Query"], "StringEquals", "ALL_PROJECTED_ATTRIBUTES"))
    _assert_denied(lambda: _index_query(deny_projected, table), "Query")

    deny_all = client_for(_deny_select(resources, ["Query"], "StringEquals", "ALL_ATTRIBUTES"))
    _index_query(deny_all, table)
    _assert_denied(lambda: _index_query(deny_all, table, Select="ALL_ATTRIBUTES"), "Query")


def test_transaction_gets_of_one_partition_are_evaluated_apart(indexed, client_for):
    """Not measured: follows the TransactGetItems rows. Two Gets of partition a
    whose only difference for authorization is that the first projects pk:
    both hold the same dynamodb:LeadingKeys and dynamodb:Attributes, and a Deny
    on ALL_ATTRIBUTES refuses the transaction because the second Get holds
    ALL_ATTRIBUTES."""
    table, resources = indexed
    client = client_for(_deny_select(resources, ["GetItem"], "StringEquals", "ALL_ATTRIBUTES"))

    def get(sk: str, **extra: Any) -> dict:
        return {"Get": {"TableName": table, "Key": {"pk": {"S": "a"}, "sk": {"S": sk}}, **extra}}

    _assert_denied(
        lambda: client.transact_get_items(
            TransactItems=[get("1", ProjectionExpression="pk"), get("2")]),
        "GetItem",
    )
    resp = client.transact_get_items(TransactItems=[
        get("1", ProjectionExpression="pk"), get("2", ProjectionExpression="pk")])
    assert resp["Responses"] == [{"Item": {"pk": {"S": "a"}}}, {"Item": {"pk": {"S": "a"}}}]

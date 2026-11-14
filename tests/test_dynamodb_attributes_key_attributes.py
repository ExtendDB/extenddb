# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""dynamodb:Attributes and the key attributes of the table and of a queried
index: Allows over ForAllValues lists on Query, UpdateItem, DeleteItem,
BatchWriteItem and Scan, a Deny over the key attribute on Scan, Null in a
Deny and an Allow on Scan, the evaluation of a BatchWriteItem once per table,
the key attributes of a scanned index, a later page of a Scan of an index and
of the table, a Query key condition and a BatchGetItem key on a table that
does not exist.

Unless its docstring says otherwise, every request shape here was sent to live
DynamoDB on 2026-11-01, 2026-11-09 or 2026-11-10 under a session policy of the
same shape as the user policy the test attaches, twice, with the same answer
both times. The 2026-11-09 shapes were measured in two accounts, and the
2026-11-10 shapes in one. The fixtures are those of
test_dynamodb_attributes_condition.py; its docstring describes the policies.

Prerequisites and run command: as for test_dynamodb_attributes_condition.py.

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Callable

import pytest
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_gsi_items

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
    """A Query of an index holds the index's key attributes (measured
    2026-11-09 on an index with a sort key, below). An Allow of Query with
    ForAllValues:StringEquals over ["pk"] refuses a Query on the index keyed
    on gk."""
    table, arn = indexed
    client = client_for(_allow_query_on_indexes(arn, ["pk"]))
    _assert_not_allowed(lambda: _query_by_gk(client, table), "Query")


def test_for_all_values_allow_with_the_index_key_admits_a_query_on_the_index(
    indexed, client_for
):
    """The same Allow over ["pk", "gk"] admits the Query on the index."""
    table, arn = indexed
    client = client_for(_allow_query_on_indexes(arn, ["pk", "gk"]))
    assert "Items" in _query_by_gk(client, table)


# -- a scanned index's key attributes, and a queried index's sort key --------


COMPOSITE_ITEM = {
    "pk": {"S": "p1"},
    "sk": {"S": "s1"},
    "ls": {"S": "l1"},
    "gk": {"S": "g1"},
    "gs": {"S": "t1"},
    "name": {"S": "n"},
}


def _create_composite_table(env: Env, table: str) -> None:
    """Create a table keyed on pk and sk, with a local secondary index lsi
    keyed on pk and ls and a global secondary index gsi2 keyed on gk and gs,
    and wait until it is ACTIVE."""
    env.admin.create_table(
        TableName=table,
        AttributeDefinitions=[
            {"AttributeName": name, "AttributeType": "S"}
            for name in ["pk", "sk", "ls", "gk", "gs"]
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        LocalSecondaryIndexes=[{
            "IndexName": "lsi",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "ls", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
        GlobalSecondaryIndexes=[{
            "IndexName": "gsi2",
            "KeySchema": [
                {"AttributeName": "gk", "KeyType": "HASH"},
                {"AttributeName": "gs", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(env.admin, table)


@pytest.fixture(scope="module")
def composite(env: Env, account_id: str, region: str):
    """A table keyed on pk and sk, with a local secondary index lsi keyed on
    pk and ls and a global secondary index gsi2 keyed on gk and gs, holding
    COMPOSITE_ITEM; and the table's ARN."""
    table = f"attrs-cx-{uuid.uuid4().hex[:8]}"
    _create_composite_table(env, table)
    env.admin.put_item(TableName=table, Item=COMPOSITE_ITEM)
    # A write reaches gsi2 after index_propagation_delay_ms, so wait until an
    # admin Scan of gsi2 returns the item; each test's single read then checks
    # authorization alone.
    seen = wait_for_gsi_items(
        lambda: env.admin.scan(TableName=table, IndexName="gsi2")["Items"], 1
    )
    assert seen == [COMPOSITE_ITEM], seen
    yield table, _arn(region, account_id, table)
    try:
        env.admin.delete_table(TableName=table)
    except ClientError:
        pass


def _allow_on_indexes(arn: str, action: str, attributes: list[str]) -> dict:
    """An Allow of `action` alone, on the table and its indexes, over a
    ForAllValues list."""
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": f"dynamodb:{action}",
            "Resource": [arn, f"{arn}/index/*"],
            "Condition": _for_all(attributes),
        }],
    }


def _deny_on_indexes(arn: str, attribute: str) -> dict:
    """deny(X) on the table and its indexes."""
    resources = [arn, f"{arn}/index/*"]
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "dynamodb:*", "Resource": resources},
            {
                "Effect": "Deny",
                "Action": "dynamodb:*",
                "Resource": resources,
                "Condition": {
                    "ForAnyValue:StringEquals": {"dynamodb:Attributes": [attribute]}
                },
            },
        ],
    }


def _scan_index_projecting_name(client: Any, table: str, index: str) -> Any:
    return client.scan(
        TableName=table, IndexName=index, ProjectionExpression="#n",
        ExpressionAttributeNames={"#n": "name"},
    )


def _assert_the_name_only(resp: dict) -> None:
    assert resp["Count"] == 1
    assert resp["Items"] == [{"name": {"S": "n"}}]


def _assert_the_whole_item(resp: dict) -> None:
    assert resp["Count"] == 1
    assert resp["Items"] == [COMPOSITE_ITEM]


@pytest.mark.parametrize("index", ["gsi2", "lsi"])
def test_for_all_values_allow_of_the_table_keys_admits_a_scan_of_an_index(
    composite, client_for, index: str
):
    """The first page of a Scan of a global or a local secondary index holds
    the table's key attributes, and not gk, gs or ls: an Allow of Scan over
    ["pk", "sk", "name"] admits a Scan of either index projecting name."""
    table, arn = composite
    client = client_for(_allow_on_indexes(arn, "Scan", ["pk", "sk", "name"]))
    _assert_the_name_only(_scan_index_projecting_name(client, table, index))


def test_for_all_values_allow_of_the_table_keys_alone_admits_a_scan_of_an_index(
    composite, client_for
):
    """An Allow of Scan over ["pk", "sk"] admits a Scan of the global
    secondary index with no projection, which returns the whole item."""
    table, arn = composite
    client = client_for(_allow_on_indexes(arn, "Scan", ["pk", "sk"]))
    _assert_the_whole_item(client.scan(TableName=table, IndexName="gsi2"))


def test_a_deny_on_an_index_key_attribute_does_not_refuse_a_scan_of_the_index(
    composite, client_for
):
    """A Deny over gk, the global secondary index's partition key, does not
    refuse a Scan of that index, which returns the whole item."""
    table, arn = composite
    client = client_for(_deny_on_indexes(arn, "gk"))
    _assert_the_whole_item(client.scan(TableName=table, IndexName="gsi2"))


def test_for_all_values_allow_of_the_index_keys_alone_refuses_a_scan_of_the_index(
    composite, client_for
):
    """An Allow of Scan over the index's keys and name, without the table's
    keys, refuses a Scan of the index projecting name: the key holds pk and
    sk."""
    table, arn = composite
    client = client_for(_allow_on_indexes(arn, "Scan", ["gk", "gs", "name"]))
    _assert_not_allowed(lambda: _scan_index_projecting_name(client, table, "gsi2"), "Scan")


def _query_gsi2_projecting_name(client: Any, table: str) -> Any:
    return client.query(
        TableName=table, IndexName="gsi2", KeyConditionExpression="gk = :g",
        ProjectionExpression="#n", ExpressionAttributeNames={"#n": "name"},
        ExpressionAttributeValues={":g": {"S": "g1"}},
    )


def test_for_all_values_allow_without_the_index_sort_key_refuses_a_query_on_the_index(
    composite, client_for
):
    """A Query of an index holds the index's full key schema: an Allow of
    Query over ["pk", "sk", "gk", "name"] refuses a Query of the global
    secondary index whose key condition names only gk, because the key holds
    gs too."""
    table, arn = composite
    client = client_for(_allow_on_indexes(arn, "Query", ["pk", "sk", "gk", "name"]))
    _assert_not_allowed(lambda: _query_gsi2_projecting_name(client, table), "Query")


def test_for_all_values_allow_with_the_index_sort_key_admits_a_query_on_the_index(
    composite, client_for
):
    """The same Allow over ["pk", "sk", "gk", "gs", "name"] admits the
    Query, which returns name."""
    table, arn = composite
    client = client_for(_allow_on_indexes(arn, "Query", ["pk", "sk", "gk", "gs", "name"]))
    _assert_the_name_only(_query_gsi2_projecting_name(client, table))


# -- a later page of a Scan --------------------------------------------------


PAGED_ITEMS = [
    {
        "pk": {"S": f"p{i}"},
        "sk": {"S": f"s{i}"},
        "ls": {"S": f"l{i}"},
        "gk": {"S": f"g{i}"},
        "gs": {"S": f"t{i}"},
        "name": {"S": f"n{i}"},
    }
    for i in (1, 2)
]

# The names in the LastEvaluatedKey of page 1, which page 2 sends as its
# ExclusiveStartKey: the table's key attributes, and on a Scan of an index the
# index's key attributes too.
START_KEY_NAMES = {
    "gsi2": ["gk", "gs", "pk", "sk"],
    "lsi": ["ls", "pk", "sk"],
    None: ["pk", "sk"],
}


@pytest.fixture(scope="module")
def paged(env: Env, account_id: str, region: str):
    """A table of the composite fixture's shape holding the two PAGED_ITEMS,
    and the table's ARN."""
    table = f"attrs-pg-{uuid.uuid4().hex[:8]}"
    _create_composite_table(env, table)
    for item in PAGED_ITEMS:
        env.admin.put_item(TableName=table, Item=item)
    seen = wait_for_gsi_items(
        lambda: env.admin.scan(TableName=table, IndexName="gsi2")["Items"], 2
    )
    assert len(seen) == 2, seen
    yield table, _arn(region, account_id, table)
    try:
        env.admin.delete_table(TableName=table)
    except ClientError:
        pass


def _first_page(env: Env, table: str, index: str | None) -> dict:
    """Page 1 of a Scan with Limit 1, read by the table's owner. Its
    LastEvaluatedKey is the ExclusiveStartKey of page 2."""
    kwargs = {"IndexName": index} if index else {}
    resp = env.admin.scan(TableName=table, Limit=1, **kwargs)
    assert resp["Count"] == 1, resp
    assert sorted(resp["LastEvaluatedKey"]) == START_KEY_NAMES[index], resp
    return resp


@pytest.mark.parametrize("index", ["gsi2", "lsi"])
def test_for_all_values_allow_of_the_table_keys_refuses_a_later_page_of_an_index_scan(
    env: Env, paged, client_for, index: str
):
    """A later page of a Scan of a global or a local secondary index holds the
    index's key attributes, which its ExclusiveStartKey carries: an Allow of
    Scan over ["pk", "sk", "name"], which admits page 1 projecting name,
    refuses page 2 projecting name."""
    table, arn = paged
    start = _first_page(env, table, index)["LastEvaluatedKey"]
    client = client_for(_allow_on_indexes(arn, "Scan", ["pk", "sk", "name"]))
    _assert_not_allowed(
        lambda: client.scan(
            TableName=table, IndexName=index, Limit=1, ExclusiveStartKey=start,
            ProjectionExpression="#n", ExpressionAttributeNames={"#n": "name"},
        ),
        "Scan",
    )


def test_a_deny_on_an_index_key_attribute_refuses_a_later_page_of_the_index_scan(
    env: Env, paged, client_for
):
    """A Deny over gk, which does not refuse page 1 of a Scan of gsi2,
    refuses page 2, whose ExclusiveStartKey holds gk."""
    table, arn = paged
    start = _first_page(env, table, "gsi2")["LastEvaluatedKey"]
    client = client_for(_deny_on_indexes(arn, "gk"))
    _assert_denied(
        lambda: client.scan(
            TableName=table, IndexName="gsi2", Limit=1, ExclusiveStartKey=start,
        ),
        "Scan",
    )


def test_for_all_values_allow_of_the_table_keys_alone_admits_a_later_page_of_the_table(
    env: Env, paged, client_for
):
    """A later page of a Scan of the table holds only the table's key
    attributes: an Allow of Scan over ["pk", "sk"] admits page 2, which
    returns the item that page 1 did not."""
    table, arn = paged
    first = _first_page(env, table, None)
    client = client_for(_allow_on_indexes(arn, "Scan", ["pk", "sk"]))
    resp = client.scan(TableName=table, Limit=1, ExclusiveStartKey=first["LastEvaluatedKey"])
    assert resp["Count"] == 1, resp
    assert resp["Items"][0] in PAGED_ITEMS, resp
    assert resp["Items"] != first["Items"], resp


# -- a table that does not exist ---------------------------------------------


@pytest.fixture(scope="module")
def missing(account_id: str, region: str) -> tuple[str, str]:
    """The name and ARN of a table that is never created."""
    table = f"attrs-missing-{uuid.uuid4().hex[:8]}"
    return table, _arn(region, account_id, table)


def _assert_not_found(call: Callable[[], Any]) -> None:
    with pytest.raises(ClientError) as exc:
        call()
    err = exc.value.response["Error"]
    assert err["Code"] == "ResourceNotFoundException", exc.value.response


def test_a_query_of_a_missing_table_holds_the_names_in_its_key_condition(
    missing, client_for
):
    """On a table that does not exist, a Query whose KeyConditionExpression
    names name (a non-key name) is refused under a Deny over name with
    AccessDeniedException, not ResourceNotFoundException: the key holds the
    names the key condition gives."""
    table, arn = missing
    client = client_for(_deny(arn, "name"))
    _assert_denied(
        lambda: client.query(
            TableName=table, KeyConditionExpression="#n = :p",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":p": {"S": "p1"}},
        ),
        "Query",
    )


def test_a_query_of_a_missing_table_holds_the_names_in_its_legacy_key_conditions(
    missing, client_for
):
    """The same with legacy KeyConditions naming pk, under a Deny over pk."""
    table, arn = missing
    client = client_for(_deny(arn, "pk"))
    _assert_denied(
        lambda: client.query(
            TableName=table,
            KeyConditions={"pk": {"ComparisonOperator": "EQ",
                                  "AttributeValueList": [{"S": "p1"}]}},
        ),
        "Query",
    )


def test_a_batch_get_of_a_missing_table_does_not_hold_the_names_in_its_keys(
    missing, client_for
):
    """On a table that does not exist, a BatchGetItem whose only key names pk
    is answered ResourceNotFoundException under a Deny over pk: the names in
    Keys are not in the key, and the table has no key schema to add."""
    table, arn = missing
    client = client_for(_deny(arn, "pk"))
    _assert_not_found(
        lambda: client.batch_get_item(RequestItems={table: {"Keys": [{"pk": {"S": "p1"}}]}})
    )


def test_a_get_of_a_missing_table_does_not_hold_the_names_in_its_key(
    missing, client_for
):
    """A GetItem of the same key under the same Deny is answered
    ResourceNotFoundException too."""
    table, arn = missing
    client = client_for(_deny(arn, "pk"))
    _assert_not_found(lambda: client.get_item(TableName=table, Key={"pk": {"S": "p1"}}))


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

# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""dynamodb:Attributes for conditions, filters, legacy parameters, update
actions, transaction items, key attributes, returned items, and bare and Null
conditions.

Unless its docstring says otherwise, every request shape here was sent to live
DynamoDB on 2026-11-01 under a session policy of the same shape as the user
policy the test attaches, twice, with the same answer both times. The policies
and the fixtures are those of test_dynamodb_attributes_condition.py; its
docstring describes them.

Prerequisites and run command: as for test_dynamodb_attributes_condition.py.

REQ-TEST-001, REQ-AUTH-002
"""

from __future__ import annotations

import re
import uuid
from typing import Any, Callable

import pytest
from botocore.exceptions import ClientError

# The fixtures are imported so this module gets its own account and table.
from test_dynamodb_attributes_condition import (  # noqa: F401
    ITEM_A,
    KEY_A,
    Env,
    _assert_denied,
    _new_key,
    _refusal,
    _stored,
    account_id,
    auth_env,
    env,
    mgmt,
    region,
)

MISSING_ATTRIBUTE = "The provided expression refers to an attribute that does not exist in the item"


def _shapes_naming_ssn(table: str, key: dict) -> dict[str, tuple[str, Callable[[Any], Any]]]:
    """Requests that name ssn only in a condition, a filter, a legacy parameter
    or an update action target, each with the action its refusal names."""
    one = {":one": {"N": "1"}}
    return {
        "put_condition": ("PutItem", lambda c: c.put_item(
            TableName=table, Item=key, ConditionExpression="attribute_not_exists(ssn)")),
        "put_expected": ("PutItem", lambda c: c.put_item(
            TableName=table, Item=key, Expected={"ssn": {"Exists": False}})),
        "delete_condition": ("DeleteItem", lambda c: c.delete_item(
            TableName=table, Key=key, ConditionExpression="attribute_not_exists(ssn)")),
        "update_remove": ("UpdateItem", lambda c: c.update_item(
            TableName=table, Key=key, UpdateExpression="REMOVE ssn")),
        "update_add": ("UpdateItem", lambda c: c.update_item(
            TableName=table, Key=key, UpdateExpression="ADD ssn :one",
            ExpressionAttributeValues=one)),
        "update_delete": ("UpdateItem", lambda c: c.update_item(
            TableName=table, Key=key, UpdateExpression="DELETE ssn :s",
            ExpressionAttributeValues={":s": {"SS": ["x"]}})),
        "update_attribute_updates": ("UpdateItem", lambda c: c.update_item(
            TableName=table, Key=key,
            AttributeUpdates={"ssn": {"Value": {"S": "1"}, "Action": "PUT"}})),
        "query_filter": ("Query", lambda c: c.query(
            TableName=table, KeyConditionExpression="pk = :p", FilterExpression="ssn = :v",
            ExpressionAttributeValues={":p": KEY_A["pk"], ":v": {"S": "1"}})),
        "scan_filter": ("Scan", lambda c: c.scan(
            TableName=table, FilterExpression="ssn = :v",
            ExpressionAttributeValues={":v": {"S": "1"}})),
        "scan_legacy_filter": ("Scan", lambda c: c.scan(
            TableName=table, ScanFilter={"ssn": {"ComparisonOperator": "NOT_NULL"}})),
        "condition_check": ("ConditionCheckItem", lambda c: c.transact_write_items(
            TransactItems=[{"ConditionCheck": {
                "TableName": table, "Key": KEY_A,
                "ConditionExpression": "attribute_exists(ssn)"}}])),
    }


@pytest.mark.parametrize("shape", sorted(_shapes_naming_ssn("t", {})))
def test_a_request_naming_a_denied_attribute_outside_its_item_is_denied(env: Env, shape: str):
    key = _new_key()
    seeded = {**key, "ssn": {"S": "1"}}
    env.admin.put_item(TableName=env.table, Item=seeded)
    action, call = _shapes_naming_ssn(env.table, key)[shape]
    _assert_denied(lambda: call(env.deny_ssn), action)
    assert _stored(env, key) == seeded, "a refused request changes nothing"


# -- SET values ------------------------------------------------------------

# Each SET value reads ssn, on an item that does not hold it, with the answer
# the service gave: authorized, then refused by evaluation (None means the
# update succeeds).
SET_VALUES_READING_SSN = {
    "SET #n = ssn": MISSING_ATTRIBUTE,
    "SET #n = #s": MISSING_ATTRIBUTE,
    "SET #n = if_not_exists(ssn, :d)": None,
    "SET #n = if_not_exists(#n, ssn)": MISSING_ATTRIBUTE,
    "SET #n = list_append(ssn, :l)": MISSING_ATTRIBUTE,
    "SET #n = list_append(:l, ssn)": MISSING_ATTRIBUTE,
    "SET #n = ssn + :one": MISSING_ATTRIBUTE,
    "SET #n = :one + ssn": MISSING_ATTRIBUTE,
}
_VALUES = {":d": {"S": "d"}, ":l": {"L": [{"S": "x"}]}, ":one": {"N": "1"}}


@pytest.mark.parametrize("expression", list(SET_VALUES_READING_SSN))
def test_a_set_value_reading_a_denied_attribute_is_authorized(env: Env, expression: str):
    key = _new_key()
    env.admin.put_item(TableName=env.table, Item=key)
    names = {"#n": "name"}
    if "#s" in expression:
        names["#s"] = "ssn"
    kwargs: dict = {"ExpressionAttributeNames": names}
    values = {v: _VALUES[v] for v in sorted(set(re.findall(r":\w+", expression)))}
    if values:
        kwargs["ExpressionAttributeValues"] = values
    expected = SET_VALUES_READING_SSN[expression]

    def update() -> Any:
        return env.deny_ssn.update_item(
            TableName=env.table, Key=key, UpdateExpression=expression, **kwargs
        )

    if expected is None:
        update()
        assert _stored(env, key) == {**key, "name": {"S": "d"}}
        return
    with pytest.raises(ClientError) as exc:
        update()
    err = exc.value.response["Error"]
    assert err["Code"] == "ValidationException", exc.value.response
    assert expected in err["Message"], err


def test_a_set_value_copies_a_denied_attribute_into_another_one(env: Env):
    key = _new_key()
    env.admin.put_item(TableName=env.table, Item={**key, "ssn": {"S": "999-00-1111"}})
    resp = env.deny_ssn.update_item(
        TableName=env.table,
        Key=key,
        UpdateExpression="SET #n = ssn",
        ExpressionAttributeNames={"#n": "name"},
        ReturnValues="UPDATED_NEW",
    )
    assert resp["Attributes"] == {"name": {"S": "999-00-1111"}}
    read = env.deny_ssn.get_item(
        TableName=env.table, Key=key, ProjectionExpression="#n",
        ExpressionAttributeNames={"#n": "name"},
    )
    assert read["Item"] == {"name": {"S": "999-00-1111"}}


# -- reads the key does not stop -------------------------------------------


def test_query_with_no_filter_or_projection_returns_the_whole_item(env: Env):
    resp = env.deny_ssn.query(
        TableName=env.table,
        KeyConditionExpression="pk = :p",
        ExpressionAttributeValues={":p": KEY_A["pk"]},
    )
    assert resp["Items"] == [ITEM_A]


def test_scan_from_an_exclusive_start_key_is_allowed(env: Env):
    resp = env.deny_ssn.scan(TableName=env.table, ExclusiveStartKey=KEY_A)
    assert "Items" in resp


# -- bare and Null conditions ----------------------------------------------


def test_a_bare_negated_operator_grants_no_scan_naming_no_attribute(env: Env):
    msg = _refusal(lambda: env.allow_scan_unless_ssn.scan(TableName=env.table), "Scan")
    assert "because no identity-based policy allows the dynamodb:Scan action" in msg, msg


def test_a_bare_negated_operator_grants_no_scan_naming_another_attribute(env: Env):
    msg = _refusal(
        lambda: env.allow_scan_unless_ssn.scan(
            TableName=env.table, ProjectionExpression="#n",
            ExpressionAttributeNames={"#n": "name"},
        ),
        "Scan",
    )
    assert "because no identity-based policy allows the dynamodb:Scan action" in msg, msg


def test_a_null_deny_does_not_refuse_a_get_item_with_no_projection(env: Env):
    resp = env.deny_get_when_absent.get_item(TableName=env.table, Key=KEY_A)
    assert resp["Item"] == ITEM_A


def test_a_null_deny_does_not_refuse_a_projected_get_item(env: Env):
    resp = env.deny_get_when_absent.get_item(
        TableName=env.table, Key=KEY_A, ProjectionExpression="#n",
        ExpressionAttributeNames={"#n": "name"},
    )
    assert resp["Item"] == {"name": {"S": "alice"}}


# -- the table's key attribute is in the key on GetItem ----------------------


def test_for_all_values_allow_without_the_key_attribute_refuses_a_projected_get(env: Env):
    msg = _refusal(
        lambda: env.allow_get_name.get_item(
            TableName=env.table, Key=KEY_A, ProjectionExpression="#n",
            ExpressionAttributeNames={"#n": "name"},
        ),
        "GetItem",
    )
    assert "because no identity-based policy allows the dynamodb:GetItem action" in msg, msg


def test_for_all_values_allow_with_the_key_attribute_admits_a_projected_get(env: Env):
    resp = env.allow_get_pk_name.get_item(
        TableName=env.table, Key=KEY_A, ProjectionExpression="#n",
        ExpressionAttributeNames={"#n": "name"},
    )
    assert resp["Item"] == {"name": {"S": "alice"}}


def test_for_all_values_allow_with_the_key_attribute_admits_a_get_with_no_projection(
    env: Env,
):
    resp = env.allow_get_pk_name.get_item(TableName=env.table, Key=KEY_A)
    assert resp["Item"] == ITEM_A


# -- legacy reads and transaction items --------------------------------------

KEY_CONDITIONS_A = {"pk": {"ComparisonOperator": "EQ", "AttributeValueList": [KEY_A["pk"]]}}


def test_query_legacy_filter_naming_a_denied_attribute_is_denied(env: Env):
    """A Query whose QueryFilter reads ssn is refused under deny(ssn)."""
    _assert_denied(
        lambda: env.deny_ssn.query(
            TableName=env.table, KeyConditions=KEY_CONDITIONS_A,
            QueryFilter={"ssn": {"ComparisonOperator": "NOT_NULL"}},
        ),
        "Query",
    )


def test_query_attributes_to_get_naming_a_denied_attribute_is_denied(env: Env):
    """A Query with AttributesToGet ["ssn"] is refused under deny(ssn)."""
    _assert_denied(
        lambda: env.deny_ssn.query(
            TableName=env.table, KeyConditions=KEY_CONDITIONS_A, AttributesToGet=["ssn"]
        ),
        "Query",
    )


def test_query_attributes_to_get_of_another_attribute_returns_only_it(env: Env):
    """A Query with AttributesToGet ["name"] returns name only."""
    resp = env.deny_ssn.query(
        TableName=env.table, KeyConditions=KEY_CONDITIONS_A, AttributesToGet=["name"]
    )
    assert resp["Items"] == [{"name": {"S": "alice"}}]


def test_batch_get_item_attributes_to_get_naming_a_denied_attribute_is_denied(env: Env):
    """A BatchGetItem with AttributesToGet ["ssn"] is refused under
    deny(ssn)."""
    _assert_denied(
        lambda: env.deny_ssn.batch_get_item(
            RequestItems={env.table: {"Keys": [KEY_A], "AttributesToGet": ["ssn"]}}
        ),
        "BatchGetItem",
    )


def test_a_transaction_update_setting_a_denied_attribute_is_denied(env: Env):
    """A TransactWriteItems Update that sets ssn is refused under
    deny(ssn), and the refusal names dynamodb:UpdateItem."""
    key = _new_key()
    env.admin.put_item(TableName=env.table, Item=key)
    _assert_denied(
        lambda: env.deny_ssn.transact_write_items(TransactItems=[{"Update": {
            "TableName": env.table, "Key": key, "UpdateExpression": "SET ssn = :v",
            "ExpressionAttributeValues": {":v": {"S": "1"}},
        }}]),
        "UpdateItem",
    )
    assert _stored(env, key) == key, "a refused write changes nothing"


def test_a_transaction_delete_whose_condition_reads_a_denied_attribute_is_denied(env: Env):
    """A TransactWriteItems Delete whose condition reads ssn is refused
    under deny(ssn), and the refusal names dynamodb:DeleteItem."""
    key = _new_key()
    seeded = {**key, "ssn": {"S": "1"}}
    env.admin.put_item(TableName=env.table, Item=seeded)
    _assert_denied(
        lambda: env.deny_ssn.transact_write_items(TransactItems=[{"Delete": {
            "TableName": env.table, "Key": key,
            "ConditionExpression": "attribute_not_exists(ssn)",
        }}]),
        "DeleteItem",
    )
    assert _stored(env, key) == seeded, "a refused write changes nothing"


def test_a_transaction_delete_with_no_condition_is_allowed(env: Env):
    """A TransactWriteItems Delete with no condition of an item holding
    ssn is allowed under deny(ssn)."""
    key = _new_key()
    env.admin.put_item(TableName=env.table, Item={**key, "ssn": {"S": "1"}})
    env.deny_ssn.transact_write_items(
        TransactItems=[{"Delete": {"TableName": env.table, "Key": key}}]
    )
    assert _stored(env, key) is None


# -- returned items the key does not stop ------------------------------------


def _seed_with_ssn(env: Env) -> tuple[dict, dict]:
    key = _new_key()
    seeded = {**key, "ssn": {"S": "1"}, "name": {"S": "old"}}
    env.admin.put_item(TableName=env.table, Item=seeded)
    return key, seeded


def test_return_values_all_old_on_an_update_returns_the_denied_attribute(env: Env):
    """Under deny(ssn), an UpdateItem that sets name with ReturnValues
    ALL_OLD on an item holding ssn is allowed and returns the old item, ssn
    included."""
    key, seeded = _seed_with_ssn(env)
    resp = env.deny_ssn.update_item(
        TableName=env.table, Key=key, UpdateExpression="SET #n = :v",
        ExpressionAttributeNames={"#n": "name"},
        ExpressionAttributeValues={":v": {"S": "new"}},
        ReturnValues="ALL_OLD",
    )
    assert resp["Attributes"] == seeded


def test_return_values_all_old_on_a_put_returns_the_denied_attribute(env: Env):
    """Follows the UpdateItem test above, not measured on PutItem: under
    deny(ssn), a PutItem of pk and name with ReturnValues ALL_OLD on an item
    holding ssn is allowed and returns the old item, ssn included."""
    key, seeded = _seed_with_ssn(env)
    resp = env.deny_ssn.put_item(
        TableName=env.table, Item={**key, "name": {"S": "new"}}, ReturnValues="ALL_OLD"
    )
    assert resp["Attributes"] == seeded


def test_a_failed_put_condition_returns_the_whole_stored_item(env: Env):
    """Under deny(ssn), a PutItem of pk whose condition fails, with
    ReturnValuesOnConditionCheckFailure ALL_OLD, answers
    ConditionalCheckFailedException carrying the stored item, ssn included."""
    key, seeded = _seed_with_ssn(env)
    with pytest.raises(ClientError) as exc:
        env.deny_ssn.put_item(
            TableName=env.table, Item=key, ConditionExpression="attribute_not_exists(pk)",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert exc.value.response.get("Item") == seeded, exc.value.response
    assert _stored(env, key) == seeded


def test_a_failed_condition_check_returns_the_whole_stored_item(env: Env):
    """Under deny(ssn), a TransactWriteItems ConditionCheck that fails,
    with ReturnValuesOnConditionCheckFailure ALL_OLD, answers
    TransactionCanceledException whose cancellation reason carries the stored
    item, ssn included."""
    key, seeded = _seed_with_ssn(env)
    with pytest.raises(ClientError) as exc:
        env.deny_ssn.transact_write_items(TransactItems=[{"ConditionCheck": {
            "TableName": env.table, "Key": key,
            "ConditionExpression": "attribute_not_exists(pk)",
            "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
        }}])
    assert exc.value.response["Error"]["Code"] == "TransactionCanceledException"
    reasons = exc.value.response.get("CancellationReasons")
    assert reasons and reasons[0]["Code"] == "ConditionalCheckFailed", exc.value.response
    assert reasons[0].get("Item") == seeded, reasons

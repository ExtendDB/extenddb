# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""PartiQL write statements (INSERT / UPDATE / DELETE) over ExecuteStatement.

Dual-target: identical assertions run against real DynamoDB and against an
ExtendDB fix server (selected by EXTENDDB_TEST_ENDPOINT, see conftest). Every
expected response and error Code+Message was measured on the live service; the
records are in conf-run/partiql/char-write.md.

The table is composite pk(S)/sk(S) with a GSI gsi1 on gpk(S), matching the
oracle table. Each test uses a distinct partition key for isolation on the one
shared table.
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import wait_for_active
from partiql_gsi_table import get as _gget
from partiql_gsi_table import gsi_table, item, put


@pytest.fixture(scope="session")
def dynamodb_client(endpoint_url):
    """DynamoDB client honoring EXTENDDB_TEST_PROFILE for the live-service run.

    Overrides conftest's fixture only for this module. When EXTENDDB_TEST_ENDPOINT
    is set (fix-server run) this behaves exactly like the conftest client; when
    targeting the live service, EXTENDDB_TEST_PROFILE selects the named profile
    whose credentials boto3 resolves via the broker credential_process.
    """
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    profile = os.environ.get("EXTENDDB_TEST_PROFILE") or None
    session = boto3.Session(profile_name=profile, region_name=region)
    kwargs: dict = {"service_name": "dynamodb"}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        if endpoint_url.startswith("https://"):
            kwargs["verify"] = False
            kwargs["config"] = Config(parameter_validation=True)
    return session.client(**kwargs)


@pytest.fixture(scope="module")
def wtable(dynamodb_client):
    """A composite pk(S)/sk(S) table with GSI gsi1 on gpk(S)."""
    name = f"extenddb-measure-b-pqlw-{uuid.uuid4().hex[:12]}"
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    try:
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
        except dynamodb_client.exceptions.ResourceNotFoundException:
            pass


def _exec(client, statement, **kwargs):
    return client.execute_statement(Statement=statement, **kwargs)


def _error(excinfo):
    err = excinfo.value.response["Error"]
    return err["Code"], err["Message"]


def _get(client, table, pk, sk):
    return client.get_item(
        TableName=table, Key={"pk": {"S": pk}, "sk": {"S": sk}}
    ).get("Item")


# --- INSERT ---------------------------------------------------------------


def test_insert_success_writes_full_item(dynamodb_client, wtable):
    resp = _exec(
        dynamodb_client,
        f"INSERT INTO \"{wtable}\" VALUE {{'pk':'p1','sk':'s1','a':1,'gpk':'g1'}}",
    )
    assert resp["Items"] == []
    assert _get(dynamodb_client, wtable, "p1", "s1") == {
        "a": {"N": "1"},
        "gpk": {"S": "g1"},
        "pk": {"S": "p1"},
        "sk": {"S": "s1"},
    }


def test_insert_duplicate_key_is_duplicate_item_exception(dynamodb_client, wtable):
    _exec(dynamodb_client, f"INSERT INTO \"{wtable}\" VALUE {{'pk':'dup','sk':'s1','a':1}}")
    with pytest.raises(ClientError) as excinfo:
        _exec(dynamodb_client, f"INSERT INTO \"{wtable}\" VALUE {{'pk':'dup','sk':'s1','a':2}}")
    code, message = _error(excinfo)
    assert code == "DuplicateItemException"
    assert message == "Duplicate primary key exists in table"
    # The original item is untouched.
    assert _get(dynamodb_client, wtable, "dup", "s1")["a"] == {"N": "1"}


def test_insert_missing_key_attribute_names_the_key(dynamodb_client, wtable):
    with pytest.raises(ClientError) as excinfo:
        _exec(dynamodb_client, f"INSERT INTO \"{wtable}\" VALUE {{'pk':'p2','a':1}}")
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Key attribute should be present in the item: Key sk"


def test_insert_wrong_key_type_names_the_key(dynamodb_client, wtable):
    with pytest.raises(ClientError) as excinfo:
        _exec(dynamodb_client, f"INSERT INTO \"{wtable}\" VALUE {{'pk':9,'sk':'s1'}}")
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert (
        message
        == "Key attribute's data type should match its data type in table's schema: Key pk"
    )


def test_insert_rich_literal_round_trips(dynamodb_client, wtable):
    _exec(
        dynamodb_client,
        f"INSERT INTO \"{wtable}\" VALUE {{'pk':'p3','sk':'s1','m':{{'x':1,'y':'z'}},"
        f"'l':[1,'two',true,null],'b':true,'n':null,'num':3.5}}",
    )
    assert _get(dynamodb_client, wtable, "p3", "s1") == {
        "b": {"BOOL": True},
        "num": {"N": "3.5"},
        "l": {"L": [{"N": "1"}, {"S": "two"}, {"BOOL": True}, {"NULL": True}]},
        "pk": {"S": "p3"},
        "m": {"M": {"x": {"N": "1"}, "y": {"S": "z"}}},
        "n": {"NULL": True},
        "sk": {"S": "s1"},
    }


def test_insert_string_set_via_parameter(dynamodb_client, wtable):
    _exec(
        dynamodb_client,
        f"INSERT INTO \"{wtable}\" VALUE {{'pk':?,'sk':?,'ss':?}}",
        Parameters=[{"S": "p4"}, {"S": "s1"}, {"SS": ["a", "b", "c"]}],
    )
    assert _get(dynamodb_client, wtable, "p4", "s1")["ss"] == {"SS": ["a", "b", "c"]}


def test_insert_number_set_via_parameter(dynamodb_client, wtable):
    _exec(
        dynamodb_client,
        f"INSERT INTO \"{wtable}\" VALUE {{'pk':?,'sk':?,'ns':?}}",
        Parameters=[{"S": "p7"}, {"S": "s1"}, {"NS": ["1", "2", "3"]}],
    )
    assert _get(dynamodb_client, wtable, "p7", "s1")["ns"] == {"NS": ["1", "2", "3"]}


def test_insert_binary_via_parameter(dynamodb_client, wtable):
    _exec(
        dynamodb_client,
        f"INSERT INTO \"{wtable}\" VALUE {{'pk':?,'sk':?,'bin':?}}",
        Parameters=[{"S": "p6"}, {"S": "s1"}, {"B": b"\x01\x02\x03"}],
    )
    assert _get(dynamodb_client, wtable, "p6", "s1")["bin"] == {"B": b"\x01\x02\x03"}


def test_insert_string_set_literal_stores_ss(dynamodb_client, wtable):
    _exec(
        dynamodb_client,
        f"INSERT INTO \"{wtable}\" VALUE {{'pk':'p5','sk':'s1','ss':<<'a','b'>>}}",
    )
    assert _get(dynamodb_client, wtable, "p5", "s1")["ss"] == {"SS": ["a", "b"]}


def test_insert_return_consumed_capacity(dynamodb_client, wtable):
    resp = _exec(
        dynamodb_client,
        f"INSERT INTO \"{wtable}\" VALUE {{'pk':'p11','sk':'s1'}}",
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["Items"] == []
    assert resp["ConsumedCapacity"]["TableName"] == wtable
    assert resp["ConsumedCapacity"]["CapacityUnits"] == 1.0


# --- UPDATE ---------------------------------------------------------------


def _seed(client, table, pk, extra=""):
    body = f"'pk':'{pk}','sk':'s1','a':1,'l':[1,2]"
    if extra:
        body += "," + extra
    _exec(client, f"INSERT INTO \"{table}\" VALUE {{{body}}}")


def test_update_set_literal(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "u1")
    resp = _exec(dynamodb_client, f"UPDATE \"{wtable}\" SET a = 2 WHERE pk='u1' AND sk='s1'")
    assert resp["Items"] == []
    assert _get(dynamodb_client, wtable, "u1", "s1")["a"] == {"N": "2"}


def test_update_increment(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "u2")
    _exec(dynamodb_client, f"UPDATE \"{wtable}\" SET a = a + 1 WHERE pk='u2' AND sk='s1'")
    assert _get(dynamodb_client, wtable, "u2", "s1")["a"] == {"N": "2"}


def test_update_list_append(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "u3")
    _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET l = list_append(l, [3]) WHERE pk='u3' AND sk='s1'",
    )
    assert _get(dynamodb_client, wtable, "u3", "s1")["l"] == {
        "L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]
    }


def test_update_remove(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "u4")
    _exec(dynamodb_client, f"UPDATE \"{wtable}\" REMOVE a WHERE pk='u4' AND sk='s1'")
    item = _get(dynamodb_client, wtable, "u4", "s1")
    assert "a" not in item
    assert "l" in item


def test_update_set_and_remove(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "u5")
    _exec(dynamodb_client, f"UPDATE \"{wtable}\" SET a = 9 REMOVE l WHERE pk='u5' AND sk='s1'")
    item = _get(dynamodb_client, wtable, "u5", "s1")
    assert item["a"] == {"N": "9"}
    assert "l" not in item


def test_update_missing_item_is_conditional_check_failed(dynamodb_client, wtable):
    with pytest.raises(ClientError) as excinfo:
        _exec(dynamodb_client, f"UPDATE \"{wtable}\" SET a = 5 WHERE pk='nope' AND sk='nope'")
    code, message = _error(excinfo)
    assert code == "ConditionalCheckFailedException"
    assert message == "The conditional request failed"
    assert _get(dynamodb_client, wtable, "nope", "nope") is None


def test_update_partial_key_is_full_key_error(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "u6")
    with pytest.raises(ClientError) as excinfo:
        _exec(dynamodb_client, f"UPDATE \"{wtable}\" SET a = 5 WHERE pk='u6'")
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Where clause does not contain a mandatory equality on all key attributes"


def test_update_non_key_where_is_full_key_error(dynamodb_client, wtable):
    with pytest.raises(ClientError) as excinfo:
        _exec(dynamodb_client, f"UPDATE \"{wtable}\" SET a = 5 WHERE a = 2")
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Where clause does not contain a mandatory equality on all key attributes"


def test_update_condition_met(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "u7")
    _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET a = 7 WHERE pk='u7' AND sk='s1' AND attribute_exists(a)",
    )
    assert _get(dynamodb_client, wtable, "u7", "s1")["a"] == {"N": "7"}


def test_update_condition_not_met(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "u8")
    with pytest.raises(ClientError) as excinfo:
        _exec(
            dynamodb_client,
            f"UPDATE \"{wtable}\" SET a = 7 WHERE pk='u8' AND sk='s1' AND attribute_exists(zzz)",
        )
    code, message = _error(excinfo)
    assert code == "ConditionalCheckFailedException"
    assert message == "The conditional request failed"
    assert _get(dynamodb_client, wtable, "u8", "s1")["a"] == {"N": "1"}


def test_update_returning_all_old(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "r1")
    resp = _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET a = 100 WHERE pk='r1' AND sk='s1' RETURNING ALL OLD *",
    )
    assert resp["Items"] == [
        {"a": {"N": "1"}, "l": {"L": [{"N": "1"}, {"N": "2"}]}, "pk": {"S": "r1"}, "sk": {"S": "s1"}}
    ]


def test_update_returning_modified_old(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "r2")
    resp = _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET a = 100 WHERE pk='r2' AND sk='s1' RETURNING MODIFIED OLD *",
    )
    assert resp["Items"] == [{"a": {"N": "1"}}]


def test_update_returning_all_new(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "r3")
    resp = _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET a = 100 WHERE pk='r3' AND sk='s1' RETURNING ALL NEW *",
    )
    assert resp["Items"] == [
        {"a": {"N": "100"}, "l": {"L": [{"N": "1"}, {"N": "2"}]}, "pk": {"S": "r3"}, "sk": {"S": "s1"}}
    ]


def test_update_returning_modified_new(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "r4")
    resp = _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET a = 100 WHERE pk='r4' AND sk='s1' RETURNING MODIFIED NEW *",
    )
    assert resp["Items"] == [{"a": {"N": "100"}}]


def test_update_returning_missing_item_still_fails(dynamodb_client, wtable):
    with pytest.raises(ClientError) as excinfo:
        _exec(
            dynamodb_client,
            f"UPDATE \"{wtable}\" SET a = 100 WHERE pk='missr' AND sk='s1' RETURNING ALL OLD *",
        )
    code, message = _error(excinfo)
    assert code == "ConditionalCheckFailedException"
    assert message == "The conditional request failed"


def test_update_set_key_attribute_rejected(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "kk")
    with pytest.raises(ClientError) as excinfo:
        _exec(
            dynamodb_client,
            f"UPDATE \"{wtable}\" SET pk = 'changed' WHERE pk='kk' AND sk='s1'",
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert (
        message
        == "One or more parameter values were invalid: Cannot update attribute pk. "
        "This attribute is part of the key"
    )


def test_update_return_consumed_capacity(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "ucc")
    resp = _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET a = 2 WHERE pk='ucc' AND sk='s1'",
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["Items"] == []
    assert resp["ConsumedCapacity"]["TableName"] == wtable
    assert resp["ConsumedCapacity"]["CapacityUnits"] == 1.0


# --- DELETE ---------------------------------------------------------------


def test_delete_success(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "d1")
    resp = _exec(dynamodb_client, f"DELETE FROM \"{wtable}\" WHERE pk='d1' AND sk='s1'")
    assert resp["Items"] == []
    assert _get(dynamodb_client, wtable, "d1", "s1") is None


def test_delete_missing_item_is_silent_success(dynamodb_client, wtable):
    resp = _exec(dynamodb_client, f"DELETE FROM \"{wtable}\" WHERE pk='ghost' AND sk='s1'")
    assert resp["Items"] == []


def test_delete_returning_all_old(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "d2")
    resp = _exec(
        dynamodb_client,
        f"DELETE FROM \"{wtable}\" WHERE pk='d2' AND sk='s1' RETURNING ALL OLD *",
    )
    assert resp["Items"] == [
        {"a": {"N": "1"}, "l": {"L": [{"N": "1"}, {"N": "2"}]}, "pk": {"S": "d2"}, "sk": {"S": "s1"}}
    ]


def test_delete_returning_missing_is_empty(dynamodb_client, wtable):
    resp = _exec(
        dynamodb_client,
        f"DELETE FROM \"{wtable}\" WHERE pk='ghost2' AND sk='s1' RETURNING ALL OLD *",
    )
    assert resp["Items"] == []


def test_delete_returning_all_new_rejected(dynamodb_client, wtable):
    with pytest.raises(ClientError) as excinfo:
        _exec(
            dynamodb_client,
            f"DELETE FROM \"{wtable}\" WHERE pk='d3' AND sk='s1' RETURNING ALL NEW *",
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert (
        message
        == "Invalid returning clause: RETURNING ALL NEW *. "
        "Only RETURNING ALL OLD * is allowed in DELETE statements."
    )


def test_delete_condition_met(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "d4")
    resp = _exec(
        dynamodb_client,
        f"DELETE FROM \"{wtable}\" WHERE pk='d4' AND sk='s1' AND attribute_exists(a)",
    )
    assert resp["Items"] == []
    assert _get(dynamodb_client, wtable, "d4", "s1") is None


def test_delete_condition_not_met(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "d5")
    with pytest.raises(ClientError) as excinfo:
        _exec(
            dynamodb_client,
            f"DELETE FROM \"{wtable}\" WHERE pk='d5' AND sk='s1' AND attribute_exists(zzz)",
        )
    code, message = _error(excinfo)
    assert code == "ConditionalCheckFailedException"
    assert message == "The conditional request failed"
    assert _get(dynamodb_client, wtable, "d5", "s1") is not None


def test_delete_partial_key_is_full_key_error(dynamodb_client, wtable):
    with pytest.raises(ClientError) as excinfo:
        _exec(dynamodb_client, f"DELETE FROM \"{wtable}\" WHERE pk='d1'")
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Where clause does not contain a mandatory equality on all key attributes"


def test_delete_return_consumed_capacity(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "d6")
    resp = _exec(
        dynamodb_client,
        f"DELETE FROM \"{wtable}\" WHERE pk='d6' AND sk='s1'",
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["Items"] == []
    assert resp["ConsumedCapacity"]["TableName"] == wtable
    assert resp["ConsumedCapacity"]["CapacityUnits"] == 1.0


def test_delete_condfail_returnvalues_all_old_puts_item_on_error(dynamodb_client, wtable):
    _seed(dynamodb_client, wtable, "rv")
    with pytest.raises(ClientError) as excinfo:
        _exec(
            dynamodb_client,
            f"DELETE FROM \"{wtable}\" WHERE pk='rv' AND sk='s1' AND attribute_exists(zzz)",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",
        )
    response = excinfo.value.response
    assert response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert response["Error"]["Message"] == "The conditional request failed"
    assert response["Item"] == {
        "a": {"N": "1"},
        "l": {"L": [{"N": "1"}, {"N": "2"}]},
        "pk": {"S": "rv"},
        "sk": {"S": "s1"},
    }


# --- RETURNING MODIFIED over several list indices ---------------------------
# MODIFIED projects each path the statement names against the old or the new
# item. Elements picked by index come back packed densely in ascending index
# order, whatever order the SET clauses name them in (measured 2026-11-02).

_LIST_SETS = {
    "ascending": "l[0] = 'x', l[2] = 'y'",
    "descending": "l[2] = 'y', l[0] = 'x'",
}


def _seed_list(client, table, pk):
    _exec(client, f"INSERT INTO \"{table}\" VALUE {{'pk':'{pk}','sk':'s1','l':['a','b','c']}}")


@pytest.mark.parametrize("order", sorted(_LIST_SETS))
def test_update_returning_modified_new_packs_list_indices_in_index_order(
    dynamodb_client, wtable, order
):
    pk = f"rl-new-{order}"
    _seed_list(dynamodb_client, wtable, pk)
    resp = _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET {_LIST_SETS[order]} WHERE pk='{pk}' AND sk='s1' "
        "RETURNING MODIFIED NEW *",
    )
    assert resp["Items"] == [{"l": {"L": [{"S": "x"}, {"S": "y"}]}}]
    assert _get(dynamodb_client, wtable, pk, "s1")["l"] == {
        "L": [{"S": "x"}, {"S": "b"}, {"S": "y"}]
    }


@pytest.mark.parametrize("order", sorted(_LIST_SETS))
def test_update_returning_modified_old_packs_list_indices_in_index_order(
    dynamodb_client, wtable, order
):
    pk = f"rl-old-{order}"
    _seed_list(dynamodb_client, wtable, pk)
    resp = _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET {_LIST_SETS[order]} WHERE pk='{pk}' AND sk='s1' "
        "RETURNING MODIFIED OLD *",
    )
    assert resp["Items"] == [{"l": {"L": [{"S": "a"}, {"S": "c"}]}}]


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        # l[2] no longer resolves on the new two-element list, so NEW keeps l[0] only.
        ("NEW", [{"S": "x"}]),
        ("OLD", [{"S": "a"}, {"S": "c"}]),
    ],
)
def test_update_returning_modified_projects_set_and_remove_list_paths(
    dynamodb_client, wtable, mode, expected
):
    pk = f"rl-setrem-{mode.lower()}"
    _seed_list(dynamodb_client, wtable, pk)
    resp = _exec(
        dynamodb_client,
        f"UPDATE \"{wtable}\" SET l[0] = 'x' REMOVE l[2] WHERE pk='{pk}' AND sk='s1' "
        f"RETURNING MODIFIED {mode} *",
    )
    assert resp["Items"] == [{"l": {"L": expected}}]
    assert _get(dynamodb_client, wtable, pk, "s1")["l"] == {"L": [{"S": "x"}, {"S": "b"}]}


# --- DELETE on a missing key with a non-key predicate -----------------------
# The predicate is a condition on an item that exists. A missing key is a
# silent no-op, not ConditionalCheckFailedException (measured 2026-11-02).


@pytest.mark.parametrize("predicate", ["a = 1", "attribute_exists(zzz)"])
def test_delete_missing_key_with_non_key_predicate_is_silent_success(
    dynamodb_client, wtable, predicate
):
    resp = _exec(
        dynamodb_client,
        f"DELETE FROM \"{wtable}\" WHERE pk='ghost-pred' AND sk='s1' AND {predicate}",
    )
    assert resp["Items"] == []
    assert _get(dynamodb_client, wtable, "ghost-pred", "s1") is None


def test_delete_missing_key_with_predicate_returning_all_old_is_empty(dynamodb_client, wtable):
    resp = _exec(
        dynamodb_client,
        f"DELETE FROM \"{wtable}\" WHERE pk='ghost-pred2' AND sk='s1' AND a = 1 "
        "RETURNING ALL OLD *",
    )
    assert resp["Items"] == []


# --- calls in a SET value ----------------------------------------------------
# A SET value may call if_not_exists and list_append, each with two operands
# and if_not_exists with a path first; set_add and set_delete are the whole
# value or not a function. Any other call is refused by what it is, after the
# call's arguments and in the order the values are written, ahead of every
# check against the table and after the operand types of the condition
# (measured 2026-11-06 in us-east-1).

NOT_ALLOWED = "The function is not allowed in an update expression; function: "


@pytest.fixture(scope="module")
def gtable(dynamodb_client):
    """The table with the index `gsi` (see partiql_gsi_table)."""
    with gsi_table(dynamodb_client, "extenddb-measure-2c-writeg") as name:
        yield name


def _set(table, value, pk, where=""):
    return f"UPDATE \"{table}\" SET a = {value} WHERE pk = '{pk}' AND sk = 'b'{where}"


def _call_at(statement, written, paren=False):
    """The `line:column:length` of the call written `written` in a statement
    on one line, or of the parenthesis after its name."""
    column = statement.index(f"{written}(") + 1
    if paren:
        return f"1:{column + len(written)}:1"
    return f"1:{column}:{len(written)}"


def _refusal(client, statement, params=None):
    kwargs = {} if params is None else {"Parameters": params}
    with pytest.raises(ClientError) as excinfo:
        client.execute_statement(Statement=statement, **kwargs)
    return _error(excinfo)


def _unrecognized(written):
    return lambda s: f"Unrecognized function: {written.lower()} at {_call_at(s, written)}"


def _operands(name, count):
    return (
        "Incorrect number of operands for operator or function; operator or function: "
        f"{name}, number of operands: {count}"
    )


SET_CALLS = [
    ("attribute_exists(m)", NOT_ALLOWED + "attribute_exists"),
    ("attribute_not_exists(m)", NOT_ALLOWED + "attribute_not_exists"),
    ("attribute_type(m, 'M')", NOT_ALLOWED + "attribute_type"),
    ("begins_with(m, 'x')", NOT_ALLOWED + "begins_with"),
    ("contains(m, 'x')", NOT_ALLOWED + "contains"),
    ("size(m)", NOT_ALLOWED + "size"),
    ("SIZE(m)", NOT_ALLOWED + "size"),
    ("size(m, 1)", NOT_ALLOWED + "size"),
    ("size()", NOT_ALLOWED + "size"),
    ("attribute_exists('x')", NOT_ALLOWED + "attribute_exists"),
    ("size(m) + 1", NOT_ALLOWED + "size"),
    ("1 + size(m)", NOT_ALLOWED + "size"),
    ("(size(m))", NOT_ALLOWED + "size"),
    ("if_not_exists(a, size(m))", NOT_ALLOWED + "size"),
    ("if_not_exists(size(m))", NOT_ALLOWED + "size"),
    ("upper(size(m))", NOT_ALLOWED + "size"),
    ("size(m), b = upper(m)", NOT_ALLOWED + "size"),
    ("upper(m)", _unrecognized("upper")),
    ("UPPER(m)", _unrecognized("UPPER")),
    ("lower(m)", _unrecognized("lower")),
    ("exists(m)", _unrecognized("exists")),
    ("char_length(m)", _unrecognized("char_length")),
    ("substring(m, 1)", _unrecognized("substring")),
    ("trim(m)", _unrecognized("trim")),
    ("foo(m)", _unrecognized("foo")),
    ("upper()", _unrecognized("upper")),
    ("(upper(m))", _unrecognized("upper")),
    ("size(upper(m))", _unrecognized("upper")),
    ("if_not_exists(a, upper(m))", _unrecognized("upper")),
    ("if_not_exists(upper(m))", _unrecognized("upper")),
    ("upper(m), b = size(m)", _unrecognized("upper")),
    ("upper(m), b = lower(m)", _unrecognized("upper")),
    ("if_not_exists(a, set_add(ss, <<'b'>>))", _unrecognized("set_add")),
    (
        "set_add(m)",
        lambda s: f"SET_ADD must have exactly two arguments at {_call_at(s, 'set_add')}",
    ),
    (
        "set_delete(m)",
        lambda s: f"SET_DELETE must have exactly two arguments at {_call_at(s, 'set_delete')}",
    ),
    (
        "missing(m)",
        "Statement wasn't well formed, can't be processed: Unexpected token after expression",
    ),
    ("count(m)", lambda s: f"Unsupported token in expression: count at {_call_at(s, 'count')}"),
    ("COUNT(m)", lambda s: f"Unsupported token in expression: count at {_call_at(s, 'COUNT')}"),
    ("sum(m)", lambda s: f"Unsupported token in expression: sum at {_call_at(s, 'sum')}"),
    ("avg(m)", lambda s: f"Unsupported token in expression: avg at {_call_at(s, 'avg')}"),
    ("min(m)", lambda s: f"Unsupported token in expression: min at {_call_at(s, 'min')}"),
    ("max(m)", lambda s: f"Unsupported token in expression: max at {_call_at(s, 'max')}"),
    (
        "nullif(1, m)",
        lambda s: f"Unsupported token in expression: UNKNOWN at {_call_at(s, 'nullif')}",
    ),
    (
        "coalesce(m, 1)",
        lambda s: f"Unsupported token in expression: UNKNOWN at {_call_at(s, 'coalesce', True)}",
    ),
    ("if_not_exists(m)", _operands("if_not_exists", 1)),
    ("if_not_exists(1)", _operands("if_not_exists", 1)),
    ("if_not_exists(a, 1, 2)", _operands("if_not_exists", 3)),
    ("if_not_exists()", _operands("if_not_exists", 0)),
    ("list_append(l)", _operands("list_append", 1)),
    ("list_append(l, [1], [2])", _operands("list_append", 3)),
    ("list_append()", _operands("list_append", 0)),
    (
        "if_not_exists(1, a)",
        "Operator or function requires a document path; operator or function: if_not_exists",
    ),
    (
        "if_not_exists(1, 2)",
        "Operator or function requires a document path; operator or function: if_not_exists",
    ),
]


@pytest.mark.parametrize("value, message", SET_CALLS, ids=[v for v, _ in SET_CALLS])
def test_a_set_value_calls_only_the_update_functions(dynamodb_client, gtable, value, message):
    pk = put(dynamodb_client, gtable, f"call-{uuid.uuid4().hex[:6]}")
    statement = _set(gtable, value, pk)
    expected = message(statement) if callable(message) else message
    assert _refusal(dynamodb_client, statement) == ("ValidationException", expected)
    assert _gget(dynamodb_client, gtable, pk) == item(pk)


@pytest.mark.parametrize(
    "value, stored",
    [
        ("if_not_exists(a, 7)", {"N": "1"}),
        ("if_not_exists(q, 7)", {"N": "7"}),
        ("list_append(l, [3])", {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]}),
        ("list_append([0], l)", {"L": [{"N": "0"}, {"N": "1"}, {"N": "2"}]}),
        ("if_not_exists(a, if_not_exists(b, 7))", {"N": "1"}),
        ("list_append(if_not_exists(l, []), [3])", {"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]}),
        ("(a + 1) + 1", {"N": "3"}),
        ("1 + (a + 1)", {"N": "3"}),
        ("a - (1 - 1)", {"N": "1"}),
        ("(a + 1) - (a - 1)", {"N": "2"}),
        ("((a + 1) + 1) + 1", {"N": "4"}),
        ("(if_not_exists(a, 0) + 1) + 1", {"N": "3"}),
        ("if_not_exists(q, (a + 1) + 1)", {"N": "3"}),
        ("if_not_exists(a, a + 5)", {"N": "1"}),
        ("if_not_exists(a, 0) + if_not_exists(b, 0)", {"N": "3"}),
    ],
)
def test_the_update_functions_and_grouped_arithmetic_run(dynamodb_client, gtable, value, stored):
    pk = put(dynamodb_client, gtable, f"run-{uuid.uuid4().hex[:6]}", l={"L": [{"N": "1"}, {"N": "2"}]})
    assert _exec(dynamodb_client, _set(gtable, value, pk))["Items"] == []
    assert _gget(dynamodb_client, gtable, pk)["a"] == stored


def test_grouped_arithmetic_takes_a_parameter_and_meets_a_missing_attribute(
    dynamodb_client, gtable
):
    pk = put(dynamodb_client, gtable, f"grp-{uuid.uuid4().hex[:6]}")
    _exec(dynamodb_client, _set(gtable, "(a + ?) + 1", pk), Parameters=[{"N": "5"}])
    assert _gget(dynamodb_client, gtable, pk)["a"] == {"N": "7"}
    assert _refusal(dynamodb_client, _set(gtable, "(q + 1) + 1", pk)) == (
        "ValidationException",
        "The provided expression refers to an attribute that does not exist in the item",
    )


OPERAND_TYPE = "Incorrect operand type for operator or function; operator or function: <, operand type: BOOL"
OVERFLOW = (
    "Number overflow. Attempting to store a number with magnitude larger than supported range "
    "under root"
)


@pytest.mark.parametrize(
    "template, message",
    [
        ("UPDATE {t} SET b = 1e126, a = size(m) WHERE pk = '{k}' AND sk = 'b'", OVERFLOW),
        ("UPDATE {t} SET a = size(m), b = 1e126 WHERE pk = '{k}' AND sk = 'b'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET a = size(m), a = 1 WHERE pk = '{k}' AND sk = 'b'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET a = 1, a = size(m) WHERE pk = '{k}' AND sk = 'b'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET a = size(m), b = ? WHERE pk = '{k}' AND sk = 'b'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET pk = 'z', a = size(m) WHERE pk = '{k}' AND sk = 'b'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET a = size(m), pk = 'z' WHERE pk = '{k}' AND sk = 'b'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET g = 1, a = size(m) WHERE pk = '{k}' AND sk = 'b'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET a = size(m) REMOVE sk WHERE pk = '{k}' AND sk = 'b'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET a = size(m) WHERE pk = '{k}'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET a = size(m) WHERE pk = 1 AND sk = 'b'", NOT_ALLOWED + "size"),
        ("UPDATE {t} SET a = size(m) WHERE pk = 'nope-{k}' AND sk = 'b'", NOT_ALLOWED + "size"),
        (
            "UPDATE {t} SET a = size(m) WHERE pk = '{k}' AND sk = 'b' AND pk.c = 1",
            NOT_ALLOWED + "size",
        ),
        (
            "UPDATE {t} SET a = size(m) WHERE pk = '{k}' AND sk = 'b' RETURNING ALL NEW *",
            NOT_ALLOWED + "size",
        ),
        (
            "UPDATE \"nosuch_tbl_r3\" SET a = size(m) WHERE pk = '{k}' AND sk = 'b'",
            NOT_ALLOWED + "size",
        ),
        ("UPDATE {t} SET a = size(m) WHERE pk = '{k}' AND sk = 'b' AND x < true", OPERAND_TYPE),
        ("UPDATE {t} SET b = 1e126 WHERE pk = '{k}' AND sk = 'b' AND x < true", OPERAND_TYPE),
        (
            "UPDATE {t} SET a = upper(m) WHERE pk = '{k}' AND sk = 'b' AND lower(x) = 1",
            lambda s: f"Unrecognized function: lower at {_call_at(s, 'lower')}",
        ),
        (
            "UPDATE {t} SET a = size(m) WHERE pk = '{k}' AND sk = 'b' AND a + 1 = 2",
            "Unsupported operator in Condition Expression. Operator: +",
        ),
        ("UPDATE {t} AS x SET a = size(m) WHERE pk = '{k}' AND sk = 'b'", "Aliasing is not supported"),
        (
            "UPDATE \"ab\" SET a = upper(m) WHERE pk = '{k}' AND sk = 'b'",
            "Table name should be within the length [3, 255] and only contain 'a-z', 'A-Z', "
            "'0-9', '-', '_', '.'.",
        ),
    ],
)
def test_a_call_is_refused_in_its_place_among_the_checks(
    dynamodb_client, gtable, template, message
):
    pk = put(dynamodb_client, gtable, f"ord-{uuid.uuid4().hex[:6]}")
    statement = template.format(t=f'"{gtable}"', k=pk)
    expected = message(statement) if callable(message) else message
    assert _refusal(dynamodb_client, statement) == ("ValidationException", expected)
    assert _gget(dynamodb_client, gtable, pk) == item(pk)


@pytest.mark.parametrize(
    "template",
    [
        "UPDATE {t}\n  SET a = upper(m)\n  WHERE pk = '{k}' AND sk = 'b'",
        "FROM {t} WHERE pk = '{k}' AND sk = 'b' SET a = upper(m)",
        "UPDATE {t} SET x = 1 SET a = upper(m) WHERE pk = '{k}' AND sk = 'b'",
        "UPDATE {t} SET a = upper(m) REMOVE x WHERE pk = '{k}' AND sk = 'b'",
        "UPDATE {t} SET ss = set_add(ss, <<'b'>>), a = if_not_exists(a, upper(m)) "
        "WHERE pk = '{k}' AND sk = 'b'",
    ],
    ids=["lines", "from-first", "two-sets", "set-and-remove", "after-set-add"],
)
def test_an_unknown_call_is_named_where_it_is_written(dynamodb_client, gtable, template):
    pk = put(dynamodb_client, gtable, f"pos-{uuid.uuid4().hex[:6]}")
    statement = template.format(t=f'"{gtable}"', k=pk)
    line_index = next(i for i, line in enumerate(statement.split("\n")) if "upper(" in line)
    column = statement.split("\n")[line_index].index("upper(") + 1
    assert _refusal(dynamodb_client, statement) == (
        "ValidationException",
        f"Unrecognized function: upper at {line_index + 1}:{column}:5",
    )

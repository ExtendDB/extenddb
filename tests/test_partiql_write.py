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

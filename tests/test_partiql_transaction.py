# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""PartiQL ExecuteTransaction over the wire.

Dual-target: identical assertions run against real DynamoDB and against an
ExtendDB fix server (selected by EXTENDDB_TEST_ENDPOINT, see conftest). Every
expected response and error Code+Message was measured on the live service; the
records are in conf-run/partiql/char-batch-txn.md (Part 2).

A transaction is all reads or all writes, all-or-nothing. Reads behave like
TransactGetItems (Responses[i] = {Item}, no TableName); any write failure
cancels with a positional CancellationReasons list. ClientRequestToken gives
idempotency. The one shared composite pk(S)/sk(S) table is reused with distinct
keys per test for isolation.
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
    """DynamoDB client honoring EXTENDDB_TEST_PROFILE for the live-service run."""
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
def ttable(dynamodb_client):
    """A composite pk(S)/sk(S) table, PAY_PER_REQUEST."""
    name = f"extenddb-measure-2c-txn-{uuid.uuid4().hex[:12]}"
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
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


def _txn(client, statements, **kwargs):
    return client.execute_transaction(TransactStatements=statements, **kwargs)


def _error(excinfo):
    err = excinfo.value.response["Error"]
    return err["Code"], err["Message"]


def _seed(client, table, pk, sk="s1", a=1):
    client.put_item(
        TableName=table,
        Item={"pk": {"S": pk}, "sk": {"S": sk}, "a": {"N": str(a)}},
    )


def _get(client, table, pk, sk="s1"):
    return client.get_item(
        TableName=table, Key={"pk": {"S": pk}, "sk": {"S": sk}}
    ).get("Item")


# --- T1 / T2 / T2b: success shapes ----------------------------------------


def test_all_writes_returns_empty_responses(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "t1u", a=1)
    _seed(dynamodb_client, ttable, "t1d", a=1)
    resp = _txn(
        dynamodb_client,
        [
            {"Statement": f"INSERT INTO \"{ttable}\" VALUE {{'pk':'t1i','sk':'s1','a':1}}"},
            {"Statement": f"UPDATE \"{ttable}\" SET a=2 WHERE pk='t1u' AND sk='s1'"},
            {"Statement": f"DELETE FROM \"{ttable}\" WHERE pk='t1d' AND sk='s1'"},
        ],
    )
    assert resp["Responses"] == []
    assert _get(dynamodb_client, ttable, "t1i")["a"] == {"N": "1"}
    assert _get(dynamodb_client, ttable, "t1u")["a"] == {"N": "2"}
    assert _get(dynamodb_client, ttable, "t1d") is None


def test_all_reads_items_no_table_name(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "t2a", a=99)
    _seed(dynamodb_client, ttable, "t2b", a=2)
    resp = _txn(
        dynamodb_client,
        [
            {"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='t2a' AND sk='s1'"},
            {"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='t2b' AND sk='s1'"},
        ],
    )
    assert resp["Responses"] == [
        {"Item": {"pk": {"S": "t2a"}, "sk": {"S": "s1"}, "a": {"N": "99"}}},
        {"Item": {"pk": {"S": "t2b"}, "sk": {"S": "s1"}, "a": {"N": "2"}}},
    ]


def test_read_absent_is_empty_object(dynamodb_client, ttable):
    resp = _txn(
        dynamodb_client,
        [{"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='ghost' AND sk='s1'"}],
    )
    assert resp["Responses"] == [{}]


# --- T2c: partial-key read (request-level) --------------------------------


def test_read_partial_key_rejects_whole_request(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "t2c")
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [{"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='t2c'"}],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        "Validation failed in TransactStatements[0]: Select statements within "
        "ExecuteTransaction must specify the primary key in the where clause."
    )


# --- T3: read/write mix (request-level) -----------------------------------


def test_mixed_read_and_write_rejects(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "t3")
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='t3' AND sk='s1'"},
                {"Statement": f"UPDATE \"{ttable}\" SET a=1 WHERE pk='t3' AND sk='s1'"},
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        "ExecuteTransaction API does not support both read and write operations in the same request."
    )


# --- T4 / T4b: all-or-nothing via a failing condition ---------------------


def test_cancel_on_failed_condition_is_positional(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "t4a", a=99)
    _seed(dynamodb_client, ttable, "t4b", a=2)
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {"Statement": f"UPDATE \"{ttable}\" SET a=5 WHERE pk='t4a' AND sk='s1'"},
                {
                    "Statement": f"UPDATE \"{ttable}\" SET a=5 WHERE pk='t4b' AND sk='s1' AND attribute_exists(zzz)"
                },
            ],
        )
    response = excinfo.value.response
    assert response["Error"]["Code"] == "TransactionCanceledException"
    assert response["Error"]["Message"] == (
        "Transaction cancelled, please refer cancellation reasons for specific reasons "
        "[None, ConditionalCheckFailed]"
    )
    assert response["CancellationReasons"] == [
        {"Code": "None"},
        {"Code": "ConditionalCheckFailed", "Message": "The conditional request failed"},
    ]
    # Nothing committed: the good UPDATE on t4a did not apply.
    assert _get(dynamodb_client, ttable, "t4a")["a"] == {"N": "99"}


# --- T4c: cancel via duplicate-key INSERT ---------------------------------


def test_cancel_on_duplicate_insert(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "t4exist", a=1)
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {"Statement": f"INSERT INTO \"{ttable}\" VALUE {{'pk':'t4new','sk':'s1','a':1}}"},
                {"Statement": f"INSERT INTO \"{ttable}\" VALUE {{'pk':'t4exist','sk':'s1','a':2}}"},
            ],
        )
    response = excinfo.value.response
    assert response["Error"]["Code"] == "TransactionCanceledException"
    assert response["Error"]["Message"] == (
        "Transaction cancelled, please refer cancellation reasons for specific reasons "
        "[None, DuplicateItem]"
    )
    assert response["CancellationReasons"] == [
        {"Code": "None"},
        {"Code": "DuplicateItem", "Message": "Duplicate primary key exists in table"},
    ]
    # The good INSERT did not commit (all-or-nothing).
    assert _get(dynamodb_client, ttable, "t4new") is None


# --- T4d: ReturnValuesOnConditionCheckFailure on a cancelled statement -----


def test_cancel_returns_item_on_all_old(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "t4d", a=2)
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {
                    "Statement": f"UPDATE \"{ttable}\" SET a=5 WHERE pk='t4d' AND sk='s1' AND a=12345",
                    "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
                }
            ],
        )
    reasons = excinfo.value.response["CancellationReasons"]
    assert reasons == [
        {
            "Item": {"pk": {"S": "t4d"}, "sk": {"S": "s1"}, "a": {"N": "2"}},
            "Code": "ConditionalCheckFailed",
            "Message": "The conditional request failed",
        }
    ]


# --- T5: ClientRequestToken idempotency -----------------------------------


def test_idempotent_replay_and_mismatch(dynamodb_client, ttable):
    token = uuid.uuid4().hex
    statements = [
        {"Statement": f"INSERT INTO \"{ttable}\" VALUE {{'pk':'t5','sk':'s1','a':1}}"}
    ]
    first = _txn(dynamodb_client, statements, ClientRequestToken=token)
    assert first["Responses"] == []
    # Same token + same body: idempotent replay, no DuplicateItem though the key
    # now exists.
    replay = _txn(dynamodb_client, statements, ClientRequestToken=token)
    assert replay["Responses"] == []
    # Same token + different body: mismatch.
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [{"Statement": f"INSERT INTO \"{ttable}\" VALUE {{'pk':'t5x','sk':'s1','a':9}}"}],
            ClientRequestToken=token,
        )
    code, message = _error(excinfo)
    assert code == "IdempotentParameterMismatchException"
    assert message == (
        "Specified idempotent token was used with different request parameters within the idempotency window"
    )


# --- T6: conflicting keys (request-level) ---------------------------------


def test_conflicting_keys_reject(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "t6")
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {"Statement": f"UPDATE \"{ttable}\" SET a=1 WHERE pk='t6' AND sk='s1'"},
                {"Statement": f"UPDATE \"{ttable}\" SET a=2 WHERE pk='t6' AND sk='s1'"},
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Transaction request cannot include multiple operations on one item"


# --- T7: statement-count limit --------------------------------------------


def test_one_hundred_statements_ok(dynamodb_client, ttable):
    statements = [
        {"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='n{i}' AND sk='s1'"}
        for i in range(100)
    ]
    resp = _txn(dynamodb_client, statements)
    assert len(resp["Responses"]) == 100


def test_one_hundred_one_statements_rejected(dynamodb_client, ttable):
    statements = [
        {"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='n{i}' AND sk='s1'"}
        for i in range(101)
    ]
    with pytest.raises(ClientError) as excinfo:
        _txn(dynamodb_client, statements)
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message.endswith("Member must have length less than or equal to 100")


# --- T8: capacity (a list, 2.0 RCU transactional read) --------------------


def test_transactional_read_capacity_is_two_rcu_list(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "t8", a=1)
    resp = _txn(
        dynamodb_client,
        [{"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='t8' AND sk='s1'"}],
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["ConsumedCapacity"] == [
        {"TableName": ttable, "CapacityUnits": 2.0, "ReadCapacityUnits": 2.0}
    ]

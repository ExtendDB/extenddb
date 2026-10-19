# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""PartiQL BatchExecuteStatement over the wire.

Dual-target: identical assertions run against real DynamoDB and against an
ExtendDB fix server (selected by EXTENDDB_TEST_ENDPOINT, see conftest). Every
expected response and error Code+Message was measured on the live service; the
records are in conf-run/partiql/char-batch-txn.md (Part 1).

A batch is all reads or all writes. Reads behave like BatchGetItem
(Responses[i] = {TableName, Item}); writes carry per-statement Error entries and
real partial success. The one shared composite pk(S)/sk(S) table is reused with
a distinct partition key per test for isolation.
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
def btable(dynamodb_client):
    """A composite pk(S)/sk(S) table, PAY_PER_REQUEST."""
    name = f"extenddb-measure-2c-batch-{uuid.uuid4().hex[:12]}"
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


def _batch(client, statements, **kwargs):
    return client.batch_execute_statement(Statements=statements, **kwargs)


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


# --- B1 / B2: reads --------------------------------------------------------


def test_all_reads_success_shape(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "p1", a=1)
    _seed(dynamodb_client, btable, "p2", a=2)
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='p1' AND sk='s1'"},
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='p2' AND sk='s1'"},
        ],
    )
    assert resp["Responses"] == [
        {"TableName": btable, "Item": {"pk": {"S": "p1"}, "sk": {"S": "s1"}, "a": {"N": "1"}}},
        {"TableName": btable, "Item": {"pk": {"S": "p2"}, "sk": {"S": "s1"}, "a": {"N": "2"}}},
    ]


def test_read_absent_key_omits_item(dynamodb_client, btable):
    resp = _batch(
        dynamodb_client,
        [{"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='nope' AND sk='nope'"}],
    )
    assert resp["Responses"] == [{"TableName": btable}]


def test_read_partial_key_is_per_statement_validation_error(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "pp")
    resp = _batch(
        dynamodb_client,
        [{"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='pp'"}],
    )
    assert resp["Responses"] == [
        {
            "Error": {
                "Code": "ValidationError",
                "Message": "Select statements within BatchExecuteStatement must specify the primary key in the where clause.",
            }
        }
    ]


# --- B4 / B6: writes -------------------------------------------------------


def test_all_writes_insert_success_shape(dynamodb_client, btable):
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'w1','sk':'s1','a':10}}"},
            {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'w2','sk':'s1','a':20}}"},
        ],
    )
    assert resp["Responses"] == [{"TableName": btable}, {"TableName": btable}]
    assert _get(dynamodb_client, btable, "w1")["a"] == {"N": "10"}
    assert _get(dynamodb_client, btable, "w2")["a"] == {"N": "20"}


def test_partial_success_good_insert_and_duplicate(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "exist", a=1)
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'fresh','sk':'s1','a':1}}"},
            {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'exist','sk':'s1','a':999}}"},
        ],
    )
    assert resp["Responses"] == [
        {"TableName": btable},
        {
            "Error": {"Code": "DuplicateItem", "Message": "Duplicate primary key exists in table"},
            "TableName": btable,
        },
    ]
    # The first INSERT committed; the duplicate left the existing item untouched.
    assert _get(dynamodb_client, btable, "fresh")["a"] == {"N": "1"}
    assert _get(dynamodb_client, btable, "exist")["a"] == {"N": "1"}


# --- B5: read/write mix (request-level) -----------------------------------


def test_mixed_read_and_write_rejects_whole_request(dynamodb_client, btable):
    with pytest.raises(ClientError) as excinfo:
        _batch(
            dynamodb_client,
            [
                {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='p1' AND sk='s1'"},
                {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'x','sk':'s1'}}"},
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Read and write requests together in the same batch is not supported."


# --- B7: duplicate key in one batch (request-level) -----------------------


def test_duplicate_key_two_updates_rejects(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "dupu")
    with pytest.raises(ClientError) as excinfo:
        _batch(
            dynamodb_client,
            [
                {"Statement": f"UPDATE \"{btable}\" SET a=1 WHERE pk='dupu' AND sk='s1'"},
                {"Statement": f"UPDATE \"{btable}\" SET a=2 WHERE pk='dupu' AND sk='s1'"},
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Provided list of item keys contains duplicates"


def test_duplicate_key_insert_and_delete_rejects(dynamodb_client, btable):
    with pytest.raises(ClientError) as excinfo:
        _batch(
            dynamodb_client,
            [
                {"Statement": f"INSERT INTO \"{btable}\" VALUE {{'pk':'dupd','sk':'s1'}}"},
                {"Statement": f"DELETE FROM \"{btable}\" WHERE pk='dupd' AND sk='s1'"},
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == "Provided list of item keys contains duplicates"


# --- B8: ReturnConsumedCapacity (a list) ----------------------------------


def test_consumed_capacity_reads_indexes_is_a_list(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "c1")
    _seed(dynamodb_client, btable, "c2")
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='c1' AND sk='s1'"},
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='c2' AND sk='s1'"},
        ],
        ReturnConsumedCapacity="INDEXES",
    )
    assert resp["ConsumedCapacity"] == [
        {"TableName": btable, "CapacityUnits": 1.0, "Table": {"CapacityUnits": 1.0}}
    ]


def test_consumed_capacity_write_total_is_a_list(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "cw")
    resp = _batch(
        dynamodb_client,
        [{"Statement": f"UPDATE \"{btable}\" SET a=2 WHERE pk='cw' AND sk='s1'"}],
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["ConsumedCapacity"] == [{"TableName": btable, "CapacityUnits": 1.0}]


# --- B9: statement-count limit --------------------------------------------


def test_twenty_five_statements_ok(dynamodb_client, btable):
    statements = [
        {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='n{i}' AND sk='s1'"}
        for i in range(25)
    ]
    resp = _batch(dynamodb_client, statements)
    assert len(resp["Responses"]) == 25


def test_twenty_six_statements_rejected(dynamodb_client, btable):
    statements = [
        {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='n{i}' AND sk='s1'"}
        for i in range(26)
    ]
    with pytest.raises(ClientError) as excinfo:
        _batch(dynamodb_client, statements)
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message.endswith("Member must have length less than or equal to 25")


# --- B10: per-statement syntax / resource errors --------------------------


def test_syntax_error_is_per_statement(dynamodb_client, btable):
    _seed(dynamodb_client, btable, "ok1")
    resp = _batch(
        dynamodb_client,
        [
            {"Statement": f"SELECT * FROM \"{btable}\" WHERE pk='ok1' AND sk='s1'"},
            {"Statement": f"SELCT * FROM \"{btable}\""},
        ],
    )
    assert resp["Responses"][0]["Item"]["pk"] == {"S": "ok1"}
    assert resp["Responses"][1] == {
        "Error": {
            "Code": "ValidationError",
            "Message": "Statement wasn't well formed, can't be processed: Expected data manipulation",
        }
    }


def test_unknown_table_is_per_statement_resource_not_found(dynamodb_client, btable):
    resp = _batch(
        dynamodb_client,
        [
            {
                "Statement": "INSERT INTO \"extenddb-measure-2c-no-such-table\" VALUE {'pk':'x','sk':'s1'}"
            }
        ],
    )
    assert resp["Responses"] == [
        {"Error": {"Code": "ResourceNotFound", "Message": "Requested resource not found"}}
    ]

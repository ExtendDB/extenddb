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
from string import Template

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import wait_for_active
from partiql_gsi_table import gsi_table, item, key, put


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


# --- RETURNING inside a transaction (request-level) -----------------------
# A member with a RETURNING clause fails the whole request up front with a
# ValidationException naming its position, and no member applies (measured
# 2026-11-02).

_RETURNING_REFUSAL = "RETURNING clause is not supported in ExecuteTransaction."


@pytest.mark.parametrize("mode", ["ALL OLD", "MODIFIED OLD", "ALL NEW", "MODIFIED NEW"])
def test_update_returning_member_rejects_whole_request(dynamodb_client, ttable, mode):
    pk = "tr-" + mode.replace(" ", "-").lower()
    _seed(dynamodb_client, ttable, pk, a=1)
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {
                    "Statement": f"UPDATE \"{ttable}\" SET a=2 WHERE pk='{pk}' AND sk='s1' RETURNING {mode} *"
                }
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == f"Validation failed in TransactStatements[0]: {_RETURNING_REFUSAL}"
    assert _get(dynamodb_client, ttable, pk)["a"] == {"N": "1"}


def test_returning_member_is_named_by_its_position(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "tr-pos-a", a=1)
    _seed(dynamodb_client, ttable, "tr-pos-b", a=1)
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {"Statement": f"UPDATE \"{ttable}\" SET a=2 WHERE pk='tr-pos-a' AND sk='s1'"},
                {
                    "Statement": f"DELETE FROM \"{ttable}\" WHERE pk='tr-pos-b' AND sk='s1' RETURNING ALL OLD *"
                },
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == f"Validation failed in TransactStatements[1]: {_RETURNING_REFUSAL}"
    # Nothing applied: neither the plain UPDATE nor the DELETE ran.
    assert _get(dynamodb_client, ttable, "tr-pos-a")["a"] == {"N": "1"}
    assert _get(dynamodb_client, ttable, "tr-pos-b") is not None


def test_returning_member_after_a_read_reports_the_returning_clause(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "tr-mix-r", a=1)
    _seed(dynamodb_client, ttable, "tr-mix-w", a=1)
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='tr-mix-r' AND sk='s1'"},
                {
                    "Statement": f"UPDATE \"{ttable}\" SET a=2 WHERE pk='tr-mix-w' AND sk='s1' RETURNING ALL NEW *"
                },
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == f"Validation failed in TransactStatements[1]: {_RETURNING_REFUSAL}"


_MISSING_TABLE = "extenddb-measure-2c-no-such-table"
_NOT_FOUND_CANCEL = (
    "Transaction cancelled, please refer cancellation reasons for specific reasons [ResourceNotFound]"
)


@pytest.mark.parametrize(
    "returning",
    ["UPDATE-RETURNING ALL NEW *", "DELETE-RETURNING ALL OLD *", "DELETE-RETURNING MODIFIED OLD *"],
)
def test_returning_on_a_missing_table_is_a_not_found_cancellation(dynamodb_client, ttable, returning):
    verb, clause = returning.split("-")
    if verb == "UPDATE":
        statement = f"UPDATE \"{_MISSING_TABLE}\" SET a=2 WHERE pk='x' AND sk='s1' {clause}"
    else:
        statement = f"DELETE FROM \"{_MISSING_TABLE}\" WHERE pk='x' AND sk='s1' {clause}"
    with pytest.raises(ClientError) as excinfo:
        _txn(dynamodb_client, [{"Statement": statement}])
    code, message = _error(excinfo)
    assert code == "TransactionCanceledException"
    assert message == _NOT_FOUND_CANCEL
    reasons = excinfo.value.response["CancellationReasons"]
    assert [r["Code"] for r in reasons] == ["ResourceNotFound"]


def test_returning_on_an_existing_table_is_refused_beside_a_missing_one(dynamodb_client, ttable):
    returning = f"UPDATE \"{ttable}\" SET a=2 WHERE pk='tr-miss' AND sk='s1' RETURNING ALL NEW *"
    plain_missing = f"UPDATE \"{_MISSING_TABLE}\" SET a=2 WHERE pk='x' AND sk='s1'"
    returning_missing = returning.replace(ttable, _MISSING_TABLE)
    for statements, index in (
        ([returning, plain_missing], 0),
        ([plain_missing, returning], 1),
        ([returning_missing, returning], 1),
    ):
        with pytest.raises(ClientError) as excinfo:
            _txn(dynamodb_client, [{"Statement": s} for s in statements])
        code, message = _error(excinfo)
        assert code == "ValidationException"
        assert message == f"Validation failed in TransactStatements[{index}]: {_RETURNING_REFUSAL}"


def test_returning_on_a_missing_table_beside_a_read_reports_the_mixed_request(dynamodb_client, ttable):
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {"Statement": f"SELECT * FROM \"{ttable}\" WHERE pk='x' AND sk='s1'"},
                {
                    "Statement": f"UPDATE \"{_MISSING_TABLE}\" SET a=2 "
                    "WHERE pk='x' AND sk='s1' RETURNING ALL NEW *"
                },
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        "ExecuteTransaction API does not support both read and write operations in the same request."
    )


def test_delete_invalid_returning_clause_beside_a_missing_table(dynamodb_client, ttable):
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {"Statement": f"UPDATE \"{_MISSING_TABLE}\" SET a=2 WHERE pk='x' AND sk='s1'"},
                {
                    "Statement": f"DELETE FROM \"{ttable}\" WHERE pk='tr-miss2' AND sk='s1' "
                    "RETURNING MODIFIED OLD *"
                },
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        "Validation failed in TransactStatements[1]: Invalid returning clause: "
        "RETURNING MODIFIED OLD *. Only RETURNING ALL OLD * is allowed in DELETE statements."
    )


def test_delete_with_another_returning_clause_reports_the_invalid_clause(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "tr-delmod", a=1)
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {
                    "Statement": f"DELETE FROM \"{ttable}\" WHERE pk='tr-delmod' AND sk='s1' RETURNING MODIFIED OLD *"
                }
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        "Validation failed in TransactStatements[0]: Invalid returning clause: "
        "RETURNING MODIFIED OLD *. Only RETURNING ALL OLD * is allowed in DELETE statements."
    )
    assert _get(dynamodb_client, ttable, "tr-delmod") is not None


# --- empty TransactStatements (request-level) -----------------------------


def test_empty_transact_statements_rejected(dynamodb_client_no_validation):
    with pytest.raises(ClientError) as excinfo:
        dynamodb_client_no_validation.execute_transaction(TransactStatements=[])
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        "1 validation error detected: Value '[]' at 'transactStatements' failed to satisfy "
        "constraint: Member must have length greater than or equal to 1"
    )


# --- DELETE on a missing key with a non-key predicate -----------------------
# The predicate is a condition on an item that exists, so a member naming a
# missing key does not cancel the transaction, as the statement does not fail
# on its own.


def test_delete_missing_key_with_predicate_does_not_cancel(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "tr-np-other", a=1)
    resp = _txn(
        dynamodb_client,
        [
            {"Statement": f"DELETE FROM \"{ttable}\" WHERE pk='tr-np-ghost' AND sk='s1' AND a = 1"},
            {"Statement": f"UPDATE \"{ttable}\" SET a=2 WHERE pk='tr-np-other' AND sk='s1'"},
        ],
    )
    assert resp["Responses"] == []
    assert _get(dynamodb_client, ttable, "tr-np-other")["a"] == {"N": "2"}


# --- request-stage refusals of a write member -------------------------------
# A statement that does not parse, or that has more placeholders than the
# request gives parameters, fails the whole request with a ValidationException
# naming its position (measured 2026-11-02).


def test_parse_error_names_its_member(dynamodb_client, ttable):
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [
                {"Statement": f"UPDATE \"{ttable}\" SET a=1 WHERE pk='tp-parse' AND sk='s1'"},
                {"Statement": "UPDATE WHERE"},
            ],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        "Validation failed in TransactStatements[1]: Statement wasn't well formed, can't be "
        "processed: Expected identifier for simple path"
    )


def test_parameter_count_mismatch_names_its_member(dynamodb_client, ttable):
    with pytest.raises(ClientError) as excinfo:
        _txn(
            dynamodb_client,
            [{"Statement": f"UPDATE \"{ttable}\" SET a=? WHERE pk='tp-count' AND sk='s1'"}],
        )
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        "Validation failed in TransactStatements[0]: Number of parameters in request and "
        "statement don't match."
    )


# --- member-stage cancellations ----------------------------------------------
# Each member is translated against its table before anything runs. A member on
# a missing table, or one whose key does not match the table's schema, cancels
# the request with one reason per member, "None" for the others. Nothing runs:
# no condition is evaluated, no duplicate INSERT is found, and two members on
# one item are not refused (measured 2026-11-02).

_NONE = {"Code": "None"}
_NOT_FOUND = {"Code": "ResourceNotFound", "Message": "Requested resource not found"}
_KEY_MISMATCH = {
    "Code": "ValidationError",
    "Message": "The provided key element does not match the schema",
}


def _invalid(message):
    return {
        "Code": "ValidationError",
        "Message": f"One or more parameter values were invalid: {message}",
    }


def _statements(templates, table):
    """Fill $T (the shared table) and $M (a table that does not exist)."""
    return [
        {"Statement": Template(t).substitute(T=table, M=_MISSING_TABLE)} for t in templates
    ]


def _expect_cancellation(client, table, templates, reasons):
    with pytest.raises(ClientError) as excinfo:
        _txn(client, _statements(templates, table))
    response = excinfo.value.response
    codes = ", ".join(reason["Code"] for reason in reasons)
    assert response["Error"]["Code"] == "TransactionCanceledException"
    assert response["Error"]["Message"] == (
        f"Transaction cancelled, please refer cancellation reasons for specific reasons [{codes}]"
    )
    assert response["CancellationReasons"] == reasons


_ON_T = "UPDATE \"$T\" SET a=2 WHERE pk='tm-y' AND sk='s1'"
_ON_M = "UPDATE \"$M\" SET a=2 WHERE pk='tm-x' AND sk='s1'"
_PARTIAL = "UPDATE \"$T\" SET a=1 WHERE pk='tm-a'"
_SAME_KEY = "UPDATE \"$T\" SET a=1 WHERE pk='tm-b' AND sk='s1'"


@pytest.mark.parametrize(
    ("templates", "reasons"),
    [
        pytest.param([_ON_T, _ON_M], [_NONE, _NOT_FOUND], id="table-then-missing-table"),
        pytest.param([_ON_M, _ON_T], [_NOT_FOUND, _NONE], id="missing-table-then-table"),
        pytest.param([_ON_M], [_NOT_FOUND], id="update-on-missing-table"),
        pytest.param(
            ["INSERT INTO \"$M\" VALUE {'pk':'tm-a','sk':'b'}"],
            [_NOT_FOUND],
            id="insert-on-missing-table",
        ),
        pytest.param(
            ["DELETE FROM \"$M\" WHERE pk='tm-a' AND sk='b'"],
            [_NOT_FOUND],
            id="delete-on-missing-table",
        ),
        pytest.param([_PARTIAL, _ON_M], [_KEY_MISMATCH, _NOT_FOUND], id="partial-key-then-missing-table"),
        pytest.param([_ON_M, _PARTIAL], [_NOT_FOUND, _KEY_MISMATCH], id="missing-table-then-partial-key"),
        pytest.param(
            [_ON_M, "UPDATE \"$T\" SET a=1 WHERE pk='tm-a' AND sk=1"],
            [_NOT_FOUND, _KEY_MISMATCH],
            id="missing-table-then-key-of-the-wrong-type",
        ),
        pytest.param(
            [_SAME_KEY, _SAME_KEY, _ON_M],
            [_NONE, _NONE, _NOT_FOUND],
            id="one-item-twice-then-missing-table",
        ),
        pytest.param([_PARTIAL], [_KEY_MISMATCH], id="partial-key"),
        pytest.param(
            ["UPDATE \"$T\" SET a=1 WHERE pk='tm-a' AND sk=1"],
            [_KEY_MISMATCH],
            id="key-of-the-wrong-type",
        ),
        pytest.param(["UPDATE \"$T\" SET a=1 WHERE a=2"], [_KEY_MISMATCH], id="no-key-in-where"),
        pytest.param(
            ["DELETE FROM \"$T\" WHERE pk='tm-a'"], [_KEY_MISMATCH], id="delete-with-a-partial-key"
        ),
        pytest.param(
            ["UPDATE \"$T\" SET pk='tm-z' WHERE pk='tm-a' AND sk='s1'"],
            [_invalid("Cannot update attribute pk. This attribute is part of the key")],
            id="set-on-a-key-attribute",
        ),
        pytest.param(
            ["INSERT INTO \"$T\" VALUE {'pk':'tm-a'}"],
            [_invalid("Missing the key sk in the item")],
            id="insert-missing-the-sort-key",
        ),
        pytest.param(
            [_PARTIAL, _SAME_KEY, _SAME_KEY],
            [_KEY_MISMATCH, _NONE, _NONE],
            id="partial-key-then-one-item-twice",
        ),
    ],
)
def test_member_stage_failure_cancels_with_a_reason_per_member(
    dynamodb_client, ttable, templates, reasons
):
    _expect_cancellation(dynamodb_client, ttable, templates, reasons)


def test_member_stage_failure_skips_duplicate_insert_detection(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "tm-dup", a=1)
    _expect_cancellation(
        dynamodb_client,
        ttable,
        ["INSERT INTO \"$T\" VALUE {'pk':'tm-dup','sk':'s1'}", _PARTIAL],
        [_NONE, _KEY_MISMATCH],
    )
    assert _get(dynamodb_client, ttable, "tm-dup") == {
        "pk": {"S": "tm-dup"},
        "sk": {"S": "s1"},
        "a": {"N": "1"},
    }


def test_member_stage_failure_skips_condition_evaluation(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "tm-cond", a=1)
    _expect_cancellation(
        dynamodb_client,
        ttable,
        ["UPDATE \"$T\" SET a=5 WHERE pk='tm-cond' AND sk='s1' AND a=99", _PARTIAL],
        [_NONE, _KEY_MISMATCH],
    )
    assert _get(dynamodb_client, ttable, "tm-cond")["a"] == {"N": "1"}


def test_member_stage_failure_applies_nothing(dynamodb_client, ttable):
    _seed(dynamodb_client, ttable, "tm-keep", a=1)
    _seed(dynamodb_client, ttable, "tm-gone", a=1)
    _expect_cancellation(
        dynamodb_client,
        ttable,
        [
            "UPDATE \"$T\" SET a=2 WHERE pk='tm-keep' AND sk='s1'",
            "INSERT INTO \"$T\" VALUE {'pk':'tm-new','sk':'s1'}",
            "DELETE FROM \"$T\" WHERE pk='tm-gone' AND sk='s1'",
            _ON_M,
        ],
        [_NONE, _NONE, _NONE, _NOT_FOUND],
    )
    assert _get(dynamodb_client, ttable, "tm-keep")["a"] == {"N": "1"}
    assert _get(dynamodb_client, ttable, "tm-new") is None
    assert _get(dynamodb_client, ttable, "tm-gone") is not None


# --- key and parameter variants ---------------------------------------------
# The same rules applied to an INSERT key of the wrong type or a missing
# partition key, a DELETE key of the wrong type, a REMOVE of a key attribute,
# and a member with a partial key that also sets a key attribute.


@pytest.mark.parametrize(
    ("templates", "reasons"),
    [
        pytest.param(
            ["INSERT INTO \"$T\" VALUE {'pk':'tv-a','sk':1}"],
            [_invalid("Type mismatch for key sk expected: S actual: N")],
            id="insert-sort-key-of-the-wrong-type",
        ),
        pytest.param(
            ["INSERT INTO \"$T\" VALUE {'sk':'s1'}"],
            [_invalid("Missing the key pk in the item")],
            id="insert-missing-the-partition-key",
        ),
        pytest.param(
            ["INSERT INTO \"$T\" VALUE {'a':1}"],
            [_invalid("Missing the key pk in the item")],
            id="insert-missing-both-keys",
        ),
        pytest.param(
            ["DELETE FROM \"$T\" WHERE pk='tv-a' AND sk=1"],
            [_KEY_MISMATCH],
            id="delete-key-of-the-wrong-type",
        ),
        pytest.param(
            ["UPDATE \"$T\" REMOVE sk WHERE pk='tv-a' AND sk='s1'"],
            [_invalid("Cannot update attribute sk. This attribute is part of the key")],
            id="remove-a-key-attribute",
        ),
        pytest.param(
            ["UPDATE \"$T\" SET pk='tv-z' WHERE pk='tv-a'"],
            [_KEY_MISMATCH],
            id="set-a-key-attribute-with-a-partial-key",
        ),
    ],
)
def test_key_problem_variants_cancel_with_a_reason(dynamodb_client, ttable, templates, reasons):
    _expect_cancellation(dynamodb_client, ttable, templates, reasons)


@pytest.mark.parametrize(
    ("templates", "index"),
    [
        pytest.param(["UPDATE \"$M\" SET a=? WHERE pk='tv-x' AND sk='s1'"], 0, id="on-the-missing-table"),
        pytest.param(
            [_ON_M, "UPDATE \"$T\" SET a=? WHERE pk='tv-x' AND sk='s1'"],
            1,
            id="beside-the-missing-table",
        ),
    ],
)
def test_parameter_count_mismatch_beside_a_missing_table(dynamodb_client, ttable, templates, index):
    with pytest.raises(ClientError) as excinfo:
        _txn(dynamodb_client, _statements(templates, ttable))
    code, message = _error(excinfo)
    assert code == "ValidationException"
    assert message == (
        f"Validation failed in TransactStatements[{index}]: Number of parameters in request "
        "and statement don't match."
    )

# --- the table with a global secondary index ------------------------------


@pytest.fixture(scope="module")
def gtable(dynamodb_client):
    """The table with the index `gsi`, holding the items of the reads."""
    with gsi_table(dynamodb_client, "extenddb-measure-2c-txng") as name:
        put(dynamodb_client, name, "seed")
        put(dynamodb_client, name, "seed2", gs="2")
        put(dynamodb_client, name, "lst", l={"L": [{"N": "1"}, {"N": "2"}, {"N": "3"}]})
        yield name


def _sql(template, table, **names):
    """`template` with `{t}` as the quoted table and each other field as the
    key that name takes in this run."""
    return template.format(t=f'"{table}"', **{n: key(v) for n, v in names.items()})


def _members(table, templates, **names):
    return [{"Statement": _sql(t, table, **names)} for t in templates]


# --- the projection of a read member ------------------------------------------
# A read member honours its projection list, and the item keeps the shape of
# the paths, as a ProjectionExpression gives it. An item that has none of the
# paths is returned empty, and a missing item returns no item. A projection
# list that names an identifier twice, or whose paths overlap, is refused with
# the member's position before its table is read (measured 2026-11-05).


@pytest.mark.parametrize(
    "projection, name, expected",
    [
        ("a, m.k", "seed", {"Item": {"a": {"N": "1"}, "m": {"M": {"k": {"N": "1"}}}}}),
        (
            "m.k, m.\"my key\"",
            "seed",
            {"Item": {"m": {"M": {"k": {"N": "1"}, "my key": {"N": "1"}}}}},
        ),
        ("sk, pk", "seed", {"Item": {"pk": {"S": key("seed")}, "sk": {"S": "b"}}}),
        ("l[1], m.k", "lst", {"Item": {"l": {"L": [{"N": "2"}]}, "m": {"M": {"k": {"N": "1"}}}}}),
        ("nope", "seed", {"Item": {}}),
        ("m.nope, a", "seed", {"Item": {"a": {"N": "1"}}}),
        ("a.b", "seed", {"Item": {}}),
        ("l[7]", "lst", {"Item": {}}),
        ("a", "no-such-item", {}),
    ],
    ids=[
        "path",
        "quoted-name",
        "key-attributes",
        "list-element",
        "absent-attribute",
        "absent-member",
        "path-into-a-number",
        "element-past-the-list",
        "missing-item",
    ],
)
def test_a_read_member_honours_its_projection(dynamodb_client, gtable, projection, name, expected):
    resp = _txn(
        dynamodb_client,
        _members(gtable, [f"SELECT {projection} FROM {{t}} WHERE pk = '{{k}}' AND sk = 'b'"],
                 k=name),
    )
    assert resp["Responses"] == [expected]


def test_each_read_member_takes_its_own_projection(dynamodb_client, gtable):
    resp = _txn(
        dynamodb_client,
        _members(
            gtable,
            [
                "SELECT pk, a FROM {t} WHERE pk = '{s}' AND sk = 'b'",
                "SELECT * FROM {t} WHERE pk = '{s2}' AND sk = 'b'",
            ],
            s="seed",
            s2="seed2",
        ),
    )
    assert resp["Responses"] == [
        {"Item": {"pk": {"S": key("seed")}, "a": {"N": "1"}}},
        {"Item": item(key("seed2"), gs="2")},
    ]


def test_a_projection_does_not_change_the_read_units(dynamodb_client, gtable):
    resp = _txn(
        dynamodb_client,
        _members(gtable, ["SELECT a FROM {t} WHERE pk = '{s}' AND sk = 'b'"], s="seed"),
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["Responses"] == [{"Item": {"a": {"N": "1"}}}]
    assert resp["ConsumedCapacity"] == [
        {"TableName": gtable, "CapacityUnits": 2.0, "ReadCapacityUnits": 2.0}
    ]


_REPEATED = "Duplicate identifiers in select clause: a"


def _overlap(one, two):
    return (
        "Two document paths overlap with each other; must remove or rewrite one of these "
        f"paths; path one: {one}, path two: {two}"
    )


@pytest.mark.parametrize(
    "templates, index, message",
    [
        (["SELECT a, a FROM {t} WHERE pk = '{s}' AND sk = 'b'"], 0, _REPEATED),
        (["SELECT m.k, k FROM {t} WHERE pk = '{s}' AND sk = 'b'"], 0,
         "Duplicate identifiers in select clause: k"),
        (["SELECT a, \"a\" FROM {t} WHERE pk = '{s}' AND sk = 'b'"], 0, _REPEATED),
        (["SELECT m.k, m FROM {t} WHERE pk = '{s}' AND sk = 'b'"], 0, _overlap("[m]", "[m, k]")),
        (["SELECT m, m.k FROM {t} WHERE pk = '{s}' AND sk = 'b'"], 0, _overlap("[m, k]", "[m]")),
        (["SELECT l[1], l FROM {t} WHERE pk = '{l}' AND sk = 'b'"], 0,
         _overlap("[l]", "[l, [1]]")),
        (["SELECT a, a FROM nosuch_tbl WHERE pk = 'a' AND sk = 'b'"], 0, _REPEATED),
        (["SELECT a, a FROM {t} WHERE pk = '{s}'"], 0, _REPEATED),
        (["SELECT a, a FROM {t}.\"gsi\" WHERE g = 'a' AND gs = '1'"], 0, _REPEATED),
        (["SELECT m.k, m FROM nosuch_tbl WHERE pk = 'a' AND sk = 'b'"], 0,
         _overlap("[m]", "[m, k]")),
        (["SELECT * FROM {t} WHERE pk = '{s}' AND sk = 'b'",
          "SELECT a, a FROM {t} WHERE pk = '{s2}' AND sk = 'b'"], 1, _REPEATED),
        (["INSERT INTO {t} VALUE {{'pk':'{n}','sk':'b','a':1}}",
          "SELECT a, a FROM {t} WHERE pk = '{s2}' AND sk = 'b'"], 1, _REPEATED),
        (["SELECT a, a FROM {t} WHERE pk = '{s}' AND sk = 'b'", "SELECT * FROM"], 0, _REPEATED),
    ],
    ids=[
        "repeated",
        "repeated-leaf",
        "repeated-quoted",
        "overlap",
        "overlap-reversed",
        "overlap-list",
        "ahead-of-a-missing-table",
        "ahead-of-a-partial-key",
        "ahead-of-an-index",
        "overlap-ahead-of-a-missing-table",
        "second-member",
        "ahead-of-the-mix",
        "ahead-of-a-later-syntax-error",
    ],
)
def test_a_projection_refusal_names_its_member(dynamodb_client, gtable, templates, index, message):
    with pytest.raises(ClientError) as excinfo:
        _txn(dynamodb_client, _members(gtable, templates, s="seed", s2="seed2", l="lst",
                                       n=f"tq-{uuid.uuid4().hex[:6]}"))
    assert _error(excinfo) == (
        "ValidationException",
        f"Validation failed in TransactStatements[{index}]: {message}",
    )


# --- which refusal a transaction with several faults reports -----------------
# The service checks each member in order and stops at the first member that
# fails: its statement, its parameters, its form against its table (an index
# read, a read without the full key, a key-only EXISTS, an EXISTS on an index
# ahead of a missing table, a RETURNING clause), then whether it is a read
# beside a write. A missing table and a key value of the wrong type cancel the
# request once every member passes (measured 2026-11-05).

_MIXED = (
    "ExecuteTransaction API does not support both read and write operations in the same "
    "request."
)
_SEED_READ = "SELECT * FROM {t} WHERE pk = '{s}' AND sk = 'b'"
_SYNTAX = "SELECT * FROM"
_INDEX_READ = "SELECT * FROM {t}.\"gsi\" WHERE g = 'a' AND gs = '1'"
_PARTIAL_READ = "SELECT * FROM {t} WHERE pk = '{s}'"
_KEY_ONLY_EXISTS = "EXISTS(SELECT * FROM {t} WHERE pk = '{s}' AND sk = 'b')"
_MISSING_READ = "SELECT * FROM nosuch_tbl WHERE pk = 'a' AND sk = 'b'"
_INSERT = "INSERT INTO {t} VALUE {{'pk':'{n}','sk':'b','a':1}}"
_UNBOUND_INSERT = "INSERT INTO {t} VALUE {{'pk':'{n}','sk':?}}"
_RETURNING_UPDATE = "UPDATE {t} SET x = 1 WHERE pk = '{r}' AND sk = 'b' RETURNING ALL NEW *"


def _positional(index, message):
    return f"Validation failed in TransactStatements[{index}]: {message}"


_RETURNING = "RETURNING clause is not supported in ExecuteTransaction."
_INDEX = "Reads on indices are not supported within transactions."
_PARTIAL_KEY = (
    "Select statements within ExecuteTransaction must specify the primary key in the where "
    "clause."
)
_EXISTS_FORM = "EXISTS() must contain a single item read with additional condition"
_EXISTS_INDEX = "Operations on indices are not supported in transactions"
_UNBOUND = "Number of parameters in request and statement don't match."
_UNEXPECTED_TERM = "Statement wasn't well formed, can't be processed: Unexpected term"


@pytest.mark.parametrize(
    "templates, message",
    [
        ([_SEED_READ, _INSERT, _SYNTAX], _MIXED),
        ([_RETURNING_UPDATE, _SYNTAX], _positional(0, _RETURNING)),
        ([_INDEX_READ, _INSERT], _positional(0, _INDEX)),
        ([_PARTIAL_READ, _MISSING_READ], _positional(0, _PARTIAL_KEY)),
        ([_KEY_ONLY_EXISTS, _SEED_READ], _positional(0, _EXISTS_FORM)),
        (["EXISTS(SELECT * FROM \"nosuch_tbl\".\"idx\" WHERE pk = 'a' AND x = 1)"],
         _positional(0, _EXISTS_INDEX)),
        ([_SEED_READ, _UNBOUND_INSERT], _positional(1, _UNBOUND)),
        ([_MISSING_READ, _PARTIAL_READ], _positional(1, _PARTIAL_KEY)),
        ([_MISSING_READ, _INDEX_READ], _positional(1, _INDEX)),
        ([_SEED_READ, _RETURNING_UPDATE], _positional(1, _RETURNING)),
        ([_INSERT, _INDEX_READ], _positional(1, _INDEX)),
        ([_INSERT, _PARTIAL_READ], _positional(1, _PARTIAL_KEY)),
        ([_SEED_READ, _KEY_ONLY_EXISTS], _positional(1, _EXISTS_FORM)),
        ([_SEED_READ,
          "EXISTS(SELECT * FROM {t}.\"gsi\" WHERE g = 'a' AND gs = '1' AND x = 0)"],
         _positional(1, _EXISTS_INDEX)),
        ([_SEED_READ, "UPDATE {t} SET a = ? WHERE pk = '{n}' AND sk = 'b'"],
         _positional(1, _UNBOUND)),
        ([_SEED_READ, _INDEX_READ, _INSERT], _positional(1, _INDEX)),
        ([_UNBOUND_INSERT, _RETURNING_UPDATE], _positional(0, _UNBOUND)),
        ([_KEY_ONLY_EXISTS, _UNBOUND_INSERT], _positional(0, _EXISTS_FORM)),
        (["INSERT INTO nosuch_tbl VALUE {{'pk':'a','sk':'b'}}", _RETURNING_UPDATE],
         _positional(1, _RETURNING)),
        (["UPDATE {t} SET x = 1 WHERE pk = '{n}'", _RETURNING_UPDATE],
         _positional(1, _RETURNING)),
        (["INSERT INTO nosuch_tbl VALUE {{'pk':'a','sk':'b'}}", _KEY_ONLY_EXISTS],
         _positional(1, _EXISTS_FORM)),
        (["SELECT * FROM {t} WHERE pk = '{s}' AND sk = 1", _PARTIAL_READ],
         _positional(1, _PARTIAL_KEY)),
        ([_MISSING_READ, _SYNTAX], _positional(1, _UNEXPECTED_TERM)),
        ([_SEED_READ, "INSERT INTO nosuch_tbl VALUE {{'pk':'a','sk':'b'}}"], _MIXED),
        ([_INSERT, _MISSING_READ], _MIXED),
        (["UPDATE nosuch_tbl SET x = 1 WHERE pk = 'a' AND sk = 'b' RETURNING ALL NEW *",
          _SEED_READ], _MIXED),
    ],
    ids=[
        "read-write-then-syntax",
        "returning-then-syntax",
        "index-read-then-write",
        "partial-key-then-missing-table",
        "key-only-exists-then-read",
        "exists-on-an-index-of-a-missing-table",
        "read-then-unbound-insert",
        "missing-table-then-partial-key",
        "missing-table-then-index-read",
        "read-then-returning",
        "write-then-index-read",
        "write-then-partial-key",
        "read-then-key-only-exists",
        "read-then-exists-on-an-index",
        "read-then-unbound-set",
        "read-then-index-read-then-write",
        "unbound-insert-then-returning",
        "key-only-exists-then-unbound-insert",
        "write-on-a-missing-table-then-returning",
        "partial-key-write-then-returning",
        "write-on-a-missing-table-then-key-only-exists",
        "key-type-then-partial-key",
        "missing-table-then-syntax",
        "read-then-write-on-a-missing-table",
        "write-then-read-of-a-missing-table",
        "returning-on-a-missing-table-then-read",
    ],
)
def test_the_first_member_that_fails_is_reported(dynamodb_client, gtable, templates, message):
    run = uuid.uuid4().hex[:6]
    with pytest.raises(ClientError) as excinfo:
        _txn(dynamodb_client, _members(gtable, templates, s="seed", n=f"tq-{run}",
                                       r=f"tq-r-{run}"))
    assert _error(excinfo) == ("ValidationException", message)


_KEY_TYPE_READ = "SELECT * FROM {t} WHERE pk = '{s}' AND sk = 1"


@pytest.mark.parametrize(
    "templates, reasons",
    [
        ([_MISSING_READ, _KEY_TYPE_READ], [_NOT_FOUND, _KEY_MISMATCH]),
        ([_KEY_TYPE_READ, _MISSING_READ], [_KEY_MISMATCH, _NOT_FOUND]),
        ([_MISSING_READ, "SELECT * FROM nosuch_tbl2 WHERE pk = 'a' AND sk = 'b'", _SEED_READ],
         [_NOT_FOUND, _NOT_FOUND, _NONE]),
        (["SELECT * FROM \"nosuch_tbl\".\"idx\" WHERE pk = 'a'"], [_NOT_FOUND]),
    ],
    ids=["missing-table-then-key-type", "key-type-then-missing-table", "two-missing-tables",
         "index-read-of-a-missing-table"],
)
def test_a_read_cancellation_gives_every_member_its_reason(
    dynamodb_client, gtable, templates, reasons
):
    with pytest.raises(ClientError) as excinfo:
        _txn(dynamodb_client, _members(gtable, templates, s="seed"))
    err = excinfo.value.response
    assert err["Error"]["Code"] == "TransactionCanceledException"
    assert err["CancellationReasons"] == reasons


# --- ConsumedCapacity of an index ------------------------------------------------
# A write transaction charges each write to an index at the units an
# UpdateItem charges it, beside the doubled units of the table (measured
# 2026-11-05). ExtendDB's TransactWriteItems has no old or new item of an
# Update or Delete member to rate an index write from, so it reports the
# table alone, for PartiQL and for the classic operation.

TRANSACTION_INDEX_UNITS = pytest.mark.xfail(
    bool(os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()),
    reason="TransactWriteItems reports no index units",
    raises=AssertionError,
    strict=True,
)


@TRANSACTION_INDEX_UNITS
def test_a_transaction_reports_its_index_units(dynamodb_client, gtable):
    put(dynamodb_client, gtable, "ct17")
    resp = _txn(
        dynamodb_client,
        _members(gtable, ["UPDATE {t} SET g = 'x' WHERE pk = '{k}' AND sk = 'b'"], k="ct17"),
        ReturnConsumedCapacity="INDEXES",
    )
    assert resp["ConsumedCapacity"] == [
        {
            "TableName": gtable,
            "CapacityUnits": 4.0,
            "WriteCapacityUnits": 4.0,
            "Table": {"CapacityUnits": 2.0, "WriteCapacityUnits": 2.0},
            "GlobalSecondaryIndexes": {"gsi": {"CapacityUnits": 2.0, "WriteCapacityUnits": 2.0}},
        }
    ]


@TRANSACTION_INDEX_UNITS
def test_a_transaction_total_counts_its_index_units(dynamodb_client, gtable):
    put(dynamodb_client, gtable, "twr")
    put(dynamodb_client, gtable, "tws")
    resp = _txn(
        dynamodb_client,
        _members(
            gtable,
            [
                "UPDATE {t} SET a = 9 WHERE pk = '{r}' AND sk = 'b'",
                "UPDATE {t} SET a = 9 WHERE pk = '{s}' AND sk = 'b'",
            ],
            r="twr",
            s="tws",
        ),
        ReturnConsumedCapacity="TOTAL",
    )
    assert resp["ConsumedCapacity"] == [
        {"TableName": gtable, "CapacityUnits": 6.0, "WriteCapacityUnits": 6.0}
    ]

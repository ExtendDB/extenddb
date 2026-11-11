# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""PartiQL behaviour on secondary indexes and multi-attribute key schemas,
measured on the service and asserted on both targets.

Three service rules this file pins:
  * a PartiQL statement against a table or index with more than one HASH or
    more than one RANGE key attribute is refused with one fixed message;
  * a BatchExecuteStatement SELECT that names an index can never satisfy the
    primary-key rule, whatever the WHERE names;
  * an ExecuteTransaction SELECT that names any index is refused, and an
    unknown table cancels the whole read transaction.

Dual-target: EXTENDDB_TEST_ENDPOINT selects a fix server, EXTENDDB_TEST_PROFILE
selects the live-service profile. Every expected value was measured on the
service; see conf-run/partiql/probe_multiattr_gsi.py,
probe_index_batch_txn.py, probe_index_order.py and probe_vector_batch_txn.py.
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import wait_for_active

MULTI_ATTR_MSG = (
    "PartiQL statements cannot be executed on tables with multi-attribute "
    "HASH keys or multi-attribute RANGE keys."
)
BATCH_KEY_MSG = (
    "Select statements within BatchExecuteStatement must specify the primary "
    "key in the where clause."
)
TXN_INDEX_MSG = "Reads on indices are not supported within transactions."


@pytest.fixture
def dynamodb_client(endpoint_url):
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


def _create(client, name, gsi_key):
    all_defs = {
        "pk": "S",
        "g1": "S",
        "g2": "S",
        "g3": "N",
    }
    used = {"pk"} | {k["AttributeName"] for k in gsi_key}
    client.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": n, "AttributeType": all_defs[n]}
            for n in all_defs
            if n in used
        ],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi",
                "KeySchema": gsi_key,
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    wait_for_active(client, name)
    client.put_item(
        TableName=name,
        Item={
            "pk": {"S": "p1"},
            "g1": {"S": "a"},
            "g2": {"S": "b1"},
            "g3": {"N": "1"},
        },
    )


@pytest.fixture
def multi_attr_table(dynamodb_client):
    """A table whose GSI key has two RANGE attributes. A backend that cannot
    store a multi-attribute GSI skips the group rather than failing it."""
    name = f"extenddb-measure-mak-{uuid.uuid4().hex[:12]}"
    gsi_key = [
        {"AttributeName": "g1", "KeyType": "HASH"},
        {"AttributeName": "g2", "KeyType": "RANGE"},
        {"AttributeName": "g3", "KeyType": "RANGE"},
    ]
    try:
        _create(dynamodb_client, name, gsi_key)
    except ClientError as exc:
        try:
            dynamodb_client.delete_table(TableName=name)
        except ClientError:
            pass
        pytest.skip(f"multi-attribute GSI not supported here: {exc}")
    try:
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
        except ClientError:
            pass


@pytest.fixture
def plain_index_table(dynamodb_client):
    name = f"extenddb-measure-idx-{uuid.uuid4().hex[:12]}"
    gsi_key = [
        {"AttributeName": "g1", "KeyType": "HASH"},
        {"AttributeName": "g2", "KeyType": "RANGE"},
    ]
    _create(dynamodb_client, name, gsi_key)
    try:
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
        except ClientError:
            pass


class TestMultiAttributeKeyRefusal:
    @pytest.mark.parametrize(
        "where",
        [
            "",
            " WHERE g1 = 'a'",
            " WHERE g1 = 'a' AND g2 = 'b1' AND g3 = 1",
        ],
    )
    def test_single_statement_refused(self, dynamodb_client, multi_attr_table, where):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(
                Statement=f'SELECT * FROM "{multi_attr_table}"."gsi"{where}'
            )
        err = ei.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == MULTI_ATTR_MSG

    def test_multi_attribute_beats_consistent_read(self, dynamodb_client, multi_attr_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(
                Statement=f'SELECT * FROM "{multi_attr_table}"."gsi" WHERE g1 = \'a\'',
                ConsistentRead=True,
            )
        assert ei.value.response["Error"]["Message"] == MULTI_ATTR_MSG

    def test_batch_statement_refused(self, dynamodb_client, multi_attr_table):
        out = dynamodb_client.batch_execute_statement(
            Statements=[
                {"Statement": f'SELECT * FROM "{multi_attr_table}"."gsi" WHERE g1 = \'a\' AND g2 = \'b1\' AND g3 = 1'}
            ]
        )
        err = out["Responses"][0]["Error"]
        assert err["Code"] == "ValidationError"
        assert err["Message"] == MULTI_ATTR_MSG

    def test_base_table_write_unaffected(self, dynamodb_client, multi_attr_table):
        # The base table key is single-attribute, so a write to it is accepted
        # even though the table carries a multi-attribute GSI.
        dynamodb_client.execute_statement(
            Statement=(
                f"INSERT INTO \"{multi_attr_table}\" "
                "VALUE {'pk': 'w1', 'g1': 'a', 'g2': 'b', 'g3': 2}"
            )
        )
        got = dynamodb_client.execute_statement(
            Statement=f"SELECT * FROM \"{multi_attr_table}\" WHERE pk = 'w1'"
        )
        assert got["Items"][0]["pk"]["S"] == "w1"


class TestBatchIndexReads:
    def test_index_with_base_key_still_needs_primary_key(self, dynamodb_client, plain_index_table):
        # Regression: a batch SELECT naming an index used to run as a base-table
        # point read when the WHERE happened to name the base key. The service
        # rejects any index SELECT in a batch.
        out = dynamodb_client.batch_execute_statement(
            Statements=[{"Statement": f'SELECT * FROM "{plain_index_table}"."gsi" WHERE pk = \'p1\''}]
        )
        err = out["Responses"][0]["Error"]
        assert err["Code"] == "ValidationError"
        assert err["Message"] == BATCH_KEY_MSG

    def test_index_with_index_key_needs_primary_key(self, dynamodb_client, plain_index_table):
        out = dynamodb_client.batch_execute_statement(
            Statements=[{"Statement": f'SELECT * FROM "{plain_index_table}"."gsi" WHERE g1 = \'a\' AND g2 = \'b1\''}]
        )
        err = out["Responses"][0]["Error"]
        assert err["Code"] == "ValidationError"
        assert err["Message"] == BATCH_KEY_MSG

    def test_missing_index_is_per_statement(self, dynamodb_client, plain_index_table):
        out = dynamodb_client.batch_execute_statement(
            Statements=[{"Statement": f'SELECT * FROM "{plain_index_table}"."nope" WHERE pk = \'p1\''}]
        )
        err = out["Responses"][0]["Error"]
        assert err["Code"] == "ValidationError"
        assert err["Message"] == "The table does not have the specified index"


@pytest.fixture
def vector_table(dynamodb_client):
    name = f"extenddb-measure-vix-{uuid.uuid4().hex[:12]}"
    try:
        dynamodb_client.create_table(
            TableName=name,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            VectorIndexes=[
                {
                    "IndexName": "vix",
                    "VectorAttribute": {"AttributeName": "embedding"},
                    "Dimensions": 3,
                    "DistanceFunction": "COSINE",
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
    except ClientError as exc:
        try:
            dynamodb_client.delete_table(TableName=name)
        except ClientError:
            pass
        msg = exc.response["Error"]["Message"].lower()
        if "vector" in msg or exc.response["Error"]["Code"] == "ValidationException":
            pytest.skip("vector indexes not supported by this backend")
        raise
    wait_for_active(dynamodb_client, name)
    dynamodb_client.put_item(
        TableName=name,
        Item={"pk": {"S": "p1"}, "embedding": {"L": [{"N": "1"}, {"N": "0"}, {"N": "0"}]}},
    )
    try:
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
        except ClientError:
            pass


class TestVectorIndexReads:
    def test_single_consistent_read_beats_vector_refusal(self, dynamodb_client, vector_table):
        # On a vector index the strong-read refusal is measured to win over the
        # vector refusal.
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(
                Statement=f'SELECT * FROM "{vector_table}"."vix"', ConsistentRead=True
            )
        assert (
            ei.value.response["Error"]["Message"]
            == "Strongly consistent read is not supported on Global Secondary Indexes"
        )

    def test_single_vector_refusal(self, dynamodb_client, vector_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(Statement=f'SELECT * FROM "{vector_table}"."vix"')
        assert (
            ei.value.response["Error"]["Message"]
            == "Scan operation not supported on this index type"
        )

    def test_batch_vector_index_needs_primary_key(self, dynamodb_client, vector_table):
        # Regression: a vector-index SELECT in a batch with the base key used to
        # return the item; the service answers with the primary-key message.
        out = dynamodb_client.batch_execute_statement(
            Statements=[{"Statement": f'SELECT * FROM "{vector_table}"."vix" WHERE pk = \'p1\''}]
        )
        err = out["Responses"][0]["Error"]
        assert err["Code"] == "ValidationError"
        assert err["Message"] == BATCH_KEY_MSG

    def test_transaction_vector_index_refused(self, dynamodb_client, vector_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_transaction(
                TransactStatements=[{"Statement": f'SELECT * FROM "{vector_table}"."vix" WHERE pk = \'p1\''}]
            )
        assert TXN_INDEX_MSG in ei.value.response["Error"]["Message"]


class TestTransactionIndexReads:
    @pytest.mark.parametrize("where", [" WHERE pk = 'p1'", " WHERE g1 = 'a' AND g2 = 'b1'"])
    def test_index_read_refused(self, dynamodb_client, plain_index_table, where):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_transaction(
                TransactStatements=[{"Statement": f'SELECT * FROM "{plain_index_table}"."gsi"{where}'}]
            )
        err = ei.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert f"TransactStatements[0]: {TXN_INDEX_MSG}" in err["Message"]

    def test_missing_index_refused(self, dynamodb_client, plain_index_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_transaction(
                TransactStatements=[{"Statement": f'SELECT * FROM "{plain_index_table}"."nope" WHERE pk = \'p1\''}]
            )
        assert TXN_INDEX_MSG in ei.value.response["Error"]["Message"]

    def test_unknown_table_cancels_the_read(self, dynamodb_client):
        missing = f"extenddb-no-such-{uuid.uuid4().hex[:8]}"
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_transaction(
                TransactStatements=[{"Statement": f'SELECT * FROM "{missing}" WHERE pk = \'a\''}]
            )
        resp = ei.value.response
        assert resp["Error"]["Code"] == "TransactionCanceledException"
        reasons = resp["CancellationReasons"]
        assert reasons[0]["Code"] == "ResourceNotFound"
        assert reasons[0]["Message"] == "Requested resource not found"

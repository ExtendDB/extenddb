# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""PartiQL phase-3 behaviours, measured on the service and asserted on both
targets: the LIMIT-clause error, the IN-on-sort-key result, a projected batch
read, and a vector-index SELECT refusal.

Dual-target: EXTENDDB_TEST_ENDPOINT selects a fix server, EXTENDDB_TEST_PROFILE
selects the live-service profile. Every expected value was measured on the
service; see conf-run/partiql/probe_gaps.py.
"""

from __future__ import annotations

import os
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from conftest import wait_for_active


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


@pytest.fixture
def table(dynamodb_client):
    name = f"extenddb-measure-p3-{uuid.uuid4().hex[:12]}"
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
        except ClientError:
            pass


class TestPartiqlPhase3:
    def test_limit_clause_is_unsupported(self, dynamodb_client, table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(
                Statement=f"SELECT * FROM \"{table}\" WHERE pk = 'p1' LIMIT 2"
            )
        err = ei.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"].startswith("Unsupported clause: LIMIT at 1:")
        assert err["Message"].endswith(":1")

    def test_in_on_sort_key_returns_the_requested_items(self, dynamodb_client, table):
        for sk in ("s1", "s2", "s3"):
            dynamodb_client.put_item(TableName=table, Item={"pk": {"S": "p1"}, "sk": {"S": sk}})
        r = dynamodb_client.execute_statement(
            Statement=f"SELECT sk FROM \"{table}\" WHERE pk = 'p1' AND sk IN ['s1','s3']"
        )
        assert sorted(i["sk"]["S"] for i in r["Items"]) == ["s1", "s3"]

    def test_batch_select_projects(self, dynamodb_client, table):
        dynamodb_client.put_item(
            TableName=table,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "name": {"S": "Alice"}, "age": {"N": "30"}},
        )
        r = dynamodb_client.batch_execute_statement(
            Statements=[{"Statement": f"SELECT name FROM \"{table}\" WHERE pk = 'p1' AND sk = 's1'"}]
        )
        assert r["Responses"][0]["Item"] == {"name": {"S": "Alice"}}

    def test_duplicate_leaf_projection_is_rejected(self, dynamodb_client, table):
        dynamodb_client.put_item(
            TableName=table,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"},
                  "a1": {"M": {"x": {"S": "A1X"}}}, "a2": {"M": {"x": {"S": "A2X"}}}},
        )
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(
                Statement=f"SELECT a1.x, a2.x FROM \"{table}\" WHERE pk = 'p1' AND sk = 's1'"
            )
        err = ei.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "Duplicate identifiers in select clause: x"

    def test_multi_dot_projection_is_rejected(self, dynamodb_client, table):
        dynamodb_client.put_item(
            TableName=table,
            Item={"pk": {"S": "p1"}, "sk": {"S": "s1"},
                  "meta": {"M": {"inner": {"M": {"leaf": {"S": "deep"}}}}}},
        )
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(
                Statement=f"SELECT meta.inner.leaf FROM \"{table}\" WHERE pk = 'p1' AND sk = 's1'"
            )
        err = ei.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "Statement wasn't well formed, can't be processed: Invalid path dot component"
        )

    def test_count_star_message(self, dynamodb_client, table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(
                Statement=f"SELECT COUNT(*) FROM \"{table}\" WHERE pk = 'p1'"
            )
        err = ei.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "Unexpected path component at 1:8:5"

    def test_duplicate_bag_member_message(self, dynamodb_client, table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(
                Statement=f"INSERT INTO \"{table}\" VALUE {{'pk':'p1','sk':'s1','db':<<'a','a'>>}}"
            )
        err = ei.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "One or more parameter values were invalid: Input collection [a, a] contains duplicates."
        )

    def test_arithmetic_string_literal_operand_message(self, dynamodb_client, table):
        dynamodb_client.put_item(
            TableName=table, Item={"pk": {"S": "p1"}, "sk": {"S": "s1"}, "a": {"N": "1"}}
        )
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(
                Statement=f"UPDATE \"{table}\" SET a = a + 'x' WHERE pk = 'p1' AND sk = 's1'"
            )
        err = ei.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == (
            "Incorrect operand type for operator or function; operator or function: +, operand type: S"
        )


@pytest.fixture
def vector_table(dynamodb_client):
    name = f"extenddb-measure-p3v-{uuid.uuid4().hex[:12]}"
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
        # A backend without vector support rejects the index; the SELECT refusal
        # is only reachable where vector indexes exist (the service, PostgreSQL).
        msg = exc.response["Error"]["Message"].lower()
        if "vector" in msg or exc.response["Error"]["Code"] == "ValidationException":
            pytest.skip("vector indexes not supported by this backend")
        raise
    try:
        wait_for_active(dynamodb_client, name)
        yield name
    finally:
        try:
            dynamodb_client.delete_table(TableName=name)
        except ClientError:
            pass


class TestPartiqlVectorIndex:
    def test_select_from_vector_index_is_refused(self, dynamodb_client, vector_table):
        with pytest.raises(ClientError) as ei:
            dynamodb_client.execute_statement(Statement=f'SELECT * FROM "{vector_table}"."vix"')
        err = ei.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert err["Message"] == "Scan operation not supported on this index type"

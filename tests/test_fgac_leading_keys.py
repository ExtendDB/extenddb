# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Fine-grained access control tests for dynamodb:LeadingKeys on Query, batch
and transaction operations.

dynamodb:LeadingKeys restricts a principal to the partition key(s) it may
touch. It was enforced on the item operations (GetItem, PutItem, UpdateItem,
DeleteItem) but not on Query, BatchGetItem, BatchWriteItem, TransactGetItems or
TransactWriteItems, so a principal fenced to its own partition could read or
write any partition through those operations. These tests pin the enforced
behaviour.

Every expected outcome here was measured against live DynamoDB and IAM on
2026-10-20. The assertions are identical whatever backend the suite targets; a
difference is a bug (REQ-TEST-004).

Measured model:
  - Query on the base table or an LSI binds LeadingKeys to the partition key
    value in the key condition (an LSI shares the base partition key).
  - Query on a GSI binds LeadingKeys to the queried index's partition key value.
  - Scan sets no LeadingKeys: a ForAllValues allow is vacuously satisfied (scan
    allowed), a ForAnyValue allow is not (scan denied).
  - BatchGetItem/BatchWriteItem authorize per table over the set of that
    table's keys: a ForAnyValue allow passes a mixed own+other batch.
  - TransactGetItems/TransactWriteItems authorize per item: a ForAnyValue allow
    denies a mixed own+other transaction, because the other item fails on its
    own.

Prerequisites:
  - extenddb running with ``auth.provider = "builtin"`` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars

Run:
  EXTENDDB_TEST_ENDPOINT=https://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin \\
  EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_fgac_leading_keys.py -v

REQ-TEST-001, REQ-TEST-004, REQ-AUTH-002
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import wait_for_active, wait_for_deleted
from management_helpers import ManagementClient

OWN = "alice"
OTHER = "bob"
G_OWN = "galice"   # the alice item's GSI partition key value
G_OTHER = "gbob"   # the bob item's GSI partition key value


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: LeadingKeys FGAC tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass


def _make_client(endpoint_url: str, access_key: str, secret_key: str,
                 region: str) -> Any:
    kwargs: dict = dict(
        service_name="dynamodb",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(retries={"max_attempts": 0}),
    )
    if endpoint_url.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def _denied(exc: ClientError) -> bool:
    return exc.value.response["Error"]["Code"] == "AccessDeniedException"


@pytest.fixture(scope="module")
def auth_env():
    return _require_auth_env()


@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    endpoint, admin_user, admin_pass = auth_env
    return ManagementClient(endpoint, admin_user, admin_pass)


@pytest.fixture(scope="module")
def region() -> str:
    return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="module")
def account_id(mgmt) -> str:
    acct_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(acct_id, f"lk-test-{acct_id}")
    assert resp.status_code == 201, resp.text
    yield acct_id
    mgmt.delete_account(acct_id)


ALLOW_ALL = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
}


class TestLeadingKeys:
    """LeadingKeys enforcement on Query, batch and transaction operations."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, mgmt, account_id, region):
        self.endpoint = auth_env[0]
        self.mgmt = mgmt
        self.account_id = account_id
        self.region = region
        self._users: list[str] = []
        yield
        for u in self._users:
            try:
                self.mgmt.delete_user(self.account_id, u)
            except Exception:
                pass

    # -- helpers -----------------------------------------------------------

    def _user_client(self, policies: list[tuple[str, dict]]) -> Any:
        """Create a user with the given named policies, return a client."""
        name = f"lk-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, name, "TestPass123!")
        assert resp.status_code == 201, resp.text
        self._users.append(name)
        resp = self.mgmt.create_access_key(self.account_id, name)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        for pname, doc in policies:
            resp = self.mgmt.put_user_policy(self.account_id, name, pname, doc)
            assert resp.status_code == 204, resp.text
        return _make_client(self.endpoint, creds["access_key_id"],
                            creds["secret_access_key"], self.region)

    def _leading_keys_policy(self, qualifier: str, values: list[str],
                             table: str) -> dict:
        arn = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/{table}"
        op = f"{qualifier}:StringEquals" if qualifier else "StringEquals"
        return {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": [arn, arn + "/index/*"],
                "Condition": {op: {"dynamodb:LeadingKeys": values}},
            }],
        }

    def _make_table(self) -> tuple[str, Any]:
        """Owner (full access), a table with a sort key, one LSI and one GSI,
        seeded with an own ("alice") and an other ("bob") partition."""
        owner = f"owner-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, owner, "TestPass123!")
        assert resp.status_code == 201, resp.text
        self._users.append(owner)
        resp = self.mgmt.create_access_key(self.account_id, owner)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        resp = self.mgmt.put_user_policy(self.account_id, owner, "full", ALLOW_ALL)
        assert resp.status_code == 204, resp.text
        client = _make_client(self.endpoint, creds["access_key_id"],
                             creds["secret_access_key"], self.region)

        table = f"lk-{uuid.uuid4().hex[:8]}"
        client.create_table(
            TableName=table,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "lsi_sk", "AttributeType": "S"},
                {"AttributeName": "gpk", "AttributeType": "S"},
                {"AttributeName": "gsk", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            LocalSecondaryIndexes=[{
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsi_sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }],
            GlobalSecondaryIndexes=[{
                "IndexName": "gsi1",
                "KeySchema": [
                    {"AttributeName": "gpk", "KeyType": "HASH"},
                    {"AttributeName": "gsk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, table)
        client.put_item(TableName=table, Item={
            "pk": {"S": OWN}, "sk": {"S": "x"}, "lsi_sk": {"S": "l"},
            "gpk": {"S": G_OWN}, "gsk": {"S": "g"}, "data": {"S": "own"}})
        client.put_item(TableName=table, Item={
            "pk": {"S": OTHER}, "sk": {"S": "x"}, "lsi_sk": {"S": "l"},
            "gpk": {"S": G_OTHER}, "gsk": {"S": "g"}, "data": {"S": "other"}})
        return table, client

    @pytest.fixture()
    def table(self):
        name, owner = self._make_table()
        yield name
        try:
            owner.delete_table(TableName=name)
            wait_for_deleted(owner, name)
        except Exception:
            pass

    @staticmethod
    def _key(pk: str) -> dict:
        return {"pk": {"S": pk}, "sk": {"S": "x"}}

    # -- item operations: the pre-existing enforcement still holds ----------

    def test_item_ops_fenced_to_own_partition(self, table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        c.get_item(TableName=table, Key=self._key(OWN))
        with pytest.raises(ClientError) as e:
            c.get_item(TableName=table, Key=self._key(OTHER))
        assert _denied(e)
        c.put_item(TableName=table, Item=self._key(OWN))
        with pytest.raises(ClientError) as e:
            c.put_item(TableName=table, Item=self._key(OTHER))
        assert _denied(e)

    # -- Query: the core defect --------------------------------------------

    def test_query_own_partition_allowed(self, table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        c.query(TableName=table, KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": OWN}})

    def test_query_other_partition_denied(self, table):
        # The bypass in the report: before the fix this returned the other
        # tenant's rows instead of AccessDeniedException.
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        with pytest.raises(ClientError) as e:
            c.query(TableName=table, KeyConditionExpression="pk = :v",
                    ExpressionAttributeValues={":v": {"S": OTHER}})
        assert _denied(e)

    def test_query_other_via_expression_attribute_name_denied(self, table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        with pytest.raises(ClientError) as e:
            c.query(TableName=table, KeyConditionExpression="#p = :v",
                    ExpressionAttributeNames={"#p": "pk"},
                    ExpressionAttributeValues={":v": {"S": OTHER}})
        assert _denied(e)

    def test_query_legacy_key_conditions_fenced(self, table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        c.query(TableName=table, KeyConditions={
            "pk": {"ComparisonOperator": "EQ", "AttributeValueList": [{"S": OWN}]}})
        with pytest.raises(ClientError) as e:
            c.query(TableName=table, KeyConditions={
                "pk": {"ComparisonOperator": "EQ",
                       "AttributeValueList": [{"S": OTHER}]}})
        assert _denied(e)

    def test_query_lsi_uses_base_partition_key(self, table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        c.query(TableName=table, IndexName="lsi1",
                KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": OWN}})
        with pytest.raises(ClientError) as e:
            c.query(TableName=table, IndexName="lsi1",
                    KeyConditionExpression="pk = :v",
                    ExpressionAttributeValues={":v": {"S": OTHER}})
        assert _denied(e)

    def test_query_gsi_uses_index_partition_key(self, table):
        # Under a base-partition policy, the GSI partition value (galice) is not
        # the permitted value (alice), so the GSI query is denied.
        base = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        with pytest.raises(ClientError) as e:
            base.query(TableName=table, IndexName="gsi1",
                       KeyConditionExpression="gpk = :v",
                       ExpressionAttributeValues={":v": {"S": G_OWN}})
        assert _denied(e)
        # A policy naming the GSI partition value permits that GSI query, and
        # still denies a base-table query on the base partition (alice).
        idx = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [G_OWN], table))])
        idx.query(TableName=table, IndexName="gsi1",
                  KeyConditionExpression="gpk = :v",
                  ExpressionAttributeValues={":v": {"S": G_OWN}})
        with pytest.raises(ClientError) as e:
            idx.query(TableName=table, KeyConditionExpression="pk = :v",
                      ExpressionAttributeValues={":v": {"S": OWN}})
        assert _denied(e)

    # -- Scan: no LeadingKeys ----------------------------------------------

    def test_scan_allowed_under_for_all_values(self, table):
        # Scan targets no partition, so a ForAllValues allow is vacuously met.
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        c.scan(TableName=table)

    def test_scan_denied_under_for_any_value(self, table):
        # A ForAnyValue allow needs at least one matching key; a scan has none.
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAnyValue", [OWN], table))])
        with pytest.raises(ClientError) as e:
            c.scan(TableName=table)
        assert _denied(e)

    # -- BatchGetItem / BatchWriteItem: per-table key set ------------------

    def test_batch_get_fenced_to_own_partition(self, table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        c.batch_get_item(RequestItems={table: {"Keys": [self._key(OWN)]}})
        with pytest.raises(ClientError) as e:
            c.batch_get_item(RequestItems={table: {"Keys": [self._key(OTHER)]}})
        assert _denied(e)
        # Mixed own+other under ForAllValues: every key must match, so denied.
        with pytest.raises(ClientError) as e:
            c.batch_get_item(RequestItems={
                table: {"Keys": [self._key(OWN), self._key(OTHER)]}})
        assert _denied(e)

    def test_batch_write_fenced_to_own_partition(self, table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        c.batch_write_item(RequestItems={
            table: [{"PutRequest": {"Item": {"pk": {"S": OWN}, "sk": {"S": "bw"}}}}]})
        with pytest.raises(ClientError) as e:
            c.batch_write_item(RequestItems={
                table: [{"PutRequest": {"Item": {"pk": {"S": OTHER}, "sk": {"S": "bw"}}}}]})
        assert _denied(e)

    def test_batch_get_mixed_passes_for_any_value(self, table):
        # A batch authorizes per table over the whole key set, so ForAnyValue is
        # satisfied when any key matches, even in a mixed own+other batch. This
        # is the distinction from a transaction (next test).
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAnyValue", [OWN], table))])
        c.batch_get_item(RequestItems={
            table: {"Keys": [self._key(OWN), self._key(OTHER)]}})

    # -- TransactGetItems / TransactWriteItems: per item -------------------

    def test_transact_get_fenced_to_own_partition(self, table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        c.transact_get_items(TransactItems=[
            {"Get": {"TableName": table, "Key": self._key(OWN)}}])
        with pytest.raises(ClientError) as e:
            c.transact_get_items(TransactItems=[
                {"Get": {"TableName": table, "Key": self._key(OTHER)}}])
        assert _denied(e)

    def test_transact_write_fenced_to_own_partition(self, table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", [OWN], table))])
        c.transact_write_items(TransactItems=[
            {"Put": {"TableName": table, "Item": {"pk": {"S": OWN}, "sk": {"S": "tw"}}}}])
        with pytest.raises(ClientError) as e:
            c.transact_write_items(TransactItems=[
                {"Put": {"TableName": table, "Item": {"pk": {"S": OTHER}, "sk": {"S": "tw"}}}}])
        assert _denied(e)
        # Each sub-op kind is authorized as its own action; an other-partition
        # Update, Delete or ConditionCheck is denied too.
        for item in [
            {"Update": {"TableName": table, "Key": self._key(OTHER),
                        "UpdateExpression": "SET d = :v",
                        "ExpressionAttributeValues": {":v": {"S": "u"}}}},
            {"Delete": {"TableName": table, "Key": self._key(OTHER)}},
            {"ConditionCheck": {"TableName": table, "Key": self._key(OTHER),
                                "ConditionExpression": "attribute_exists(pk)"}},
        ]:
            with pytest.raises(ClientError) as e:
                c.transact_write_items(TransactItems=[item])
            assert _denied(e)

    def test_transact_get_mixed_denied_for_any_value(self, table):
        # A transaction authorizes each item on its own, so a ForAnyValue allow
        # does NOT pass a mixed own+other transaction: the other item fails.
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAnyValue", [OWN], table))])
        with pytest.raises(ClientError) as e:
            c.transact_get_items(TransactItems=[
                {"Get": {"TableName": table, "Key": self._key(OWN)}},
                {"Get": {"TableName": table, "Key": self._key(OTHER)}}])
        assert _denied(e)

    # -- GSI with a two-attribute partition key ----------------------------
    #
    # Live DynamoDB accepts a GSI whose partition key has two HASH attributes
    # (g1, g2); a two-HASH base table is rejected. Measured 2026-10-20: a Query
    # on such a GSI evaluates dynamodb:LeadingKeys against the FIRST HASH
    # attribute's value only. Under ForAllValues:StringEquals ["alice"] a query
    # of g1=alice was allowed for any g2 value and g1=bob was denied; a policy
    # naming the second attribute's value ("blue") denied even g1=alice,g2=blue.

    def _make_multi_hash_gsi_table(self) -> tuple[str, Any]:
        owner = f"owner-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, owner, "TestPass123!")
        assert resp.status_code == 201, resp.text
        self._users.append(owner)
        resp = self.mgmt.create_access_key(self.account_id, owner)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        resp = self.mgmt.put_user_policy(self.account_id, owner, "full", ALLOW_ALL)
        assert resp.status_code == 204, resp.text
        client = _make_client(self.endpoint, creds["access_key_id"],
                             creds["secret_access_key"], self.region)
        table = f"lk-mh-{uuid.uuid4().hex[:8]}"
        client.create_table(
            TableName=table,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "g1", "AttributeType": "S"},
                {"AttributeName": "g2", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            GlobalSecondaryIndexes=[{
                "IndexName": "gsi2",
                "KeySchema": [
                    {"AttributeName": "g1", "KeyType": "HASH"},
                    {"AttributeName": "g2", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, table)
        for pk, g1, g2 in [("i1", "alice", "blue"), ("i2", "alice", "red"),
                           ("i3", "bob", "blue"), ("i4", "bob", "red")]:
            client.put_item(TableName=table, Item={
                "pk": {"S": pk}, "g1": {"S": g1}, "g2": {"S": g2}})
        return table, client

    @pytest.fixture()
    def mh_table(self):
        name, owner = self._make_multi_hash_gsi_table()
        yield name
        try:
            owner.delete_table(TableName=name)
            wait_for_deleted(owner, name)
        except Exception:
            pass

    @staticmethod
    def _q_gsi2(client, table, g1, g2):
        return client.query(
            TableName=table, IndexName="gsi2",
            KeyConditionExpression="g1 = :a AND g2 = :b",
            ExpressionAttributeValues={":a": {"S": g1}, ":b": {"S": g2}})

    def test_multi_hash_gsi_binds_first_hash_attribute_only(self, mh_table):
        # Policy fences the first HASH attribute (g1) to "alice".
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", ["alice"], mh_table))])
        # g1=alice is allowed for either g2 value: the second attribute is not
        # part of LeadingKeys.
        self._q_gsi2(c, mh_table, "alice", "blue")
        self._q_gsi2(c, mh_table, "alice", "red")
        # g1=bob is denied regardless of g2.
        with pytest.raises(ClientError) as e:
            self._q_gsi2(c, mh_table, "bob", "blue")
        assert _denied(e)
        with pytest.raises(ClientError) as e:
            self._q_gsi2(c, mh_table, "bob", "red")
        assert _denied(e)

    def test_multi_hash_gsi_second_attribute_value_not_in_leading_keys(self, mh_table):
        # A policy naming the second HASH attribute's value ("blue") matches no
        # leading key, so even g1=alice,g2=blue is denied.
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", ["blue"], mh_table))])
        with pytest.raises(ClientError) as e:
            self._q_gsi2(c, mh_table, "alice", "blue")
        assert _denied(e)

    def test_multi_hash_gsi_base_partition_key_unchanged(self, mh_table):
        # The base table partition key still binds to its own value when the
        # multi-attribute GSI exists.
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", ["i1"], mh_table))])
        c.get_item(TableName=mh_table, Key={"pk": {"S": "i1"}})
        c.query(TableName=mh_table, KeyConditionExpression="pk = :v",
                ExpressionAttributeValues={":v": {"S": "i1"}})
        with pytest.raises(ClientError) as e:
            c.get_item(TableName=mh_table, Key={"pk": {"S": "i3"}})
        assert _denied(e)

    # -- Numeric (N) partition key rendering --------------------------------
    #
    # The service writes an N partition key into dynamodb:LeadingKeys in one of
    # two forms, depending on the operation, and matches the policy value as
    # written (measured against live DynamoDB on 2026-10-20 over 21 spellings):
    #   - GetItem, PutItem, UpdateItem, DeleteItem, Query and BatchGetItem use
    #     the normalized number, so 010, 10.0, 1E1, 1.0E1 and +10 are all "10";
    #   - BatchWriteItem (put and delete), TransactGetItems and every
    #     TransactWriteItems action use the number as Java's BigDecimal prints
    #     it: 010, +10 and 1.0E1 are "10", 10.0 is "10.0", 1E1 is "1E+1".
    # A fence of ["1E1"] matches nothing on either path.

    def _make_num_table(self) -> tuple[str, Any]:
        owner = f"owner-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, owner, "TestPass123!")
        assert resp.status_code == 201, resp.text
        self._users.append(owner)
        resp = self.mgmt.create_access_key(self.account_id, owner)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        resp = self.mgmt.put_user_policy(self.account_id, owner, "full", ALLOW_ALL)
        assert resp.status_code == 204, resp.text
        client = _make_client(self.endpoint, creds["access_key_id"],
                             creds["secret_access_key"], self.region)
        table = f"lk-n-{uuid.uuid4().hex[:8]}"
        client.create_table(
            TableName=table,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "N"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(client, table)
        client.put_item(TableName=table, Item={
            "pk": {"N": "10"}, "secret": {"S": "ten-secret"}})
        client.put_item(TableName=table, Item={
            "pk": {"N": "20"}, "secret": {"S": "twenty-secret"}})
        return table, client

    @pytest.fixture()
    def num_table(self):
        name, owner = self._make_num_table()
        yield name
        try:
            owner.delete_table(TableName=name)
            wait_for_deleted(owner, name)
        except Exception:
            pass

    # Every non-canonical spelling of the principal's own numeric partition is
    # allowed under a canonical fence, and the other partition stays denied.
    NONCANON_OWN = ["010", "10.0", "1E1", "1.0E1", "+10"]

    def test_num_item_ops_canonical_fence_allows_noncanonical_own(self, num_table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", ["10"], num_table))])
        for spelling in self.NONCANON_OWN:
            c.get_item(TableName=num_table, Key={"pk": {"N": spelling}})
        # The other partition stays denied whatever the spelling.
        with pytest.raises(ClientError) as e:
            c.get_item(TableName=num_table, Key={"pk": {"N": "20"}})
        assert _denied(e)
        with pytest.raises(ClientError) as e:
            c.get_item(TableName=num_table, Key={"pk": {"N": "2.0E1"}})
        assert _denied(e)

    def test_num_query_canonical_fence_allows_noncanonical_own(self, num_table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", ["10"], num_table))])
        for spelling in self.NONCANON_OWN:
            c.query(TableName=num_table, KeyConditionExpression="pk = :v",
                    ExpressionAttributeValues={":v": {"N": spelling}})
        with pytest.raises(ClientError) as e:
            c.query(TableName=num_table, KeyConditionExpression="pk = :v",
                    ExpressionAttributeValues={":v": {"N": "20"}})
        assert _denied(e)

    def test_num_batch_get_canonical_fence_allows_noncanonical_own(self, num_table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", ["10"], num_table))])
        # BatchGetItem compares the normalized number. 1E1 and 10.0 both
        # normalize to partition 10, so they cannot share one batch (the
        # engine rejects duplicate keys), hence one key per request.
        for spelling in self.NONCANON_OWN:
            c.batch_get_item(RequestItems={
                num_table: {"Keys": [{"pk": {"N": spelling}}]}})
        with pytest.raises(ClientError) as e:
            c.batch_get_item(RequestItems={num_table: {"Keys": [{"pk": {"N": "20"}}]}})
        assert _denied(e)
        # A mixed own+other batch under ForAllValues is denied: the other key
        # does not normalize to 10.
        with pytest.raises(ClientError) as e:
            c.batch_get_item(RequestItems={
                num_table: {"Keys": [{"pk": {"N": "010"}}, {"pk": {"N": "20"}}]}})
        assert _denied(e)

    # TransactGetItems, every TransactWriteItems action and BatchWriteItem put
    # requests compare the number as Java's BigDecimal prints it, which keeps
    # the scale the request was written with: 10.0 stays "10.0" and 1E1 is
    # "1E+1", while 010, +10 and 1.0E1 are "10".
    DECIMAL_SAME = ["10", "010", "+10", "1.0E1"]
    DECIMAL_DIFFERENT = ["10.0", "1E1"]

    def _decimal_path_ops(self, c, table: str, spelling: str) -> dict:
        key = {"pk": {"N": spelling}}
        return {
            "transact_get": lambda: c.transact_get_items(TransactItems=[
                {"Get": {"TableName": table, "Key": key}}]),
            "transact_put": lambda: c.transact_write_items(TransactItems=[
                {"Put": {"TableName": table, "Item": key}}]),
            "transact_update": lambda: c.transact_write_items(TransactItems=[
                {"Update": {"TableName": table, "Key": key,
                            "UpdateExpression": "SET a = :x",
                            "ExpressionAttributeValues": {":x": {"S": "y"}}}}]),
            "transact_delete": lambda: c.transact_write_items(TransactItems=[
                {"Delete": {"TableName": table, "Key": key}}]),
            "transact_condition_check": lambda: c.transact_write_items(TransactItems=[
                {"ConditionCheck": {"TableName": table, "Key": key,
                                    "ConditionExpression":
                                        "attribute_exists(pk) OR attribute_not_exists(pk)"}}]),
            "batch_write_put": lambda: c.batch_write_item(RequestItems={
                table: [{"PutRequest": {"Item": key}}]}),
            "batch_write_delete": lambda: c.batch_write_item(RequestItems={
                table: [{"DeleteRequest": {"Key": key}}]}),
        }

    def test_num_decimal_path_canonical_fence(self, num_table):
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", ["10"], num_table))])
        for spelling in self.DECIMAL_SAME:
            for name, op in self._decimal_path_ops(c, num_table, spelling).items():
                op()
        for spelling in self.DECIMAL_DIFFERENT:
            for name, op in self._decimal_path_ops(c, num_table, spelling).items():
                with pytest.raises(ClientError) as e:
                    op()
                assert _denied(e), (spelling, name)
        with pytest.raises(ClientError) as e:
            c.transact_get_items(TransactItems=[
                {"Get": {"TableName": num_table, "Key": {"pk": {"N": "20"}}}}])
        assert _denied(e)

    def test_num_decimal_fence_matches_only_the_decimal_path(self, num_table):
        # A fence written as "10.0" matches a request spelled 10.0 on the
        # decimal path and matches nothing on the normalized path, where every
        # spelling of 10 is "10". The same holds for "1E+1" and the spelling 1E1.
        for fence, spelling in (("10.0", "10.0"), ("1E+1", "1E1")):
            c = self._user_client([("lk", self._leading_keys_policy(
                "ForAllValues", [fence], num_table))])
            for name, op in self._decimal_path_ops(c, num_table, spelling).items():
                op()
            for other in ("10", "010", "1.0E1"):
                with pytest.raises(ClientError) as e:
                    c.transact_get_items(TransactItems=[
                        {"Get": {"TableName": num_table, "Key": {"pk": {"N": other}}}}])
                assert _denied(e), (fence, other)
            with pytest.raises(ClientError) as e:
                c.get_item(TableName=num_table, Key={"pk": {"N": spelling}})
            assert _denied(e), fence
            with pytest.raises(ClientError) as e:
                c.query(TableName=num_table, KeyConditionExpression="pk = :v",
                        ExpressionAttributeValues={":v": {"N": spelling}})
            assert _denied(e), fence
            with pytest.raises(ClientError) as e:
                c.batch_get_item(RequestItems={
                    num_table: {"Keys": [{"pk": {"N": spelling}}]}})
            assert _denied(e), fence

    def test_num_deny_statement_follows_the_operation_form(self, num_table):
        # A Deny on ["10"] matches every spelling of 10 on the normalized path,
        # and on the decimal path only the spellings whose decimal form is "10".
        arn = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/{num_table}"
        c = self._user_client([("lk", {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:*", "Resource": arn},
                {"Effect": "Deny", "Action": "dynamodb:*", "Resource": arn,
                 "Condition": {"ForAnyValue:StringEquals": {"dynamodb:LeadingKeys": ["10"]}}},
            ]})])
        for spelling in ("10", "10.0"):
            with pytest.raises(ClientError) as e:
                c.get_item(TableName=num_table, Key={"pk": {"N": spelling}})
            assert _denied(e), spelling
            with pytest.raises(ClientError) as e:
                c.delete_item(TableName=num_table, Key={"pk": {"N": spelling}})
            assert _denied(e), spelling
        decimal_ops = ("transact_get", "batch_write_put", "batch_write_delete")
        ops_10_0 = self._decimal_path_ops(c, num_table, "10.0")
        for name in decimal_ops:
            ops_10_0[name]()
        ops_10 = self._decimal_path_ops(c, num_table, "10")
        for name in decimal_ops:
            with pytest.raises(ClientError) as e:
                ops_10[name]()
            assert _denied(e), name

    def test_num_decimal_path_edge_spellings(self, num_table):
        # The decimal form of each spelling, as the service matched it on
        # TransactGetItems. The normalized form of each differs, so GetItem is
        # denied under the same fence.
        cases = {"100E-2": "1.00", "0.0000001": "1E-7", "123E2": "1.23E+4",
                 "-5.50": "-5.50", "+1.5E+3": "1.5E+3", "12.3400E-1": "1.23400",
                 "1.00E-130": "1.00E-130", "80E-1": "8.0"}
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", sorted(cases.values()), num_table))])
        for spelling in cases:
            c.transact_get_items(TransactItems=[
                {"Get": {"TableName": num_table, "Key": {"pk": {"N": spelling}}}}])
            with pytest.raises(ClientError) as e:
                c.get_item(TableName=num_table, Key={"pk": {"N": spelling}})
            assert _denied(e), spelling

    def test_num_noncanonical_fence_matches_nothing(self, num_table):
        # The policy value ["1E1"] is not normalized by the service, so no
        # request normalizes to it: every operation is denied, including a
        # request spelled 1E1 that reads canonical partition 10. Before the fix
        # ExtendDB served partition 10 here.
        c = self._user_client([("lk", self._leading_keys_policy(
            "ForAllValues", ["1E1"], num_table))])
        with pytest.raises(ClientError) as e:
            c.get_item(TableName=num_table, Key={"pk": {"N": "1E1"}})
        assert _denied(e)
        with pytest.raises(ClientError) as e:
            c.get_item(TableName=num_table, Key={"pk": {"N": "10"}})
        assert _denied(e)
        with pytest.raises(ClientError) as e:
            c.query(TableName=num_table, KeyConditionExpression="pk = :v",
                    ExpressionAttributeValues={":v": {"N": "1E1"}})
        assert _denied(e)
        with pytest.raises(ClientError) as e:
            c.batch_get_item(RequestItems={num_table: {"Keys": [{"pk": {"N": "1E1"}}]}})
        assert _denied(e)
        with pytest.raises(ClientError) as e:
            c.transact_get_items(TransactItems=[
                {"Get": {"TableName": num_table, "Key": {"pk": {"N": "1E1"}}}}])
        assert _denied(e)

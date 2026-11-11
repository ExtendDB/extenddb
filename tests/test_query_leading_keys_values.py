# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Query under each dynamodb:LeadingKeys fence form with an N value that is
not a number. The fences need users created through the extenddb management
API, so this file is skipped against the service.

Run:
  EXTENDDB_TEST_ENDPOINT=https://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_query_leading_keys_values.py -v
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import boto3
import pytest
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from conftest import scoped_table
from test_query_key_value_types import NB, NUMERIC
from test_query_value_shapes import EMPTY_AV, INVALID_VALUE, X1, _raw_query


class TestLeadingKeysWithAnInvalidKeyValue:
    """A principal under each LeadingKeys fence form gets ValidationException and
    no items for a Query whose ExpressionAttributeValues carries an N value that
    is not a number, whether the key condition, the filter or nothing reads it.

    extenddb only: the fences need users created through the management API."""

    @pytest.fixture(autouse=True)
    def setup(self):
        endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
        admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
        admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
        if not endpoint:
            pytest.skip("LeadingKeys fences need the extenddb management API")
        if not admin_user or not admin_pass:
            pytest.fail("MISCONFIGURED: EXTENDDB_ADMIN_USER and EXTENDDB_ADMIN_PASSWORD are required")
        from management_helpers import ManagementClient

        self.endpoint = endpoint
        self.region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        self.mgmt = ManagementClient(endpoint, admin_user, admin_pass)
        self.account_id = f"{uuid.uuid4().int % 10**12:012d}"
        resp = self.mgmt.create_account(self.account_id, f"kv-test-{self.account_id}")
        assert resp.status_code == 201, resp.text
        self._users: list[str] = []
        yield
        for u in self._users:
            try:
                self.mgmt.delete_user(self.account_id, u)
            except Exception:
                pass
        self.mgmt.delete_account(self.account_id)

    def _client(self, policy: dict | None) -> Any:
        name = f"kv-{uuid.uuid4().hex[:8]}"
        resp = self.mgmt.create_user(self.account_id, name, "TestPass123!")
        assert resp.status_code == 201, resp.text
        self._users.append(name)
        resp = self.mgmt.create_access_key(self.account_id, name)
        assert resp.status_code == 201, resp.text
        creds = resp.json()
        if policy is not None:
            resp = self.mgmt.put_user_policy(self.account_id, name, "p", policy)
            assert resp.status_code == 204, resp.text
        return boto3.client(
            "dynamodb", endpoint_url=self.endpoint, region_name=self.region,
            aws_access_key_id=creds["access_key_id"],
            aws_secret_access_key=creds["secret_access_key"],
            verify=False,
            config=BotoConfig(retries={"max_attempts": 0}, parameter_validation=False))

    def _fence(self, condition: dict, table: str) -> dict:
        arn = f"arn:aws:dynamodb:{self.region}:{self.account_id}:table/{table}"
        return {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": [arn, arn + "/index/*"],
                "Condition": condition,
            }],
        }

    def test_every_fence_form_gets_validation_exception_and_no_items(self):
        owner = self._client({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
        })
        failures = []
        with scoped_table(owner) as table:
            for pk in ("alice", "bob"):
                owner.put_item(TableName=table, Item={"pk": {"S": pk}})

            fences = {
                "ForAllValues": {"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["alice"]}},
                "ForAnyValue": {"ForAnyValue:StringEquals": {"dynamodb:LeadingKeys": ["alice"]}},
                "Null false": {"Null": {"dynamodb:LeadingKeys": "false"},
                               "ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["alice"]}},
                "StringEquals": {"StringEquals": {"dynamodb:LeadingKeys": "alice"}},
                "StringLike": {"StringLike": {"dynamodb:LeadingKeys": "ali*"}},
                "StringNotEquals": {"StringNotEquals": {"dynamodb:LeadingKeys": "bob"}},
            }
            # The reported shape, and an N value that only the filter reads or
            # nothing reads beside a key value the fence would bind.
            requests = {
                "pk = N bob": {"TableName": table, "KeyConditionExpression": "pk = :u",
                               "ExpressionAttributeValues": {":u": NB}},
                "pk = S alice, N bob in filter": {
                    "TableName": table, "KeyConditionExpression": "pk = :u",
                    "FilterExpression": "a = :n",
                    "ExpressionAttributeValues": {":u": {"S": "alice"}, ":n": NB}},
                "pk = S bob, N bob unused": {
                    "TableName": table, "KeyConditionExpression": "pk = :u",
                    "ExpressionAttributeValues": {":u": {"S": "bob"}, ":n": NB}},
                "pk = N bob, invalid Select": {
                    "TableName": table, "KeyConditionExpression": "pk = :u",
                    "ExpressionAttributeValues": {":u": NB}, "Select": "bad"},
            }
            clients = {label: self._client(self._fence(condition, table))
                       for label, condition in fences.items()}
            clients["owner"] = owner
            for shape, request in requests.items():
                for label, client in clients.items():
                    try:
                        resp = client.query(**request)
                        failures.append(f"{shape} / {label}: {resp['Count']} items returned")
                    except ClientError as exc:
                        err = exc.response["Error"]
                        if (err["Code"], err["Message"]) != ("ValidationException", NUMERIC + "bob"):
                            failures.append(f"{shape} / {label}: {err['Code']} {err['Message']}")
            # Beside a value with an unknown type descriptor, which boto3 cannot
            # send, every principal gets the ValidationException that names
            # :z. The service names :z for the owner (measured 2026-11-11). Its
            # answer for a fenced principal was not measured.
            body = {"TableName": table, "KeyConditionExpression": "pk = :u",
                    "ExpressionAttributeValues": {":u": NB, ":z": X1}}
            expected = INVALID_VALUE + EMPTY_AV + " for key :z"
            for label, client in clients.items():
                status, payload = _raw_query(client, body)
                kind = payload.get("__type", "").rsplit("#", 1)[-1]
                msg = payload.get("message", payload.get("Message"))
                if (status, kind, msg) != (400, "ValidationException", expected):
                    failures.append(f"N bob beside X / {label}: {status} {kind} {msg}")
        assert not failures, "\n".join(failures)

    def test_a_unicode_digit_key_value_reads_no_fenced_partition(self):
        """A partition key value written with Unicode digits is not bound to a
        LeadingKeys value, so it must not read a partition the fence keeps the
        principal from. The service's answer for a fenced principal cannot be
        measured here; the test asserts only that no item of partition 20 is
        returned, whatever the answer is."""
        owner = self._client({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
        })
        leaks = []
        with scoped_table(owner, attribute_definitions=[
                {"AttributeName": "pk", "AttributeType": "N"}]) as table:
            for pk in ("10", "20"):
                owner.put_item(TableName=table, Item={"pk": {"N": pk}, "v": {"S": "p" + pk}})
            fenced = self._client(self._fence(
                {"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": ["10"]}}, table))
            for label, value in (("fullwidth", "\uff12\uff10"), ("arabic-indic", "\u0662\u0660"),
                                 ("devanagari", "\u0968\u0966")):
                try:
                    resp = fenced.query(
                        TableName=table, KeyConditionExpression="pk = :p",
                        ExpressionAttributeValues={":p": {"N": value}})
                except ClientError:
                    continue
                if any(item.get("v") == {"S": "p20"} for item in resp["Items"]):
                    leaks.append(label)
        assert not leaks, leaks

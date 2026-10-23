# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""TableThroughputMode request member and TableThroughputModeSummary response member.

Measured against Amazon DynamoDB (2026-08-21, raw signed JSON):

- ``TableThroughputMode`` is accepted on CreateTable and UpdateTable as a
  fallback alias of ``BillingMode``: when both members are present,
  ``BillingMode`` decides and the other member is ignored. There is no
  conflict refusal.
- Enum validation is per member and pre-semantic: an invalid value fails at
  ``tableThroughputMode`` even when a valid ``BillingMode`` would win, before
  required-member validation and before the table lookup on UpdateTable.
  Multiple invalid members join under "N validation errors detected:".
- Downstream throughput validation phrases the mode as ``BillingMode`` even
  when only ``TableThroughputMode`` was sent.
- Table descriptions carry ``TableThroughputModeSummary`` as an exact mirror
  of ``BillingModeSummary``: present if and only if the sibling is present,
  same mode, identical ``LastUpdateToPayPerRequestDateTime``.

The botocore DynamoDB model (checked at botocore 1.33.13) does not yet
include either member: boto3 refuses to send ``TableThroughputMode`` and
silently drops ``TableThroughputModeSummary`` when parsing responses. All
requests and response assertions in this module therefore go over raw
SigV4-signed JSON (same approach as ``test_attribute_value_validation.py``),
against either target. boto3 fixtures are used only for cleanup.
"""

from __future__ import annotations

import json
import os
import time
import uuid

import boto3
import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

ENUM_SET = "[PROVISIONED, PAY_PER_REQUEST]"

REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

_EXTENDDB_ENDPOINT = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
ENDPOINT = _EXTENDDB_ENDPOINT or f"https://dynamodb.{REGION}.amazonaws.com/"


def _signed_post(operation: str, body: dict) -> requests.Response:
    """POST ``body`` under the DynamoDB JSON-1.0 protocol to the active target.

    Signs with the default credential chain, so it follows the same
    credentials the boto3 fixtures use for either target.
    """
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }
    creds = boto3.Session().get_credentials()
    if creds is not None:
        aws_req = AWSRequest(
            method="POST", url=ENDPOINT, data=body_bytes, headers=headers
        )
        SigV4Auth(creds.get_frozen_credentials(), "dynamodb", REGION).add_auth(aws_req)
        headers = dict(aws_req.headers)
    return requests.post(
        ENDPOINT,
        data=body_bytes,
        headers=headers,
        # The default `extenddb init` cert is self-signed; real endpoints verify.
        verify=not bool(_EXTENDDB_ENDPOINT),
    )


def _raw_ok(operation: str, body: dict) -> dict:
    resp = _signed_post(operation, body)
    assert resp.status_code == 200, f"{operation} failed: {resp.text}"
    return resp.json()


def _wait_active_raw(name: str, timeout: float = 120.0) -> None:
    interval = 0.2 if not _EXTENDDB_ENDPOINT else 0.02
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        table = _raw_ok("DescribeTable", {"TableName": name})["Table"]
        if table["TableStatus"] == "ACTIVE":
            return
        time.sleep(interval)
    raise TimeoutError(f"Table {name} did not become ACTIVE within {timeout}s")


def _key_shape() -> dict:
    return {
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
    }


PT = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}


def _unique_name() -> str:
    return f"extenddb-test-ttm-{uuid.uuid4().hex[:12]}"


def _assert_validation_error(resp: requests.Response, expected_message: str) -> None:
    assert resp.status_code == 400, f"expected 400, got {resp.status_code}: {resp.text}"
    payload = resp.json()
    assert "ValidationException" in payload.get("__type", ""), payload
    message = payload.get("message", payload.get("Message", ""))
    assert message == expected_message, f"got: {message!r}"


def _assert_summaries_mirror(desc: dict) -> None:
    """Assert the measured invariant: TableThroughputModeSummary mirrors
    BillingModeSummary exactly (presence, mode, timestamp)."""
    bms = desc.get("BillingModeSummary")
    ttms = desc.get("TableThroughputModeSummary")
    assert (bms is None) == (ttms is None), (
        f"summaries must be emitted together: BMS={bms}, TTMS={ttms}"
    )
    if bms is not None:
        assert ttms["TableThroughputMode"] == bms["BillingMode"]
        assert ttms.get("LastUpdateToPayPerRequestDateTime") == bms.get(
            "LastUpdateToPayPerRequestDateTime"
        )


@pytest.fixture()
def raw_table_cleanup(dynamodb_client):
    """Delete raw-created tables after the test, tolerating absence."""
    names: list[str] = []
    yield names
    for name in names:
        try:
            _wait_active_raw(name)
            dynamodb_client.delete_table(TableName=name)
        except Exception:
            pass


class TestCreateTableThroughputMode:
    def test_member_alone_creates_pay_per_request_table(self, raw_table_cleanup):
        name = _unique_name()
        raw_table_cleanup.append(name)
        out = _raw_ok(
            "CreateTable",
            {"TableName": name, **_key_shape(), "TableThroughputMode": "PAY_PER_REQUEST"},
        )
        desc = out["TableDescription"]
        assert desc["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        _assert_summaries_mirror(desc)
        _wait_active_raw(name)
        table = _raw_ok("DescribeTable", {"TableName": name})["Table"]
        assert table["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        assert (
            table["TableThroughputModeSummary"]["TableThroughputMode"]
            == "PAY_PER_REQUEST"
        )
        _assert_summaries_mirror(table)

    def test_billing_mode_wins_when_both_present(self, raw_table_cleanup):
        # Conflict is not refused: BillingMode decides.
        name = _unique_name()
        raw_table_cleanup.append(name)
        _raw_ok(
            "CreateTable",
            {
                "TableName": name,
                **_key_shape(),
                "BillingMode": "PROVISIONED",
                "ProvisionedThroughput": PT,
                "TableThroughputMode": "PAY_PER_REQUEST",
            },
        )
        _wait_active_raw(name)
        table = _raw_ok("DescribeTable", {"TableName": name})["Table"]
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 1
        assert "BillingModeSummary" not in table
        assert "TableThroughputModeSummary" not in table

    def test_billing_mode_pay_per_request_wins_and_rejects_throughput(self):
        # BillingMode=PAY_PER_REQUEST beats TableThroughputMode=PROVISIONED,
        # so the supplied throughput is rejected against PAY_PER_REQUEST.
        resp = _signed_post(
            "CreateTable",
            {
                "TableName": _unique_name(),
                **_key_shape(),
                "BillingMode": "PAY_PER_REQUEST",
                "TableThroughputMode": "PROVISIONED",
                "ProvisionedThroughput": PT,
            },
        )
        _assert_validation_error(
            resp,
            "One or more parameter values were invalid: Neither ReadCapacityUnits "
            "nor WriteCapacityUnits can be specified when BillingMode is "
            "PAY_PER_REQUEST",
        )

    def test_member_provisioned_with_throughput(self, raw_table_cleanup):
        name = _unique_name()
        raw_table_cleanup.append(name)
        _raw_ok(
            "CreateTable",
            {
                "TableName": name,
                **_key_shape(),
                "TableThroughputMode": "PROVISIONED",
                "ProvisionedThroughput": PT,
            },
        )
        _wait_active_raw(name)
        table = _raw_ok("DescribeTable", {"TableName": name})["Table"]
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 1
        assert "BillingModeSummary" not in table
        assert "TableThroughputModeSummary" not in table

    def test_member_provisioned_without_throughput_matches_billing_mode_path(self):
        # The member resolves into the same billing-mode logic, so the missing
        # throughput failure is byte-identical whichever member carried the mode.
        # Deliberately a relative assertion: the absolute wording of the
        # missing-throughput message differs between targets (a pre-existing,
        # separately tracked divergence), while the TTM-path/BM-path equality
        # holds on both.
        via_ttm = _signed_post(
            "CreateTable",
            {
                "TableName": _unique_name(),
                **_key_shape(),
                "TableThroughputMode": "PROVISIONED",
            },
        )
        via_bm = _signed_post(
            "CreateTable",
            {"TableName": _unique_name(), **_key_shape(), "BillingMode": "PROVISIONED"},
        )
        assert via_ttm.status_code == 400, via_ttm.text
        assert via_bm.status_code == 400, via_bm.text
        assert "ValidationException" in via_ttm.json().get("__type", "")
        assert via_ttm.json()["message"] == via_bm.json()["message"]

    def test_member_pay_per_request_with_throughput_rejected(self):
        resp = _signed_post(
            "CreateTable",
            {
                "TableName": _unique_name(),
                **_key_shape(),
                "TableThroughputMode": "PAY_PER_REQUEST",
                "ProvisionedThroughput": PT,
            },
        )
        _assert_validation_error(
            resp,
            "One or more parameter values were invalid: Neither ReadCapacityUnits "
            "nor WriteCapacityUnits can be specified when BillingMode is "
            "PAY_PER_REQUEST",
        )

    def test_invalid_enum_value(self):
        resp = _signed_post(
            "CreateTable",
            {"TableName": _unique_name(), **_key_shape(), "TableThroughputMode": "BOGUS"},
        )
        _assert_validation_error(
            resp,
            "1 validation error detected: Value 'BOGUS' at 'tableThroughputMode' "
            f"failed to satisfy constraint: Member must satisfy enum value set: {ENUM_SET}",
        )

    def test_invalid_enum_reported_even_when_billing_mode_present(self):
        # Enum validation is per member and precedes resolution: the losing
        # member still fails validation.
        resp = _signed_post(
            "CreateTable",
            {
                "TableName": _unique_name(),
                **_key_shape(),
                "BillingMode": "PAY_PER_REQUEST",
                "TableThroughputMode": "BOGUS",
            },
        )
        _assert_validation_error(
            resp,
            "1 validation error detected: Value 'BOGUS' at 'tableThroughputMode' "
            f"failed to satisfy constraint: Member must satisfy enum value set: {ENUM_SET}",
        )

    def test_both_invalid_enums_joined(self):
        resp = _signed_post(
            "CreateTable",
            {
                "TableName": _unique_name(),
                **_key_shape(),
                "BillingMode": "BOGUS1",
                "TableThroughputMode": "BOGUS2",
            },
        )
        _assert_validation_error(
            resp,
            "2 validation errors detected: Value 'BOGUS1' at 'billingMode' failed "
            f"to satisfy constraint: Member must satisfy enum value set: {ENUM_SET}; "
            "Value 'BOGUS2' at 'tableThroughputMode' failed to satisfy constraint: "
            f"Member must satisfy enum value set: {ENUM_SET}",
        )


class TestUpdateTableThroughputMode:
    def test_switch_to_provisioned_via_member(self, raw_table_cleanup):
        name = _unique_name()
        raw_table_cleanup.append(name)
        _raw_ok(
            "CreateTable",
            {"TableName": name, **_key_shape(), "BillingMode": "PAY_PER_REQUEST"},
        )
        _wait_active_raw(name)
        out = _raw_ok(
            "UpdateTable",
            {
                "TableName": name,
                "TableThroughputMode": "PROVISIONED",
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            },
        )
        _assert_summaries_mirror(out["TableDescription"])
        _wait_active_raw(name)
        table = _raw_ok("DescribeTable", {"TableName": name})["Table"]
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 5
        _assert_summaries_mirror(table)

    def test_switch_to_pay_per_request_via_member(self, raw_table_cleanup):
        name = _unique_name()
        raw_table_cleanup.append(name)
        _raw_ok(
            "CreateTable",
            {
                "TableName": name,
                **_key_shape(),
                "BillingMode": "PROVISIONED",
                "ProvisionedThroughput": PT,
            },
        )
        _wait_active_raw(name)
        _raw_ok(
            "UpdateTable",
            {"TableName": name, "TableThroughputMode": "PAY_PER_REQUEST"},
        )
        _wait_active_raw(name)
        table = _raw_ok("DescribeTable", {"TableName": name})["Table"]
        assert table["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        assert (
            table["TableThroughputModeSummary"]["TableThroughputMode"]
            == "PAY_PER_REQUEST"
        )
        _assert_summaries_mirror(table)

    def test_billing_mode_wins_when_both_present(self, raw_table_cleanup):
        name = _unique_name()
        raw_table_cleanup.append(name)
        _raw_ok(
            "CreateTable",
            {"TableName": name, **_key_shape(), "BillingMode": "PAY_PER_REQUEST"},
        )
        _wait_active_raw(name)
        _raw_ok(
            "UpdateTable",
            {
                "TableName": name,
                "BillingMode": "PROVISIONED",
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 2,
                    "WriteCapacityUnits": 2,
                },
                "TableThroughputMode": "PAY_PER_REQUEST",
            },
        )
        _wait_active_raw(name)
        table = _raw_ok("DescribeTable", {"TableName": name})["Table"]
        assert table["ProvisionedThroughput"]["ReadCapacityUnits"] == 2

    def test_member_provisioned_without_throughput_matches_billing_mode_path(
        self, raw_table_cleanup
    ):
        # UpdateTable's downstream missing-throughput message phrases the mode
        # as BillingMode whichever member carried it (measured: both paths
        # return the identical string). Deliberately a relative assertion: the
        # absolute wording differs between targets (a pre-existing, separately
        # tracked divergence), while the TTM-path/BM-path equality holds on
        # both.
        name = _unique_name()
        raw_table_cleanup.append(name)
        _raw_ok(
            "CreateTable",
            {"TableName": name, **_key_shape(), "BillingMode": "PAY_PER_REQUEST"},
        )
        _wait_active_raw(name)
        via_ttm = _signed_post(
            "UpdateTable",
            {"TableName": name, "TableThroughputMode": "PROVISIONED"},
        )
        via_bm = _signed_post(
            "UpdateTable",
            {"TableName": name, "BillingMode": "PROVISIONED"},
        )
        assert via_ttm.status_code == 400, via_ttm.text
        assert via_bm.status_code == 400, via_bm.text
        assert "ValidationException" in via_ttm.json().get("__type", "")
        assert via_ttm.json()["message"] == via_bm.json()["message"]
        assert "ProvisionedThroughput must be specified" in via_ttm.json()["message"]

    def test_invalid_enum_precedes_table_lookup(self):
        # Enum validation fires before the table lookup, so a nonexistent
        # table still reports the enum failure.
        resp = _signed_post(
            "UpdateTable",
            {
                "TableName": f"extenddb-test-nonexistent-{uuid.uuid4().hex[:8]}",
                "TableThroughputMode": "BOGUS",
            },
        )
        _assert_validation_error(
            resp,
            "1 validation error detected: Value 'BOGUS' at 'tableThroughputMode' "
            f"failed to satisfy constraint: Member must satisfy enum value set: {ENUM_SET}",
        )

    def test_both_invalid_enums_joined(self):
        resp = _signed_post(
            "UpdateTable",
            {
                "TableName": f"extenddb-test-nonexistent-{uuid.uuid4().hex[:8]}",
                "BillingMode": "B1",
                "TableThroughputMode": "B2",
            },
        )
        _assert_validation_error(
            resp,
            "2 validation errors detected: Value 'B1' at 'billingMode' failed to "
            f"satisfy constraint: Member must satisfy enum value set: {ENUM_SET}; "
            "Value 'B2' at 'tableThroughputMode' failed to satisfy constraint: "
            f"Member must satisfy enum value set: {ENUM_SET}",
        )


class TestTableThroughputModeSummary:
    def test_pay_per_request_lifecycle_carries_mirrored_summaries(
        self, raw_table_cleanup
    ):
        name = _unique_name()
        raw_table_cleanup.append(name)
        created = _raw_ok(
            "CreateTable",
            {"TableName": name, **_key_shape(), "BillingMode": "PAY_PER_REQUEST"},
        )
        desc = created["TableDescription"]
        assert desc["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        _assert_summaries_mirror(desc)
        _wait_active_raw(name)
        table = _raw_ok("DescribeTable", {"TableName": name})["Table"]
        assert (
            table["TableThroughputModeSummary"]["TableThroughputMode"]
            == "PAY_PER_REQUEST"
        )
        _assert_summaries_mirror(table)
        deleted = _raw_ok("DeleteTable", {"TableName": name})
        _assert_summaries_mirror(deleted["TableDescription"])

    def test_provisioned_table_omits_both_summaries(self, raw_table_cleanup):
        name = _unique_name()
        raw_table_cleanup.append(name)
        created = _raw_ok(
            "CreateTable",
            {
                "TableName": name,
                **_key_shape(),
                "BillingMode": "PROVISIONED",
                "ProvisionedThroughput": PT,
            },
        )
        desc = created["TableDescription"]
        assert "BillingModeSummary" not in desc
        assert "TableThroughputModeSummary" not in desc
        _wait_active_raw(name)
        table = _raw_ok("DescribeTable", {"TableName": name})["Table"]
        assert "BillingModeSummary" not in table
        assert "TableThroughputModeSummary" not in table
        deleted = _raw_ok("DeleteTable", {"TableName": name})
        desc = deleted["TableDescription"]
        assert "BillingModeSummary" not in desc
        assert "TableThroughputModeSummary" not in desc

    def test_update_table_response_mirrors_summaries(self, raw_table_cleanup):
        name = _unique_name()
        raw_table_cleanup.append(name)
        _raw_ok(
            "CreateTable",
            {"TableName": name, **_key_shape(), "BillingMode": "PAY_PER_REQUEST"},
        )
        _wait_active_raw(name)
        out = _raw_ok(
            "UpdateTable",
            {"TableName": name, "DeletionProtectionEnabled": False},
        )
        desc = out["TableDescription"]
        assert desc["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
        _assert_summaries_mirror(desc)

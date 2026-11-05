# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""The S3 actions a load performs as its caller, decided by the caller's
ExtendDB policies on whichever store the server is configured with:
s3:ListBucket on the input bucket at Start, s3:GetObject on each input
object, and s3:PutObject on each report object and the manifest."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass

import pytest

from bulk_put.bulk_helpers import *  # noqa: F401,F403
from bulk_put.bulk_helpers import (
    _boto_client,
    _provision_account,
    _release_account,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip(),
    reason="bulk operations tests require extenddb (EXTENDDB_TEST_ENDPOINT)",
)

USER = "s3caller"
TABLE = "bulk-s3-permissions"


@dataclass
class Caller:
    """A fresh account's only user, its table and its empty bucket."""

    account: str
    creds: tuple[str, str]
    client: object
    bucket: str

    def arn(self) -> str:
        return f"arn:aws:iam::{self.account}:user/{USER}"


@pytest.fixture()
def caller(mgmt):
    """A fresh account whose user holds dynamodb:* and the three S3 actions,
    with a composite-key table and an empty bucket."""
    account, creds = _provision_account(mgmt, USER)
    client = _boto_client(creds)
    bucket = f"bulk-perm-{uuid.uuid4().hex[:8]}"
    try:
        client.create_table(
            TableName=TABLE,
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
        wait_for_active(client, TABLE)
        create_bucket(bucket, account)
        yield Caller(account, creds, client, bucket)
    finally:
        try:
            client.delete_table(TableName=TABLE)
            wait_for_deleted(client, TABLE)
        except Exception:
            pass
        discard_bucket(bucket, account)
        _release_account(mgmt, account, USER)


def put_policy(mgmt, caller: Caller, name: str, statements: list[dict]) -> None:
    policy = {"Version": "2012-10-17", "Statement": statements}
    assert mgmt.put_user_policy(caller.account, USER, name, policy).status_code == 204


def write_input(caller: Caller, key: str, rows: list[dict]) -> bytes:
    """Write rows as one input object and return its bytes."""
    payload = "".join(json.dumps(r) + "\n" for r in rows).encode("utf-8")
    write_object(caller.bucket, key, payload, caller.account)
    return payload


def start(caller: Caller, **kwargs) -> dict:
    return extenddb_request(
        "StartBulkPutItems", start_body(TABLE, caller.bucket, **kwargs), caller.creds
    )["BulkOperationDescription"]


def stored(caller: Caller, pk: str, sk: str) -> bool:
    item = caller.client.get_item(
        TableName=TABLE, Key={"pk": {"S": pk}, "sk": {"S": sk}}
    )
    return "Item" in item


class TestS3Permissions:
    """Each denial on its own S3 action, with the counters, reports and
    manifest the operation ends with."""

    def test_a_listing_no_policy_allows_fails_the_operation_at_start(
        self, mgmt, caller
    ):
        # s3:ListBucket is allowed for listings under other/ only, so no
        # statement allows the load's listing under in/.
        put_policy(mgmt, caller, "s3", [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": "*",
            },
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{caller.bucket}",
                "Condition": {"StringLike": {"s3:prefix": "other/*"}},
            },
        ])
        write_input(caller, "in/rows.json", [row("a", "1")])
        write_input(caller, "other/rows.json", [row("b", "1")])

        desc = start(caller)
        assert desc["BulkOperationStatus"] == "FAILED"
        assert desc["FailureCode"] == "S3AccessDenied"
        assert desc["FailureMessage"] == (
            f"User: {caller.arn()} is not authorized to perform: s3:ListBucket "
            f'on resource: "arn:aws:s3:::{caller.bucket}" because no '
            "identity-based policy allows the s3:ListBucket action"
        )
        # Born terminal before enumeration: neither byte member is present.
        assert desc["ProcessedItemCount"] == 0
        assert "TotalInputBytes" not in desc
        assert "ProcessedInputBytes" not in desc
        arn = desc["BulkOperationArn"]
        assert read_manifest(caller.bucket, "reports", arn, caller.account) is None
        assert not stored(caller, "a", "1")

        # The listing under other/ satisfies the condition and the load runs.
        allowed = start(caller, in_prefix="other/")
        assert allowed["BulkOperationStatus"] == "IN_PROGRESS"
        terminal = wait_until_terminal(allowed["BulkOperationArn"], creds=caller.creds)
        assert terminal["BulkOperationStatus"] == "COMPLETED"
        assert terminal["ProcessedItemCount"] == 1
        assert stored(caller, "b", "1")

    def test_an_object_read_a_policy_denies_leaves_the_file_not_processed(
        self, mgmt, caller
    ):
        put_policy(mgmt, caller, "deny-read", [{
            "Effect": "Deny",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{caller.bucket}/in/*",
        }])
        payload = write_input(
            caller, "in/rows.json", [row("r", str(i)) for i in range(5)]
        )

        desc = start(caller)
        assert desc["BulkOperationStatus"] == "IN_PROGRESS"
        assert desc["TotalInputBytes"] == len(payload)
        arn = desc["BulkOperationArn"]
        terminal = wait_until_terminal(arn, creds=caller.creds)
        assert terminal["BulkOperationStatus"] == "FAILED"
        assert terminal["FailureCode"] == "S3AccessDenied"
        object_arn = f"arn:aws:s3:::{caller.bucket}/in/rows.json"
        assert terminal["FailureMessage"] == (
            f"User: {caller.arn()} is not authorized to perform: s3:GetObject "
            f'on resource: "{object_arn}" with an explicit deny in an '
            "identity-based policy"
        )
        assert terminal["TotalInputBytes"] == len(payload)
        assert terminal["ProcessedInputBytes"] == 0
        assert terminal["ProcessedItemCount"] == 0
        assert terminal["FailedItemCount"] == 0
        assert not stored(caller, "r", "0")

        # The file was refused before any of its rows was read.
        assert read_manifest(caller.bucket, "reports", arn, caller.account) == [{
            "inputSourceArn": object_arn,
            "fileProcessingStatus": "NOT_PROCESSED",
            "processedRanges": [],
            "etag": hashlib.md5(payload).hexdigest(),
        }]

    def test_a_report_write_a_policy_denies_is_held_to_the_end_of_the_input(
        self, mgmt, caller
    ):
        put_policy(mgmt, caller, "deny-write", [{
            "Effect": "Deny",
            "Action": "s3:PutObject",
            "Resource": f"arn:aws:s3:::{caller.bucket}/reports/*",
        }])
        rows = [row("w", str(i)) for i in range(10)]
        rows[4] = row("w", None)  # missing the sort key: the first error record
        write_input(caller, "in/rows.json", rows)

        desc = start(caller)
        assert desc["BulkOperationStatus"] == "IN_PROGRESS"
        arn = desc["BulkOperationArn"]
        terminal = wait_until_terminal(arn, creds=caller.creds)
        assert terminal["BulkOperationStatus"] == "FAILED"
        assert terminal["FailureCode"] == "S3AccessDenied"
        key = report_key("reports", arn, "errors", caller.account)
        assert terminal["FailureMessage"] == (
            f"User: {caller.arn()} is not authorized to perform: s3:PutObject "
            f'on resource: "arn:aws:s3:::{caller.bucket}/{key}" with an explicit '
            "deny in an identity-based policy"
        )
        # The denial found at the first report record is held while the
        # rest of the input is written, and the operation then ends with
        # the rows written, no failed row, no processed bytes and no usage.
        assert terminal["ProcessedItemCount"] == 9
        assert terminal["FailedItemCount"] == 0
        assert terminal["ProcessedInputBytes"] == 0
        usage = terminal["BulkOperationUsage"]
        assert [entry["BulkPutBytes"] for entry in usage] == [0]
        assert usage[0]["Table"]["BulkPutBytes"] == 0
        for i in (0, 1, 2, 3, 5, 6, 7, 8, 9):
            assert stored(caller, "w", str(i)), f"row {i} was not written"
        assert read_report(caller.bucket, "reports", arn, "errors", caller.account) is None
        assert read_manifest(caller.bucket, "reports", arn, caller.account) is None

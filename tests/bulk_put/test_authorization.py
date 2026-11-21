# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Authorization timing and dev-mode service."""

from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import tempfile
import time
import uuid

import pytest
import requests

from bulk_put.bulk_helpers import *  # noqa: F401,F403

pytestmark = pytest.mark.skipif(
    not os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip(),
    reason="bulk operations tests require extenddb (EXTENDDB_TEST_ENDPOINT)",
)


class TestAuthorizationTiming:
    """The DynamoDB actions of a request are authorized when the request
    arrives, so a dynamodb policy change after Start stops the principal's
    future requests without stopping the operation the principal already
    started; cancellation, which the admin can restore access for or perform
    on the caller's behalf, stops a running load. The S3 actions the
    operation performs as its caller are evaluated while it runs: an
    s3:GetObject deny added after Start fails the load at the next check.
    """

    USER = "revoked"
    RUN_ROWS = 20000

    def test_a_policy_revoked_after_start_does_not_stop_the_operation(self, mgmt):
        account_id, creds = _provision_account(mgmt, self.USER)
        client = _boto_client(creds)
        bucket = f"bulk-revoke-{uuid.uuid4().hex[:8]}"
        table = "bulk-revoke"
        allow = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}
            ],
        }
        deny = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Deny", "Action": "dynamodb:*", "Resource": "*"}
            ],
        }
        try:
            client.create_table(
                TableName=table,
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
            wait_for_active(client, table)
            rows = [
                row("k", str(i), f={"S": "x" * 40}) for i in range(self.RUN_ROWS)
            ]
            write_rows(bucket, "in/rows.json", rows, account_id)

            arn = extenddb_request(
                "StartBulkPutItems", start_body(table, bucket), creds
            )["BulkOperationDescription"]["BulkOperationArn"]

            # The revocation lands while the operation is running: the same
            # row volume keeps the per-table-guard test's second request
            # inside the run window on every attempt.
            before = extenddb_request(
                "DescribeBulkOperation", {"BulkOperationArn": arn}, creds
            )["BulkOperationDescription"]
            assert before["BulkOperationStatus"] == "IN_PROGRESS"
            assert (
                mgmt.put_user_policy(account_id, self.USER, "full", deny).status_code
                == 204
            )

            # The principal itself is refused at once (write-through cache
            # invalidation), so the deny is live while the operation runs.
            status_code, result = _signed_post(
                "DescribeBulkOperation", {"BulkOperationArn": arn}, creds
            )
            assert status_code >= 400
            assert (
                result.get("__type", "").split("#")[-1] == "AccessDeniedException"
            ), result

            # The operation itself runs to a clean completion regardless.
            assert (
                mgmt.put_user_policy(account_id, self.USER, "full", allow).status_code
                == 204
            )
            terminal = wait_until_terminal(arn, timeout=120.0, creds=creds)
            assert terminal["BulkOperationStatus"] == "COMPLETED"
            assert terminal["ProcessedItemCount"] == self.RUN_ROWS
            assert terminal["FailedItemCount"] == 0
        finally:
            try:
                mgmt.put_user_policy(account_id, self.USER, "full", allow)
                client.delete_table(TableName=table)
                wait_for_deleted(client, table)
            except Exception:
                pass
            discard_bucket(bucket, account_id)
            _release_account(mgmt, account_id, self.USER)


    def test_an_s3_get_object_deny_after_start_fails_the_operation(self, mgmt):
        user = "s3revoked"
        account_id, creds = _provision_account(mgmt, user)
        client = _boto_client(creds)
        bucket = f"bulk-s3-revoke-{uuid.uuid4().hex[:8]}"
        table = "bulk-s3-revoke"
        deny = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket}/*",
                }
            ],
        }
        try:
            client.create_table(
                TableName=table,
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
            wait_for_active(client, table)
            rows = [
                row("k", str(i), f={"S": "x" * 40}) for i in range(self.RUN_ROWS)
            ]
            write_rows(bucket, "in/rows.json", rows, account_id)

            arn = extenddb_request(
                "StartBulkPutItems", start_body(table, bucket), creds
            )["BulkOperationDescription"]["BulkOperationArn"]
            before = extenddb_request(
                "DescribeBulkOperation", {"BulkOperationArn": arn}, creds
            )["BulkOperationDescription"]
            assert before["BulkOperationStatus"] == "IN_PROGRESS"
            assert (
                mgmt.put_user_policy(account_id, user, "deny-read", deny).status_code
                == 204
            )

            terminal = wait_until_terminal(arn, timeout=120.0, creds=creds)
            assert terminal["BulkOperationStatus"] == "FAILED", terminal
            assert terminal["FailureCode"] == "S3AccessDenied"
            assert terminal["FailureMessage"] == (
                f"User: arn:aws:iam::{account_id}:user/{user} is not authorized to "
                f'perform: s3:GetObject on resource: "arn:aws:s3:::{bucket}/in/rows.json" '
                "with an explicit deny in an identity-based policy"
            )
            assert terminal["ProcessedItemCount"] < self.RUN_ROWS
            assert terminal["FailedItemCount"] == 0
        finally:
            try:
                mgmt.delete_user_policy(account_id, user, "deny-read")
                client.delete_table(TableName=table)
                wait_for_deleted(client, table)
            except Exception:
                pass
            discard_bucket(bucket, account_id)
            _release_account(mgmt, account_id, user)


# ---------------------------------------------------------------------------
# Dev mode
# ---------------------------------------------------------------------------


class TestDevMode:
    """All four operations are served in dev mode without policies.

    The seeded dev user holds no dynamodb policy, so a denial here means an
    operation bypasses the dev-mode gate. Requires a dev-mode server; gated
    the way tests/test_dev_mode_authorization.py is.
    """

    pytestmark = pytest.mark.skipif(
        os.environ.get("EXTENDDB_TEST_DEV_MODE", "").strip() != "1",
        reason="requires a dev-mode server (set EXTENDDB_TEST_DEV_MODE=1)",
    )

    def test_all_four_operations_are_served_without_policies(
        self, dynamodb_client
    ):
        name = f"bulk-dev-{uuid.uuid4().hex[:8]}"
        dynamodb_client.create_table(
            TableName=name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        wait_for_active(dynamodb_client, name)
        table_arn = dynamodb_client.describe_table(TableName=name)["Table"]["TableArn"]
        account = table_arn.split(":")[4]
        bucket = f"bulk-dev-{uuid.uuid4().hex[:8]}"

        try:
            write_rows(bucket, "in/rows.json", [row("dev1"), row("dev2")],
                       account=account)
            resp = extenddb_request(
                "StartBulkPutItems", start_body(name, bucket)
            )
            desc = resp["BulkOperationDescription"]
            assert desc["BulkOperationStatus"] == "IN_PROGRESS"
            arn = desc["BulkOperationArn"]

            described = extenddb_request(
                "DescribeBulkOperation", {"BulkOperationArn": arn}
            )["BulkOperationDescription"]
            assert described["BulkOperationArn"] == arn

            listed = list_all_operations()
            assert arn in [s["BulkOperationArn"] for s in listed]

            status_code, result = _signed_post(
                "CancelBulkOperation", {"BulkOperationArn": arn}
            )
            if status_code >= 400:
                got_type = result.get("__type", "").split("#")[-1]
                assert got_type == "ValidationException", (
                    f"cancel was denied in dev mode: {result}"
                )
                message = result.get("message", result.get("Message", ""))
                assert message.startswith(
                    "Invalid Request: Operation already in terminal state:"
                )
            wait_until_terminal(arn)
        finally:
            discard_bucket(bucket, account)
            try:
                dynamodb_client.delete_table(TableName=name)
            except Exception:
                pass

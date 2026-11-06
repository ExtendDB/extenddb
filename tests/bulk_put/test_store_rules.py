# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Behaviour that belongs to one bulk store: the rules the filesystem
store's directory layout decides, and the text each store renders for a
bucket that does not exist. Each class runs on its own store only
(EXTENDDB_TEST_BULK_STORE)."""

from __future__ import annotations

import os
import re
import tempfile
import uuid

import pytest

from bulk_put import bulk_store
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

# The tail the S3 store renders after the S3 service's message for the
# Start-time listing, and the one it renders for a report write.
LISTING_TAIL = re.compile(
    r"(?P<message>.+) \(Service: Amazon S3; Status Code: 404; Error Code: "
    r"NoSuchBucket; Request ID: [^;]+; S3 Extended Request ID: [^;]+; "
    r"Proxy: null\)"
)
REPORT_WRITE_TAIL = re.compile(
    r"(?P<message>.+) \(Service: S3, Status Code: 404, Request ID: [^,]+, "
    r"Extended Request ID: [^)]+\) \(SDK Attempt Count: 1\)"
)


def start_with_a_missing_input_bucket(bulk_table) -> tuple[dict, str]:
    absent = f"absent-{uuid.uuid4().hex[:12]}"
    desc = extenddb_request(
        "StartBulkPutItems", start_body(bulk_table, absent)
    )["BulkOperationDescription"]
    assert desc["BulkOperationStatus"] == "FAILED"
    assert desc["FailureCode"] == "S3NoSuchBucket"
    return desc, absent


def run_into_a_missing_output_bucket(dynamodb_client, bulk_table, bulk_bucket):
    """A load whose output bucket does not exist and whose third row fails.

    The fault found at the first report record is held to the end of the
    input on both stores: every good row is written and the operation ends
    with them counted, no failed row, no processed bytes and no usage.
    Returns the terminal description and the output bucket's name.
    """
    rows = [row("h", str(i)) for i in range(6)]
    rows[2] = row("h", None)
    write_rows(bulk_bucket, "in/rows.json", rows)
    absent = f"absent-{uuid.uuid4().hex[:12]}"
    body = start_body(bulk_table, bulk_bucket)
    body["OutputS3Configuration"]["S3Bucket"] = absent
    desc = extenddb_request("StartBulkPutItems", body)["BulkOperationDescription"]
    assert desc["BulkOperationStatus"] == "IN_PROGRESS"
    terminal = wait_until_terminal(desc["BulkOperationArn"])
    assert terminal["BulkOperationStatus"] == "FAILED"
    assert terminal["FailureCode"] == "S3NoSuchBucket"
    assert terminal["ProcessedItemCount"] == 5
    assert terminal["FailedItemCount"] == 0
    assert terminal["ProcessedInputBytes"] == 0
    assert terminal["BulkOperationUsage"][0]["BulkPutBytes"] == 0
    for i in (0, 1, 3, 4, 5):
        item = dynamodb_client.get_item(
            TableName=bulk_table, Key={"pk": {"S": "h"}, "sk": {"S": str(i)}}
        )
        assert "Item" in item, f"row {i} was not written"
    return terminal, absent


class TestFilesystemStore:
    """A bucket is the directory {root}/{account}/{S3Bucket}."""

    pytestmark = filesystem_store_only

    def test_a_missing_input_bucket_answers_the_stores_sentence(self, bulk_table):
        desc, _ = start_with_a_missing_input_bucket(bulk_table)
        assert desc["FailureMessage"] == "The specified bucket does not exist"

    def test_a_missing_output_bucket_answers_the_stores_sentence(
        self, dynamodb_client, bulk_table, bulk_bucket
    ):
        terminal, _ = run_into_a_missing_output_bucket(
            dynamodb_client, bulk_table, bulk_bucket
        )
        assert terminal["FailureMessage"] == "The specified bucket does not exist"

    def test_a_symlinked_file_under_the_prefix_is_not_an_input(
        self, dynamodb_client, bulk_table, bulk_bucket
    ):
        size = write_rows(bulk_bucket, "in/rows.json", [row("kept", "1")])
        outside = os.path.join(
            tempfile.gettempdir(), f"outside-{uuid.uuid4().hex[:8]}.json"
        )
        with open(outside, "w") as f:
            f.write('{"Item": {"pk": {"S": "linked"}, "sk": {"S": "1"}}}\n')
        try:
            os.symlink(outside, os.path.join(bucket_dir(bulk_bucket), "in", "link.json"))
            desc = extenddb_request(
                "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
            )["BulkOperationDescription"]
            assert desc["TotalInputBytes"] == size
            terminal = wait_until_terminal(desc["BulkOperationArn"])
            assert terminal["BulkOperationStatus"] == "COMPLETED"
            assert terminal["ProcessedItemCount"] == 1
            linked = dynamodb_client.get_item(
                TableName=bulk_table, Key={"pk": {"S": "linked"}, "sk": {"S": "1"}}
            )
            assert "Item" not in linked
        finally:
            os.remove(outside)

    def test_a_bucket_under_another_accounts_directory_is_not_found(
        self, mgmt, bulk_bucket
    ):
        write_rows(bulk_bucket, "in/rows.json", [row("mine", "1")])
        account_id, creds = _provision_account(mgmt, "neighbour")
        client = _boto_client(creds)
        table = "bulk-neighbour"
        try:
            client.create_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            wait_for_active(client, table)
            desc = extenddb_request(
                "StartBulkPutItems", start_body(table, bulk_bucket), creds
            )["BulkOperationDescription"]
            assert desc["BulkOperationStatus"] == "FAILED"
            assert desc["FailureCode"] == "S3NoSuchBucket"
            assert desc["FailureMessage"] == "The specified bucket does not exist"
        finally:
            try:
                client.delete_table(TableName=table)
                wait_for_deleted(client, table)
            except Exception:
                pass
            _release_account(mgmt, account_id, "neighbour")

    def test_an_input_owner_other_than_the_caller_is_refused_at_start(
        self, bulk_table, bulk_bucket
    ):
        write_rows(bulk_bucket, "in/rows.json", [row("o", "1")])
        body = start_body(bulk_table, bulk_bucket)
        body["InputS3Configuration"]["S3BucketOwner"] = "999999999999"
        desc = extenddb_request("StartBulkPutItems", body)["BulkOperationDescription"]
        assert desc["BulkOperationStatus"] == "FAILED"
        assert desc["FailureCode"] == "S3AccessDenied"
        assert desc["FailureMessage"] == "Access Denied"
        assert "TotalInputBytes" not in desc


class TestS3Store:
    """A bucket is a bucket on the S3-compatible service, and a fault the
    service answers is rendered with its own message."""

    pytestmark = s3_store_only

    @staticmethod
    def service_message(call) -> str:
        """The message the S3 service itself answers for a missing bucket."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as raised:
            call(bulk_store.s3_client())
        assert raised.value.response["Error"]["Code"] == "NoSuchBucket"
        return raised.value.response["Error"]["Message"]

    def test_a_missing_input_bucket_carries_the_services_message_and_tail(
        self, bulk_table
    ):
        desc, absent = start_with_a_missing_input_bucket(bulk_table)
        expected = self.service_message(
            lambda s3: s3.list_objects_v2(Bucket=absent, MaxKeys=1)
        )
        match = LISTING_TAIL.fullmatch(desc["FailureMessage"])
        assert match, desc["FailureMessage"]
        assert match.group("message") == expected

    def test_a_missing_output_bucket_carries_the_services_message_and_tail(
        self, dynamodb_client, bulk_table, bulk_bucket
    ):
        terminal, absent = run_into_a_missing_output_bucket(
            dynamodb_client, bulk_table, bulk_bucket
        )
        expected = self.service_message(
            lambda s3: s3.create_multipart_upload(Bucket=absent, Key="probe")
        )
        match = REPORT_WRITE_TAIL.fullmatch(terminal["FailureMessage"])
        assert match, terminal["FailureMessage"]
        assert match.group("message") == expected

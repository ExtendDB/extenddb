# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""ListBulkOperations paging and cross-account isolation."""

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


class TestList:
    """Pagination, ordering, and account scoping of ListBulkOperations."""

    OPERATIONS = 27

    @pytest.fixture(scope="class")
    def listed_operations(self, mgmt):
        """A fresh account holding exactly 27 operations; yields their ARNs."""
        account_id, creds = _provision_account(mgmt, "lister")
        client = _boto_client(creds)
        bucket = f"bulk-list-{uuid.uuid4().hex[:8]}"
        create_bucket(bucket, account_id)

        tables = []
        arns = []
        try:
            for i in range(self.OPERATIONS):
                name = f"bulk-list-{i:02d}"
                client.create_table(
                    TableName=name,
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    BillingMode="PAY_PER_REQUEST",
                )
                tables.append(name)
            for name in tables:
                wait_for_active(client, name)
            for name in tables:
                resp = extenddb_request(
                    "StartBulkPutItems",
                    start_body(name, bucket, in_prefix="empty/"),
                    creds,
                )
                arns.append(resp["BulkOperationDescription"]["BulkOperationArn"])
            yield account_id, creds, arns
        finally:
            for arn in arns:
                try:
                    wait_until_terminal(arn, creds=creds)
                except Exception:
                    pass
            for name in tables:
                try:
                    client.delete_table(TableName=name)
                except Exception:
                    pass
            for name in tables:
                try:
                    wait_for_deleted(client, name)
                except Exception:
                    pass
            discard_bucket(bucket, account_id)
            _release_account(mgmt, account_id, "lister")

    def test_the_default_page_holds_10_and_tokens_fetch_the_rest(
        self, listed_operations
    ):
        _, creds, arns = listed_operations
        pages = []
        body: dict = {}
        while True:
            resp = extenddb_request("ListBulkOperations", body, creds)
            pages.append(resp["BulkOperationsSummaryList"])
            if "NextToken" not in resp:
                break
            body = {"NextToken": resp["NextToken"]}
        assert [len(p) for p in pages] == [10, 10, self.OPERATIONS - 20]

        listed = [s["BulkOperationArn"] for page in pages for s in page]
        assert len(set(listed)) == self.OPERATIONS
        assert set(listed) == set(arns)
        starts = [s["StartTime"] for page in pages for s in page]
        assert starts == sorted(starts, reverse=True)

    def test_the_listing_is_ordered_newest_first(self, listed_operations):
        _, creds, _ = listed_operations
        summaries = list_all_operations(creds)
        start_times = [s["StartTime"] for s in summaries]
        assert start_times == sorted(start_times, reverse=True)

    def test_only_the_callers_operations_are_listed(
        self, listed_operations, bulk_table, bulk_bucket
    ):
        account_id, creds, _ = listed_operations
        # Plant an operation on the default account so the listing has
        # something foreign to leak; without it this test could only fail
        # if an earlier test happened to leave foreign operations behind.
        write_rows(bulk_bucket, "in/rows.json", [row("leak", "1")])
        foreign = extenddb_request(
            "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
        )["BulkOperationDescription"]["BulkOperationArn"]
        listed = [s["BulkOperationArn"] for s in list_all_operations(creds)]
        assert foreign not in listed
        for arn in listed:
            assert f":{account_id}:" in arn
        wait_until_terminal(foreign)


# ---------------------------------------------------------------------------
# Account isolation
# ---------------------------------------------------------------------------


class TestAccountIsolation:
    """A second account cannot see or touch the first account's operations."""

    @pytest.fixture(scope="class")
    def other_account(self, mgmt):
        account_id, creds = _provision_account(mgmt, "outsider")
        yield account_id, creds
        _release_account(mgmt, account_id, "outsider")

    @pytest.fixture()
    def own_operation(self, bulk_table, bulk_bucket):
        write_rows(bulk_bucket, "in/rows.json", [row("iso", "1")])
        resp = extenddb_request(
            "StartBulkPutItems", start_body(bulk_table, bulk_bucket)
        )
        arn = resp["BulkOperationDescription"]["BulkOperationArn"]
        yield arn
        wait_until_terminal(arn)

    def test_another_account_cannot_describe_the_operation(
        self, other_account, own_operation
    ):
        _, creds = other_account
        status_code, result = _signed_post(
            "DescribeBulkOperation", {"BulkOperationArn": own_operation}, creds
        )
        assert status_code == 400
        assert result["__type"].split("#")[-1] == "AccessDeniedException"
        message = result.get("message", result.get("Message", ""))
        assert message.startswith("User: ")
        assert message.endswith(
            f"is not authorized to perform: dynamodb:DescribeBulkOperation "
            f"on resource: {own_operation}"
        )

    def test_another_account_cannot_cancel_the_operation(
        self, other_account, own_operation
    ):
        _, creds = other_account
        status_code, result = _signed_post(
            "CancelBulkOperation", {"BulkOperationArn": own_operation}, creds
        )
        assert status_code == 400
        assert result["__type"].split("#")[-1] == "AccessDeniedException"
        message = result.get("message", result.get("Message", ""))
        assert message.startswith("User: ")
        assert message.endswith(
            f"is not authorized to perform: dynamodb:CancelBulkOperation "
            f"on resource: {own_operation}"
        )

    def test_a_fresh_account_lists_no_operations(self, other_account):
        _, creds = other_account
        resp = extenddb_request("ListBulkOperations", {}, creds)
        assert resp["BulkOperationsSummaryList"] == []
        assert "NextToken" not in resp


# ---------------------------------------------------------------------------
# Authorization timing
# ---------------------------------------------------------------------------

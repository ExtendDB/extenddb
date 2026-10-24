# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The DELETING window of DeleteTable: what DescribeTable and CreateTable
answer while a table is still being deleted.

Amazon DynamoDB keeps the table describable as ``TableStatus DELETING`` for a
few seconds after ``DeleteTable`` returns, and refuses ``CreateTable`` with the
same name in that window with ``ResourceInUseException``.

Against ExtendDB on PostgreSQL the table is normally gone before
``DeleteTable`` answers, so the window is held open the way a slow reader
would: an outside transaction holding ``ACCESS SHARE`` on the table's data
table makes the storage drop wait. That needs
``EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING`` (a base URL with no database
component) and the catalog database name (``EXTENDDB_TEST_PG_CATALOG_DB``,
default ``extenddb_catalog``); without them these tests skip on ExtendDB.
Against the service they run as they are.
"""

from __future__ import annotations

import os

import pytest
from botocore.exceptions import ClientError

from helpers import unique_name, wait_for_active, wait_for_deleted

PG_ADMIN = os.environ.get("EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING", "").rstrip("/")
CATALOG_DB = os.environ.get("EXTENDDB_TEST_PG_CATALOG_DB", "extenddb_catalog")


class _HeldLock:
    """``ACCESS SHARE`` on ``_ddb_<table_id>``, held until ``release``."""

    def __init__(self, table_id: str):
        import psycopg2

        with psycopg2.connect(f"{PG_ADMIN}/{CATALOG_DB}") as catalog, catalog.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = 'data_database_name'")
            row = cur.fetchone()
        data_db = row[0] if row else CATALOG_DB
        self.conn = psycopg2.connect(f"{PG_ADMIN}/{data_db}")
        cur = self.conn.cursor()
        cur.execute(f'LOCK TABLE "_ddb_{table_id}" IN ACCESS SHARE MODE')

    def release(self) -> None:
        self.conn.rollback()
        self.conn.close()


def _open_window(dynamodb_client, is_extenddb: bool, name: str) -> _HeldLock | None:
    """Create the table and, on ExtendDB, hold the lock that keeps it DELETING."""
    dynamodb_client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    wait_for_active(dynamodb_client, name)
    if not is_extenddb:
        return None
    if not PG_ADMIN:
        dynamodb_client.delete_table(TableName=name)
        pytest.skip("EXTENDDB_TEST_PG_ADMIN_CONNECTION_STRING is not set; cannot hold the table DELETING")
    table_id = dynamodb_client.describe_table(TableName=name)["Table"]["TableId"]
    return _HeldLock(table_id)


class TestDeleteTableDeletingWindow:
    def test_describe_table_reports_deleting(self, dynamodb_client, is_extenddb):
        """DeleteTable answers DELETING, and DescribeTable reports DELETING while the delete runs."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name)
        try:
            resp = dynamodb_client.delete_table(TableName=name)
            assert resp["TableDescription"]["TableStatus"] == "DELETING"
            desc = dynamodb_client.describe_table(TableName=name)
            assert desc["Table"]["TableStatus"] == "DELETING"
            assert desc["Table"]["TableName"] == name
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name)

    def test_create_table_with_deleting_name_is_in_use(self, dynamodb_client, is_extenddb):
        """CreateTable with the name of a DELETING table is ResourceInUseException."""
        name = unique_name("del-window")
        lock = _open_window(dynamodb_client, is_extenddb, name)
        try:
            dynamodb_client.delete_table(TableName=name)
            with pytest.raises(ClientError) as exc:
                dynamodb_client.create_table(
                    TableName=name,
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    BillingMode="PAY_PER_REQUEST",
                )
            err = exc.value.response["Error"]
            assert err["Code"] == "ResourceInUseException"
            # Measured on the service 2026-10-24: the same sentence as for an
            # ACTIVE table of that name.
            assert err["Message"] == f"Table already exists: {name}"
        finally:
            if lock is not None:
                lock.release()
        wait_for_deleted(dynamodb_client, name)

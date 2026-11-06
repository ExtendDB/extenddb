# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""A GSI added by UpdateTable walks the two CREATING phases the service reports.

Measured against Amazon DynamoDB on 2026-10-26 (us-east-1, a 3000-item table,
two runs) with whole strings, both refusals ValidationException:

- CREATING, ``Backfilling: false``: Query and Scan on the index answer
  ``The table does not have the specified index: gsi_slow``
- CREATING, ``Backfilling: true``: both answer
  ``Cannot read from backfilling global secondary index: gsi_slow``
- ACTIVE: ``Backfilling`` is absent and both are served

The CREATING description carries exactly the members in ``CREATING_MEMBERS``.

On the service the first phase lasted about 30 s and the second about 480 s.
ExtendDB builds a three-item index in milliseconds, so the test sets the
``gsi_creating_phase_delay_ms:<table>`` hold, which lengthens both phases, and
is therefore ExtendDB-only. Setting it needs the admin credentials that
``devtools/run-tests`` exports.
"""

from __future__ import annotations

import os
import threading
import time

import botocore.exceptions
import pytest
import requests

from helpers import make_dynamodb_client

GSI = "gsi_slow"
NOT_FOUND = f"The table does not have the specified index: {GSI}"
BACKFILLING = f"Cannot read from backfilling global secondary index: {GSI}"
CREATING_MEMBERS = {
    "Backfilling",
    "IndexArn",
    "IndexName",
    "IndexSizeBytes",
    "IndexStatus",
    "ItemCount",
    "KeySchema",
    "Projection",
    "ProvisionedThroughput",
}
HOLD_MS = 2000
ABSENT = "absent"

_extenddb_only = pytest.mark.skipif(
    os.environ.get("EXTENDDB_VALIDATION_MODE", "").lower() == "true",
    reason="extenddb_only: skipped in validation mode",
)


def _set_hold(endpoint: str, table: str, ms: int) -> None:
    password = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not password:
        pytest.fail("EXTENDDB_ADMIN_PASSWORD is required to set the GSI phase hold")
    user = os.environ.get("EXTENDDB_ADMIN_USER", "admin")
    ca_cert = os.environ.get("EXTENDDB_CA_CERT", "")
    response = requests.put(
        f"{endpoint.rstrip('/')}/management/settings/gsi_creating_phase_delay_ms:{table}",
        auth=(user, password),
        json={"value": str(ms)},
        timeout=30,
        verify=ca_cert if ca_cert else False,
    )
    assert response.ok, f"setting the phase hold failed: {response.status_code} {response.text}"


def _outcome(call, **kwargs):
    """``("OK", count)`` for a served read, ``(code, message)`` for a refusal."""
    try:
        return ("OK", call(**kwargs)["Count"])
    except botocore.exceptions.ClientError as e:
        return (e.response["Error"]["Code"], e.response["Error"]["Message"])


def _gsi(client, table: str):
    gsis = client.describe_table(TableName=table)["Table"].get("GlobalSecondaryIndexes", [])
    return next((g for g in gsis if g["IndexName"] == GSI), None)


@_extenddb_only
def test_added_gsi_reports_backfilling_and_refuses_reads_by_phase(
    dynamodb_client, endpoint_url, table_factory
):
    if endpoint_url is None:
        pytest.skip("the phase hold is an ExtendDB setting")
    table = table_factory()
    for n in range(3):
        dynamodb_client.put_item(
            TableName=table, Item={"pk": {"S": f"p{n}"}, "g": {"S": "v"}}
        )
    _set_hold(endpoint_url, table, HOLD_MS)

    # UpdateTable answers only once the index is ACTIVE on some backends, so it
    # runs on its own client in a thread while this one watches the phases.
    failures: list[Exception] = []

    def add_gsi() -> None:
        try:
            make_dynamodb_client(endpoint_url).update_table(
                TableName=table,
                AttributeDefinitions=[{"AttributeName": "g", "AttributeType": "S"}],
                GlobalSecondaryIndexUpdates=[{"Create": {
                    "IndexName": GSI,
                    "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }}],
            )
        except Exception as e:  # reported after the join
            failures.append(e)

    updater = threading.Thread(target=add_gsi)
    updater.start()
    try:
        # Phases seen, in order: 1 is Backfilling false, 2 is true, 3 is ACTIVE.
        # A reading counts only when DescribeTable agrees before and after the
        # two reads, so a phase change between them is read again.
        seen: list[int] = []
        deadline = time.monotonic() + 120
        while seen[-1:] != [3]:
            assert time.monotonic() < deadline, f"no ACTIVE state; seen {seen}"
            before = _gsi(dynamodb_client, table)
            query = _outcome(
                dynamodb_client.query,
                TableName=table,
                IndexName=GSI,
                KeyConditionExpression="g = :v",
                ExpressionAttributeValues={":v": {"S": "v"}},
            )
            scan = _outcome(dynamodb_client.scan, TableName=table, IndexName=GSI)
            after = _gsi(dynamodb_client, table)
            if before is None or before != after:
                time.sleep(0.05)
                continue
            state = (before["IndexStatus"], before.get("Backfilling", ABSENT))
            if state == ("CREATING", False):
                assert set(before) == CREATING_MEMBERS, sorted(before)
                assert query == ("ValidationException", NOT_FOUND), query
                assert scan == ("ValidationException", NOT_FOUND), scan
                phase = 1
            elif state == ("CREATING", True):
                assert set(before) == CREATING_MEMBERS, sorted(before)
                assert query == ("ValidationException", BACKFILLING), query
                assert scan == ("ValidationException", BACKFILLING), scan
                phase = 2
            elif state == ("ACTIVE", ABSENT):
                assert query == ("OK", 3), query
                assert scan == ("OK", 3), scan
                phase = 3
            else:
                pytest.fail(f"a state the service does not report: {state}")
            if seen[-1:] != [phase]:
                assert not seen or seen[-1] < phase, f"phase {phase} after {seen}"
                seen.append(phase)
            time.sleep(0.05)
        assert seen == [1, 2, 3], seen
    finally:
        updater.join(timeout=120)
        _set_hold(endpoint_url, table, 0)
    assert not failures, failures

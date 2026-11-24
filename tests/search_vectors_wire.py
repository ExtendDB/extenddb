# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Wire helpers shared by the SearchVectors condition modules.

Requests are raw SigV4-signed JSON. Against the service, SearchVectors goes to
the ``search-dynamodb`` host the SDKs use for it and every other operation to
the ``dynamodb`` host; against ExtendDB both go to ``EXTENDDB_TEST_ENDPOINT``.

The grammar a target serves an account is the equality-only grammar or the
extended grammar. Against ExtendDB with admin credentials in the environment,
``expected_grammar`` reads it from the ``search_inequality_filters`` setting,
and ``grammar_of`` fails when the target answers with the other grammar.
These helpers never change the setting. ``devtools/run-tests`` writes it,
from ``EXTENDDB_TEST_SEARCH_INEQUALITY_FILTERS``, before any suite starts.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Callable

import boto3
import pytest
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

ENDPOINT_VAR = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
ENDPOINT = ENDPOINT_VAR or f"https://dynamodb.{REGION}.amazonaws.com/"
SEARCH_ENDPOINT = ENDPOINT_VAR or f"https://search-dynamodb.{REGION}.amazonaws.com/"
SETTING = "search_inequality_filters"

EQUALITY_ONLY = (
    "Invalid SearchConditionExpression: Invalid comparator used in "
    "SearchConditionExpression"
)
INDEX_NOT_SUPPORTED = "Vector indexes are not supported by this storage backend"
SEARCH_NOT_SUPPORTED = "SearchVectors is not supported by this storage backend"

# How long a changed setting may take to reach the request path.
GRAMMAR_DEADLINE_S = 15.0


def table_name() -> str:
    """A fresh table name; ``EXTENDDB_TEST_TABLE_PREFIX`` replaces ``extenddb-test``."""
    prefix = os.environ.get("EXTENDDB_TEST_TABLE_PREFIX", "").strip() or "extenddb-test"
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def signed_post(operation: str, body: dict) -> requests.Response:
    """POST ``body`` under the DynamoDB JSON-1.0 protocol, SigV4-signed."""
    url = SEARCH_ENDPOINT if operation == "SearchVectors" else ENDPOINT
    body_bytes = json.dumps(body).encode("utf-8")
    headers = {
        "X-Amz-Target": f"DynamoDB_20120810.{operation}",
        "Content-Type": "application/x-amz-json-1.0",
    }
    creds = boto3.Session().get_credentials()
    if creds is not None:
        aws_req = AWSRequest(method="POST", url=url, data=body_bytes, headers=headers)
        SigV4Auth(creds.get_frozen_credentials(), "dynamodb", REGION).add_auth(aws_req)
        headers = dict(aws_req.headers)
    verify = True if not ENDPOINT_VAR else not url.startswith("https://")
    return requests.post(url, data=body_bytes, headers=headers, verify=verify, timeout=30)


def message(resp: requests.Response) -> str:
    payload = resp.json()
    return payload.get("message", payload.get("Message", ""))


def refused(resp: requests.Response, expected: str) -> None:
    """Refused with a ValidationException carrying exactly ``expected``."""
    assert resp.status_code == 400, resp.text
    assert resp.json()["__type"].endswith("ValidationException"), resp.text
    assert message(resp) == expected, resp.text


def expect_vectors() -> bool | None:
    """``EXTENDDB_EXPECT_VECTORS``: 1 requires vector support, 0 forbids it."""
    value = os.environ.get("EXTENDDB_EXPECT_VECTORS")
    if value is None or not ENDPOINT_VAR:
        return None
    if value not in ("0", "1"):
        pytest.fail(f"EXTENDDB_EXPECT_VECTORS must be 0 or 1, got {value!r}")
    return value == "1"


def wait_active(name: str) -> None:
    """Poll until the table and its vector indexes are all ACTIVE.

    The service takes minutes to build a vector index; ExtendDB takes well
    under a second.
    """
    timeout = 180.0 if ENDPOINT_VAR else 1800.0
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = signed_post("DescribeTable", {"TableName": name})
        if resp.status_code == 200:
            table = resp.json()["Table"]
            if table["TableStatus"] == "ACTIVE" and all(
                vi.get("IndexStatus") == "ACTIVE" for vi in table.get("VectorIndexes", [])
            ):
                return
        time.sleep(0.05 if ENDPOINT_VAR else 10.0)
    raise TimeoutError(f"table {name} did not become ACTIVE within {timeout}s")


def delete_table(name: str) -> None:
    """Delete a table, retrying only on connection errors."""
    for attempt in range(3):
        try:
            resp = signed_post("DeleteTable", {"TableName": name})
        except requests.exceptions.ConnectionError:
            if attempt == 2:
                raise
            time.sleep(5.0)
            continue
        assert resp.status_code == 200, f"DeleteTable {name} failed: {resp.text}"
        return


def create_vector_table(name: str, create_body: dict) -> None:
    """Create a table with one vector index and wait for it.

    Skips when the backend refuses vector indexes, unless
    ``EXTENDDB_EXPECT_VECTORS=1``; fails when that is 0 and the backend
    accepts one. Deletes the table when anything after the create fails.
    """
    expected = expect_vectors()
    resp = signed_post("CreateTable", {"TableName": name, **create_body})
    if resp.status_code == 400 and message(resp) == INDEX_NOT_SUPPORTED:
        if expected is True:
            pytest.fail("EXTENDDB_EXPECT_VECTORS=1 but the backend refuses vector indexes")
        pytest.skip("target backend does not support vector indexes")
    assert resp.status_code == 200, f"CreateTable failed: {resp.text}"
    try:
        assert expected is not False, (
            "EXTENDDB_EXPECT_VECTORS=0 but the backend accepted a vector index"
        )
        wait_active(name)
        # A target may accept CreateTable while ignoring VectorIndexes, and the
        # wait above then passes with no index to search.
        describe = signed_post("DescribeTable", {"TableName": name})
        assert describe.status_code == 200, describe.text
        if not describe.json()["Table"].get("VectorIndexes"):
            if expected is True:
                pytest.fail("EXTENDDB_EXPECT_VECTORS=1 but the vector index was not created")
            pytest.skip("target accepted CreateTable but did not create the vector index")
    except BaseException:
        delete_table(name)
        raise


def _settings_api() -> tuple[str, tuple[str, str]] | None:
    password = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not ENDPOINT_VAR or not password:
        return None
    user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip() or "admin"
    return f"{ENDPOINT_VAR.rstrip('/')}/management/settings/{SETTING}", (user, password)


def expected_grammar() -> str | None:
    """``equality`` or ``extended`` from the setting; ``None`` when it cannot be read.

    An absent or empty setting is every account, so the extended grammar;
    ``off`` is the equality-only grammar. A list of accounts names the
    account in ``EXTENDDB_TEST_ACCOUNT_ID``, which
    ``devtools/provision-test-credentials`` exports. A build without the
    ``search-inequality-filters`` Cargo feature has no such setting and
    serves the equality-only grammar.
    """
    api = _settings_api()
    if api is None:
        return None
    url, auth = api
    resp = requests.get(url, auth=auth, timeout=30, verify=False)
    if resp.status_code == 404:
        return "extended" if _setting_is_known(url, auth) else "equality"
    assert resp.status_code == 200, f"GET {url}: {resp.status_code} {resp.text}"
    value = resp.json()["value"].strip()
    if value == "off":
        return "equality"
    if value in ("all", ""):
        return "extended"
    account = os.environ.get("EXTENDDB_TEST_ACCOUNT_ID", "").strip()
    if not account:
        pytest.fail(
            f"{SETTING} lists accounts ({value!r}) and EXTENDDB_TEST_ACCOUNT_ID is unset"
        )
    listed = {entry.strip() for entry in value.split(",")}
    return "extended" if account in listed else "equality"


def _setting_is_known(url: str, auth: tuple[str, str]) -> bool:
    """Whether the server knows the setting, from a write of a value every
    build refuses: a build with the feature refuses the value, and one
    without it refuses the key. Neither stores anything."""
    resp = requests.put(url, json={"value": "?"}, auth=auth, timeout=30, verify=False)
    assert resp.status_code == 400, f"PUT {url}: {resp.status_code} {resp.text}"
    if "Unknown setting" in resp.text:
        return False
    assert "Invalid value" in resp.text, f"PUT {url}: {resp.text}"
    return True


def grammar_of(probe: Callable[[], requests.Response]) -> str:
    """The grammar the target serves, from ``probe``: an inequality search.

    The extended grammar answers the probe with 200 and the equality-only
    grammar refuses it with the comparator message. Against ExtendDB the
    answer must match the setting; a setting written just before the run may
    take ``GRAMMAR_DEADLINE_S`` to reach the request path.
    """
    expected = expected_grammar()
    deadline = time.monotonic() + GRAMMAR_DEADLINE_S
    while True:
        resp = probe()
        if resp.status_code == 200:
            served = "extended"
        elif resp.status_code == 400 and message(resp) == EQUALITY_ONLY:
            served = "equality"
        else:
            pytest.fail(f"neither grammar answered the probe: {resp.status_code} {resp.text}")
        if expected is None or served == expected:
            return served
        if time.monotonic() > deadline:
            pytest.fail(
                f"the {SETTING} setting gives this account the {expected} grammar, "
                f"and the target serves the {served} grammar: {resp.status_code} {resp.text}"
            )
        time.sleep(0.2)

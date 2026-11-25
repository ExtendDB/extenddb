# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""MongoDB-specific check: a backend that cannot be reached answers 503.

A data-plane request resolves the table's key schema before it runs: the
server's authorization path reads it to evaluate ``dynamodb:LeadingKeys``, and
the engine reads it again for the operation itself. A MongoDB deployment that
cannot be reached makes that read fail, and the status the client sees is
whatever the backend classified the driver error as. A transient backend fault
is a ``ServiceUnavailable`` (503), the status of a fault that clears when the
deployment comes back. Before the fix this backend answered 500
``InternalServerError``, the status of a server fault.

The fault is induced by stopping the MongoDB container, so every other request
to the same server fails while this test runs. ``devtools/run-tests
--parallel`` distributes test FILES across workers, which would put other
files' requests inside this window, so the test only runs when
``EXTENDDB_TEST_BACKEND_FAULT=1`` is set:

    EXTENDDB_TEST_BACKEND_FAULT=1 devtools/run-mongodb-tests -- \\
        --pytest --filter backend_unreachable

The credential cache and the key-info cache share one hard TTL of 60 seconds.
One request before the fault leaves the access key cached, so authentication is
served from cache and the faulted requests reach the key-info lookup rather
than failing authentication first. Each faulted request costs the driver's
30 second server-selection timeout, so the four run concurrently to finish
inside that window, and they name tables the key-info cache has never seen so
no cached entry answers for them.
"""

from __future__ import annotations

import concurrent.futures
import os
import subprocess
import time
import uuid

import pytest

from conftest import scoped_table

pytestmark = pytest.mark.skipif(
    os.environ.get("EXTENDDB_TEST_BACKEND_FAULT", "") != "1",
    reason="stops the shared MongoDB container; set EXTENDDB_TEST_BACKEND_FAULT=1 to run",
)


@pytest.fixture()
def mongodb_container() -> str:
    container = os.environ.get("EXTENDDB_TEST_MONGODB_CONTAINER", "").strip()
    if not container:
        pytest.skip("requires devtools/run-mongodb-tests")
    return container


@pytest.fixture()
def no_retry_client(dynamodb_client):
    """A client that reports the first response rather than retrying it.

    503 is retryable, so the shared session client retries it: four attempts
    at the driver's 30 second server-selection timeout outlast the 60 second
    credential cache, and the attempt that finally reports back has failed
    authentication instead, with a 500. One attempt per operation keeps every
    request inside the window this test sets up.
    """
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    if not endpoint:
        pytest.skip("requires EXTENDDB_TEST_ENDPOINT")
    return boto3.client(
        "dynamodb",
        endpoint_url=endpoint,
        region_name=dynamodb_client.meta.region_name,
        verify=False,
        config=Config(retries={"max_attempts": 0}, read_timeout=120, connect_timeout=20),
    )


def _docker(container: str, *args: str) -> None:
    subprocess.run(["docker", *args, container], check=True, capture_output=True)


def _wait_primary(container: str) -> None:
    for _ in range(60):
        probe = subprocess.run(
            ["docker", "exec", container, "mongosh", "--quiet", "--eval",
             "rs.status().members[0].stateStr"],
            capture_output=True,
            text=True,
        )
        if probe.stdout.strip().splitlines()[-1:] == ["PRIMARY"]:
            return
        time.sleep(1)
    pytest.fail("mongod did not return to PRIMARY; later tests would fail too")


def _status_code_message(client, call) -> tuple[int, str, str]:
    try:
        response = call()
    except client.exceptions.ClientError as exc:
        error = exc.response["Error"]
        return (
            exc.response["ResponseMetadata"]["HTTPStatusCode"],
            error.get("Code", ""),
            error.get("Message", ""),
        )
    return response["ResponseMetadata"]["HTTPStatusCode"], "OK", ""


def test_backend_unreachable_is_service_unavailable(
    dynamodb_client, no_retry_client, mongodb_container
):
    """GetItem, PutItem, Query and BatchGetItem answer 503 while the backend
    is unreachable."""
    # scoped_table deletes the table on exit, a failed assertion included; the
    # container is back by then, because the inner finally restarts it.
    with scoped_table(dynamodb_client) as table_name:
        # Leaves the access key in the credential cache, so the faulted
        # requests below fail on the key-info lookup rather than on
        # authentication.
        no_retry_client.get_item(TableName=table_name, Key={"pk": {"S": "warmup"}})

        cold = f"extenddb-cold-{uuid.uuid4().hex[:12]}"
        calls = {
            "GetItem": lambda: no_retry_client.get_item(
                TableName=f"{cold}-g", Key={"pk": {"S": "a"}}
            ),
            "PutItem": lambda: no_retry_client.put_item(
                TableName=f"{cold}-p", Item={"pk": {"S": "a"}}
            ),
            "Query": lambda: no_retry_client.query(
                TableName=f"{cold}-q",
                KeyConditionExpression="pk = :p",
                ExpressionAttributeValues={":p": {"S": "a"}},
            ),
            "BatchGetItem": lambda: no_retry_client.batch_get_item(
                RequestItems={f"{cold}-b": {"Keys": [{"pk": {"S": "a"}}]}}
            ),
        }

        try:
            _docker(mongodb_container, "stop")
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(calls)) as pool:
                futures = {
                    name: pool.submit(_status_code_message, no_retry_client, call)
                    for name, call in calls.items()
                }
                observed = {name: future.result() for name, future in futures.items()}
        finally:
            _docker(mongodb_container, "start")
            _wait_primary(mongodb_container)

        # The message tells a key-info 500 ("Internal server error") from an
        # authentication 500 ("Internal error during authentication"), which
        # the status and code alone cannot.
        expected = (503, "ServiceUnavailable", "Service is temporarily unavailable")
        for name, answer in observed.items():
            assert answer == expected, (
                f"{name} answered {answer} while the backend was unreachable"
            )

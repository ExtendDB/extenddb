# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Fixtures for the bulk put suite. Shared constants and request helpers live in
bulk_helpers.py so this module's name does not shadow the top-level
tests/conftest.py for the rest of the suite."""

import uuid

import pytest

from bulk_put.bulk_helpers import *  # noqa: F401,F403
from bulk_put.bulk_helpers import (  # noqa: F401
    _boto_client,
    _provision_account,
    _release_account,
    _require_admin_env,
)

@pytest.fixture(scope="module", autouse=True)
def skip_on_unsupported_backend():
    """On a backend that refuses bulk operations (MongoDB, Cassandra), assert
    the client-visible refusal before skipping the module.

    Asserting before the skip turns a regression that answered
    InternalServerError (a 500) or a different message into a failure instead
    of a silent skip. ListBulkOperations, DescribeBulkOperation and
    CancelBulkOperation reach the bulk store directly, so their refusal is
    asserted here. StartBulkPutItems reaches the store only after the [bulk]
    configuration and table-lookup steps, so its refusal is pinned at the
    handler level in the engine tests rather than assumed from the server's
    configuration here.
    """
    status_code, result = _signed_post("ListBulkOperations", {})
    if status_code < 400:
        return
    message = result.get("message", result.get("Message", ""))
    if message != MONGODB_UNSUPPORTED_MESSAGE:
        return
    arn = (
        f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:bulk-operation/"
        "00000000000001-deadbeef"
    )
    expect_error(
        "ListBulkOperations", {},
        "ValidationException", MONGODB_UNSUPPORTED_MESSAGE,
    )
    expect_error(
        "DescribeBulkOperation", {"BulkOperationArn": arn},
        "ValidationException", MONGODB_UNSUPPORTED_MESSAGE,
    )
    expect_error(
        "CancelBulkOperation", {"BulkOperationArn": arn},
        "ValidationException", MONGODB_UNSUPPORTED_MESSAGE,
    )
    pytest.skip(MONGODB_UNSUPPORTED_MESSAGE)

@pytest.fixture()
def bulk_table(dynamodb_client):
    """A composite-key table, deleted after the test."""
    name = f"extenddb-bulk-{uuid.uuid4().hex[:8]}"
    dynamodb_client.create_table(
        TableName=name,
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
    wait_for_active(dynamodb_client, name)
    yield name
    try:
        dynamodb_client.delete_table(TableName=name)
        wait_for_deleted(dynamodb_client, name)
    except Exception:
        pass

@pytest.fixture()
def bulk_bucket():
    """A unique bucket on the configured store, in the caller's account."""
    bucket = f"bulk-{uuid.uuid4().hex[:8]}"
    create_bucket(bucket)
    yield bucket
    discard_bucket(bucket)

@pytest.fixture(scope="module")
def mgmt() -> ManagementClient:
    return _require_admin_env()

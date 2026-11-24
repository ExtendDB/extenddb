# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Helpers shared by the wire tests that read ``/metrics``.

The admin environment, the ``/metrics`` read, the lookups in its rows, and
the accounts and users whose requests the tests record.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig

from management_helpers import ManagementClient


def require_env() -> tuple[str, str, str]:
    """The endpoint and the admin credentials devtools/run-tests sets."""
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: the metrics tests need EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER and EXTENDDB_ADMIN_PASSWORD, which "
            "devtools/run-tests sets."
        )
    return endpoint, admin_user, admin_pass


def admin_metrics(env: tuple[str, str, str], **params: str) -> dict[str, Any]:
    """The last hour's metrics, read as the admin; ``params`` filter them."""
    endpoint, admin_user, admin_pass = env
    resp = requests.get(
        f"{endpoint}/metrics",
        params={"window": "LastHour", **params},
        auth=(admin_user, admin_pass),
        timeout=10,
        verify=False,
    )
    assert resp.status_code == 200, f"/metrics: {resp.status_code} {resp.text[:200]}"
    return resp.json()


def dimension(metric: dict[str, Any], key: str) -> str | None:
    """The value of the dimension ``key`` of one metrics row, None if absent."""
    for d in metric["dimensions"]:
        if isinstance(d, dict) and key in d:
            return d[key]
    return None


def per_account(
    data: dict[str, Any],
    metric: str,
    operation: str | None,
    table: str | None,
    field: str = "sum",
) -> dict[str | None, float]:
    """``field`` of the rows of ``metric``, summed per AccountId.

    Only rows whose Operation is ``operation`` and whose TableName is
    ``table`` count; None asks for a row without that dimension. A count
    stays an integer, a sum a float.
    """
    sums: dict[str | None, float] = {}
    for m in data["metrics"]:
        if (
            m["metric"] == metric
            and dimension(m, "Operation") == operation
            and dimension(m, "TableName") == table
        ):
            account = dimension(m, "AccountId")
            sums[account] = sums.get(account, 0) + m[field]
    return sums


def canonical(data: dict[str, Any]) -> list[str]:
    """The rows in one order, so two reads can be compared."""
    return sorted(json.dumps(m, sort_keys=True) for m in data["metrics"])


def new_user(
    mgmt: ManagementClient,
    endpoint: str,
    account_id: str,
    name: str,
    policy: bool = True,
    validate: bool = True,
):
    """A user of ``account_id`` with an access key; returns its DynamoDB client.

    With ``policy`` the user may call every DynamoDB action, without it the
    user has no policy. The client sends each request once and checks its
    parameters only with ``validate``.
    """
    resp = mgmt.create_user(account_id, name)
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(account_id, name)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    if policy:
        document = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
        }
        resp = mgmt.put_user_policy(account_id, name, "full", document)
        assert resp.status_code == 204, resp.text
    kwargs: dict[str, Any] = dict(
        service_name="dynamodb",
        endpoint_url=endpoint,
        aws_access_key_id=creds["access_key_id"],
        aws_secret_access_key=creds["secret_access_key"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=BotoConfig(retries={"max_attempts": 0}, parameter_validation=validate),
    )
    if endpoint.startswith("https://"):
        kwargs["verify"] = False
    return boto3.client(**kwargs)


def new_account(mgmt: ManagementClient, endpoint: str, label: str, validate: bool = True):
    """A new account with a user that may call every DynamoDB action.

    Returns the account id, the user's DynamoDB client and the user's name.
    """
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(account_id, f"metrics-{label}-{account_id}")
    assert resp.status_code == 201, resp.text
    name = f"metrics-{label}"
    return account_id, new_user(mgmt, endpoint, account_id, name, validate=validate), name

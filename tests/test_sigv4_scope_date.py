# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""The credential scope date is checked before the access key is looked up.

Every SigV4 fault the service answers before it reads the access key gives the
same status, error type, message and response header set for a key that exists
and for one that does not. The scope date is one of them: measured against
dynamodb.us-east-1 on 2026-10-23 with signed ListTables requests, a scope date
of the previous day answers InvalidSignatureException "Date in Credential scope
does not match YYYYMMDD from ISO-8601 version of date from HTTP. " for an
unknown key and for a known key.

These tests need a key that exists, so they run against extenddb with builtin
auth and the management API, not against the service.

REQ-TEST-001, REQ-AUTH-001
"""

from __future__ import annotations

import datetime
import hashlib
import hmac
import os
import uuid
from urllib.parse import urlparse

import pytest
import requests

from management_helpers import ManagementClient

UNKNOWN_KEY = "AKIAEXAMPLEEXAMPLE01"
UNKNOWN_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
TARGET = "DynamoDB_20120810.ListTables"
BODY = b"{}"
SIGNED_HEADERS = "content-type;host;x-amz-date;x-amz-target"

REGION_MESSAGE = "Credential should be scoped to a valid region. "
SERVICE_MESSAGE = "Credential should be scoped to correct service: 'dynamodb'. "
SCOPE_DATE_MESSAGE = (
    "Date in Credential scope does not match YYYYMMDD from ISO-8601 "
    "version of date from HTTP. "
)


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint:
        pytest.skip("scope date tests need a key that exists, so they need extenddb")
    if not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: these tests require EXTENDDB_ADMIN_USER and "
            "EXTENDDB_ADMIN_PASSWORD. These must be set by devtools/run-tests "
            "before test execution."
        )
    return endpoint, admin_user, admin_pass


@pytest.fixture(scope="module")
def auth_env():
    return _require_auth_env()


@pytest.fixture(scope="module")
def known_key(auth_env):
    """An account, user and access key that exist, removed afterwards."""
    endpoint, admin_user, admin_pass = auth_env
    mgmt = ManagementClient(endpoint, admin_user, admin_pass)
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    resp = mgmt.create_account(account_id, f"scopedate-{account_id}")
    assert resp.status_code == 201, resp.text
    user = "scope-date-user"
    resp = mgmt.create_user(account_id, user, "TestPass123!")
    assert resp.status_code == 201, resp.text
    resp = mgmt.create_access_key(account_id, user)
    assert resp.status_code == 201, resp.text
    creds = resp.json()
    yield creds["access_key_id"], creds["secret_access_key"]
    mgmt.delete_account(account_id)


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _authorization(
    access_key: str,
    secret: str,
    host: str,
    amzdate: str,
    scope_date: str,
    region: str,
    service: str,
) -> str:
    """An Authorization header signed over the scope exactly as stated.

    The signature is computed from the same scope the header carries, so a
    request that names a bad region, service or date carries no second fault.
    """
    canonical_headers = (
        "content-type:application/x-amz-json-1.0\n"
        f"host:{host}\n"
        f"x-amz-date:{amzdate}\n"
        f"x-amz-target:{TARGET}\n"
    )
    canonical_request = "\n".join(
        [
            "POST",
            "/",
            "",
            canonical_headers,
            SIGNED_HEADERS,
            hashlib.sha256(BODY).hexdigest(),
        ]
    )
    scope = f"{scope_date}/{region}/{service}/aws4_request"
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amzdate,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signing_key = _sign(f"AWS4{secret}".encode("utf-8"), scope_date)
    signing_key = _sign(signing_key, region)
    signing_key = _sign(signing_key, service)
    signing_key = _sign(signing_key, "aws4_request")
    signature = hmac.new(
        signing_key, to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={SIGNED_HEADERS}, Signature={signature}"
    )


def _send(
    endpoint: str,
    access_key: str,
    secret: str,
    *,
    amzdate: str | None = None,
    scope_date: str | None = None,
    region: str | None = None,
    service: str = "dynamodb",
) -> tuple[int, str, str, tuple[str, ...]]:
    """Send one signed ListTables request.

    Returns the status, the error type, the message and the sorted response
    header names, which is everything a caller without a credential can read.
    """
    region = region or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    now = datetime.datetime.now(datetime.timezone.utc)
    amzdate = amzdate or now.strftime("%Y%m%dT%H%M%SZ")
    if scope_date is None:
        scope_date = amzdate[:8]
    host = urlparse(endpoint).netloc
    headers = {
        "Content-Type": "application/x-amz-json-1.0",
        "X-Amz-Target": TARGET,
        "X-Amz-Date": amzdate,
        "Authorization": _authorization(
            access_key, secret, host, amzdate, scope_date, region, service
        ),
    }
    resp = requests.post(
        endpoint,
        data=BODY,
        headers=headers,
        timeout=30,
        # D4: Self-signed certs from ``extenddb init``.
        verify=not endpoint.startswith("https://"),
    )
    try:
        payload = resp.json()
    except ValueError:
        payload = {}
    names = tuple(sorted(name.lower() for name in resp.headers))
    return resp.status_code, payload.get("__type", ""), payload.get("message", ""), names


def _stale_amzdate() -> str:
    hour_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    return hour_ago.strftime("%Y%m%dT%H%M%SZ")


def _yesterday() -> str:
    day_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    return day_ago.strftime("%Y%m%d")


class TestScopeDateBeforeTheKeyLookup:
    """A pre-lookup fault tells a client nothing about the access key."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, known_key):
        self.endpoint = auth_env[0]
        self.known_access_key, self.known_secret = known_key

    def _both_keys(self, **kwargs):
        known = _send(self.endpoint, self.known_access_key, self.known_secret, **kwargs)
        unknown = _send(self.endpoint, UNKNOWN_KEY, UNKNOWN_SECRET, **kwargs)
        return known, unknown

    def test_a_correct_request_still_tells_the_two_keys_apart(self):
        """The control: with no fault, the unknown key is reported as unknown.

        This is what makes the other cases meaningful. A key that exists gets
        past the credential lookup, so its answer is about authorization
        rather than the key.
        """
        known, unknown = self._both_keys()
        assert unknown[1].endswith("UnrecognizedClientException"), unknown
        assert not known[1].endswith("UnrecognizedClientException"), known

    @pytest.mark.parametrize(
        "name,kwargs,expected_message",
        [
            ("scope region", {"region": "us-west-2"}, REGION_MESSAGE),
            ("scope service", {"service": "s3"}, SERVICE_MESSAGE),
            ("scope date the previous day", {"scope_date": _yesterday()}, SCOPE_DATE_MESSAGE),
            ("scope date eight letters", {"scope_date": "ABCDEFGH"}, SCOPE_DATE_MESSAGE),
            ("scope date empty", {"scope_date": ""}, SCOPE_DATE_MESSAGE),
        ],
    )
    def test_scope_faults_answer_alike_for_a_known_and_an_unknown_key(
        self, name, kwargs, expected_message
    ):
        """Measured against the service on 2026-10-23 for each message."""
        known, unknown = self._both_keys(**kwargs)
        assert known == unknown, f"{name}: the two keys answered differently"
        status, error_type, message, _ = known
        assert status == 400, known
        assert error_type.endswith("InvalidSignatureException"), known
        assert message == expected_message, known

    def test_a_stale_request_date_answers_alike_for_a_known_and_an_unknown_key(self):
        """The timestamp is checked before the lookup as well.

        The error type and message for a stale date are the server's own and
        are not compared with the service here; what this asserts is that the
        two keys cannot be told apart by it.
        """
        stale = _stale_amzdate()
        known, unknown = self._both_keys(amzdate=stale)
        assert known == unknown, "a stale date: the two keys answered differently"
        assert known[0] == 400, known
        assert known[2].startswith("Signature expired"), known

    def test_a_scope_date_in_the_future_answers_alike_for_a_known_and_an_unknown_key(self):
        """A scope date ahead of X-Amz-Date is a mismatch like any other."""
        tomorrow = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
        ).strftime("%Y%m%d")
        known, unknown = self._both_keys(scope_date=tomorrow)
        assert known == unknown, "a scope date ahead: the two keys answered differently"
        assert known[2] == SCOPE_DATE_MESSAGE, known

    @pytest.mark.parametrize(
        "name,kwargs,expected_message",
        [
            (
                "region and date",
                {"region": "us-west-2"},
                REGION_MESSAGE + SCOPE_DATE_MESSAGE,
            ),
            ("service and date", {"service": "s3"}, SERVICE_MESSAGE + SCOPE_DATE_MESSAGE),
            (
                "region, service and date",
                {"region": "us-west-2", "service": "s3"},
                REGION_MESSAGE + SERVICE_MESSAGE + SCOPE_DATE_MESSAGE,
            ),
        ],
    )
    def test_more_than_one_wrong_scope_part_reports_each_complaint(
        self, name, kwargs, expected_message
    ):
        """Measured against the service on 2026-10-24: one message, date last."""
        known, unknown = self._both_keys(scope_date=_yesterday(), **kwargs)
        assert known == unknown, f"{name}: the two keys answered differently"
        assert known[2] == expected_message, known

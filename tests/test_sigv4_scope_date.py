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
    # The key may list tables, so a request that verifies answers 200.
    resp = mgmt.put_user_policy(account_id, user, "list", {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "dynamodb:ListTables", "Resource": "*"}],
    })
    assert resp.status_code in (200, 201, 204), resp.text
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
    date_line: str | None = None,
    date_header: str = "x-amz-date",
) -> str:
    """An Authorization header signed over the scope exactly as stated.

    The signature is computed from the same scope the header carries, so a
    request that names a bad region, service or date carries no second fault.
    ``date_header`` is the header that carries ``amzdate``; ``date_line`` is
    the date line of the string to sign, by default ``amzdate`` as sent.
    """
    headers = {
        "content-type": "application/x-amz-json-1.0",
        "host": host,
        date_header: amzdate,
        "x-amz-target": TARGET,
    }
    names = sorted(headers)
    signed_headers = ";".join(names)
    canonical_request = "\n".join(
        [
            "POST",
            "/",
            "",
            "".join(f"{name}:{headers[name]}\n" for name in names),
            signed_headers,
            hashlib.sha256(BODY).hexdigest(),
        ]
    )
    scope = f"{scope_date}/{region}/{service}/aws4_request"
    to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            date_line or amzdate,
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
        f"SignedHeaders={signed_headers}, Signature={signature}"
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
    date_line: str | None = None,
    date_header: str = "x-amz-date",
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
        date_header: amzdate,
        "Authorization": _authorization(
            access_key, secret, host, amzdate, scope_date, region, service, date_line, date_header
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

        A key that exists gets past the credential lookup, so its answer is
        about authorization rather than the key, and the two keys answer
        differently when no pre-lookup fault is present.
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


class TestTheDateLineOfTheStringToSign:
    """The string to sign carries the UTC basic form of the request date.

    Measured against the service on 2026-11-21 and 2026-11-22 with signed
    ListTables requests whose X-Amz-Date, or Date, is in the ISO-8601
    extended form, or carries a second or a minute of 60: the request
    verifies when the date line of the string to sign is the UTC
    YYYYMMDDTHHMMSSZ form of the instant read, fraction dropped, and fails as
    a signature mismatch when it is the value as sent or the local time. The
    x-amz-date and date canonical headers keep the value as sent.
    """

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, known_key):
        self.endpoint = auth_env[0]
        self.access_key, self.secret = known_key

    # A second of 60 reads as the next minute (measured 2026-11-22 00:52Z:
    # `20261122T005260Z` signed as `20261122T005300Z` answered 200, signed as
    # sent a mismatch), so the value shows the minute before the current one
    # with second 60. A minute of 60 reads as the next hour (`20261122T006050Z`
    # as `20261122T010050Z` answered 200, `20261122T006051Z` as sent a
    # mismatch); the measurement ran in the last quarter of the hour, when the
    # next hour is inside the window, so here the value shows the hour before
    # with minute 60 and an offset of minus the current minute (an offset's
    # minutes shift the instant, measured with `+00:99`), and the instant is
    # the current time at any hour.
    @pytest.mark.parametrize(
        "header,template,shift,line,verifies",
        [
            ("x-amz-date", "%Y%m%dT%H%M%SZ", 0, "basic", True),
            ("x-amz-date", "%Y-%m-%dT%H:%M:%SZ", 0, "sent", False),
            ("x-amz-date", "%Y-%m-%dT%H:%M:%SZ", 0, "basic", True),
            ("x-amz-date", "%Y-%m-%dT%H:%M:%S.123Z", 0, "sent", False),
            ("x-amz-date", "%Y-%m-%dT%H:%M:%S.123Z", 0, "basic", True),
            ("x-amz-date", "%Y-%m-%dT%H:%M:%S+00:00", 0, "sent", False),
            ("x-amz-date", "%Y-%m-%dT%H:%M:%S+00:00", 0, "basic", True),
            ("x-amz-date", "%Y-%m-%dT%H:%M:%S+03:00", 3 * 3600, "basic", True),
            ("x-amz-date", "%Y-%m-%dT%H:%M:%S+03:00", 3 * 3600, "local", False),
            ("date", "%Y-%m-%dT%H:%M:%SZ", 0, "basic", True),
            ("date", "%Y-%m-%dT%H:%M:%SZ", 0, "sent", False),
            ("x-amz-date", "%Y%m%dT%H%M60Z", -60, "basic", True),
            ("x-amz-date", "%Y%m%dT%H%M60Z", -60, "sent", False),
            ("x-amz-date", "%Y%m%dT%H60%S-00%M", -3600, "basic", True),
            ("x-amz-date", "%Y%m%dT%H60%S-00%M", -3600, "sent", False),
        ],
    )
    def test_the_date_line_is_the_utc_basic_form(self, header, template, shift, line, verifies):
        """``template`` shapes the date sent, showing the time ``shift`` seconds
        ahead of the current minute (the instant of every row); ``line`` is the
        date line of the string to sign: the value as sent, the UTC basic form,
        or the basic form of the local time shown."""
        now = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)
        shown = now + datetime.timedelta(seconds=shift)
        sent = shown.strftime(template)
        date_line = {
            "sent": sent,
            "basic": now.strftime("%Y%m%dT%H%M%SZ"),
            "local": shown.strftime("%Y%m%dT%H%M%SZ"),
        }[line]
        status, error_type, message, _ = _send(
            self.endpoint,
            self.access_key,
            self.secret,
            amzdate=sent,
            scope_date=now.strftime("%Y%m%d"),
            date_line=date_line,
            date_header=header,
        )
        if verifies:
            assert status == 200, (sent, date_line, status, error_type, message)
        else:
            assert status == 400, (sent, date_line, status, error_type, message)
            assert error_type.endswith("InvalidSignatureException"), (sent, date_line, message)
            assert message.startswith("The request signature we calculated does not match"), message

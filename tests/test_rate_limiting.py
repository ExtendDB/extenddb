# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Login rate limiting integration tests.

Failed logins are limited per user and source address: five failures for a
user from one address refuse that user from that address, and twenty
failures from one address refuse every user from it. These tests check both
limits on the management API, the console login form and ``/metrics``,
against the storage backend's RateLimitStore implementation.

Each test sends its attempts from loopback addresses of its own
(``127.x.y.z``, which Linux routes to the loopback interface), so the suite
records no failure against 127.0.0.1 and can be run again straight away. A
test skips when the endpoint is not a loopback address or the address
cannot be bound (macOS configures only 127.0.0.1 by default).

ExtendDB-only: rate limiting is not exercised against real DynamoDB.

Prerequisites:
  - extenddb running with ``auth.provider = "builtin"`` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars

REQ-AUTH-003
"""

from __future__ import annotations

import ipaddress
import os
import random
import socket
import uuid
from urllib.parse import urlsplit

import pytest
import requests
from requests.adapters import HTTPAdapter

from management_helpers import ManagementClient


def _require_extenddb_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.skip(
            "Rate limiting tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD."
        )
    return endpoint, admin_user, admin_pass


# MAX_FAILURES_PER_PRINCIPAL from crates/server/src/rate_limit.rs
_LOCKOUT_THRESHOLD = 5

# MAX_FAILURES_PER_IP from crates/server/src/rate_limit.rs
_SOURCE_LIMIT = 20

_TIMEOUT = 30


class _SourceAddressAdapter(HTTPAdapter):
    """Opens every connection from one local address."""

    def __init__(self, source_ip: str) -> None:
        # Set before HTTPAdapter.__init__, which calls init_poolmanager.
        self._source_address = (source_ip, 0)
        super().__init__()

    def init_poolmanager(self, *args, **kwargs):
        kwargs["source_address"] = self._source_address
        super().init_poolmanager(*args, **kwargs)


def _session_from_fresh_loopback_address(endpoint: str) -> requests.Session:
    """A session whose connections leave from a loopback address no other
    test uses, so the failures it records count against nobody else."""
    host = urlsplit(endpoint).hostname or ""
    if host != "localhost":
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None
        if address is None or address.version != 4 or not address.is_loopback:
            pytest.skip(f"sending from 127.x.y.z needs a loopback endpoint, not {host}")
    source_ip = "127.{}.{}.{}".format(
        random.randint(1, 254), random.randint(0, 255), random.randint(1, 254)
    )
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        try:
            probe.bind((source_ip, 0))
        except OSError as exc:
            pytest.skip(f"cannot send from {source_ip}: {exc}")
    session = requests.Session()
    adapter = _SourceAddressAdapter(source_ip)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class TestRateLimiting:
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        endpoint, admin_user, admin_pass = _require_extenddb_env()
        self.endpoint = endpoint.rstrip("/")
        self.mgmt = ManagementClient(endpoint, admin_user, admin_pass)
        self.verify = not endpoint.startswith("https://")

        # Address A sends the failures; address B checks that another
        # address is not affected by them.
        self.address_a = _session_from_fresh_loopback_address(endpoint)
        self.address_b = _session_from_fresh_loopback_address(endpoint)

        self.account_id = f"{uuid.uuid4().int % 10**12:012d}"
        self.user_name = f"ratelimit-{uuid.uuid4().hex[:8]}"
        self.admins: list[str] = []

        resp = self.mgmt.create_account(self.account_id, f"ratelimit-acct-{self.account_id}")
        assert resp.status_code == 201, resp.text
        resp = self.mgmt.create_user(self.account_id, self.user_name, "ValidPass123!")
        assert resp.status_code == 201, resp.text

        yield

        for name in self.admins:
            requests.delete(
                f"{self.mgmt.base_url}/admins/{name}",
                auth=self.mgmt.admin_auth,
                timeout=_TIMEOUT,
                verify=self.verify,
            )
        self.mgmt.delete_account(self.account_id)
        self.address_a.close()
        self.address_b.close()

    def _management_request_with_password(
        self, password: str, session: requests.Session | None = None
    ) -> requests.Response:
        """Hit a self-service management endpoint as the test user with the given password."""
        return (session or self.address_a).get(
            f"{self.mgmt.base_url}/accounts/{self.account_id}/users/{self.user_name}/access-keys",
            auth=(f"{self.account_id}/{self.user_name}", password),
            timeout=_TIMEOUT,
            verify=self.verify,
        )

    def _new_admin(self) -> tuple[str, str]:
        """Create an admin for this test only; teardown deletes it."""
        name = f"ratelimit-{uuid.uuid4().hex[:8]}"
        password = "AdminPass123!"
        resp = requests.post(
            f"{self.mgmt.base_url}/admins",
            json={"admin_name": name, "password": password},
            auth=self.mgmt.admin_auth,
            timeout=_TIMEOUT,
            verify=self.verify,
        )
        assert resp.status_code == 201, resp.text
        self.admins.append(name)
        return name, password

    def _get_as(
        self, session: requests.Session, path: str, user: str, password: str
    ) -> requests.Response:
        return session.get(
            f"{self.endpoint}{path}",
            auth=(user, password),
            timeout=_TIMEOUT,
            verify=self.verify,
        )

    def _console_login(
        self, session: requests.Session, user: str, password: str
    ) -> requests.Response:
        return session.post(
            f"{self.endpoint}/console/login",
            data={"username": user, "password": password},
            allow_redirects=False,
            timeout=_TIMEOUT,
            verify=self.verify,
        )

    def test_principal_locked_out_after_threshold_failures(self):
        """After MAX_FAILURES_PER_PRINCIPAL bad passwords the user is refused from that address."""
        for i in range(_LOCKOUT_THRESHOLD):
            resp = self._management_request_with_password("wrongpassword")
            assert resp.status_code == 401, (
                f"Expected 401 on attempt {i + 1}, got {resp.status_code}: {resp.text}"
            )

        # The next attempt should be rejected with 429, not 401.
        resp = self._management_request_with_password("wrongpassword")
        assert resp.status_code == 429, (
            f"Expected 429 (lockout) after {_LOCKOUT_THRESHOLD} failures, "
            f"got {resp.status_code}: {resp.text}"
        )
        assert "too many" in resp.text.lower(), (
            f"Expected lockout message in response body, got: {resp.text}"
        )

    def test_correct_password_still_works_before_lockout(self):
        """Fewer than threshold failures do not refuse the user."""
        for _ in range(_LOCKOUT_THRESHOLD - 1):
            resp = self._management_request_with_password("wrongpassword")
            assert resp.status_code == 401

        # Valid password should still succeed.
        resp = self._management_request_with_password("ValidPass123!")
        assert resp.status_code == 200, (
            f"Expected 200 with correct password before lockout, "
            f"got {resp.status_code}: {resp.text}"
        )

    def test_failures_from_one_address_do_not_refuse_the_user_from_another(self):
        """The per-user limit counts failures from one source address only."""
        for _ in range(_LOCKOUT_THRESHOLD):
            assert self._management_request_with_password("wrongpassword").status_code == 401
        resp = self._management_request_with_password("ValidPass123!")
        assert resp.status_code == 429, f"address A should be refused: {resp.text}"

        resp = self._management_request_with_password("ValidPass123!", self.address_b)
        assert resp.status_code == 200, (
            f"Expected 200 from another address, got {resp.status_code}: {resp.text}"
        )

    def test_an_address_is_refused_after_twenty_failures_across_users(self):
        """MAX_FAILURES_PER_IP failures for different users refuse every user from that address."""
        run = uuid.uuid4().hex[:8]
        for n in range(_SOURCE_LIMIT):
            resp = self._get_as(self.address_a, "/management/admins", f"nobody-{run}-{n}", "x")
            assert resp.status_code == 401, (
                f"Expected 401 for user {n + 1}, got {resp.status_code}: {resp.text}"
            )

        newcomer = f"nobody-{run}-new"
        resp = self._get_as(self.address_a, "/management/admins", newcomer, "x")
        assert resp.status_code == 429, (
            f"Expected 429 after {_SOURCE_LIMIT} failures from one address, "
            f"got {resp.status_code}: {resp.text}"
        )
        assert "from this address" in resp.text, resp.text

        resp = self._get_as(self.address_b, "/management/admins", newcomer, "x")
        assert resp.status_code == 401, (
            f"Expected 401 from another address, got {resp.status_code}: {resp.text}"
        )

    def test_admin_wrong_password_is_counted_on_the_management_api(self):
        name, password = self._new_admin()
        for i in range(_LOCKOUT_THRESHOLD):
            resp = self._get_as(self.address_a, "/management/accounts", name, "wrongpassword")
            assert resp.status_code == 401, (
                f"Expected 401 on attempt {i + 1}, got {resp.status_code}: {resp.text}"
            )
        resp = self._get_as(self.address_a, "/management/accounts", name, password)
        assert resp.status_code == 429, (
            f"Expected 429 for the right password after {_LOCKOUT_THRESHOLD} failures, "
            f"got {resp.status_code}: {resp.text}"
        )

        resp = self._get_as(self.address_b, "/management/accounts", name, password)
        assert resp.status_code == 200, (
            f"Expected 200 from another address, got {resp.status_code}: {resp.text}"
        )

    def test_console_login_failures_are_counted(self):
        name, password = self._new_admin()
        for i in range(_LOCKOUT_THRESHOLD):
            resp = self._console_login(self.address_a, name, "wrongpassword")
            assert resp.status_code == 200, (
                f"Expected the login form on attempt {i + 1}, got {resp.status_code}"
            )
            assert "Invalid credentials" in resp.text
        resp = self._console_login(self.address_a, name, password)
        assert resp.status_code == 429, (
            f"Expected 429 for the right password after {_LOCKOUT_THRESHOLD} failures, "
            f"got {resp.status_code}"
        )
        assert "too many" in resp.text.lower()

        resp = self._console_login(self.address_b, name, password)
        assert resp.status_code == 303, (
            f"Expected a redirect after login from another address, got {resp.status_code}"
        )

    def test_metrics_login_failures_are_counted(self):
        name, password = self._new_admin()
        for i in range(_LOCKOUT_THRESHOLD):
            resp = self._get_as(self.address_a, "/metrics", name, "wrongpassword")
            assert resp.status_code == 401, (
                f"Expected 401 on attempt {i + 1}, got {resp.status_code}: {resp.text}"
            )
        resp = self._get_as(self.address_a, "/metrics", name, password)
        assert resp.status_code == 429, (
            f"Expected 429 for the right password after {_LOCKOUT_THRESHOLD} failures, "
            f"got {resp.status_code}: {resp.text}"
        )

        resp = self._get_as(self.address_b, "/metrics", name, password)
        assert resp.status_code == 200, (
            f"Expected 200 from another address, got {resp.status_code}: {resp.text}"
        )

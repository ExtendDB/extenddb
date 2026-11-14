# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Console sessions end when their principal is deleted or its password changes.

A console session is bound to the password hash its principal logged in with.
Every console request reads the principal's hash from the catalog again, so a
session stops working at its next request after the principal is deleted or
its password is changed, on any server that shares the catalog.

Prerequisites match tests/test_console_integration.py:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD

The cross-server test also needs EXTENDDB_TEST_PEER_ENDPOINT, a second server
on the same catalog. It is skipped when that variable is unset.
"""

from __future__ import annotations

import os
import uuid

import pytest
import requests

from management_helpers import ManagementClient


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: console session tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _password() -> str:
    return uuid.uuid4().hex


class Console:
    """One browser: a cookie jar talking to one server's console."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = requests.Session()
        # Self-signed certs from ``extenddb init``. Passed on each request:
        # a session-level ``verify = False`` loses to REQUESTS_CA_BUNDLE.
        self.verify = not self.base_url.startswith("https://")

    def login(self, username: str, password: str) -> requests.Response:
        return self.http.post(
            f"{self.base_url}/console/login",
            data={"username": username, "password": password},
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )

    def dashboard(self) -> requests.Response:
        return self.http.get(
            f"{self.base_url}/console",
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )


def logged_in(base_url: str, username: str, password: str) -> Console:
    console = Console(base_url)
    resp = console.login(username, password)
    assert resp.status_code == 303, f"login as {username}: {resp.status_code} {resp.text[:200]}"
    assert_reaches_console(console)
    return console


def assert_reaches_console(console: Console) -> None:
    resp = console.dashboard()
    assert resp.status_code == 200, f"expected the dashboard, got {resp.status_code}"
    assert "Dashboard" in resp.text


def assert_sent_to_login(console: Console) -> None:
    resp = console.dashboard()
    assert resp.status_code == 303, f"expected the login redirect, got {resp.status_code}"
    assert resp.headers.get("location") == "/console/login"


class Admins:
    """Creates admins through the management API and deletes leftovers."""

    def __init__(self, mgmt: ManagementClient) -> None:
        self.mgmt = mgmt
        self.created: list[str] = []

    def _request(self, method: str, path: str, body: dict | None = None) -> requests.Response:
        return requests.request(
            method,
            f"{self.mgmt.base_url}/admins{path}",
            json=body,
            auth=self.mgmt.admin_auth,
            timeout=self.mgmt.timeout,
            verify=self.mgmt.verify,
        )

    def create(self) -> tuple[str, str]:
        name, password = _unique("sess-rev"), _password()
        resp = self._request("POST", "", {"admin_name": name, "password": password})
        assert resp.status_code == 201, resp.text
        self.created.append(name)
        return name, password

    def delete(self, name: str) -> None:
        resp = self._request("DELETE", f"/{name}")
        assert resp.status_code == 204, resp.text

    def change_password(self, name: str, password: str) -> None:
        resp = self._request("PUT", f"/{name}/password", {"password": password})
        assert resp.status_code == 204, resp.text

    def cleanup(self) -> None:
        for name in self.created:
            self._request("DELETE", f"/{name}")


@pytest.fixture(scope="module")
def auth_env() -> tuple[str, str, str]:
    return _require_auth_env()


@pytest.fixture(scope="module")
def endpoint(auth_env) -> str:
    return auth_env[0]


@pytest.fixture(scope="module")
def mgmt(auth_env) -> ManagementClient:
    return ManagementClient(auth_env[0], auth_env[1], auth_env[2])


@pytest.fixture()
def admins(mgmt):
    registry = Admins(mgmt)
    yield registry
    registry.cleanup()


class TestConsoleSessionRevocation:
    def test_deleted_admin_is_sent_to_login_and_other_admin_keeps_access(
        self, endpoint, admins
    ):
        alice, alice_pw = admins.create()
        bob, bob_pw = admins.create()
        alice_console = logged_in(endpoint, alice, alice_pw)
        bob_console = logged_in(endpoint, bob, bob_pw)

        admins.delete(alice)

        assert_sent_to_login(alice_console)
        assert_reaches_console(bob_console)

    def test_admin_password_change_ends_session_and_new_password_logs_in(
        self, endpoint, admins
    ):
        name, old_pw = admins.create()
        old_console = logged_in(endpoint, name, old_pw)

        new_pw = _password()
        admins.change_password(name, new_pw)

        assert_sent_to_login(old_console)
        logged_in(endpoint, name, new_pw)

    def test_iam_user_session_ends_on_password_change_and_delete(self, endpoint, mgmt):
        account_id = f"{uuid.uuid4().int % 10**12:012d}"
        user, first_pw = _unique("sess-rev"), _password()
        resp = mgmt.create_account(account_id, _unique("sess-rev"))
        assert resp.status_code == 201, resp.text
        try:
            resp = mgmt.create_user(account_id, user, first_pw)
            assert resp.status_code == 201, resp.text
            first_console = logged_in(endpoint, f"{account_id}/{user}", first_pw)

            second_pw = _password()
            resp = requests.put(
                f"{mgmt.base_url}/accounts/{account_id}/users/{user}/password",
                json={"password": second_pw},
                auth=mgmt.admin_auth,
                timeout=mgmt.timeout,
                verify=mgmt.verify,
            )
            assert resp.status_code == 204, resp.text
            assert_sent_to_login(first_console)

            second_console = logged_in(endpoint, f"{account_id}/{user}", second_pw)
            resp = mgmt.delete_user(account_id, user)
            assert resp.status_code == 204, resp.text
            assert_sent_to_login(second_console)
        finally:
            mgmt.delete_user(account_id, user)
            mgmt.delete_account(account_id)

    def test_deletion_through_one_server_ends_session_on_another(self, admins):
        peer = os.environ.get("EXTENDDB_TEST_PEER_ENDPOINT", "").strip()
        if not peer:
            pytest.skip("EXTENDDB_TEST_PEER_ENDPOINT is not set")
        name, password = admins.create()
        peer_console = logged_in(peer, name, password)

        # Deleted through EXTENDDB_TEST_ENDPOINT, checked on the peer at once.
        admins.delete(name)

        assert_sent_to_login(peer_console)

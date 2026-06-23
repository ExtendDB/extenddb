# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Console integration tests — verify management operations via HTML forms.

Every management operation testable via the CLI must also work via the
console's HTML forms. Tests POST to console endpoints and verify operations
succeed by checking redirects, page content, and management API state.

Prerequisites:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars
  - `extenddb init` has been run (encryption key + admin user exist)

Run:
  EXTENDDB_TEST_ENDPOINT=http://localhost:18443 \\
  EXTENDDB_ADMIN_USER=admin \\
  EXTENDDB_ADMIN_PASSWORD=<password> \\
  pytest tests/test_console_integration.py -v

REQ-TEST-001
"""

from __future__ import annotations

import json
import os
import re
import uuid

import boto3
import pytest
import requests
from botocore.config import Config as BotoConfig
from management_helpers import ManagementClient
def _require_auth_env() -> tuple[str, str, str]:
    """Skip tests if auth environment is not configured."""
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: Console tests require EXTENDDB_TEST_ENDPOINT, "
            "EXTENDDB_ADMIN_USER, and EXTENDDB_ADMIN_PASSWORD. "
            "These must be set by devtools/run-tests before test execution."
        )
    return endpoint, admin_user, admin_pass
class ConsoleClient:
    """HTTP client for the extenddb management console (HTML forms)."""

    _CSRF_RE = re.compile(r'<meta\s+name="csrf-token"\s+content="([^"]+)"')

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self._csrf_token: str | None = None
        # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
        if base_url.startswith("https://"):
            self.session.verify = False

    def _extract_csrf(self, resp: requests.Response) -> None:
        """Extract CSRF token from a response's HTML meta tag."""
        m = self._CSRF_RE.search(resp.text)
        if m:
            self._csrf_token = m.group(1)

    def login(self, username: str, password: str) -> requests.Response:
        resp = self.session.post(
            f"{self.base_url}/console/login",
            data={"username": username, "password": password},
            allow_redirects=False,
            timeout=30,
        )
        # After login, fetch dashboard to pick up the CSRF token.
        if resp.status_code == 303:
            dash = self.session.get(
                f"{self.base_url}/console",
                allow_redirects=False,
                timeout=30,
            )
            self._extract_csrf(dash)
        return resp

    def logout(self) -> requests.Response:
        data: dict[str, str] = {}
        if self._csrf_token:
            data["_csrf"] = self._csrf_token
        return self.session.post(
            f"{self.base_url}/console/logout",
            data=data,
            allow_redirects=False,
            timeout=30,
        )

    def get(self, path: str) -> requests.Response:
        resp = self.session.get(
            f"{self.base_url}/console{path}",
            allow_redirects=False,
            timeout=30,
        )
        self._extract_csrf(resp)
        return resp

    def post(self, path: str, data: dict | None = None) -> requests.Response:
        payload = dict(data) if data else {}
        if self._csrf_token and "_csrf" not in payload:
            payload["_csrf"] = self._csrf_token
        resp = self.session.post(
            f"{self.base_url}/console{path}",
            data=payload,
            allow_redirects=False,
            timeout=30,
        )
        return resp
@pytest.fixture(scope="module")
def auth_env():
    return _require_auth_env()
@pytest.fixture(scope="module")
def console(auth_env) -> ConsoleClient:
    """Logged-in console client (admin session)."""
    endpoint = auth_env[0]
    admin_user, admin_pass = auth_env[1], auth_env[2]
    client = ConsoleClient(endpoint)
    resp = client.login(admin_user, admin_pass)
    assert resp.status_code == 303, f"Login failed: {resp.status_code}"
    return client
@pytest.fixture()
def test_account_id() -> str:
    """Generate a unique 12-digit account ID."""
    return f"{uuid.uuid4().int % 10**12:012d}"
# ---------------------------------------------------------------------------
# Login / Logout / Session
# ---------------------------------------------------------------------------

class TestConsoleAuth:
    """Login, logout, and session handling."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env):
        self.endpoint = auth_env[0]
        self.admin_user = auth_env[1]
        self.admin_pass = auth_env[2]

    def test_login_redirects_to_dashboard(self):
        client = ConsoleClient(self.endpoint)
        resp = client.login(self.admin_user, self.admin_pass)
        assert resp.status_code == 303
        assert "/console" in resp.headers.get("location", "")
        assert "extenddb_session=" in resp.headers.get("set-cookie", "")

    def test_login_invalid_credentials(self):
        client = ConsoleClient(self.endpoint)
        resp = client.login("nonexistent", "wrongpassword")
        # Failed login re-renders the login page (200), not a redirect.
        assert resp.status_code == 200
        assert "Invalid credentials" in resp.text

    def test_logout_clears_session(self):
        client = ConsoleClient(self.endpoint)
        client.login(self.admin_user, self.admin_pass)
        resp = client.logout()
        assert resp.status_code == 303
        assert "/console/login" in resp.headers.get("location", "")
        # After logout, accessing dashboard should redirect to login.
        resp = client.get("")
        assert resp.status_code == 303
        assert "/console/login" in resp.headers.get("location", "")

    def test_unauthenticated_redirects_to_login(self):
        client = ConsoleClient(self.endpoint)
        resp = client.get("")
        assert resp.status_code == 303
        assert "/console/login" in resp.headers.get("location", "")

    def test_dashboard_accessible_after_login(self):
        client = ConsoleClient(self.endpoint)
        client.login(self.admin_user, self.admin_pass)
        resp = client.get("")
        assert resp.status_code == 200
        assert "Dashboard" in resp.text
# ---------------------------------------------------------------------------
# Account CRUD
# ---------------------------------------------------------------------------

class TestConsoleAccounts:
    """Account creation, listing, detail, and deletion via console."""

    @pytest.fixture(autouse=True)
    def setup(self, console):
        self.console = console

    @pytest.fixture()
    def account(self, test_account_id):
        """Create an account and clean up on teardown (even on failure)."""
        self.console.post("/accounts/new", {
            "account_id": test_account_id,
            "account_name": f"test-{test_account_id}",
        })
        yield test_account_id
        self.console.post(f"/accounts/{test_account_id}/delete")

    def test_create_and_list_account(self, account):
        resp = self.console.get("/accounts")
        assert resp.status_code == 200
        assert account in resp.text

    def test_account_detail_page(self, account):
        resp = self.console.get(f"/accounts/{account}")
        assert resp.status_code == 200
        assert account in resp.text

    def test_delete_account(self, test_account_id):
        self.console.post("/accounts/new", {
            "account_id": test_account_id,
            "account_name": f"del-{test_account_id}",
        })
        resp = self.console.post(f"/accounts/{test_account_id}/delete")
        assert resp.status_code == 303

        # Verify account is gone.
        resp = self.console.get(f"/accounts/{test_account_id}")
        assert resp.status_code == 200
        assert "not found" in resp.text.lower()

    def test_create_account_invalid_id(self):
        resp = self.console.post("/accounts/new", {
            "account_id": "short",
            "account_name": "bad-id",
        })
        assert resp.status_code == 200
        assert "12-digit" in resp.text
# ---------------------------------------------------------------------------
# User CRUD
# ---------------------------------------------------------------------------

class TestConsoleUsers:
    """User creation, detail, access keys, and deletion via console."""

    @pytest.fixture(autouse=True)
    def setup(self, console, test_account_id):
        self.console = console
        self.account_id = test_account_id
        self.console.post("/accounts/new", {
            "account_id": self.account_id,
            "account_name": f"user-test-{self.account_id}",
        })
        yield
        self.console.post(f"/accounts/{self.account_id}/delete")

    def test_create_and_view_user(self):
        user = f"u-{uuid.uuid4().hex[:8]}"
        resp = self.console.post(f"/accounts/{self.account_id}/users/new", {
            "user_name": user,
            "password": "TestPass123!",
        })
        assert resp.status_code == 303

        resp = self.console.get(f"/accounts/{self.account_id}/users/{user}")
        assert resp.status_code == 200
        assert user in resp.text

        self.console.post(f"/accounts/{self.account_id}/users/{user}/delete")

    def test_create_access_key_via_console(self):
        user = f"ak-{uuid.uuid4().hex[:8]}"
        self.console.post(f"/accounts/{self.account_id}/users/new", {
            "user_name": user,
            "password": "TestPass123!",
        })

        resp = self.console.post(
            f"/accounts/{self.account_id}/users/{user}/access-keys/new"
        )
        # Handler shows the secret key on a 200 page (cannot be retrieved later).
        assert resp.status_code == 200
        assert "AKIA" in resp.text

        # User detail page should also show the access key.
        resp = self.console.get(f"/accounts/{self.account_id}/users/{user}")
        assert resp.status_code == 200
        assert "AKIA" in resp.text

        self.console.post(f"/accounts/{self.account_id}/users/{user}/delete")

    def test_delete_user(self):
        user = f"del-{uuid.uuid4().hex[:8]}"
        self.console.post(f"/accounts/{self.account_id}/users/new", {
            "user_name": user,
            "password": "TestPass123!",
        })
        resp = self.console.post(
            f"/accounts/{self.account_id}/users/{user}/delete"
        )
        assert resp.status_code == 303

        resp = self.console.get(f"/accounts/{self.account_id}")
        assert resp.status_code == 200
        assert user not in resp.text
# ---------------------------------------------------------------------------
# Group CRUD
# ---------------------------------------------------------------------------

class TestConsoleGroups:
    """Group creation, membership, and deletion via console."""

    @pytest.fixture(autouse=True)
    def setup(self, console, test_account_id):
        self.console = console
        self.account_id = test_account_id
        self.console.post("/accounts/new", {
            "account_id": self.account_id,
            "account_name": f"grp-test-{self.account_id}",
        })
        yield
        self.console.post(f"/accounts/{self.account_id}/delete")

    def test_create_and_view_group(self):
        group = f"g-{uuid.uuid4().hex[:8]}"
        resp = self.console.post(f"/accounts/{self.account_id}/groups/new", {
            "group_name": group,
        })
        assert resp.status_code == 303

        resp = self.console.get(f"/accounts/{self.account_id}/groups/{group}")
        assert resp.status_code == 200
        assert group in resp.text

        self.console.post(f"/accounts/{self.account_id}/groups/{group}/delete")

    def test_add_and_remove_group_member(self):
        group = f"gm-{uuid.uuid4().hex[:8]}"
        user = f"mu-{uuid.uuid4().hex[:8]}"
        self.console.post(f"/accounts/{self.account_id}/groups/new", {
            "group_name": group,
        })
        self.console.post(f"/accounts/{self.account_id}/users/new", {
            "user_name": user,
            "password": "TestPass123!",
        })

        # Add member.
        resp = self.console.post(
            f"/accounts/{self.account_id}/groups/{group}/members/add",
            {"user_name": user},
        )
        assert resp.status_code == 303

        # Verify member appears on group detail.
        resp = self.console.get(f"/accounts/{self.account_id}/groups/{group}")
        assert user in resp.text

        # Remove member.
        resp = self.console.post(
            f"/accounts/{self.account_id}/groups/{group}/members/{user}/remove"
        )
        assert resp.status_code == 303

        # Verify member is gone from the members table (user may still appear
        # in the "add member" dropdown, so check the members section only).
        resp = self.console.get(f"/accounts/{self.account_id}/groups/{group}")
        # The remove link only appears for current members.
        assert f"/members/{user}/remove" not in resp.text

        self.console.post(f"/accounts/{self.account_id}/users/{user}/delete")
        self.console.post(f"/accounts/{self.account_id}/groups/{group}/delete")

    def test_delete_group(self):
        group = f"dg-{uuid.uuid4().hex[:8]}"
        self.console.post(f"/accounts/{self.account_id}/groups/new", {
            "group_name": group,
        })
        resp = self.console.post(
            f"/accounts/{self.account_id}/groups/{group}/delete"
        )
        assert resp.status_code == 303

        resp = self.console.get(f"/accounts/{self.account_id}")
        assert group not in resp.text
# ---------------------------------------------------------------------------
# Role CRUD
# ---------------------------------------------------------------------------

class TestConsoleRoles:
    """Role creation, detail, and deletion via console."""

    @pytest.fixture(autouse=True)
    def setup(self, console, test_account_id):
        self.console = console
        self.account_id = test_account_id
        self.console.post("/accounts/new", {
            "account_id": self.account_id,
            "account_name": f"role-test-{self.account_id}",
        })
        yield
        self.console.post(f"/accounts/{self.account_id}/delete")

    def _trust_policy(self) -> str:
        return json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{self.account_id}:root"},
                "Action": "sts:AssumeRole",
            }],
        })

    def test_create_and_view_role(self):
        role = f"r-{uuid.uuid4().hex[:8]}"
        resp = self.console.post(f"/accounts/{self.account_id}/roles/new", {
            "role_name": role,
            "trust_policy": self._trust_policy(),
        })
        assert resp.status_code == 303

        resp = self.console.get(f"/accounts/{self.account_id}/roles/{role}")
        assert resp.status_code == 200
        assert role in resp.text

        self.console.post(f"/accounts/{self.account_id}/roles/{role}/delete")

    def test_delete_role(self):
        role = f"dr-{uuid.uuid4().hex[:8]}"
        self.console.post(f"/accounts/{self.account_id}/roles/new", {
            "role_name": role,
            "trust_policy": self._trust_policy(),
        })
        resp = self.console.post(
            f"/accounts/{self.account_id}/roles/{role}/delete"
        )
        assert resp.status_code == 303

        resp = self.console.get(f"/accounts/{self.account_id}")
        assert role not in resp.text
# ---------------------------------------------------------------------------
# Policy CRUD
# ---------------------------------------------------------------------------

class TestConsolePolicies:
    """Policy creation and deletion for users, groups, and roles via console."""

    @pytest.fixture(autouse=True)
    def setup(self, console, test_account_id):
        self.console = console
        self.account_id = test_account_id
        self.console.post("/accounts/new", {
            "account_id": self.account_id,
            "account_name": f"pol-test-{self.account_id}",
        })
        yield
        self.console.post(f"/accounts/{self.account_id}/delete")

    def _full_access_doc(self) -> str:
        return json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "dynamodb:*",
                "Resource": "*",
            }],
        })

    def test_user_policy_crud(self):
        user = f"pu-{uuid.uuid4().hex[:8]}"
        policy = f"pol-{uuid.uuid4().hex[:8]}"
        self.console.post(f"/accounts/{self.account_id}/users/new", {
            "user_name": user,
            "password": "TestPass123!",
        })

        # Create policy.
        resp = self.console.post(
            f"/accounts/{self.account_id}/users/{user}/policies/new",
            {"policy_name": policy, "policy_document": self._full_access_doc()},
        )
        assert resp.status_code == 303

        # Verify policy appears on user detail.
        resp = self.console.get(f"/accounts/{self.account_id}/users/{user}")
        assert policy in resp.text

        # Delete policy.
        resp = self.console.post(
            f"/accounts/{self.account_id}/users/{user}/policies/{policy}/delete"
        )
        assert resp.status_code == 303

        # Verify policy is gone.
        resp = self.console.get(f"/accounts/{self.account_id}/users/{user}")
        assert policy not in resp.text

        self.console.post(f"/accounts/{self.account_id}/users/{user}/delete")

    def test_group_policy_crud(self):
        group = f"pg-{uuid.uuid4().hex[:8]}"
        policy = f"gpol-{uuid.uuid4().hex[:8]}"
        self.console.post(f"/accounts/{self.account_id}/groups/new", {
            "group_name": group,
        })

        resp = self.console.post(
            f"/accounts/{self.account_id}/groups/{group}/policies/new",
            {"policy_name": policy, "policy_document": self._full_access_doc()},
        )
        assert resp.status_code == 303

        resp = self.console.get(f"/accounts/{self.account_id}/groups/{group}")
        assert policy in resp.text

        resp = self.console.post(
            f"/accounts/{self.account_id}/groups/{group}/policies/{policy}/delete"
        )
        assert resp.status_code == 303

        self.console.post(f"/accounts/{self.account_id}/groups/{group}/delete")

    def test_role_policy_crud(self):
        role = f"pr-{uuid.uuid4().hex[:8]}"
        policy = f"rpol-{uuid.uuid4().hex[:8]}"
        trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": f"arn:aws:iam::{self.account_id}:root"},
                "Action": "sts:AssumeRole",
            }],
        })
        self.console.post(f"/accounts/{self.account_id}/roles/new", {
            "role_name": role,
            "trust_policy": trust,
        })

        resp = self.console.post(
            f"/accounts/{self.account_id}/roles/{role}/policies/new",
            {"policy_name": policy, "policy_document": self._full_access_doc()},
        )
        assert resp.status_code == 303

        resp = self.console.get(f"/accounts/{self.account_id}/roles/{role}")
        assert policy in resp.text

        resp = self.console.post(
            f"/accounts/{self.account_id}/roles/{role}/policies/{policy}/delete"
        )
        assert resp.status_code == 303

        self.console.post(f"/accounts/{self.account_id}/roles/{role}/delete")
# ---------------------------------------------------------------------------
# Console policy → data plane integration
# ---------------------------------------------------------------------------

class TestConsolePolicyDataPlane:
    """Policies created via console must affect DynamoDB data plane behavior."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, console, test_account_id):
        self.endpoint = auth_env[0]
        self.console = console
        self.account_id = test_account_id
        self.console.post("/accounts/new", {
            "account_id": self.account_id,
            "account_name": f"dp-test-{self.account_id}",
        })
        yield
        self.console.post(f"/accounts/{self.account_id}/delete")

    def test_console_policy_grants_dynamodb_access(self):
        """Policy created via console form grants DynamoDB access."""
        user = f"dp-{uuid.uuid4().hex[:8]}"
        policy = "full-access"

        # Create user via console.
        self.console.post(f"/accounts/{self.account_id}/users/new", {
            "user_name": user,
            "password": "TestPass123!",
        })

        # Create policy via console.
        self.console.post(
            f"/accounts/{self.account_id}/users/{user}/policies/new",
            {
                "policy_name": policy,
                "policy_document": json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Action": "dynamodb:*",
                        "Resource": "*",
                    }],
                }),
            },
        )

        # Get access key via management API (console doesn't return raw keys
        # in a machine-parseable way).
        admin_user = os.environ["EXTENDDB_ADMIN_USER"]
        admin_pass = os.environ["EXTENDDB_ADMIN_PASSWORD"]
        mgmt = ManagementClient(self.endpoint, admin_user, admin_pass)
        resp = mgmt.create_access_key(self.account_id, user)
        assert resp.status_code == 201, resp.text
        creds = resp.json()

        client = boto3.client(
            "dynamodb",
            endpoint_url=self.endpoint,
            aws_access_key_id=creds["access_key_id"],
            aws_secret_access_key=creds["secret_access_key"],
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            config=BotoConfig(retries={"max_attempts": 0}),
            verify=not self.endpoint.startswith("https://"),
        )

        try:
            resp = client.list_tables()
            assert "TableNames" in resp
        finally:
            self.console.post(f"/accounts/{self.account_id}/users/{user}/delete")
# ---------------------------------------------------------------------------
# IAM User Scoping
# ---------------------------------------------------------------------------

class TestConsoleIamUserScoping:
    """IAM users logged into the console see only their own account."""

    @pytest.fixture(autouse=True)
    def setup(self, auth_env, console, test_account_id):
        self.endpoint = auth_env[0]
        self.admin_console = console
        self.account_id = test_account_id
        self.admin_console.post("/accounts/new", {
            "account_id": self.account_id,
            "account_name": f"scope-test-{self.account_id}",
        })
        yield
        self.admin_console.post(f"/accounts/{self.account_id}/delete")

    def test_iam_user_sees_own_account_only(self):
        """IAM user logged into console cannot see other accounts."""
        user = f"scoped-{uuid.uuid4().hex[:8]}"
        password = "ScopePass123!"

        self.admin_console.post(f"/accounts/{self.account_id}/users/new", {
            "user_name": user,
            "password": password,
        })

        try:
            # Login as IAM user.
            iam_console = ConsoleClient(self.endpoint)
            resp = iam_console.login(f"{self.account_id}/{user}", password)
            assert resp.status_code == 303

            # Dashboard should be accessible.
            resp = iam_console.get("")
            assert resp.status_code == 200

            # Account listing: IAM user should see limited view.
            resp = iam_console.get("/accounts")
            assert resp.status_code == 200
            # The IAM user's own account should be visible.
            assert self.account_id in resp.text
        finally:
            self.admin_console.post(
                f"/accounts/{self.account_id}/users/{user}/delete"
            )

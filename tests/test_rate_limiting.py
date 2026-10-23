# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Rate limiting and account lockout integration tests.

Verifies that repeated failed login attempts on the management API trigger the
per-principal lockout enforced by the storage backend's RateLimitStore
implementation.

ExtendDB-only: rate limiting is not exercised against real DynamoDB.

Prerequisites:
  - extenddb running with ``auth.provider = "builtin"`` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD env vars

REQ-AUTH-003
"""

from __future__ import annotations

import os
import uuid

import pytest
import requests

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


class TestRateLimiting:
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        endpoint, admin_user, admin_pass = _require_extenddb_env()
        self.endpoint = endpoint
        self.mgmt = ManagementClient(endpoint, admin_user, admin_pass)
        self.verify = not endpoint.startswith("https://")

        self.account_id = f"{uuid.uuid4().int % 10**12:012d}"
        self.user_name = f"ratelimit-{uuid.uuid4().hex[:8]}"

        resp = self.mgmt.create_account(self.account_id, f"ratelimit-acct-{self.account_id}")
        assert resp.status_code == 201, resp.text
        resp = self.mgmt.create_user(self.account_id, self.user_name, "ValidPass123!")
        assert resp.status_code == 201, resp.text

        yield

        self.mgmt.delete_account(self.account_id)

    def _management_request_with_password(self, password: str) -> requests.Response:
        """Hit a self-service management endpoint as the test user with the given password."""
        return requests.get(
            f"{self.mgmt.base_url}/accounts/{self.account_id}/users/{self.user_name}/access-keys",
            auth=(f"{self.account_id}/{self.user_name}", password),
            timeout=10,
            verify=self.verify,
        )

    def test_principal_locked_out_after_threshold_failures(self):
        """After MAX_FAILURES_PER_PRINCIPAL bad passwords the account is locked out."""
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
        """Fewer than threshold failures do not lock out the account."""
        for _ in range(_LOCKOUT_THRESHOLD - 1):
            resp = self._management_request_with_password("wrongpassword")
            assert resp.status_code == 401

        # Valid password should still succeed.
        resp = self._management_request_with_password("ValidPass123!")
        assert resp.status_code == 200, (
            f"Expected 200 with correct password before lockout, "
            f"got {resp.status_code}: {resp.text}"
        )

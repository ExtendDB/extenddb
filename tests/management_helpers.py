# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Helper functions for calling the extenddb management API from tests.

Wraps HTTP calls to /management/* endpoints with Basic auth. Used by
the auth integration tests (Phase 12i) to provision accounts, users,
roles, policies, and access keys before exercising the DynamoDB API.
"""

from __future__ import annotations

from typing import Any

import requests
class ManagementClient:
    """Thin client for the extenddb management API."""

    def __init__(self, base_url: str, admin_user: str, admin_password: str,
                 *, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/") + "/management"
        self.admin_auth = (admin_user, admin_password)
        self.timeout = timeout
        # D4: Self-signed certs from ``extenddb init`` — disable SSL verification.
        self.verify = not base_url.startswith("https://")

    def _iam_auth(self, account_id: str, user_name: str, password: str) -> tuple[str, str]:
        return (f"{account_id}/{user_name}", password)

    # -- Accounts --

    def create_account(self, account_id: str, account_name: str) -> requests.Response:
        return requests.post(
            f"{self.base_url}/accounts",
            json={"account_id": account_id, "account_name": account_name},
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_account(self, account_id: str) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    # -- IAM Users --

    def create_user(
        self, account_id: str, user_name: str, password: str | None = None
    ) -> requests.Response:
        body: dict[str, Any] = {"user_name": user_name}
        if password is not None:
            body["password"] = password
        return requests.post(
            f"{self.base_url}/accounts/{account_id}/users",
            json=body,
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_user(self, account_id: str, user_name: str) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    # -- Access Keys (self-service or admin) --

    def create_access_key(
        self,
        account_id: str,
        user_name: str,
        *,
        as_admin: bool = True,
        user_password: str | None = None,
    ) -> requests.Response:
        auth = self.admin_auth
        if not as_admin:
            assert user_password is not None
            auth = self._iam_auth(account_id, user_name, user_password)
        return requests.post(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/access-keys",
            auth=auth,
            timeout=self.timeout, verify=self.verify,
        )

    # -- Policies --

    def put_user_policy(
        self,
        account_id: str,
        user_name: str,
        policy_name: str,
        policy_document: dict,
    ) -> requests.Response:
        return requests.put(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/policy/{policy_name}",
            json=policy_document,
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def put_group_policy(
        self,
        account_id: str,
        group_name: str,
        policy_name: str,
        policy_document: dict,
    ) -> requests.Response:
        return requests.put(
            f"{self.base_url}/accounts/{account_id}/groups/{group_name}/policy/{policy_name}",
            json=policy_document,
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def put_role_policy(
        self,
        account_id: str,
        role_name: str,
        policy_name: str,
        policy_document: dict,
    ) -> requests.Response:
        return requests.put(
            f"{self.base_url}/accounts/{account_id}/roles/{role_name}/policy/{policy_name}",
            json=policy_document,
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    # -- Groups --

    def create_group(self, account_id: str, group_name: str) -> requests.Response:
        return requests.post(
            f"{self.base_url}/accounts/{account_id}/groups",
            json={"group_name": group_name},
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def add_group_member(
        self, account_id: str, group_name: str, user_name: str
    ) -> requests.Response:
        return requests.post(
            f"{self.base_url}/accounts/{account_id}/groups/{group_name}/members",
            json={"user_name": user_name},
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_group(self, account_id: str, group_name: str) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/groups/{group_name}",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    # -- Roles --

    def create_role(
        self,
        account_id: str,
        role_name: str,
        trust_policy: dict,
    ) -> requests.Response:
        return requests.post(
            f"{self.base_url}/accounts/{account_id}/roles",
            json={"role_name": role_name, "trust_policy": trust_policy},
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_role(self, account_id: str, role_name: str) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/roles/{role_name}",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def assume_role(
        self,
        account_id: str,
        role_name: str,
        caller_arn: str,
        session_name: str,
        *,
        session_tags: dict | None = None,
        session_policy: dict | None = None,
        duration_seconds: int = 3600,
    ) -> requests.Response:
        body: dict[str, Any] = {
            "caller_arn": caller_arn,
            "session_name": session_name,
            "duration_seconds": duration_seconds,
        }
        if session_tags is not None:
            body["session_tags"] = session_tags
        if session_policy is not None:
            body["session_policy"] = session_policy
        return requests.post(
            f"{self.base_url}/accounts/{account_id}/roles/{role_name}/assume",
            json=body,
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    # -- Tags --

    def tag_user(
        self, account_id: str, user_name: str, tags: dict[str, str]
    ) -> requests.Response:
        tag_list = [{"key": k, "value": v} for k, v in tags.items()]
        return requests.put(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/tags",
            json={"tags": tag_list},
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def tag_role(
        self, account_id: str, role_name: str, tags: dict[str, str]
    ) -> requests.Response:
        tag_list = [{"key": k, "value": v} for k, v in tags.items()]
        return requests.put(
            f"{self.base_url}/accounts/{account_id}/roles/{role_name}/tags",
            json={"tags": tag_list},
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    # -- Listing --

    def list_accounts(self) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def list_users(self, account_id: str) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts/{account_id}/users",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def list_groups(self, account_id: str) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts/{account_id}/groups",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def list_roles(self, account_id: str) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts/{account_id}/roles",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def list_access_keys(self, account_id: str, user_name: str) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/access-keys",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_access_key(
        self, account_id: str, user_name: str, access_key_id: str
    ) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/access-keys/{access_key_id}",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def import_access_key(
        self, account_id: str, user_name: str,
        access_key_id: str, secret_access_key: str,
    ) -> requests.Response:
        return requests.post(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/access-keys/import",
            json={"access_key_id": access_key_id, "secret_access_key": secret_access_key},
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def list_user_policies(self, account_id: str, user_name: str) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/policies",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_user_policy(
        self, account_id: str, user_name: str, policy_name: str
    ) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/policy/{policy_name}",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def list_group_policies(self, account_id: str, group_name: str) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts/{account_id}/groups/{group_name}/policies",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_group_policy(
        self, account_id: str, group_name: str, policy_name: str
    ) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/groups/{group_name}/policy/{policy_name}",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def list_role_policies(self, account_id: str, role_name: str) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts/{account_id}/roles/{role_name}/policies",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_role_policy(
        self, account_id: str, role_name: str, policy_name: str
    ) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/roles/{role_name}/policy/{policy_name}",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def remove_group_member(
        self, account_id: str, group_name: str, user_name: str
    ) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/groups/{group_name}/members/{user_name}",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    # -- Permissions Boundaries --

    def set_user_boundary(
        self, account_id: str, user_name: str, policy_document: dict
    ) -> requests.Response:
        return requests.put(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/permissions-boundary",
            json=policy_document,
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def get_user_boundary(self, account_id: str, user_name: str) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/permissions-boundary",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_user_boundary(self, account_id: str, user_name: str) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/users/{user_name}/permissions-boundary",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def set_role_boundary(
        self, account_id: str, role_name: str, policy_document: dict
    ) -> requests.Response:
        return requests.put(
            f"{self.base_url}/accounts/{account_id}/roles/{role_name}/permissions-boundary",
            json=policy_document,
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def get_role_boundary(self, account_id: str, role_name: str) -> requests.Response:
        return requests.get(
            f"{self.base_url}/accounts/{account_id}/roles/{role_name}/permissions-boundary",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    def delete_role_boundary(self, account_id: str, role_name: str) -> requests.Response:
        return requests.delete(
            f"{self.base_url}/accounts/{account_id}/roles/{role_name}/permissions-boundary",
            auth=self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

    # -- Cache (admin break-glass) --

    def invalidate_cache(
        self,
        scope: str,
        selectors: dict | None = None,
        *,
        auth=None,
    ) -> requests.Response:
        """POST /management/cache/invalidate.

        ``auth`` defaults to admin credentials; pass a tuple to test auth gating.
        """
        body = {"scope": scope, "selectors": selectors or {}}
        return requests.post(
            f"{self.base_url}/cache/invalidate",
            json=body,
            auth=auth or self.admin_auth,
            timeout=self.timeout, verify=self.verify,
        )

# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""Console pages under /console/accounts/{account_id} serve an IAM user its
own account only.

An admin session may read and change every account. An IAM-user session that
names another account's ID in the path is redirected to /console before the
handler looks up the account in the path, so the answer is the same for an
account that exists and for an ID that no account has, and it names nothing
in the other account. The console home page shows the deployment's account
and admin counts to admin sessions only.

Account A holds users, a group, a role, inline policies, an access key and a
tag, each named with a random suffix so that no other page shows the name by
chance. Account B holds the same, plus the IAM user whose console session the
tests use.

Prerequisites match tests/test_cross_account_isolation.py:
  - extenddb running with `auth.provider = "builtin"` on EXTENDDB_TEST_ENDPOINT
  - Admin credentials in EXTENDDB_ADMIN_USER / EXTENDDB_ADMIN_PASSWORD
"""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass, field
from typing import Callable

import pytest
import requests

from management_helpers import ManagementClient

_CSRF_RE = re.compile(r'<meta\s+name="csrf-token"\s+content="([^"]+)"')

_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "dynamodb:GetItem", "Resource": "*"}],
}

def _trust_policy(account_id: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
            "Action": "sts:AssumeRole",
        }],
    }


def _require_auth_env() -> tuple[str, str, str]:
    endpoint = os.environ.get("EXTENDDB_TEST_ENDPOINT", "").strip()
    admin_user = os.environ.get("EXTENDDB_ADMIN_USER", "").strip()
    admin_pass = os.environ.get("EXTENDDB_ADMIN_PASSWORD", "").strip()
    if not endpoint or not admin_user or not admin_pass:
        pytest.fail(
            "MISCONFIGURED: console account scope tests require "
            "EXTENDDB_TEST_ENDPOINT, EXTENDDB_ADMIN_USER, and "
            "EXTENDDB_ADMIN_PASSWORD. These must be set by devtools/run-tests "
            "before test execution."
        )
    return endpoint, admin_user, admin_pass


class Console:
    """One browser: a cookie jar talking to the server's console."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = requests.Session()
        # Self-signed certs from ``extenddb init``. The harness sets
        # REQUESTS_CA_BUNDLE, which overrides a session-level ``verify``, so
        # the environment is ignored and ``verify`` is passed per request.
        self.http.trust_env = False
        self.verify = not self.base_url.startswith("https://")
        self.csrf = ""

    def login(self, username: str, password: str) -> None:
        resp = self.http.post(
            f"{self.base_url}/console/login",
            data={"username": username, "password": password},
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )
        assert resp.status_code == 303, (
            f"login as {username}: {resp.status_code} {resp.text[:200]}"
        )
        home = self.get("")
        assert home.status_code == 200, home.status_code
        match = _CSRF_RE.search(home.text)
        assert match, "the console home page carries no CSRF token"
        self.csrf = match.group(1)

    def get(self, path: str) -> requests.Response:
        return self.http.get(
            f"{self.base_url}/console{path}",
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )

    def post(self, path: str, form: dict[str, str]) -> requests.Response:
        return self.http.post(
            f"{self.base_url}/console{path}",
            data={"_csrf": self.csrf, **form},
            allow_redirects=False,
            timeout=30,
            verify=self.verify,
        )


@dataclass
class Account:
    """An account and the IAM entities created in it."""

    id: str
    name: str
    owner: str
    peer: str
    group: str
    role: str
    user_policy: str
    group_policy: str
    role_policy: str
    owner_key: str
    tag_key: str
    tag_value: str

    def names(self) -> list[str]:
        """Every name in the account that a console page can show."""
        return [
            self.name, self.owner, self.peer, self.group, self.role,
            self.user_policy, self.group_policy, self.role_policy,
            self.owner_key, self.tag_key, self.tag_value,
        ]


def _new_account(label: str) -> Account:
    """The names of an account that `_populate` creates."""
    suffix = uuid.uuid4().hex[:10]
    account_id = f"{uuid.uuid4().int % 10**12:012d}"
    return Account(
        id=account_id,
        name=f"scope-{label}-{suffix}",
        owner=f"owner-{suffix}",
        peer=f"peer-{suffix}",
        group=f"group-{suffix}",
        role=f"role-{suffix}",
        user_policy=f"user-policy-{suffix}",
        group_policy=f"group-policy-{suffix}",
        role_policy=f"role-policy-{suffix}",
        owner_key="",
        tag_key=f"tag-key-{suffix}",
        tag_value=f"tag-value-{suffix}",
    )


def _populate(mgmt: ManagementClient, acct: Account) -> None:
    """Create the account with two users, a group with the first user as its
    member, a role, an inline policy on each, an access key and a tag on the
    first user."""
    steps: list[Callable[[], requests.Response]] = [
        lambda: mgmt.create_account(acct.id, acct.name),
        lambda: mgmt.create_user(acct.id, acct.owner),
        lambda: mgmt.create_user(acct.id, acct.peer),
        lambda: mgmt.create_group(acct.id, acct.group),
        lambda: mgmt.add_group_member(acct.id, acct.group, acct.owner),
        lambda: mgmt.create_role(acct.id, acct.role, _trust_policy(acct.id)),
        lambda: mgmt.put_user_policy(acct.id, acct.owner, acct.user_policy, _POLICY),
        lambda: mgmt.put_group_policy(acct.id, acct.group, acct.group_policy, _POLICY),
        lambda: mgmt.put_role_policy(acct.id, acct.role, acct.role_policy, _POLICY),
        lambda: mgmt.tag_user(acct.id, acct.owner, {acct.tag_key: acct.tag_value}),
    ]
    for step in steps:
        resp = step()
        assert resp.status_code in (200, 201, 204), f"{resp.url}: {resp.text}"
    resp = mgmt.create_access_key(acct.id, acct.owner)
    assert resp.status_code == 201, resp.text
    acct.owner_key = resp.json()["access_key_id"]


def _delete_account(mgmt: ManagementClient, acct: Account, users: list[str]) -> None:
    """Best-effort cleanup; a failure here must not hide a test result."""
    mgmt.delete_user_policy(acct.id, acct.owner, acct.user_policy)
    mgmt.delete_group_policy(acct.id, acct.group, acct.group_policy)
    mgmt.delete_role_policy(acct.id, acct.role, acct.role_policy)
    mgmt.remove_group_member(acct.id, acct.group, acct.owner)
    mgmt.delete_group(acct.id, acct.group)
    mgmt.delete_role(acct.id, acct.role)
    for user in users:
        keys = mgmt.list_access_keys(acct.id, user)
        for key in keys.json() if keys.status_code == 200 else []:
            mgmt.delete_access_key(acct.id, user, key["access_key_id"])
        mgmt.delete_user(acct.id, user)
    mgmt.delete_account(acct.id)


@dataclass
class World:
    mgmt: ManagementClient
    admin: Console
    viewer: Console  # the console session of account B's IAM user
    viewer_name: str
    a: Account
    b: Account
    missing: str  # an account ID that no account has
    # The last state read of each account, by account ID (see _assert_unchanged).
    states: dict[str, dict[str, object]] = field(default_factory=dict)


@pytest.fixture(scope="module")
def world():
    endpoint, admin_user, admin_pass = _require_auth_env()
    mgmt = ManagementClient(endpoint, admin_user, admin_pass)
    a, b = _new_account("a"), _new_account("b")
    viewer_name, viewer_password = f"viewer-{uuid.uuid4().hex[:10]}", uuid.uuid4().hex
    created = [(a, [a.owner, a.peer]), (b, [b.owner, b.peer, viewer_name])]
    try:
        _populate(mgmt, a)
        _populate(mgmt, b)
        resp = mgmt.create_user(b.id, viewer_name, viewer_password)
        assert resp.status_code == 201, resp.text

        accounts = mgmt.list_accounts()
        assert accounts.status_code == 200, accounts.text
        missing = a.id
        while missing in accounts.text:
            missing = f"{uuid.uuid4().int % 10**12:012d}"

        admin = Console(endpoint)
        admin.login(admin_user, admin_pass)
        viewer = Console(endpoint)
        viewer.login(f"{b.id}/{viewer_name}", viewer_password)
        w = World(mgmt, admin, viewer, viewer_name, a, b, missing)
        for acct in (a, b):
            w.states[acct.id] = _state(w, acct)
        yield w
    finally:
        for acct, users in created:
            _delete_account(mgmt, acct, users)


# Console paths under /console/accounts/{account_id} that answer GET.
PAGES: dict[str, Callable[[Account], str]] = {
    "account": lambda a: f"/accounts/{a.id}",
    "user": lambda a: f"/accounts/{a.id}/users/{a.owner}",
    "group": lambda a: f"/accounts/{a.id}/groups/{a.group}",
    "role": lambda a: f"/accounts/{a.id}/roles/{a.role}",
    "new-user-form": lambda a: f"/accounts/{a.id}/users/new",
    "new-group-form": lambda a: f"/accounts/{a.id}/groups/new",
    "new-role-form": lambda a: f"/accounts/{a.id}/roles/new",
    "user-policy-form": lambda a: f"/accounts/{a.id}/users/{a.owner}/policies/new",
    "group-policy-form": lambda a: f"/accounts/{a.id}/groups/{a.group}/policies/new",
    "role-policy-form": lambda a: f"/accounts/{a.id}/roles/{a.role}/policies/new",
}

# Console paths under /console/accounts/{account_id} that answer POST, with a
# complete form for each, so that the handler runs. Each one changes the
# account if it is let through.
FORMS: dict[str, Callable[[Account], tuple[str, dict[str, str]]]] = {
    "delete-account": lambda a: (f"/accounts/{a.id}/delete", {}),
    "create-user": lambda a: (
        f"/accounts/{a.id}/users/new", {"user_name": f"new-{a.owner}", "password": ""}),
    "delete-user": lambda a: (f"/accounts/{a.id}/users/{a.peer}/delete", {}),
    "create-access-key": lambda a: (
        f"/accounts/{a.id}/users/{a.owner}/access-keys/new", {}),
    "delete-access-key": lambda a: (
        f"/accounts/{a.id}/users/{a.owner}/access-keys/{a.owner_key}/delete", {}),
    "create-group": lambda a: (
        f"/accounts/{a.id}/groups/new", {"group_name": f"new-{a.group}"}),
    "delete-group": lambda a: (f"/accounts/{a.id}/groups/{a.group}/delete", {}),
    "add-group-member": lambda a: (
        f"/accounts/{a.id}/groups/{a.group}/members/add", {"user_name": a.peer}),
    "remove-group-member": lambda a: (
        f"/accounts/{a.id}/groups/{a.group}/members/{a.owner}/remove", {}),
    "create-role": lambda a: (
        f"/accounts/{a.id}/roles/new",
        {"role_name": f"new-{a.role}", "trust_policy": json.dumps(_trust_policy(a.id))}),
    "delete-role": lambda a: (f"/accounts/{a.id}/roles/{a.role}/delete", {}),
    "put-user-policy": lambda a: (
        f"/accounts/{a.id}/users/{a.owner}/policies/new",
        {"policy_name": f"new-{a.user_policy}", "policy_document": json.dumps(_POLICY)}),
    "delete-user-policy": lambda a: (
        f"/accounts/{a.id}/users/{a.owner}/policies/{a.user_policy}/delete", {}),
    "put-group-policy": lambda a: (
        f"/accounts/{a.id}/groups/{a.group}/policies/new",
        {"policy_name": f"new-{a.group_policy}", "policy_document": json.dumps(_POLICY)}),
    "delete-group-policy": lambda a: (
        f"/accounts/{a.id}/groups/{a.group}/policies/{a.group_policy}/delete", {}),
    "put-role-policy": lambda a: (
        f"/accounts/{a.id}/roles/{a.role}/policies/new",
        {"policy_name": f"new-{a.role_policy}", "policy_document": json.dumps(_POLICY)}),
    "delete-role-policy": lambda a: (
        f"/accounts/{a.id}/roles/{a.role}/policies/{a.role_policy}/delete", {}),
}


def _answer(resp: requests.Response) -> tuple[int, str | None, bytes]:
    return resp.status_code, resp.headers.get("location"), resp.content


def _state(w: World, acct: Account) -> dict[str, object]:
    """What a console POST could change in `acct`, read as admin."""
    reads = {
        "accounts": w.mgmt.list_accounts(),
        "users": w.mgmt.list_users(acct.id),
        "groups": w.mgmt.list_groups(acct.id),
        "roles": w.mgmt.list_roles(acct.id),
        "owner_keys": w.mgmt.list_access_keys(acct.id, acct.owner),
        "user_policies": w.mgmt.list_user_policies(acct.id, acct.owner),
        "group_policies": w.mgmt.list_group_policies(acct.id, acct.group),
        "role_policies": w.mgmt.list_role_policies(acct.id, acct.role),
    }
    state: dict[str, object] = {}
    for what, resp in reads.items():
        assert resp.status_code == 200, f"{what}: {resp.status_code} {resp.text[:200]}"
        state[what] = resp.json()
    state["accounts"] = acct.id in json.dumps(state["accounts"])
    page = w.admin.get(f"/accounts/{acct.id}/groups/{acct.group}")
    assert page.status_code == 200, page.status_code
    state["members"] = sorted(re.findall(
        rf'<tr><td><a href="/console/accounts/{acct.id}/users/([^"]+)">', page.text))
    return state


def _assert_unchanged(w: World, acct: Account) -> None:
    """Read `acct`'s state and compare it with the last state read of it.

    The fixture reads each account's state once, before the first form test,
    and each form test reads it once after its POST. The new state replaces
    the stored one before the comparison, so when a form is let through only
    its own test fails.
    """
    after = _state(w, acct)
    before, w.states[acct.id] = w.states[acct.id], after
    assert after == before


@pytest.mark.parametrize("page", sorted(PAGES))
def test_iam_user_gets_no_name_of_another_account(world: World, page: str) -> None:
    resp = world.viewer.get(PAGES[page](world.a))
    assert resp.status_code == 303, f"{resp.status_code} {resp.text[:300]}"
    assert resp.headers.get("location") == "/console"
    for name in world.a.names():
        assert name not in resp.text, f"{page} shows {name}"
        assert name not in str(resp.headers), f"{page} headers name {name}"


@pytest.mark.parametrize("page", sorted(PAGES))
def test_missing_account_gets_the_same_answer_as_another_account(
    world: World, page: str
) -> None:
    path = PAGES[page](world.a)
    other = world.viewer.get(path)
    missing = world.viewer.get(path.replace(world.a.id, world.missing, 1))
    assert _answer(missing) == _answer(other)


@pytest.mark.parametrize("action", sorted(FORMS))
def test_iam_user_cannot_change_another_account(world: World, action: str) -> None:
    path, form = FORMS[action](world.a)

    resp = world.viewer.post(path, form)
    missing = world.viewer.post(path.replace(world.a.id, world.missing, 1), form)

    _assert_unchanged(world, world.a)
    assert resp.status_code == 303, f"{resp.status_code} {resp.text[:300]}"
    assert resp.headers.get("location") == "/console"
    assert _answer(missing) == _answer(resp)


@pytest.mark.parametrize("action", sorted(FORMS))
def test_iam_user_cannot_change_its_own_account(world: World, action: str) -> None:
    """Every change in an IAM user's own account is for admins, except the
    user's own access keys (the access key forms here name another user)."""
    path, form = FORMS[action](world.b)

    resp = world.viewer.post(path, form)

    _assert_unchanged(world, world.b)
    assert resp.status_code == 303, f"{resp.status_code} {resp.text[:300]}"


def test_iam_user_manages_its_own_access_keys(world: World) -> None:
    b, user = world.b, world.viewer_name
    created = world.viewer.post(f"/accounts/{b.id}/users/{user}/access-keys/new", {})
    assert created.status_code == 200, created.status_code
    match = re.search(r'<div class="secret-box">(AKIA[^<]+)</div>', created.text)
    assert match, created.text[:500]
    key_id = match.group(1)
    assert key_id in world.mgmt.list_access_keys(b.id, user).text

    deleted = world.viewer.post(
        f"/accounts/{b.id}/users/{user}/access-keys/{key_id}/delete", {})
    assert deleted.status_code == 303, deleted.status_code
    assert deleted.headers.get("location") == f"/console/accounts/{b.id}/users/{user}"
    assert key_id not in world.mgmt.list_access_keys(b.id, user).text


def test_iam_user_reads_its_own_user_page_and_not_its_account_page(world: World) -> None:
    # tests/test_console_iam_user_pages.py checks every page of the user's
    # own account; here the account page stands for them.
    b, user = world.b, world.viewer_name
    resp = world.viewer.get(PAGES["account"](b))
    assert resp.status_code == 303, resp.status_code
    assert resp.headers.get("location") == "/console"
    assert b.name not in resp.text

    resp = world.viewer.get(f"/accounts/{b.id}/users/{user}")
    assert resp.status_code == 200, resp.status_code
    assert "location" not in resp.headers
    assert f"<h1>User: {user}</h1>" in resp.text


# The names each detail page shows an admin, which the IAM-user tests above
# check are absent.
SHOWN = {
    "account": lambda a: [a.name, a.owner, a.peer, a.group, a.role],
    "user": lambda a: [a.owner, a.owner_key, a.user_policy, a.group, a.tag_key, a.tag_value],
    "group": lambda a: [a.group, a.owner, a.group_policy],
    "role": lambda a: [a.role, a.role_policy],
}


@pytest.mark.parametrize("page", sorted(SHOWN))
def test_admin_reads_every_account(world: World, page: str) -> None:
    for acct in (world.a, world.b):
        resp = world.admin.get(PAGES[page](acct))
        assert resp.status_code == 200, f"{page} {acct.id}: {resp.status_code}"
        for name in SHOWN[page](acct):
            assert name in resp.text, f"{page} {acct.id} lacks {name}"


def test_home_page_shows_deployment_counts_to_admins_only(world: World) -> None:
    page = world.viewer.get("")
    assert page.status_code == 200, page.status_code
    assert "Admin Users" not in page.text
    assert "<p>Accounts</p>" not in page.text
    assert f'<a href="/console/accounts/{world.b.id}/users/{world.viewer_name}">' in page.text
    for name in world.a.names() + [world.a.id]:
        assert name not in page.text, name

    page = world.admin.get("")
    assert page.status_code == 200, page.status_code
    assert "<p>Admin Users</p>" in page.text
    assert "<p>Accounts</p>" in page.text

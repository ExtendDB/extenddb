#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Fixed test values for the released PostgreSQL fixtures.

scrub.py --dump < pg_dump-output > fixture.sql
    Replace the encryption key, every password hash and the password inside
    the data connection string with fixed test values, and drop the
    `SET search_path`/`set_config` lines' effect on later statements by
    leaving them out: the fixtures are loaded on one connection each, and the
    tests read the objects through the public schema.

scrub.py --bootstrap <tag>
    Print the rows `init` of that tag wrote after its schema files: the
    encryption key, the default account (with `default_account_id` from
    v0.1.3 on), the admin user and the data connection rows, with the same
    fixed values.

The access key's stored secret stays encrypted under the release's own key,
which these values replace, so it cannot be decrypted; no test needs it.
"""
import re
import sys

TEST_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="  # bytes 0..31, base64
# bcrypt of "fixture-password", cost 4.
TEST_HASH = "$2b$04$KQg8SAyQ3g2v0nmyz2wPXOWb2pGZtfJ0yLe6E8V1OAQmvQjQlZ9ka"
TEST_CONNECTION = "postgresql://extenddb_app:fixture@fixture-host:5432/extenddb"

INSERT = re.compile(r"^INSERT INTO public\.(\w+) VALUES \((.*)\);$")


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def scrub_line(line):
    m = INSERT.match(line)
    if not m:
        return line
    table, values = m.group(1), m.group(2)
    if table == "settings":
        key = values.split(",", 1)[0].strip()
        if key == "'encryption_key'":
            return f"INSERT INTO public.settings VALUES ('encryption_key', {quote(TEST_KEY)});"
        if key == "'data_database_connection_string'":
            return (
                "INSERT INTO public.settings VALUES "
                f"('data_database_connection_string', {quote(TEST_CONNECTION)});"
            )
    if table in ("admin_users", "iam_users"):
        # Replace every bcrypt hash in the row; NULL hashes stay NULL.
        return re.sub(r"'\$2[aby]\$[^']*'", quote(TEST_HASH), line)
    return line


def dump():
    for raw in sys.stdin:
        line = raw.rstrip("\n")
        # psql meta-commands (`\restrict`) are not SQL.
        if line.startswith("SELECT pg_catalog.set_config('search_path'") or line.startswith("\\"):
            continue
        print(scrub_line(line))


def bootstrap(tag):
    account = "111122223333"
    print(f"INSERT INTO settings (key, value) VALUES ('encryption_key', {quote(TEST_KEY)});")
    print(f"INSERT INTO accounts (account_id, account_name) VALUES ('{account}', 'default');")
    minor_patch = tuple(int(x) for x in tag.lstrip("v").split(".")[1:])
    if minor_patch >= (1, 3):
        print(f"INSERT INTO settings (key, value) VALUES ('default_account_id', '{account}');")
    print(
        "INSERT INTO admin_users (admin_name, password_hash) VALUES "
        f"('admin', {quote(TEST_HASH)});"
    )
    print(
        "INSERT INTO settings (key, value) VALUES "
        f"('data_database_connection_string', {quote(TEST_CONNECTION)});"
    )
    print("INSERT INTO settings (key, value) VALUES ('data_database_name', 'extenddb');")


if __name__ == "__main__":
    if sys.argv[1] == "--dump":
        dump()
    elif sys.argv[1] == "--bootstrap":
        bootstrap(sys.argv[2])
    else:
        raise SystemExit("usage: scrub.py --dump | --bootstrap <tag>")

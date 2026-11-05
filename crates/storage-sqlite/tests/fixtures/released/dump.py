#!/usr/bin/env python3
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Dump a released SQLite database as a fixture.

Usage: dump.py <database> <output.sql>

Works on a copy: checkpoints the WAL, replaces the `encryption_key` row with
the fixed test key (32 zero bytes), and writes Python's sqlite3 `iterdump`
(the statements the sqlite3 shell's `.dump` prints) after
`PRAGMA foreign_keys=OFF`, because tables are dumped in name order and child
rows can come before their parents. The database itself is not changed.
"""
import pathlib
import shutil
import sqlite3
import sys
import tempfile

TEST_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def main():
    source = pathlib.Path(sys.argv[1])
    output = pathlib.Path(sys.argv[2])
    with tempfile.TemporaryDirectory() as tmp:
        copy = pathlib.Path(tmp) / "db.sqlite"
        for suffix in ("", "-wal", "-shm"):
            part = source.with_name(source.name + suffix)
            if part.exists():
                shutil.copy(part, copy.with_name(copy.name + suffix))
        conn = sqlite3.connect(copy)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("UPDATE settings SET value = ? WHERE key = 'encryption_key'", (TEST_KEY,))
        conn.commit()
        lines = ["PRAGMA foreign_keys=OFF;"]
        lines.extend(conn.iterdump())
        conn.close()
    output.write_text("\n".join(lines) + "\n")
    print("%s: %d statements" % (output, len(lines)))


if __name__ == "__main__":
    main()

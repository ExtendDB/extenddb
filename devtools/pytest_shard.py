# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""pytest plugin for `devtools/run-tests --shard K/N`.

Loaded with `-p pytest_shard` (run-tests puts devtools/ on PYTHONPATH). It
keeps the collected tests whose file is listed in $EXTENDDB_SHARD_FILES, one
path per line relative to the working directory, as `devtools/test-shards
plan` writes them, and deselects the rest. The suite's own arguments are left
as they are, so filters, ignores and xdist options apply exactly as in an
unsharded run. Under xdist every worker reads the same list and keeps the
same tests.
"""

import os

import pytest


def _shard_files():
    path = os.environ.get("EXTENDDB_SHARD_FILES")
    if not path:
        raise pytest.UsageError(
            "pytest_shard: EXTENDDB_SHARD_FILES is not set")
    with open(path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def pytest_collection_modifyitems(config, items):
    files = _shard_files()
    keep, drop = [], []
    for item in items:
        (keep if os.path.relpath(str(item.path)) in files else drop).append(item)
    if drop:
        config.hook.pytest_deselected(items=drop)
        items[:] = keep

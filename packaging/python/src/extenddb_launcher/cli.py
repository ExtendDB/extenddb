# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0

"""``extenddb start`` console entry point."""

from __future__ import annotations

import argparse
import signal
import sys

from . import start


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="extenddb",
        description=(
            "Start an ExtendDB dev server (DynamoDB-compatible) on loopback. "
            "In-memory by default; --db persists to a SQLite file."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)
    start_cmd = sub.add_parser("start", help="start the dev server")
    start_cmd.add_argument("--db", help="persist to this SQLite file")
    start_cmd.add_argument("--port", type=int, help="fixed port (default: ephemeral)")
    args = parser.parse_args()

    eb = start(db_path=args.db, port=args.port)
    print("ExtendDB dev server ready")
    print(f"  endpoint:   {eb.endpoint}")
    print(f"  region:     {eb.region}")
    print(f"  accessKey:  {eb.access_key_id}")
    print(f"  storage:    {args.db or ':memory: (ephemeral)'}")
    print("Press Ctrl-C to stop.")

    def _shutdown(_sig, _frame):
        eb.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    signal.pause()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

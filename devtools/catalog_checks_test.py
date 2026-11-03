# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
"""Tests of devtools/catalog_checks.py, which the catalog check scripts
report through: a check passes only when the command's exit code and its
output both match, every failure is counted, and finish returns 1 after one.
initiate_replica_set sends rs.initiate only to a mongod that answered with
no configuration, sends it again until it takes effect, and reports a member
that is never primary with the last poll's and the last initiate's output.

    python3 -m unittest discover -s devtools -p '*_test.py'
"""
import contextlib
import io
import subprocess
import unittest
from unittest import mock

import catalog_checks


class CatalogChecksTest(unittest.TestCase):
    def setUp(self) -> None:
        catalog_checks.failures = 0

    def check(self, *args, **kwargs) -> tuple[str, str]:
        """Run expect_exit, returning its output and what it printed."""
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            output = catalog_checks.expect_exit(*args, **kwargs)
        return output, printed.getvalue()

    def test_an_expected_exit_code_passes(self) -> None:
        for want, cmd in ((0, ["true"]), (1, ["false"]), (6, ["sh", "-c", "exit 6"])):
            _, printed = self.check(want, "the code", cmd)
            self.assertTrue(printed.startswith(f"ok   exit {want}: the code"), printed)
        self.assertEqual(catalog_checks.failures, 0)

    def test_another_exit_code_fails_with_no_expected_text(self) -> None:
        _, printed = self.check(0, "the code", ["sh", "-c", "exit 6"])
        self.assertTrue(printed.startswith("FAIL exit 6, expected 0: the code"), printed)
        _, printed = self.check(6, "the code", ["false"])
        self.assertTrue(printed.startswith("FAIL exit 1, expected 6: the code"), printed)
        self.assertEqual(catalog_checks.failures, 2)

    def test_the_expected_text_must_be_in_the_output_as_well(self) -> None:
        output, printed = self.check(6, "the text", ["sh", "-c", "echo held; exit 6"], contains="held")
        self.assertEqual(output, "held\n")
        self.assertTrue(printed.startswith("ok   exit 6: the text"), printed)
        _, printed = self.check(6, "the text", ["sh", "-c", "echo held; exit 6"], contains="free")
        self.assertTrue(printed.startswith("FAIL exit 6, expected 6"), printed)
        self.assertIn("     | held", printed)
        _, printed = self.check(0, "the text", ["sh", "-c", "echo held; exit 6"], contains="held")
        self.assertTrue(printed.startswith("FAIL exit 6, expected 0"), printed)
        self.assertEqual(catalog_checks.failures, 2)

    def test_a_command_past_its_time_limit_is_killed_and_fails(self) -> None:
        code, output = catalog_checks.run(["sleep", "5"], timeout=0.2)
        self.assertEqual(code, -1)
        self.assertIn("still running after 0.2 s", output)

    def test_finish_returns_1_after_a_failure_and_0_without(self) -> None:
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            self.assertEqual(catalog_checks.finish(), 0)
            catalog_checks.report(False, "a check", "line one\nline two")
            self.assertEqual(catalog_checks.finish(), 1)
        text = printed.getvalue()
        self.assertIn("all checks passed", text)
        self.assertIn("FAIL a check\n     | line one\n     | line two\n", text)
        self.assertIn("1 check(s) failed", text)


class FakeMongod:
    """mongosh calls against a mongod that refuses the calls whose numbers,
    counted from 0, are in `refused`, on which the first `failing` initiates
    take no effect, and whose member answers `initiated` to the first
    `electing` polls after the initiate that took effect, then `primary`."""

    def __init__(self, refused: range = range(0), failing: int = 0, electing: int = 0) -> None:
        self.refused = refused
        self.failing = failing
        self.electing = electing
        self.initiated = False
        self.calls: list[tuple[str, bool]] = []

    def __call__(self, container: str, port: int, script: str, timeout: float = 60) -> tuple[int, str]:
        listening = len(self.calls) not in self.refused
        kind = "initiate" if "rs.initiate" in script else "hello"
        self.calls.append((kind, listening))
        if not listening:
            return 1, f"MongoNetworkError: connect ECONNREFUSED 127.0.0.1:{port}\n"
        if kind == "initiate":
            if self.failing:
                self.failing -= 1
                return 1, "MongoServerError: the initiate did not take effect\n"
            self.initiated = True
            return 0, "{ ok: 1 }\n"
        if not self.initiated:
            return 0, "uninitiated\n"
        if self.electing:
            self.electing -= 1
            return 0, "initiated\n"
        return 0, "primary\n"


class InitiateReplicaSetTest(unittest.TestCase):
    """initiate_replica_set on port 27071, whose refused-connection error
    ends in 1: the ping check it replaces took that error for an answer
    (#793)."""

    def setUp(self) -> None:
        catalog_checks.failures = 0

    def initiate(self, mongod: FakeMongod) -> tuple[bool, str]:
        printed = io.StringIO()
        log = subprocess.CompletedProcess([], 0, "the container's log\n", "")
        with (
            mock.patch.object(catalog_checks, "mongosh", mongod),
            mock.patch.object(catalog_checks, "docker", lambda *args, **kwargs: log),
            mock.patch.object(catalog_checks.time, "sleep", lambda seconds: None),
            contextlib.redirect_stdout(printed),
        ):
            ok = catalog_checks.initiate_replica_set("c", 27071)
        return ok, printed.getvalue()

    def test_no_initiate_is_sent_while_connections_are_refused(self) -> None:
        mongod = FakeMongod(refused=range(3))
        ok, printed = self.initiate(mongod)
        self.assertTrue(ok, printed)
        self.assertEqual(
            mongod.calls, [("hello", False)] * 3 + [("hello", True), ("initiate", True), ("hello", True)]
        )

    def test_the_initiate_goes_only_to_a_member_without_a_configuration(self) -> None:
        # The two polls after the initiate are refused, and the two after
        # those find the configuration with no primary yet.
        mongod = FakeMongod(refused=range(2, 4), electing=2)
        ok, printed = self.initiate(mongod)
        self.assertTrue(ok, printed)
        self.assertEqual(
            mongod.calls, [("hello", True), ("initiate", True)] + [("hello", False)] * 2 + [("hello", True)] * 3
        )

    def test_an_initiate_that_takes_no_effect_is_sent_again(self) -> None:
        mongod = FakeMongod(failing=2)
        ok, printed = self.initiate(mongod)
        self.assertTrue(ok, printed)
        self.assertEqual([kind for kind, _ in mongod.calls].count("initiate"), 3)
        self.assertEqual(catalog_checks.failures, 0)

    def test_a_member_never_primary_is_reported_with_the_last_poll_and_initiate(self) -> None:
        ok, printed = self.initiate(FakeMongod(failing=1000))
        self.assertFalse(ok)
        self.assertEqual(catalog_checks.failures, 1)
        self.assertIn("FAIL c did not become primary", printed)
        self.assertIn("     | last db.hello() poll output:\n     | uninitiated\n", printed)
        self.assertIn(
            "     | last rs.initiate output:\n     | MongoServerError: the initiate did not take effect\n", printed
        )
        self.assertIn("     | the container's log", printed)

    def test_a_mongod_that_never_answers_is_reported_with_no_initiate_sent(self) -> None:
        mongod = FakeMongod(refused=range(1000))
        ok, printed = self.initiate(mongod)
        self.assertFalse(ok)
        self.assertEqual(set(mongod.calls), {("hello", False)})
        self.assertIn(
            "     | last db.hello() poll output:\n     | MongoNetworkError: connect ECONNREFUSED 127.0.0.1:27071\n",
            printed,
        )
        self.assertIn(
            "     | last rs.initiate output:\n     | (no rs.initiate sent: no poll printed `uninitiated`)\n", printed
        )


if __name__ == "__main__":
    unittest.main()

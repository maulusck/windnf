#!/usr/bin/env python3
"""Comprehensive CLI parser/dispatch tests for windnf.

This suite validates all commands and aliases exposed in ``src/windnf/cli.py``
without touching network, filesystem config, or real database state.
"""

from __future__ import annotations

import contextlib
import io
import sys
import unittest
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from unittest.mock import patch

# Ensure local package imports resolve when run as: python3 tests/run.py
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from windnf import cli  # noqa: E402


class FakeConfig:
    """Minimal config object required by CLI startup."""

    def __init__(self) -> None:
        self.log_level = "info"
        self.downloader = "python"
        self.skip_ssl_verify = False


class FakeOperations:
    """Operation sink that records which command was dispatched."""

    last_call: Optional[Tuple[str, Dict[str, Any]]] = None

    def __init__(self, _config: FakeConfig) -> None:
        pass

    @classmethod
    def _record(cls, name: str, kwargs: Dict[str, Any]) -> None:
        cls.last_call = (name, kwargs)

    def repoadd(self, **kwargs: Any) -> None:
        self._record("repoadd", kwargs)

    def repolink(self, **kwargs: Any) -> None:
        self._record("repolink", kwargs)

    def repolist(self, **kwargs: Any) -> None:
        self._record("repolist", kwargs)

    def reposync(self, **kwargs: Any) -> None:
        self._record("reposync", kwargs)

    def repodel(self, **kwargs: Any) -> None:
        self._record("repodel", kwargs)

    def search(self, **kwargs: Any) -> None:
        self._record("search", kwargs)

    def info(self, **kwargs: Any) -> None:
        self._record("info", kwargs)

    def resolve(self, **kwargs: Any) -> None:
        self._record("resolve", kwargs)

    def download(self, **kwargs: Any) -> None:
        self._record("download", kwargs)


@contextlib.contextmanager
def cli_patches() -> Iterable[None]:
    """Patch side-effectful CLI collaborators for deterministic tests."""

    with patch.object(cli, "Config", FakeConfig), patch.object(cli, "Operations", FakeOperations), patch.object(
        cli, "setup_logger", lambda level: None
    ), patch.object(cli, "is_dumb_terminal", lambda: True):
        yield


def invoke(args: List[str]) -> Tuple[int, str, Optional[Dict[str, Any]]]:
    """Invoke CLI with args and return (exit_code, command_name, kwargs)."""

    FakeOperations.last_call = None

    with cli_patches(), patch.object(sys, "argv", ["windnf", *args]):
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            try:
                cli.main()
            except SystemExit as exc:
                code = int(exc.code) if isinstance(exc.code, int) else 1
            else:
                code = 0

    if FakeOperations.last_call is None:
        return code, "", None

    name, kwargs = FakeOperations.last_call
    return code, name, kwargs


class TestCliTopLevel(unittest.TestCase):
    def test_version(self) -> None:
        code, name, kwargs = invoke(["--version"])
        self.assertEqual(code, 0)
        self.assertEqual(name, "")
        self.assertIsNone(kwargs)

    def test_help(self) -> None:
        code, name, kwargs = invoke(["--help"])
        self.assertEqual(code, 0)
        self.assertEqual(name, "")
        self.assertIsNone(kwargs)

    def test_invalid_command(self) -> None:
        code, name, kwargs = invoke(["nosuchcommand"])
        self.assertEqual(code, 2)
        self.assertEqual(name, "")
        self.assertIsNone(kwargs)


class TestCliDispatchMatrix(unittest.TestCase):
    def _assert_case(
        self,
        args: List[str],
        expected_command: str,
        expected_subset: Optional[Dict[str, Any]] = None,
        expected_code: int = 0,
    ) -> None:
        code, cmd, kwargs = invoke(args)
        self.assertEqual(code, expected_code, msg=f"unexpected exit for args={args!r}")
        self.assertEqual(cmd, expected_command, msg=f"unexpected command for args={args!r}")
        self.assertIsNotNone(kwargs)

        if kwargs is None:
            return

        for key, value in (expected_subset or {}).items():
            self.assertEqual(kwargs.get(key), value, msg=f"args={args!r}, key={key!r}")

    def test_repo_commands(self) -> None:
        cases = [
            (["repoadd", "base", "https://example.invalid/base"], "repoadd", {"repo_type": "binary", "sync": False}),
            (
                [
                    "ra",
                    "src",
                    "https://example.invalid/src",
                    "-m",
                    "repodata/repomd.xml",
                    "-t",
                    "source",
                    "-s",
                    "base",
                    "-S",
                ],
                "repoadd",
                {"repo_type": "source", "source_repo": "base", "sync": True},
            ),
            (["repolink", "base", "src"], "repolink", {"binary_repo": "base", "source_repo": "src"}),
            (["rlk", "base", "src"], "repolink", {"binary_repo": "base", "source_repo": "src"}),
            (["repolist"], "repolist", {}),
            (["rl"], "repolist", {}),
            (["reposync", "base", "src"], "reposync", {"names": ["base", "src"], "all_": False}),
            (["rs", "-A"], "reposync", {"names": [], "all_": True}),
            (["repodel", "base", "src", "-f"], "repodel", {"names": ["base", "src"], "force": True}),
            (["rd", "-A", "-f"], "repodel", {"all_": True, "force": True}),
        ]
        for args, cmd, subset in cases:
            with self.subTest(args=args):
                self._assert_case(args, cmd, subset)

    def test_query_commands(self) -> None:
        cases = [
            (["search", "bash"], "search", {"patterns": ["bash"], "repo": None, "showduplicates": False}),
            (["s", "bash", "--repo", "base", "app", "--showduplicates"], "search", {"showduplicates": True}),
            (["info", "bash"], "info", {"packages": ["bash"], "repo": None}),
            (["i", "bash", "coreutils", "-r", "base"], "info", {"packages": ["bash", "coreutils"], "repo": ["base"]}),
            (["resolve", "bash"], "resolve", {"recursive": None, "weakdeps": False, "verbose": False}),
            (
                ["rv", "bash", "--recursive", "2", "--weakdeps", "-v", "--arch", "x86_64", "-r", "base"],
                "resolve",
                {"recursive": 2, "weakdeps": True, "verbose": True, "arch": "x86_64", "repo": ["base"]},
            ),
            (["deplist", "bash", "-R"], "resolve", {"recursive": -1}),
            (["download", "bash"], "download", {"resolve_flag": False, "recurse": None, "source": False, "urls": False}),
            (
                [
                    "dl",
                    "bash",
                    "--resolve",
                    "--recurse",
                    "3",
                    "--source",
                    "--urls",
                    "--downloaddir",
                    "./dl",
                    "--destdir",
                    "./dest",
                    "--arch",
                    "x86_64",
                    "-r",
                    "base",
                ],
                "download",
                {
                    "resolve_flag": True,
                    "recurse": 3,
                    "source": True,
                    "urls": True,
                    "downloaddir": "./dl",
                    "destdir": "./dest",
                    "arch": "x86_64",
                    "repo": ["base"],
                },
            ),
            (["download", "bash", "-R"], "download", {"recurse": -1}),
        ]

        for args, cmd, subset in cases:
            with self.subTest(args=args):
                self._assert_case(args, cmd, subset)

    def test_required_args_validation(self) -> None:
        for args in (["repoadd", "name"], ["repolink", "onlyone"], ["search"], ["download"]):
            with self.subTest(args=args):
                code, cmd, kwargs = invoke(list(args))
                self.assertEqual(code, 2)
                self.assertEqual(cmd, "")
                self.assertIsNone(kwargs)


if __name__ == "__main__":
    unittest.main(verbosity=2)

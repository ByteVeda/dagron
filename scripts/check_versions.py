#!/usr/bin/env python3
"""Verify that every declared dagron version agrees.

The version is written down in three places that no build step reconciles:
the Cargo workspace, the Python project metadata, and ``dagron.__version__``.
Run with no arguments to compare them. Pass ``--expect TAG`` — as the release
workflows do — to also require them to match the tag being released.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

ROOT = Path(__file__).resolve().parent.parent

_DUNDER_VERSION = re.compile(r'^__version__\s*=\s*"([^"]+)"', re.MULTILINE)


def _cargo_workspace_version() -> str:
    data = tomllib.loads((ROOT / "Cargo.toml").read_text())
    return str(data["workspace"]["package"]["version"])


def _pyproject_version() -> str:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return str(data["project"]["version"])


def _dunder_version() -> str:
    path = ROOT / "dagron" / "__init__.py"
    match = _DUNDER_VERSION.search(path.read_text())
    if match is None:
        raise SystemExit(f"{path}: no __version__ assignment found")
    return match.group(1)


SOURCES: dict[str, Callable[[], str]] = {
    "Cargo.toml [workspace.package]": _cargo_workspace_version,
    "pyproject.toml [project]": _pyproject_version,
    "dagron/__init__.py __version__": _dunder_version,
}


def _fail(message: str) -> None:
    sys.stdout.flush()  # keep the table above the error when stdout is piped
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::error::{message}")
    print(f"error: {message}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expect",
        metavar="VERSION",
        help="require the declarations to also match this version (e.g. a release tag)",
    )
    args = parser.parse_args()

    versions = {label: read() for label, read in SOURCES.items()}
    if args.expect:
        versions["release tag"] = args.expect

    width = max(len(label) for label in versions)
    for label, version in versions.items():
        print(f"{label:<{width}}  {version}")

    distinct = set(versions.values())
    if len(distinct) > 1:
        _fail(f"version mismatch across {len(distinct)} declarations: {sorted(distinct)}")
        return 1

    print(f"\nOK — all declarations agree on {distinct.pop()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

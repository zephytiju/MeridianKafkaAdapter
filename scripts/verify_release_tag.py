# SPDX-License-Identifier: Apache-2.0
"""Fail a release when its immutable tag disagrees with project metadata."""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path


def main(arguments: list[str]) -> None:
    if len(arguments) != 1 or re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", arguments[0]) is None:
        raise SystemExit("expected exactly one semantic release tag")
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))["project"]
    expected = f"v{project['version']}"
    if arguments[0] != expected:
        raise SystemExit(f"release tag {arguments[0]!r} does not match {expected!r}")
    print(f"release tag {arguments[0]} matches package metadata: PASS")


if __name__ == "__main__":
    main(sys.argv[1:])

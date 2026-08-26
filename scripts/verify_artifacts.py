# SPDX-License-Identifier: Apache-2.0
"""Verify wheel and sdist contents for the one-distribution release boundary."""

from __future__ import annotations

import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
EXPECTED_VERSION = str(PROJECT["version"])

REQUIRED_WHEEL_SUFFIXES = {
    "meridian_storage/adapters/kafka/__init__.py",
    "meridian_storage/adapters/kafka/compatibility.json",
    "meridian_storage/adapters/kafka/py.typed",
}
FORBIDDEN_WHEEL_PARTS = {"tests", "conformance", "scripts"}


def _verify_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        for suffix in REQUIRED_WHEEL_SUFFIXES:
            if suffix not in names:
                raise SystemExit(f"{path.name} is missing {suffix}")
        package_paths = {
            name.split("/")[2]
            for name in names
            if name.startswith("meridian_storage/adapters/") and len(name.split("/")) > 2
        }
        if package_paths != {"kafka"}:
            raise SystemExit(f"{path.name} contains unexpected adapter packages: {package_paths}")
        if any(set(name.split("/")) & FORBIDDEN_WHEEL_PARTS for name in names):
            raise SystemExit(f"{path.name} contains development-only paths")
        metadata = [name for name in names if name.endswith(".dist-info/METADATA")]
        entry_points = [name for name in names if name.endswith(".dist-info/entry_points.txt")]
        if len(metadata) != 1 or len(entry_points) != 1:
            raise SystemExit(f"{path.name} must contain one distribution and entry-point table")
        text = archive.read(metadata[0]).decode("utf-8")
        if (
            "Name: meridian-storage-kafka" not in text
            or f"Version: {EXPECTED_VERSION}" not in text
            or "License-Expression: Apache-2.0" not in text
        ):
            raise SystemExit(f"{path.name} metadata identity or SPDX license is invalid")
        entry_text = archive.read(entry_points[0]).decode("utf-8")
        if entry_text.strip().splitlines() != [
            "[meridian_storage.adapters]",
            "kafka = meridian_storage.adapters.kafka:KafkaAdapterFactory",
        ]:
            raise SystemExit(f"{path.name} entry-point table is not exact")


def _verify_sdist(path: Path) -> None:
    with tarfile.open(path, "r:gz") as archive:
        names = archive.getnames()
        if not any(name.endswith("/LICENSE") for name in names):
            raise SystemExit(f"{path.name} is missing LICENSE")
        if not any(name.endswith("/NOTICE") for name in names):
            raise SystemExit(f"{path.name} is missing NOTICE")
        if not any(name.endswith("/SECURITY.md") for name in names):
            raise SystemExit(f"{path.name} is missing SECURITY.md")


def main(arguments: list[str]) -> None:
    paths = tuple(Path(item) for item in arguments)
    wheels = tuple(path for path in paths if path.suffix == ".whl")
    sdists = tuple(path for path in paths if path.name.endswith(".tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        raise SystemExit("verification requires exactly one wheel and one sdist")
    if not wheels[0].name.startswith(f"meridian_storage_kafka-{EXPECTED_VERSION}-"):
        raise SystemExit(f"wheel filename does not match project version {EXPECTED_VERSION}")
    if sdists[0].name != f"meridian_storage_kafka-{EXPECTED_VERSION}.tar.gz":
        raise SystemExit(f"sdist filename does not match project version {EXPECTED_VERSION}")
    _verify_wheel(wheels[0])
    _verify_sdist(sdists[0])
    print("single distribution, SPDX metadata, entry point, and release contents: PASS")


if __name__ == "__main__":
    main(sys.argv[1:])

# SPDX-License-Identifier: Apache-2.0
"""Fail closed when the one-package or IaC authority boundary drifts."""

from __future__ import annotations

import ast
import re
import tomllib
from importlib.metadata import requires
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src" / "meridian_storage" / "adapters" / "kafka"
FORBIDDEN_IMPORT_ROOTS = {
    "boto3",
    "kafka",
    "meridian_constructs",
    "meridian_storage.adapters.kafka_native",
    "terraform",
}
FORBIDDEN_ADMIN_MUTATIONS = {
    "alter_configs",
    "create_partitions",
    "create_topics",
    "delete_topics",
    "incremental_alter_configs",
}
EXPECTED_RUNTIME_DEPENDENCIES = {
    "confluent-kafka==2.15.0",
    "meridian-storage-core==1.0.0",
    "meridian-storage-semantics==1.0.0",
    "meridian-storage-streaming==1.0.0",
}
CONSUMER_DISTRIBUTIONS = (
    "meridian-storage-core",
    "meridian-storage-semantics",
    "meridian-storage-streaming",
)


def main() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    if project["name"] != "meridian-storage-kafka":
        raise SystemExit("repository must own only meridian-storage-kafka")
    if set(project["dependencies"]) != EXPECTED_RUNTIME_DEPENDENCIES:
        raise SystemExit("runtime dependencies differ from the exact released compatibility graph")
    package_roots = sorted(
        path.relative_to(ROOT / "src").as_posix()
        for path in (ROOT / "src").glob("meridian_storage/adapters/*")
        if path.is_dir()
    )
    if package_roots != ["meridian_storage/adapters/kafka"]:
        raise SystemExit(f"expected exactly one adapter package, found {package_roots!r}")
    for path in SOURCE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = {item.name for item in node.names}
            elif isinstance(node, ast.ImportFrom):
                names = {node.module or ""}
            else:
                names = set()
            leaked = {
                name
                for name in names
                if any(
                    name == root or name.startswith(f"{root}.") for root in FORBIDDEN_IMPORT_ROOTS
                )
            }
            if leaked:
                raise SystemExit(f"forbidden dependency import in {path}: {sorted(leaked)!r}")
            if isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_ADMIN_MUTATIONS:
                raise SystemExit(f"IaC-owned mutation {node.attr!r} appears in {path}")
    for distribution in CONSUMER_DISTRIBUTIONS:
        for requirement in requires(distribution) or ():
            dependency = re.split(r"[\s;<>=!~\[(]", requirement, maxsplit=1)[0]
            normalized = dependency.casefold().replace("_", "-")
            if "kafka" in normalized:
                raise SystemExit(
                    f"consumer distribution {distribution} unexpectedly depends on {dependency}"
                )
    print("one-package, provider-neutral, read-only lifecycle boundary: PASS")


if __name__ == "__main__":
    main()

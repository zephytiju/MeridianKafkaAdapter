# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from importlib.metadata import entry_points
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_repository_contains_exactly_one_distribution_and_adapter_package() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert project["name"] == "meridian-storage-kafka"
    assert project["version"] == "1.0.0"
    assert project["license"] == "Apache-2.0"
    assert project["dependencies"] == [
        "confluent-kafka==2.15.0",
        "meridian-storage-core==1.0.0",
        "meridian-storage-semantics==1.0.0",
        "meridian-storage-streaming==1.0.0",
    ]
    packages = sorted(
        path.name
        for path in (ROOT / "src" / "meridian_storage" / "adapters").iterdir()
        if path.is_dir()
    )
    assert packages == ["kafka"]


def test_entry_point_and_typed_marker_are_exact() -> None:
    selected = {item.name: item.value for item in entry_points(group="meridian_storage.adapters")}
    assert selected == {"kafka": "meridian_storage.adapters.kafka:KafkaAdapterFactory"}
    assert (ROOT / "src/meridian_storage/adapters/kafka/py.typed").is_file()


def test_spdx_license_notice_and_compatibility_evidence_are_present() -> None:
    assert "Apache License" in (ROOT / "LICENSE").read_text(encoding="utf-8")
    assert "Meridian Kafka Adapter" in (ROOT / "NOTICE").read_text(encoding="utf-8")
    compatibility = json.loads(
        (ROOT / "src/meridian_storage/adapters/kafka/compatibility.json").read_text(
            encoding="utf-8"
        )
    )
    assert compatibility["distribution"] == "meridian-storage-kafka==1.0.0"
    assert compatibility["brokerMatrix"]["supported"] == ["4.1.2", "4.2.1", "4.3.1"]
    assert compatibility["pins"]["meridian-storage-streaming"]["version"] == "1.0.0"


def test_boundary_verifier_rejects_lifecycle_and_package_drift() -> None:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts/verify_boundaries.py")],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip().endswith("PASS")


def test_public_package_has_no_native_query_or_lifecycle_mutation_surface() -> None:
    import meridian_storage.adapters.kafka as kafka

    public = set(kafka.__all__)
    assert not {"NativeQuery", "create_topic", "delete_topic", "alter_partition"} & public
    factory = kafka.KafkaAdapterFactory
    assert not {
        "create_topics",
        "delete_topics",
        "create_partitions",
        "alter_configs",
    } & set(dir(factory))

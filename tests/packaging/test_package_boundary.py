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
    assert project["version"] == "1.1.0"
    assert project["license"] == "Apache-2.0"
    assert project["dependencies"] == [
        "confluent-kafka>=2.15.0,<3",
        "meridian-storage-core>=1.1.0,<2",
        "meridian-storage-semantics>=2.0.1,<3",
        "meridian-storage-streaming>=1.0.1,<2",
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
    assert compatibility["distribution"] == "meridian-storage-kafka==1.1.0"
    assert "never runtime membership" in compatibility["releaseSelectionPolicy"]
    pins = compatibility["pins"]
    assert pins["meridian-storage-core"]["version"] == "1.1.0"
    assert pins["meridian-storage-core"]["wheelSha256"] == (
        "fc7372a17993f43ec285e3d52c0e9c458ef8e6f5ce13bb2d1b01b064aa8eff49"
    )
    assert pins["meridian-storage-semantics"]["version"] == "2.0.1"
    assert pins["meridian-storage-streaming"]["version"] == "1.0.1"
    assert pins["meridian-storage-streaming"]["wheelSha256"] == (
        "8a14158255a1594e953fc0929c6476e4b9f1a59c8c7b219470a431cdebdf8582"
    )


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

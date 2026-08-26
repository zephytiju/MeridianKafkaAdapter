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
    assert project["version"] == "1.0.1"
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
    assert compatibility["distribution"] == "meridian-storage-kafka==1.0.1"
    assert compatibility["brokerMatrix"]["supported"] == ["4.1.2", "4.2.1", "4.3.1"]
    pins = compatibility["pins"]
    assert pins["meridian-storage-core"]["sdistSha256"] == (
        "2c44d44569a380f44ea7f797e7fe623d0242fa79b6bc34606d6bad1bc53f2d5a"
    )
    assert pins["meridian-storage-semantics"]["sdistSha256"] == (
        "02605909db5dc7ff22d4ae5e3ae1b3fe6c25a68e9ecaa4c3ead36082848d0311"
    )
    assert pins["meridian-storage-streaming"]["version"] == "1.0.0"
    assert pins["meridian-storage-streaming"]["sdistSha256"] == (
        "a5b259c03ddf82dde8d1e6696e492a6c62f540151633b722107e8518c7cb5831"
    )
    assert pins["meridian-storage-streaming"]["conformanceFingerprint"] == (
        "sha256:f8aa3e2c092062d1c758722b9ab2d9388f0deedeabf214f45e6212dc29e7cbff"
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

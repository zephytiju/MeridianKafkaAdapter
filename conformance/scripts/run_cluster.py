# SPDX-License-Identifier: Apache-2.0
"""Run isolated Kafka conformance and emit a canonical evidence ledger entry."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess  # nosec B404
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree  # nosec B405

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "conformance" / "docker-compose.yml"
EVIDENCE_DIR = ROOT / "evidence"
SUPPORTED_VERSIONS = ("4.1.2", "4.2.1", "4.3.1")
ACCEPTANCE_COVERAGE = {
    "ack-recovery": "test_redelivery_ack_recovery_and_dead_letter",
    "acl-denial": "test_acl_denial_and_live_credential_rotation",
    "broker-controller-failure": "test_broker_controller_failure_is_normalized_and_recovers",
    "compaction": "test_compaction_preserves_latest_keyed_event",
    "credential-rotation": "test_acl_denial_and_live_credential_rotation",
    "cursor-expiry": "test_cursor_ttl_and_retention_expiry",
    "dead-letter": "test_redelivery_ack_recovery_and_dead_letter",
    "finite-consume": "test_publish_batch_finite_consume_order_ack_recovery_probe_and_telemetry",
    "idempotent-producer": "test_idempotent_producer_and_transactional_consume_publish",
    "order": "test_publish_batch_finite_consume_order_ack_recovery_probe_and_telemetry",
    "partition-migration": "test_partition_migration_detection_and_iac_recovery",
    "publish": "test_publish_batch_finite_consume_order_ack_recovery_probe_and_telemetry",
    "rebalance": "test_rebalance_invalidates_inflight_delivery_and_recovers",
    "recovery": "test_partition_migration_detection_and_iac_recovery",
    "redelivery": "test_redelivery_ack_recovery_and_dead_letter",
    "retention": "test_cursor_ttl_and_retention_expiry",
    "schema-evolution": "test_schema_evolution_accepts_compatible_and_rejects_incompatible",
    "telemetry": "test_publish_batch_finite_consume_order_ack_recovery_probe_and_telemetry",
    "transactional-consume-publish": "test_idempotent_producer_and_transactional_consume_publish",
}


def _run(
    command: list[str],
    *,
    environment: dict[str, str],
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # nosec B603
        command,
        cwd=ROOT,
        env=environment,
        check=check,
        text=True,
        capture_output=capture,
        timeout=600,
    )


def _source_tree_sha256() -> str:
    digest = hashlib.sha256()
    selected_roots = (
        ROOT / ".github",
        ROOT / "conformance",
        ROOT / "docs",
        ROOT / "scripts",
        ROOT / "src",
        ROOT / "tests",
    )
    files = [ROOT / name for name in ("pyproject.toml", "README.md", "SECURITY.md")]
    for selected_root in selected_roots:
        files.extend(path for path in selected_root.rglob("*") if path.is_file())
    for path in sorted(files):
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".tmp", ".xml"}:
            continue
        relative = path.relative_to(ROOT).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        payload = path.read_bytes()
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return f"sha256:{digest.hexdigest()}"


def _image_digest(image: str, environment: dict[str, str]) -> str:
    completed = _run(
        ["docker", "image", "inspect", image, "--format", "{{json .RepoDigests}}"],
        environment=environment,
        capture=True,
    )
    values = json.loads(completed.stdout.strip())
    if not isinstance(values, list) or not values or not isinstance(values[0], str):
        raise RuntimeError("Docker did not return an immutable Kafka image digest")
    return values[0]


def _parse_junit(path: Path) -> tuple[dict[str, int | float], list[dict[str, object]]]:
    root = ElementTree.parse(path).getroot()  # nosec B314
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    totals: dict[str, int | float] = {
        "tests": sum(int(item.attrib.get("tests", "0")) for item in suites),
        "failures": sum(int(item.attrib.get("failures", "0")) for item in suites),
        "errors": sum(int(item.attrib.get("errors", "0")) for item in suites),
        "skipped": sum(int(item.attrib.get("skipped", "0")) for item in suites),
        "seconds": round(sum(float(item.attrib.get("time", "0")) for item in suites), 3),
    }
    cases: list[dict[str, object]] = []
    for suite in suites:
        for item in suite.findall("testcase"):
            name = item.attrib.get("name", "unknown")
            status = "passed"
            if item.find("failure") is not None:
                status = "failed"
            elif item.find("error") is not None:
                status = "error"
            elif item.find("skipped") is not None:
                status = "skipped"
            cases.append(
                {
                    "name": name,
                    "seconds": round(float(item.attrib.get("time", "0")), 3),
                    "status": status,
                }
            )
    return totals, sorted(cases, key=lambda value: str(value["name"]))


def _sha256(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-version", required=True, choices=SUPPORTED_VERSIONS)
    parser.add_argument("--port", type=int, default=19094)
    parser.add_argument(
        "--full",
        action="store_true",
        help="enable controlled broker/controller stop-start failure injection",
    )
    return parser.parse_args()


def main() -> int:
    arguments = _arguments()
    version = str(arguments.kafka_version)
    port = int(arguments.port)
    if not 1024 <= port <= 65_535:
        raise ValueError("--port must be between 1024 and 65535")
    project = f"meridian-kafka-conformance-{version.replace('.', '')}"
    if re.fullmatch(r"[a-z0-9-]+", project) is None:
        raise ValueError("compose project name is invalid")
    environment = dict(os.environ)
    environment.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": f"127.0.0.1:{port}",
            "KAFKA_ENGINE_VERSION": version,
            "KAFKA_PORT": str(port),
            "KAFKA_VERSION": version,
            "MERIDIAN_KAFKA_COMPOSE_FILE": str(COMPOSE_FILE),
            "MERIDIAN_KAFKA_COMPOSE_PROJECT": project,
            "MERIDIAN_KAFKA_DESTRUCTIVE": "1" if arguments.full else "0",
        }
    )
    compose = ["docker", "compose", "-p", project, "-f", str(COMPOSE_FILE)]
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    junit = EVIDENCE_DIR / f"cluster-conformance-{version}.xml"
    evidence_path = EVIDENCE_DIR / f"cluster-conformance-{version}.json"
    log_path = EVIDENCE_DIR / f"cluster-conformance-{version}.log"
    started_at = _utc_now()
    result_code = 1
    image = f"apache/kafka:{version}"
    image_digest = "unavailable"
    pytest_command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "tests/cluster",
        "-m",
        "cluster",
        "--no-cov",
        "--timeout=180",
        f"--junitxml={junit}",
    ]
    try:
        _run([*compose, "up", "-d", "--wait", "--wait-timeout", "180"], environment=environment)
        image_digest = _image_digest(image, environment)
        completed = _run(pytest_command, environment=environment, check=False, capture=True)
        result_code = completed.returncode
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        if result_code != 0:
            logs = _run(
                [*compose, "logs", "--no-color", "broker"],
                environment=environment,
                check=False,
                capture=True,
            )
            log_path.write_text(logs.stdout + logs.stderr, encoding="utf-8")
    finally:
        _run(
            [*compose, "down", "-v", "--remove-orphans"],
            environment=environment,
            check=False,
        )

    totals: dict[str, int | float] = {
        "tests": 0,
        "failures": 0,
        "errors": 1,
        "skipped": 0,
        "seconds": 0.0,
    }
    cases: list[dict[str, object]] = []
    junit_sha256 = "unavailable"
    if junit.exists():
        totals, cases = _parse_junit(junit)
        junit_sha256 = _sha256(junit)
    evidence: dict[str, Any] = {
        "acceptanceCoverage": ACCEPTANCE_COVERAGE,
        "adapter": {
            "distribution": "meridian-storage-kafka",
            "version": "1.0.0",
        },
        "dependencies": {
            "confluent-kafka": "2.15.0",
            "meridian-storage-core": "1.0.0",
            "meridian-storage-semantics": "1.0.0",
            "meridian-storage-streaming": "1.0.0",
        },
        "engine": {
            "image": image,
            "imageDigest": image_digest,
            "mode": "single-node-kraft-combined",
            "version": version,
        },
        "finishedAt": _utc_now(),
        "fullFailureInjection": bool(arguments.full),
        "junitSha256": junit_sha256,
        "repository": "https://github.com/zephytiju/MeridianKafkaAdapter",
        "schemaVersion": "meridian.kafka.conformance-evidence.v1",
        "securityProfile": {
            "aclDefault": "deny",
            "authentication": "SASL/PLAIN",
            "authorizer": "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
            "scope": "isolated-test-only",
            "transport": "SASL_PLAINTEXT",
        },
        "sourceTreeSha256": _source_tree_sha256(),
        "startedAt": started_at,
        "status": "passed" if result_code == 0 else "failed",
        "testCases": cases,
        "totals": totals,
    }
    evidence_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"conformance evidence: {evidence_path}")
    return result_code


if __name__ == "__main__":
    raise SystemExit(main())

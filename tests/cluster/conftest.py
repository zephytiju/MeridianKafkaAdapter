# SPDX-License-Identifier: Apache-2.0
"""External provisioning fixture for the isolated real Kafka cluster."""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess  # nosec B404 - restricted to the isolated compose fixture
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import (  # type: ignore[attr-defined]
    AclBinding,
    AclOperation,
    AclPermissionType,
    AdminClient,
    NewPartitions,
    NewTopic,
    ResourcePatternType,
    ResourceType,
)

from meridian_storage.runtime.config import BindingConfig
from meridian_storage.spi import AdapterCreateContext, SecretValue
from tests.support import (
    DLQ_REF,
    GROUP_REF,
    STREAM_REF,
    binding_document,
)

_PREFIX = "meridian-conformance-"
_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE = _ROOT / "conformance" / "docker-compose.yml"


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("test Binding fixture expected an object")
    return cast(dict[str, object], value)


@dataclass(frozen=True, slots=True)
class ClusterCase:
    harness: ClusterHarness
    topic: str
    dead_letter_topic: str
    group_id: str
    transactional_id: str
    partitions: int
    retention_ms: int
    compacted: bool

    def context(
        self,
        *,
        username: str = "meridian",
        password: str = "meridian-secret-one",
        transaction_enabled: bool = False,
        acknowledgement_timeout_ms: int = 2_000,
        max_delivery_attempts: int = 2,
        cursor_ttl_ms: int = 60_000,
        partitions: int | None = None,
    ) -> AdapterCreateContext:
        selected_partitions = self.partitions if partitions is None else partitions
        document = binding_document(
            endpoint=self.harness.bootstrap_servers,
            engine_version=self.harness.engine_version,
            transaction_enabled=transaction_enabled,
            acknowledgement_timeout_ms=acknowledgement_timeout_ms,
            max_delivery_attempts=max_delivery_attempts,
        )
        document["id"] = f"kafka-{self.topic}"
        client = _mapping(document["client"])
        client["operationTimeoutMs"] = 8_000
        settings = _mapping(document["settings"])
        # Range readers derive an ephemeral group from clientId, so keep the
        # IaC-provisioned ACL prefix on that client-generated group as well.
        settings["clientId"] = f"{self.topic}-client"
        settings["cursorTtlMs"] = cursor_ttl_ms
        settings["saslMechanism"] = "PLAIN"
        settings["transactionalId"] = self.transactional_id if transaction_enabled else None
        settings["sessionTimeoutMs"] = 10_000
        settings["maxPollIntervalMs"] = 60_000
        resources = _mapping(settings["resources"])
        stream = _mapping(resources[str(STREAM_REF)])
        stream["topic"] = self.topic
        stream["logicalPartitions"] = selected_partitions
        stream["retentionMs"] = self.retention_ms
        stream["compacted"] = self.compacted
        dead_letter = _mapping(resources[str(DLQ_REF)])
        dead_letter["topic"] = self.dead_letter_topic
        group = _mapping(resources[str(GROUP_REF)])
        group["groupId"] = self.group_id
        binding = BindingConfig.from_mapping(document, "$.bindings[0]")
        identity = {
            "principal": f"User:{username}",
            "username": username,
        }
        credential = {
            "password": password,
            "privateKeyPem": None,
            "privateKeyPassword": None,
            "cursorKeys": [
                {
                    "id": "active",
                    "keyBase64": base64.b64encode(b"real-cluster-cursor-key-material").decode(
                        "ascii"
                    ),
                    "active": True,
                }
            ],
        }
        return AdapterCreateContext(
            binding,
            SecretValue(json.dumps(identity, sort_keys=True).encode("utf-8")),
            SecretValue(json.dumps(credential, sort_keys=True).encode("utf-8")),
        )


@dataclass(slots=True)
class ClusterHarness:
    bootstrap_servers: str
    engine_version: str
    admin: AdminClient
    compose_file: Path
    compose_project: str
    _topics: set[str] = field(default_factory=set)

    def client_configuration(
        self,
        *,
        username: str = "meridian",
        password: str = "meridian-secret-one",
    ) -> dict[str, object]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": username,
            "sasl.password": password,
            "socket.timeout.ms": 8_000,
            "request.timeout.ms": 8_000,
        }

    def producer(
        self,
        *,
        username: str = "meridian",
        password: str = "meridian-secret-one",
    ) -> Producer:
        return Producer(self.client_configuration(username=username, password=password))

    def consumer(
        self,
        group_id: str,
        *,
        username: str = "meridian",
        password: str = "meridian-secret-one",
    ) -> Consumer:
        config = self.client_configuration(username=username, password=password)
        config.update(
            {
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
            }
        )
        return Consumer(config)

    def create_case(
        self,
        label: str,
        *,
        partitions: int = 2,
        retention_ms: int = 60_000,
        compacted: bool = False,
        segment_bytes: int | None = None,
    ) -> ClusterCase:
        normalized = re.sub(r"[^a-z0-9-]", "-", label.lower()).strip("-")
        suffix = uuid.uuid4().hex[:10]
        stem = f"{_PREFIX}{normalized}-{suffix}"
        topic = f"{stem}-events"
        dead_letter_topic = f"{stem}-dead-letters"
        topic_config = {
            "cleanup.policy": "compact,delete" if compacted else "delete",
            "retention.ms": str(retention_ms),
            "min.insync.replicas": "1",
        }
        if segment_bytes is not None:
            topic_config.update(
                {
                    "segment.bytes": str(segment_bytes),
                    "segment.ms": "1000",
                    "file.delete.delay.ms": "0",
                    "min.cleanable.dirty.ratio": "0.01",
                    "min.compaction.lag.ms": "0",
                    "delete.retention.ms": "1000",
                    "max.message.bytes": "2000000",
                }
            )
        definitions = [
            NewTopic(topic, partitions, 1, config=topic_config),
            NewTopic(
                dead_letter_topic,
                1,
                1,
                config={
                    "cleanup.policy": "delete",
                    "retention.ms": "60000",
                    "min.insync.replicas": "1",
                },
            ),
        ]
        futures = self.admin.create_topics(definitions, operation_timeout=15)
        for name, future in futures.items():
            future.result(20)
            self._topics.add(name)
        self._wait_for_partitions(topic, partitions)
        self._wait_for_partitions(dead_letter_topic, 1)
        return ClusterCase(
            self,
            topic,
            dead_letter_topic,
            f"{stem}-group",
            f"{stem}-tx",
            partitions,
            retention_ms,
            compacted,
        )

    def increase_partitions(self, topic: str, total: int) -> None:
        self.admin.create_partitions([NewPartitions(topic, total)])[topic].result(20)
        self._wait_for_partitions(topic, total)

    def wait_ready(self, timeout: float = 90) -> None:
        deadline = time.monotonic() + timeout
        last_error: BaseException | None = None
        while time.monotonic() < deadline:
            try:
                metadata = self.admin.list_topics(timeout=5)
                if getattr(metadata, "brokers", None):
                    return
            except BaseException as exc:
                last_error = exc
            time.sleep(1)
        raise RuntimeError("Kafka did not recover before the conformance deadline") from last_error

    def stop_broker(self) -> None:
        self._compose("stop", "broker")

    def start_broker(self) -> None:
        self._compose("start", "broker")
        self.wait_ready()

    def cleanup(self) -> None:
        if not self._topics:
            return
        try:
            futures = self.admin.delete_topics(sorted(self._topics), operation_timeout=15)
            for future in futures.values():
                future.result(20)
        except BaseException:
            # The compose runner destroys the isolated cluster even if best-effort cleanup fails.
            pass

    def _wait_for_partitions(self, topic: str, expected: int) -> None:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            metadata = self.admin.list_topics(topic=topic, timeout=5)
            selected = getattr(metadata, "topics", {}).get(topic)
            if selected is not None and len(getattr(selected, "partitions", {})) == expected:
                return
            time.sleep(0.25)
        raise RuntimeError(f"Kafka topic {topic!r} did not expose {expected} partitions")

    def _compose(self, *arguments: str) -> None:
        environment = dict(os.environ)
        environment["KAFKA_VERSION"] = self.engine_version
        subprocess.run(  # nosec B603 - fixed docker/compose executable and isolated project
            [
                "docker",
                "compose",
                "-p",
                self.compose_project,
                "-f",
                str(self.compose_file),
                *arguments,
            ],
            check=True,
            cwd=_ROOT,
            env=environment,
            timeout=120,
        )


def _grant_data_plane_acls(admin: AdminClient) -> None:
    bindings: list[AclBinding] = []
    for username in ("meridian", "rotated"):
        principal = f"User:{username}"
        bindings.extend(
            [
                AclBinding(
                    resource_type,
                    _PREFIX,
                    ResourcePatternType.PREFIXED,
                    principal,
                    "*",
                    AclOperation.ALL,
                    AclPermissionType.ALLOW,
                )
                for resource_type in (
                    ResourceType.TOPIC,
                    ResourceType.GROUP,
                    ResourceType.TRANSACTIONAL_ID,
                )
            ]
        )
        bindings.extend(
            [
                AclBinding(
                    ResourceType.BROKER,
                    "kafka-cluster",
                    ResourcePatternType.LITERAL,
                    principal,
                    "*",
                    operation,
                    AclPermissionType.ALLOW,
                )
                for operation in (AclOperation.DESCRIBE, AclOperation.IDEMPOTENT_WRITE)
            ]
        )
    for future in admin.create_acls(bindings).values():
        future.result(20)


@pytest.fixture(scope="session")
def kafka_cluster() -> Iterator[ClusterHarness]:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if bootstrap is None:
        pytest.skip("KAFKA_BOOTSTRAP_SERVERS is required for real-cluster conformance")
    engine_version = os.environ.get("KAFKA_ENGINE_VERSION", "4.3.1")
    compose_file = Path(os.environ.get("MERIDIAN_KAFKA_COMPOSE_FILE", str(_COMPOSE))).resolve()
    compose_project = os.environ.get("MERIDIAN_KAFKA_COMPOSE_PROJECT", "meridian-kafka-conformance")
    admin = AdminClient(
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "admin",
            "sasl.password": "admin-secret",
            "socket.timeout.ms": 8_000,
            "request.timeout.ms": 8_000,
        }
    )
    harness = ClusterHarness(
        bootstrap,
        engine_version,
        admin,
        compose_file,
        compose_project,
    )
    harness.wait_ready()
    _grant_data_plane_acls(admin)
    try:
        yield harness
    finally:
        harness.cleanup()


__all__ = ["ClusterCase", "ClusterHarness"]

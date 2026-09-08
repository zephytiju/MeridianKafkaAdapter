# SPDX-License-Identifier: Apache-2.0
"""Deterministic Kafka doubles and released-contract fixtures."""

from __future__ import annotations

import base64
import json
import zlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from importlib.metadata import version
from types import SimpleNamespace
from typing import cast

from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka.admin import ConfigResource  # type: ignore[attr-defined]

from meridian_storage import Operation, OperationContext, ResourceRef, SchemaRef
from meridian_storage.adapters.kafka.clients import (
    AdminLike,
    ConsumerLike,
    DeliveryCallback,
    KafkaClientConfig,
    MessageLike,
    ProducerLike,
)
from meridian_storage.adapters.kafka.config import KafkaBindingSettings
from meridian_storage.adapters.kafka.probe.protocol import BASE_APIS, TRANSACTION_APIS
from meridian_storage.runtime.config import BindingConfig
from meridian_storage.spi import (
    AdapterCreateContext,
    ExecutionRequest,
    PhysicalResource,
    SecretValue,
)
from meridian_storage.streaming import Event, StreamingCatalogProvider

ZERO_FINGERPRINT = f"sha256:{'0' * 64}"
STREAM_FINGERPRINT = f"sha256:{'1' * 64}"
SUBSCRIPTION_FINGERPRINT = f"sha256:{'2' * 64}"
GROUP_FINGERPRINT = f"sha256:{'3' * 64}"
DLQ_FINGERPRINT = f"sha256:{'4' * 64}"
SCHEMA_FINGERPRINT = f"sha256:{'5' * 64}"
COMPATIBLE_SCHEMA_FINGERPRINT = f"sha256:{'6' * 64}"

STREAM_REF = ResourceRef("streaming", "conformance", "events")
SUBSCRIPTION_REF = ResourceRef("streaming", "conformance", "subscription")
GROUP_REF = ResourceRef("streaming", "conformance", "group")
DLQ_REF = ResourceRef("streaming", "conformance", "dead_letters")
SCHEMA_REF = SchemaRef("streaming", "conformance", "event", "1.0.0")
DLQ_SCHEMA_REF = SchemaRef("streaming", "conformance", "dead_letter", "1.0.0")


@dataclass(slots=True)
class FakeRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes | None]]
    committed: bool = True
    aborted: bool = False


class FakeMessage:
    def __init__(self, record: FakeRecord, error: KafkaError | None = None) -> None:
        self.record = record
        self._error = error

    def error(self) -> KafkaError | None:
        return self._error

    def key(self) -> bytes | None:
        return self.record.key

    def value(self) -> bytes | None:
        return self.record.value

    def headers(self) -> list[tuple[str, bytes | None]] | None:
        return list(self.record.headers)

    def topic(self) -> str:
        return self.record.topic

    def partition(self) -> int:
        return self.record.partition

    def offset(self) -> int:
        return self.record.offset


@dataclass(slots=True)
class FakeTopicMetadata:
    partitions: Mapping[int, object]
    error: KafkaError | None = None


@dataclass(slots=True)
class FakeClusterMetadata:
    topics: Mapping[str, FakeTopicMetadata]
    brokers: Mapping[int, object]
    controller_id: int = 1
    cluster_id: str = "fake-cluster"


class FakeBroker:
    def __init__(self) -> None:
        self.records: dict[str, list[list[FakeRecord]]] = {}
        self.configs: dict[str, dict[str, str]] = {}
        self.committed: dict[tuple[str, str, int], int] = {}
        self.available = True
        self.create_topic("events", partitions=2, retention_ms=60_000, compacted=False)
        self.create_topic("dead-letters", partitions=1, retention_ms=60_000, compacted=False)

    def create_topic(
        self,
        name: str,
        *,
        partitions: int,
        retention_ms: int,
        compacted: bool,
    ) -> None:
        self.records[name] = [[] for _ in range(partitions)]
        self.configs[name] = {
            "cleanup.policy": "compact,delete" if compacted else "delete",
            "retention.ms": str(retention_ms),
        }

    def append(
        self,
        topic: str,
        *,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes | None]],
        partition: int = -1,
        committed: bool = True,
    ) -> FakeRecord:
        if not self.available:
            raise RuntimeError("broker unavailable")
        partitions = self.records[topic]
        selected = partition
        if selected < 0:
            selected = 0 if key is None else zlib.crc32(key) % len(partitions)
        records = partitions[selected]
        record = FakeRecord(
            topic,
            selected,
            len(records),
            key,
            value,
            list(headers),
            committed,
        )
        records.append(record)
        return record

    def reset(self) -> None:
        for partitions in self.records.values():
            for records in partitions:
                records.clear()
        self.committed.clear()
        self.available = True

    def metadata(self, topic: str | None = None) -> FakeClusterMetadata:
        if not self.available:
            raise RuntimeError("broker unavailable")
        names = tuple(self.records) if topic is None else (topic,)
        topics = {
            name: FakeTopicMetadata(
                {partition: SimpleNamespace() for partition in range(len(self.records[name]))}
            )
            for name in names
            if name in self.records
        }
        return FakeClusterMetadata(topics, {1: SimpleNamespace(id=1, host="fake", port=9092)})


class FakeFuture:
    def __init__(self, value: object) -> None:
        self.value = value

    def result(self, timeout: float | None = None) -> object:
        del timeout
        return self.value


class FakeAdmin:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> object:
        del timeout
        return self.broker.metadata(topic)

    def describe_configs(
        self, resources: list[ConfigResource], **kwargs: object
    ) -> Mapping[ConfigResource, FakeFuture]:
        del kwargs
        return {
            resource: FakeFuture(
                {
                    key: SimpleNamespace(value=value)
                    for key, value in self.broker.configs[resource.name].items()
                }
            )
            for resource in resources
        }


class FakeProducer:
    def __init__(self, broker: FakeBroker, *, transactional: bool = False) -> None:
        self.broker = broker
        self.transactional = transactional
        self.initialized = False
        self.active = False
        self.pending: list[FakeRecord] = []
        self.pending_offsets: tuple[list[TopicPartition], object] | None = None
        self.fail_next: BaseException | None = None

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int = -1,
        on_delivery: DeliveryCallback | None = None,
        headers: list[tuple[str, bytes | None]] | None = None,
    ) -> None:
        if self.fail_next is not None:
            failure, self.fail_next = self.fail_next, None
            raise failure
        record = self.broker.append(
            topic,
            key=key,
            value=value,
            headers=headers or [],
            partition=partition,
            committed=not self.active,
        )
        if self.active:
            self.pending.append(record)
        if on_delivery is not None:
            on_delivery(None, FakeMessage(record))

    def poll(self, timeout: float) -> int:
        del timeout
        return 0

    def flush(self, timeout: float = -1) -> int:
        del timeout
        return 0

    def init_transactions(self, timeout: float = -1) -> None:
        del timeout
        if not self.transactional:
            raise RuntimeError("producer has no transactional id")
        self.initialized = True

    def begin_transaction(self) -> None:
        if not self.initialized or self.active:
            raise RuntimeError("transaction producer is not ready")
        self.active = True
        self.pending.clear()
        self.pending_offsets = None

    def send_offsets_to_transaction(
        self,
        positions: list[TopicPartition],
        group_metadata: object,
        timeout: float = -1,
    ) -> None:
        del timeout
        if not self.active:
            raise RuntimeError("transaction is not active")
        self.pending_offsets = (positions, group_metadata)

    def commit_transaction(self, timeout: float = -1) -> None:
        del timeout
        if not self.active:
            raise RuntimeError("transaction is not active")
        for record in self.pending:
            record.committed = True
        if self.pending_offsets is not None:
            positions, metadata = self.pending_offsets
            group_id = cast(str, metadata)
            for position in positions:
                self.broker.committed[(group_id, position.topic, position.partition)] = (
                    position.offset
                )
        self.active = False
        self.pending.clear()
        self.pending_offsets = None

    def abort_transaction(self, timeout: float = -1) -> None:
        del timeout
        if not self.active:
            raise RuntimeError("transaction is not active")
        for record in self.pending:
            record.aborted = True
        self.active = False
        self.pending.clear()
        self.pending_offsets = None


class FakeConsumer:
    def __init__(self, broker: FakeBroker, config: KafkaClientConfig) -> None:
        self.broker = broker
        self.group_id = cast(str, config["group.id"])
        self.read_committed = config.get("isolation.level") == "read_committed"
        self.range_reader = bool(config.get("enable.partition.eof", False))
        self._topics: list[str] = []
        self._on_assign: Callable[[ConsumerLike, list[TopicPartition]], None] | None = None
        self._on_revoke: Callable[[ConsumerLike, list[TopicPartition]], None] | None = None
        self._assigned: list[TopicPartition] = []
        self._positions: dict[tuple[str, int], int] = {}
        self._paused: set[int] = set()
        self.closed = False

    def subscribe(
        self,
        topics: list[str],
        on_assign: Callable[[ConsumerLike, list[TopicPartition]], None] | None = None,
        on_revoke: Callable[[ConsumerLike, list[TopicPartition]], None] | None = None,
    ) -> None:
        self._topics = list(topics)
        self._on_assign = on_assign
        self._on_revoke = on_revoke

    def _ensure_assignment(self) -> None:
        if self._assigned or not self._topics:
            return
        selected = [
            TopicPartition(topic, partition)
            for topic in self._topics
            for partition in range(len(self.broker.records[topic]))
        ]
        if self._on_assign is None:
            self.assign(selected)
        else:
            self._on_assign(self, selected)

    def assign(self, partitions: list[TopicPartition]) -> None:
        self._assigned = [TopicPartition(item.topic, item.partition) for item in partitions]
        for item in partitions:
            selected = item.offset
            if selected < 0:
                selected = self.broker.committed.get((self.group_id, item.topic, item.partition), 0)
            self._positions[(item.topic, item.partition)] = selected

    def unassign(self) -> None:
        self._assigned.clear()

    def assignment(self) -> list[TopicPartition]:
        self._ensure_assignment()
        return list(self._assigned)

    def poll(self, timeout: float = -1) -> MessageLike | None:
        del timeout
        if not self.broker.available:
            raise RuntimeError("broker unavailable")
        self._ensure_assignment()
        for assigned in self._assigned:
            if assigned.partition in self._paused:
                continue
            coordinate = (assigned.topic, assigned.partition)
            records = self.broker.records[assigned.topic][assigned.partition]
            position = self._positions.get(coordinate, 0)
            while position < len(records):
                record = records[position]
                position += 1
                self._positions[coordinate] = position
                if record.aborted or (self.read_committed and not record.committed):
                    continue
                return FakeMessage(record)
        return None

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[MessageLike]:
        result: list[MessageLike] = []
        while len(result) < num_messages:
            selected = self.poll(timeout)
            if selected is None:
                break
            result.append(selected)
        return result

    def commit(
        self,
        message: MessageLike | None = None,
        offsets: list[TopicPartition] | None = None,
        asynchronous: bool = True,
    ) -> list[TopicPartition] | None:
        del asynchronous
        selected = offsets
        if selected is None and message is not None:
            selected = [TopicPartition(message.topic(), message.partition(), message.offset() + 1)]
        for item in selected or []:
            self.broker.committed[(self.group_id, item.topic, item.partition)] = item.offset
        return selected

    def committed(
        self, partitions: list[TopicPartition], timeout: float = -1
    ) -> list[TopicPartition]:
        del timeout
        return [
            TopicPartition(
                item.topic,
                item.partition,
                self.broker.committed.get((self.group_id, item.topic, item.partition), -1001),
            )
            for item in partitions
        ]

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        return [
            TopicPartition(
                item.topic,
                item.partition,
                self._positions.get((item.topic, item.partition), -1001),
            )
            for item in partitions
        ]

    def seek(self, partition: TopicPartition) -> None:
        self._positions[(partition.topic, partition.partition)] = partition.offset

    def pause(self, partitions: list[TopicPartition]) -> None:
        self._paused.update(item.partition for item in partitions)

    def resume(self, partitions: list[TopicPartition]) -> None:
        self._paused.difference_update(item.partition for item in partitions)

    def get_watermark_offsets(
        self,
        partition: TopicPartition,
        timeout: float | None = None,
        cached: bool = False,
    ) -> tuple[int, int]:
        del timeout, cached
        return 0, len(self.broker.records[partition.topic][partition.partition])

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> object:
        del timeout
        return self.broker.metadata(topic)

    def consumer_group_metadata(self) -> object:
        return self.group_id

    def close(self) -> None:
        if self.closed:
            return
        if self._assigned and self._on_revoke is not None:
            self._on_revoke(self, list(self._assigned))
        self.closed = True

    def trigger_rebalance(self) -> None:
        self._ensure_assignment()
        selected = list(self._assigned)
        if self._on_revoke is not None:
            self._on_revoke(self, selected)
        if self._on_assign is not None:
            self._on_assign(self, selected)


class FakeFactory:
    def __init__(self, broker: FakeBroker | None = None) -> None:
        self.broker = broker or FakeBroker()
        self.admin_client = FakeAdmin(self.broker)
        self.producers: list[FakeProducer] = []
        self.consumers: list[FakeConsumer] = []
        self.protocol_versions = {
            key: (minimum, maximum)
            for key, (_, minimum, maximum) in (BASE_APIS | TRANSACTION_APIS).items()
        }

    def api_versions(
        self, settings: KafkaBindingSettings, host: str, port: int, *, deadline: float
    ) -> Mapping[int, tuple[int, int]]:
        del settings, host, port, deadline
        return self.protocol_versions

    def producer(self, config: KafkaClientConfig) -> ProducerLike:
        selected = FakeProducer(self.broker, transactional="transactional.id" in config)
        self.producers.append(selected)
        return selected

    def consumer(self, config: KafkaClientConfig) -> ConsumerLike:
        selected = FakeConsumer(self.broker, config)
        self.consumers.append(selected)
        return selected

    def admin(self, config: KafkaClientConfig) -> AdminLike:
        del config
        return self.admin_client


def binding_document(
    *,
    endpoint: str = "fake:9092",
    engine_version: str = "4.3.1",
    transaction_enabled: bool = True,
    acknowledgement_timeout_ms: int = 5_000,
    max_delivery_attempts: int = 2,
    required_capability_fingerprint: str = ZERO_FINGERPRINT,
) -> dict[str, object]:
    resources = {
        str(STREAM_REF): {
            "kind": "stream",
            "topic": "events",
            "schemaRef": SCHEMA_REF.to_dict(),
            "schemaFingerprint": SCHEMA_FINGERPRINT,
            "compatibleSchemaFingerprints": [COMPATIBLE_SCHEMA_FINGERPRINT],
            "resourceFingerprint": STREAM_FINGERPRINT,
            "logicalPartitions": 2,
            "retentionMs": 60_000,
            "compacted": False,
        },
        str(DLQ_REF): {
            "kind": "stream",
            "topic": "dead-letters",
            "schemaRef": DLQ_SCHEMA_REF.to_dict(),
            "schemaFingerprint": SCHEMA_FINGERPRINT,
            "compatibleSchemaFingerprints": [],
            "resourceFingerprint": DLQ_FINGERPRINT,
            "logicalPartitions": 1,
            "retentionMs": 60_000,
            "compacted": False,
        },
        str(SUBSCRIPTION_REF): {
            "kind": "subscription",
            "stream": str(STREAM_REF),
            "resourceFingerprint": SUBSCRIPTION_FINGERPRINT,
            "acknowledgementTimeoutMs": acknowledgement_timeout_ms,
            "maxDeliveryAttempts": max_delivery_attempts,
            "deadLetterStream": str(DLQ_REF),
            "filter": {},
        },
        str(GROUP_REF): {
            "kind": "consumer-group",
            "subscription": str(SUBSCRIPTION_REF),
            "groupId": "conformance-group",
            "resourceFingerprint": GROUP_FINGERPRINT,
        },
    }
    return {
        "id": "kafka-conformance",
        "adapterId": "meridian.kafka",
        "adapterContract": "1.0.0",
        "engineProfile": "apache-kafka-test",
        "engineVersion": engine_version,
        "endpoint": endpoint,
        "serviceRef": None,
        "physicalNamespace": "conformance",
        "tls": {
            "mode": "disabled",
            "serverName": None,
            "caRef": None,
            "clientCertificateRef": None,
        },
        "identityRef": {"provider": "test", "reference": "identity"},
        "secretRef": {"provider": "test", "reference": "credential"},
        "client": {
            "minSize": 0,
            "maxSize": 8,
            "acquireTimeoutMs": 1_000,
            "idleTimeoutMs": 30_000,
            "operationTimeoutMs": 250,
            "maxResultBytes": 16_777_216,
            "iteratorLifetimeMs": 60_000,
        },
        "requiredCapabilityFingerprint": required_capability_fingerprint,
        "requiredPhysicalFingerprint": None,
        "compatibilityPins": {
            "adapterContract": "1.0.0",
            "clientVersion": version("confluent-kafka"),
            "coreVersion": "1.0.0",
            "coreDistributionVersion": version("meridian-storage-core"),
            "driver": "confluent-kafka",
            "semanticsVersion": version("meridian-storage-semantics"),
            "streamingVersion": version("meridian-storage-streaming"),
        },
        "settings": {
            "clientId": "meridian-kafka-conformance",
            "cursorTtlMs": 60_000,
            "saslMechanism": None,
            "transactionalId": "meridian-kafka-conformance-tx" if transaction_enabled else None,
            "readCommitted": True,
            "autoOffsetReset": "earliest",
            "sessionTimeoutMs": 10_000,
            "maxPollIntervalMs": 60_000,
            "allowPlaintextForTesting": True,
            "resources": resources,
        },
        "extensions": {},
    }


def make_context(
    *,
    endpoint: str = "fake:9092",
    engine_version: str = "4.3.1",
    transaction_enabled: bool = True,
    acknowledgement_timeout_ms: int = 5_000,
    max_delivery_attempts: int = 2,
    required_capability_fingerprint: str = ZERO_FINGERPRINT,
) -> AdapterCreateContext:
    document = binding_document(
        endpoint=endpoint,
        engine_version=engine_version,
        transaction_enabled=transaction_enabled,
        acknowledgement_timeout_ms=acknowledgement_timeout_ms,
        max_delivery_attempts=max_delivery_attempts,
        required_capability_fingerprint=required_capability_fingerprint,
    )
    binding = BindingConfig.from_mapping(document, "$.bindings[0]")
    credential = {
        "password": None,
        "privateKeyPem": None,
        "privateKeyPassword": None,
        "cursorKeys": [
            {
                "id": "active",
                "keyBase64": base64.b64encode(b"k" * 32).decode("ascii"),
                "active": True,
            }
        ],
    }
    return AdapterCreateContext(
        binding,
        SecretValue(b'{"principal":"test-principal","username":null}'),
        SecretValue(json.dumps(credential, sort_keys=True).encode("utf-8")),
    )


def repin_context(
    context: AdapterCreateContext, capability_fingerprint: str
) -> AdapterCreateContext:
    document = context.binding.to_dict()
    document["requiredCapabilityFingerprint"] = capability_fingerprint
    return AdapterCreateContext(
        BindingConfig.from_mapping(document, "$.bindings[0]"),
        context.identity,
        context.credential,
        context.tls_ca,
        context.tls_client_certificate,
    )


def make_event(
    event_id: str,
    *,
    sequence: int = 1,
    logical_partition: str = "a",
    stream: ResourceRef = STREAM_REF,
    schema: SchemaRef = SCHEMA_REF,
) -> dict[str, object]:
    return Event(
        event_id=event_id,
        stream=stream,
        schema=schema,
        data={"sequence": sequence},
        occurred_at="2026-01-01T00:00:00Z",
        produced_at="2026-01-01T00:00:00Z",
        logical_partition_key=logical_partition,
        trace_context={"traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"},
    ).to_dict()


def provider_operation(method: str, **arguments: object) -> Operation:
    provider = StreamingCatalogProvider()
    surface = provider.create_surface()
    expression = getattr(surface, method)(**arguments)
    return provider.normalize(expression)


def publish_operation(
    event_id: str,
    *,
    sequence: int = 1,
    logical_partition: str = "a",
    resource: ResourceRef = STREAM_REF,
    schema: SchemaRef = SCHEMA_REF,
    idempotency_key: str | None = None,
) -> Operation:
    return provider_operation(
        "publish",
        resource=resource.to_dict(),
        data=make_event(
            event_id,
            sequence=sequence,
            logical_partition=logical_partition,
            stream=resource,
            schema=schema,
        ),
        idempotency_key=idempotency_key,
    )


def request(operation: Operation, *, tenant: str | None = "tenant-a") -> ExecutionRequest:
    context = OperationContext(
        "principal:test",
        request_id="request-1",
        tenant=tenant,
    )
    return ExecutionRequest(
        operation,
        context,
        context.request_id or "request-1",
        "execution-1",
        "kafka-conformance",
        1,
        ZERO_FINGERPRINT,
        1,
    )


def physical_resources(context: AdapterCreateContext) -> tuple[PhysicalResource, ...]:
    settings = KafkaBindingSettings.from_context(context)
    return tuple(
        PhysicalResource(
            resource_ref=item.ref,
            resource_fingerprint=item.resource_fingerprint,
            schema_fingerprint=(getattr(item, "schema_fingerprint", None)),
            profile="conformance",
        )
        for item in settings.resources.values()
    )


__all__ = [
    "COMPATIBLE_SCHEMA_FINGERPRINT",
    "DLQ_REF",
    "GROUP_REF",
    "SCHEMA_FINGERPRINT",
    "SCHEMA_REF",
    "STREAM_REF",
    "SUBSCRIPTION_REF",
    "ZERO_FINGERPRINT",
    "FakeAdmin",
    "FakeBroker",
    "FakeConsumer",
    "FakeFactory",
    "FakeMessage",
    "FakeProducer",
    "binding_document",
    "make_context",
    "make_event",
    "physical_resources",
    "provider_operation",
    "publish_operation",
    "repin_context",
    "request",
]

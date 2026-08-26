# SPDX-License-Identifier: Apache-2.0
"""Authenticated metadata probe and read-only physical-resource verification."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from confluent_kafka.admin import (  # type: ignore[attr-defined]
    ConfigResource,
    ResourceType,
)

from meridian_storage import MeridianError
from meridian_storage.spi import (
    AdapterProbe,
    CapabilityManifest,
    PhysicalResource,
    PhysicalVerification,
)
from meridian_storage.streaming import MigrationRequired, StreamingDescriptor

from .._constants import (
    CLIENT_VERSION,
    CORE_TRANSACTION_CONTRACT,
    TRANSACTION_OPERATION_CONTRACT,
)
from ..canonical import sha256_bytes, sha256_fingerprint
from ..clients import AdminLike
from ..config import (
    KafkaBindingSettings,
    PhysicalConsumerGroup,
    PhysicalStream,
    PhysicalSubscription,
)
from ..descriptor import adapter_descriptor
from ..errors import KafkaConfigurationError, normalize_kafka_error
from ..telemetry import KafkaTelemetry


class KafkaProbeEngine:
    def __init__(
        self,
        settings: KafkaBindingSettings,
        admin: AdminLike,
        telemetry: KafkaTelemetry,
    ) -> None:
        self._settings = settings
        self._admin = admin
        self._telemetry = telemetry

    def probe(self) -> AdapterProbe:
        try:
            metadata = self._admin.list_topics(timeout=self._settings.operation_timeout_ms / 1000)
            brokers = getattr(metadata, "brokers", None)
            if not isinstance(brokers, Mapping) or not brokers:
                raise KafkaConfigurationError("authenticated Kafka probe returned no brokers")
            controller_id = getattr(metadata, "controller_id", None)
            if not isinstance(controller_id, int) or controller_id < 0:
                raise KafkaConfigurationError("authenticated Kafka probe returned no controller")
            cluster_id = getattr(metadata, "cluster_id", None)
            cluster_fingerprint = (
                "unavailable"
                if not isinstance(cluster_id, str) or not cluster_id
                else sha256_bytes(cluster_id.encode("utf-8"))
            )
        except KafkaConfigurationError:
            raise
        except BaseException as exc:
            raise normalize_kafka_error(exc, operation_contract="meridian.kafka.probe") from exc
        descriptor = adapter_descriptor()
        available = [item.operation_contract for item in descriptor.capabilities]
        if not self._settings.transaction_enabled:
            available = [
                item
                for item in available
                if item not in {CORE_TRANSACTION_CONTRACT, TRANSACTION_OPERATION_CONTRACT}
            ]
        manifest = CapabilityManifest(
            descriptor,
            self._settings.engine_profile,
            self._settings.engine_version,
            tuple(available),
            extensions={
                "clientVersion": CLIENT_VERSION,
                "coreVersion": "1.0.0",
                "semanticsVersion": "1.0.0",
                "streamingVersion": "1.0.0",
                "streamingDescriptorFingerprint": StreamingDescriptor().fingerprint,
                "securityProfile": (
                    "isolated-plaintext-test"
                    if self._settings.allow_plaintext_for_testing
                    else "authenticated-tls"
                ),
            },
        )
        self._telemetry.emit(
            "meridian.kafka.probe.completed",
            {
                "brokerCount": len(brokers),
                "controllerPresent": True,
                "transactionEnabled": self._settings.transaction_enabled,
            },
        )
        return AdapterProbe(
            manifest,
            evidence={
                "brokerCount": str(len(brokers)),
                "controllerPresent": "true",
                "clusterFingerprint": cluster_fingerprint,
                "securityProfile": cast(str, manifest.extensions["securityProfile"]),
            },
        )

    def verify_physical(self, resources: tuple[PhysicalResource, ...]) -> PhysicalVerification:
        mappings: dict[str, str] = {}
        evidence: dict[str, str] = {}
        topics: dict[str, PhysicalStream] = {}
        for planned in resources:
            configured = self._settings.resource(planned.resource_ref)
            actual_fingerprint = configured.resource_fingerprint
            if planned.resource_fingerprint != actual_fingerprint:
                raise MigrationRequired(
                    "Resource fingerprint differs from the provisioned Kafka mapping",
                    requirement="resource.migration",
                    logical_references=(str(planned.resource_ref),),
                )
            if isinstance(configured, PhysicalStream):
                if (
                    planned.schema_fingerprint is not None
                    and planned.schema_fingerprint != configured.schema_fingerprint
                ):
                    raise MigrationRequired(
                        "Schema fingerprint differs from the provisioned Kafka mapping",
                        requirement="schema.migration",
                        logical_references=(str(planned.resource_ref),),
                    )
                topics[configured.topic] = configured
                physical = {
                    "kind": "stream",
                    "physicalFingerprint": sha256_bytes(configured.topic.encode("utf-8")),
                    "resourceFingerprint": configured.resource_fingerprint,
                    "schemaFingerprint": configured.schema_fingerprint,
                }
            elif isinstance(configured, PhysicalSubscription):
                stream = self._settings.stream(configured.stream)
                topics[stream.topic] = stream
                physical = {
                    "kind": "subscription",
                    "physicalFingerprint": sha256_bytes(stream.topic.encode("utf-8")),
                    "resourceFingerprint": configured.resource_fingerprint,
                }
            elif isinstance(configured, PhysicalConsumerGroup):
                subscription = self._settings.subscription(configured.subscription)
                stream = self._settings.stream(subscription.stream)
                topics[stream.topic] = stream
                physical = {
                    "kind": "consumer-group",
                    "physicalFingerprint": sha256_fingerprint(
                        {
                            "topic": sha256_bytes(stream.topic.encode("utf-8")),
                            "group": sha256_bytes(configured.group_id.encode("utf-8")),
                        }
                    ),
                    "resourceFingerprint": configured.resource_fingerprint,
                }
            else:  # pragma: no cover - closed resource parser makes this unreachable.
                raise AssertionError("unknown physical resource mapping")
            mappings[str(planned.resource_ref)] = sha256_fingerprint(physical)

        for topic, stream in sorted(topics.items()):
            self._verify_topic(topic, stream)
        evidence["verifiedResourceCount"] = str(len(resources))
        evidence["verifiedTopicCount"] = str(len(topics))
        fingerprint = sha256_fingerprint(
            {
                "bindingId": self._settings.binding_id,
                "engineProfile": self._settings.engine_profile,
                "engineVersion": self._settings.engine_version,
                "mappings": mappings,
                "topicCount": len(topics),
            }
        )
        self._telemetry.emit(
            "meridian.kafka.physical-verification.completed",
            {"resourceCount": len(resources), "physicalResourceCount": len(topics)},
        )
        return PhysicalVerification(fingerprint, mappings, evidence)

    def _verify_topic(self, topic: str, stream: PhysicalStream) -> None:
        try:
            metadata = self._admin.list_topics(topic=topic, timeout=5)
            topics = getattr(metadata, "topics", None)
            if not isinstance(topics, Mapping) or topic not in topics:
                raise MigrationRequired(
                    "IaC-provisioned Kafka topic is absent",
                    requirement="resource.provisioning",
                    logical_references=(str(stream.ref),),
                )
            selected = topics[topic]
            error = getattr(selected, "error", None)
            if error is not None:
                raise normalize_kafka_error(error, operation_contract="meridian.kafka.probe")
            partitions = getattr(selected, "partitions", None)
            if not isinstance(partitions, Mapping) or len(partitions) != stream.logical_partitions:
                raise MigrationRequired(
                    "Kafka partition count differs from the Binding fingerprint",
                    requirement="resource.partition-migration",
                    logical_references=(str(stream.ref),),
                )
            resource = ConfigResource(ResourceType.TOPIC, topic)
            future = self._admin.describe_configs([resource])[resource]
            config = future.result(timeout=5)
            if not isinstance(config, Mapping):
                raise KafkaConfigurationError("Kafka topic configuration probe was invalid")
            cleanup_entry = config.get("cleanup.policy")
            retention_entry = config.get("retention.ms")
            cleanup = getattr(cleanup_entry, "value", None)
            retention = getattr(retention_entry, "value", None)
            if not isinstance(cleanup, str) or not isinstance(retention, str):
                raise KafkaConfigurationError("Kafka topic lifecycle configuration is unavailable")
            compacted = "compact" in {item.strip() for item in cleanup.split(",")}
            if compacted != stream.compacted:
                raise MigrationRequired(
                    "Kafka compaction policy differs from the Binding fingerprint",
                    requirement="resource.compaction-migration",
                    logical_references=(str(stream.ref),),
                )
            if int(retention) != stream.retention_ms:
                raise MigrationRequired(
                    "Kafka retention differs from the Binding fingerprint",
                    requirement="resource.retention-migration",
                    logical_references=(str(stream.ref),),
                )
        except MeridianError:
            raise
        except BaseException as exc:
            raise normalize_kafka_error(exc, operation_contract="meridian.kafka.probe") from exc


__all__ = ["KafkaProbeEngine"]

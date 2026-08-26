# SPDX-License-Identifier: Apache-2.0
"""Core 1.0.0 AdapterRuntime and AdapterSession implementation."""

from __future__ import annotations

from threading import RLock
from typing import cast

from meridian_storage import MeridianError, ResourceRef
from meridian_storage.spi import (
    AdapterCreateContext,
    AdapterProbe,
    AdapterSession,
    ExecutionRequest,
    ExecutionResult,
    PhysicalResource,
    PhysicalVerification,
)
from meridian_storage.streaming import (
    MigrationRequired,
    StreamingPolicyDenied,
)

from ._constants import ADAPTER_ID
from .canonical import JsonValue, canonical_json_bytes, sha256_fingerprint
from .clients import AdminLike, ConfluentKafkaClientFactory, KafkaClientFactory
from .compiler import (
    AcknowledgeCommand,
    CompiledCommand,
    GroupPositionCommand,
    KafkaOperationCompiler,
    NegativeAcknowledgeCommand,
    PollCommand,
    PublishCommand,
    ReadRangeCommand,
    SubscribeCommand,
    TransactionalConsumePublishCommand,
    ValidateResourceCommand,
    ValidateSchemaCommand,
)
from .config import KafkaBindingSettings, PhysicalStream
from .consumer import KafkaConsumerEngine
from .cursor import KafkaCursorCodec
from .errors import KafkaConfigurationError, KafkaTransactionAborted, normalize_kafka_error
from .probe import KafkaProbeEngine
from .producer import KafkaProducerEngine
from .telemetry import KafkaTelemetry, KafkaTelemetrySink
from .transactions import KafkaTransactionBridge


class KafkaAdapterFactory:
    """Immutable entry-point factory; optional injection exists only for conformance tests."""

    adapter_id = ADAPTER_ID

    def __init__(
        self,
        *,
        client_factory: KafkaClientFactory | None = None,
        telemetry_sink: KafkaTelemetrySink | None = None,
    ) -> None:
        self._client_factory = client_factory or ConfluentKafkaClientFactory()
        self._telemetry_sink = telemetry_sink

    def create(self, context: AdapterCreateContext) -> KafkaAdapterRuntime:
        return KafkaAdapterRuntime(
            context,
            client_factory=self._client_factory,
            telemetry_sink=self._telemetry_sink,
        )


class KafkaAdapterRuntime:
    def __init__(
        self,
        context: AdapterCreateContext,
        *,
        client_factory: KafkaClientFactory,
        telemetry_sink: KafkaTelemetrySink | None = None,
        shared_telemetry: KafkaTelemetry | None = None,
    ) -> None:
        self._binding = context.binding
        self._settings = KafkaBindingSettings.from_context(context)
        self._client_factory = client_factory
        if telemetry_sink is not None and shared_telemetry is not None:
            raise ValueError("telemetry_sink and shared_telemetry are mutually exclusive")
        self._telemetry = shared_telemetry or KafkaTelemetry(telemetry_sink)
        self._admin: AdminLike | None = None
        self._probe_engine: KafkaProbeEngine | None = None
        self._producer_engine: KafkaProducerEngine | None = None
        self._consumer_engine: KafkaConsumerEngine | None = None
        self._transaction_bridge: KafkaTransactionBridge | None = None
        self._compiler = KafkaOperationCompiler(self._settings)
        self._opened = False
        self._closed = False
        self._lock = RLock()

    @property
    def settings(self) -> KafkaBindingSettings:
        return self._settings

    @property
    def telemetry(self) -> KafkaTelemetry:
        return self._telemetry

    def open(self) -> None:
        with self._lock:
            if self._opened or self._closed:
                raise RuntimeError("Kafka Adapter runtime cannot be opened in its current state")
            configuration = self._settings.client_configuration()
            try:
                admin = self._client_factory.admin(configuration.admin())
                producer_client = self._client_factory.producer(configuration.producer())
                cursor = KafkaCursorCodec(
                    self._settings.secrets.cursor_keys,
                    ttl_ms=self._settings.cursor_ttl_ms,
                )
                producer = KafkaProducerEngine(
                    producer_client,
                    cursor,
                    self._telemetry,
                    timeout_ms=self._settings.operation_timeout_ms,
                )
                consumer = KafkaConsumerEngine(
                    self._settings,
                    configuration,
                    self._client_factory,
                    cursor,
                    producer,
                    self._telemetry,
                )
                self._admin = admin
                self._probe_engine = KafkaProbeEngine(self._settings, admin, self._telemetry)
                self._producer_engine = producer
                self._consumer_engine = consumer
                transaction: KafkaTransactionBridge | None = None
                if self._settings.transaction_enabled:
                    transaction = KafkaTransactionBridge(
                        self._client_factory.producer(configuration.producer(transactional=True)),
                        self._telemetry,
                        timeout_ms=self._settings.operation_timeout_ms,
                    )
                    self._transaction_bridge = transaction
                    transaction.open()
                self._opened = True
                self._telemetry.emit(
                    "meridian.kafka.runtime.opened",
                    {
                        "transactionEnabled": self._settings.transaction_enabled,
                        "resourceCount": len(self._settings.resources),
                    },
                )
            except BaseException:
                self._close_components()
                raise

    def probe(self) -> AdapterProbe:
        return self._require_probe().probe()

    def verify_physical(self, resources: tuple[PhysicalResource, ...]) -> PhysicalVerification:
        return self._require_probe().verify_physical(resources)

    def open_session(self, *, transactional: bool) -> AdapterSession:
        self._require_open()
        if transactional and self._transaction_bridge is None:
            raise KafkaTransactionAborted(
                "Binding does not advertise a Kafka transaction Capability",
                retryable=False,
            )
        return KafkaAdapterSession(self, transactional=transactional)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._close_components()
            self._telemetry.emit("meridian.kafka.runtime.closed", {"closed": True})

    def rotate_credentials(self, context: AdapterCreateContext) -> None:
        """Probe replacement clients before a drain-and-swap credential rotation."""

        replacement = KafkaAdapterRuntime(
            context,
            client_factory=self._client_factory,
            shared_telemetry=self._telemetry,
        )
        if context.binding != self._binding:
            raise KafkaConfigurationError("credential rotation cannot replace the Binding")
        replacement.open()
        try:
            replacement.probe()
        except BaseException:
            replacement.close()
            raise
        with self._lock:
            self._require_open()
            old_consumer = self._consumer_engine
            old_producer = self._producer_engine
            old_transaction = self._transaction_bridge
            self._binding = replacement._binding
            self._settings = replacement._settings
            self._admin = replacement._admin
            self._probe_engine = replacement._probe_engine
            self._producer_engine = replacement._producer_engine
            self._consumer_engine = replacement._consumer_engine
            self._transaction_bridge = replacement._transaction_bridge
            self._compiler = replacement._compiler
            replacement._admin = None
            replacement._probe_engine = None
            replacement._producer_engine = None
            replacement._consumer_engine = None
            replacement._transaction_bridge = None
            replacement._opened = False
            replacement._closed = True
        if old_consumer is not None:
            old_consumer.close()
        if old_transaction is not None:
            old_transaction.close()
        if old_producer is not None:
            old_producer.close()
        self._telemetry.emit("meridian.kafka.credentials.rotated", {"validated": True})

    def _require_open(self) -> None:
        if not self._opened or self._closed:
            raise RuntimeError("Kafka Adapter runtime is not open")

    def _require_probe(self) -> KafkaProbeEngine:
        self._require_open()
        selected = self._probe_engine
        if selected is None:
            raise RuntimeError("Kafka probe is unavailable")
        return selected

    def _close_components(self) -> None:
        consumer, transaction, producer = (
            self._consumer_engine,
            self._transaction_bridge,
            self._producer_engine,
        )
        self._consumer_engine = None
        self._transaction_bridge = None
        self._producer_engine = None
        self._probe_engine = None
        self._admin = None
        if consumer is not None:
            consumer.close()
        if transaction is not None:
            transaction.close()
        if producer is not None:
            producer.close()


class KafkaAdapterSession:
    def __init__(self, runtime: KafkaAdapterRuntime, *, transactional: bool) -> None:
        self._runtime = runtime
        self._transactional = transactional
        self._active = False
        self._closed = False
        self._touched_groups: set[tuple[ResourceRef, ResourceRef]] = set()

    def begin(self) -> None:
        self._ensure_open()
        if not self._transactional:
            raise KafkaTransactionAborted("non-transactional session cannot begin a transaction")
        if self._active:
            raise KafkaTransactionAborted("Kafka transaction is already active")
        bridge = self._bridge()
        bridge.begin()
        self._active = True

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        self._ensure_open()
        if self._transactional and not self._active:
            raise KafkaTransactionAborted("transactional session must begin before execution")
        if request.context.tenant is None:
            raise StreamingPolicyDenied(
                "Kafka streaming Operations require a tenant scope",
                requirement="operation.tenant",
            )
        operation_contract = request.operation.operation_contract
        self._runtime.telemetry.emit(
            "meridian.kafka.operation.started",
            {"operationContract": operation_contract, "attempt": request.attempt},
        )
        try:
            command = self._runtime._compiler.compile(request.operation)
            data = self._dispatch(command, request)
        except MeridianError:
            raise
        except BaseException as exc:
            raise normalize_kafka_error(exc, operation_contract=operation_contract) from exc
        self._runtime.telemetry.emit(
            "meridian.kafka.operation.completed",
            {"operationContract": operation_contract, "transactional": self._active},
        )
        probe = self._runtime._require_probe().probe().manifest
        return ExecutionResult(
            data=data,
            result_bytes=len(canonical_json_bytes(data)),
            provenance={
                "adapterId": ADAPTER_ID,
                "capabilityFingerprint": probe.fingerprint,
                "engineProfile": self._runtime.settings.engine_profile,
                "engineVersion": self._runtime.settings.engine_version,
                "operationFingerprint": request.operation.request_fingerprint,
            },
        )

    def commit(self) -> None:
        self._ensure_open()
        if not self._active:
            raise KafkaTransactionAborted("Kafka transaction is not active")
        try:
            self._bridge().commit()
        except BaseException:
            self._recover_touched_groups()
            raise
        finally:
            self._active = False
            self._touched_groups.clear()

    def rollback(self) -> None:
        self._ensure_open()
        if not self._active:
            return
        try:
            self._bridge().abort()
        finally:
            self._active = False
            self._recover_touched_groups()
            self._touched_groups.clear()

    def close(self) -> None:
        if self._closed:
            return
        if self._active:
            self.rollback()
        self._closed = True

    def _dispatch(self, command: CompiledCommand, request: ExecutionRequest) -> JsonValue:
        producer = self._producer()
        consumer = self._consumer()
        bridge = self._bridge() if self._active else None
        if isinstance(command, PublishCommand):
            receipts = producer.publish(
                command, producer=None if bridge is None else bridge.producer
            )
            values = [cast(JsonValue, item.to_dict()) for item in receipts]
            return values if command.batch else values[0]
        if isinstance(command, SubscribeCommand):
            return {
                "validated": True,
                "subscription": str(command.subscription.ref),
                "stream": str(command.stream.ref),
                "physicalFingerprint": sha256_fingerprint(
                    {
                        "stream": command.stream.resource_fingerprint,
                        "subscription": command.subscription.resource_fingerprint,
                    }
                ),
                "lifecycleOwner": "iac",
            }
        if isinstance(command, PollCommand):
            return [cast(JsonValue, item.to_dict()) for item in consumer.poll(command)]
        if isinstance(command, NegativeAcknowledgeCommand):
            if bridge is not None:
                raise KafkaTransactionAborted(
                    "negative acknowledgement is not valid inside a Core transaction"
                )
            return cast(JsonValue, dict(consumer.negative_acknowledge(command)))
        if isinstance(command, AcknowledgeCommand):
            if bridge is not None:
                self._touched_groups.add((command.subscription.ref, command.consumer_group.ref))
            return cast(JsonValue, dict(consumer.acknowledge(command, transaction=bridge)))
        if isinstance(command, ReadRangeCommand):
            return cast(JsonValue, consumer.read_range(command).to_dict())
        if isinstance(command, GroupPositionCommand):
            if bridge is not None:
                raise KafkaTransactionAborted(
                    "group-position transition is not valid inside a transaction"
                )
            return cast(JsonValue, dict(consumer.group_position(command)))
        if isinstance(command, ValidateSchemaCommand):
            if command.allow_breaking:
                raise MigrationRequired(
                    "breaking Schema publication requires an IaC migration job",
                    requirement="schema.lifecycle-authority",
                )
            if (
                command.expected_registry_revision is not None
                and command.expected_registry_revision != request.registry_revision
            ):
                raise MigrationRequired(
                    "Schema registry revision differs from the runtime snapshot",
                    requirement="schema.registry-revision",
                )
            matching = [
                item
                for item in self._runtime.settings.resources.values()
                if isinstance(item, PhysicalStream)
                and item.schema_ref.namespace == command.namespace
                and item.schema_ref.name == command.name
                and item.schema_ref.version == command.version
            ]
            if not matching:
                raise MigrationRequired(
                    "Schema is not present in the pinned Kafka Binding",
                    requirement="schema.migration",
                )
            return {
                "validated": True,
                "registryRevision": request.registry_revision,
                "bindingCount": len(matching),
                "lifecycleOwner": "iac",
            }
        if isinstance(command, ValidateResourceCommand):
            return {
                "validated": True,
                "resource": str(command.resource.ref),
                "resourceType": command.resource_type,
                "resourceFingerprint": command.resource.resource_fingerprint,
                "lifecycleOwner": "iac",
            }
        if isinstance(command, TransactionalConsumePublishCommand):
            return self._execute_transactional(command)
        raise AssertionError("compiler returned an unknown command")

    def _execute_transactional(self, command: TransactionalConsumePublishCommand) -> JsonValue:
        bridge = self._bridge()
        owns_transaction = not self._active
        if owns_transaction:
            bridge.begin()
        receipts: list[JsonValue] = []
        ack_command = AcknowledgeCommand(
            command.poll.subscription,
            command.poll.consumer_group,
            command.poll.stream,
            command.delivery,
        )
        self._touched_groups.add((command.poll.subscription.ref, command.poll.consumer_group.ref))
        try:
            for output in command.outputs:
                receipts.extend(
                    cast(JsonValue, item.to_dict())
                    for item in self._producer().publish(output, producer=bridge.producer)
                )
            acknowledgement = self._consumer().acknowledge(ack_command, transaction=bridge)
            if owns_transaction:
                bridge.commit()
                self._touched_groups.clear()
            return {
                "receipts": receipts,
                "acknowledgement": cast(JsonValue, dict(acknowledgement)),
                "transactional": True,
            }
        except BaseException:
            if owns_transaction and bridge.active:
                bridge.abort()
            self._recover_touched_groups()
            self._touched_groups.clear()
            raise

    def _producer(self) -> KafkaProducerEngine:
        selected = self._runtime._producer_engine
        if selected is None:
            raise RuntimeError("Kafka producer is unavailable")
        return selected

    def _consumer(self) -> KafkaConsumerEngine:
        selected = self._runtime._consumer_engine
        if selected is None:
            raise RuntimeError("Kafka consumer is unavailable")
        return selected

    def _bridge(self) -> KafkaTransactionBridge:
        selected = self._runtime._transaction_bridge
        if selected is None:
            raise KafkaTransactionAborted("Kafka transaction Capability is unavailable")
        return selected

    def _recover_touched_groups(self) -> None:
        consumer = self._consumer()
        for subscription, group in self._touched_groups:
            consumer.recover(subscription, group)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("Kafka Adapter session is closed")
        self._runtime._require_open()


__all__ = ["KafkaAdapterFactory", "KafkaAdapterRuntime", "KafkaAdapterSession"]

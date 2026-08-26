# SPDX-License-Identifier: Apache-2.0
"""Strict compiler from released Streaming Operations to private Kafka commands."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import cast

from meridian_storage import Operation, ResourceRef
from meridian_storage.streaming import (
    MAX_BATCH_SIZE,
    MAX_POLL_SIZE,
    MAX_RANGE_SIZE,
    MAX_WAIT_TIMEOUT_MS,
    Event,
    InvalidEvent,
    InvalidStreamingDefinition,
    StreamingCapabilityMismatch,
)

from .._constants import TRANSACTION_OPERATION_CONTRACT, TRANSACTION_OPERATION_VERSION
from ..canonical import as_object, bounded_string, closed_object
from ..config import (
    KafkaBindingSettings,
    PhysicalConsumerGroup,
    PhysicalStream,
    PhysicalSubscription,
)

_MAX_RETRY_AFTER_MS = 86_400_000


@dataclass(frozen=True, slots=True)
class PublishCommand:
    stream: PhysicalStream
    events: tuple[Event, ...]
    idempotency_key: str | None
    batch: bool


@dataclass(frozen=True, slots=True)
class SubscribeCommand:
    stream: PhysicalStream
    subscription: PhysicalSubscription


@dataclass(frozen=True, slots=True)
class PollCommand:
    subscription: PhysicalSubscription
    consumer_group: PhysicalConsumerGroup
    stream: PhysicalStream
    limit: int
    wait_timeout_ms: int


@dataclass(frozen=True, slots=True)
class AcknowledgeCommand:
    subscription: PhysicalSubscription
    consumer_group: PhysicalConsumerGroup
    stream: PhysicalStream
    delivery: str | Mapping[str, object]


@dataclass(frozen=True, slots=True)
class NegativeAcknowledgeCommand(AcknowledgeCommand):
    retry_after_ms: int
    classification: str | None


@dataclass(frozen=True, slots=True)
class ReadRangeCommand:
    stream: PhysicalStream
    start: str | Mapping[str, object] | None
    end: str | Mapping[str, object] | None
    cursor: str | Mapping[str, object] | None
    limit: int
    replay: bool = False


@dataclass(frozen=True, slots=True)
class GroupPositionCommand:
    subscription: PhysicalSubscription
    consumer_group: PhysicalConsumerGroup
    stream: PhysicalStream
    position: str | Mapping[str, object]
    expected_fingerprint: str


@dataclass(frozen=True, slots=True)
class ValidateSchemaCommand:
    namespace: str
    name: str
    version: str
    expected_registry_revision: int | None
    allow_breaking: bool


@dataclass(frozen=True, slots=True)
class ValidateResourceCommand:
    resource: PhysicalStream | PhysicalSubscription | PhysicalConsumerGroup
    resource_type: str


@dataclass(frozen=True, slots=True)
class TransactionalConsumePublishCommand:
    poll: PollCommand
    delivery: str | Mapping[str, object]
    outputs: tuple[PublishCommand, ...]


CompiledCommand = (
    PublishCommand
    | SubscribeCommand
    | PollCommand
    | AcknowledgeCommand
    | NegativeAcknowledgeCommand
    | ReadRangeCommand
    | GroupPositionCommand
    | ValidateSchemaCommand
    | ValidateResourceCommand
    | TransactionalConsumePublishCommand
)


class KafkaOperationCompiler:
    def __init__(self, settings: KafkaBindingSettings) -> None:
        self._settings = settings

    def compile(self, operation: Operation) -> CompiledCommand:
        if operation.catalog != "streaming":
            raise StreamingCapabilityMismatch(
                "Kafka Adapter accepts only streaming Catalog Operations",
                requirement="operation.catalog",
            )
        if operation.operation_version != "1.0.0":
            raise StreamingCapabilityMismatch(
                "Kafka Adapter supports Streaming Operation version 1.0.0 only",
                requirement="operation.version",
            )
        value = cast(Mapping[str, object], operation.input)
        contract = operation.operation_contract
        if contract == "meridian.streaming.publish":
            return self._publish(value, batch=False)
        if contract == "meridian.streaming.publish-batch":
            return self._publish(value, batch=True)
        if contract == "meridian.streaming.subscribe":
            return self._subscribe(value)
        if contract == "meridian.streaming.poll":
            return self._poll(value)
        if contract == "meridian.streaming.acknowledge":
            return self._acknowledge(value)
        if contract == "meridian.streaming.negative-acknowledge":
            return self._negative_acknowledge(value)
        if contract == "meridian.streaming.read-range":
            return self._read_range(value)
        if contract == "meridian.streaming.replay":
            return self._replay(value)
        if contract == "meridian.streaming.group-position":
            return self._group_position(value)
        if contract == "meridian.streaming.publish-schema":
            return self._validate_schema(value)
        if contract == "meridian.streaming.create-resource":
            return self._validate_resource(value)
        if contract == TRANSACTION_OPERATION_CONTRACT:
            if operation.operation_version != TRANSACTION_OPERATION_VERSION:
                raise StreamingCapabilityMismatch("transaction Operation version is unsupported")
            return self._transactional(value)
        raise StreamingCapabilityMismatch(
            f"Kafka Adapter does not implement {contract!r}",
            requirement="operation.contract",
        )

    def _publish(self, value: Mapping[str, object], *, batch: bool) -> PublishCommand:
        value = _closed(
            value,
            "publish-batch" if batch else "publish",
            required={"resource", "data"},
            optional={"idempotencyKey"},
        )
        stream = self._settings.stream(_ref(value["resource"], "resource"))
        raw_events = value["data"]
        if batch:
            if not isinstance(raw_events, Sequence) or isinstance(raw_events, str | bytes):
                raise InvalidStreamingDefinition("publish-batch Data must be an array")
            if not 1 <= len(raw_events) <= MAX_BATCH_SIZE:
                raise InvalidStreamingDefinition(
                    f"publish-batch requires between 1 and {MAX_BATCH_SIZE} Events"
                )
            selected = tuple(self._event(item, stream) for item in raw_events)
        else:
            selected = (self._event(raw_events, stream),)
        key = value.get("idempotencyKey")
        if key is not None:
            key = bounded_string(key, "idempotencyKey", 512)
        return PublishCommand(stream, selected, key, batch)

    @staticmethod
    def _event(value: object, stream: PhysicalStream) -> Event:
        if not isinstance(value, Mapping):
            raise InvalidEvent("Event Data must be an object")
        try:
            event = Event.from_mapping(cast(Mapping[str, object], value))
        except (TypeError, ValueError) as exc:
            if isinstance(exc, InvalidEvent):
                raise
            raise InvalidEvent("Event Data does not satisfy the released envelope") from exc
        if event.stream != stream.ref:
            raise InvalidEvent(
                "Event targets another Stream Resource",
                requirement="event.stream",
                logical_references=(str(stream.ref),),
            )
        if event.schema != stream.schema_ref:
            raise InvalidEvent(
                "Event does not use the active Schema reference",
                requirement="event.schema",
                logical_references=(str(stream.ref),),
            )
        return event

    def _subscribe(self, value: Mapping[str, object]) -> SubscribeCommand:
        value = _closed(
            value,
            "subscribe",
            required={"stream", "subscription", "options"},
        )
        stream = self._settings.stream(_ref(value["stream"], "stream"))
        subscription = self._settings.subscription(_ref(value["subscription"], "subscription"))
        if subscription.stream != stream.ref:
            raise InvalidStreamingDefinition("Subscription mapping belongs to another Stream")
        options = as_object(value["options"], "subscribe options")
        if options:
            raise InvalidStreamingDefinition(
                "V1 Kafka subscribe options must be empty and provisioned by IaC",
                requirement="subscription.lifecycle-authority",
            )
        return SubscribeCommand(stream, subscription)

    def _poll(self, value: Mapping[str, object]) -> PollCommand:
        value = _closed(
            value,
            "poll",
            required={"subscription", "consumerGroup", "limit", "waitTimeoutMs"},
        )
        subscription, group, stream = self._settings.group_stream(
            _ref(value["subscription"], "subscription"),
            _ref(value["consumerGroup"], "consumerGroup"),
        )
        return PollCommand(
            subscription,
            group,
            stream,
            _integer(value["limit"], "limit", minimum=1, maximum=MAX_POLL_SIZE),
            _integer(
                value["waitTimeoutMs"],
                "waitTimeoutMs",
                minimum=0,
                maximum=MAX_WAIT_TIMEOUT_MS,
            ),
        )

    def _acknowledge(self, value: Mapping[str, object]) -> AcknowledgeCommand:
        value = _closed(
            value,
            "acknowledge",
            required={"subscription", "consumerGroup", "delivery"},
        )
        subscription, group, stream = self._settings.group_stream(
            _ref(value["subscription"], "subscription"),
            _ref(value["consumerGroup"], "consumerGroup"),
        )
        return AcknowledgeCommand(
            subscription, group, stream, _opaque(value["delivery"], "delivery")
        )

    def _negative_acknowledge(self, value: Mapping[str, object]) -> NegativeAcknowledgeCommand:
        value = _closed(
            value,
            "negative-acknowledge",
            required={"subscription", "consumerGroup", "delivery"},
            optional={"retryAfterMs", "classification"},
        )
        subscription, group, stream = self._settings.group_stream(
            _ref(value["subscription"], "subscription"),
            _ref(value["consumerGroup"], "consumerGroup"),
        )
        classification = value.get("classification")
        if classification is not None:
            classification = bounded_string(classification, "classification", 256)
        return NegativeAcknowledgeCommand(
            subscription,
            group,
            stream,
            _opaque(value["delivery"], "delivery"),
            _integer(
                value.get("retryAfterMs", 0),
                "retryAfterMs",
                minimum=0,
                maximum=_MAX_RETRY_AFTER_MS,
            ),
            classification,
        )

    def _read_range(self, value: Mapping[str, object]) -> ReadRangeCommand:
        value = _closed(
            value,
            "read-range",
            required={"resource", "start", "end", "cursor", "limit"},
        )
        stream = self._settings.stream(_ref(value["resource"], "resource"))
        return ReadRangeCommand(
            stream,
            _optional_opaque(value["start"], "start"),
            _optional_opaque(value["end"], "end"),
            _optional_opaque(value["cursor"], "cursor"),
            _integer(value["limit"], "limit", minimum=1, maximum=MAX_RANGE_SIZE),
        )

    def _replay(self, value: Mapping[str, object]) -> ReadRangeCommand:
        value = _closed(
            value,
            "replay",
            required={
                "stream",
                "start",
                "end",
                "limit",
                "authorizationRef",
                "reason",
                "mutatesConsumerGroup",
            },
        )
        if value["mutatesConsumerGroup"] is not False:
            raise InvalidStreamingDefinition("replay cannot mutate ConsumerGroup position")
        stream = self._settings.stream(_ref(value["stream"], "stream"))
        return ReadRangeCommand(
            stream,
            _optional_opaque(value["start"], "start"),
            _optional_opaque(value.get("end"), "end"),
            None,
            _integer(value["limit"], "limit", minimum=1, maximum=MAX_RANGE_SIZE),
            replay=True,
        )

    def _group_position(self, value: Mapping[str, object]) -> GroupPositionCommand:
        value = _closed(
            value,
            "group-position",
            required={
                "subscription",
                "consumerGroup",
                "position",
                "expectedPositionFingerprint",
                "authorizationRef",
                "reason",
            },
        )
        subscription, group, stream = self._settings.group_stream(
            _ref(value["subscription"], "subscription"),
            _ref(value["consumerGroup"], "consumerGroup"),
        )
        return GroupPositionCommand(
            subscription,
            group,
            stream,
            _opaque(value["position"], "position"),
            _fingerprint(value["expectedPositionFingerprint"], "expectedPositionFingerprint"),
        )

    @staticmethod
    def _validate_schema(value: Mapping[str, object]) -> ValidateSchemaCommand:
        value = _closed(
            value,
            "publish-schema",
            required={"namespace", "name", "version", "definition", "allowBreaking"},
            optional={"expectedRegistryRevision"},
        )
        as_object(value["definition"], "definition")
        expected = value.get("expectedRegistryRevision")
        return ValidateSchemaCommand(
            bounded_string(value["namespace"], "namespace", 256),
            bounded_string(value["name"], "name", 256),
            bounded_string(value["version"], "version", 256),
            None if expected is None else _integer(expected, "expectedRegistryRevision", minimum=0),
            _boolean(value["allowBreaking"], "allowBreaking"),
        )

    def _validate_resource(self, value: Mapping[str, object]) -> ValidateResourceCommand:
        value = _closed(
            value,
            "create-resource",
            required={"namespace", "name", "resourceType", "schema", "options"},
        )
        as_object(value["options"], "options")
        ref = ResourceRef(
            "streaming",
            bounded_string(value["namespace"], "namespace", 256),
            bounded_string(value["name"], "name", 256),
        )
        selected = self._settings.resource(ref)
        expected_type = {
            PhysicalStream: "stream",
            PhysicalSubscription: "subscription",
            PhysicalConsumerGroup: "consumer-group",
        }[type(selected)]
        resource_type = bounded_string(value["resourceType"], "resourceType", 64)
        if resource_type != expected_type:
            raise InvalidStreamingDefinition("Resource kind disagrees with the Binding mapping")
        return ValidateResourceCommand(selected, resource_type)

    def _transactional(self, value: Mapping[str, object]) -> TransactionalConsumePublishCommand:
        if not self._settings.transaction_enabled:
            raise StreamingCapabilityMismatch(
                "Binding does not enable transactional consume-publish",
                requirement="transaction.binding",
            )
        value = _closed(
            value,
            "transactional-consume-publish",
            required={
                "subscription",
                "consumerGroup",
                "limit",
                "waitTimeoutMs",
                "delivery",
                "outputs",
            },
        )
        poll = self._poll(
            {
                "subscription": value["subscription"],
                "consumerGroup": value["consumerGroup"],
                "limit": value["limit"],
                "waitTimeoutMs": value["waitTimeoutMs"],
            }
        )
        delivery = _opaque(value["delivery"], "delivery")
        raw_outputs = value["outputs"]
        if not isinstance(raw_outputs, Sequence) or isinstance(raw_outputs, str | bytes):
            raise InvalidStreamingDefinition("transaction outputs must be an array")
        if not 1 <= len(raw_outputs) <= MAX_BATCH_SIZE:
            raise InvalidStreamingDefinition(
                f"transaction outputs requires between 1 and {MAX_BATCH_SIZE} entries"
            )
        outputs = tuple(
            self._publish(as_object(item, "transaction output"), batch=False)
            for item in raw_outputs
        )
        return TransactionalConsumePublishCommand(poll, delivery, outputs)


def _ref(value: object, field_name: str) -> ResourceRef:
    try:
        if not isinstance(value, str | Mapping):
            raise TypeError
        selected = ResourceRef.parse(value, catalog="streaming")
        if selected.catalog != "streaming":
            raise ValueError
        return selected
    except (TypeError, ValueError) as exc:
        raise InvalidStreamingDefinition(f"{field_name} is not a streaming Resource") from exc


def _opaque(value: object, field_name: str) -> str | Mapping[str, object]:
    if isinstance(value, str):
        return bounded_string(value, field_name, 4096)
    if isinstance(value, Mapping):
        return cast(Mapping[str, object], value)
    raise InvalidStreamingDefinition(f"{field_name} must be opaque string or mapping Data")


def _optional_opaque(value: object, field_name: str) -> str | Mapping[str, object] | None:
    return None if value is None else _opaque(value, field_name)


def _integer(
    value: object,
    field_name: str,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        upper = "" if maximum is None else f" and no larger than {maximum}"
        raise InvalidStreamingDefinition(
            f"{field_name} must be an integer no smaller than {minimum}{upper}"
        )
    return value


def _boolean(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise InvalidStreamingDefinition(f"{field_name} must be boolean")
    return value


def _fingerprint(value: object, field_name: str) -> str:
    selected = bounded_string(value, field_name, 71)
    if re.fullmatch(r"sha256:[0-9a-f]{64}", selected) is None:
        raise InvalidStreamingDefinition(f"{field_name} must be a SHA-256 fingerprint")
    return selected


def _closed(
    value: Mapping[str, object],
    operation_name: str,
    *,
    required: set[str],
    optional: set[str] | frozenset[str] = frozenset(),
) -> Mapping[str, object]:
    try:
        return closed_object(
            value,
            f"{operation_name} Operation input",
            required=required,
            optional=optional,
        )
    except (TypeError, ValueError) as exc:
        raise InvalidStreamingDefinition(
            f"{operation_name} Operation input contains unknown or missing fields",
            requirement="operation.input",
        ) from exc


__all__ = [
    "AcknowledgeCommand",
    "CompiledCommand",
    "GroupPositionCommand",
    "KafkaOperationCompiler",
    "NegativeAcknowledgeCommand",
    "PollCommand",
    "PublishCommand",
    "ReadRangeCommand",
    "SubscribeCommand",
    "TransactionalConsumePublishCommand",
    "ValidateResourceCommand",
    "ValidateSchemaCommand",
]

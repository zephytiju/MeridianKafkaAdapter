# SPDX-License-Identifier: Apache-2.0
"""Idempotent Event serialization and Kafka production."""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock

from confluent_kafka import KafkaError

from meridian_storage.streaming import Event, InvalidEvent, PublishReceipt

from ..canonical import canonical_json_bytes, sha256_fingerprint, utc_now
from ..clients import MessageLike, ProducerLike
from ..compiler import PublishCommand
from ..cursor import KafkaCursorCodec
from ..errors import KafkaOperationFailed, normalize_kafka_error
from ..telemetry import KafkaTelemetry

_MAX_IDEMPOTENCY_ENTRIES = 10_000
_RESERVED_HEADER_PREFIX = "meridian."


@dataclass(slots=True)
class _Delivery:
    error: KafkaError | None = None
    message: MessageLike | None = None


class KafkaProducerEngine:
    def __init__(
        self,
        producer: ProducerLike,
        cursor: KafkaCursorCodec,
        telemetry: KafkaTelemetry,
        *,
        timeout_ms: int,
    ) -> None:
        self._producer = producer
        self._cursor = cursor
        self._telemetry = telemetry
        self._timeout_ms = timeout_ms
        self._idempotency: OrderedDict[str, tuple[str, tuple[PublishReceipt, ...]]] = OrderedDict()
        self._lock = RLock()

    def publish(
        self,
        command: PublishCommand,
        *,
        producer: ProducerLike | None = None,
    ) -> tuple[PublishReceipt, ...]:
        selected = producer or self._producer
        cache_application_key = producer is None
        operation_fingerprint = sha256_fingerprint(
            {
                "stream": str(command.stream.ref),
                "events": [event.to_dict() for event in command.events],
            }
        )
        if cache_application_key and command.idempotency_key is not None:
            with self._lock:
                replay = self._idempotency.get(command.idempotency_key)
                if replay is not None:
                    if replay[0] != operation_fingerprint:
                        raise InvalidEvent(
                            "idempotency key was reused with different Event Data",
                            requirement="publish.idempotency-key",
                        )
                    self._idempotency.move_to_end(command.idempotency_key)
                    self._telemetry.emit(
                        "meridian.kafka.publish.idempotency-replay",
                        {"eventCount": len(replay[1])},
                    )
                    return replay[1]

        deliveries = [_Delivery() for _ in command.events]
        deadline = time.monotonic() + self._timeout_ms / 1000
        for index, event in enumerate(command.events):
            self._produce_one(selected, command, event, deliveries[index], deadline)
        remaining = max(0.0, deadline - time.monotonic())
        queued = selected.flush(remaining)
        if queued:
            raise KafkaOperationFailed(
                "Kafka producer did not deliver every Event before the deadline",
                retryable=True,
            )
        receipts: list[PublishReceipt] = []
        for delivery, event in zip(deliveries, command.events, strict=True):
            if delivery.error is not None:
                raise normalize_kafka_error(
                    delivery.error, operation_contract="meridian.streaming.publish"
                )
            if delivery.message is None:
                raise KafkaOperationFailed("Kafka delivery callback did not return metadata")
            message = delivery.message
            position = self._cursor.encode_position(
                command.stream.ref,
                command.stream.topic,
                message.partition(),
                message.offset(),
                logical_partition=event.logical_partition_key,
            )
            receipts.append(PublishReceipt(event.event_id, position, None, utc_now()))
        result = tuple(receipts)
        if cache_application_key and command.idempotency_key is not None:
            with self._lock:
                self._idempotency[command.idempotency_key] = (operation_fingerprint, result)
                self._idempotency.move_to_end(command.idempotency_key)
                while len(self._idempotency) > _MAX_IDEMPOTENCY_ENTRIES:
                    self._idempotency.popitem(last=False)
        self._telemetry.emit(
            "meridian.kafka.publish.completed",
            {
                "eventCount": len(result),
                "idempotent": command.idempotency_key is not None,
            },
        )
        return result

    def close(self) -> None:
        queued = self._producer.flush(self._timeout_ms / 1000)
        if queued:
            self._telemetry.emit("meridian.kafka.publish.close-timeout", {"queuedCount": queued})

    def _produce_one(
        self,
        producer: ProducerLike,
        command: PublishCommand,
        event: Event,
        delivery: _Delivery,
        deadline: float,
    ) -> None:
        for key in event.headers:
            if key.lower().startswith(_RESERVED_HEADER_PREFIX):
                raise InvalidEvent(
                    "Event headers cannot use the reserved meridian. prefix",
                    requirement="event.headers",
                )
        headers: list[tuple[str, bytes | None]] = [
            ("meridian.format", event.format_version.encode("utf-8")),
            ("meridian.event-id", event.event_id.encode("utf-8")),
            (
                "meridian.schema-fingerprint",
                command.stream.schema_fingerprint.encode("ascii"),
            ),
        ]
        if traceparent := event.trace_context.get("traceparent"):
            headers.append(("traceparent", traceparent.encode("utf-8")))

        def delivered(error: KafkaError | None, message: MessageLike) -> None:
            delivery.error = error
            delivery.message = message

        while True:
            try:
                producer.produce(
                    command.stream.topic,
                    value=canonical_json_bytes(event.to_dict()),
                    key=(event.logical_partition_key or event.event_id).encode("utf-8"),
                    on_delivery=delivered,
                    headers=headers,
                )
                producer.poll(0)
                return
            except BufferError as exc:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise KafkaOperationFailed(
                        "Kafka producer queue remained full until the deadline",
                        retryable=True,
                    ) from exc
                producer.poll(min(0.05, remaining))
            except BaseException as exc:
                raise normalize_kafka_error(
                    exc, operation_contract="meridian.streaming.publish"
                ) from exc


__all__ = ["KafkaProducerEngine"]

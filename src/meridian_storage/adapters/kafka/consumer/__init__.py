# SPDX-License-Identifier: Apache-2.0
"""Persistent group consumption, safe acknowledgement, and finite range reads."""

from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from threading import RLock
from typing import cast

from confluent_kafka import KafkaError, TopicPartition

from meridian_storage import MeridianError, ResourceRef
from meridian_storage.streaming import (
    CursorExpired,
    Delivery,
    DeliveryRejected,
    Event,
    GroupPositionConflict,
    InvalidCursor,
    InvalidEvent,
    RangePage,
    RebalanceConflict,
    RetentionBoundaryExceeded,
)

from ..canonical import JsonValue, utc_now
from ..clients import ConsumerLike, KafkaClientFactory, MessageLike
from ..compiler import (
    AcknowledgeCommand,
    GroupPositionCommand,
    NegativeAcknowledgeCommand,
    PollCommand,
    PublishCommand,
    ReadRangeCommand,
)
from ..config import KafkaBindingSettings, KafkaClientConfiguration, PhysicalStream
from ..cursor import MAX_CURSOR_PARTITIONS, KafkaCursorCodec
from ..errors import normalize_kafka_error
from ..producer import KafkaProducerEngine
from ..telemetry import KafkaTelemetry
from ..transactions import KafkaTransactionBridge


@dataclass(slots=True)
class _Inflight:
    event: Event
    attempt: int
    assignment_epoch: int
    expires_at_ms: int


@dataclass(slots=True)
class _PausedDelivery:
    offset: int
    resume_at: float


@dataclass(slots=True)
class _GroupState:
    command: PollCommand
    consumer: ConsumerLike
    assignment_epoch: int = 0
    assigned: set[int] = field(default_factory=set)
    inflight: dict[tuple[int, int], _Inflight] = field(default_factory=dict)
    attempts: dict[tuple[int, int], int] = field(default_factory=dict)
    acknowledged: dict[int, set[int]] = field(default_factory=lambda: defaultdict(set))
    safe_next: dict[int, int] = field(default_factory=dict)
    paused: dict[int, _PausedDelivery] = field(default_factory=dict)
    lock: RLock = field(default_factory=RLock)


class KafkaConsumerEngine:
    def __init__(
        self,
        settings: KafkaBindingSettings,
        client_configuration: KafkaClientConfiguration,
        factory: KafkaClientFactory,
        cursor: KafkaCursorCodec,
        producer: KafkaProducerEngine,
        telemetry: KafkaTelemetry,
    ) -> None:
        self._settings = settings
        self._client_configuration = client_configuration
        self._factory = factory
        self._cursor = cursor
        self._producer = producer
        self._telemetry = telemetry
        self._states: dict[tuple[ResourceRef, ResourceRef], _GroupState] = {}
        self._lock = RLock()
        self._closed = False

    def poll(self, command: PollCommand) -> tuple[Delivery, ...]:
        state = self._state(command)
        deadline = time.monotonic() + command.wait_timeout_ms / 1000
        deliveries: list[Delivery] = []
        with state.lock:
            while len(deliveries) < command.limit:
                self._expire_inflight(state)
                self._resume_due(state)
                remaining = deadline - time.monotonic()
                if command.wait_timeout_ms > 0 and remaining <= 0:
                    break
                timeout = 0.0 if command.wait_timeout_ms == 0 else remaining
                if state.paused:
                    nearest = min(item.resume_at for item in state.paused.values())
                    timeout = min(timeout, max(0.0, nearest - time.monotonic()))
                try:
                    message = state.consumer.poll(timeout)
                except BaseException as exc:
                    raise normalize_kafka_error(
                        exc, operation_contract="meridian.streaming.poll"
                    ) from exc
                if message is None:
                    if command.wait_timeout_ms == 0:
                        break
                    continue
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise normalize_kafka_error(error, operation_contract="meridian.streaming.poll")
                delivery = self._delivery(state, message)
                if delivery is not None:
                    deliveries.append(delivery)
        lag = self._lag(state)
        self._telemetry.emit(
            "meridian.kafka.poll.completed",
            {
                "deliveryCount": len(deliveries),
                "redeliveryCount": sum(item.redelivered for item in deliveries),
                "lagRecords": lag,
            },
        )
        return tuple(deliveries)

    def acknowledge(
        self,
        command: AcknowledgeCommand,
        *,
        transaction: KafkaTransactionBridge | None = None,
    ) -> Mapping[str, JsonValue]:
        state = self._existing_state(command.subscription.ref, command.consumer_group.ref)
        coordinates = self._cursor.decode_delivery(
            command.delivery,
            subscription=command.subscription.ref,
            consumer_group=command.consumer_group.ref,
            topic=command.stream.topic,
        )
        with state.lock:
            if coordinates.assignment_epoch != state.assignment_epoch:
                raise RebalanceConflict(
                    "delivery belongs to a revoked Kafka assignment",
                    requirement="delivery.group-generation",
                )
            key = (coordinates.partition, coordinates.offset)
            inflight = state.inflight.get(key)
            if (
                inflight is None
                or inflight.assignment_epoch != state.assignment_epoch
                or inflight.attempt != coordinates.attempt
            ):
                raise DeliveryRejected("delivery is unknown, revoked, or already acknowledged")
            state.acknowledged[coordinates.partition].add(coordinates.offset)
            original = state.safe_next.setdefault(coordinates.partition, coordinates.offset)
            safe = original
            acknowledged = state.acknowledged[coordinates.partition]
            while safe in acknowledged:
                acknowledged.remove(safe)
                safe += 1
            committed = safe > original
            if committed:
                if transaction is None:
                    try:
                        state.consumer.commit(
                            offsets=[
                                TopicPartition(command.stream.topic, coordinates.partition, safe)
                            ],
                            asynchronous=False,
                        )
                    except BaseException as exc:
                        raise normalize_kafka_error(
                            exc, operation_contract="meridian.streaming.acknowledge"
                        ) from exc
                else:
                    transaction.stage_offset(
                        group_key=str(command.consumer_group.ref),
                        consumer=state.consumer,
                        topic=command.stream.topic,
                        partition=coordinates.partition,
                        next_offset=safe,
                    )
                state.safe_next[coordinates.partition] = safe
            del state.inflight[key]
            state.attempts.pop(key, None)
            position = self._cursor.encode_position(
                command.stream.ref,
                command.stream.topic,
                coordinates.partition,
                state.safe_next[coordinates.partition],
            )
        self._telemetry.emit(
            "meridian.kafka.acknowledge.completed",
            {"committed": committed, "transactional": transaction is not None},
        )
        return {
            "acknowledged": True,
            "committed": committed,
            "safePosition": cast(JsonValue, position.to_dict()),
        }

    def negative_acknowledge(self, command: NegativeAcknowledgeCommand) -> Mapping[str, JsonValue]:
        state = self._existing_state(command.subscription.ref, command.consumer_group.ref)
        coordinates = self._cursor.decode_delivery(
            command.delivery,
            subscription=command.subscription.ref,
            consumer_group=command.consumer_group.ref,
            topic=command.stream.topic,
        )
        with state.lock:
            if coordinates.assignment_epoch != state.assignment_epoch:
                raise RebalanceConflict("delivery belongs to a revoked Kafka assignment")
            key = (coordinates.partition, coordinates.offset)
            inflight = state.inflight.get(key)
            if inflight is None or inflight.attempt != coordinates.attempt:
                raise DeliveryRejected("delivery is unknown, revoked, or already resolved")
            if (
                coordinates.attempt >= command.subscription.max_delivery_attempts
                and command.subscription.dead_letter_stream is not None
            ):
                target = self._settings.stream(command.subscription.dead_letter_stream)
                event = self._dead_letter_event(
                    command, inflight.event, target, coordinates.attempt
                )
                # Source remains unacknowledged if this publish raises.
                receipt = self._producer.publish(
                    PublishCommand(
                        target,
                        (event,),
                        f"dead-letter-{inflight.event.event_id}-{coordinates.attempt}",
                        False,
                    )
                )[0]
                ack = self.acknowledge(
                    AcknowledgeCommand(
                        command.subscription,
                        command.consumer_group,
                        command.stream,
                        command.delivery,
                    )
                )
                self._telemetry.emit(
                    "meridian.kafka.dead-letter.completed",
                    {"attempt": coordinates.attempt, "committed": ack["committed"]},
                )
                return {
                    "redeliveryScheduled": False,
                    "deadLettered": True,
                    "targetEventId": receipt.event_id,
                    "sourceCommitted": ack["committed"],
                }
            del state.inflight[key]
            partition = TopicPartition(
                command.stream.topic, coordinates.partition, coordinates.offset
            )
            try:
                state.consumer.seek(partition)
                if command.retry_after_ms > 0:
                    state.consumer.pause([partition])
                    state.paused[coordinates.partition] = _PausedDelivery(
                        coordinates.offset,
                        time.monotonic() + command.retry_after_ms / 1000,
                    )
            except BaseException as exc:
                raise normalize_kafka_error(
                    exc, operation_contract="meridian.streaming.negative-acknowledge"
                ) from exc
        self._telemetry.emit(
            "meridian.kafka.redelivery.scheduled",
            {"attempt": coordinates.attempt + 1, "retryAfterMs": command.retry_after_ms},
        )
        return {
            "redeliveryScheduled": True,
            "deadLettered": False,
            "nextAttempt": coordinates.attempt + 1,
            "retryAfterMs": command.retry_after_ms,
        }

    def recover(self, subscription: ResourceRef, consumer_group: ResourceRef) -> None:
        key = (subscription, consumer_group)
        with self._lock:
            state = self._states.pop(key, None)
        if state is not None:
            with state.lock:
                state.consumer.close()
            self._telemetry.emit("meridian.kafka.consumer.recovered", {"rejoined": True})

    def read_range(self, command: ReadRangeCommand) -> RangePage:
        reader = KafkaRangeReader(
            self._settings,
            self._client_configuration,
            self._factory,
            self._cursor,
            self._telemetry,
        )
        return reader.read(command)

    def group_position(self, command: GroupPositionCommand) -> Mapping[str, JsonValue]:
        state = self._state(
            PollCommand(
                command.subscription,
                command.consumer_group,
                command.stream,
                1,
                0,
            )
        )
        with state.lock:
            partitions = _partition_ids(
                state.consumer.list_topics(command.stream.topic, 5), command.stream.topic
            )
            before_positions = self._committed_positions(state.consumer, command.stream, partitions)
            before = self._cursor.group_position_fingerprint(command.stream.topic, before_positions)
            if before != command.expected_fingerprint:
                raise GroupPositionConflict("ConsumerGroup position compare-and-set failed")
            target = _decode_positions(
                self._cursor,
                command.position,
                command.stream,
            )
            if set(target) != set(partitions):
                raise InvalidCursor("group-position requires one coordinate for every partition")
            self._validate_bounds(state.consumer, command.stream, target)
            try:
                state.consumer.commit(
                    offsets=[
                        TopicPartition(command.stream.topic, partition, offset)
                        for partition, offset in sorted(target.items())
                    ],
                    asynchronous=False,
                )
            except BaseException as exc:
                raise normalize_kafka_error(
                    exc, operation_contract="meridian.streaming.group-position"
                ) from exc
            state.assignment_epoch += 1
            state.inflight.clear()
            state.acknowledged.clear()
            state.safe_next = dict(target)
            after = self._cursor.group_position_fingerprint(command.stream.topic, target)
        self._telemetry.emit(
            "meridian.kafka.group-position.completed", {"partitionCount": len(target)}
        )
        return {"previousFingerprint": before, "newFingerprint": after}

    def group_position_fingerprint(
        self,
        subscription: ResourceRef,
        consumer_group: ResourceRef,
    ) -> str:
        subscription_config, group, stream = self._settings.group_stream(
            subscription, consumer_group
        )
        state = self._state(PollCommand(subscription_config, group, stream, 1, 0))
        with state.lock:
            partitions = _partition_ids(state.consumer.list_topics(stream.topic, 5), stream.topic)
            positions = self._committed_positions(state.consumer, stream, partitions)
            return self._cursor.group_position_fingerprint(stream.topic, positions)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            states = tuple(self._states.values())
            self._states.clear()
        for state in states:
            with state.lock:
                state.consumer.close()

    def _state(self, command: PollCommand) -> _GroupState:
        key = (command.subscription.ref, command.consumer_group.ref)
        with self._lock:
            if self._closed:
                raise RuntimeError("Kafka consumer engine is closed")
            state = self._states.get(key)
            if state is not None:
                return state
            consumer = self._factory.consumer(
                self._client_configuration.consumer(command.consumer_group.group_id)
            )
            state = _GroupState(command, consumer)

            def assigned(selected_consumer: ConsumerLike, partitions: list[TopicPartition]) -> None:
                with state.lock:
                    state.assignment_epoch += 1
                    state.assigned = {item.partition for item in partitions}
                    selected_consumer.assign(partitions)
                    self._telemetry.emit(
                        "meridian.kafka.rebalance.assigned",
                        {
                            "partitionCount": len(partitions),
                            "assignmentEpoch": state.assignment_epoch,
                        },
                    )

            def revoked(selected_consumer: ConsumerLike, partitions: list[TopicPartition]) -> None:
                with state.lock:
                    revoked_partitions = {item.partition for item in partitions}
                    state.assignment_epoch += 1
                    state.assigned -= revoked_partitions
                    state.inflight = {
                        key: item
                        for key, item in state.inflight.items()
                        if key[0] not in revoked_partitions
                    }
                    state.attempts = {
                        key: item
                        for key, item in state.attempts.items()
                        if key[0] not in revoked_partitions
                    }
                    for partition in revoked_partitions:
                        state.acknowledged.pop(partition, None)
                        state.paused.pop(partition, None)
                    selected_consumer.unassign()
                    self._telemetry.emit(
                        "meridian.kafka.rebalance.revoked",
                        {
                            "partitionCount": len(partitions),
                            "assignmentEpoch": state.assignment_epoch,
                        },
                    )

            consumer.subscribe([command.stream.topic], on_assign=assigned, on_revoke=revoked)
            self._states[key] = state
            return state

    def _existing_state(
        self, subscription: ResourceRef, consumer_group: ResourceRef
    ) -> _GroupState:
        with self._lock:
            state = self._states.get((subscription, consumer_group))
        if state is None:
            raise DeliveryRejected("delivery has no active ConsumerGroup session")
        return state

    def _delivery(self, state: _GroupState, message: MessageLike) -> Delivery | None:
        stream = state.command.stream
        if message.topic() != stream.topic:
            raise InvalidEvent("Kafka returned an Event from another physical mapping")
        key = (message.partition(), message.offset())
        paused = state.paused.get(message.partition())
        if paused is not None and paused.offset == message.offset():
            if paused.resume_at > time.monotonic():
                state.consumer.seek(
                    TopicPartition(stream.topic, message.partition(), message.offset())
                )
                return None
            del state.paused[message.partition()]
        event = _decode_event(message, stream)
        attempt = state.attempts.get(key, 0) + 1
        state.attempts[key] = attempt
        state.safe_next.setdefault(message.partition(), message.offset())
        expires_at_ms = (
            time.time_ns() // 1_000_000 + state.command.subscription.acknowledgement_timeout_ms
        )
        token = self._cursor.encode_delivery(
            state.command.subscription.ref,
            state.command.consumer_group.ref,
            stream.topic,
            message.partition(),
            message.offset(),
            state.assignment_epoch,
            attempt,
            expires_at_ms,
        )
        position = self._cursor.encode_position(
            stream.ref,
            stream.topic,
            message.partition(),
            message.offset(),
            logical_partition=event.logical_partition_key,
        )
        state.inflight[key] = _Inflight(event, attempt, state.assignment_epoch, expires_at_ms)
        return Delivery(
            event,
            token,
            position,
            attempt,
            attempt > 1,
            utc_now(),
            token.expires_at,
        )

    def _resume_due(self, state: _GroupState) -> None:
        now = time.monotonic()
        for partition, paused in tuple(state.paused.items()):
            if paused.resume_at > now:
                continue
            selected = TopicPartition(state.command.stream.topic, partition, paused.offset)
            state.consumer.seek(selected)
            state.consumer.resume([selected])
            del state.paused[partition]

    def _expire_inflight(self, state: _GroupState) -> None:
        now_ms = time.time_ns() // 1_000_000
        expired = [key for key, item in state.inflight.items() if item.expires_at_ms <= now_ms]
        if not expired:
            return
        rewind: dict[int, int] = {}
        for partition, offset in expired:
            rewind[partition] = min(offset, rewind.get(partition, offset))
        for partition, offset in rewind.items():
            state.consumer.seek(TopicPartition(state.command.stream.topic, partition, offset))
            state.inflight = {
                key: item
                for key, item in state.inflight.items()
                if key[0] != partition or key[1] < offset
            }
            state.acknowledged[partition] = {
                selected for selected in state.acknowledged[partition] if selected < offset
            }
        self._telemetry.emit(
            "meridian.kafka.delivery.expired",
            {"deliveryCount": len(expired), "partitionCount": len(rewind)},
        )

    def _lag(self, state: _GroupState) -> int:
        lag = 0
        with state.lock:
            for partition in state.assigned:
                try:
                    _, high = state.consumer.get_watermark_offsets(
                        TopicPartition(state.command.stream.topic, partition),
                        timeout=1,
                        cached=True,
                    )
                except BaseException:
                    continue
                lag += max(0, high - state.safe_next.get(partition, high))
        return lag

    @staticmethod
    def _dead_letter_event(
        command: NegativeAcknowledgeCommand,
        original: Event,
        target: PhysicalStream,
        attempt: int,
    ) -> Event:
        classification = command.classification or "negative-acknowledgement"
        return Event.from_mapping(
            {
                "formatVersion": original.format_version,
                "eventId": f"dlq-{original.event_id}-{attempt}",
                "stream": target.ref.to_dict(),
                "schema": target.schema_ref.to_dict(),
                "data": {
                    "originalEvent": original.to_dict(),
                    "sourceStream": str(original.stream),
                    "deliveryAttempt": attempt,
                    "failureClassification": classification,
                    "failingConsumer": str(command.consumer_group.ref),
                },
                "occurredAt": original.occurred_at,
                "producedAt": utc_now().isoformat(),
                "logicalPartitionKey": original.logical_partition_key,
                "headers": {},
                "traceContext": dict(original.trace_context),
                "extensions": {"deadLetter": True},
            }
        )

    @staticmethod
    def _committed_positions(
        consumer: ConsumerLike, stream: PhysicalStream, partitions: tuple[int, ...]
    ) -> dict[int, int]:
        requested = [TopicPartition(stream.topic, partition) for partition in partitions]
        try:
            committed = consumer.committed(requested, timeout=5)
        except BaseException as exc:
            raise normalize_kafka_error(
                exc, operation_contract="meridian.streaming.group-position"
            ) from exc
        result: dict[int, int] = {}
        for item in committed:
            if item.offset >= 0:
                result[item.partition] = item.offset
            else:
                low, _ = consumer.get_watermark_offsets(
                    TopicPartition(stream.topic, item.partition), timeout=5
                )
                result[item.partition] = low
        return result

    @staticmethod
    def _validate_bounds(
        consumer: ConsumerLike, stream: PhysicalStream, positions: Mapping[int, int]
    ) -> None:
        for partition, offset in positions.items():
            low, high = consumer.get_watermark_offsets(
                TopicPartition(stream.topic, partition), timeout=5
            )
            if offset < low:
                raise CursorExpired(requirement="cursor.retained-range")
            if offset > high:
                raise RetentionBoundaryExceeded(
                    "group position is beyond the retained high watermark"
                )


class KafkaRangeReader:
    def __init__(
        self,
        settings: KafkaBindingSettings,
        client_configuration: KafkaClientConfiguration,
        factory: KafkaClientFactory,
        cursor: KafkaCursorCodec,
        telemetry: KafkaTelemetry,
    ) -> None:
        self._settings = settings
        self._client_configuration = client_configuration
        self._factory = factory
        self._cursor = cursor
        self._telemetry = telemetry

    def read(self, command: ReadRangeCommand) -> RangePage:
        group = f"{self._settings.client_id}.range.{uuid.uuid4().hex}"
        consumer = self._factory.consumer(
            self._client_configuration.consumer(group, range_reader=True)
        )
        try:
            metadata = consumer.list_topics(command.stream.topic, 5)
            partitions = _partition_ids(metadata, command.stream.topic)
            if len(partitions) > MAX_CURSOR_PARTITIONS:
                raise RetentionBoundaryExceeded(
                    f"finite range supports at most {MAX_CURSOR_PARTITIONS} partitions"
                )
            lows: dict[int, int] = {}
            highs: dict[int, int] = {}
            for partition in partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(command.stream.topic, partition), timeout=5
                )
                lows[partition], highs[partition] = low, high
            source = command.cursor if command.cursor is not None else command.start
            positions = (
                dict(lows)
                if source is None
                else _decode_positions(self._cursor, source, command.stream)
            )
            ends = (
                dict(highs)
                if command.end is None
                else _decode_positions(self._cursor, command.end, command.stream)
            )
            for partition in partitions:
                positions.setdefault(partition, lows[partition])
                ends.setdefault(partition, highs[partition])
                if positions[partition] < lows[partition]:
                    raise CursorExpired(requirement="cursor.retained-range")
                if positions[partition] > highs[partition] or ends[partition] > highs[partition]:
                    raise RetentionBoundaryExceeded(
                        "range bound is beyond the retained high watermark"
                    )
                if ends[partition] < positions[partition]:
                    raise RetentionBoundaryExceeded("range end precedes its start")
            if set(positions) != set(partitions) or set(ends) != set(partitions):
                raise InvalidCursor("range coordinates contain an unknown partition")
            consumer.assign(
                [
                    TopicPartition(command.stream.topic, partition, positions[partition])
                    for partition in partitions
                ]
            )
            events: list[Event] = []
            deadline = time.monotonic() + self._settings.operation_timeout_ms / 1000
            while len(events) < command.limit and any(
                positions[item] < ends[item] for item in partitions
            ):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                message = consumer.poll(min(0.25, remaining))
                if message is None:
                    continue
                error = message.error()
                if error is not None:
                    if error.code() == KafkaError._PARTITION_EOF:
                        positions[message.partition()] = max(
                            positions[message.partition()], message.offset()
                        )
                        continue
                    raise normalize_kafka_error(
                        error,
                        operation_contract=(
                            "meridian.streaming.replay"
                            if command.replay
                            else "meridian.streaming.read-range"
                        ),
                    )
                partition, offset = message.partition(), message.offset()
                positions[partition] = max(positions[partition], offset + 1)
                if offset >= ends[partition]:
                    continue
                events.append(_decode_event(message, command.stream))
            truncated = any(positions[item] < ends[item] for item in partitions)
            cursor = self._cursor.encode_cursor(command.stream.ref, command.stream.topic, positions)
            result = RangePage(tuple(events), cursor, truncated)
            self._telemetry.emit(
                "meridian.kafka.range.completed",
                {
                    "eventCount": len(events),
                    "partitionCount": len(partitions),
                    "truncated": truncated,
                    "replay": command.replay,
                },
            )
            return result
        except MeridianError:
            raise
        except BaseException as exc:
            raise normalize_kafka_error(
                exc,
                operation_contract=(
                    "meridian.streaming.replay"
                    if command.replay
                    else "meridian.streaming.read-range"
                ),
            ) from exc
        finally:
            consumer.close()


def _decode_event(message: MessageLike, stream: PhysicalStream) -> Event:
    raw = message.value()
    if raw is None or len(raw) > 16 * 1024 * 1024:
        raise InvalidEvent("Kafka Event value is absent or exceeds the adapter bound")
    try:
        decoded = json.loads(raw)
        if not isinstance(decoded, Mapping):
            raise TypeError
        event = Event.from_mapping(cast(Mapping[str, object], decoded))
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise InvalidEvent("Kafka Event value does not satisfy the released envelope") from exc
    if event.stream != stream.ref:
        raise InvalidEvent("Kafka Event targets another logical Stream")
    fingerprint = _header(message, "meridian.schema-fingerprint")
    if fingerprint is None or fingerprint.decode("ascii", errors="ignore") not in set(
        stream.compatible_schema_fingerprints
    ):
        raise InvalidEvent(
            "Kafka Event Schema fingerprint is absent or incompatible",
            requirement="event.schema-fingerprint",
        )
    return event


def _header(message: MessageLike, name: str) -> bytes | None:
    for key, value in message.headers() or []:
        if key == name:
            return value
    return None


def _partition_ids(metadata: object, topic: str) -> tuple[int, ...]:
    topics = getattr(metadata, "topics", None)
    if not isinstance(topics, Mapping) or topic not in topics:
        raise RetentionBoundaryExceeded("physical Stream topic metadata is unavailable")
    topic_metadata = topics[topic]
    error = getattr(topic_metadata, "error", None)
    if error is not None:
        raise normalize_kafka_error(error, operation_contract="meridian.streaming.read-range")
    partitions = getattr(topic_metadata, "partitions", None)
    if not isinstance(partitions, Mapping) or not partitions:
        raise RetentionBoundaryExceeded("physical Stream has no readable partitions")
    result = tuple(sorted(int(value) for value in partitions))
    if any(value < 0 for value in result):
        raise RetentionBoundaryExceeded("physical Stream partition metadata is invalid")
    return result


def _decode_positions(
    cursor: KafkaCursorCodec,
    value: str | Mapping[str, object],
    stream: PhysicalStream,
) -> dict[int, int]:
    try:
        return dict(cursor.decode_cursor(value, resource=stream.ref, topic=stream.topic).positions)
    except InvalidCursor as cursor_error:
        try:
            position = cursor.decode_position(value, resource=stream.ref, topic=stream.topic)
        except InvalidCursor:
            raise cursor_error from None
        return {position.partition: position.offset}


__all__ = ["KafkaConsumerEngine", "KafkaRangeReader"]

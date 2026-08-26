# SPDX-License-Identifier: Apache-2.0
"""Single-Binding Kafka transaction bridge for produced records and group offsets."""

from __future__ import annotations

from contextlib import suppress
from threading import Lock

from confluent_kafka import TopicPartition

from ..clients import ConsumerLike, ProducerLike
from ..errors import KafkaTransactionAborted, normalize_kafka_error
from ..telemetry import KafkaTelemetry


class KafkaTransactionBridge:
    def __init__(
        self,
        producer: ProducerLike,
        telemetry: KafkaTelemetry,
        *,
        timeout_ms: int,
    ) -> None:
        self._producer = producer
        self._telemetry = telemetry
        self._timeout = timeout_ms / 1000
        self._lease = Lock()
        self._active = False
        self._offsets: dict[tuple[str, int], int] = {}
        self._group_metadata: object | None = None
        self._group_key: str | None = None

    @property
    def producer(self) -> ProducerLike:
        if not self._active:
            raise RuntimeError("Kafka transaction is not active")
        return self._producer

    @property
    def active(self) -> bool:
        return self._active

    def open(self) -> None:
        try:
            self._producer.init_transactions(self._timeout)
        except BaseException as exc:
            raise normalize_kafka_error(
                exc,
                operation_contract="meridian.streaming.transactional-consume-publish",
            ) from exc
        self._telemetry.emit("meridian.kafka.transaction.initialized", {"ready": True})

    def begin(self) -> None:
        if not self._lease.acquire(timeout=self._timeout):
            raise KafkaTransactionAborted(
                "Kafka transactional producer lease timed out", retryable=True
            )
        try:
            self._producer.begin_transaction()
            self._active = True
            self._offsets.clear()
            self._group_metadata = None
            self._group_key = None
            self._telemetry.emit("meridian.kafka.transaction.began", {"active": True})
        except BaseException as exc:
            self._lease.release()
            raise normalize_kafka_error(
                exc,
                operation_contract="meridian.streaming.transactional-consume-publish",
            ) from exc

    def stage_offset(
        self,
        *,
        group_key: str,
        consumer: ConsumerLike,
        topic: str,
        partition: int,
        next_offset: int,
    ) -> None:
        if not self._active:
            raise RuntimeError("cannot stage an offset outside a Kafka transaction")
        if self._group_key is not None and self._group_key != group_key:
            raise KafkaTransactionAborted(
                "transactional consume-publish cannot cross ConsumerGroups",
                retryable=False,
            )
        self._group_key = group_key
        self._group_metadata = consumer.consumer_group_metadata()
        coordinate = (topic, partition)
        self._offsets[coordinate] = max(next_offset, self._offsets.get(coordinate, 0))

    def commit(self) -> None:
        if not self._active:
            raise RuntimeError("Kafka transaction is not active")
        try:
            if self._offsets:
                if self._group_metadata is None:
                    raise KafkaTransactionAborted("transaction offsets have no group metadata")
                positions = [
                    TopicPartition(topic, partition, offset)
                    for (topic, partition), offset in sorted(self._offsets.items())
                ]
                self._producer.send_offsets_to_transaction(
                    positions, self._group_metadata, self._timeout
                )
            self._producer.commit_transaction(self._timeout)
            self._telemetry.emit(
                "meridian.kafka.transaction.committed",
                {"offsetCount": len(self._offsets)},
            )
        except BaseException as exc:
            with suppress(BaseException):
                self._producer.abort_transaction(self._timeout)
            raise normalize_kafka_error(
                exc,
                operation_contract="meridian.streaming.transactional-consume-publish",
            ) from exc
        finally:
            self._finish()

    def abort(self) -> None:
        if not self._active:
            return
        try:
            self._producer.abort_transaction(self._timeout)
            self._telemetry.emit(
                "meridian.kafka.transaction.aborted", {"offsetCount": len(self._offsets)}
            )
        except BaseException as exc:
            raise normalize_kafka_error(
                exc,
                operation_contract="meridian.streaming.transactional-consume-publish",
            ) from exc
        finally:
            self._finish()

    def close(self) -> None:
        if self._active:
            self.abort()
        self._producer.flush(self._timeout)

    def _finish(self) -> None:
        self._active = False
        self._offsets.clear()
        self._group_metadata = None
        self._group_key = None
        if self._lease.locked():
            self._lease.release()


__all__ = ["KafkaTransactionBridge"]

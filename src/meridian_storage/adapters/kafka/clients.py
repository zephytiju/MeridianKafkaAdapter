# SPDX-License-Identifier: Apache-2.0
"""Narrow librdkafka client protocols and the production client factory."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Protocol, cast

from confluent_kafka import Consumer, KafkaError, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource  # type: ignore[attr-defined]

if TYPE_CHECKING:
    from .config import KafkaBindingSettings

type KafkaClientConfigValue = str | int | float
type KafkaClientConfig = Mapping[str, KafkaClientConfigValue]


class MessageLike(Protocol):
    def error(self) -> KafkaError | None: ...

    def key(self) -> bytes | None: ...

    def value(self) -> bytes | None: ...

    def headers(self) -> list[tuple[str, bytes | None]] | None: ...

    def topic(self) -> str: ...

    def partition(self) -> int: ...

    def offset(self) -> int: ...


type DeliveryCallback = Callable[[KafkaError | None, MessageLike], None]


class ProducerLike(Protocol):
    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int = -1,
        on_delivery: DeliveryCallback | None = None,
        headers: list[tuple[str, bytes | None]] | None = None,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float = -1) -> int: ...

    def init_transactions(self, timeout: float = -1) -> None: ...

    def begin_transaction(self) -> None: ...

    def send_offsets_to_transaction(
        self, positions: list[TopicPartition], group_metadata: object, timeout: float = -1
    ) -> None: ...

    def commit_transaction(self, timeout: float = -1) -> None: ...

    def abort_transaction(self, timeout: float = -1) -> None: ...


class ConsumerLike(Protocol):
    def subscribe(
        self,
        topics: list[str],
        on_assign: Callable[[ConsumerLike, list[TopicPartition]], None] | None = None,
        on_revoke: Callable[[ConsumerLike, list[TopicPartition]], None] | None = None,
    ) -> None: ...

    def assign(self, partitions: list[TopicPartition]) -> None: ...

    def unassign(self) -> None: ...

    def assignment(self) -> list[TopicPartition]: ...

    def poll(self, timeout: float = -1) -> MessageLike | None: ...

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[MessageLike]: ...

    def commit(
        self,
        message: MessageLike | None = None,
        offsets: list[TopicPartition] | None = None,
        asynchronous: bool = True,
    ) -> list[TopicPartition] | None: ...

    def committed(
        self, partitions: list[TopicPartition], timeout: float = -1
    ) -> list[TopicPartition]: ...

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]: ...

    def seek(self, partition: TopicPartition) -> None: ...

    def pause(self, partitions: list[TopicPartition]) -> None: ...

    def resume(self, partitions: list[TopicPartition]) -> None: ...

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float | None = None, cached: bool = False
    ) -> tuple[int, int]: ...

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> object: ...

    def consumer_group_metadata(self) -> object: ...

    def close(self) -> None: ...


class AdminLike(Protocol):
    def list_topics(self, topic: str | None = None, timeout: float = -1) -> object: ...

    def describe_configs(
        self, resources: list[ConfigResource], **kwargs: object
    ) -> Mapping[ConfigResource, FutureLike]: ...


class FutureLike(Protocol):
    def result(self, timeout: float | None = None) -> object: ...


class KafkaClientFactory(Protocol):
    def api_versions(
        self, settings: KafkaBindingSettings, host: str, port: int, *, deadline: float
    ) -> Mapping[int, tuple[int, int]]: ...

    def producer(self, config: KafkaClientConfig) -> ProducerLike: ...

    def consumer(self, config: KafkaClientConfig) -> ConsumerLike: ...

    def admin(self, config: KafkaClientConfig) -> AdminLike: ...


class ConfluentKafkaClientFactory:
    """Create only data-plane clients; no Admin mutation method is exposed."""

    def api_versions(
        self, settings: KafkaBindingSettings, host: str, port: int, *, deadline: float
    ) -> Mapping[int, tuple[int, int]]:
        from .probe.protocol import probe_api_versions

        return probe_api_versions(settings, host, port, deadline=deadline)

    def producer(self, config: KafkaClientConfig) -> ProducerLike:
        return cast(ProducerLike, Producer(dict(config)))

    def consumer(self, config: KafkaClientConfig) -> ConsumerLike:
        return cast(ConsumerLike, Consumer(dict(config)))

    def admin(self, config: KafkaClientConfig) -> AdminLike:
        return cast(AdminLike, AdminClient(dict(config)))


def as_message(value: Message) -> MessageLike:
    return cast(MessageLike, value)


def topic_partitions(values: Sequence[tuple[str, int, int]]) -> list[TopicPartition]:
    return [TopicPartition(topic, partition, offset) for topic, partition, offset in values]


__all__ = [
    "AdminLike",
    "ConfluentKafkaClientFactory",
    "ConsumerLike",
    "DeliveryCallback",
    "FutureLike",
    "KafkaClientConfig",
    "KafkaClientConfigValue",
    "KafkaClientFactory",
    "MessageLike",
    "ProducerLike",
    "as_message",
    "topic_partitions",
]

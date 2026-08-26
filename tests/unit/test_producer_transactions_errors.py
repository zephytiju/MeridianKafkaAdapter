# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import cast

import pytest
from confluent_kafka import KafkaError
from tests.support import FakeBroker, FakeConsumer, FakeProducer, make_context, publish_operation

from meridian_storage import MeridianError
from meridian_storage.adapters.kafka.compiler import KafkaOperationCompiler, PublishCommand
from meridian_storage.adapters.kafka.config import KafkaBindingSettings
from meridian_storage.adapters.kafka.cursor import KafkaCursorCodec
from meridian_storage.adapters.kafka.errors import (
    KafkaOperationFailed,
    KafkaTransactionAborted,
    normalize_kafka_error,
)
from meridian_storage.adapters.kafka.producer import KafkaProducerEngine
from meridian_storage.adapters.kafka.telemetry import KafkaTelemetry
from meridian_storage.adapters.kafka.transactions import KafkaTransactionBridge
from meridian_storage.streaming import (
    CursorExpired,
    InvalidEvent,
    RebalanceConflict,
    StreamingPolicyDenied,
    StreamingUnavailable,
)


def _components() -> tuple[
    FakeBroker,
    FakeProducer,
    KafkaProducerEngine,
    KafkaOperationCompiler,
    KafkaTelemetry,
]:
    broker = FakeBroker()
    client = FakeProducer(broker)
    settings = KafkaBindingSettings.from_context(make_context())
    telemetry = KafkaTelemetry()
    engine = KafkaProducerEngine(
        client,
        KafkaCursorCodec(settings.secrets.cursor_keys, ttl_ms=settings.cursor_ttl_ms),
        telemetry,
        timeout_ms=250,
    )
    return broker, client, engine, KafkaOperationCompiler(settings), telemetry


def test_idempotent_publish_returns_receipt_and_rejects_key_conflict() -> None:
    broker, _, engine, compiler, telemetry = _components()
    first = compiler.compile(publish_operation("one", idempotency_key="stable"))
    assert isinstance(first, PublishCommand)

    receipt = engine.publish(first)[0]
    replay = engine.publish(first)[0]

    assert receipt.to_dict() == replay.to_dict()
    assert sum(len(records) for records in broker.records["events"]) == 1
    assert "meridian.kafka.publish.idempotency-replay" in telemetry.names()

    conflict = compiler.compile(
        publish_operation("different", sequence=2, idempotency_key="stable")
    )
    assert isinstance(conflict, PublishCommand)
    with pytest.raises(InvalidEvent, match="reused"):
        engine.publish(conflict)


def test_transaction_abort_does_not_poison_application_idempotency_cache() -> None:
    broker, _, engine, compiler, telemetry = _components()
    transactional = FakeProducer(broker, transactional=True)
    bridge = KafkaTransactionBridge(transactional, telemetry, timeout_ms=250)
    bridge.open()
    command = compiler.compile(publish_operation("transactional", idempotency_key="tx-key"))
    assert isinstance(command, PublishCommand)

    bridge.begin()
    first = engine.publish(command, producer=bridge.producer)[0]
    bridge.abort()
    bridge.begin()
    second = engine.publish(command, producer=bridge.producer)[0]
    bridge.commit()

    records = [record for partition in broker.records["events"] for record in partition]
    assert first.position is not None and second.position is not None
    assert first.position.value != second.position.value
    assert len(records) == 2
    assert records[0].aborted and not records[0].committed
    assert records[1].committed and not records[1].aborted


def test_transaction_stages_offsets_atomically_and_rejects_cross_group_use() -> None:
    broker = FakeBroker()
    telemetry = KafkaTelemetry()
    producer = FakeProducer(broker, transactional=True)
    bridge = KafkaTransactionBridge(producer, telemetry, timeout_ms=250)
    consumer = FakeConsumer(
        broker,
        {
            "group.id": "group-one",
            "isolation.level": "read_committed",
            "enable.partition.eof": False,
        },
    )
    bridge.open()
    bridge.begin()
    bridge.stage_offset(
        group_key="group-one",
        consumer=consumer,
        topic="events",
        partition=0,
        next_offset=2,
    )
    bridge.stage_offset(
        group_key="group-one",
        consumer=consumer,
        topic="events",
        partition=0,
        next_offset=3,
    )
    with pytest.raises(KafkaTransactionAborted, match="cannot cross"):
        bridge.stage_offset(
            group_key="group-two",
            consumer=consumer,
            topic="events",
            partition=1,
            next_offset=1,
        )
    bridge.commit()

    assert broker.committed[("group-one", "events", 0)] == 3
    assert not bridge.active
    assert "meridian.kafka.transaction.committed" in telemetry.names()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED), StreamingPolicyDenied),
        (KafkaError(KafkaError.OFFSET_OUT_OF_RANGE), CursorExpired),
        (KafkaError(KafkaError.REBALANCE_IN_PROGRESS), RebalanceConflict),
        (KafkaError(KafkaError.PRODUCER_FENCED), KafkaTransactionAborted),
        (KafkaError(KafkaError._ALL_BROKERS_DOWN), StreamingUnavailable),
    ],
)
def test_kafka_errors_are_normalized_without_broker_text(
    error: KafkaError, expected: type[BaseException]
) -> None:
    normalized = normalize_kafka_error(error, operation_contract="meridian.streaming.poll")
    assert isinstance(normalized, expected)
    assert "broker" not in str(normalized).lower()
    assert cast(MeridianError, normalized).adapter_provenance["kafkaErrorName"]


def test_unknown_producer_failure_is_stable_and_non_secret_bearing() -> None:
    _, producer, engine, compiler, _ = _components()
    producer.fail_next = RuntimeError("password=do-not-retain")
    command = compiler.compile(publish_operation("failure"))
    assert isinstance(command, PublishCommand)

    with pytest.raises(KafkaOperationFailed) as captured:
        engine.publish(command)
    assert "do-not-retain" not in str(captured.value)
    assert captured.value.adapter_provenance["errorType"] == "RuntimeError"

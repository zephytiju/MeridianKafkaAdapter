# SPDX-License-Identifier: Apache-2.0
"""Acceptance matrix against an externally provisioned Apache Kafka cluster."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import replace
from typing import Any, cast

import pytest
from confluent_kafka import TopicPartition

from meridian_storage import Operation
from meridian_storage.adapters.kafka import KafkaAdapterFactory, KafkaAdapterRuntime
from meridian_storage.adapters.kafka._constants import TRANSACTION_OPERATION_CONTRACT
from meridian_storage.adapters.kafka.errors import KafkaOperationFailed
from meridian_storage.spi import AdapterCreateContext, AdapterSession
from meridian_storage.streaming import (
    CursorExpired,
    Delivery,
    InvalidEvent,
    MigrationRequired,
    RangePage,
    RebalanceConflict,
    StreamingPolicyDenied,
    StreamingUnavailable,
)
from tests.cluster.conftest import ClusterCase, ClusterHarness
from tests.support import (
    COMPATIBLE_SCHEMA_FINGERPRINT,
    DLQ_REF,
    GROUP_REF,
    STREAM_REF,
    SUBSCRIPTION_REF,
    ZERO_FINGERPRINT,
    make_event,
    physical_resources,
    provider_operation,
    publish_operation,
    request,
)

pytestmark = pytest.mark.cluster


def _open(context: AdapterCreateContext) -> tuple[KafkaAdapterRuntime, AdapterSession]:
    runtime = KafkaAdapterFactory().create(context)
    runtime.open()
    return runtime, runtime.open_session(transactional=False)


def _execute(session: AdapterSession, operation: Operation) -> object:
    return session.execute(request(operation)).data


def _poll_once(
    session: AdapterSession,
    *,
    limit: int = 100,
    wait_timeout_ms: int = 500,
) -> tuple[Delivery, ...]:
    raw = _execute(
        session,
        provider_operation(
            "poll",
            subscription=SUBSCRIPTION_REF.to_dict(),
            consumer_group=GROUP_REF.to_dict(),
            limit=limit,
            wait_timeout_ms=wait_timeout_ms,
        ),
    )
    if not isinstance(raw, Sequence) or isinstance(raw, str | bytes):
        raise AssertionError("Kafka poll did not return the released Delivery sequence")
    return tuple(
        Delivery.from_mapping(cast(Mapping[str, object], item))
        for item in raw
        if isinstance(item, Mapping)
    )


def _poll_until(
    session: AdapterSession,
    count: int,
    *,
    timeout: float = 15,
) -> tuple[Delivery, ...]:
    deadline = time.monotonic() + timeout
    deliveries: list[Delivery] = []
    while len(deliveries) < count and time.monotonic() < deadline:
        deliveries.extend(_poll_once(session, limit=count - len(deliveries)))
    assert len(deliveries) == count
    return tuple(deliveries)


def _ack(session: AdapterSession, delivery: Delivery) -> Mapping[str, object]:
    result = _execute(
        session,
        provider_operation(
            "acknowledge",
            subscription=SUBSCRIPTION_REF.to_dict(),
            consumer_group=GROUP_REF.to_dict(),
            delivery=delivery.delivery.to_dict(),
        ),
    )
    return cast(Mapping[str, object], result)


def _negative_acknowledge(
    session: AdapterSession,
    delivery: Delivery,
) -> Mapping[str, object]:
    result = _execute(
        session,
        provider_operation(
            "negative_acknowledge",
            subscription=SUBSCRIPTION_REF.to_dict(),
            consumer_group=GROUP_REF.to_dict(),
            delivery=delivery.delivery.to_dict(),
            retry_after_ms=0,
            classification="real-cluster-conformance",
        ),
    )
    return cast(Mapping[str, object], result)


def _range(
    session: AdapterSession,
    *,
    resource: object = STREAM_REF,
    start: object = None,
    limit: int = 100,
) -> RangePage:
    selected = resource.to_dict() if hasattr(resource, "to_dict") else resource
    result = _execute(
        session,
        provider_operation(
            "read_range",
            resource=selected,
            start=start,
            end=None,
            cursor=None,
            limit=limit,
        ),
    )
    return RangePage.from_mapping(cast(Mapping[str, object], result))


def _close(runtime: KafkaAdapterRuntime, session: AdapterSession) -> None:
    with suppress(BaseException):
        session.close()
    with suppress(BaseException):
        runtime.close()


def _large_event(event_id: str, sequence: int, *, key: str = "retained") -> dict[str, object]:
    event = make_event(event_id, sequence=sequence, logical_partition=key)
    # Kafka 4.x enforces a one-MiB minimum segment size. Two sub-MiB records
    # reliably roll that segment without exceeding the default producer limit.
    event["data"] = {"sequence": sequence, "payload": "x" * 600_000}
    return event


def _publish_raw(
    cluster: ClusterHarness,
    case: ClusterCase,
    event: Mapping[str, object],
    fingerprint: str,
) -> None:
    producer = cluster.producer()
    producer.produce(
        case.topic,
        key=b"schema-evolution",
        value=json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        headers=[
            ("meridian.format", b"1.0.0"),
            ("meridian.event-id", cast(str, event["eventId"]).encode("utf-8")),
            ("meridian.schema-fingerprint", fingerprint.encode("ascii")),
        ],
    )
    assert producer.flush(10) == 0


def test_publish_batch_finite_consume_order_ack_recovery_probe_and_telemetry(
    kafka_cluster: ClusterHarness,
) -> None:
    case = kafka_cluster.create_case("surface-order", partitions=2)
    context = case.context()
    runtime, session = _open(context)
    try:
        probe = runtime.probe()
        assert probe.manifest.engine_version == kafka_cluster.engine_version
        assert probe.evidence["controllerPresent"] == "true"
        assert probe.observed_engine_version is None
        assert probe.evidence["protocolValidation"] == "authenticated-api-ranges.v1"
        verification = runtime.verify_physical(physical_resources(context))
        assert verification.evidence["verifiedTopicCount"] == "2"
        subscribed = cast(
            Mapping[str, object],
            _execute(
                session,
                provider_operation(
                    "subscribe",
                    stream=STREAM_REF.to_dict(),
                    subscription=SUBSCRIPTION_REF.to_dict(),
                    options={},
                ),
            ),
        )
        assert subscribed["lifecycleOwner"] == "iac"

        events = [
            make_event("a-1", sequence=1, logical_partition="a"),
            make_event("b-1", sequence=1, logical_partition="b"),
            make_event("a-2", sequence=2, logical_partition="a"),
            make_event("b-2", sequence=2, logical_partition="b"),
        ]
        receipts = _execute(
            session,
            provider_operation(
                "publish_batch",
                resource=STREAM_REF.to_dict(),
                data=events,
                idempotency_key="ordered-batch",
            ),
        )
        assert isinstance(receipts, Sequence) and len(receipts) == 4
        deliveries = _poll_until(session, 4)
        observed: dict[str, list[int]] = {}
        for delivery in deliveries:
            key = delivery.event.logical_partition_key
            assert key is not None
            observed.setdefault(key, []).append(cast(int, delivery.event.data["sequence"]))
        assert observed == {"a": [1, 2], "b": [1, 2]}
        acknowledgements = [_ack(session, item) for item in reversed(deliveries)]
        assert all(item["acknowledged"] is True for item in acknowledgements)
        assert runtime._consumer_engine is not None
        runtime._consumer_engine.recover(SUBSCRIPTION_REF, GROUP_REF)
        assert _poll_once(session) == ()

        names = runtime.telemetry.names()
        assert {
            "meridian.kafka.probe.completed",
            "meridian.kafka.publish.completed",
            "meridian.kafka.poll.completed",
            "meridian.kafka.acknowledge.completed",
            "meridian.kafka.consumer.recovered",
        }.issubset(names)
        for event in runtime.telemetry.snapshot():
            assert not any(
                private in key.lower().replace("_", "")
                for key in event.attributes
                for private in ("secret", "password", "credential", "endpoint", "topic", "groupid")
            )
    finally:
        _close(runtime, session)


def test_redelivery_ack_recovery_and_dead_letter(kafka_cluster: ClusterHarness) -> None:
    case = kafka_cluster.create_case("delivery-recovery", partitions=1)
    context = case.context(max_delivery_attempts=2)
    runtime, session = _open(context)
    try:
        _execute(session, publish_operation("unacknowledged"))
        first = _poll_until(session, 1)[0]
        assert first.event.event_id == "unacknowledged"
    finally:
        _close(runtime, session)

    recovered_runtime, recovered_session = _open(context)
    try:
        recovered = _poll_until(recovered_session, 1)[0]
        assert recovered.event.event_id == "unacknowledged"
        assert _ack(recovered_session, recovered)["committed"] is True

        _execute(recovered_session, publish_operation("poison"))
        poison = _poll_until(recovered_session, 1)[0]
        scheduled = _negative_acknowledge(recovered_session, poison)
        assert scheduled["redeliveryScheduled"] is True
        retry = _poll_until(recovered_session, 1)[0]
        assert retry.event.event_id == "poison"
        assert retry.attempt == 2 and retry.redelivered
        dead_lettered = _negative_acknowledge(recovered_session, retry)
        assert dead_lettered["deadLettered"] is True
        dead_letters = _range(recovered_session, resource=DLQ_REF)
        assert len(dead_letters.events) == 1
        assert dead_letters.events[0].extensions["deadLetter"] is True
        assert _poll_once(recovered_session) == ()
    finally:
        _close(recovered_runtime, recovered_session)


def test_rebalance_invalidates_inflight_delivery_and_recovers(
    kafka_cluster: ClusterHarness,
) -> None:
    case = kafka_cluster.create_case("rebalance", partitions=1)
    runtime, session = _open(case.context(acknowledgement_timeout_ms=30_000))
    competitor = kafka_cluster.consumer(case.group_id)
    try:
        _execute(session, publish_operation("rebalance-source"))
        original = _poll_until(session, 1)[0]
        competitor.subscribe([case.topic])
        assert runtime._consumer_engine is not None
        state = next(iter(runtime._consumer_engine._states.values()))
        deadline = time.monotonic() + 20
        while (
            "meridian.kafka.rebalance.revoked" not in runtime.telemetry.names()
            and time.monotonic() < deadline
        ):
            competitor.poll(0.25)
            state.consumer.poll(0.25)
        assert "meridian.kafka.rebalance.revoked" in runtime.telemetry.names()
        with pytest.raises(RebalanceConflict):
            _ack(session, original)
        competitor.close()
        runtime._consumer_engine.recover(SUBSCRIPTION_REF, GROUP_REF)
        replayed = _poll_until(session, 1)[0]
        assert replayed.event.event_id == "rebalance-source"
        assert _ack(session, replayed)["acknowledged"] is True
    finally:
        with suppress(BaseException):
            competitor.close()
        _close(runtime, session)


def test_idempotent_producer_and_transactional_consume_publish(
    kafka_cluster: ClusterHarness,
) -> None:
    case = kafka_cluster.create_case("transactions", partitions=1)
    runtime, session = _open(case.context(transaction_enabled=True))
    try:
        operation = publish_operation("deduplicated", idempotency_key="stable-key")
        first = cast(Mapping[str, object], _execute(session, operation))
        second = cast(Mapping[str, object], _execute(session, operation))
        assert first == second
        assert [event.event_id for event in _range(session).events] == ["deduplicated"]

        source = _poll_until(session, 1)[0]
        transaction = Operation(
            catalog="streaming",
            operation_contract=TRANSACTION_OPERATION_CONTRACT,
            operation_version="1.0.0",
            resources=(STREAM_REF, GROUP_REF),
            input=cast(
                Any,
                {
                    "subscription": SUBSCRIPTION_REF.to_dict(),
                    "consumerGroup": GROUP_REF.to_dict(),
                    "limit": 1,
                    "waitTimeoutMs": 0,
                    "delivery": source.delivery.to_dict(),
                    "outputs": [
                        {
                            "resource": STREAM_REF.to_dict(),
                            "data": make_event("transaction-output", sequence=2),
                            "idempotencyKey": "transaction-output",
                        }
                    ],
                },
            ),
        )
        result = cast(Mapping[str, object], _execute(session, transaction))
        assert result["transactional"] is True
        assert runtime._consumer_engine is not None
        runtime._consumer_engine.recover(SUBSCRIPTION_REF, GROUP_REF)
        output = _poll_until(session, 1)[0]
        assert output.event.event_id == "transaction-output"
        assert _ack(session, output)["acknowledged"] is True
    finally:
        _close(runtime, session)


def test_cursor_ttl_and_retention_expiry(kafka_cluster: ClusterHarness) -> None:
    ttl_case = kafka_cluster.create_case("cursor-ttl", partitions=1)
    ttl_runtime, ttl_session = _open(ttl_case.context(cursor_ttl_ms=100))
    try:
        receipt = cast(
            Mapping[str, object],
            _execute(ttl_session, publish_operation("ttl-position")),
        )
        time.sleep(0.15)
        with pytest.raises(CursorExpired):
            _range(ttl_session, start=receipt["position"])
    finally:
        _close(ttl_runtime, ttl_session)

    retention_case = kafka_cluster.create_case(
        "retention-expiry",
        partitions=1,
        retention_ms=1_000,
        segment_bytes=1_048_576,
    )
    retention_runtime, retention_session = _open(retention_case.context())
    watermark_reader = kafka_cluster.consumer(f"{retention_case.group_id}-watermarks")
    try:
        first = cast(
            Mapping[str, object],
            _execute(
                retention_session,
                provider_operation(
                    "publish",
                    resource=STREAM_REF.to_dict(),
                    data=_large_event("retention-0", 0),
                    idempotency_key=None,
                ),
            ),
        )
        time.sleep(1.25)
        for index in range(1, 6):
            _execute(
                retention_session,
                provider_operation(
                    "publish",
                    resource=STREAM_REF.to_dict(),
                    data=_large_event(f"retention-{index}", index),
                    idempotency_key=None,
                ),
            )
        deadline = time.monotonic() + 40
        low = 0
        while low == 0 and time.monotonic() < deadline:
            low, _ = watermark_reader.get_watermark_offsets(
                TopicPartition(retention_case.topic, 0),
                timeout=5,
                cached=False,
            )
            if low == 0:
                time.sleep(1)
        assert low > 0, "Kafka retention did not advance the low watermark"
        with pytest.raises(CursorExpired):
            _range(retention_session, start=first["position"])
    finally:
        watermark_reader.close()
        _close(retention_runtime, retention_session)


def test_compaction_preserves_latest_keyed_event(kafka_cluster: ClusterHarness) -> None:
    case = kafka_cluster.create_case(
        "compaction",
        partitions=1,
        compacted=True,
        segment_bytes=1_048_576,
    )
    context = case.context()
    runtime, session = _open(context)
    try:
        runtime.verify_physical(physical_resources(context))
        for index in range(8):
            _execute(
                session,
                provider_operation(
                    "publish",
                    resource=STREAM_REF.to_dict(),
                    data=_large_event(f"compacted-{index}", index, key="compact-key"),
                    idempotency_key=None,
                ),
            )
        _execute(
            session,
            provider_operation(
                "publish",
                resource=STREAM_REF.to_dict(),
                data=_large_event("compaction-rollover", 100, key="rollover-key"),
                idempotency_key=None,
            ),
        )
        deadline = time.monotonic() + 40
        compacted_events: list[object] = []
        page = _range(session)
        while time.monotonic() < deadline:
            page = _range(session)
            compacted_events = [
                event for event in page.events if event.logical_partition_key == "compact-key"
            ]
            if len(compacted_events) == 1:
                break
            time.sleep(1)
        assert len(compacted_events) == 1
        latest = cast(Any, compacted_events[0])
        assert latest.event_id == "compacted-7"
        assert any(event.event_id == "compaction-rollover" for event in page.events)
    finally:
        _close(runtime, session)


def test_schema_evolution_accepts_compatible_and_rejects_incompatible(
    kafka_cluster: ClusterHarness,
) -> None:
    compatible_case = kafka_cluster.create_case("schema-compatible", partitions=1)
    compatible_runtime, compatible_session = _open(compatible_case.context())
    try:
        _publish_raw(
            kafka_cluster,
            compatible_case,
            make_event("compatible-schema"),
            COMPATIBLE_SCHEMA_FINGERPRINT,
        )
        assert _range(compatible_session).events[0].event_id == "compatible-schema"
    finally:
        _close(compatible_runtime, compatible_session)

    incompatible_case = kafka_cluster.create_case("schema-incompatible", partitions=1)
    incompatible_runtime, incompatible_session = _open(incompatible_case.context())
    try:
        _publish_raw(
            kafka_cluster,
            incompatible_case,
            make_event("incompatible-schema"),
            ZERO_FINGERPRINT,
        )
        with pytest.raises(InvalidEvent):
            _range(incompatible_session)
    finally:
        _close(incompatible_runtime, incompatible_session)


def test_acl_denial_and_live_credential_rotation(kafka_cluster: ClusterHarness) -> None:
    case = kafka_cluster.create_case("security", partitions=1)
    denied_runtime, denied_session = _open(
        case.context(username="denied", password="denied-secret")
    )
    try:
        with pytest.raises(StreamingPolicyDenied):
            _execute(denied_session, publish_operation("acl-denied"))
    finally:
        _close(denied_runtime, denied_session)

    context = case.context()
    runtime, session = _open(context)
    try:
        runtime.probe()
        rotated = case.context(username="rotated", password="meridian-secret-two")
        assert rotated.binding == context.binding
        runtime.rotate_credentials(rotated)
        _execute(session, publish_operation("after-rotation"))
        assert _range(session).events[0].event_id == "after-rotation"
        assert "meridian.kafka.credentials.rotated" in runtime.telemetry.names()
    finally:
        _close(runtime, session)


def test_partition_migration_detection_and_iac_recovery(
    kafka_cluster: ClusterHarness,
) -> None:
    case = kafka_cluster.create_case("partition-migration", partitions=1)
    original = case.context()
    runtime, session = _open(original)
    try:
        runtime.verify_physical(physical_resources(original))
        kafka_cluster.increase_partitions(case.topic, 2)
        with pytest.raises(MigrationRequired):
            runtime.verify_physical(physical_resources(original))
    finally:
        _close(runtime, session)

    migrated = case.context(partitions=2)
    recovered_runtime, recovered_session = _open(migrated)
    try:
        verification = recovered_runtime.verify_physical(physical_resources(migrated))
        assert verification.evidence["verifiedTopicCount"] == "2"
        _execute(recovered_session, publish_operation("post-migration"))
    finally:
        _close(recovered_runtime, recovered_session)


@pytest.mark.destructive_cluster
def test_broker_controller_failure_is_normalized_and_recovers(
    kafka_cluster: ClusterHarness,
) -> None:
    if os.environ.get("MERIDIAN_KAFKA_DESTRUCTIVE") != "1":
        pytest.skip("destructive cluster checks require the isolated full-suite runner")
    case = kafka_cluster.create_case("broker-controller-failure", partitions=1)
    runtime, session = _open(case.context())
    stopped = False
    try:
        runtime.probe()
        kafka_cluster.stop_broker()
        stopped = True
        with pytest.raises((StreamingUnavailable, KafkaOperationFailed)):
            runtime.probe()
    finally:
        if stopped:
            kafka_cluster.start_broker()
        _close(runtime, session)

    recovered_runtime, recovered_session = _open(case.context())
    try:
        recovered_runtime.probe()
        _execute(recovered_session, publish_operation("post-failure-recovery"))
        assert _range(recovered_session).events[0].event_id == "post-failure-recovery"
    finally:
        _close(recovered_runtime, recovered_session)


def test_unlisted_metadata_and_authenticated_tls(kafka_cluster: ClusterHarness) -> None:
    case = kafka_cluster.create_case("release-independent-tls", partitions=1)
    initial = case.context(tls=True, transaction_enabled=True)
    context = replace(
        initial, binding=replace(initial.binding, engine_version="99.0.0-metadata-only")
    )
    runtime, session = _open(context)
    try:
        probe = runtime.probe()
        assert probe.evidence["selectedEngineVersion"] == "99.0.0-metadata-only"
        assert probe.observed_engine_version is None
        assert probe.evidence["securityProfile"] == "authenticated-tls"
        runtime.verify_physical(physical_resources(context))
        _execute(session, publish_operation("tls-unlisted-metadata"))
        assert _range(session).events[0].event_id == "tls-unlisted-metadata"
    finally:
        _close(runtime, session)

    invalid_name = replace(
        context,
        binding=replace(
            context.binding, tls=replace(context.binding.tls, server_name="untrusted.invalid")
        ),
    )
    with pytest.raises((KafkaOperationFailed, StreamingUnavailable)):
        _open(invalid_name)
    with pytest.raises((KafkaOperationFailed, StreamingUnavailable)):
        _open(case.context(tls=True, password="wrong-isolated-test-password"))

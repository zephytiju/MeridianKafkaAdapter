# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import json
import time
from collections.abc import Mapping
from typing import Any, cast

import pytest
from tests.support import (
    DLQ_REF,
    GROUP_REF,
    SCHEMA_REF,
    STREAM_REF,
    SUBSCRIPTION_REF,
    FakeFactory,
    make_context,
    make_event,
    provider_operation,
    publish_operation,
    request,
)

from meridian_storage import MeridianError, Operation, ResourceRef
from meridian_storage.adapters.kafka import KafkaAdapterFactory, KafkaAdapterRuntime
from meridian_storage.adapters.kafka._constants import TRANSACTION_OPERATION_CONTRACT
from meridian_storage.adapters.kafka.errors import KafkaConfigurationError
from meridian_storage.spi import AdapterCreateContext, AdapterSession, SecretValue
from meridian_storage.streaming import (
    Delivery,
    DeliveryTimeout,
    GroupPositionTransition,
    RangePage,
    RebalanceConflict,
    ReplayOperation,
    StreamingPolicyDenied,
)


def _runtime(
    *, acknowledgement_timeout_ms: int = 5_000, max_delivery_attempts: int = 2
) -> tuple[KafkaAdapterRuntime, FakeFactory]:
    factory = FakeFactory()
    runtime = KafkaAdapterFactory(client_factory=factory).create(
        make_context(
            acknowledgement_timeout_ms=acknowledgement_timeout_ms,
            max_delivery_attempts=max_delivery_attempts,
        )
    )
    runtime.open()
    return runtime, factory


def _execute(session: AdapterSession, operation: Operation) -> object:
    return session.execute(request(operation)).data


def _poll(session: AdapterSession, *, limit: int = 100) -> tuple[Delivery, ...]:
    result = _execute(
        session,
        provider_operation(
            "poll",
            subscription=SUBSCRIPTION_REF.to_dict(),
            consumer_group=GROUP_REF.to_dict(),
            limit=limit,
            wait_timeout_ms=0,
        ),
    )
    assert isinstance(result, tuple)
    return tuple(Delivery.from_mapping(cast(Mapping[str, object], item)) for item in result)


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


def _negative_ack(session: AdapterSession, delivery: Delivery) -> Mapping[str, object]:
    result = _execute(
        session,
        provider_operation(
            "negative_acknowledge",
            subscription=SUBSCRIPTION_REF.to_dict(),
            consumer_group=GROUP_REF.to_dict(),
            delivery=delivery.delivery.to_dict(),
            retry_after_ms=0,
            classification="test-retry",
        ),
    )
    return cast(Mapping[str, object], result)


def _range(
    session: AdapterSession,
    *,
    resource: object = STREAM_REF,
    limit: int = 100,
) -> RangePage:
    selected_resource = resource.to_dict() if isinstance(resource, ResourceRef) else resource
    result = _execute(
        session,
        provider_operation(
            "read_range",
            resource=selected_resource,
            start=None,
            end=None,
            cursor=None,
            limit=limit,
        ),
    )
    return RangePage.from_mapping(cast(Mapping[str, object], result))


def test_publish_finite_poll_order_safe_acknowledgement_and_recovery() -> None:
    runtime, _ = _runtime()
    session = runtime.open_session(transactional=False)
    try:
        for event_id, partition, sequence in (
            ("a-1", "a", 1),
            ("b-1", "b", 1),
            ("a-2", "a", 2),
        ):
            receipt = _execute(
                session,
                publish_operation(
                    event_id,
                    logical_partition=partition,
                    sequence=sequence,
                    idempotency_key=event_id,
                ),
            )
            assert isinstance(receipt, Mapping)
        deliveries = _poll(session)
        assert len(deliveries) == 3
        by_partition: dict[str, list[int]] = {}
        for delivery in deliveries:
            key = delivery.event.logical_partition_key
            assert key is not None
            by_partition.setdefault(key, []).append(cast(int, delivery.event.data["sequence"]))
        assert all(values == sorted(values) for values in by_partition.values())

        acknowledgements = [_ack(session, item) for item in reversed(deliveries)]
        assert all(item["acknowledged"] is True for item in acknowledgements)
        assert any(item["committed"] is True for item in acknowledgements)
        assert runtime._consumer_engine is not None
        runtime._consumer_engine.recover(SUBSCRIPTION_REF, GROUP_REF)
        assert _poll(session) == ()
    finally:
        session.close()
        runtime.close()


def test_negative_acknowledgement_redelivery_and_dead_letter() -> None:
    runtime, _ = _runtime(max_delivery_attempts=2)
    session = runtime.open_session(transactional=False)
    try:
        _execute(session, publish_operation("poison"))
        first = _poll(session, limit=1)[0]
        scheduled = _negative_ack(session, first)
        assert scheduled["redeliveryScheduled"] is True
        second = _poll(session, limit=1)[0]
        assert second.event.event_id == first.event.event_id
        assert second.attempt == 2 and second.redelivered

        dead_lettered = _negative_ack(session, second)
        assert dead_lettered["deadLettered"] is True
        page = _range(session, resource=DLQ_REF.to_dict())
        assert len(page.events) == 1
        assert page.events[0].extensions["deadLetter"] is True
        assert page.events[0].data["failureClassification"] == "test-retry"
        assert _poll(session, limit=1) == ()
    finally:
        session.close()
        runtime.close()


def test_acknowledgement_timeout_redelivers_and_rejects_stale_token() -> None:
    runtime, _ = _runtime(acknowledgement_timeout_ms=50)
    session = runtime.open_session(transactional=False)
    try:
        _execute(session, publish_operation("expires"))
        first = _poll(session, limit=1)[0]
        time.sleep(0.06)
        second = _poll(session, limit=1)[0]
        assert second.event.event_id == "expires"
        assert second.attempt == 2
        with pytest.raises(DeliveryTimeout):
            _ack(session, first)
        assert _ack(session, second)["acknowledged"] is True
    finally:
        session.close()
        runtime.close()


def test_rebalance_invalidates_inflight_delivery_and_recovers() -> None:
    runtime, factory = _runtime()
    session = runtime.open_session(transactional=False)
    try:
        _execute(session, publish_operation("rebalance"))
        delivery = _poll(session, limit=1)[0]
        consumer = factory.consumers[0]
        consumer.trigger_rebalance()
        with pytest.raises(RebalanceConflict):
            _ack(session, delivery)
        recovered = _poll(session, limit=1)[0]
        assert recovered.event.event_id == "rebalance"
        assert _ack(session, recovered)["acknowledged"] is True
    finally:
        session.close()
        runtime.close()


def test_range_replay_and_group_position_compare_and_set_are_separate() -> None:
    runtime, _ = _runtime()
    session = runtime.open_session(transactional=False)
    try:
        _execute(session, publish_operation("range-1", sequence=1))
        _execute(session, publish_operation("range-2", sequence=2))
        page = _range(session, limit=1)
        assert page.truncated and len(page.events) == 1
        assert page.next_cursor is not None
        assert runtime._consumer_engine is not None
        before = runtime._consumer_engine.group_position_fingerprint(SUBSCRIPTION_REF, GROUP_REF)

        replay = ReplayOperation(
            STREAM_REF,
            page.next_cursor.to_dict(),
            limit=10,
            authorization_ref="policy/replay",
            reason="conformance",
        ).to_operation()
        replayed = RangePage.from_mapping(cast(Mapping[str, object], _execute(session, replay)))
        assert replayed.events
        assert (
            runtime._consumer_engine.group_position_fingerprint(SUBSCRIPTION_REF, GROUP_REF)
            == before
        )

        transition = GroupPositionTransition(
            SUBSCRIPTION_REF,
            GROUP_REF,
            page.next_cursor.to_dict(),
            before,
            "policy/group-position",
            "approved-recovery",
        ).to_operation()
        changed = cast(Mapping[str, object], _execute(session, transition))
        assert changed["previousFingerprint"] == before
        assert changed["newFingerprint"] != before
        with pytest.raises(MeridianError):
            _execute(session, transition)
    finally:
        session.close()
        runtime.close()


def test_transactional_consume_publish_commits_output_and_source_offset() -> None:
    runtime, _ = _runtime()
    session = runtime.open_session(transactional=False)
    try:
        _execute(session, publish_operation("source"))
        source = _poll(session, limit=1)[0]
        operation = Operation(
            catalog="streaming",
            operation_contract=TRANSACTION_OPERATION_CONTRACT,
            operation_version="1.0.0",
            resources=(STREAM_REF, SUBSCRIPTION_REF, GROUP_REF),
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
        result = cast(Mapping[str, object], _execute(session, operation))
        assert result["transactional"] is True
        assert runtime._consumer_engine is not None
        runtime._consumer_engine.recover(SUBSCRIPTION_REF, GROUP_REF)
        assert _poll(session, limit=1)[0].event.event_id == "transaction-output"
    finally:
        session.close()
        runtime.close()


def test_core_transaction_commit_rollback_and_tenant_policy() -> None:
    runtime, _ = _runtime()
    rolled_back = runtime.open_session(transactional=True)
    committed = runtime.open_session(transactional=True)
    non_transactional = runtime.open_session(transactional=False)
    try:
        rolled_back.begin()
        rolled_back.execute(request(publish_operation("rolled-back")))
        rolled_back.rollback()
        committed.begin()
        committed.execute(request(publish_operation("committed")))
        committed.commit()
        page = _range(non_transactional)
        assert [item.event_id for item in page.events] == ["committed"]
        with pytest.raises(StreamingPolicyDenied):
            non_transactional.execute(request(publish_operation("denied"), tenant=None))
    finally:
        rolled_back.close()
        committed.close()
        non_transactional.close()
        runtime.close()


def test_subscribe_lifecycle_validation_and_credential_rotation() -> None:
    runtime, _ = _runtime()
    session = runtime.open_session(transactional=False)
    try:
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
        schema = cast(
            Mapping[str, object],
            _execute(
                session,
                provider_operation(
                    "publish_schema",
                    namespace="conformance",
                    name="event",
                    version="1.0.0",
                    definition={"type": "object"},
                    expected_registry_revision=1,
                    allow_breaking=False,
                ),
            ),
        )
        assert schema["validated"] is True
        resource = cast(
            Mapping[str, object],
            _execute(
                session,
                provider_operation(
                    "create_resource",
                    namespace="conformance",
                    name="events",
                    resource_type="stream",
                    schema=SCHEMA_REF.to_dict(),
                    options={},
                ),
            ),
        )
        assert resource["lifecycleOwner"] == "iac"

        context = make_context()
        credential = {
            "password": None,
            "privateKeyPem": None,
            "privateKeyPassword": None,
            "cursorKeys": [
                {
                    "id": "active",
                    "keyBase64": base64.b64encode(b"k" * 32).decode(),
                    "active": False,
                },
                {
                    "id": "new",
                    "keyBase64": base64.b64encode(b"n" * 32).decode(),
                    "active": True,
                },
            ],
        }
        rotated = AdapterCreateContext(
            context.binding,
            context.identity,
            SecretValue(json.dumps(credential).encode()),
        )
        runtime.rotate_credentials(rotated)
        assert "meridian.kafka.credentials.rotated" in runtime.telemetry.names()

        changed = make_context(engine_version="4.2.1")
        with pytest.raises(KafkaConfigurationError, match="cannot replace"):
            runtime.rotate_credentials(changed)
    finally:
        session.close()
        runtime.close()

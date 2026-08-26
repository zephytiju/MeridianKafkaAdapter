# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from tests.support import (
    GROUP_REF,
    STREAM_REF,
    SUBSCRIPTION_REF,
    FakeFactory,
    make_context,
    physical_resources,
    provider_operation,
    publish_operation,
    repin_context,
    request,
)

from meridian_storage import OperationContext
from meridian_storage.adapters.kafka import KafkaAdapterFactory, KafkaAdapterRuntime
from meridian_storage.spi import ExecutionResult
from meridian_storage.streaming import (
    CursorExpired,
    Delivery,
    GroupPositionTransition,
    InvalidStreamingDefinition,
    RangePage,
    ReplayOperation,
    StreamingPolicyDenied,
)
from meridian_storage.streaming.testing import (
    ConformanceDelivery,
    ConformanceRange,
    StreamingConformanceReport,
    run_streaming_conformance,
)
from meridian_storage.testing.adapter_conformance import (
    AdapterConformanceTarget,
    run_adapter_conformance,
)


def test_released_core_adapter_conformance_runner() -> None:
    clients = FakeFactory()
    factory = KafkaAdapterFactory(client_factory=clients)
    initial = make_context()
    probe_runtime = factory.create(initial)
    probe_runtime.open()
    try:
        capability_fingerprint = probe_runtime.probe().manifest.fingerprint
    finally:
        probe_runtime.close()
    context = repin_context(initial, capability_fingerprint)

    def assert_result(result: ExecutionResult) -> None:
        assert isinstance(result.data, Mapping)
        assert result.data["eventId"] == "core-conformance"
        assert result.provenance["adapterId"] == "meridian.kafka"

    report = run_adapter_conformance(
        AdapterConformanceTarget(
            factory=factory,
            create_context=context,
            resources=physical_resources(context),
            operation=publish_operation("core-conformance", idempotency_key="core-conformance"),
            context=OperationContext("principal:conformance", tenant="tenant-a"),
            assert_result=assert_result,
        )
    )

    assert report.adapter_id == "meridian.kafka"
    assert report.resource_count == 4
    assert report.checks == (
        "authenticated-open",
        "deterministic-capability-manifest",
        "deterministic-physical-verification",
        "normalized-execution",
        "transaction-commit-rollback",
    )


class _StreamingTarget:
    target_id = "meridian.kafka.fake-cluster"

    def __init__(self) -> None:
        self.factory = FakeFactory()
        self.runtime: KafkaAdapterRuntime = KafkaAdapterFactory(client_factory=self.factory).create(
            make_context()
        )
        self.runtime.open()
        self.session = self.runtime.open_session(transactional=False)
        self._cursors: dict[str, Mapping[str, object]] = {}
        self._expired: set[str] = set()
        self._evidence: list[str] = []

    @property
    def capability_fingerprint(self) -> str:
        return cast(str, self.runtime.probe().manifest.fingerprint)

    def reset(self) -> None:
        if self.runtime._consumer_engine is not None:
            self.runtime._consumer_engine.recover(SUBSCRIPTION_REF, GROUP_REF)
        self.factory.broker.reset()
        self._cursors.clear()
        self._expired.clear()

    def publish(
        self,
        *,
        event_id: str,
        logical_partition: str,
        sequence: int,
        tenant: str,
        authorized: bool = True,
    ) -> None:
        if not authorized:
            raise StreamingPolicyDenied("composition policy denied the tenant")
        self.session.execute(
            request(
                publish_operation(
                    event_id,
                    logical_partition=logical_partition,
                    sequence=sequence,
                    idempotency_key=event_id,
                ),
                tenant=tenant,
            )
        )
        self._evidence.extend(("audit", "lineage", "telemetry"))

    def poll(self, *, limit: int = 100) -> tuple[ConformanceDelivery, ...]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise InvalidStreamingDefinition("poll limit must be positive")
        result = self.session.execute(
            request(
                provider_operation(
                    "poll",
                    subscription=SUBSCRIPTION_REF.to_dict(),
                    consumer_group=GROUP_REF.to_dict(),
                    limit=limit,
                    wait_timeout_ms=0,
                )
            )
        )
        assert isinstance(result.data, tuple)
        deliveries = tuple(
            Delivery.from_mapping(cast(Mapping[str, object], item)) for item in result.data
        )
        return tuple(
            ConformanceDelivery(
                item.event.event_id,
                item.event.logical_partition_key or "unpartitioned",
                cast(int, item.event.data["sequence"]),
                item.delivery.value,
                item.attempt,
            )
            for item in deliveries
        )

    def acknowledge(self, token: str) -> None:
        self.session.execute(
            request(
                provider_operation(
                    "acknowledge",
                    subscription=SUBSCRIPTION_REF.to_dict(),
                    consumer_group=GROUP_REF.to_dict(),
                    delivery=token,
                )
            )
        )
        self._evidence.extend(("audit", "telemetry"))

    def negative_acknowledge(self, token: str) -> None:
        self.session.execute(
            request(
                provider_operation(
                    "negative_acknowledge",
                    subscription=SUBSCRIPTION_REF.to_dict(),
                    consumer_group=GROUP_REF.to_dict(),
                    delivery=token,
                    retry_after_ms=0,
                )
            )
        )

    def recover(self) -> None:
        assert self.runtime._consumer_engine is not None
        self.runtime._consumer_engine.recover(SUBSCRIPTION_REF, GROUP_REF)

    def read_range(self, cursor: str | None = None, *, limit: int = 100) -> ConformanceRange:
        if cursor in self._expired:
            raise CursorExpired(requirement="cursor.retained-range")
        result = self.session.execute(
            request(
                provider_operation(
                    "read_range",
                    resource=STREAM_REF.to_dict(),
                    start=None,
                    end=None,
                    cursor=cursor,
                    limit=limit,
                )
            )
        )
        page = RangePage.from_mapping(cast(Mapping[str, object], result.data))
        next_cursor = None if page.next_cursor is None else page.next_cursor.value
        if page.next_cursor is not None:
            self._cursors[page.next_cursor.value] = page.next_cursor.to_dict()
        return ConformanceRange(
            tuple(item.event_id for item in page.events),
            next_cursor,
            page.truncated,
        )

    def replay(self, cursor: str) -> ConformanceRange:
        result = self.session.execute(
            request(
                ReplayOperation(
                    STREAM_REF,
                    cursor,
                    limit=100,
                    authorization_ref="policy/replay",
                    reason="conformance",
                ).to_operation()
            )
        )
        page = RangePage.from_mapping(cast(Mapping[str, object], result.data))
        next_cursor = None if page.next_cursor is None else page.next_cursor.value
        return ConformanceRange(
            tuple(item.event_id for item in page.events),
            next_cursor,
            page.truncated,
        )

    def group_position_fingerprint(self) -> str:
        assert self.runtime._consumer_engine is not None
        return self.runtime._consumer_engine.group_position_fingerprint(SUBSCRIPTION_REF, GROUP_REF)

    def transition_group_position(self, cursor: str, expected_fingerprint: str) -> None:
        self.session.execute(
            request(
                GroupPositionTransition(
                    SUBSCRIPTION_REF,
                    GROUP_REF,
                    cursor,
                    expected_fingerprint,
                    "policy/group-position",
                    "conformance",
                ).to_operation()
            )
        )
        self._evidence.extend(("audit", "lineage", "telemetry"))

    def expire_cursor(self, cursor: str) -> None:
        if cursor not in self._cursors:
            raise AssertionError("conformance cursor is unknown")
        self._expired.add(cursor)

    def evidence_kinds(self) -> tuple[str, ...]:
        return tuple(self._evidence)

    def close(self) -> None:
        self.session.close()
        self.runtime.close()


def test_released_streaming_conformance_runner() -> None:
    target = _StreamingTarget()
    try:
        report: StreamingConformanceReport = run_streaming_conformance(target)
    finally:
        target.close()

    assert report.passed
    assert len(report.cases) == 8
    assert report.fingerprint.startswith("sha256:")

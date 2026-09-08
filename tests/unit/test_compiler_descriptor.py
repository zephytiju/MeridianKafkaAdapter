# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pytest
from tests.support import (
    GROUP_REF,
    SCHEMA_REF,
    STREAM_REF,
    SUBSCRIPTION_REF,
    ZERO_FINGERPRINT,
    make_context,
    make_event,
    provider_operation,
    publish_operation,
)

from meridian_storage import Operation, ResourceRef
from meridian_storage.adapters.kafka._constants import (
    CORE_TRANSACTION_CONTRACT,
    TRANSACTION_OPERATION_CONTRACT,
)
from meridian_storage.adapters.kafka.compiler import (
    AcknowledgeCommand,
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
from meridian_storage.adapters.kafka.config import KafkaBindingSettings
from meridian_storage.adapters.kafka.descriptor import adapter_descriptor
from meridian_storage.streaming import (
    GroupPositionTransition,
    InvalidEvent,
    InvalidStreamingDefinition,
    ReplayOperation,
    StreamingCapabilityMismatch,
    streaming_requirements,
)


def _compiler(*, transaction_enabled: bool = True) -> KafkaOperationCompiler:
    return KafkaOperationCompiler(
        KafkaBindingSettings.from_context(make_context(transaction_enabled=transaction_enabled))
    )


def test_descriptor_is_exact_deterministic_and_covers_released_requirements() -> None:
    descriptor = adapter_descriptor()
    contracts = {item.operation_contract for item in descriptor.capabilities}
    expected = {item.operation_contract for item in streaming_requirements()}
    expected.update(
        {
            "meridian.streaming.replay",
            "meridian.streaming.group-position",
            TRANSACTION_OPERATION_CONTRACT,
            CORE_TRANSACTION_CONTRACT,
        }
    )

    assert descriptor.adapter_id == "meridian.kafka"
    assert descriptor.driver == "confluent-kafka"
    assert contracts == expected
    assert len(descriptor.capabilities) == 13
    assert descriptor.supported_engine_versions["apache-kafka"] == (
        "4.1.2",
        "4.2.1",
        "4.3.1",
    )
    assert descriptor.fingerprint == adapter_descriptor().fingerprint
    assert descriptor.fingerprint == (
        "sha256:e1595e61aa5a2ce8ccc05dc5ec0aaa1c8018059652c553aadb24b4e785a540d5"
    )
    assert all(item.migration_behavior == "iac-external" for item in descriptor.capabilities)


def test_compiler_covers_exact_catalog_and_explicit_operation_surface() -> None:
    compiler = _compiler()
    publish = publish_operation("one", idempotency_key="key-one")
    batch = provider_operation(
        "publish_batch",
        resource=STREAM_REF.to_dict(),
        data=[make_event("two"), make_event("three", sequence=2)],
        idempotency_key="batch-one",
    )
    subscribe = provider_operation(
        "subscribe",
        stream=STREAM_REF.to_dict(),
        subscription=SUBSCRIPTION_REF.to_dict(),
        options={},
    )
    poll = provider_operation(
        "poll",
        subscription=SUBSCRIPTION_REF.to_dict(),
        consumer_group=GROUP_REF.to_dict(),
        limit=10,
        wait_timeout_ms=0,
    )
    acknowledge = provider_operation(
        "acknowledge",
        subscription=SUBSCRIPTION_REF.to_dict(),
        consumer_group=GROUP_REF.to_dict(),
        delivery="opaque-delivery",
    )
    negative = provider_operation(
        "negative_acknowledge",
        subscription=SUBSCRIPTION_REF.to_dict(),
        consumer_group=GROUP_REF.to_dict(),
        delivery="opaque-delivery",
        retry_after_ms=5,
        classification="retryable",
    )
    read_range = provider_operation(
        "read_range",
        resource=STREAM_REF.to_dict(),
        start=None,
        end=None,
        cursor=None,
        limit=5,
    )
    schema = provider_operation(
        "publish_schema",
        namespace="conformance",
        name="event",
        version="1.0.0",
        definition={"type": "object"},
        expected_registry_revision=1,
        allow_breaking=False,
    )
    resource = provider_operation(
        "create_resource",
        namespace="conformance",
        name="events",
        resource_type="stream",
        schema=SCHEMA_REF.to_dict(),
        options={},
    )
    replay = ReplayOperation(
        STREAM_REF,
        "opaque-position",
        limit=5,
        authorization_ref="policy/replay",
        reason="test",
    ).to_operation()
    transition = GroupPositionTransition(
        SUBSCRIPTION_REF,
        GROUP_REF,
        "opaque-position",
        ZERO_FINGERPRINT,
        "policy/group-position",
        "test",
    ).to_operation()

    assert isinstance(compiler.compile(publish), PublishCommand)
    compiled_batch = compiler.compile(batch)
    assert isinstance(compiled_batch, PublishCommand) and compiled_batch.batch
    assert isinstance(compiler.compile(subscribe), SubscribeCommand)
    assert isinstance(compiler.compile(poll), PollCommand)
    assert isinstance(compiler.compile(acknowledge), AcknowledgeCommand)
    assert isinstance(compiler.compile(negative), NegativeAcknowledgeCommand)
    assert isinstance(compiler.compile(read_range), ReadRangeCommand)
    assert isinstance(compiler.compile(schema), ValidateSchemaCommand)
    assert isinstance(compiler.compile(resource), ValidateResourceCommand)
    compiled_replay = compiler.compile(replay)
    assert isinstance(compiled_replay, ReadRangeCommand) and compiled_replay.replay
    assert isinstance(compiler.compile(transition), GroupPositionCommand)


def test_compiler_supports_single_binding_transaction_operation() -> None:
    compiler = _compiler()
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
                "delivery": "opaque-delivery",
                "outputs": [
                    {
                        "resource": STREAM_REF.to_dict(),
                        "data": make_event("transaction-output"),
                        "idempotencyKey": "transaction-output",
                    }
                ],
            },
        ),
    )

    command = compiler.compile(operation)
    assert isinstance(command, TransactionalConsumePublishCommand)
    assert command.poll.consumer_group.ref == GROUP_REF
    assert len(command.outputs) == 1

    disabled = _compiler(transaction_enabled=False)
    with pytest.raises(StreamingCapabilityMismatch):
        disabled.compile(operation)


def test_compiler_fails_closed_on_malformed_or_wrong_scope_operations() -> None:
    compiler = _compiler()
    publish = publish_operation("valid")

    with pytest.raises(StreamingCapabilityMismatch):
        compiler.compile(replace(publish, operation_version="2.0.0"))
    with pytest.raises(StreamingCapabilityMismatch):
        compiler.compile(
            Operation(
                "structured",
                "meridian.streaming.publish",
                "1.0.0",
                (ResourceRef("structured", "conformance", "events"),),
            )
        )
    with pytest.raises(InvalidStreamingDefinition, match="unknown or missing"):
        compiler.compile(replace(publish, input=cast(Any, {"resource": str(STREAM_REF)})))
    with pytest.raises(InvalidStreamingDefinition, match="unknown or missing"):
        compiler.compile(
            replace(
                publish,
                input=cast(
                    Any,
                    {
                        "resource": str(STREAM_REF),
                        "data": make_event("valid"),
                        "unexpected": True,
                    },
                ),
            )
        )

    partial = provider_operation(
        "publish",
        resource=STREAM_REF.to_dict(),
        data={"eventId": "partial", "data": {"value": 1}},
    )
    with pytest.raises(InvalidEvent):
        compiler.compile(partial)

    wrong_stream = provider_operation(
        "publish",
        resource=STREAM_REF.to_dict(),
        data=make_event(
            "wrong",
            stream=ResourceRef("streaming", "conformance", "other"),
        ),
    )
    with pytest.raises(InvalidEvent, match="another Stream"):
        compiler.compile(wrong_stream)

# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace

import pytest
from tests.support import (
    FakeFactory,
    make_context,
    physical_resources,
)

from meridian_storage.adapters.kafka import KafkaAdapterFactory
from meridian_storage.adapters.kafka._constants import (
    CORE_TRANSACTION_CONTRACT,
    TRANSACTION_OPERATION_CONTRACT,
)
from meridian_storage.adapters.kafka.errors import (
    KafkaConfigurationError,
    KafkaOperationFailed,
)
from meridian_storage.spi import PhysicalResource
from meridian_storage.streaming import MigrationRequired


def test_probe_is_deterministic_conditional_and_redacted() -> None:
    factory = FakeFactory()
    runtime = KafkaAdapterFactory(client_factory=factory).create(make_context())
    runtime.open()
    try:
        first = runtime.probe()
        second = runtime.probe()
        available = set(first.manifest.available_operation_contracts)
        assert first.manifest.to_dict() == second.manifest.to_dict()
        assert CORE_TRANSACTION_CONTRACT in available
        assert TRANSACTION_OPERATION_CONTRACT in available
        assert first.evidence["brokerCount"] == "1"
        assert "fake-cluster" not in str(first.evidence)
        assert first.evidence["clusterFingerprint"].startswith("sha256:")
    finally:
        runtime.close()

    disabled = KafkaAdapterFactory(client_factory=factory).create(
        make_context(transaction_enabled=False)
    )
    disabled.open()
    try:
        available = set(disabled.probe().manifest.available_operation_contracts)
        assert CORE_TRANSACTION_CONTRACT not in available
        assert TRANSACTION_OPERATION_CONTRACT not in available
    finally:
        disabled.close()


def test_physical_verification_is_complete_and_detects_mapping_drift() -> None:
    context = make_context()
    factory = FakeFactory()
    runtime = KafkaAdapterFactory(client_factory=factory).create(context)
    runtime.open()
    resources = physical_resources(context)
    try:
        first = runtime.verify_physical(resources)
        second = runtime.verify_physical(resources)
        assert first.fingerprint == second.fingerprint
        assert set(first.mappings) == {str(item.resource_ref) for item in resources}
        assert first.evidence == {
            "verifiedResourceCount": "4",
            "verifiedTopicCount": "2",
        }

        mismatch = (replace(resources[0], resource_fingerprint=f"sha256:{'9' * 64}"),)
        with pytest.raises(MigrationRequired, match="fingerprint differs"):
            runtime.verify_physical(mismatch)

        stream = next(item for item in resources if item.schema_fingerprint is not None)
        wrong_schema = replace(stream, schema_fingerprint=f"sha256:{'8' * 64}")
        with pytest.raises(MigrationRequired, match="Schema fingerprint"):
            runtime.verify_physical((wrong_schema,))
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("partition", "partition count"),
        ("retention", "retention differs"),
        ("compaction", "compaction policy"),
        ("missing", "topic is absent"),
    ],
)
def test_probe_detects_iac_owned_topic_lifecycle_drift(mutation: str, message: str) -> None:
    context = make_context()
    factory = FakeFactory()
    runtime = KafkaAdapterFactory(client_factory=factory).create(context)
    runtime.open()
    stream = next(
        item for item in physical_resources(context) if item.resource_ref.name == "events"
    )
    if mutation == "partition":
        factory.broker.records["events"].append([])
    elif mutation == "retention":
        factory.broker.configs["events"]["retention.ms"] = "120000"
    elif mutation == "compaction":
        factory.broker.configs["events"]["cleanup.policy"] = "compact,delete"
    else:
        del factory.broker.records["events"]
    try:
        with pytest.raises(MigrationRequired, match=message):
            runtime.verify_physical((stream,))
    finally:
        runtime.close()


def test_probe_broker_failure_is_normalized_and_recovers() -> None:
    factory = FakeFactory()
    runtime = KafkaAdapterFactory(client_factory=factory).create(make_context())
    runtime.open()
    try:
        factory.broker.available = False
        with pytest.raises(KafkaOperationFailed):
            runtime.probe()
        factory.broker.available = True
        assert runtime.probe().evidence["controllerPresent"] == "true"
    finally:
        runtime.close()


def test_verification_rejects_unknown_logical_resource() -> None:
    context = make_context()
    runtime = KafkaAdapterFactory(client_factory=FakeFactory()).create(context)
    runtime.open()
    unknown = PhysicalResource(
        resource_ref="streaming:conformance.unknown",
        resource_fingerprint=f"sha256:{'7' * 64}",
        schema_fingerprint=None,
        profile="conformance",
    )
    try:
        with pytest.raises(KafkaConfigurationError, match="no Kafka Binding mapping"):
            runtime.verify_physical((unknown,))
    finally:
        runtime.close()

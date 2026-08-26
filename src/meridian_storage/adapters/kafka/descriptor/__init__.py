# SPDX-License-Identifier: Apache-2.0
"""Immutable Core Adapter descriptor for Kafka 1.0.0."""

from __future__ import annotations

from functools import lru_cache

from meridian_storage.spi import AdapterDescriptor, OperationCapability
from meridian_storage.streaming import (
    MAX_BATCH_SIZE,
    MAX_POLL_SIZE,
    MAX_RANGE_SIZE,
    MAX_WAIT_TIMEOUT_MS,
    streaming_requirements,
)

from .._constants import (
    ADAPTER_CONTRACT_VERSION,
    ADAPTER_ID,
    CORE_TRANSACTION_CONTRACT,
    CORE_TRANSACTION_VERSION,
    DRIVER,
    PRODUCTION_ENGINE_PROFILE,
    SUPPORTED_ENGINE_VERSIONS,
    TEST_ENGINE_PROFILE,
    TRANSACTION_OPERATION_CONTRACT,
    TRANSACTION_OPERATION_VERSION,
)
from ..cursor import MAX_CURSOR_PARTITIONS

_LIMITS = {
    "meridian.streaming.publish-batch": {"maxBatchSize": MAX_BATCH_SIZE},
    "meridian.streaming.poll": {
        "maxPollSize": MAX_POLL_SIZE,
        "maxWaitTimeoutMs": MAX_WAIT_TIMEOUT_MS,
    },
    "meridian.streaming.read-range": {
        "maxRangeSize": MAX_RANGE_SIZE,
        "maxRangePartitions": MAX_CURSOR_PARTITIONS,
    },
    "meridian.streaming.replay": {
        "maxRangeSize": MAX_RANGE_SIZE,
        "maxRangePartitions": MAX_CURSOR_PARTITIONS,
    },
}

_EXTRA_GUARANTEES = {
    "meridian.streaming.publish": ("idempotent-producer", "schema-fingerprint"),
    "meridian.streaming.publish-batch": ("idempotent-producer", "schema-fingerprint"),
    "meridian.streaming.negative-acknowledge": ("dead-letter",),
    "meridian.streaming.read-range": ("retention-boundary-validation",),
    "meridian.streaming.replay": ("retention-boundary-validation",),
    "meridian.streaming.group-position": ("compare-and-set",),
}


def _streaming_capability(requirement: object) -> OperationCapability:
    operation_contract = requirement.operation_contract  # type: ignore[attr-defined]
    guarantees = tuple(
        sorted(
            set(requirement.guarantees)  # type: ignore[attr-defined]
            | set(_EXTRA_GUARANTEES.get(operation_contract, ()))
        )
    )
    cursor_behavior = (
        "opaque-hmac-sha256"
        if operation_contract
        in {
            "meridian.streaming.acknowledge",
            "meridian.streaming.negative-acknowledge",
            "meridian.streaming.poll",
            "meridian.streaming.publish",
            "meridian.streaming.publish-batch",
            "meridian.streaming.read-range",
            "meridian.streaming.replay",
            "meridian.streaming.group-position",
        }
        else "none"
    )
    return OperationCapability(
        operation_contract=operation_contract,
        operation_versions=(requirement.operation_version,),  # type: ignore[attr-defined]
        guarantees=guarantees,
        limits=_LIMITS.get(operation_contract, {}),
        cursor_behavior=cursor_behavior,
        migration_behavior="iac-external",
        health_probes=("authenticated-metadata", "physical-mapping", "transaction-coordinator"),
        extensions={
            "catalog": "streaming",
            "consumerSurface": "mapping-first",
            "lifecycleAuthority": "iac",
        },
    )


@lru_cache(maxsize=1)
def adapter_descriptor() -> AdapterDescriptor:
    capabilities = [_streaming_capability(item) for item in streaming_requirements()]
    capabilities.extend(
        (
            OperationCapability(
                operation_contract=CORE_TRANSACTION_CONTRACT,
                operation_versions=(CORE_TRANSACTION_VERSION,),
                guarantees=("atomic", "no-dirty-reads", "single-binding"),
                cursor_behavior="opaque-hmac-sha256",
                migration_behavior="iac-external",
                health_probes=("authenticated-metadata", "transaction-coordinator"),
                extensions={"scope": "single-compatible-kafka-binding"},
            ),
            OperationCapability(
                operation_contract=TRANSACTION_OPERATION_CONTRACT,
                operation_versions=(TRANSACTION_OPERATION_VERSION,),
                guarantees=(
                    "atomic-consumed-offset",
                    "atomic-publish",
                    "committed-reads",
                    "idempotent-producer",
                    "single-binding",
                ),
                limits={"maxBatchSize": MAX_BATCH_SIZE},
                cursor_behavior="opaque-hmac-sha256",
                migration_behavior="iac-external",
                health_probes=("authenticated-metadata", "transaction-coordinator"),
                extensions={"externalSideEffects": False},
            ),
        )
    )
    return AdapterDescriptor(
        adapter_id=ADAPTER_ID,
        adapter_contract_version=ADAPTER_CONTRACT_VERSION,
        driver=DRIVER,
        supported_engine_versions={
            PRODUCTION_ENGINE_PROFILE: SUPPORTED_ENGINE_VERSIONS,
            TEST_ENGINE_PROFILE: SUPPORTED_ENGINE_VERSIONS,
        },
        capabilities=tuple(capabilities),
    )


__all__ = ["adapter_descriptor"]

# SPDX-License-Identifier: Apache-2.0
"""Stable Meridian failure mapping for librdkafka and adapter validation."""

from __future__ import annotations

from typing import Any

from confluent_kafka import KafkaError, KafkaException

from meridian_storage import AuthorizationError, CompatibilityError, ConflictError, UnavailableError
from meridian_storage.streaming import (
    CursorExpired,
    RebalanceConflict,
    StreamingPolicyDenied,
    StreamingResourceNotFound,
    StreamingUnavailable,
)

from ._constants import ADAPTER_ID


class KafkaConfigurationError(CompatibilityError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__("MERIDIAN_KAFKA_CONFIGURATION_INVALID", message, **details)


class KafkaAuthenticationFailed(AuthorizationError):
    def __init__(self, message: str = "Kafka authentication failed", **details: Any) -> None:
        super().__init__("MERIDIAN_STREAMING_AUTHENTICATION_FAILED", message, **details)


class KafkaTransactionAborted(ConflictError):
    def __init__(self, message: str = "Kafka transaction was aborted", **details: Any) -> None:
        super().__init__("MERIDIAN_STREAMING_TRANSACTION_ABORTED", message, **details)


class KafkaOperationFailed(UnavailableError):
    def __init__(self, message: str = "Kafka operation failed", **details: Any) -> None:
        super().__init__("MERIDIAN_KAFKA_OPERATION_FAILED", message, **details)


def _known_codes(*names: str) -> set[int]:
    return {
        int(value) for name in names if isinstance((value := getattr(KafkaError, name, None)), int)
    }


_AUTHENTICATION = _known_codes("_AUTHENTICATION", "SASL_AUTHENTICATION_FAILED")
_AUTHORIZATION = _known_codes(
    "TOPIC_AUTHORIZATION_FAILED",
    "GROUP_AUTHORIZATION_FAILED",
    "CLUSTER_AUTHORIZATION_FAILED",
    "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
)
_NOT_FOUND = _known_codes("UNKNOWN_TOPIC_OR_PART", "UNKNOWN_TOPIC_ID")
_OFFSET = _known_codes("OFFSET_OUT_OF_RANGE")
_REBALANCE = _known_codes(
    "FENCED_INSTANCE_ID",
    "ILLEGAL_GENERATION",
    "UNKNOWN_MEMBER_ID",
    "REBALANCE_IN_PROGRESS",
)
_TRANSACTION = _known_codes(
    "PRODUCER_FENCED",
    "INVALID_PRODUCER_EPOCH",
    "CONCURRENT_TRANSACTIONS",
    "TRANSACTION_COORDINATOR_FENCED",
    "INVALID_TXN_STATE",
)
_UNAVAILABLE = _known_codes(
    "_ALL_BROKERS_DOWN",
    "_TRANSPORT",
    "_TIMED_OUT",
    "_MSG_TIMED_OUT",
    "BROKER_NOT_AVAILABLE",
    "LEADER_NOT_AVAILABLE",
    "NOT_CONTROLLER",
    "NOT_COORDINATOR",
    "COORDINATOR_NOT_AVAILABLE",
)


def kafka_error(value: BaseException | KafkaError) -> KafkaError | None:
    if isinstance(value, KafkaError):
        return value
    if isinstance(value, KafkaException) and value.args and isinstance(value.args[0], KafkaError):
        return value.args[0]
    return None


def normalize_kafka_error(
    value: BaseException | KafkaError,
    *,
    operation_contract: str | None = None,
) -> BaseException:
    """Map a client failure without retaining broker text or secret-bearing config."""

    error = kafka_error(value)
    if error is None:
        return KafkaOperationFailed(
            adapter_provenance={"adapterId": ADAPTER_ID, "errorType": type(value).__name__},
            operation_contract=operation_contract,
            retryable=False,
        )
    code = error.code()
    provenance = {
        "adapterId": ADAPTER_ID,
        "kafkaErrorCode": str(code),
        "kafkaErrorName": error.name(),
    }
    if code in _AUTHENTICATION:
        return KafkaAuthenticationFailed(
            adapter_provenance=provenance,
            operation_contract=operation_contract,
        )
    if code in _AUTHORIZATION:
        return StreamingPolicyDenied(
            "Kafka denied the scoped data-plane operation",
            requirement="binding.authorization",
            adapter_provenance=provenance,
            operation_contract=operation_contract,
        )
    if code in _NOT_FOUND:
        return StreamingResourceNotFound(
            "The provisioned Kafka Resource mapping is unavailable",
            requirement="binding.physical-resource",
            adapter_provenance=provenance,
            operation_contract=operation_contract,
        )
    if code in _OFFSET:
        return CursorExpired(
            requirement="cursor.retained-range",
            adapter_provenance=provenance,
            operation_contract=operation_contract,
        )
    if code in _REBALANCE:
        return RebalanceConflict(
            "Kafka group assignment changed before acknowledgement",
            requirement="delivery.group-generation",
            adapter_provenance=provenance,
            operation_contract=operation_contract,
        )
    if code in _TRANSACTION or error.txn_requires_abort() or error.fatal():
        return KafkaTransactionAborted(
            retryable=error.retriable(),
            adapter_provenance=provenance,
            operation_contract=operation_contract,
        )
    if code in _UNAVAILABLE or error.retriable():
        return StreamingUnavailable(
            "Kafka Binding is temporarily unavailable",
            requirement="binding.availability",
            adapter_provenance=provenance,
            operation_contract=operation_contract,
        )
    return KafkaOperationFailed(
        retryable=False,
        adapter_provenance=provenance,
        operation_contract=operation_contract,
    )


__all__ = [
    "KafkaAuthenticationFailed",
    "KafkaConfigurationError",
    "KafkaOperationFailed",
    "KafkaTransactionAborted",
    "kafka_error",
    "normalize_kafka_error",
]

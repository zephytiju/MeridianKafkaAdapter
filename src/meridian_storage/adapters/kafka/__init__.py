# SPDX-License-Identifier: Apache-2.0
"""Apache Kafka Adapter for the Meridian Streaming Catalog."""

from ._constants import (
    ADAPTER_CONTRACT_VERSION,
    ADAPTER_ID,
    CLIENT_VERSION,
    MIGRATION_SOURCE_VERSIONS,
    SUPPORTED_ENGINE_VERSIONS,
)
from ._version import __version__
from .config import KafkaBindingSettings
from .cursor import KafkaCursorCodec
from .descriptor import adapter_descriptor
from .errors import (
    KafkaAuthenticationFailed,
    KafkaConfigurationError,
    KafkaOperationFailed,
    KafkaTransactionAborted,
)
from .runtime import KafkaAdapterFactory, KafkaAdapterRuntime, KafkaAdapterSession

__all__ = [
    "ADAPTER_CONTRACT_VERSION",
    "ADAPTER_ID",
    "CLIENT_VERSION",
    "MIGRATION_SOURCE_VERSIONS",
    "SUPPORTED_ENGINE_VERSIONS",
    "KafkaAdapterFactory",
    "KafkaAdapterRuntime",
    "KafkaAdapterSession",
    "KafkaAuthenticationFailed",
    "KafkaBindingSettings",
    "KafkaConfigurationError",
    "KafkaCursorCodec",
    "KafkaOperationFailed",
    "KafkaTransactionAborted",
    "__version__",
    "adapter_descriptor",
]

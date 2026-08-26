# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from collections.abc import Mapping

import pytest
from confluent_kafka import KafkaError
from tests.support import make_context

from meridian_storage.adapters.kafka.config import KafkaBindingSettings
from meridian_storage.adapters.kafka.errors import KafkaConfigurationError, normalize_kafka_error
from meridian_storage.adapters.kafka.telemetry import KafkaTelemetry


def test_secret_values_and_client_configuration_are_redacted() -> None:
    context = make_context()
    settings = KafkaBindingSettings.from_context(context)
    representations = (
        repr(context),
        repr(context.identity),
        repr(context.credential),
        repr(settings),
        repr(settings.client_configuration()),
    )

    assert all("kkkk" not in item for item in representations)
    assert all("fake:9092" not in item for item in representations[1:])
    assert all("cursorKeys" not in item for item in representations)


def test_telemetry_is_bounded_and_rejects_private_attribute_names() -> None:
    telemetry = KafkaTelemetry(maximum_events=2)
    telemetry.emit("meridian.kafka.test", {"count": 1})
    telemetry.emit("meridian.kafka.test", {"count": 2})
    telemetry.emit("meridian.kafka.test", {"count": 3})
    assert len(telemetry.snapshot()) == 2
    assert telemetry.snapshot()[0].attributes["count"] == "2"

    for key in ("password", "credentialRef", "bootstrapEndpoint", "physicalTopic", "groupId"):
        with pytest.raises(ValueError, match="private data"):
            telemetry.emit("meridian.kafka.rejected", {key: "secret"})


def test_normalized_acl_denial_exposes_only_stable_safe_provenance() -> None:
    failure = normalize_kafka_error(
        KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED),
        operation_contract="meridian.streaming.publish",
    )
    envelope = failure.to_dict()  # type: ignore[attr-defined]
    assert envelope["code"] == "MERIDIAN_STREAMING_POLICY_DENIED"
    assert envelope["category"] == "AUTHORIZATION"
    provenance = envelope["adapterProvenance"]
    assert isinstance(provenance, Mapping)
    assert set(provenance) == {"adapterId", "kafkaErrorCode", "kafkaErrorName"}
    assert "secret" not in json.dumps(envelope).lower()


def test_production_profile_fails_closed_without_tls_and_authentication() -> None:
    context = make_context()
    document = context.binding.to_dict()
    document["engineProfile"] = "apache-kafka"
    from meridian_storage.runtime.config import BindingConfig
    from meridian_storage.spi import AdapterCreateContext

    changed = AdapterCreateContext(
        BindingConfig.from_mapping(document, "$.bindings[0]"),
        context.identity,
        context.credential,
    )
    with pytest.raises(KafkaConfigurationError, match="requires TLS"):
        KafkaBindingSettings.from_context(changed)


def test_closed_secrets_reject_unknown_fields_and_short_cursor_keys() -> None:
    context = make_context()
    secret = json.loads(context.credential.reveal())
    secret["unknown"] = "value"
    from meridian_storage.spi import AdapterCreateContext, SecretValue

    changed = AdapterCreateContext(
        context.binding,
        context.identity,
        SecretValue(json.dumps(secret).encode()),
    )
    with pytest.raises(KafkaConfigurationError):
        KafkaBindingSettings.from_context(changed)

    del secret["unknown"]
    secret["cursorKeys"][0]["keyBase64"] = "eA=="
    changed = AdapterCreateContext(
        context.binding,
        context.identity,
        SecretValue(json.dumps(secret).encode()),
    )
    with pytest.raises(KafkaConfigurationError, match="32 to 128 bytes"):
        KafkaBindingSettings.from_context(changed)

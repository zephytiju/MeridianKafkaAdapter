# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable, Mapping
from typing import cast

import pytest
from tests.support import (
    GROUP_REF,
    STREAM_REF,
    SUBSCRIPTION_REF,
    binding_document,
    make_context,
)

from meridian_storage import ResourceRef
from meridian_storage.adapters.kafka.config import (
    KafkaBindingSettings,
    TokenKey,
)
from meridian_storage.adapters.kafka.cursor import KafkaCursorCodec, TokenCodec
from meridian_storage.adapters.kafka.errors import KafkaConfigurationError
from meridian_storage.runtime.config import BindingConfig
from meridian_storage.spi import AdapterCreateContext, SecretValue
from meridian_storage.streaming import (
    CursorExpired,
    DeliveryRejected,
    DeliveryTimeout,
    InvalidCursor,
)


def _context_from_document(
    document: Mapping[str, object], template: AdapterCreateContext
) -> AdapterCreateContext:
    return AdapterCreateContext(
        BindingConfig.from_mapping(document, "$.bindings[0]"),
        template.identity,
        template.credential,
        template.tls_ca,
        template.tls_client_certificate,
    )


def test_closed_binding_parses_and_redacts_client_material() -> None:
    settings = KafkaBindingSettings.from_context(make_context())

    assert settings.transaction_enabled
    assert settings.stream(STREAM_REF).topic == "events"
    assert settings.group_stream(SUBSCRIPTION_REF, GROUP_REF)[2].ref == STREAM_REF
    summary = settings.redacted_summary()
    client = settings.client_configuration()
    representation = repr(client)

    assert summary["resourceCount"] == 4
    assert client.producer()["enable.idempotence"] is True
    assert client.producer()["request.timeout.ms"] == settings.operation_timeout_ms
    assert client.producer(transactional=True)["transactional.id"]
    assert client.consumer("group")["enable.auto.commit"] is False
    assert "request.timeout.ms" not in client.consumer("group")
    assert "request.timeout.ms" not in client.admin()
    assert "fake:9092" not in representation
    assert "cursorKeys" not in representation


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda value: cast(dict[str, object], value["settings"]).update({"unknown": True}),
            "settings are invalid",
        ),
        (
            lambda value: value.update({"engineVersion": "4.0.2"}),
            "unsupported Kafka Engine version",
        ),
        (
            lambda value: cast(dict[str, object], value["settings"]).update(
                {"allowPlaintextForTesting": False}
            ),
            "explicit plaintext opt-in",
        ),
    ],
)
def test_binding_rejects_closed_or_unsupported_configuration(
    mutate: Callable[[dict[str, object]], None], message: str
) -> None:
    template = make_context()
    document = binding_document()
    mutate(document)
    context = _context_from_document(document, template)

    with pytest.raises(KafkaConfigurationError, match=message):
        KafkaBindingSettings.from_context(context)


def test_binding_rejects_token_unsafe_key_id() -> None:
    context = make_context()
    credential = json.loads(context.credential.reveal())
    credential["cursorKeys"][0]["id"] = "unsafe.key"
    changed = AdapterCreateContext(
        context.binding,
        context.identity,
        SecretValue(json.dumps(credential).encode()),
    )

    with pytest.raises(KafkaConfigurationError, match="token-safe"):
        KafkaBindingSettings.from_context(changed)


def test_binding_rejects_duplicate_group_and_cross_namespace_schema() -> None:
    template = make_context()
    duplicate = binding_document()
    resources = cast(dict[str, object], cast(dict[str, object], duplicate["settings"])["resources"])
    resources["streaming:conformance.second_group"] = {
        "kind": "consumer-group",
        "subscription": str(SUBSCRIPTION_REF),
        "groupId": "conformance-group",
        "resourceFingerprint": f"sha256:{'9' * 64}",
    }
    with pytest.raises(KafkaConfigurationError, match="must be unique"):
        KafkaBindingSettings.from_context(_context_from_document(duplicate, template))

    namespace = binding_document()
    resources = cast(dict[str, object], cast(dict[str, object], namespace["settings"])["resources"])
    stream = cast(dict[str, object], resources[str(STREAM_REF)])
    schema = cast(dict[str, object], stream["schemaRef"])
    schema["namespace"] = "other"
    with pytest.raises(KafkaConfigurationError, match="share a logical Namespace"):
        KafkaBindingSettings.from_context(_context_from_document(namespace, template))


def test_cursor_position_and_delivery_tokens_are_opaque_scoped_and_authenticated() -> None:
    settings = KafkaBindingSettings.from_context(make_context())
    codec = KafkaCursorCodec(settings.secrets.cursor_keys, ttl_ms=60_000)
    cursor = codec.encode_cursor(STREAM_REF, "events", {0: 2, 1: 4})
    position = codec.encode_position(STREAM_REF, "events", 1, 7, logical_partition="a")
    delivery = codec.encode_delivery(
        SUBSCRIPTION_REF,
        GROUP_REF,
        "events",
        0,
        2,
        3,
        1,
        time.time_ns() // 1_000_000 + 60_000,
    )

    assert codec.decode_cursor(cursor.to_dict(), resource=STREAM_REF, topic="events").positions == {
        0: 2,
        1: 4,
    }
    assert (
        codec.decode_position(position.to_dict(), resource=STREAM_REF, topic="events").offset == 7
    )
    assert (
        codec.decode_delivery(
            delivery.to_dict(),
            subscription=SUBSCRIPTION_REF,
            consumer_group=GROUP_REF,
            topic="events",
        ).assignment_epoch
        == 3
    )
    assert "events" not in cursor.value
    assert "conformance-group" not in delivery.value

    tampered = f"{cursor.value[:-1]}{'A' if cursor.value[-1] != 'A' else 'B'}"
    with pytest.raises(InvalidCursor):
        codec.decode_cursor(tampered, resource=STREAM_REF, topic="events")
    with pytest.raises(InvalidCursor):
        codec.decode_cursor(
            cursor.value, resource=ResourceRef("streaming", "other", "x"), topic="events"
        )
    with pytest.raises(DeliveryRejected):
        codec.decode_delivery(
            delivery.value,
            subscription=SUBSCRIPTION_REF,
            consumer_group=ResourceRef("streaming", "conformance", "other"),
            topic="events",
        )


def test_cursor_expiry_and_key_rotation() -> None:
    old = TokenKey("old", b"o" * 32, True)
    old_codec = KafkaCursorCodec((old,), ttl_ms=100)
    cursor = old_codec.encode_cursor(
        STREAM_REF,
        "events",
        {0: 0},
        expires_at_ms=time.time_ns() // 1_000_000 - 1,
    )
    with pytest.raises(CursorExpired):
        old_codec.decode_cursor(cursor.value, resource=STREAM_REF, topic="events")

    live_cursor = old_codec.encode_cursor(STREAM_REF, "events", {0: 9})
    rotated = KafkaCursorCodec(
        (TokenKey("old", b"o" * 32, False), TokenKey("new", b"n" * 32, True)),
        ttl_ms=100,
    )
    assert rotated.decode_cursor(
        live_cursor.value, resource=STREAM_REF, topic="events"
    ).positions == {0: 9}

    expired_delivery = old_codec.encode_delivery(
        SUBSCRIPTION_REF,
        GROUP_REF,
        "events",
        0,
        0,
        1,
        1,
        time.time_ns() // 1_000_000 - 1,
    )
    with pytest.raises(DeliveryTimeout):
        old_codec.decode_delivery(
            expired_delivery.value,
            subscription=SUBSCRIPTION_REF,
            consumer_group=GROUP_REF,
            topic="events",
        )


def test_token_codec_rejects_invalid_shape_key_and_size() -> None:
    codec = TokenCodec((TokenKey("active", b"x" * 32, True),))
    token = codec.encode(
        {
            "type": "test",
            "resource": str(STREAM_REF),
            "topicFingerprint": f"sha256:{'0' * 64}",
            "positions": {"0": 0},
            "issuedAtMs": 1,
            "expiresAtMs": 2**62,
        }
    )
    assert codec.decode(token, "test")["type"] == "test"
    with pytest.raises(ValueError, match="semantic type"):
        codec.decode(token, "other")
    with pytest.raises(ValueError, match="unsupported format"):
        codec.decode(base64.b64encode(b"bad").decode(), "test")

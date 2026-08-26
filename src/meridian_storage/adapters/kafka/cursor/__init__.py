# SPDX-License-Identifier: Apache-2.0
"""Opaque authenticated Cursor, position, and delivery-token mapping."""

from __future__ import annotations

import base64
import hmac
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from meridian_storage import ResourceRef
from meridian_storage.streaming import (
    Cursor,
    CursorExpired,
    DeliveryRejected,
    DeliveryTimeout,
    DeliveryToken,
    InvalidCursor,
    Position,
)

from ..canonical import canonical_json_bytes, closed_object, sha256_bytes, sha256_fingerprint
from ..config import TokenKey

TOKEN_FORMAT = "mk1"  # nosec B105
MAX_CURSOR_PARTITIONS = 128
_MAX_TOKEN_BYTES = 4096


@dataclass(frozen=True, slots=True)
class CursorCoordinates:
    resource: ResourceRef
    positions: Mapping[int, int]
    expires_at_ms: int


@dataclass(frozen=True, slots=True)
class PositionCoordinates:
    resource: ResourceRef
    partition: int
    offset: int


@dataclass(frozen=True, slots=True)
class DeliveryCoordinates:
    subscription: ResourceRef
    consumer_group: ResourceRef
    partition: int
    offset: int
    assignment_epoch: int
    attempt: int
    expires_at_ms: int


class _TokenExpired(ValueError):
    pass


class TokenCodec:
    """HMAC key ring with one active signing key and bounded prior keys."""

    def __init__(self, keys: tuple[TokenKey, ...]) -> None:
        if not keys or sum(item.active for item in keys) != 1:
            raise ValueError("token codec requires exactly one active key")
        self._keys = {item.key_id: item for item in keys}
        self._active = next(item for item in keys if item.active)

    def encode(self, payload: Mapping[str, object]) -> str:
        body = _b64(canonical_json_bytes(payload))
        signed = f"{TOKEN_FORMAT}.{self._active.key_id}.{body}".encode("ascii")
        signature = _b64(hmac.digest(self._active.key, signed, "sha256"))
        token = f"{TOKEN_FORMAT}.{self._active.key_id}.{body}.{signature}"
        if len(token.encode("utf-8")) > _MAX_TOKEN_BYTES:
            raise ValueError("opaque token exceeds the V1 size bound")
        return token

    def decode(self, token: str, expected_type: str) -> Mapping[str, object]:
        if not isinstance(token, str) or not token or len(token.encode("utf-8")) > _MAX_TOKEN_BYTES:
            raise ValueError("opaque token has an invalid size")
        parts = token.split(".")
        if len(parts) != 4 or parts[0] != TOKEN_FORMAT:
            raise ValueError("opaque token has an unsupported format")
        _, key_id, body, encoded_signature = parts
        key = self._keys.get(key_id)
        if key is None:
            raise ValueError("opaque token was signed by an unavailable key")
        signed = f"{TOKEN_FORMAT}.{key_id}.{body}".encode("ascii")
        expected = hmac.digest(key.key, signed, "sha256")
        if not hmac.compare_digest(expected, _unb64(encoded_signature)):
            raise ValueError("opaque token integrity validation failed")
        decoded = _unb64(body)
        if len(decoded) > _MAX_TOKEN_BYTES:
            raise ValueError("opaque token payload exceeds the V1 size bound")
        value = json.loads(decoded)
        item = closed_object(
            value,
            "opaque token",
            required={
                "type",
                "resource",
                "topicFingerprint",
                "positions",
                "issuedAtMs",
                "expiresAtMs",
            },
            optional={
                "subscription",
                "consumerGroup",
                "assignmentEpoch",
                "attempt",
            },
        )
        if item["type"] != expected_type:
            raise ValueError("opaque token has an unexpected semantic type")
        expiry = _integer(item["expiresAtMs"], "expiresAtMs", minimum=1)
        if _now_ms() >= expiry:
            raise _TokenExpired("opaque token expired")
        return item


class KafkaCursorCodec:
    def __init__(self, keys: tuple[TokenKey, ...], *, ttl_ms: int) -> None:
        if not 1 <= ttl_ms <= 31_536_000_000:
            raise ValueError("cursor ttl must be between 1 ms and one year")
        self._codec = TokenCodec(keys)
        self._ttl_ms = ttl_ms

    def encode_cursor(
        self,
        resource: ResourceRef,
        topic: str,
        positions: Mapping[int, int],
        *,
        expires_at_ms: int | None = None,
    ) -> Cursor:
        normalized = _positions(positions)
        now = _now_ms()
        expiry = expires_at_ms or now + self._ttl_ms
        token = self._codec.encode(_payload("cursor", resource, topic, normalized, now, expiry))
        return Cursor(token, resource, _utc_from_ms(expiry))

    def decode_cursor(
        self,
        value: str | Mapping[str, object],
        *,
        resource: ResourceRef,
        topic: str,
    ) -> CursorCoordinates:
        try:
            if isinstance(value, Mapping):
                cursor = Cursor.from_mapping(value)
                if cursor.resource != resource:
                    raise InvalidCursor("Cursor targets another logical Resource")
                token = cursor.value
            else:
                token = value
            item = self._codec.decode(token, "cursor")
            _validate_resource_and_topic(item, resource, topic)
            return CursorCoordinates(
                resource,
                _parse_positions(item["positions"]),
                _integer(item["expiresAtMs"], "expiresAtMs", minimum=1),
            )
        except CursorExpired:
            raise
        except _TokenExpired as exc:
            raise CursorExpired(requirement="cursor.expiry") from exc
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise InvalidCursor("Cursor integrity or shape validation failed") from exc

    def encode_position(
        self,
        resource: ResourceRef,
        topic: str,
        partition: int,
        offset: int,
        *,
        logical_partition: str | None = None,
    ) -> Position:
        now = _now_ms()
        token = self._codec.encode(
            _payload(
                "position",
                resource,
                topic,
                {partition: offset},
                now,
                now + self._ttl_ms,
            )
        )
        return Position(token, resource, logical_partition)

    def decode_position(
        self,
        value: str | Mapping[str, object],
        *,
        resource: ResourceRef,
        topic: str,
    ) -> PositionCoordinates:
        try:
            if isinstance(value, Mapping):
                position = Position.from_mapping(value)
                if position.resource != resource:
                    raise ValueError("Position targets another logical Resource")
                token = position.value
            else:
                token = value
            item = self._codec.decode(token, "position")
            _validate_resource_and_topic(item, resource, topic)
            positions = _parse_positions(item["positions"])
            if len(positions) != 1:
                raise ValueError("Position must contain exactly one coordinate")
            partition, offset = next(iter(positions.items()))
            return PositionCoordinates(resource, partition, offset)
        except _TokenExpired as exc:
            raise CursorExpired(requirement="position.expiry") from exc
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise InvalidCursor("Position integrity or shape validation failed") from exc

    def encode_delivery(
        self,
        subscription: ResourceRef,
        consumer_group: ResourceRef,
        topic: str,
        partition: int,
        offset: int,
        assignment_epoch: int,
        attempt: int,
        expires_at_ms: int,
    ) -> DeliveryToken:
        now = _now_ms()
        payload = _payload(
            "delivery",
            subscription,
            topic,
            {partition: offset},
            now,
            expires_at_ms,
        )
        payload.update(
            {
                "subscription": str(subscription),
                "consumerGroup": str(consumer_group),
                "assignmentEpoch": assignment_epoch,
                "attempt": attempt,
            }
        )
        return DeliveryToken(
            self._codec.encode(payload),
            subscription,
            consumer_group,
            _utc_from_ms(expires_at_ms),
        )

    def decode_delivery(
        self,
        value: str | Mapping[str, object],
        *,
        subscription: ResourceRef,
        consumer_group: ResourceRef,
        topic: str,
    ) -> DeliveryCoordinates:
        try:
            if isinstance(value, Mapping):
                delivery = DeliveryToken.from_mapping(value)
                if (
                    delivery.subscription != subscription
                    or delivery.consumer_group != consumer_group
                ):
                    raise ValueError("delivery token targets another Subscription or ConsumerGroup")
                token = delivery.value
            else:
                token = value
            item = self._codec.decode(token, "delivery")
            _validate_resource_and_topic(item, subscription, topic)
            if item["subscription"] != str(subscription) or item["consumerGroup"] != str(
                consumer_group
            ):
                raise ValueError("delivery token scope mismatch")
            positions = _parse_positions(item["positions"])
            if len(positions) != 1:
                raise ValueError("delivery token must contain one coordinate")
            partition, offset = next(iter(positions.items()))
            return DeliveryCoordinates(
                subscription,
                consumer_group,
                partition,
                offset,
                _integer(item["assignmentEpoch"], "assignmentEpoch", minimum=0),
                _integer(item["attempt"], "attempt", minimum=1),
                _integer(item["expiresAtMs"], "expiresAtMs", minimum=1),
            )
        except _TokenExpired as exc:
            raise DeliveryTimeout(requirement="delivery.acknowledgement-timeout") from exc
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise DeliveryRejected("delivery token integrity or scope validation failed") from exc

    @staticmethod
    def group_position_fingerprint(topic: str, positions: Mapping[int, int]) -> str:
        return sha256_fingerprint(
            {
                "topicFingerprint": sha256_bytes(topic.encode("utf-8")),
                "positions": {
                    str(partition): offset for partition, offset in _positions(positions).items()
                },
            }
        )


def _payload(
    token_type: str,
    resource: ResourceRef,
    topic: str,
    positions: Mapping[int, int],
    issued_at_ms: int,
    expires_at_ms: int,
) -> dict[str, object]:
    return {
        "type": token_type,
        "resource": str(resource),
        "topicFingerprint": sha256_bytes(topic.encode("utf-8")),
        "positions": {str(key): value for key, value in sorted(positions.items())},
        "issuedAtMs": issued_at_ms,
        "expiresAtMs": expires_at_ms,
    }


def _positions(value: Mapping[int, int]) -> dict[int, int]:
    if not 1 <= len(value) <= MAX_CURSOR_PARTITIONS:
        raise ValueError(f"positions must contain between 1 and {MAX_CURSOR_PARTITIONS} entries")
    result: dict[int, int] = {}
    for partition, offset in value.items():
        if isinstance(partition, bool) or not isinstance(partition, int) or partition < 0:
            raise ValueError("partition must be a non-negative integer")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a non-negative integer")
        result[partition] = offset
    return dict(sorted(result.items()))


def _parse_positions(value: object) -> Mapping[int, int]:
    item = cast(Mapping[str, object], value)
    if not isinstance(item, Mapping):
        raise TypeError("positions must be an object")
    parsed: dict[int, int] = {}
    for raw_partition, raw_offset in item.items():
        if not isinstance(raw_partition, str) or not raw_partition.isdigit():
            raise ValueError("partition key must be a non-negative integer string")
        partition = int(raw_partition)
        parsed[partition] = _integer(raw_offset, "offset", minimum=0)
    return _positions(parsed)


def _validate_resource_and_topic(
    item: Mapping[str, object], resource: ResourceRef, topic: str
) -> None:
    if item["resource"] != str(resource):
        raise ValueError("opaque token targets another logical Resource")
    if item["topicFingerprint"] != sha256_bytes(topic.encode("utf-8")):
        raise ValueError("opaque token targets another physical mapping")


def _integer(value: object, field_name: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field_name} must be an integer no smaller than {minimum}")
    return value


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _unb64(value: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise ValueError("base64url value must be non-empty")
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _utc_from_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)


__all__ = [
    "MAX_CURSOR_PARTITIONS",
    "TOKEN_FORMAT",
    "CursorCoordinates",
    "DeliveryCoordinates",
    "KafkaCursorCodec",
    "PositionCoordinates",
    "TokenCodec",
]

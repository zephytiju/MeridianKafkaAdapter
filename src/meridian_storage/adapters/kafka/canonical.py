# SPDX-License-Identifier: Apache-2.0
"""Small deterministic helpers kept independent from private Core modules."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import cast

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]


def bounded_string(value: object, field_name: str, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"{field_name} must be a bounded non-empty string")
    return value


def json_value(value: object, *, path: str = "$") -> JsonValue:
    """Copy portable JSON while rejecting bool-as-number and non-finite floats."""

    if value is None or isinstance(value, bool | str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite number")
        return value
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            bounded_string(key, f"{path} key", 512)
            result[key] = json_value(item, path=f"{path}.{key}")
        return result
    if isinstance(value, tuple | list):
        return [json_value(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    raise TypeError(f"{path} is not portable JSON")


def canonical_json_bytes(value: object) -> bytes:
    selected = json_value(value)
    return json.dumps(
        selected,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_fingerprint(value: object) -> str:
    return f"sha256:{hashlib.sha256(canonical_json_bytes(value)).hexdigest()}"


def sha256_bytes(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_text(value: datetime | None = None) -> str:
    return (value or utc_now()).astimezone(UTC).isoformat().replace("+00:00", "Z")


def as_object(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise TypeError(f"{field_name} must be an object")
    return cast(Mapping[str, object], value)


def closed_object(
    value: object,
    field_name: str,
    *,
    required: set[str],
    optional: set[str] | frozenset[str] = frozenset(),
) -> Mapping[str, object]:
    item = as_object(value, field_name)
    missing = required - set(item)
    unknown = set(item) - required - optional
    if missing or unknown:
        raise ValueError(
            f"{field_name} has unknown or missing fields "
            f"(missing={sorted(missing)!r}, unknown={sorted(unknown)!r})"
        )
    return item


__all__ = [
    "JsonValue",
    "as_object",
    "bounded_string",
    "canonical_json_bytes",
    "closed_object",
    "json_value",
    "sha256_bytes",
    "sha256_fingerprint",
    "utc_now",
    "utc_text",
]

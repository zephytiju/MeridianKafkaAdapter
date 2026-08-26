# SPDX-License-Identifier: Apache-2.0
"""Bounded generic adapter evidence with an injectable composition-root sink."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from threading import RLock
from typing import Protocol, runtime_checkable

from .canonical import bounded_string, utc_text


@dataclass(frozen=True, slots=True)
class KafkaTelemetryEvent:
    name: str
    occurred_at: str
    attributes: Mapping[str, str]


@runtime_checkable
class KafkaTelemetrySink(Protocol):
    def emit(self, event: KafkaTelemetryEvent) -> None: ...


class NullKafkaTelemetrySink:
    def emit(self, event: KafkaTelemetryEvent) -> None:
        del event


class KafkaTelemetry:
    """Keep bounded release-test evidence and forward it to an injected sink."""

    _FORBIDDEN_KEYS = ("secret", "password", "credential", "endpoint", "topic", "groupid")

    def __init__(
        self,
        sink: KafkaTelemetrySink | None = None,
        *,
        maximum_events: int = 4096,
    ) -> None:
        if not 1 <= maximum_events <= 100_000:
            raise ValueError("maximum_events must be between 1 and 100000")
        self._sink = sink or NullKafkaTelemetrySink()
        self._events: deque[KafkaTelemetryEvent] = deque(maxlen=maximum_events)
        self._lock = RLock()

    def emit(self, name: str, attributes: Mapping[str, object] | None = None) -> None:
        selected: dict[str, str] = {}
        for key, raw in (attributes or {}).items():
            safe_key = bounded_string(key, "telemetry attribute name", 128)
            lowered = safe_key.lower().replace("_", "")
            if any(forbidden in lowered for forbidden in self._FORBIDDEN_KEYS):
                raise ValueError(f"telemetry attribute {safe_key!r} may expose private data")
            value = bounded_string(str(raw), f"telemetry attribute {safe_key}", 512)
            selected[safe_key] = value
        event = KafkaTelemetryEvent(
            bounded_string(name, "telemetry event name", 256),
            utc_text(),
            dict(sorted(selected.items())),
        )
        with self._lock:
            self._events.append(event)
        self._sink.emit(event)

    def snapshot(self) -> tuple[KafkaTelemetryEvent, ...]:
        with self._lock:
            return tuple(self._events)

    def names(self) -> tuple[str, ...]:
        return tuple(event.name for event in self.snapshot())


__all__ = [
    "KafkaTelemetry",
    "KafkaTelemetryEvent",
    "KafkaTelemetrySink",
    "NullKafkaTelemetrySink",
]

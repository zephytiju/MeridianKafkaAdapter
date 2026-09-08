# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import socket
import struct
import time
from dataclasses import replace
from importlib.metadata import version
from typing import cast

import pytest
from tests.support import FakeFactory, make_context

from meridian_storage.adapters.kafka import KafkaAdapterFactory, adapter_descriptor
from meridian_storage.adapters.kafka.canonical import sha256_fingerprint
from meridian_storage.adapters.kafka.config import KafkaBindingSettings
from meridian_storage.adapters.kafka.errors import (
    KafkaAuthenticationFailed,
    KafkaConfigurationError,
)
from meridian_storage.adapters.kafka.probe.protocol import (
    BASE_APIS,
    TRANSACTION_APIS,
    _authenticate,
    _Connection,
    _Reader,
    _scram,
    parse_api_versions,
    validate_api_versions,
)


@pytest.mark.parametrize("release", ["3.9.2", "4.0.2", "99.17.3-vendor"])
def test_unlisted_selected_release_is_not_observed_or_a_membership_gate(release: str) -> None:
    runtime = KafkaAdapterFactory(client_factory=FakeFactory()).create(
        make_context(engine_version=release)
    )
    runtime.open()
    try:
        probe = runtime.probe()
        assert probe.manifest.engine_version == release
        assert probe.observed_engine_version is None
        assert probe.evidence["selectedEngineVersion"] == release
        assert probe.evidence["observedEngineVersion"].startswith("unavailable:")
        assert probe.manifest.extensions["coreDistributionVersion"] == version(
            "meridian-storage-core"
        )
        manifest = json.loads(json.dumps(probe.manifest.to_dict()))
        assert sha256_fingerprint(manifest) == probe.manifest.fingerprint
        descriptor = adapter_descriptor()
        assert (
            sha256_fingerprint(json.loads(json.dumps(descriptor.to_dict())))
            == descriptor.fingerprint
        )
    finally:
        runtime.close()


@pytest.mark.parametrize("key", list(BASE_APIS | TRANSACTION_APIS))
def test_missing_required_api_fails_before_readiness_even_for_tested_release(key: int) -> None:
    factory = FakeFactory()
    del factory.protocol_versions[key]
    runtime = KafkaAdapterFactory(client_factory=factory).create(make_context())
    with pytest.raises(KafkaConfigurationError, match="required API"):
        runtime.open()
    runtime.close()


def test_protocol_ranges_and_optional_transaction_gates() -> None:
    versions = dict(FakeFactory().protocol_versions)
    del versions[24]
    assert validate_api_versions(versions, transactional=False)
    with pytest.raises(KafkaConfigurationError, match="AddPartitionsToTxn"):
        validate_api_versions(versions, transactional=True)
    for incompatible in ((0, 2), (20, 21), (-1, 5), (5, 4)):
        versions[0] = incompatible
        with pytest.raises(KafkaConfigurationError, match="Produce"):
            validate_api_versions(versions, transactional=False)


@pytest.mark.parametrize(
    "pin", ["clientVersion", "coreDistributionVersion", "semanticsVersion", "streamingVersion"]
)
def test_selected_distribution_lock_drift_is_retained(pin: str) -> None:
    context = make_context()
    pins = dict(context.binding.compatibility_pins) | {pin: "99.0.0"}
    changed = replace(context, binding=replace(context.binding, compatibility_pins=pins))
    with pytest.raises(KafkaConfigurationError, match="deployment-selected lock"):
        KafkaBindingSettings.from_context(changed)


def test_independently_varied_installed_library_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    context = make_context(engine_version="77.0.0")
    pins = dict(context.binding.compatibility_pins) | {"streamingVersion": "1.7.9"}
    changed = replace(context, binding=replace(context.binding, compatibility_pins=pins))

    def observed(name: str) -> str:
        return "1.7.9" if name == "meridian-storage-streaming" else version(name)

    monkeypatch.setattr("meridian_storage.adapters.kafka.config.version", observed)
    monkeypatch.setattr("meridian_storage.adapters.kafka.probe.version", observed)
    runtime = KafkaAdapterFactory(client_factory=FakeFactory()).create(changed)
    runtime.open()
    try:
        assert runtime.probe().manifest.extensions["streamingVersion"] == "1.7.9"
    finally:
        runtime.close()


def test_api_response_parser_is_closed_and_bounded() -> None:
    payload = struct.pack(">hi", 0, 2) + struct.pack(">hhhhhh", 0, 3, 13, 18, 0, 4)
    assert parse_api_versions(payload) == {0: (3, 13), 18: (0, 4)}
    for bad in (
        b"",
        struct.pack(">h", 35),
        struct.pack(">hi", 0, 1001),
        payload[:-1],
        payload + b"extra",
        struct.pack(">hi", 0, 2) + struct.pack(">hhhhhh", 0, 3, 13, 0, 0, 4),
        struct.pack(">hihhh", 0, 1, 0, 9, 3),
    ):
        with pytest.raises(KafkaConfigurationError):
            parse_api_versions(bad)
    assert _Reader(b"\xff\xff").string() == b""


@pytest.mark.parametrize("mechanism", ["SCRAM-SHA-256", "SCRAM-SHA-512"])
@pytest.mark.parametrize("failure", [None, "nonce", "iterations", "signature"])
def test_scram_checks_nonce_work_factor_and_server_signature(
    mechanism: str, failure: str | None
) -> None:
    class Server:
        first = ""
        challenge = ""

        def authenticate(self, token: bytes) -> bytes:
            algorithm = "sha256" if mechanism.endswith("256") else "sha512"
            if not self.first:
                assert token.startswith(b"n,,n=test=2Cuser=3D,r=")
                self.first = token.decode()[3:]
                nonce = self.first.split(",r=")[1]
                self.challenge = (
                    f"r={'bad' if failure == 'nonce' else nonce}server,"
                    f"s=c2FsdA==,i={1 if failure == 'iterations' else 4096}"
                )
                return self.challenge.encode()
            final, proof = token.decode().rsplit(",p=", 1)
            assert len(base64.b64decode(proof)) == hashlib.new(algorithm).digest_size
            salted = hashlib.pbkdf2_hmac(algorithm, b"secret", b"salt", 4096)
            message = f"{self.first},{self.challenge},{final}".encode()
            signature = hmac.digest(
                hmac.digest(salted, b"Server Key", algorithm), message, algorithm
            )
            return b"v=" + base64.b64encode(b"bad" if failure == "signature" else signature)

    server = cast(_Connection, Server())
    if failure:
        with pytest.raises(KafkaAuthenticationFailed):
            _scram(server, mechanism, "test,user=", "secret")
    else:
        _scram(server, mechanism, "test,user=", "secret")


@pytest.mark.parametrize("failure", [None, "correlation", "frame", "closed", "timeout", "auth"])
def test_framed_sasl_protocol_checks_responses_and_deadline(failure: str | None) -> None:
    class Transport:
        incoming = bytearray()
        keys: list[int]

        def __init__(self) -> None:
            self.keys = []

        def settimeout(self, timeout: float) -> None:
            assert 0 < timeout <= 10

        def sendall(self, data: bytes) -> None:
            key, request_version, correlation = struct.unpack(">hhi", data[4:12])
            self.keys.append(key)
            if key == 17:
                assert request_version == 1
                body = struct.pack(">hih", 0, 1, 5) + b"PLAIN"
            else:
                assert key == 36 and request_version == 0
                assert data.endswith(b"\x00user\x00password")
                body = struct.pack(">hhi", 58 if failure == "auth" else 0, -1, 0)
            response = struct.pack(">i", 99 if failure == "correlation" else correlation) + body
            self.incoming.extend(
                struct.pack(">i", 2 if failure == "frame" else len(response)) + response
            )

        def recv(self, size: int) -> bytes:
            if failure == "closed":
                return b""
            value = bytes(self.incoming[: min(size, 2)])
            del self.incoming[: len(value)]
            return value

    settings = KafkaBindingSettings.from_context(make_context())
    settings = replace(
        settings,
        sasl_mechanism="PLAIN",
        identity=replace(settings.identity, username="user"),
        secrets=replace(settings.secrets, password="password"),
    )
    transport = Transport()
    connection = _Connection(
        cast(socket.socket, transport), time.monotonic() + (0 if failure == "timeout" else 10)
    )
    if failure:
        with pytest.raises((KafkaConfigurationError, KafkaAuthenticationFailed, TimeoutError)):
            _authenticate(connection, settings)
    else:
        _authenticate(connection, settings)
        assert transport.keys == [17, 36]


def test_exact_recipe_golden_documents_and_binding_roundtrip() -> None:
    from pathlib import Path

    from meridian_storage.runtime.config import BindingConfig

    root = Path(__file__).resolve().parents[1] / "fixtures"
    context = make_context()
    runtime = KafkaAdapterFactory(client_factory=FakeFactory()).create(context)
    runtime.open()
    try:
        probe = runtime.probe()
        assert probe.manifest.to_dict() == json.loads((root / "manifest.json").read_text())
        assert adapter_descriptor().to_dict() == json.loads((root / "descriptor.json").read_text())
        binding = json.loads((root / "binding.json").read_text())
        expected = replace(
            context.binding, required_capability_fingerprint=probe.manifest.fingerprint
        )
        assert BindingConfig.from_mapping(binding, "$.bindings[0]").to_dict() == expected.to_dict()
    finally:
        runtime.close()

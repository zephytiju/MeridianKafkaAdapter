# SPDX-License-Identifier: Apache-2.0
"""Bounded, authenticated ApiVersions probe; no data or administration writes.

Kafka's ApiVersions response describes protocol ranges, not broker releases.
The data clients still negotiate their own wire versions through librdkafka.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import socket
import ssl
import struct
import tempfile
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

from ..errors import KafkaAuthenticationFailed, KafkaConfigurationError

if TYPE_CHECKING:
    from ..config import KafkaBindingSettings

type ApiVersions = Mapping[int, tuple[int, int]]

# Required feature floors and wire ranges implemented by the data-plane client.
# These are protocol contracts, never broker implementation release numbers.
BASE_APIS = {
    0: ("Produce/record-batch-v2", 3, 10),
    1: ("Fetch/committed-reads", 4, 16),
    2: ("ListOffsets/retained-range", 1, 7),
    3: ("Metadata", 0, 13),
    8: ("OffsetCommit", 1, 9),
    9: ("OffsetFetch", 1, 9),
    10: ("FindCoordinator", 0, 2),
    11: ("JoinGroup", 0, 5),
    12: ("Heartbeat", 0, 3),
    13: ("LeaveGroup", 0, 1),
    14: ("SyncGroup", 0, 3),
    18: ("ApiVersions", 0, 0),
    22: ("InitProducerId/idempotence", 0, 4),
    32: ("DescribeConfigs/physical-lifecycle", 0, 1),
}
TRANSACTION_APIS = {
    10: ("FindCoordinator/transaction", 1, 2),
    24: ("AddPartitionsToTxn", 0, 0),
    25: ("AddOffsetsToTxn", 0, 0),
    26: ("EndTxn", 0, 1),
    28: ("TxnOffsetCommit/group-generation", 3, 3),
}


def validate_api_versions(versions: ApiVersions, *, transactional: bool) -> dict[str, int]:
    required = BASE_APIS | (TRANSACTION_APIS if transactional else {})
    negotiated: dict[str, int] = {}
    for key, (name, minimum, maximum) in required.items():
        observed = versions.get(key)
        if (
            observed is None
            or len(observed) != 2
            or any(type(value) is not int for value in observed)
            or not 0 <= observed[0] <= observed[1]
            or observed[1] < minimum
            or observed[0] > maximum
        ):
            raise KafkaConfigurationError(
                f"Kafka required API {name} has no compatible protocol version",
                adapter_provenance={"requirement": f"kafka.api.{key}"},
            )
        negotiated[str(key)] = min(observed[1], maximum)
    return negotiated


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def take(self, size: int) -> bytes:
        if size < 0 or self.offset + size > len(self.data):
            raise KafkaConfigurationError("Kafka probe response is truncated or invalid")
        value = self.data[self.offset : self.offset + size]
        self.offset += size
        return value

    def integer(self, size: int) -> int:
        return int.from_bytes(self.take(size), "big", signed=True)

    def string(self) -> bytes:
        size = self.integer(2)
        return b"" if size == -1 else self.take(size)

    def finish(self) -> None:
        if self.offset != len(self.data):
            raise KafkaConfigurationError("Kafka probe response contains unexpected fields")


def _string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack(">h", len(encoded)) + encoded


def parse_api_versions(data: bytes) -> dict[int, tuple[int, int]]:
    reader = _Reader(data)
    if reader.integer(2) != 0:
        raise KafkaConfigurationError("Kafka ApiVersions protocol request was rejected")
    count = reader.integer(4)
    if not 1 <= count <= 1_000:
        raise KafkaConfigurationError("Kafka ApiVersions count is invalid")
    result: dict[int, tuple[int, int]] = {}
    for _ in range(count):
        key, minimum, maximum = (reader.integer(2) for _ in range(3))
        if key < 0 or key in result or not 0 <= minimum <= maximum:
            raise KafkaConfigurationError("Kafka ApiVersions range is invalid")
        result[key] = (minimum, maximum)
    reader.finish()
    return result


class _Connection:
    def __init__(self, stream: socket.socket, deadline: float) -> None:
        self.stream = stream
        self.deadline = deadline
        self.correlation = 0

    def _timeout(self) -> None:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Kafka protocol probe deadline exceeded")
        self.stream.settimeout(remaining)

    def _read(self, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            self._timeout()
            chunk = self.stream.recv(size - len(chunks))
            if not chunk:
                raise KafkaConfigurationError("Kafka probe connection closed before response")
            chunks.extend(chunk)
        return bytes(chunks)

    def request(self, key: int, version: int, body: bytes) -> bytes:
        self.correlation += 1
        header = struct.pack(">hhi", key, version, self.correlation) + _string("meridian-probe")
        payload = header + body
        self._timeout()
        self.stream.sendall(struct.pack(">i", len(payload)) + payload)
        size = int.from_bytes(self._read(4), "big", signed=True)
        if not 4 <= size <= 1_048_576:
            raise KafkaConfigurationError("Kafka probe frame size is invalid")
        reader = _Reader(self._read(size))
        if reader.integer(4) != self.correlation:
            raise KafkaConfigurationError("Kafka probe correlation mismatch")
        return reader.take(size - 4)

    def authenticate(self, token: bytes) -> bytes:
        reader = _Reader(self.request(36, 0, struct.pack(">i", len(token)) + token))
        error = reader.integer(2)
        reader.string()  # Broker text may contain credentials; never retain it.
        response = reader.take(reader.integer(4))
        reader.finish()
        if error:
            raise KafkaAuthenticationFailed()
        return response


def _scram(connection: _Connection, mechanism: str, username: str, password: str) -> None:
    algorithm = "sha256" if mechanism == "SCRAM-SHA-256" else "sha512"
    nonce = secrets.token_urlsafe(24)
    escaped = username.replace("=", "=3D").replace(",", "=2C")
    first = f"n={escaped},r={nonce}"
    server = connection.authenticate(f"n,,{first}".encode()).decode("utf-8")
    try:
        fields = dict(item.split("=", 1) for item in server.split(","))
        iterations = int(fields["i"])
        if (
            len(fields) != len(server.split(","))
            or "m" in fields
            or not fields["r"].startswith(nonce)
            or fields["r"] == nonce
            or not 4_096 <= iterations <= 1_000_000
        ):
            raise ValueError("invalid SCRAM challenge")
        salt = base64.b64decode(fields["s"], validate=True)
        salted = hashlib.pbkdf2_hmac(algorithm, password.encode(), salt, iterations)
        final = f"c=biws,r={fields['r']}"
        message = f"{first},{server},{final}".encode()
        client_key = hmac.digest(salted, b"Client Key", algorithm)
        signature = hmac.digest(hashlib.new(algorithm, client_key).digest(), message, algorithm)
        proof = bytes(a ^ b for a, b in zip(client_key, signature, strict=True))
        reply = connection.authenticate(f"{final},p={base64.b64encode(proof).decode()}".encode())
        expected = hmac.digest(hmac.digest(salted, b"Server Key", algorithm), message, algorithm)
        if not reply.startswith(b"v=") or not hmac.compare_digest(
            base64.b64decode(reply[2:], validate=True), expected
        ):
            raise ValueError("invalid SCRAM server signature")
    except (ValueError, KeyError) as exc:
        raise KafkaAuthenticationFailed() from exc


def _authenticate(connection: _Connection, settings: KafkaBindingSettings) -> None:
    mechanism = settings.sasl_mechanism
    if mechanism is None:
        return
    reader = _Reader(connection.request(17, 1, _string(mechanism)))
    error = reader.integer(2)
    count = reader.integer(4)
    if not 0 <= count <= 100:
        raise KafkaConfigurationError("Kafka SASL mechanism count is invalid")
    mechanisms = [reader.string().decode() for _ in range(count)]
    reader.finish()
    if error or mechanism not in mechanisms:
        raise KafkaAuthenticationFailed("Kafka does not offer the selected SASL mechanism")
    username, password = settings.identity.username, settings.secrets.password
    if username is None or password is None:
        raise KafkaAuthenticationFailed()
    if mechanism == "PLAIN":
        if "\x00" in username or "\x00" in password:
            raise KafkaAuthenticationFailed()
        connection.authenticate(f"\x00{username}\x00{password}".encode())
    else:
        _scram(connection, mechanism, username, password)


def _tls_context(settings: KafkaBindingSettings) -> ssl.SSLContext:
    context = ssl.create_default_context(cadata=settings.tls_ca_pem)
    if settings.tls_mode == "mutual":
        # SSLContext needs filenames. NamedTemporaryFile is owner-only and removes
        # both files immediately after OpenSSL loads the key/certificate.
        with tempfile.NamedTemporaryFile() as certificate, tempfile.NamedTemporaryFile() as key:
            certificate.write((settings.tls_client_certificate_pem or "").encode())
            key.write((settings.secrets.private_key_pem or "").encode())
            certificate.flush()
            key.flush()
            context.load_cert_chain(
                certificate.name, key.name, settings.secrets.private_key_password
            )
    return context


def probe_api_versions(
    settings: KafkaBindingSettings, host: str, port: int, *, deadline: float
) -> dict[int, tuple[int, int]]:
    timeout = deadline - time.monotonic()
    if timeout <= 0:
        raise TimeoutError("Kafka protocol probe deadline exceeded")
    with socket.create_connection((host, port), timeout=timeout) as raw:
        if settings.tls_mode != "disabled":
            context = _tls_context(settings)
            with context.wrap_socket(raw, server_hostname=settings.tls_server_name or host) as tls:
                connection = _Connection(tls, deadline)
                _authenticate(connection, settings)
                return parse_api_versions(connection.request(18, 0, b""))
        connection = _Connection(raw, deadline)
        _authenticate(connection, settings)
        return parse_api_versions(connection.request(18, 0, b""))

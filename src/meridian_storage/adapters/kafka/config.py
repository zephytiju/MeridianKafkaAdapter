# SPDX-License-Identifier: Apache-2.0
"""Closed Binding settings and redacted Kafka client configuration."""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import cast

from meridian_storage import ResourceRef, SchemaRef
from meridian_storage.spi import AdapterCreateContext

from ._constants import (
    ADAPTER_CONTRACT_VERSION,
    ADAPTER_ID,
    PRODUCTION_ENGINE_PROFILE,
    SUPPORTED_ENGINE_VERSIONS,
    TEST_ENGINE_PROFILE,
)
from .canonical import as_object, bounded_string, closed_object
from .clients import KafkaClientConfigValue
from .errors import KafkaConfigurationError

_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_KAFKA_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_TOKEN_KEY_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_SASL_MECHANISMS = frozenset({"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"})
_MAX_CURSOR_KEYS = 4


def _fingerprint(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _FINGERPRINT_RE.fullmatch(value) is None:
        raise KafkaConfigurationError(f"{field_name} must be a SHA-256 fingerprint")
    return value


def _positive(value: object, field_name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise KafkaConfigurationError(f"{field_name} must be between 1 and {maximum}")
    return value


def _nonnegative(value: object, field_name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise KafkaConfigurationError(f"{field_name} must be between 0 and {maximum}")
    return value


def _boolean(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise KafkaConfigurationError(f"{field_name} must be boolean")
    return value


def _kafka_name(value: object, field_name: str, maximum: int = 249) -> str:
    try:
        selected = bounded_string(value, field_name, maximum)
    except ValueError as exc:
        raise KafkaConfigurationError(str(exc)) from exc
    if _KAFKA_NAME_RE.fullmatch(selected) is None or selected in {".", ".."}:
        raise KafkaConfigurationError(f"{field_name} has an invalid Kafka identifier")
    return selected


def _resource_ref(value: object, field_name: str) -> ResourceRef:
    try:
        if not isinstance(value, str | Mapping):
            raise TypeError
        selected = ResourceRef.parse(value, catalog="streaming")
        if selected.catalog != "streaming":
            raise ValueError
        return selected
    except (TypeError, ValueError) as exc:
        raise KafkaConfigurationError(
            f"{field_name} must be a logical streaming Resource reference"
        ) from exc


def _schema_ref(value: object, field_name: str) -> SchemaRef:
    try:
        if not isinstance(value, Mapping):
            raise TypeError
        selected = SchemaRef.parse(cast(Mapping[str, object], value))
        if selected.catalog != "streaming":
            raise ValueError
        return selected
    except (TypeError, ValueError) as exc:
        raise KafkaConfigurationError(
            f"{field_name} must be an exact streaming Schema reference"
        ) from exc


@dataclass(frozen=True, slots=True)
class TokenKey:
    key_id: str
    key: bytes = field(repr=False)
    active: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "key_id", bounded_string(self.key_id, "cursor key id", 64))
        if _TOKEN_KEY_ID_RE.fullmatch(self.key_id) is None:
            raise KafkaConfigurationError("cursor key id must be token-safe")
        if len(self.key) < 32 or len(self.key) > 128:
            raise KafkaConfigurationError("cursor keys must contain 32 to 128 bytes")


@dataclass(frozen=True, slots=True)
class KafkaIdentity:
    principal: str
    username: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class KafkaSecrets:
    password: str | None = field(default=None, repr=False)
    private_key_pem: str | None = field(default=None, repr=False)
    private_key_password: str | None = field(default=None, repr=False)
    cursor_keys: tuple[TokenKey, ...] = field(default=(), repr=False)

    @property
    def active_cursor_key(self) -> TokenKey:
        return next(item for item in self.cursor_keys if item.active)


@dataclass(frozen=True, slots=True)
class PhysicalStream:
    ref: ResourceRef
    topic: str = field(repr=False)
    schema_ref: SchemaRef
    schema_fingerprint: str
    compatible_schema_fingerprints: tuple[str, ...]
    resource_fingerprint: str
    logical_partitions: int
    retention_ms: int
    compacted: bool


@dataclass(frozen=True, slots=True)
class PhysicalSubscription:
    ref: ResourceRef
    stream: ResourceRef
    resource_fingerprint: str
    acknowledgement_timeout_ms: int
    max_delivery_attempts: int
    dead_letter_stream: ResourceRef | None
    filter: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class PhysicalConsumerGroup:
    ref: ResourceRef
    subscription: ResourceRef
    group_id: str = field(repr=False)
    resource_fingerprint: str = ""


PhysicalResourceConfig = PhysicalStream | PhysicalSubscription | PhysicalConsumerGroup


@dataclass(frozen=True, slots=True)
class KafkaBindingSettings:
    binding_id: str
    engine_profile: str
    engine_version: str
    bootstrap_servers: str = field(repr=False)
    client_id: str
    cursor_ttl_ms: int
    sasl_mechanism: str | None
    transactional_id: str | None = field(repr=False)
    read_committed: bool = True
    auto_offset_reset: str = "earliest"
    session_timeout_ms: int = 45_000
    max_poll_interval_ms: int = 300_000
    allow_plaintext_for_testing: bool = False
    resources: Mapping[ResourceRef, PhysicalResourceConfig] = field(
        default_factory=dict, repr=False
    )
    identity: KafkaIdentity = field(default_factory=lambda: KafkaIdentity("unknown"), repr=False)
    secrets: KafkaSecrets = field(default_factory=KafkaSecrets, repr=False)
    tls_mode: str = "disabled"
    tls_server_name: str | None = None
    tls_ca_pem: str | None = field(default=None, repr=False)
    tls_client_certificate_pem: str | None = field(default=None, repr=False)
    operation_timeout_ms: int = 30_000

    @classmethod
    def from_context(cls, context: AdapterCreateContext) -> KafkaBindingSettings:
        try:
            return cls._from_context(context)
        except KafkaConfigurationError:
            raise
        except (TypeError, ValueError, KeyError, UnicodeError, json.JSONDecodeError) as exc:
            raise KafkaConfigurationError("Kafka Binding settings are invalid") from exc

    @classmethod
    def _from_context(cls, context: AdapterCreateContext) -> KafkaBindingSettings:
        binding = context.binding
        if binding.adapter_id != ADAPTER_ID:
            raise KafkaConfigurationError("Binding selected an unexpected Adapter id")
        if binding.adapter_contract != ADAPTER_CONTRACT_VERSION:
            raise KafkaConfigurationError("Binding must pin Adapter contract 1.0.0 exactly")
        if binding.engine_profile not in {PRODUCTION_ENGINE_PROFILE, TEST_ENGINE_PROFILE}:
            raise KafkaConfigurationError("Binding selected an unsupported Kafka Engine profile")
        if binding.engine_version not in SUPPORTED_ENGINE_VERSIONS:
            raise KafkaConfigurationError("Binding selected an unsupported Kafka Engine version")
        bootstrap = binding.endpoint or binding.service_ref
        if bootstrap is None:
            raise KafkaConfigurationError("Kafka Binding requires an endpoint or service reference")
        bootstrap = bounded_string(bootstrap, "Kafka bootstrap servers", 2048)

        settings = closed_object(
            binding.settings,
            "Kafka Binding settings",
            required={
                "clientId",
                "cursorTtlMs",
                "saslMechanism",
                "transactionalId",
                "readCommitted",
                "autoOffsetReset",
                "sessionTimeoutMs",
                "maxPollIntervalMs",
                "allowPlaintextForTesting",
                "resources",
            },
        )
        identity = _parse_identity(context.identity.reveal())
        secrets = _parse_secrets(context.credential.reveal())
        resources = _parse_resources(settings["resources"])
        sasl_raw = settings["saslMechanism"]
        if sasl_raw is not None and sasl_raw not in _SASL_MECHANISMS:
            raise KafkaConfigurationError("saslMechanism is not supported")
        sasl_mechanism = cast(str | None, sasl_raw)
        transactional_raw = settings["transactionalId"]
        transactional_id = (
            None
            if transactional_raw is None
            else _kafka_name(transactional_raw, "transactionalId", 255)
        )
        auto_offset_reset = cast(str, settings["autoOffsetReset"])
        if auto_offset_reset not in {"earliest", "latest"}:
            raise KafkaConfigurationError("autoOffsetReset must be earliest or latest")
        allow_plaintext = _boolean(settings["allowPlaintextForTesting"], "allowPlaintextForTesting")
        if binding.engine_profile == PRODUCTION_ENGINE_PROFILE:
            if binding.tls.mode == "disabled":
                raise KafkaConfigurationError("the production Kafka profile requires TLS")
            if binding.tls.mode != "mutual" and sasl_mechanism is None:
                raise KafkaConfigurationError(
                    "the production Kafka profile requires mTLS or SASL authentication"
                )
            if allow_plaintext:
                raise KafkaConfigurationError(
                    "the production Kafka profile forbids the plaintext test override"
                )
        elif not allow_plaintext or binding.tls.mode != "disabled":
            raise KafkaConfigurationError(
                "apache-kafka-test requires disabled TLS and explicit plaintext opt-in"
            )
        if sasl_mechanism is not None and (identity.username is None or secrets.password is None):
            raise KafkaConfigurationError("SASL requires a resolved username and password")

        ca_pem = _secret_text(context.tls_ca.reveal(), "TLS CA") if context.tls_ca else None
        certificate_pem = (
            _secret_text(context.tls_client_certificate.reveal(), "TLS client certificate")
            if context.tls_client_certificate
            else None
        )
        if binding.tls.mode in {"server", "mutual"} and (
            ca_pem is None or "BEGIN CERTIFICATE" not in ca_pem
        ):
            raise KafkaConfigurationError("authenticated TLS requires a PEM CA")
        if binding.tls.mode == "mutual":
            if certificate_pem is None or "BEGIN CERTIFICATE" not in certificate_pem:
                raise KafkaConfigurationError("mTLS requires a PEM client certificate")
            if secrets.private_key_pem is None or "PRIVATE KEY" not in secrets.private_key_pem:
                raise KafkaConfigurationError("mTLS requires a PEM private key in secretRef")

        return cls(
            binding_id=binding.id,
            engine_profile=binding.engine_profile,
            engine_version=binding.engine_version,
            bootstrap_servers=bootstrap,
            client_id=bounded_string(settings["clientId"], "clientId", 255),
            cursor_ttl_ms=_positive(settings["cursorTtlMs"], "cursorTtlMs", 31_536_000_000),
            sasl_mechanism=sasl_mechanism,
            transactional_id=transactional_id,
            read_committed=_boolean(settings["readCommitted"], "readCommitted"),
            auto_offset_reset=auto_offset_reset,
            session_timeout_ms=_positive(
                settings["sessionTimeoutMs"], "sessionTimeoutMs", 3_600_000
            ),
            max_poll_interval_ms=_positive(
                settings["maxPollIntervalMs"], "maxPollIntervalMs", 86_400_000
            ),
            allow_plaintext_for_testing=allow_plaintext,
            resources=MappingProxyType(resources),
            identity=identity,
            secrets=secrets,
            tls_mode=binding.tls.mode,
            tls_server_name=binding.tls.server_name,
            tls_ca_pem=ca_pem,
            tls_client_certificate_pem=certificate_pem,
            operation_timeout_ms=binding.client.operation_timeout_ms,
        )

    @property
    def transaction_enabled(self) -> bool:
        return self.transactional_id is not None and self.read_committed

    @property
    def active_cursor_key(self) -> TokenKey:
        return self.secrets.active_cursor_key

    def resource(self, ref: ResourceRef) -> PhysicalResourceConfig:
        try:
            return self.resources[ref]
        except KeyError as exc:
            raise KafkaConfigurationError(f"Resource {ref} has no Kafka Binding mapping") from exc

    def stream(self, ref: ResourceRef) -> PhysicalStream:
        selected = self.resource(ref)
        if not isinstance(selected, PhysicalStream):
            raise KafkaConfigurationError(f"Resource {ref} is not a Stream mapping")
        return selected

    def subscription(self, ref: ResourceRef) -> PhysicalSubscription:
        selected = self.resource(ref)
        if not isinstance(selected, PhysicalSubscription):
            raise KafkaConfigurationError(f"Resource {ref} is not a Subscription mapping")
        return selected

    def consumer_group(self, ref: ResourceRef) -> PhysicalConsumerGroup:
        selected = self.resource(ref)
        if not isinstance(selected, PhysicalConsumerGroup):
            raise KafkaConfigurationError(f"Resource {ref} is not a ConsumerGroup mapping")
        return selected

    def group_stream(
        self,
        subscription_ref: ResourceRef,
        group_ref: ResourceRef,
    ) -> tuple[PhysicalSubscription, PhysicalConsumerGroup, PhysicalStream]:
        subscription = self.subscription(subscription_ref)
        group = self.consumer_group(group_ref)
        if group.subscription != subscription.ref:
            raise KafkaConfigurationError(
                "ConsumerGroup mapping belongs to another Subscription Resource"
            )
        return subscription, group, self.stream(subscription.stream)

    def redacted_summary(self) -> Mapping[str, object]:
        return {
            "bindingId": self.binding_id,
            "engineProfile": self.engine_profile,
            "engineVersion": self.engine_version,
            "clientId": self.client_id,
            "tlsMode": self.tls_mode,
            "saslMechanism": self.sasl_mechanism,
            "transactionEnabled": self.transaction_enabled,
            "resourceCount": len(self.resources),
        }

    def client_configuration(self) -> KafkaClientConfiguration:
        return KafkaClientConfiguration(self)


class KafkaClientConfiguration:
    """Create librdkafka dictionaries while never exposing them in repr output."""

    __slots__ = ("_settings",)

    def __init__(self, settings: KafkaBindingSettings) -> None:
        self._settings = settings

    def __repr__(self) -> str:
        return f"KafkaClientConfiguration({dict(self._settings.redacted_summary())!r})"

    def common(self) -> dict[str, KafkaClientConfigValue]:
        settings = self._settings
        if settings.tls_mode == "disabled":
            protocol = "SASL_PLAINTEXT" if settings.sasl_mechanism else "PLAINTEXT"
        else:
            protocol = "SASL_SSL" if settings.sasl_mechanism else "SSL"
        result: dict[str, KafkaClientConfigValue] = {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": settings.client_id,
            "security.protocol": protocol,
            "socket.timeout.ms": settings.operation_timeout_ms,
            "statistics.interval.ms": 10_000,
        }
        if settings.sasl_mechanism:
            username = settings.identity.username
            password = settings.secrets.password
            if username is None or password is None:
                raise KafkaConfigurationError("resolved SASL material is unavailable")
            result.update(
                {
                    "sasl.mechanism": settings.sasl_mechanism,
                    "sasl.username": username,
                    "sasl.password": password,
                }
            )
        if settings.tls_mode != "disabled":
            if settings.tls_ca_pem is None:
                raise KafkaConfigurationError("resolved TLS CA is unavailable")
            result.update(
                {
                    "enable.ssl.certificate.verification": True,
                    "ssl.endpoint.identification.algorithm": "https",
                    "ssl.ca.pem": settings.tls_ca_pem,
                }
            )
        if settings.tls_mode == "mutual":
            certificate = settings.tls_client_certificate_pem
            private_key = settings.secrets.private_key_pem
            if certificate is None or private_key is None:
                raise KafkaConfigurationError("resolved mTLS material is unavailable")
            result.update(
                {
                    "ssl.certificate.pem": certificate,
                    "ssl.key.pem": private_key,
                }
            )
            if settings.secrets.private_key_password:
                result["ssl.key.password"] = settings.secrets.private_key_password
        return result

    def producer(self, *, transactional: bool = False) -> dict[str, KafkaClientConfigValue]:
        result = self.common()
        result.update(
            {
                "enable.idempotence": True,
                "acks": "all",
                "retries": 2_147_483_647,
                "max.in.flight.requests.per.connection": 5,
                "delivery.timeout.ms": self._settings.operation_timeout_ms,
                "request.timeout.ms": self._settings.operation_timeout_ms,
                "message.send.max.retries": 2_147_483_647,
            }
        )
        if transactional:
            if not self._settings.transaction_enabled:
                raise KafkaConfigurationError(
                    "transactional producer requested without a valid transactional Binding"
                )
            transactional_id = self._settings.transactional_id
            if transactional_id is None:
                raise KafkaConfigurationError("resolved transactional id is unavailable")
            result["transactional.id"] = transactional_id
        return result

    def consumer(
        self, group_id: str, *, range_reader: bool = False
    ) -> dict[str, KafkaClientConfigValue]:
        result = self.common()
        result.update(
            {
                "group.id": group_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": self._settings.auto_offset_reset,
                "isolation.level": (
                    "read_committed" if self._settings.read_committed else "read_uncommitted"
                ),
                "session.timeout.ms": self._settings.session_timeout_ms,
                "max.poll.interval.ms": self._settings.max_poll_interval_ms,
                "enable.partition.eof": range_reader,
            }
        )
        return result

    def admin(self) -> dict[str, KafkaClientConfigValue]:
        return self.common()


def _parse_identity(value: bytes) -> KafkaIdentity:
    text = _secret_text(value, "identity")
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        return KafkaIdentity(principal=bounded_string(text, "identity principal", 512))
    item = closed_object(decoded, "identity secret", required={"principal", "username"})
    principal = bounded_string(item["principal"], "identity principal", 512)
    username = item["username"]
    if username is not None:
        username = bounded_string(username, "SASL username", 512)
    return KafkaIdentity(principal, username)


def _parse_secrets(value: bytes) -> KafkaSecrets:
    text = _secret_text(value, "credential")
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError as exc:
        raise KafkaConfigurationError(
            "credential secret must be a closed JSON object with a cursor key ring"
        ) from exc
    item = closed_object(
        decoded,
        "credential secret",
        required={"password", "privateKeyPem", "privateKeyPassword", "cursorKeys"},
    )
    raw_keys = item["cursorKeys"]
    if not isinstance(raw_keys, Sequence) or isinstance(raw_keys, str | bytes):
        raise KafkaConfigurationError("cursorKeys must be an array")
    if not 1 <= len(raw_keys) <= _MAX_CURSOR_KEYS:
        raise KafkaConfigurationError("cursorKeys must contain between one and four keys")
    keys: list[TokenKey] = []
    for index, raw in enumerate(raw_keys):
        key_item = closed_object(
            raw,
            f"cursorKeys[{index}]",
            required={"id", "keyBase64", "active"},
        )
        encoded = bounded_string(key_item["keyBase64"], "cursor key material", 256)
        try:
            key_bytes = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise KafkaConfigurationError("cursor key material must be canonical base64") from exc
        keys.append(
            TokenKey(
                bounded_string(key_item["id"], "cursor key id", 64),
                key_bytes,
                _boolean(key_item["active"], "cursor key active"),
            )
        )
    if sum(item.active for item in keys) != 1:
        raise KafkaConfigurationError("cursorKeys requires exactly one active key")
    if len({item.key_id for item in keys}) != len(keys):
        raise KafkaConfigurationError("cursor key ids must be unique")

    def optional_secret(name: str, maximum: int) -> str | None:
        raw = item[name]
        return None if raw is None else bounded_string(raw, name, maximum)

    return KafkaSecrets(
        password=optional_secret("password", 4096),
        private_key_pem=optional_secret("privateKeyPem", 64 * 1024),
        private_key_password=optional_secret("privateKeyPassword", 4096),
        cursor_keys=tuple(keys),
    )


def _secret_text(value: bytes, field_name: str) -> str:
    if not value or len(value) > 1024 * 1024:
        raise KafkaConfigurationError(f"{field_name} secret has an invalid size")
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise KafkaConfigurationError(f"{field_name} secret must be UTF-8") from exc


def _parse_resources(value: object) -> dict[ResourceRef, PhysicalResourceConfig]:
    raw = as_object(value, "resources")
    result: dict[ResourceRef, PhysicalResourceConfig] = {}
    stream_topics: set[str] = set()
    group_ids: set[str] = set()
    for logical_name, document in raw.items():
        ref = _resource_ref(logical_name, "resource map key")
        item = as_object(document, f"resources[{logical_name!r}]")
        kind = item.get("kind")
        if kind == "stream":
            selected = closed_object(
                item,
                f"resources[{logical_name!r}]",
                required={
                    "kind",
                    "topic",
                    "schemaRef",
                    "schemaFingerprint",
                    "resourceFingerprint",
                    "logicalPartitions",
                    "retentionMs",
                    "compacted",
                },
                optional={"compatibleSchemaFingerprints"},
            )
            topic = _kafka_name(selected["topic"], "topic")
            if topic in stream_topics:
                raise KafkaConfigurationError("Stream topic mappings must be unique")
            stream_topics.add(topic)
            raw_compatible = selected.get("compatibleSchemaFingerprints", ())
            if not isinstance(raw_compatible, Sequence) or isinstance(raw_compatible, str | bytes):
                raise KafkaConfigurationError("compatibleSchemaFingerprints must be an array")
            schema_fingerprint = _fingerprint(selected["schemaFingerprint"], "schemaFingerprint")
            schema_ref = _schema_ref(selected["schemaRef"], "schemaRef")
            if schema_ref.namespace != ref.namespace:
                raise KafkaConfigurationError(
                    "Stream and Schema mappings must share a logical Namespace"
                )
            compatible = tuple(
                sorted(
                    {
                        schema_fingerprint,
                        *(
                            _fingerprint(item, "compatibleSchemaFingerprint")
                            for item in raw_compatible
                        ),
                    }
                )
            )
            resource: PhysicalResourceConfig = PhysicalStream(
                ref=ref,
                topic=topic,
                schema_ref=schema_ref,
                schema_fingerprint=schema_fingerprint,
                compatible_schema_fingerprints=compatible,
                resource_fingerprint=_fingerprint(
                    selected["resourceFingerprint"], "resourceFingerprint"
                ),
                logical_partitions=_positive(
                    selected["logicalPartitions"], "logicalPartitions", 1_000_000
                ),
                retention_ms=_positive(selected["retentionMs"], "retentionMs", 10**15),
                compacted=_boolean(selected["compacted"], "compacted"),
            )
        elif kind == "subscription":
            selected = closed_object(
                item,
                f"resources[{logical_name!r}]",
                required={
                    "kind",
                    "stream",
                    "resourceFingerprint",
                    "acknowledgementTimeoutMs",
                    "maxDeliveryAttempts",
                    "deadLetterStream",
                    "filter",
                },
            )
            filter_value = as_object(selected["filter"], "subscription filter")
            dead_letter = selected["deadLetterStream"]
            resource = PhysicalSubscription(
                ref=ref,
                stream=_resource_ref(selected["stream"], "subscription stream"),
                resource_fingerprint=_fingerprint(
                    selected["resourceFingerprint"], "resourceFingerprint"
                ),
                acknowledgement_timeout_ms=_positive(
                    selected["acknowledgementTimeoutMs"],
                    "acknowledgementTimeoutMs",
                    86_400_000,
                ),
                max_delivery_attempts=_positive(
                    selected["maxDeliveryAttempts"], "maxDeliveryAttempts", 10_000
                ),
                dead_letter_stream=(
                    None if dead_letter is None else _resource_ref(dead_letter, "deadLetterStream")
                ),
                filter=MappingProxyType(dict(filter_value)),
            )
        elif kind == "consumer-group":
            selected = closed_object(
                item,
                f"resources[{logical_name!r}]",
                required={"kind", "subscription", "groupId", "resourceFingerprint"},
            )
            group_id = _kafka_name(selected["groupId"], "groupId", 255)
            if group_id in group_ids:
                raise KafkaConfigurationError("ConsumerGroup physical mappings must be unique")
            group_ids.add(group_id)
            resource = PhysicalConsumerGroup(
                ref=ref,
                subscription=_resource_ref(selected["subscription"], "group subscription"),
                group_id=group_id,
                resource_fingerprint=_fingerprint(
                    selected["resourceFingerprint"], "resourceFingerprint"
                ),
            )
        else:
            raise KafkaConfigurationError("resource mapping has an unknown kind")
        if ref in result:
            raise KafkaConfigurationError("resource mapping contains a duplicate reference")
        result[ref] = resource
    if not result or not any(isinstance(item, PhysicalStream) for item in result.values()):
        raise KafkaConfigurationError("Kafka Binding requires at least one Stream mapping")
    for resource in result.values():
        if isinstance(resource, PhysicalSubscription):
            stream = result.get(resource.stream)
            if not isinstance(stream, PhysicalStream):
                raise KafkaConfigurationError("Subscription stream mapping is missing")
            if resource.dead_letter_stream is not None:
                dead_letter = result.get(resource.dead_letter_stream)
                if not isinstance(dead_letter, PhysicalStream):
                    raise KafkaConfigurationError("dead-letter Stream mapping is missing")
                if dead_letter.ref == stream.ref:
                    raise KafkaConfigurationError("dead-letter target must be another Stream")
            if resource.filter:
                raise KafkaConfigurationError(
                    "V1 rejects Subscription filters requiring client-side scanning"
                )
        if isinstance(resource, PhysicalConsumerGroup):
            subscription = result.get(resource.subscription)
            if not isinstance(subscription, PhysicalSubscription):
                raise KafkaConfigurationError("ConsumerGroup Subscription mapping is missing")
    return result


__all__ = [
    "KafkaBindingSettings",
    "KafkaClientConfiguration",
    "KafkaIdentity",
    "KafkaSecrets",
    "PhysicalConsumerGroup",
    "PhysicalResourceConfig",
    "PhysicalStream",
    "PhysicalSubscription",
    "TokenKey",
]

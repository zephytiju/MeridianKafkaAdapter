# Security policy

## Supported versions

Security fixes are applied to the latest released minor line. Version 1.0.x
supports Python 3.12–3.14, confluent-kafka 2.15.0, and the Kafka broker matrix
in `docs/compatibility.md`.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use GitHub's private
security-advisory workflow for this repository and include the affected
version, minimal reproduction, impact, and any suggested mitigation. Secret
bytes, credentials, certificates, endpoints, physical topic names, group IDs,
or opaque Meridian tokens must not be included in public reports.

## Runtime boundary

The adapter resolves secrets only through the Core `SecretResolver` boundary.
Secret values are redacted from string representations, exceptions, telemetry,
probe evidence, physical verification, and operation provenance. Production
Bindings require TLS. Plaintext is accepted only by the isolated
`apache-kafka-test` conformance profile when the Binding opts in explicitly.

The runtime principal has data-plane permissions only. Topic, ACL, partition,
retention, compaction, credential, controller, and cluster lifecycle changes
belong to the separately authorized IaC or migration identity.

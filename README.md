# MeridianKafkaAdapter

[![CI](https://github.com/zephytiju/MeridianKafkaAdapter/actions/workflows/ci.yml/badge.svg)](https://github.com/zephytiju/MeridianKafkaAdapter/actions/workflows/ci.yml)
[![Cluster conformance](https://github.com/zephytiju/MeridianKafkaAdapter/actions/workflows/cluster-conformance.yml/badge.svg)](https://github.com/zephytiju/MeridianKafkaAdapter/actions/workflows/cluster-conformance.yml)
[![PyPI](https://img.shields.io/pypi/v/meridian-storage-kafka.svg)](https://pypi.org/project/meridian-storage-kafka/)
[![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)

`meridian-storage-kafka` is the Apache Kafka engine adapter for the released
Meridian `streaming` Catalog. This public repository owns exactly this one
Python distribution and contributes only `meridian_storage.adapters.kafka`.

Applications never import this package or `confluent_kafka`. They use the
mapping-first `streaming` Catalog methods from `meridian-storage-streaming`:
`publish`, `publish_batch`, `subscribe`, `poll`, `acknowledge`,
`negative_acknowledge`, and `read_range`. Replay and ConsumerGroup position
changes remain explicit versioned Operations. Deployment IaC installs and
selects this adapter through a closed Meridian Binding.

## Compatibility

Release 1.1.0 validates Kafka protocol/API capabilities independently of broker
release numbers. Deployment selects exact package/image locks; historical tested
broker versions are conformance records, not startup allowlists. Untested
combinations remain unverified.

| Dependency | Public API compatibility bounds | Exact validation recipe |
|---|---|---|
| Core | >=1.1.0,<2 | 1.1.0 |
| Semantics | >=2.0.1,<3 | 2.0.1 |
| Streaming | >=1.0.1,<2 | 1.0.1 |
| confluent-kafka | >=2.15.0,<3 | 2.15.0 |
| Python | >=3.12 | 3.12, 3.13, 3.14 |

See [compatibility and migration](docs/compatibility.md) for real protocol
requirements, selected/observed provenance, deployment integrity checks and
[release validation](docs/release-validation.md) for exact verified combinations.

## Install

Install at the deployment boundary with its own resolved, hashed lock. The
repository's exact regression recipe is `requirements-audit.txt`:

```bash
python -m pip install meridian-storage-kafka==1.1.0 -c requirements-audit.txt
```

Core discovers the immutable `meridian.kafka` factory through the
`meridian_storage.adapters` entry-point group. A Binding supplies opaque
identity, credential, TLS, topic, group, schema, Capability, and physical
fingerprints. Startup performs authenticated probes and read-only physical
verification; it never creates topics, changes partitions or retention,
installs ACLs, or owns broker/controller lifecycle.

## Guarantees

- At-least-once delivery by default, with ordering only within one logical
  partition.
- Opaque, authenticated Cursors, positions, and delivery tokens; raw Kafka
  topics, partitions, offsets, groups, and generations are never public Data.
- Monotonic safe-position acknowledgement, negative acknowledgement and
  redelivery, explicit dead-letter routing, and retained finite range reads.
- Idempotent Kafka production. Transactional consume-publish is advertised
  only when its single-Binding and committed-read preconditions are enabled.
- Stable Meridian failures with Kafka details retained only as redacted
  adapter diagnostics.
- Generic audit, lineage, telemetry, lag, rebalance, transaction, and probe
  evidence through the configured composition boundary.

## Authority boundary

Platform or Vangu IaC through MeridianConstructs owns engine selection,
provisioning or external reference, state, identities, secrets, ACLs, topic and
partition migrations, retention, compaction, recovery, and broker/controller
lifecycle. This runtime validates those outputs. It has no infrastructure
mutation API and no `NativeQuery` surface.

See [docs/architecture.md](docs/architecture.md),
[docs/operations.md](docs/operations.md), and
[docs/security-and-lifecycle.md](docs/security-and-lifecycle.md).

## Development and conformance

```bash
python -m pip install -e '.[test]'
ruff check .
ruff format --check .
mypy src scripts conformance/scripts tests
pytest -q -m 'not cluster'
python conformance/scripts/run_cluster.py --kafka-version 4.3.1 --full
python conformance/scripts/run_cluster.py --kafka-version 4.2.1 --full
python conformance/scripts/run_cluster.py --kafka-version 4.1.2 --full
```

The primary real-cluster profile runs the full acceptance matrix against
Apache Kafka 4.3.1. Compatibility profiles repeat portable semantics against
4.1.2 and 4.2.1. Cluster evidence is emitted as a deterministic JSON report and
attached to each release with its SPDX SBOM and provenance attestations.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

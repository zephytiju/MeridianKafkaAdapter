# Release 1.1.0 validation

Runtime changes are validated against a normally installed built wheel, using only public owning-package dependencies: Core 1.1.0, Semantics 2.0.1, Streaming 1.0.1 and confluent-kafka 2.15.0. `requirements-runtime-lock.txt` contains registry hashes; `compatibility.json` records the selected upstream wheel/sdist identities.

Local pre-PR evidence (Python 3.13.3): 94 unit/contract/security tests passed, 81.29% coverage; strict typing, lint, package-boundary checks, build/archive verification and dependency audit passed.

| Kafka | Image digest | Full suite |
|---|---|---|
| 4.0.2 | `apache/kafka@sha256:836cafdad9f4825880d7cf1d5a21202915ae2527bd0ef1c3600c526ed7814d1f` | 11 passed; zero skipped |
| 4.3.1 | `apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` | 11 passed; zero skipped |

Both clusters exercised TLS and hostname/authentication negatives, independently changed selected-release metadata, publish/finite consume, ordering, redelivery/acknowledgement, rebalance, cursor expiry, transactions, retention/compaction, migration and broker/controller loss/recovery. These are isolated single-node KRaft tests; they do not claim multi-node HA or arbitrary future compatibility.

CI runs the full suite for Kafka 4.0.2, 4.1.2, 4.2.1 and 4.3.1 from the built wheel. The tag workflow repeats every profile from its release wheel before trusted PyPI publication, attaches fresh conformance JSON (including installed wheel hashes), SPDX SBOM and Sigstore provenance, and publishes SHA256SUMS. Repository evidence for older 4.1.2/4.2.1 runs remains historical until regenerated; only new CI reports establish this release combination.

Public publication and release-run verification are recorded in the Feishu task after completion. They are not inferred from this pre-publication document. Migration rules and the exhaustive owned gate inventory are in [compatibility.md](compatibility.md).

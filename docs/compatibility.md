# Release selection and compatibility

Deployment chooses independently locked broker images and Python distributions.
The tested release lists in the descriptor and `compatibility.json` are historical
conformance metadata. Neither configuration parsing nor manifest construction
rejects a release because it is absent from that list. This does not establish
compatibility for an untested combination.

## Runtime contracts

Startup authenticates metadata, then requests ApiVersions v0 on a separate
bounded connection to **every advertised broker**, with the Binding's same trust
roots, mTLS material and/or SASL identity. It validates required API range
intersections in `probe/protocol.py`; there is no broker release inference or
fallback table. PLAIN and SCRAM-SHA-256/512 use Kafka SaslHandshake v1 and
SaslAuthenticate v0. SCRAM validates nonce, bounded iteration count and server
signature. SASL failure, missing API or disjoint protocol range fails startup.
Responses have bounded frames/counts, checked correlations and a shared deadline.

The required APIs cover record-batch v2, committed Fetch, retained offsets,
metadata, group coordination/commit/fetch and rebalances, idempotent production,
and read-only topic configuration. Transactional Bindings additionally require
transaction coordinator and transaction commit APIs. The existing transaction
bridge still initializes the producer through librdkafka before readiness.
Librdkafka independently negotiates the wire version used for data operations.
The probe never writes events or changes topics, ACLs, retention or topology.

API ranges establish protocol availability; real publish/consume/transaction,
authorization, rebalance and recovery tests establish behavior. ACLs, Schema,
partition count, retention, compaction, single-Binding transactions, committed
reads, opaque cursors and finite budgets retain their existing checks. Production
`apache-kafka` requires authenticated TLS; `apache-kafka-test` requires explicit
isolated plaintext opt-in. Provisioned and external clusters use the same
Adapter runtime; their IaC lifecycle remains in MeridianConstructs.

## Provenance, pins and migration from 1.0.1

`manifest.engineVersion` and `evidence.selectedEngineVersion` are deployment
selection, never a broker observation. Kafka ApiVersions contains no broker
implementation release: `AdapterProbe.observed_engine_version` is `None` and the
evidence states unavailable. Do not copy the selected version into observation.
An explicit `observedEngineVersion` expectation therefore fails Core validation
when the observation is unavailable.

Observed installed distributions populate `clientVersion`,
`coreDistributionVersion`, `semanticsVersion` and `streamingVersion`. The legacy
`coreVersion` field retains Core SPI contract 1.0.0 semantics. `driver` is the
client identity `confluent-kafka`; its distribution release is separate. Explicit
version pins compare to the deployment's own selected lock, not the historical
recipe. Missing/malformed configuration and Core fingerprint checks remain.
Physical hashes retain engine selection: changing a selected release may require
an explicit deployment/configuration update without implying incompatibility.

1.1.0 consumes the released Core 1.1.0 / Semantics 2.0.1 / Streaming 1.0.1 APIs.
Metadata uses justified major-contract compatibility bounds; the exact validated
recipe and public artifact hashes remain in `requirements-audit.txt` and
`compatibility.json`. Core 1.1.0 supplies the release-independent shared manifest;
Semantics 2.0.1 and Streaming 1.0.1 supply a normally installable graph. Client
2.15.0 supplies the existing typed, producer, consumer and transaction APIs.
Higher releases within those bounds need their own conformance evidence.

Serialized descriptor/config/manifest v1 shapes are unchanged. The driver value
and runtime provenance extensions change canonical hashes. Regenerate the
expected capability fingerprint from the selected public release combination,
update driver and distribution pins together, verify the physical configuration,
and roll out explicitly. Do not relabel old fingerprints or reuse old conformance
reports. Golden documents in `tests/fixtures` capture the new exact recipe.

## Gate inventory

| Owned path / variant | Classification and treatment |
|---|---|
| config, both production and test profiles | Removed broker list predicate; retain closed profile/TLS/auth, Schema and topology validation. |
| descriptor/constants, compatibility ledger | Historical tested releases only; no release membership gates. |
| probe/protocol, every advertised broker | Actual authenticated API/feature contract intersection, independent of release labels. |
| probe, physical verification | Observed installed distributions; selected broker provenance; honest unavailable broker release; preserve physical drift. |
| runtime/transactions/consumer/producer/compiler | Existing operation and behavioral contracts; startup API validation before readiness. |
| compatibility pins | Explicit deployment lock integrity; legacy Core contract pin remains a contract. |
| pyproject, boundary verifier | Public API dependency bounds; enforce one distribution and no sibling source. |
| audit lock / CI recipe | Exact reproducible test selection; no runtime release allowlist. |
| conformance runner / compose | Any well-formed selected release; resolve image to immutable digest before provisioning; actual installed dependency versions; fail on skipped required tests. |
| provisioned/external cluster | Shared Adapter path; provider-specific provisioning and helper inventory is owned by Constructs, not this repository. |

Protocol references: [Kafka protocol](https://kafka.apache.org/protocol/) and
[librdkafka feature discovery](https://github.com/confluentinc/librdkafka/blob/v2.15.0/INTRODUCTION.md#feature-discovery).

The protocol ceilings are the Adapter's implemented data-client wire contracts,
verified against librdkafka 2.15.0 [request builders](https://github.com/confluentinc/librdkafka/blob/v2.15.0/src/rdkafka_request.c),
[Produce](https://github.com/confluentinc/librdkafka/blob/v2.15.0/src/rdkafka_msgset_writer.c),
[Fetch](https://github.com/confluentinc/librdkafka/blob/v2.15.0/src/rdkafka_fetcher.c),
and [transaction offset commit](https://github.com/confluentinc/librdkafka/blob/v2.15.0/src/rdkafka_txnmgr.c).
Transactional offset commit requires protocol v3 for group-generation fencing.
A broker offering only incompatible wire ranges fails for that explicit API
reason, regardless of its implementation release label.

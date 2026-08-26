# Compatibility and support matrix

## Released dependency pins

| Contract | Pin | Evidence |
|---|---|---|
| Core | `meridian-storage-core==1.0.0` | wheel `6b8ebb70ee1a8467a96d668878a8eebf826c1c4b63b3832ae70f2c630a8ef4a1`; sdist `2c44d44569a380f44ea7f797e7fe623d0242fa79b6bc34606d6bad1bc53f2d5a` |
| Semantics | `meridian-storage-semantics==1.0.0` | wheel `76fced0bc083f145fad1949b85147a3563f3584f99994471693915a8ba1851ec`; sdist `02605909db5dc7ff22d4ae5e3ae1b3fe6c25a68e9ecaa4c3ead36082848d0311` |
| Streaming | `meridian-storage-streaming==1.0.0` | wheel `8fa802d1f4d69082b1bb2643856f82db9159ebe92fcd819aa529c143cd8d51eb`; sdist `a5b259c03ddf82dde8d1e6696e492a6c62f540151633b722107e8518c7cb5831` |
| Kafka client | `confluent-kafka==2.15.0` | Apache-2.0; librdkafka-backed typed client |

The Streaming manifest fingerprint is
`sha256:1a13fc3917164af1b192c13e7d29caa7c082540eb0d9cd7d57e23298db07bbf2`.
The released Streaming conformance fingerprint is
`sha256:f8aa3e2c092062d1c758722b9ab2d9388f0deedeabf214f45e6212dc29e7cbff`.
No sibling source tree or workspace linkage is supported.

Adapter 1.0.1 corrects evidence-only values in the 1.0.0 compatibility ledger;
the adapter contract and all runtime dependency pins remain unchanged.

## Apache Kafka brokers

| Broker | Status | Release gate |
|---|---|---|
| 4.1.2 | Supported | portable real-cluster conformance |
| 4.2.1 | Supported | portable real-cluster conformance |
| 4.3.1 | Supported, primary | full failure, security, migration, recovery, and telemetry matrix |
| 4.0.2 | Archived migration source only | documented migration input; runtime rejected |
| 3.9.2 | Archived migration source only | documented migration input; runtime rejected |

Versions not listed as supported fail startup descriptor validation. A Binding
pins one exact broker version and the authenticated probe must return that same
profile/version and the exact Capability fingerprint.

## Capability rules

The adapter advertises every released Streaming 1.0.0 requirement, explicit
replay and group-position Operations, idempotent production, dead-letter
routing, and authenticated health probes. Transactional consume-publish is
advertised only when the Binding supplies a stable transactional ID and uses
`read_committed` consumers. It is limited to one compatible Kafka Binding and
does not include database, object-store, HTTP, or other-engine effects.

The `apache-kafka` profile requires authenticated TLS and deny-by-default ACLs.
`apache-kafka-test` exists solely for isolated CI clusters and requires the
explicit `allowPlaintextForTesting` setting. A production deployment cannot
select that profile.

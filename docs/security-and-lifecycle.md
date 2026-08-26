# Security and lifecycle contract

## Identity and secrets

The Binding contains opaque `identityRef`, `secretRef`, CA, and optional client
certificate references. Core resolves them to redacted `SecretValue` objects
before factory creation. Credentials may contain a SASL password, an mTLS
private key, and a bounded cursor-key ring. No secret byte is copied into
Binding output, Capability evidence, telemetry, provenance, exceptions, or
tokens.

Supported production transports are TLS with mTLS or SASL PLAIN/SCRAM over
TLS. Hostname verification is enabled. The runtime principal receives only the
declared topic, group, and transactional-ID permissions. Cluster and ACL
administration require a separate identity.

Credential rotation is a drain-and-swap operation. New clients authenticate
and probe before the previous producer and consumers close. The active cursor
key changes while previous keys remain verification-only for their bounded
retention window. Rotation failure leaves the old clients active.

## Lifecycle ownership

MeridianConstructs plus the owning Platform or Vangu IaC stack controls:

- provisioned versus external cluster selection;
- broker/controller, network, storage, replication, and recovery lifecycle;
- topic names, partitions, retention, compaction, quotas, and ACLs;
- workload identity, secret issuance/rotation, and migration jobs;
- partition migration, group reset authorization, failover, and restoration.

The adapter controls only runtime clients, compilation, serialization,
delivery, acknowledgements, finite reads, probes, normalized errors, and
evidence. It exposes no topic-creation or cluster-administration method.

## Failure and recovery evidence

Release conformance interrupts the combined broker/controller only inside a
dedicated Docker cluster. The real-cluster matrix verifies publish, finite
consume, ordering, redelivery, acknowledgement recovery, group rebalance,
idempotent production, transactional consume-publish, dead letters, Cursor and
retention expiry, compaction, schema evolution, partition expansion migration,
recovery, credential rotation, ACL denial, and telemetry. Deterministic client
doubles additionally cover transaction abort/retry branches. Cleanup targets
only resources carrying the conformance name prefix and per-case identifier.

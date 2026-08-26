# Architecture

`meridian-storage-kafka` implements the released Core 1.0.0 Adapter SPI for
Operations normalized by `meridian-storage-streaming==1.0.0`.

```text
mapping-first Streaming Expression
  -> released StreamingCatalogProvider normalization
  -> Core Binding and Capability validation
  -> Kafka compiler
  -> producer / persistent group consumer / finite range reader
  -> normalized Meridian Data, provenance, and failures
```

The factory is discovered as `meridian.kafka`. It receives a closed
`BindingConfig` plus already-resolved secret values. The runtime opens shared
clients, performs an authenticated metadata probe, verifies every physical
mapping read-only, and then creates lightweight sessions. Non-transactional
sessions borrow runtime-owned clients. A transactional session serializes use
of one stable transactional producer and either commits produced records plus
staged group offsets or aborts both.

The compiler accepts only the released operation contracts and exact versions.
It resolves logical Resource references through the Binding map; it never
derives topic, group, or transactional identifiers in application code. Empty
subscription filters are supported in V1. A filter that would require
unbounded client-side scanning is rejected before consumption.

Cursor, position, and delivery coordinates are encoded with canonical JSON and
HMAC-SHA-256. Tokens include only logical references, physical-name
fingerprints, partition coordinates, group-assignment epoch where applicable,
expiry, and key ID. They do not contain a topic or group name. Rotation accepts
one active key and bounded previous verification keys.

The consumer keeps one group member per logical Subscription/ConsumerGroup
pair. Acknowledgement advances only the contiguous safe offset for each
partition, so acknowledging a later delivery cannot skip an earlier one.
Negative acknowledgement seeks the delivery again or, after the declared
attempt limit, publishes a failure envelope to the configured dead-letter
Stream and commits the source only after that publish succeeds.

Finite range and replay readers use direct partition assignment and never
commit a ConsumerGroup position. Group-position transition is a separate
compare-and-set Operation. Startup, Expression execution, and probes never
mutate topics, partitions, retention, compaction, ACLs, or cluster lifecycle.

# Operation mapping

| Released Operation | Kafka behavior | Result |
|---|---|---|
| `publish` | Validate and serialize one Event; produce with idempotence and `acks=all`. | `PublishReceipt` |
| `publish-batch` | Validate a bounded batch and produce every Event with per-key partition ordering. | Array of `PublishReceipt` |
| `subscribe` | Validate the pre-provisioned Stream/Subscription mapping and supported filter. | Subscription validation evidence |
| `poll` | Poll one persistent group member with auto-commit disabled. | Finite array of `Delivery` |
| `acknowledge` | Validate the delivery token and commit only the monotonic contiguous safe offset. | Acknowledgement evidence |
| `negative-acknowledge` | Seek for redelivery or route to a configured dead-letter Stream before source commit. | Redelivery/dead-letter evidence |
| `read-range` | Directly assign partitions and read a finite retained range without a group commit. | `RangePage` |
| `replay` | Read a finite retained range from an explicit opaque bound; never reset a group. | `RangePage` |
| `group-position` | Compare the current committed-position fingerprint and commit the explicit new opaque positions. | Previous/new fingerprints |
| `publish-schema` | Validate the pinned Schema mapping and migration state; never mutate a registry or topic. | Read-only deployment validation evidence |
| `create-resource` | Validate that IaC already provisioned the exact Resource mapping and fingerprint. | Read-only deployment validation evidence |

The optional `meridian.streaming.transactional-consume-publish@1.0.0`
Capability begins a Kafka transaction, produces bounded output records, stages
the consumed offsets with the active group metadata, and commits or aborts them
atomically. It does not add a Catalog convenience method.

<!-- SPDX-License-Identifier: Apache-2.0 -->
# Real-cluster conformance

The conformance profile runs only against an isolated, ephemeral Apache Kafka KRaft
container. It uses the official JVM image, SASL/PLAIN authentication, Kafka's
`StandardAuthorizer`, explicit ACLs, disabled topic auto-creation, and a one-node combined
broker/controller. Both isolated SASL plaintext and authenticated TLS listeners are exercised.
The runner generates an ephemeral certificate, resolves the selected image to
an immutable digest, and records actual installed distribution provenance.

Run the primary release matrix from the repository root:

```console
.venv/bin/python conformance/scripts/run_cluster.py --kafka-version 4.3.1 --full
.venv/bin/python conformance/scripts/run_cluster.py --kafka-version 4.2.1 --full
.venv/bin/python conformance/scripts/run_cluster.py --kafka-version 4.1.2 --full
```

The runner owns cluster provisioning and teardown, waits for authenticated readiness, executes
the released public contracts, and writes a deterministic evidence document under `evidence/`.
The destructive failure test is enabled only by `--full` and may stop the `broker` service; never
point it at a shared or production cluster.

Topic creation, ACLs, partition changes, and failure injection live in this harness because those
are Platform/Vangu IaC responsibilities. None of those operations is exposed by
`meridian-storage-kafka`.

The same full suite runs against previously unlisted Kafka 4.0.2. The runner
accepts any well-formed release coordinate and optional `--image` selection; it
does not declare untested combinations compatible. Every release run must use
`--full`: skipped required cases fail the evidence gate. External harnesses must
also supply KAFKA_TLS_BOOTSTRAP_SERVERS and KAFKA_TLS_CERT for TLS acceptance.

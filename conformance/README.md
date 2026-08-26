<!-- SPDX-License-Identifier: Apache-2.0 -->
# Real-cluster conformance

The conformance profile runs only against an isolated, ephemeral Apache Kafka KRaft
container. It uses the official JVM image, SASL/PLAIN authentication, Kafka's
`StandardAuthorizer`, explicit ACLs, disabled topic auto-creation, and a one-node combined
broker/controller. The plaintext transport is a test-only profile; production Bindings are
rejected unless they use authenticated TLS.

Run the primary release matrix from the repository root:

```console
.venv/bin/python conformance/scripts/run_cluster.py --kafka-version 4.3.1 --full
.venv/bin/python conformance/scripts/run_cluster.py --kafka-version 4.2.1
.venv/bin/python conformance/scripts/run_cluster.py --kafka-version 4.1.2
```

The runner owns cluster provisioning and teardown, waits for authenticated readiness, executes
the released public contracts, and writes a deterministic evidence document under `evidence/`.
The destructive failure test is enabled only by `--full` and may stop the `broker` service; never
point it at a shared or production cluster.

Topic creation, ACLs, partition changes, and failure injection live in this harness because those
are Platform/Vangu IaC responsibilities. None of those operations is exposed by
`meridian-storage-kafka`.

# Isolated end-to-end environment, 2026-09-17

Scope: copied course under audit-work/e2e-validation. The main audit-work/apache_spark_course repo and synced sources were not changed by this validation task.

- Docker engine available memory: 8,249,708,544 bytes (7.68 GiB).
- Explicit e2e-stepik-* containers, separate e2e-stepik-validation network and volumes.
- Only published port: 127.0.0.1:13000 -> Metabase 3000.
- Spark local[2], driver 1400m, Jupyter limit 3 GiB; original data volume: 10,000 clients and 200,000 transactions before injected duplicates.
- Each notebook executes with a fresh kernel. Executed copies and logs are stored in results, never written back into the source notebooks.
- MinIO and mc DockerHub release pulls failed; official Quay release mirrors succeeded and are pinned by digest in the isolated compose.

## Proven bootstrap defect and repair

Original Kafka service failed on a new named volume: appuser uid=1000 gid=1000 could not write bootstrap.checkpoint.tmp into root-owned /tmp/kraft-combined-logs. See kafka-start-failure.log.

The official apache/kafka:3.9.0 image's user is appuser (1000:1000); /tmp/kraft-combined-logs does not exist in its image. /var/lib/kafka/data is appuser:root. See kafka-image-ownership.log.

An initial one-shot ownership adjustment proved the diagnosis. A reproducible kafka-init service was then added to the isolated compose, using the same Kafka image with user 0:0, no network, only the data volume, and mkdir/chown of the mountpoint. Kafka depends on successful init.

The init service was validated against a second fresh volume, e2e-stepik-validation_kafka-bootstrap-data. Init exited 0; Kafka became healthy. Its mount was inspected to confirm it uses this fresh volume. The original e2e-stepik-validation_kafka-data volume was preserved. See compose-up-fresh-kafka-bootstrap-verified.log and kafka-fresh-bootstrap-health.log.
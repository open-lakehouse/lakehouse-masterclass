# Maintainer TODO (not course content)

This file tracks build/infra items for the people producing the course. It is not
part of the textbook and students never need it.

## Decision locked
- Native Iceberg end to end via Unity Catalog 0.7's writable Iceberg REST catalog.
  No Delta, no UniForm.

## Runtime verification, 2026-10-12

The whole course was followed as a student, chapter by chapter, on a Linux host with
Docker (16 cores, 156 GB RAM), and every code block in Chapters 2 to 13 was run. The
chapters were then corrected to match what actually works. Versions verified:
Spark 4.1.0 (Connect), Iceberg 1.12.0, hadoop-aws 3.4.2, Unity Catalog v0.7.0,
Kafka (Confluent 7.5.0), Airflow 3.3.2, MLflow 3.1.0, DuckDB 1.5.6, PySpark client
4.1.0, Python 3.12.

SeaweedFS: the first pass used the `chrislusf/seaweedfs:dev` image, because no
release had both fixes the course needs. SeaweedFS 4.49
shipped the session-token fix (seaweedfs#11694) but was never published as a Docker
image, so the chapters pin `chrislusf/seaweedfs:4.51` (4.50 is the first image with
both fixes). The course was replayed on 4.51 from an empty stack on 2026-10-26 and
every check passed with no other changes.

Unity Catalog: the first pass used an image built from the `v0.7.0` source tag. The
official `unitycatalog/unitycatalog:v0.7.0` image (published 2026-10-14, amd64 and
arm64, same commit `2b17f3b5`) was then swapped in and the course replayed from an
empty stack on 2026-10-14: catalog creation, Chapter 5's create, insert, schema
evolution, delete, time travel and rollback, ingest, the medallion on repeated runs
(Gold for 2026-01-05 matches the book exactly), streaming dedup, an Airflow run,
DuckDB matching Spark row for row, MLflow train and register, and data surviving
`docker compose down` + `up`. All passed with no chapter changes.

Verified end to end: S3 auth and round trip; Spark Connect jobs; catalog creation,
Iceberg create/insert/schema evolution/time travel/rollback through UC; idempotent
per-day ingest; the SDP medallion with tested cleaning rules; Kafka streaming into
Bronze with watermark dedup and checkpoint restart (no duplicates, no gaps); the
Airflow DAG and a sequential backfill; DuckDB reading Gold row-for-row identical to
Spark; MLflow tracking, registry, and aliases; `make` targets; data surviving
`docker compose down` + `up` in every stateful service.

Also added and runtime-verified during this pass:
- ZooKeeper (`zookeeper-data`, `zookeeper-log`) and Kafka (`kafka-data`) volumes.
  Without them the `orders` topic was lost on `down` + `up` and the restarted stream
  failed with `UnknownTopicOrPartitionException`; with them the topic survives.
- The streaming job sets shuffle partitions to 4, so the dedup state store holds 4
  partitions (`state/0/` in the checkpoint) instead of the default 200 tiny ones.

## Open items

Everything that can be fixed now is fixed. The rest waits on upstream releases or is
planned work, and each has an issue:

- **#6** Restore the Iceberg metadata-table queries (`.snapshots`, `.files`,
  `.history`) once a Unity Catalog release includes unitycatalog/unitycatalog#1984.
  The fix was merged 2026-10-23; 0.7.0 is still the latest release. Chapters 5 and 11
  explain the limitation and use `lakehouse/catalog.py`, `SHOW TBLPROPERTIES` and the
  `_file` column meanwhile.
- **#7** Relax Chapter 7's "`select`, not `withColumn`" rule once a Spark release fixes
  SPARK-59663 (fix under review in apache/spark#59100). Reproduced on stock Spark
  4.1.0, 4.1.3, 4.2.0 and main.
- **#8** Move to Spark 4.2 once `iceberg-spark-runtime-4.2_2.13` is on Maven Central
  (also recheck whether a newer MLflow image ships `boto3`; `v3.1.0` doesn't, so
  Chapter 11 builds a two-line image).
- **#9** The companion code repository with a git tag per chapter, described as
  planned in `COURSE.md`.

# Maintainer TODO (not course content)

This file tracks build/infra items for the people producing the course. It is not
part of the textbook and students never need it.

## Decision locked
- Native Iceberg end to end via Unity Catalog 0.7's writable Iceberg REST catalog.
  No Delta, no UniForm.

## Spark 4.3 move, verified 2026-11-06

The reference build (`open-lakehouse`, branch `spark-4.3`) was replayed chapter by
chapter from an empty stack. Versions: Spark 4.3.0 (Connect) in a course image built
from `docker/spark/Dockerfile` (`open-lakehouse-spark:4.3.0`): Iceberg Spark runtime
built from apache/iceberg#18749 at commit `b1eab64f8d30`, hadoop-aws 3.5.0, AWS SDK
bundle 2.56.0, analyticsaccelerator-s3 1.3.1, Python 3.12 in the image. Kafka
connector 4.3.0 is the only `spark.jars.packages` entry. PySpark client 4.3.0 with
`pandas<3`, Python 3.12. Unity Catalog v0.7.0, SeaweedFS 4.51, Airflow 3.3.2, MLflow
3.1.0, DuckDB 1.5.6. Gold for 2026-01-05 is unchanged.

Changes that came with it: deterministic Silver dedup (window, not `dropDuplicates`),
`spark.sql.session.timeZone UTC` on the server, SDP storage on
`s3a://lakehouse/_pipelines/medallion`, new DAGs paused at creation (Chapter 9
unpauses), and Chapter 9's section on overlapping refreshes, built from
`data/race.sh` and its log.

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
  4.1.0 through 4.3.0; the fix is not in v4.3.0.
- **#8** Drop the Iceberg build stage in `docker/spark/Dockerfile` once
  `iceberg-spark-runtime-4.3_2.13` is on Maven Central: it becomes one entry in
  `spark.jars.packages`, and Chapter 4's Step 1a shrinks to match. (Also recheck
  whether a newer MLflow image ships `boto3`; `v3.1.0` doesn't, so Chapter 11 builds
  a two-line image.)
- **#9** The companion code repository with a git tag per chapter, described as
  planned in `COURSE.md`.
- **#10** Metric views on the Iceberg catalog: Unity Catalog 0.7.0's Iceberg REST
  catalog has no view endpoints, so a metric view can't be stored there yet.

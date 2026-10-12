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

Two images in the chapters are not yet published, so verification used stand-ins:
- Unity Catalog `unitycatalog/unitycatalog:v0.7.0` was built locally from the
  `v0.7.0` source tag (`docker build -t unitycatalog/unitycatalog:v0.7.0 .`).
- SeaweedFS `chrislusf/seaweedfs:4.49` does not exist yet; verification used the
  `chrislusf/seaweedfs:dev` image built 2026-10-12 (see open item 2).

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

1. **Swap in the official Unity Catalog v0.7.0 image** when it is published. Confirm
   the real tag/namespace (the project has skipped tags before; v0.4.1 was never
   published). No chapter text should ever instruct building from source.

2. **SeaweedFS 4.49 must ship before the course works as written.** Two SeaweedFS
   behaviors matter, and no released version has both right:
   - S3A directory markers: through 4.05, `HEAD dir` (no slash) on a marker answers
     200 `application/x-directory`, so Hadoop S3A treats the parent as a file and every
     Spark file write (`FileOutputCommitter` rename, streaming checkpoints) fails with
     "destination parent is not a directory". Fixed by 4.12.
   - Static session tokens: Unity Catalog 0.7 only vends static keys together with a
     session token. 3.80 to 4.05 ignore the token; from 4.07 on it is rejected
     (`InvalidAccessKeyId`), so UC's own metadata writes fail.
   - Fix: seaweedfs/seaweedfs#11694 ("s3api: tolerate session tokens on statically
     configured credentials", merged 2026-10-10). Verified working on the `dev` image.
     When 4.49 (or whichever release first contains #11694) is out, confirm the tag
     and update `image:` in Chapter 3 if it differs.
   - With a release containing both fixes, the AWS SDK checksum env vars
     (`AWS_REQUEST_CHECKSUM_CALCULATION` / `AWS_RESPONSE_CHECKSUM_VALIDATION`) are
     not needed; they were only required against 3.80.

3. **Upstream bug candidates** (reproduced; workarounds are in the chapters):
   - **Unity Catalog 0.7: metadata tables fail.** `loadTable` for an identifier like
     `bronze.orders.snapshots` returns 404 `NoSuchNamespaceException` ("Schema not
     found: iceberg.bronze.orders"). The Iceberg REST spec defines LoadTable's 404 as
     `NoSuchTableException`, and Iceberg's client only falls back to metadata-table
     resolution on that type, so `.snapshots`, `.files`, `.history` are unusable from
     Spark. Workarounds in the course: `SHOW TBLPROPERTIES ... ('current-snapshot-id')`,
     the `_file` metadata column, and `lakehouse/catalog.py` (snapshot list via the REST
     loadTable call; worth keeping as a teaching tool even after a fix). When fixed,
     restore the metadata-table queries in Chapters 5 and 11.
   - **OSS SDP 4.1 (also 4.1.3) on Iceberg: MV reading an MV in one run is empty.**
     At run start SDP truncates every materialized view; the downstream flow's scan of
     the upstream table is planned against the truncated snapshot, so Gold reads an
     empty Silver and commits zero rows (only the first run after a drop works). The
     Spark log shows Gold scanning Silver's `delete` snapshot. Workaround in the course:
     refresh in two runs (`--refresh silver.orders`, then `--refresh gold.daily_revenue`)
     in the Makefile and as two DAG tasks. When fixed, restore the single
     `spark-pipelines run` in Chapters 7, 9, 12, 13.
   - **MLflow image** `ghcr.io/mlflow/mlflow:v3.1.0` ships without `boto3`; the course
     builds a two-line image. Recheck on the next MLflow pin.

4. **No Iceberg Spark runtime for Spark 4.2 yet** (`iceberg-spark-runtime-4.2_2.13`
   is not on Maven Central). Stay on Spark 4.1 until it exists, then re-verify
   (including whether the SDP issue above is fixed there).

5. **Companion code repository.** `COURSE.md` describes per-chapter git tags as
   planned. A verified reference build of the finished project exists from this run
   and could seed that repository.

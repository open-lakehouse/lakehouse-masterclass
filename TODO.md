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

3. **Upstream bugs** (root-caused; fixes drafted locally, not yet filed or pushed):
   - **Unity Catalog 0.7: metadata tables fail.** `loadTable` for an identifier like
     `bronze.orders.snapshots` returns 404 `NoSuchNamespaceException` ("Schema not
     found: iceberg.bronze.orders"). The Iceberg REST spec answers table routes with
     `NoSuchTableException`, and Iceberg's client only falls back to metadata-table
     resolution on that type, so `.snapshots`, `.files`, `.history` are unusable from
     Spark. With authorization on it fails earlier, while resolving securables.
     Fix: the Iceberg routes' exception handler reports SCHEMA_NOT_FOUND as
     TABLE_NOT_FOUND on table-scoped routes. It is prepared for upstream with tests (one
     with authorization on), and a patched image made `.snapshots`, `.files`
     and `.history` work from Spark end to end. Workarounds in the course meanwhile:
     `SHOW TBLPROPERTIES ... ('current-snapshot-id')`, the `_file` column, and
     `lakehouse/catalog.py` (keep that one as a teaching tool). When a UC release
     carries the fix, restore the metadata-table queries in Chapters 5 and 11.
   - **Spark 4.1 (also 4.1.3, same code on master): SDP over Spark Connect loses a
     flow's dependency when the flow uses `withColumn` / `withColumnRenamed` on a read
     of another pipeline dataset that already exists.** The Connect planner analyzes
     those nodes eagerly when the pipeline registers the flow, resolving the read to
     the catalog table, so SDP treats it as external: on every run after the first,
     both flows start together and the downstream one reads a just-truncated table
     (Gold empty, run "COMPLETED"). Reproduces without Iceberg (session catalog,
     Parquet). `drop`, `na.fill` and other eager transforms fail outright. A fix is
     prepared for upstream: pipeline flow plans are converted with eager analysis
     deferred, with a PythonPipelineSuite test that fails without it.
     Course workaround (not a hack, a real rule until the fix ships): Gold shapes its
     columns with `select`, not `withColumn`, so the single `spark-pipelines run` is
     correct on every run (verified across repeated runs and in Airflow). Chapter 7
     teaches the rule and the mechanism. When a Spark release carries the fix, soften
     the rule in Chapter 7 to a note about older versions.
   - **MLflow image** `ghcr.io/mlflow/mlflow:v3.1.0` ships without `boto3`; the course
     builds a two-line image. Recheck on the next MLflow pin.

4. **No Iceberg Spark runtime for Spark 4.2 yet** (`iceberg-spark-runtime-4.2_2.13`
   is not on Maven Central). Stay on Spark 4.1 until it exists, then re-verify
   (including whether the SDP issue above is fixed there).

5. **Companion code repository.** `COURSE.md` describes per-chapter git tags as
   planned. A verified reference build of the finished project exists from this run
   and could seed that repository.

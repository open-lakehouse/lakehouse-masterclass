# Maintainer TODO (not course content)

This file tracks build/infra items for the people producing the course. It is not
part of the textbook and students never need it.

## Unity Catalog image (the one to swap)
- Chapter 5 pulls `unitycatalog/unitycatalog:v0.7.0`, presented as a normal image
  pull. As of 2026-10 that exact image is **not yet published** on Docker Hub
  (only v0.6.0 is released, and its Iceberg REST catalog is read-only). The 0.7
  write endpoints (createTable / commit / updateTable) exist on the `v0.7.0` source
  tag.
- INTERIM, for building and testing the course only — do NOT put this in the
  chapters: build UC 0.7 from source and run that image locally:
    git clone https://github.com/unitycatalog/unitycatalog
    cd unitycatalog && git checkout v0.7.0
    # build the server + container per the repo's build docs (sbt), tag it
    # unitycatalog/unitycatalog:v0.7.0 so the Compose file in Ch5 finds it
- ACTION: the official 0.7 image is expected in ~a couple of weeks. When it lands,
  confirm the real tag/namespace (the project has skipped tags before — v0.4.1 was
  never published) and update the `image:` in Chapter 5 to the official container.
  No chapter text should ever instruct building from source.

## Decision locked
- Native Iceberg end to end via UC 0.7's writable IRC. No Delta, no UniForm.
  Keeping the course Delta-free is exactly why we wait for 0.7.

## Other open build items (from source-level verification, 2026-10)
- config/spark/spark-defaults.conf: Iceberg runtime + spark.sql.extensions +
  S3FileIO (SeaweedFS endpoint + path-style) + spark-sql-kafka + hadoop-aws.
- Define top-level `networks: lakehouse-network:` and attach every service.
- Ch8: align the streaming schema with orders_raw (or write a separate table).
- UC -> SeaweedFS: assume Spark's S3FileIO performs the data/metadata I/O
  (SeaweedFS endpoint + path-style set in spark-defaults.conf); UC only records
  the IRC commits. CONFIRM on the first real run.
## Native (no-Docker) verification run, 2026-10-11 (orchestrator, via sys_os_shell)
VERIFIED on this box:
- Stage 1 (Ch3 SeaweedFS 3.80): unauth -> 403; wrong key -> InvalidAccessKeyId;
  right key + path-style round-trip IDENTICAL. Auth model is correct.
- Stage 2 (Ch4 Spark 4.1 + Connect): spark.range(5) over sc://localhost:15002
  worked, and start-connect-server.sh came up WITHOUT an explicit
  --packages spark-connect. Ch4's drop of that package is correct.
FIXES CONFIRMED + APPLIED from the run:
- hadoop-aws was MISSING: a catalog/`s3a://` read throws
  ClassNotFoundException org.apache.hadoop.fs.s3a.S3AFileSystem. Added
  hadoop-aws:3.4.1 to spark-defaults.conf (Ch4). Needed by Ch6's s3a JSON read.
- AWS region: S3FileIO / AWS SDK v2 need a region even for SeaweedFS. Added
  AWS_REGION env to the Spark services + spark.sql.catalog.iceberg.s3.region (Ch4/5).
BLOCKED by RAM (environment ceiling, not a course bug):
- Stage 3 (native Iceberg write to SeaweedFS via S3FileIO) and Stage 4 (UC 0.7
  from source) OOM-kill on this ~8GB box (Spark+Iceberg too heavy). Could not
  complete at runtime here. Verify these on a >=16GB box, or via Docker once the
  official UC 0.7 image ships.
APPLIED 2026-10-11 (source-level, still not runtime-verified; confirm on first >=16GB / Docker run):
- Ch5: added Step 1b creating the UC catalog `iceberg` (POST .../catalogs). Renamed
  the warehouse/UC-catalog from `lakehouse` to `iceberg` so it matches the Spark
  catalog alias used everywhere (`iceberg.bronze.orders`), removing the alias vs
  warehouse-name mismatch. Changed `spark.sql.catalog.iceberg.warehouse = iceberg`.
- Ch5: curl paths fixed to `.../iceberg/v1/config?warehouse=iceberg` and
  `.../iceberg/v1/catalogs/iceberg/namespaces/bronze/tables` (IRC prefix form).
- Ch5: `.snapshots` now says 1 row after the first insert (CREATE makes no snapshot);
  Step 2 "what just happened" and the rollback pitfall reworded to match.
- Ch5: added a troubleshooting bullet for NoSuchCatalogException / storage-root.
- CONFIRM ON FIRST RUN: exact UC 0.7 catalog-create REST shape, the IRC prefix
  (`catalogs/iceberg`), and whether the catalog needs an explicit storage_root.

- Ch4: changed the Spark Connect mount from `./config/spark:/opt/spark/conf` (which
  would hide the image's other conf files) to a single-file overlay
  `./config/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf`.

(All source-level verification items now applied. Remaining work is a true runtime
run on a >=16GB / Docker-capable host, and swapping in the official UC 0.7 image
when it publishes.)

## Hands-on deepening pass, 2026-10-12 (Ch3-13) -- VERIFY THESE ON A REAL RUN
The build sections were deepened with complete artifacts, see-it-change inspection,
and real expected output. The following illustrative values/paths are NOT runtime-
verified (sandbox can't run Docker; Spark+Iceberg OOMs here). A verification session
should confirm each on a real >=16GB / Docker host and correct any that differ:
- Ch3: `docker compose ps` columns; `s3 mb`/`cp`/`ls` output text; recursive-listing
  key layout (object keys, no folder objects).
- Ch4: `docker compose ps` 4-service table; Spark master UI labels (Workers ALIVE,
  "Spark Connect server" running application); `spark.version` -> 4.1.0; the
  2M-row groupBy appearing as a job in the UI.
- Ch5: the EXACT object-storage layout of an Iceberg table (data/ Parquet +
  metadata/ metadata.json + snap-*.avro + *.avro), the metadata.json JSON shape,
  and that a 2nd insert adds a new metadata.json/snap/manifest while old ones remain.
- Ch6: `generate_orders.py` output; raw drop = part-*.json + _SUCCESS; Bronze count
  1000 then 1000 (idempotent); physical partition dir `_ingest_date=2026-01-05/`.
- Ch7: Bronze (~1000) vs Silver (fewer) counts; the "zero bad rows in Silver" query
  returns empty; Gold output shape.
- Ch8: Bronze count starts at 1000 (batch) and climbs (stream); checkpoint dir holds
  offsets/ commits/ metadata state/.
- Ch9: the filled-in `ingest_bronze` task runs; `airflow dags list-runs` state=success.
- Ch10: DuckDB output rows byte-for-byte match Spark's Gold rows.
- Ch11: `models:/orders-order-count/<version>` loads and `.predict()` returns a value.
- Ch12: an agent can operate the stack from the AGENTS.md; verify-the-agent query matches.
- Ch13: data survives `docker compose down` + `up -d` (Gold count unchanged); the
  Terraform snippet is illustrative only (not meant to apply).

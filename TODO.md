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
STILL TO APPLY (from source-level reading, not yet runtime-verified):
- Ch5: add a step creating the UC catalog `lakehouse` with a storage_root (UC
  ships only `unity`); fix curl paths to `.../iceberg/v1/config?warehouse=lakehouse`
  and `.../iceberg/v1/catalogs/lakehouse/namespaces/bronze/tables`.
- Ch5: `.snapshots` shows 1 row after the first insert (CREATE makes no snapshot).
- Ch4 Docker note: mounting ./config/spark over /opt/spark/conf replaces the
  image's whole conf dir; mount just spark-defaults.conf, or copy defaults in.

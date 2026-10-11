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
- Runtime end-to-end verification still pending: needs a Docker-capable box, or
  explicit permission to download and run the service binaries natively.

# Chapter 5: Tables and Catalog

This is the chapter where your lakehouse stops being "files in a bucket" and becomes a database. In Chapter 3 you made two decisions: where the bytes live (object storage) and how loose files become a table (the Iceberg table format). In Chapter 4 you got an engine (Spark) and a thin way to drive it (Spark Connect). Now you tie them together. You'll stand up Unity Catalog OSS as the single catalog every engine in your lakehouse talks to, create your first Iceberg table through it, load and query data, evolve a table's schema without rewriting a single data file, and the crowd-pleaser, time-travel: query your table as it looked in the past and roll back a bad write as if it never happened.

By the end you'll understand not just the commands but what Iceberg is actually doing underneath, because once you see the metadata tree, every "magic" feature (ACID, schema evolution, time travel) turns into something obvious.

![Progress: Tables and Catalog](../figures/ch05/fig-5.1-progress-tables.svg)

**Figure 5.1**. Progress map with **Tables and Catalog** highlighted.

## Learning objectives

By the end of this chapter you'll be able to:

- Explain what a catalog is, the three questions it answers, and why a lakehouse is nearly useless without one.
- Describe how Unity Catalog OSS exposes tables to any engine through an Iceberg REST catalog endpoint, and why that single endpoint matters so much.
- Create a namespace and an Iceberg table, insert rows, and query them through Spark Connect, and explain what happened in object storage each time.
- Read an Iceberg table's internal structure (its snapshots, its data files, its `metadata.json`) straight from the catalog, from Spark, and from the bucket.
- Evolve a table's schema (add, rename, drop, reorder columns) without rewriting data, and explain why that is cheap.
- Use snapshots to time-travel: query an older version, then roll a table back to undo a bad write.
- Diagnose the most common catalog and table errors you'll actually hit.

## The problem a catalog solves

Start with a scenario, because the catalog only makes sense once you feel the pain of not having one.

Imagine you skipped this chapter. You have Parquet files sitting in object storage at `s3a://lakehouse/warehouse/orders/`. You want to query them from Spark. You can: point Spark at that path and it reads the files. Fine.

Now a teammate wants to query the same data from DuckDB. Where do they point it? At the path, but they also need to know the schema, which columns are partitions, and (critically) which files are current versus leftover from a half-finished job that crashed last night. Now a third tool wants in. And now you ran a job that rewrote half the files: how does every reader know which version is "the table" right now, without reading a corrupt mix of old and new?

Every one of those questions is the reader guessing about state that lives outside the files. That guessing is exactly how data lakes rotted into "data swamps." The catalog is the fix: one authoritative service that every engine asks.

> **Catalog** *(canonical term)*: the service that maps human table names
> (`iceberg.bronze.orders`) to the metadata and files that physically make up the
> table, so any engine can find and read it consistently.

Concretely, a catalog answers three questions for any client, the same way every time:

1. **What tables exist?** (and how are they organized into namespaces?)
2. **What is this table's current schema and current snapshot?** (its shape right now, not a stale or half-written version)
3. **Where are its data files?** (the exact list, as of that current snapshot)

That's the difference between a swamp and a database. The files hold the data; the catalog holds the truth about the data.

![Catalog: names to metadata to files](../figures/ch05/fig-5.2-catalog-role.svg)

**Figure 5.2**. The catalog sits between engines and storage: a client asks the catalog for `iceberg.bronze.orders`, gets back the current metadata pointer, and then reads the data files directly from object storage. The catalog brokers where to look; the engine still does the actual reading.

### A quick note on naming: catalog, namespace, table

Iceberg names have three parts: `catalog.namespace.table`, for example `iceberg.bronze.orders`. Think of it like a filing system:

- **catalog** (`iceberg`): the top-level registry. We have one, named `iceberg`.
- **namespace** (`bronze`): a schema or folder grouping related tables. We'll use `bronze`, `silver`, and `gold` (the medallion tiers from Chapter 7).
- **table** (`orders`): the table itself.

If you've used `database.table` in a warehouse, a namespace is that middle level; Iceberg just lets you nest and organize them per catalog.

## One catalog for everything: Unity Catalog OSS

Here's a decision that shapes the entire rest of the course: we use Unity Catalog OSS as the catalog for the whole lakehouse. Not one catalog for Spark and a different one for your BI tool and a third for your notebooks. One catalog, for everything.

Why so emphatic? Because the single most valuable property of an open lakehouse, the thing a closed warehouse cannot give you, is that many different engines read the same tables with no copies and no drift. That's only possible if they share a catalog. Split your tables across two catalogs and you've quietly rebuilt the silos you were trying to escape.

The mechanism that makes this work is the Iceberg REST catalog (IRC), an open, standardized HTTP protocol for catalogs. Unity Catalog OSS implements it and exposes it at:

```
http://localhost:8081/api/2.1/unity-catalog/iceberg
```

One version detail matters here, because it's the reason we pin a recent Unity Catalog. Earlier UC releases exposed this endpoint read-only: engines could list and read tables through it, but not create or commit them. Unity Catalog OSS 0.7 added the write side of the Iceberg REST protocol (create table, commit, update, drop), which is what lets you create and write native Iceberg tables through the catalog rather than only read them. That single capability is why the whole course can stay native Iceberg end to end while keeping one catalog for everything. Pin 0.7 or newer.

Any engine that speaks the Iceberg REST protocol (Spark, DuckDB, Trino, DataFusion, and a growing list of others) points at that one URL and immediately sees the same tables, the same schemas, the same current snapshots. No per-engine metastore to keep in sync. No export step. No "let me copy that table into a format your tool understands."

> **Multi-engine / interoperability** *(canonical term)*: the same open tables, read
> by different engines, because they share one catalog and one open format.

You won't feel the full payoff until Chapter 10 (Serving), where you'll query these exact tables from a different engine and watch identical rows come back. But the decision is made here, in this chapter, when you create your first table in this one catalog.

### Under the hood: static config lives server-side (and why your client stays thin)

Remember `CANNOT_MODIFY_STATIC_CONFIG` from Chapter 4? This is where it pays off.

The catalog wiring (the Iceberg SQL extensions, the REST catalog URI, the warehouse name, the S3 settings Spark uses to reach object storage) is all static Spark configuration. It's set once, server-side, in the Connect server's `spark-defaults.conf`. Concretely, that wiring makes `iceberg` a REST catalog pointed at Unity Catalog's IRC endpoint:

```properties
# add to config/spark/spark-defaults.conf (the file you started in Chapter 4)
spark.sql.catalog.iceberg                      = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type                 = rest
spark.sql.catalog.iceberg.uri                  = http://unity-catalog:8080/api/2.1/unity-catalog/iceberg
spark.sql.catalog.iceberg.warehouse            = iceberg
# Iceberg writes its own data + metadata files to SeaweedFS, so give its S3 client
# the same endpoint and path-style setting:
spark.sql.catalog.iceberg.io-impl              = org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.iceberg.s3.endpoint          = http://seaweedfs:8333
spark.sql.catalog.iceberg.s3.path-style-access = true
spark.sql.catalog.iceberg.client.refresh-credentials-enabled = false
```

Read it top to bottom. The first four lines say "there is a catalog called `iceberg`, it speaks the REST protocol, it lives at this URL, and inside Unity Catalog it's the catalog named `iceberg`." Note the URI uses the in-network address `unity-catalog:8080` (the service name and container port from the Compose file), not `localhost:8081`, because the Connect server reaches the catalog over the Compose network. The `warehouse` is a Unity Catalog catalog name, not an object-storage path.

The next three lines are about bytes. When Spark writes a table, it writes the Parquet data files and the manifests itself, straight to SeaweedFS, through Iceberg's own S3 client (`S3FileIO`). That client needs the same endpoint and path-style addressing you set for Hadoop's S3A client in Chapter 4; it doesn't read the `fs.s3a.*` settings.

The last line needs a word of explanation, because without it every write fails. When Spark loads a table, Unity Catalog hands back short-term storage credentials for it ("credential vending") along with an address where the client can come back for fresh ones. Our local setup has no real token service, so Unity Catalog vends the static keys from its config, with no expiry time. Iceberg sees a refresh address, looks for the expiry, finds none, and gives up with `Invalid S3 Credentials: s3.session-token-expires-at-ms not set`. Turning off credential refresh tells Iceberg to simply use the keys it was given. Against real S3 with a token service, you would leave refresh on.

Who writes what, then? Spark writes the data files, the manifests, and every later `metadata.json`. Unity Catalog writes the very first `metadata.json` when a table is created, and records each commit (the pointer swap) after that. That's why Unity Catalog also needs a route to SeaweedFS, which you'll give it in a moment.

Why is all this server-side config good news rather than a limitation? Because it means your client code is gloriously boring. You connect over `sc://` and just write `spark.sql("CREATE TABLE ...")`, and the catalog is already wired, already authenticated, already pointed at the right storage. The platform owns the plumbing; you write logic. Every engineer who connects gets the same correct wiring for free, and nobody can accidentally point their session at the wrong catalog. Thin clients, central configuration: that's the Spark Connect philosophy from Chapter 4 doing real work.

## Build: create and query an Iceberg table

Time to make a real table. Spark should still be up from Chapter 4; now you add the catalog as the next service in the same `docker-compose.yml`.

**Step 1: add Unity Catalog OSS to your Compose file.** It joins the same `lakehouse-network`, so Spark reaches it by service name (`unity-catalog:8080`) and you reach it from your host on the published port (`localhost:8081`). Add it under `services:`, and add `uc-db:` to the top-level `volumes:` block next to `seaweedfs-data:`:

```yaml
  unity-catalog:
    image: unitycatalog/unitycatalog:v0.7.0   # 0.7+ exposes a writable Iceberg REST catalog
    depends_on:
      - seaweedfs
    environment:
      # Unity Catalog writes each new table's first metadata.json itself, so it needs
      # its own route to SeaweedFS (the AWS SDK reads this standard variable).
      AWS_ENDPOINT_URL_S3: http://seaweedfs:8333
    volumes:
      - ./config/unity-catalog/server.properties:/home/unitycatalog/etc/conf/server.properties
      - uc-db:/home/unitycatalog/etc/db   # the catalog's own database; without this it resets on recreate
    ports:
      - "8081:8080"      # host 8081 maps to container 8080
    networks:
      - lakehouse-network
```

```yaml
volumes:
  seaweedfs-data:
  uc-db:
```

Three things in that block are worth understanding before you move on.

- **The image is pinned to `v0.7.0`.** Earlier releases served the Iceberg REST endpoint read-only. 0.7 is the first with the write side, which is what everything below depends on.
- **`AWS_ENDPOINT_URL_S3`.** Unity Catalog is a Java service that uses the AWS SDK to talk to S3. Left alone, the SDK talks to Amazon. This variable is the SDK's standard way to say "the S3 service is over here instead," and it's how Unity Catalog finds SeaweedFS. Unity Catalog's S3 client always uses virtual-host addressing (`lakehouse.seaweedfs:8333`), which is exactly why Chapter 3 gave SeaweedFS the `-s3.domainName` flag and the `lakehouse.seaweedfs` network alias.
- **The `uc-db` volume.** Unity Catalog keeps its own records (which catalogs, schemas, and tables exist, and where each table's current metadata lives) in a small embedded database at `/home/unitycatalog/etc/db`. Without a volume there, that database lives inside the container, and the next time the container is recreated (an image change, a config change, `docker compose down` then `up`) Unity Catalog starts empty. Your Parquet and metadata files would still sit in the bucket, but nothing would point at them anymore: orphaned data. A catalog is state, so it gets a volume, just like the object store.

Now create `config/unity-catalog/server.properties`, which tells Unity Catalog how to reach your bucket. Fill the key and secret with the same values you put in `.env` (a properties file is read literally, so it does not expand `.env` for you):

```properties
# config/unity-catalog/server.properties
server.env=dev
server.authorization=disable
s3.bucketPath.0=s3://lakehouse
s3.region.0=us-east-1
s3.accessKey.0=lakehouse
s3.secretKey.0=lakehouse-secret
s3.sessionToken.0=local-static
```

Each line earns its place:

- **Mounting this file replaces the image's default `server.properties` entirely**, so the first two lines restore the two defaults that matter: development mode, and authorization off (fine for a laptop; Chapter 13's governance roadmap turns it on).
- **`s3.bucketPath.0` is the bucket root** (`s3://lakehouse`), not the warehouse prefix. Unity Catalog matches every table location against its configured buckets by bucket, so `s3://lakehouse/warehouse` here would never match and you'd get `S3 bucket configuration not found`.
- **`s3.region.0`** stops Unity Catalog from asking Amazon which region the bucket is in.
- **The keys and the session token.** Unity Catalog 0.7 only accepts static keys for a bucket when a session token is supplied with them; otherwise it silently ignores the whole bucket entry and expects to mint credentials from a real AWS token service. `local-static` is a placeholder. SeaweedFS accepts it alongside the configured keys, so the requests authenticate as your `lakehouse` identity.
- **What's missing:** there is no endpoint or path-style key in Unity Catalog's config. That's why the endpoint comes in through `AWS_ENDPOINT_URL_S3` and the addressing style through the SeaweedFS alias.

Bring it up and confirm it is healthy:

```bash
docker compose up -d unity-catalog
docker compose ps
```

Expected: `unity-catalog` shows up alongside `seaweedfs` and the Spark services, state `running`. It takes several seconds to start answering. Confirm it is live by listing the catalogs it already has; a fresh Unity Catalog ships with a single built-in catalog named `unity`:

```bash
curl -s http://localhost:8081/api/2.1/unity-catalog/catalogs
```

Expected (trimmed):

```json
{"catalogs":[{"name":"unity","comment":"Main catalog", ..., "storage_root":null, ...}],"next_page_token":null}
```

If the curl hangs or is refused, wait a few seconds and retry; if it still fails, check `docker compose logs unity-catalog` before continuing. Creating a table against a half-started catalog produces confusing errors.

Restart the Connect server too, so it picks up the catalog lines you added to `spark-defaults.conf`:

```bash
docker compose restart spark-connect
```

**Step 1b: create the `iceberg` catalog, with a storage root.** Your `spark-defaults.conf` points Spark at `warehouse = iceberg`, which is the name of a Unity Catalog *catalog*. That catalog doesn't exist yet (only `unity` does), so create it once. The `storage_root` tells Unity Catalog where tables created in this catalog should live; without it, the first `CREATE TABLE` fails because the catalog has nowhere to put the table:

```bash
curl -s -X POST http://localhost:8081/api/2.1/unity-catalog/catalogs \
  -H 'Content-Type: application/json' \
  -d '{"name": "iceberg", "comment": "The lakehouse catalog", "storage_root": "s3://lakehouse/warehouse"}'
```

Expected (trimmed; your ids differ):

```json
{"name":"iceberg","comment":"The lakehouse catalog", ...,
 "storage_root":"s3://lakehouse/warehouse",
 "storage_location":"s3://lakehouse/warehouse/__unitystorage/catalogs/c28baec3-..."}
```

Notice `storage_location`. Unity Catalog doesn't put tables at a path built from their names; it gives every catalog and table its own id-based folder under `__unitystorage/`. You'll see what that looks like in the bucket shortly.

Now confirm the Iceberg REST endpoint resolves the catalog. The protocol takes the catalog name as a `warehouse` query parameter:

```bash
curl -s "http://localhost:8081/api/2.1/unity-catalog/iceberg/v1/config?warehouse=iceberg"
```

Expected (trimmed):

```json
{"defaults":{},"overrides":{"prefix":"catalogs/iceberg"},"endpoints":["GET /v1/{prefix}/namespaces", ..., "POST /v1/{prefix}/namespaces/{namespace}/tables", ...]}
```

Two things to read there. The `prefix` (`catalogs/iceberg`) is what every later REST call is scoped under. And the `endpoints` list includes `POST .../tables`: that's the write side of the protocol, the thing 0.7 added. A read-only catalog wouldn't advertise it.

**Step 2: connect a thin client, create the namespaces, and create a table.** The catalog name `iceberg` and the REST wiring are configured server-side, so from the client you simply use them. Create all three medallion namespaces now, not just `bronze`. Spark Declarative Pipelines in Chapter 7 writes into `silver` and `gold` but won't create them for you, so they must exist before then:

```python
from lakehouse.session import get_spark

spark = get_spark()   # the factory from Chapter 4; one connection, defined once

# One namespace (schema) per medallion tier. Bronze now; Silver and Gold for Chapter 7.
for ns in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{ns}")
spark.sql("SHOW NAMESPACES IN iceberg").show()

# A first Iceberg table for raw orders.
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.bronze.orders (
        order_id    STRING,
        customer_id STRING,
        amount      DOUBLE,
        status      STRING,
        created_at  TIMESTAMP
    )
    USING iceberg
""")
```

Expected:

```
+---------+
|namespace|
+---------+
|   bronze|
|     gold|
|   silver|
+---------+
```

**What just happened?** Even before you inserted a single row, a `metadata.json` describing the table's schema was written to object storage, and the catalog recorded a pointer to it. This first metadata file is the one Unity Catalog writes itself, which is why it needed `AWS_ENDPOINT_URL_S3`. Note what it did *not* write: a snapshot. A snapshot records a specific set of data files, and a freshly created table has had no data operation yet, so it has none. The catalog now knows `iceberg.bronze.orders` exists, what shape it has, and where its metadata lives. The table is real; it just has no data, and no snapshots, yet.

**Step 3: insert a few rows and read them back:**

```python
spark.sql("""
    INSERT INTO iceberg.bronze.orders VALUES
        ('o-1001', 'c-1', 42.50, 'placed',    TIMESTAMP '2026-01-05 09:12:00'),
        ('o-1002', 'c-2', 19.99, 'placed',    TIMESTAMP '2026-01-05 09:15:30'),
        ('o-1003', 'c-1', 88.00, 'cancelled', TIMESTAMP '2026-01-05 09:20:10')
""")

spark.table("iceberg.bronze.orders").show()
```

Expected output:

```
+--------+-----------+------+---------+-------------------+
|order_id|customer_id|amount|   status|         created_at|
+--------+-----------+------+---------+-------------------+
|  o-1001|        c-1|  42.5|   placed|2026-01-05 09:12:00|
|  o-1002|        c-2| 19.99|   placed|2026-01-05 09:15:30|
|  o-1003|        c-1|  88.0|cancelled|2026-01-05 09:20:10|
+--------+-----------+------+---------+-------------------+
```

**What just happened?** That `INSERT` did not edit an existing file. Spark wrote new Parquet data files and a manifest straight to SeaweedFS, then wrote a new `metadata.json` declaring a fresh snapshot whose file list includes them, and asked Unity Catalog to move the table's pointer to it. The catalog's pointer moved atomically. This is the ACID guarantee in action: until that pointer flips, no reader sees the half-written state; the instant it flips, every reader sees the complete new version. There is never an in-between.

**Step 4: verify the catalog knows the table, independent of Spark.** Ask the Iceberg REST endpoint directly, with no Spark involved, to list the tables in the `bronze` namespace, under the `catalogs/iceberg` prefix the `config` call returned:

```bash
curl -s http://localhost:8081/api/2.1/unity-catalog/iceberg/v1/catalogs/iceberg/namespaces/bronze/tables
```

Expected:

```json
{"identifiers":[{"namespace":["bronze"],"name":"orders"}],"next-page-token":null}
```

Different client, same catalog, same table: a tiny preview of Chapter 10, where a completely different engine reads these exact rows.

## Look inside: Iceberg is a metadata tree

Most people use Iceberg for months treating it as a black box. Ten minutes of
looking inside will make everything else in this course click, so let's look.

An Iceberg table is **not** the data files alone. It's a small tree of metadata that
*points at* immutable data files:

- The **catalog** holds a pointer to the table's current **metadata file**.
- The **metadata file** (`metadata.json`) holds the schema and the list of all
  **snapshots**.
- Each **snapshot** points at a **manifest list**, which points at **manifests**,
  which finally list the actual **data files** (Parquet) that belong to that
  snapshot.

![Iceberg table anatomy and snapshots](../figures/ch05/fig-5.3-iceberg-anatomy.svg)

**Figure 5.3**. Iceberg's layers: the catalog points at the current metadata file, which lists manifests, which list data files. Each write adds a new snapshot without mutating the old ones.

The key insight: data files are immutable, and a write never edits them. A write adds new data files and a new snapshot that references the right set. Old snapshots keep pointing at the old file sets, untouched. That one design choice is what buys you three features at once:

- **ACID**: a write is "done" only when the current-snapshot pointer atomically flips. Readers always see one consistent snapshot.
- **Concurrent safety**: two writers producing new files and new snapshots don't stomp each other's data files.
- **Time travel**: old snapshots still exist and still point at valid files, so you can read the table "as of" any of them.

### Ask the catalog for the tree

You don't have to take this on faith. Iceberg normally exposes its internals as queryable *metadata tables*: append `.snapshots`, `.files`, or `.history` to a table name and query it like any table. With Unity Catalog 0.7 that convenience doesn't work yet, and it's worth seeing why, because it's a lesson in how engines and catalogs talk:

```python
spark.sql("SELECT * FROM iceberg.bronze.orders.snapshots").show()
# pyspark.errors.exceptions.connect.UnknownException:
#   (org.apache.iceberg.exceptions.NoSuchNamespaceException) Schema not found: iceberg.bronze.orders
```

To resolve `iceberg.bronze.orders.snapshots`, Spark first asks the catalog for a table named `snapshots` in a namespace named `bronze.orders`. The Iceberg REST spec says a catalog should answer "no such *table*," and Iceberg then retries it as "the `snapshots` metadata of table `bronze.orders`." Unity Catalog 0.7 answers "no such *namespace*" instead, so Spark never gets to the retry. Metadata tables work against catalogs that answer the way the spec says, and you'll see DuckDB's own `iceberg_snapshots()` read this tree in Chapter 10.

That's a small loss, because the information isn't hidden; it's one HTTP call away. The catalog serves a table's entire current `metadata.json` from its `loadTable` endpoint, the same response every engine reads before it plans a query. So read it yourself. First give your config the catalog's REST address, as seen from your host (add this to `lakehouse/config.py`):

```python
# The catalog's Iceberg REST endpoint as seen from wherever this code runs.
CATALOG_URI  = os.environ.get("CATALOG_URI", "http://localhost:8081/api/2.1/unity-catalog/iceberg")
```

Then add a small module that asks the catalog directly, `lakehouse/catalog.py`:

```python
# lakehouse/catalog.py
"""Ask the catalog directly, with no engine in between.

Unity Catalog serves the Iceberg REST protocol, so a table's full metadata (schema
history, snapshot list, current snapshot) is one HTTP GET away. This module is how
the course inspects snapshots: it reads exactly what every engine reads."""
import json
from datetime import datetime, timezone
from urllib.request import urlopen

from lakehouse.config import CATALOG, CATALOG_URI


def table_metadata(namespace: str, table: str) -> dict:
    """The table's current metadata.json, as served by the catalog's loadTable call."""
    url = f"{CATALOG_URI}/v1/catalogs/{CATALOG}/namespaces/{namespace}/tables/{table}"
    with urlopen(url) as resp:
        return json.load(resp)["metadata"]


def snapshots(namespace: str, table: str) -> list[dict]:
    """One row per snapshot, oldest first: id, commit time, operation, row counts."""
    meta = table_metadata(namespace, table)
    current = meta.get("current-snapshot-id")
    return [{
        "snapshot_id": s["snapshot-id"],
        "committed_at": datetime.fromtimestamp(s["timestamp-ms"] / 1000, timezone.utc)
                                .strftime("%Y-%m-%d %H:%M:%S"),
        "operation": s["summary"]["operation"],
        "added_rows": int(s["summary"].get("added-records", 0)),
        "total_rows": int(s["summary"].get("total-records", 0)),
        "current": s["snapshot-id"] == current,
    } for s in sorted(meta.get("snapshots", []), key=lambda s: s["timestamp-ms"])]


def print_snapshots(namespace: str, table: str) -> None:
    for s in snapshots(namespace, table):
        mark = "*" if s["current"] else " "
        print(f"{mark} {s['snapshot_id']:>20}  {s['committed_at']}  "
              f"{s['operation']:<9} +{s['added_rows']:<5} total={s['total_rows']}")
```

It uses only the standard library: no Spark, no Iceberg client. Now look at the tree three ways:

```python
from lakehouse.catalog import print_snapshots, table_metadata

# 1. The snapshots that make up this table's history (* marks the current one):
print_snapshots("bronze", "orders")

# 2. The current snapshot id, as Spark reports it:
spark.sql("SHOW TBLPROPERTIES iceberg.bronze.orders ('current-snapshot-id')").show(truncate=False)

# 3. The data files backing the current snapshot, via Iceberg's _file metadata column:
spark.sql("SELECT _file, count(*) AS rows FROM iceberg.bronze.orders GROUP BY _file").show(truncate=False)
```

Expected (your ids, times, and paths differ):

```
*  5582016758237887382  2026-01-05 09:44:28  append    +3     total=3

+-------------------+-------------------+
|key                |value              |
+-------------------+-------------------+
|current-snapshot-id|5582016758237887382|
+-------------------+-------------------+

+----------------------------------------------------------------------------+----+
|_file                                                                       |rows|
+----------------------------------------------------------------------------+----+
|s3://lakehouse/warehouse/__unitystorage/catalogs/.../data/00000-0-...parquet|1   |
|s3://lakehouse/warehouse/__unitystorage/catalogs/.../data/00001-1-...parquet|1   |
|s3://lakehouse/warehouse/__unitystorage/catalogs/.../data/00002-2-...parquet|1   |
+----------------------------------------------------------------------------+----+
```

One snapshot so far, your insert, recorded as an `append`. (The `CREATE TABLE` itself made no snapshot: snapshots record data operations, and the empty create had none.) And three data files, not one: Spark split the three `VALUES` rows across three tasks, and each task wrote its own Parquet file. On real data you'd see far fewer, far larger files; the point is that a "table" is whatever set of files the current snapshot lists. `_file` is one of the hidden metadata columns every Iceberg table carries (`DESCRIBE TABLE EXTENDED` lists them), and it works no matter which catalog you use.

### Now go look at the actual files in the bucket

The catalog's metadata is Iceberg describing itself. For the version that makes it click, open the bucket and look at the real objects. Find the table's location first:

```python
spark.sql("DESCRIBE TABLE EXTENDED iceberg.bronze.orders").where("col_name = 'Location'").show(truncate=False)
# s3://lakehouse/warehouse/__unitystorage/catalogs/<catalog-id>/tables/<table-id>
```

Then list everything under the warehouse with the S3 CLI from Chapter 3:

```bash
aws --endpoint-url http://localhost:8333 s3 ls --recursive s3://lakehouse/warehouse/
```

Expected (trimmed; the long ids differ on your machine):

```
.../tables/<table-id>/data/00000-0-12a34173-...-00001.parquet       # one of your 3 rows (Parquet)
.../tables/<table-id>/data/00001-1-12a34173-...-00001.parquet
.../tables/<table-id>/data/00002-2-12a34173-...-00001.parquet
.../tables/<table-id>/metadata/00000-cc63e3aa-....metadata.json     # written by Unity Catalog at CREATE
.../tables/<table-id>/metadata/00001-c0f4745b-....metadata.json     # written by Spark at INSERT
.../tables/<table-id>/metadata/snap-5582016758237887382-1-....avro  # the manifest list for your snapshot
.../tables/<table-id>/metadata/11597205-...-m0.avro                 # the manifest (lists the data files)
```

There it is. The folder isn't named `bronze/orders`: Unity Catalog places each table under its own id, so renaming a table never moves a byte. Your rows are Parquet objects under `data/`; everything under `metadata/` is the tree that turns them into a table. `00000` is the empty table's first metadata file, the one the catalog wrote; `00001` is the version your insert created. Pull the current `metadata.json` down and read it; the catalog tells you exactly which one is current:

```python
meta = table_metadata("bronze", "orders")
print(meta["current-snapshot-id"])
print(meta["snapshots"][-1]["manifest-list"])     # the snap-....avro you saw in the listing
print([f["name"] + ":" + str(f["id"]) for f in meta["schemas"][-1]["fields"]])
```

```bash
# or the raw file itself (copy the 00001-....metadata.json key from your listing):
aws --endpoint-url http://localhost:8333 \
    s3 cp s3://lakehouse/warehouse/__unitystorage/catalogs/<catalog-id>/tables/<table-id>/metadata/00001-<uuid>.metadata.json - | python3 -m json.tool
```

Expected (trimmed): a JSON document with `current-snapshot-id`, a `schemas` array whose fields carry the stable integer `id`s that make schema evolution safe (`order_id:1`, `customer_id:2`, `amount:3`, and so on), and a `snapshots` array where each entry's `manifest-list` points at exactly the `snap-....avro` you saw in the listing. You are reading, in raw form, the tree in Figure 5.3: `metadata.json` -> manifest list -> manifest -> data file.

Now watch the tree grow. Insert one more row, then re-list the `metadata/` prefix:

```python
spark.sql("INSERT INTO iceberg.bronze.orders VALUES "
          "('o-1009','c-9', 10.0, 'placed', TIMESTAMP '2026-01-05 11:00:00')")
print_snapshots("bronze", "orders")
```

```bash
aws --endpoint-url http://localhost:8333 s3 ls --recursive s3://lakehouse/warehouse/ | grep "/metadata/"
```

Expected: a second snapshot (`append +1 total=4`, now marked current), and in the bucket a **new** `metadata.json` (`00002-...`), a **new** `snap-....avro` manifest list, and a new manifest, while the old ones are still present, untouched. That single observation is the entire design: a write never edits old files, it writes new metadata and flips the catalog's pointer to it. Both snapshots' manifest lists now sit side by side in the bucket, which is why time travel (next-but-one section) is possible at all: the old snapshot's files were never removed.

> **These exact object names are illustrative** (version numbers, UUIDs, and the catalog-assigned ids depend on your run). The shape, `data/` Parquet plus a `metadata/` tree of `metadata.json` + `snap-*.avro` + `*-m0.avro`, is what to confirm.

## Schema evolution: change the shape, keep the data

Requirements change. A month in, you need to know which channel each order came from (web, mobile, in-store). In a naive files-on-disk world, adding a column can mean rewriting every file. Iceberg makes it a metadata-only operation.

> **Schema evolution** *(canonical term)*: changing a table's columns (add, rename,
> drop, reorder) without rewriting existing data files.

Because the schema lives in metadata, not baked immutably into every Parquet file, Iceberg tracks columns by a stable internal field ID, not by position or by
re-reading files. Adding a column just records a new field in the metadata.

**Add a column.** Existing rows read back `NULL` for it; no data is rewritten:

```python
spark.sql("ALTER TABLE iceberg.bronze.orders ADD COLUMN channel STRING")

spark.sql("""
    INSERT INTO iceberg.bronze.orders VALUES
        ('o-1004', 'c-3', 55.25, 'placed', TIMESTAMP '2026-01-05 10:00:00', 'mobile')
""")

spark.table("iceberg.bronze.orders").show()
```

Expected (row order may differ):

```
+--------+-----------+------+---------+-------------------+-------+
|order_id|customer_id|amount|   status|         created_at|channel|
+--------+-----------+------+---------+-------------------+-------+
|  o-1001|        c-1|  42.5|   placed|2026-01-05 09:12:00|   NULL|
|  o-1009|        c-9|  10.0|   placed|2026-01-05 11:00:00|   NULL|
|  o-1002|        c-2| 19.99|   placed|2026-01-05 09:15:30|   NULL|
|  o-1004|        c-3| 55.25|   placed|2026-01-05 10:00:00| mobile|
|  o-1003|        c-1|  88.0|cancelled|2026-01-05 09:20:10|   NULL|
+--------+-----------+------+---------+-------------------+-------+
```

Every row that existed before the `ADD COLUMN` reads back `NULL` under `channel`; only the newly inserted `o-1004` row shows `mobile`. Crucially, the original Parquet files were not touched. Iceberg knows they predate the `channel` field and simply returns `NULL` for it. (Run the `_file` query again if you want proof: the old file paths are all still there, plus one new one.)

**Rename a column**, also metadata-only, because of those stable field IDs:

```python
spark.sql("ALTER TABLE iceberg.bronze.orders RENAME COLUMN amount TO order_amount")
spark.table("iceberg.bronze.orders").printSchema()
```

Expected:

```
root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- order_amount: double (nullable = true)
 |-- status: string (nullable = true)
 |-- created_at: timestamp (nullable = true)
 |-- channel: string (nullable = true)
```

Existing data files still store the column under its old name; Iceberg maps it via
the field ID. No rewrite, no broken reads.

### Under the hood: why field IDs matter

If Iceberg tracked columns by name or position, a rename or reorder would be a minefield: old files say `amount`, new files say `order_amount`, and a reader wouldn't know they're the same column. By assigning every column a permanent numeric field ID the first time it appears, Iceberg decouples the logical schema you see from the physical layout in each file. Rename, reorder, add, drop: all become metadata edits, and every old file remains perfectly readable. This is the unglamorous engineering that makes "just add a column" actually safe in production.

## Time travel and rollback: your undo button for data

Now the feature that makes people fall in love with the lakehouse. Because every
write is a preserved snapshot, you can *query the past* and *undo mistakes*.

> **Snapshot / time travel** *(canonical term)*: every write is a versioned snapshot
> you can query, and roll back to.

**First, inspect the history** so you have snapshot IDs to work with:

```python
print_snapshots("bronze", "orders")
```

Expected (your ids and commit times differ; times are when you ran it, in UTC):

```
   5582016758237887382  2026-01-05 09:44:28  append    +3     total=3
   9147408084755816744  2026-01-05 09:46:57  append    +1     total=4
*  7404065150051679918  2026-01-05 09:46:57  append    +1     total=5
```

Three appends: your first insert, the `o-1009` row, and the `o-1004` row. (The `ALTER TABLE` statements changed the schema in a new `metadata.json` but added no snapshot; they touched no data.)

**Capture a known-good state before you break anything.** In real work you'd do this before any risky operation, and it's one line:

```python
good = spark.sql("SHOW TBLPROPERTIES iceberg.bronze.orders ('current-snapshot-id')").collect()[0]["value"]
print("known-good snapshot:", good)
```

**Now simulate a disaster**, the kind of one-line mistake that ends careers in a warehouse without backups:

```python
# A WHERE clause that accidentally matches everything.
spark.sql("DELETE FROM iceberg.bronze.orders WHERE order_amount > 0")
spark.table("iceberg.bronze.orders").count()   # 0, everything gone
```

Your live table is empty. In many systems, this is where you start restoring from last night's backup and losing a day of data. Here, the data never actually left: the `DELETE` just created a new snapshot whose file list excludes those rows. The old snapshot still points at all the original files.

**Query the past** to confirm the rows are still there in the earlier snapshot. Spark has a built-in time-travel option, `versionAsOf`, and an equivalent SQL clause:

```python
before = (spark.read
          .option("versionAsOf", good)        # the snapshot you captured above
          .table("iceberg.bronze.orders"))
before.count()                                # 5: your rows are still here, untouched

spark.sql(f"SELECT count(*) FROM iceberg.bronze.orders VERSION AS OF {good}").show()
```

(If you've seen older Iceberg examples use `.option("snapshot-id", ...)`: that option was removed. On this stack it fails with `Time travel option snapshot-id is no longer supported, use Spark built-in versionAsOf instead`.)

**Roll back** to make that good snapshot current again:

```python
spark.sql(f"CALL iceberg.system.rollback_to_snapshot('bronze.orders', {good})").show()
spark.table("iceberg.bronze.orders").count()   # 5: restored, as if the DELETE never happened
```

Expected:

```
+--------------------+-------------------+
|previous_snapshot_id|current_snapshot_id|
+--------------------+-------------------+
| 8627029015067133546|7404065150051679918|
+--------------------+-------------------+
```

`previous_snapshot_id` is the `DELETE`'s snapshot; `current_snapshot_id` is your known-good one, current again. Look at the history once more:

```python
print_snapshots("bronze", "orders")
```

```
   5582016758237887382  2026-01-05 09:44:28  append    +3     total=3
   9147408084755816744  2026-01-05 09:46:57  append    +1     total=4
*  7404065150051679918  2026-01-05 09:46:57  append    +1     total=5
   8627029015067133546  2026-01-05 09:47:08  delete    +0     total=0
```

The `delete` snapshot is still in the history; it just isn't current anymore. The rollback itself is just another metadata operation: flip the current pointer back to the good snapshot. Instant, cheap, and complete. Nothing was copied, restored, or rewritten.

This is the "wow" of the lakehouse, and it's worth pausing on *why* it's possible:
warehouse-grade safety (undo a destructive write in seconds) sitting on warehouse-cheap storage (immutable files in an object store). You are not paying for an expensive proprietary system to get this; it falls out of the open table format's design.

> **A note on retention:** snapshots don't live forever by default in production. You expire old ones to reclaim storage (a maintenance job you'll meet in the "where to go next" roadmap). Time travel works within your retention window. For this course, nothing is expired, so all your snapshots are available.

## Common pitfalls and troubleshooting

Real learning includes the errors. Here are the ones you're most likely to hit in this chapter, with the exact messages, and what they mean.

- **`None of catalog, schema or storage-root.tables server property has managed location configured`** on `CREATE TABLE`. The `iceberg` catalog was created without a `storage_root`. Delete it (`curl -X DELETE "http://localhost:8081/api/2.1/unity-catalog/catalogs/iceberg?force=true"`) and recreate it with `"storage_root": "s3://lakehouse/warehouse"` as in Step 1b.

- **`S3 bucket configuration not found`.** Unity Catalog has no usable entry for your bucket. Check `server.properties`: `s3.bucketPath.0` must be the bucket root (`s3://lakehouse`), and the key, secret, region, *and* session token lines must all be present. Without the session token, Unity Catalog silently skips the whole entry.

- **`UnknownHostException`, or "Connect to lakehouse.seaweedfs:8333 ... Connection refused".** Unity Catalog's S3 client addresses the bucket as `lakehouse.seaweedfs`. An unknown host means the SeaweedFS network alias from Chapter 3 is missing; connection refused usually means SeaweedFS is still starting (wait a few seconds) or was started without `-s3.domainName=seaweedfs`.

- **`Invalid S3 Credentials: s3.session-token-expires-at-ms not set`** (from executors, during an `INSERT`). The `client.refresh-credentials-enabled = false` line is missing from `spark-defaults.conf`, or you didn't restart `spark-connect` after adding it.

- **`Schema not found: iceberg.bronze.orders`** when you query `...orders.snapshots`, `.files`, or `.history`. That's the Unity Catalog 0.7 metadata-table limitation explained above, not a mistake on your part. Use `lakehouse.catalog.print_snapshots`, `SHOW TBLPROPERTIES`, and the `_file` column instead.

- **`Time travel option snapshot-id is no longer supported, use Spark built-in versionAsOf instead`.** Use `.option("versionAsOf", id)` or `VERSION AS OF id`.

- **Your catalog or tables vanished after a restart** (`NoSuchWarehouseException: Catalog not found: iceberg`). Unity Catalog's database wasn't on a volume, so recreating the container reset it. Add the `uc-db` volume from Step 1, recreate the catalog, and recreate your tables. The old files are still in the bucket, but nothing points at them now.

- **`NoSuchNamespaceException` / "namespace does not exist" on CREATE TABLE.**
  You skipped the `CREATE NAMESPACE` loop, or you typo'd the namespace. Iceberg won't auto-create the parent namespace for you.

- **`CANNOT_MODIFY_STATIC_CONFIG` when you try to set a catalog option from the
  client.** Exactly the Chapter 4 lesson: catalog wiring is static, server-side. Don't set `spark.sql.catalog.*` from your `sc://` session; it's already configured. If it's genuinely misconfigured, fix `spark-defaults.conf` on the server and restart it, not the client.

- **`ConnectException` / cannot reach the catalog.** Unity Catalog OSS isn't up, or isn't healthy yet. Run `docker compose ps`; if `unity-catalog` isn't running, bring it up and curl the catalogs endpoint on 8081 before creating tables.

- **A Unity Catalog error that just says "Server error" with no detail.** Add `server.include-stacktrace-in-error=true` to `server.properties` and restart `unity-catalog`; the REST responses then carry the underlying exception. The server's own log is at `/home/unitycatalog/etc/logs/server.log` inside the container.

- **`TableAlreadyExistsException`.** You ran a `CREATE TABLE` (without `IF NOT
  EXISTS`) against a table that's already there. Either add `IF NOT EXISTS` or drop
  it first if you meant to recreate it.

- **Rollback "works" but you still see no rows.** You rolled back to the *wrong*
  snapshot, one from before the rows you wanted were inserted. Re-check `print_snapshots`,
  pick the `append` snapshot whose `total` has your data, and roll back to that one.

- **A stray/typo'd column in `INSERT ... VALUES` order.** Iceberg maps positional `VALUES` to the current schema order. After a schema change, re-check column order, an easy way to silently put `channel` values into `status`.

## Try it yourself

Don't just read, extend the build. Each of these reinforces something from the chapter:

1. **Read the catalog's raw answer.** `curl` the `loadTable` endpoint yourself (`.../iceberg/v1/catalogs/iceberg/namespaces/bronze/tables/orders`) and find, in the JSON, the `metadata-location`, the current snapshot id, and the `config` block with the credentials Unity Catalog vended. Everything `catalog.py` prints comes from this one response.

2. **Prove immutability.** After your first insert, note the file paths from the `_file` query. Do a second insert, then query again. How many data files are
   there now? Did the original files change, or did new ones appear? Explain what
   that tells you about how Iceberg writes.

3. **Evolve, then time-travel across the evolution.** Time-travel to a snapshot from
   *before* you added the `channel` column (the first one in `print_snapshots`). What columns does the old snapshot show?
   (Answer: `amount`, not `order_amount`, and no `channel`. Iceberg reads a historical snapshot with the schema that was current when it was written.)

4. **Drop and reorder.** Try `ALTER TABLE ... DROP COLUMN` and `ALTER TABLE ...
   ALTER COLUMN ... FIRST/AFTER`. Query the data after each. Did any data file get
   rewritten? Check the `_file` query and the bucket listing to be sure.

5. **Break it and recover it, deliberately.** Do a bad `UPDATE` (e.g. set every
   `status` to `'wat'`), confirm the damage, then roll back. Time yourself: how
   fast is a full recovery?

## Check your understanding

- In one sentence, what are the three questions a catalog answers for any engine?
- Why can adding a column to a huge Iceberg table be nearly instant, when it might rewrite terabytes in a naive files-on-disk setup?
- What actually happens, physically, when you `DELETE FROM` an Iceberg table? Do the data files get erased?
- Why do we insist on one catalog for the whole lakehouse instead of one per engine?
- Where does the catalog wiring live, and why can't your Spark Connect client change it?
- Which files does Spark write during an `INSERT`, and which does Unity Catalog write? Why does that mean both need a route to storage?

## Recap and what's next

- A catalog maps table names to metadata and files so any engine can find them consistently, answering what tables exist, what's the current schema and snapshot, and where the files are.
- Unity Catalog OSS is our one catalog for everything, exposing a standard Iceberg REST endpoint that many engines share, the basis of Chapter 10's multi-engine serving. Its own records live on the `uc-db` volume, because a catalog is state.
- An Iceberg table is a metadata tree over immutable data files. That design gives you ACID, concurrent-writer safety, cheap schema evolution, and time travel and rollback, all as metadata operations.
- You can read that tree straight from the catalog (`lakehouse/catalog.py`), from Spark (`SHOW TBLPROPERTIES`, the `_file` column), and from the bucket itself.
- Static catalog wiring lives server-side; your thin client just uses it.
- **Next, Chapter 6, Ingestion:** now that you can create and trust tables, you'll land raw order data into the Bronze layer and meet the ingestion patterns that keep it correct: batch, ELT, and idempotency.

![Progress: Tables complete, Ingestion next](../figures/ch05/fig-5.4-progress-tables-done.svg)

**Figure 5.4**. Progress map with **Tables and Catalog done** and **Ingestion** next.

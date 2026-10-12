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
- Read an Iceberg table's metadata tables (`.snapshots`, `.files`, `.history`) to see its internal structure.
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

The catalog wiring (the Iceberg SQL extensions, the REST catalog URI, the warehouse name, the S3 credentials Spark uses to reach object storage) is all static Spark configuration. It's set once, server-side, in the Connect server's `spark-defaults.conf`. Concretely, that wiring makes `iceberg` a REST catalog pointed at Unity Catalog's IRC endpoint:

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
```

These lines make Spark the engine that actually writes Iceberg data and metadata to SeaweedFS, while Unity Catalog only records the commits over its REST API. That division is why native Iceberg writes work here even though the catalog itself doesn't move your bytes. Note the catalog URI uses the in-network address `unity-catalog:8080` (the service name and container port from the Compose file), not `localhost:8081`, because the Connect server reaches the catalog over the Compose network. The `warehouse` is the Unity Catalog catalog name, not an object-storage path. None of this is something your thin client injects at runtime.

Why is that good news rather than a limitation? Because it means your client code is gloriously boring. You connect over `sc://` and just write `spark.sql("CREATE TABLE ...")`, and the catalog is already wired, already authenticated, already pointed at the right storage. The platform owns the plumbing; you write logic. Every engineer who connects gets the same correct wiring for free, and nobody can accidentally point their session at the wrong catalog. Thin clients, central configuration: that's the Spark Connect philosophy from Chapter 4 doing real work.

## Build: create and query an Iceberg table

Time to make a real table. Spark should still be up from Chapter 4; now you add the catalog as the next service in the same `docker-compose.yml`.

**Step 1: add Unity Catalog OSS to your Compose file.** It joins the same `lakehouse-network`, so Spark reaches it by service name (`unity-catalog:8080`) and you reach it from your host on the published port (`localhost:8081`). Unity Catalog reads its configuration from a `server.properties` file you mount in, which is where you point it at your SeaweedFS bucket as managed storage.

```yaml
  unity-catalog:
    image: unitycatalog/unitycatalog:v0.7.0   # 0.7+ exposes a writable Iceberg REST catalog
    container_name: unity-catalog
    depends_on:
      - seaweedfs
    volumes:
      - ./config/unity-catalog/server.properties:/home/unitycatalog/etc/conf/server.properties
    ports:
      - "8081:8080"      # host 8081 maps to container 8080
    networks:
      - lakehouse-network
```

Create `config/unity-catalog/server.properties` to tell Unity Catalog where its storage lives and how to reach the object store. Fill the key and secret with the same values you put in `.env` in Chapter 2 (a properties file is read literally, so it does not expand `.env` for you):

```properties
# config/unity-catalog/server.properties
s3.bucketPath.0=s3://lakehouse/warehouse
s3.endpoint.0=http://seaweedfs:8333
s3.accessKey.0=<same value as S3_ACCESS_KEY in your .env>
s3.secretKey.0=<same value as S3_SECRET_KEY in your .env>
s3.pathStyleAccess.0=true
```

These property names track the Unity Catalog OSS 0.7 `server.properties` reference; confirm them against the exact version you run. Pin the image to a specific version rather than `latest` so your build is reproducible. Then bring it up and confirm it is healthy:

```bash
docker compose up -d unity-catalog
docker compose ps
```

Expected: `unity-catalog` shows up alongside `seaweedfs` and the Spark services, state `running`. Confirm it is answering by listing the catalogs it already has. A fresh Unity Catalog ships with a single built-in catalog named `unity`:

```bash
curl -s http://localhost:8081/api/2.1/unity-catalog/catalogs
```

A JSON response listing `unity` means the server is live. If the container is not running or the curl hangs, jump to troubleshooting before continuing: creating a table against a half-started catalog produces confusing errors.

**Step 1b: create the `iceberg` catalog.** Your `spark-defaults.conf` points Spark at `warehouse = iceberg`, which is the name of a Unity Catalog *catalog*. That catalog doesn't exist yet (only `unity` does), so create it once:

```bash
curl -s -X POST http://localhost:8081/api/2.1/unity-catalog/catalogs \
  -H 'Content-Type: application/json' \
  -d '{"name": "iceberg", "comment": "The lakehouse catalog, served over the Iceberg REST protocol"}'
```

Now confirm the Iceberg REST endpoint resolves it. The protocol takes the catalog name as a `warehouse` query parameter:

```bash
curl -s "http://localhost:8081/api/2.1/unity-catalog/iceberg/v1/config?warehouse=iceberg"
```

A JSON config response means the catalog is live, speaking the Iceberg REST protocol, and Spark will find it under the name `iceberg`. (Catalog names, the exact REST shape, and whether your Unity Catalog version wants a storage root on the catalog can vary by release; confirm against the version you run, and see troubleshooting if a later table creation complains about a storage location.)

**Step 2: connect a thin client and create a namespace and a table.** The catalog name `iceberg` and the REST wiring are configured server-side (see the callout above), so from the client you simply use them:

```python
from lakehouse.session import get_spark

spark = get_spark()   # the factory from Chapter 4; one connection, defined once

# A namespace (schema) to hold our Bronze tables.
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

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

**What just happened?** Even before you inserted a single row, Iceberg wrote
*metadata* to object storage: a `metadata.json` file describing the table's schema,
registered under a pointer in the catalog. Note what it did *not* write: a snapshot.
A snapshot records a specific set of data files, and a freshly created table has had
no data operation yet, so it has none. The catalog now knows `iceberg.bronze.orders`
exists, what shape it has, and where its metadata lives. The table is real; it just
has no data, and no snapshots, yet.

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

**What just happened?** That `INSERT` did not edit an existing file. Iceberg wrote one or more new Parquet data files, then wrote new metadata declaring a fresh snapshot whose file list includes them. The catalog's pointer moved to that new snapshot atomically. This is the ACID guarantee in action: until that pointer flips, no reader sees the half-written state; the instant it flips, every reader sees the complete new version. There is never an in-between.

**Step 4: verify the catalog knows the table, independent of Spark.** Ask the Iceberg REST endpoint directly, with no Spark involved, to list the tables in the `bronze` namespace. The protocol scopes calls under a prefix that the `config` call above returned for your warehouse; for the `iceberg` catalog that prefix is `catalogs/iceberg`:

```bash
curl -s http://localhost:8081/api/2.1/unity-catalog/iceberg/v1/catalogs/iceberg/namespaces/bronze/tables
```

You should see `orders` in the response. Different client, same catalog, same table: a tiny preview of Chapter 10, where a completely different engine reads these exact rows.

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

You don't have to take this on faith. Iceberg exposes its own internals as queryable metadata tables. Try them:

```python
# The snapshots that make up this table's history:
spark.sql("SELECT snapshot_id, committed_at, operation "
          "FROM iceberg.bronze.orders.snapshots").show(truncate=False)

# The actual data files backing the current snapshot:
spark.sql("SELECT file_path, record_count, file_size_in_bytes "
          "FROM iceberg.bronze.orders.files").show(truncate=False)

# A human-readable history of what happened when:
spark.sql("SELECT made_current_at, snapshot_id, is_current_ancestor "
          "FROM iceberg.bronze.orders.history").show(truncate=False)
```

Expected: `.snapshots` shows one row so far, your insert, recorded as an `append`. (The `CREATE TABLE` itself made no snapshot: snapshots record data operations, and the empty create had none.) `.files` shows the Parquet file(s) your insert produced, with a `record_count` of 3. `.history` shows the order in which snapshots became current. You are literally reading the tree from Figure 5.3.

### Now go look at the actual files in the bucket

The metadata tables are Iceberg describing itself. For the version that makes it click, open the bucket and look at the real objects. The `file_path` column you just saw gives you the exact object-storage location; take its directory part and list the table's prefix recursively with the S3 CLI from Chapter 3:

```bash
aws --endpoint-url http://localhost:8333 s3 ls --recursive s3://lakehouse/warehouse/ | grep orders
```

Expected: two kinds of object, under two directories (the exact prefix is assigned by the catalog, so match whatever `file_path` reported):

```
.../bronze/orders/data/00000-0-....parquet          # the 3 rows you inserted (Parquet)
.../bronze/orders/metadata/00000-....metadata.json  # table metadata: schema + snapshot list
.../bronze/orders/metadata/snap-....avro            # a manifest list (one per snapshot)
.../bronze/orders/metadata/....-m0.avro             # a manifest (lists the data files)
```

There it is: your three rows are one Parquet object under `data/`; everything under `metadata/` is the tree that turns that object into a table. Pull the current `metadata.json` down and read it:

```bash
aws --endpoint-url http://localhost:8333 \
    s3 cp s3://lakehouse/warehouse/.../bronze/orders/metadata/00000-....metadata.json - | python3 -m json.tool
```

Expected (trimmed): a JSON document with `current-snapshot-id`, a `schemas` array whose fields carry the stable integer `id`s that make schema evolution safe, and a `snapshots` array where each entry's `manifest-list` points at exactly the `snap-....avro` you saw in the listing. You are reading, in raw form, the tree in Figure 5.3: `metadata.json` -> manifest list -> manifest -> data file.

Now watch the tree grow. Insert one more row, then re-list the `metadata/` prefix:

```python
spark.sql("INSERT INTO iceberg.bronze.orders VALUES "
          "('o-1009','c-9', 10.0, 'placed', TIMESTAMP '2026-01-05 11:00:00')")
```

```bash
aws --endpoint-url http://localhost:8333 s3 ls --recursive s3://lakehouse/warehouse/ | grep "orders/metadata"
```

Expected: a **new** `metadata.json` (version bumped from `00000` to `00001`), a **new** `snap-....avro` manifest list, and a new manifest, while the old ones are still present, untouched. That single observation is the entire design: a write never edits old files, it writes new metadata and flips the catalog's pointer to it. Both snapshots' manifest lists now sit side by side in the bucket, which is why time travel (next-but-one section) is possible at all, the old snapshot's files were never removed.

> **These exact object names and prefixes are illustrative** (version numbers, UUIDs, and the catalog-assigned path depend on your run). The shape, `data/` Parquet plus a `metadata/` tree of `metadata.json` + `snap-*.avro` + `*.avro`, is what to confirm.

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

Expected: every row that existed before the `ADD COLUMN` reads back `NULL` under `channel`; only the newly inserted `o-1004` row shows `mobile`. Crucially, the original Parquet files were not touched. Iceberg knows they predate the `channel` field and simply returns `NULL` for it.

**Rename a column**, also metadata-only, because of those stable field IDs:

```python
spark.sql("ALTER TABLE iceberg.bronze.orders RENAME COLUMN amount TO order_amount")
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
spark.sql("SELECT snapshot_id, committed_at, operation "
          "FROM iceberg.bronze.orders.snapshots").show(truncate=False)
```

Note the `snapshot_id` of a known-good state, say right after your inserts, before
we break anything. Copy it.

**Now simulate a disaster**, the kind of one-line mistake that ends careers in a warehouse without backups:

```python
# A WHERE clause that accidentally matches everything.
spark.sql("DELETE FROM iceberg.bronze.orders WHERE order_amount > 0")
spark.table("iceberg.bronze.orders").count()   # 0, everything gone
```

Your live table is empty. In many systems, this is where you start restoring from last night's backup and losing a day of data. Here, the data never actually left: the `DELETE` just created a new snapshot whose file list excludes those rows. The old snapshot still points at all the original files.

**Query the past** to confirm the rows are still there in the earlier snapshot:

```python
before = (spark.read
          .option("snapshot-id", "<good_snapshot_id>")   # from the history above
          .table("iceberg.bronze.orders"))
before.count()   # your rows are still here, untouched
```

**Roll back** to make that good snapshot current again:

```python
spark.sql("CALL iceberg.system.rollback_to_snapshot('bronze.orders', <good_snapshot_id>)")
spark.table("iceberg.bronze.orders").count()   # restored, as if the DELETE never happened
```

The rollback itself is just another metadata operation: flip the current pointer back to the good snapshot. Instant, cheap, and complete.

This is the "wow" of the lakehouse, and it's worth pausing on *why* it's possible:
warehouse-grade safety (undo a destructive write in seconds) sitting on warehouse-cheap storage (immutable files in an object store). You are not paying for an expensive proprietary system to get this; it falls out of the open table format's design.

> **A note on retention:** snapshots don't live forever by default in production. You expire old ones to reclaim storage (a maintenance job you'll meet in the "where to go next" roadmap). Time travel works within your retention window. For this course, nothing is expired, so all your snapshots are available.

## Common pitfalls and troubleshooting

Real learning includes the errors. Here are the ones you're most likely to hit in this chapter, and what they mean.

- **`NoSuchNamespaceException` / "namespace does not exist" on CREATE TABLE.**
  You skipped `CREATE NAMESPACE IF NOT EXISTS iceberg.bronze`, or you typo'd the
  namespace. Iceberg won't auto-create the parent namespace for you.

- **`CANNOT_MODIFY_STATIC_CONFIG` when you try to set a catalog option from the
  client.** Exactly the Chapter 4 lesson: catalog wiring is static, server-side. Don't set `spark.sql.catalog.*` from your `sc://` session; it's already configured. If it's genuinely misconfigured, fix `spark-defaults.conf` on the server and restart it, not the client.

- **`ConnectException` / cannot reach the catalog.** Unity Catalog OSS isn't up, or isn't healthy yet. Run `docker compose ps`; if `unity-catalog` isn't running, bring it up and curl the catalogs endpoint on 8081 before creating tables.

- **`NoSuchCatalogException` / the warehouse can't be found when Spark connects.** You skipped Step 1b. Unity Catalog ships only a `unity` catalog, but Spark is pointed at one named `iceberg`. Create it with the `POST .../catalogs` call, then retry. If table creation then fails with a storage or location error instead, your Unity Catalog version wants a storage root on the catalog; recreate it including your bucket path (`s3://lakehouse/warehouse`) as the storage root.

- **`TableAlreadyExistsException`.** You ran a `CREATE TABLE` (without `IF NOT
  EXISTS`) against a table that's already there. Either add `IF NOT EXISTS` or drop
  it first if you meant to recreate it.

- **Rollback "works" but you still see no rows.** You rolled back to the *wrong*
  snapshot, one from before the rows you wanted were inserted. Re-check `.snapshots`,
  pick the `append` snapshot that has your data, and roll back to that one.

- **A stray/typo'd column in `INSERT ... VALUES` order.** Iceberg maps positional `VALUES` to the current schema order. After a schema change, re-check column order, an easy way to silently put `channel` values into `status`.

## Try it yourself

Don't just read, extend the build. Each of these reinforces something from the chapter:

1. **Add and populate `silver` and `gold` namespaces.** Create
   `iceberg.silver` and `iceberg.gold` namespaces now (you'll fill them in Chapter
   7). Confirm with `SHOW NAMESPACES IN iceberg`.

2. **Prove immutability.** After your first insert, note the file path from
   `.files`. Do a second insert, then query `.files` again. How many data files are
   there now? Did the original file change, or did a new one appear? Explain what
   that tells you about how Iceberg writes.

3. **Evolve, then time-travel across the evolution.** Time-travel to a snapshot from
   *before* you added the `channel` column. What columns does the old snapshot show?
   (This reveals how Iceberg reconstructs the schema for a historical read.)

4. **Drop and reorder.** Try `ALTER TABLE ... DROP COLUMN` and `ALTER TABLE ...
   ALTER COLUMN ... FIRST/AFTER`. Query the data after each. Did any data file get
   rewritten? Check `.files` to be sure.

5. **Break it and recover it, deliberately.** Do a bad `UPDATE` (e.g. set every
   `status` to `'wat'`), confirm the damage, then roll back. Time yourself: how
   fast is a full recovery?

## Check your understanding

- In one sentence, what are the three questions a catalog answers for any engine?
- Why can adding a column to a huge Iceberg table be nearly instant, when it might rewrite terabytes in a naive files-on-disk setup?
- What actually happens, physically, when you `DELETE FROM` an Iceberg table? Do the data files get erased?
- Why do we insist on one catalog for the whole lakehouse instead of one per engine?
- Where does the catalog wiring live, and why can't your Spark Connect client change it?

## Recap and what's next

- A catalog maps table names to metadata and files so any engine can find them consistently, answering what tables exist, what's the current schema and snapshot, and where the files are.
- Unity Catalog OSS is our one catalog for everything, exposing a standard Iceberg REST endpoint that many engines share, the basis of Chapter 10's multi-engine serving.
- An Iceberg table is a metadata tree over immutable data files. That design gives you ACID, concurrent-writer safety, cheap schema evolution, and time travel and rollback, all as metadata operations.
- You can read Iceberg's own internals via `.snapshots`, `.files`, and `.history`.
- Static catalog wiring lives server-side; your thin client just uses it.
- **Next, Chapter 6, Ingestion:** now that you can create and trust tables, you'll land raw order data into the Bronze layer and meet the ingestion patterns that keep it correct: batch, ELT, and idempotency.

![Progress: Tables complete, Ingestion next](../figures/ch05/fig-5.4-progress-tables-done.svg)

**Figure 5.4**. Progress map with **Tables and Catalog done** and **Ingestion** next.

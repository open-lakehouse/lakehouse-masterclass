# Chapter 5 — Tables & Catalog: Iceberg through Unity Catalog OSS

## 5.0 What you'll build

Real, transactional tables. In Chapter 3 you decided *where bytes live* (object
storage) and *how files become a table* (the Iceberg open table format). Now you
make it concrete: you stand up **Unity Catalog OSS** as the one catalog every
engine talks to, create **Iceberg** tables through it, evolve their schema without
a rewrite, and travel through their **snapshots** to query — and roll back to —
past states. This is the layer that turns a pile of Parquet into a database.

![Progress: Tables & Catalog](../figures/ch05/fig-5.3-progress-tables.svg)

**Figure 5.3** — Progress map with **Tables & Catalog** highlighted.

## 5.1 Learning objectives

By the end of this chapter you can:

- Explain what a **catalog** is and why the lakehouse needs one.
- Describe how **Unity Catalog OSS** exposes tables to any engine via an **Iceberg
  REST catalog** endpoint.
- Create an Iceberg table, insert rows, and query it through Spark Connect.
- Evolve a table's **schema** (add/rename a column) without rewriting data.
- Use **snapshots** to time-travel: query an older version and roll back a mistake.

## 5.2 What a catalog is

> **Catalog** *(canonical term)*: the service that maps human table names
> (`iceberg.bronze.orders`) to the metadata and files that physically make up the
> table, so any engine can find and read it.

Without a catalog, a "table" is just a folder of files in object storage — and
every engine has to be told, out of band, exactly which files and which schema.
The catalog is the source of truth that answers three questions for *any* client:
*what tables exist*, *what is each table's current schema and snapshot*, and *where
are its files*. It's the difference between a data swamp and a queryable database.

![Catalog: names to metadata to files](../figures/ch05/fig-5.1-catalog-role.svg)

**Figure 5.1** — The catalog sits between engines and storage: a client asks
the catalog for `iceberg.bronze.orders`, gets back the current metadata pointer,
and reads the data files directly from object storage.

## 5.3 One catalog for everything: Unity Catalog OSS

We use **Unity Catalog OSS** as *the* catalog for the whole lakehouse — not one
catalog for Spark and a different one for everything else. That single choice is
what makes the Serving chapter (Chapter 10) possible: because the tables live in
one open catalog, *any* Iceberg-aware engine can read them.

The key mechanism is the **Iceberg REST catalog**. Unity Catalog OSS exposes a
standard Iceberg REST API at:

```
http://localhost:8081/api/2.1/unity-catalog/iceberg
```

Any engine that speaks the Iceberg REST protocol — Spark, DuckDB, Trino,
DataFusion — points at that one URL and sees the same tables. No per-engine
metastore, no copies, no drift.

> **Multi-engine / interoperability** *(canonical term)*: the same open tables,
> read by different engines, because they share one catalog and one open format.

### Under the hood — static config lives server-side

Recall `CANNOT_MODIFY_STATIC_CONFIG` from Chapter 4. The catalog wiring — the
Iceberg SQL extensions, the REST catalog URI, the warehouse location, S3
credentials — is **static** Spark configuration. It is set once in the server's
`spark-defaults.conf`, *not* injected from your Connect client. So when you connect
over `sc://` and simply write `spark.sql("CREATE TABLE …")`, the catalog is already
wired for you server-side. This is by design: clients stay thin, the platform owns
the wiring.

## 5.4 Build — create and query an Iceberg table

Step 1 — make sure the catalog is running (Spark is already up from
Chapter 4):

```bash
./lakehouse start unity-catalog
./lakehouse status
```

Expected: Unity Catalog OSS shows healthy on port **8081**, alongside the Spark
master, worker, and Connect server.

Step 2 — connect a thin client and create a namespace + table. The
catalog name `iceberg` and the REST wiring are configured server-side, so from the
client you just use them:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

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

Step 3 — insert a few rows and read them back:

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

Step 4 — verify it through the CLI's data browser, which reads the *same*
table through the catalog:

```bash
./lakehouse browsedata iceberg.bronze.orders
```

## 5.5 What Iceberg gives you: ACID, schema evolution, time travel

An Iceberg table is not the data files alone — it's a **metadata tree** on top of
them. Every write creates a new immutable **snapshot**: a metadata file that lists
exactly which data files make up the table at that instant. Readers see a
consistent snapshot; concurrent writers don't corrupt each other; and old
snapshots stick around, which is what makes time travel and rollback possible.

![Iceberg table anatomy and snapshots](../figures/ch05/fig-5.2-iceberg-anatomy.svg)

**Figure 5.2** — Iceberg's layers: the catalog points at the current metadata
file → which lists manifests → which list data files. Each write adds a new
snapshot without mutating the old ones.

### Schema evolution

> **Schema evolution** *(canonical term)*: changing a table's columns (add, rename,
> drop, reorder) without rewriting existing data files.

Because the schema lives in metadata, not baked into every file, Iceberg evolves it
cheaply.

Add a column — existing rows simply read back `NULL` for it, no rewrite:

```python
spark.sql("ALTER TABLE iceberg.bronze.orders ADD COLUMN channel STRING")
spark.sql("""
    INSERT INTO iceberg.bronze.orders VALUES
        ('o-1004', 'c-3', 55.25, 'placed', TIMESTAMP '2026-01-05 10:00:00', 'mobile')
""")
spark.table("iceberg.bronze.orders").show()
```

Expected: the three original rows show `NULL` under `channel`; the new row shows
`mobile`. No data was rewritten.

### Time travel and rollback

> **Snapshot / time travel** *(canonical term)*: every write is a versioned
> snapshot you can query, and roll back to.

Inspect the snapshot history:

```python
spark.sql("SELECT snapshot_id, committed_at, operation FROM iceberg.bronze.orders.snapshots").show(truncate=False)
```

Simulate a mistake, then travel back and undo it:

```python
# Oops — an accidental full delete.
spark.sql("DELETE FROM iceberg.bronze.orders WHERE amount > 0")
spark.table("iceberg.bronze.orders").count()   # 0 — everything gone

# Read the table AS OF an earlier snapshot (pick an id from the history above).
before = spark.read.option("snapshot-id", "<earlier_snapshot_id>").table("iceberg.bronze.orders")
before.count()   # rows are still there in that snapshot

# Roll the live table back to that good snapshot.
spark.sql("CALL iceberg.system.rollback_to_snapshot('bronze.orders', <earlier_snapshot_id>)")
spark.table("iceberg.bronze.orders").count()   # restored
```

This is the "wow" of the lakehouse: a destructive mistake on cheap object storage
is *recoverable*, because the table is versioned. Warehouse-grade safety on
warehouse-cheap storage.

## 5.6 Checkpoint

- `./lakehouse status` shows Unity Catalog OSS healthy on **8081**.
- You created `iceberg.bronze.orders` and inserted/queried rows over `sc://`.
- You added a column with `ALTER TABLE` and existing rows read back `NULL` — no
  rewrite.
- You listed `.snapshots`, queried the table `AS OF` an older snapshot, and rolled
  back a bad `DELETE`.

## 5.7 Recap & what's next

- A **catalog** maps table names to metadata and files so any engine can find them.
- **Unity Catalog OSS** is our one catalog for everything, exposing an **Iceberg
  REST** endpoint that many engines can share — the basis of Chapter 10's serving.
- **Iceberg** gives ACID snapshots, cheap **schema evolution**, and **time
  travel/rollback**, all through metadata over immutable data files.
- Static catalog wiring lives **server-side**; your thin client just uses it.
- **Next — Chapter 6, Ingestion:** land raw order data into the Bronze layer, and
  meet the ingestion patterns (batch, ELT, idempotency).

![Progress: Tables complete, Ingestion next](../figures/ch05/fig-5.4-progress-tables-done.svg)

**Figure 5.4** — Progress map with **Tables & Catalog ✓** and **Ingestion** next.

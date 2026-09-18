# Chapter 3 — Storage: Object Store + Open Table Format

## 3.0 What you'll build

The foundation of the whole lakehouse: a running **object store** on your laptop,
a **warehouse** location to hold table files, and a firm decision about the **open
table format** you'll layer on top. By the end you can put bytes in and get them
back out through the S3 API — and you'll know exactly why we chose SeaweedFS and
Iceberg.

![Progress: Storage](../figures/ch03/fig-3.3-progress-storage.svg)

**Figure 3.3** — Progress map with **Storage** highlighted.

## 3.1 Learning objectives

By the end of this chapter you can:

- Explain what an **object store** is and why it's the base of a lakehouse.
- Name three S3-API object stores (S3, MinIO, SeaweedFS) and when you'd pick each.
- Explain what an **open table format** adds on top of raw files.
- Compare Iceberg / Delta / Hudi and justify the Iceberg choice.
- Start the object store and create the warehouse bucket.

## 3.2 Part 1 — Where the bytes live: the object store

> **Object storage** *(canonical term)*: cheap, effectively infinite, S3-API blob
> storage that holds all your data files.

Why object storage, and not a filesystem or a database? Three reasons: it's cheap
per gigabyte, it scales effectively without limit, and — critically for a
lakehouse — it's **decoupled from compute**, so you can point many engines at the
same files. The universal interface is the **S3 API**: originally Amazon's, now the
lingua franca that every object store speaks. Because your code targets the S3 API
rather than a specific product, moving between object stores is a configuration
change, not a rewrite.

You'll meet three:

- **Amazon S3** — the cloud original. Infinite, durable, zero-ops — but needs a
  cloud account and costs money to run.
- **MinIO** — a popular self-hosted, fully S3-compatible store. Great in labs;
  slightly heavier footprint.
- **SeaweedFS** — lightweight, fast, tiny RAM footprint. **Our choice**, because
  it's the most laptop-friendly way to get a real S3 endpoint locally.

![Object stores over one S3 API](../figures/ch03/fig-3.1-object-stores.svg)

**Figure 3.1** — S3 vs. MinIO vs. SeaweedFS, all speaking the same S3 API.

> **Warehouse (location)** *(canonical term)*: the object-storage path where
> lakehouse tables physically live. Ours will be `s3a://lakehouse/warehouse`.

## 3.3 Part 2 — How files become a table: the open table format

Cheap files alone are a data lake — and a data lake has no transactions, no
reliable schema, no safe concurrent writes. To get warehouse guarantees we add a
layer on top of the files.

> **Open table format** *(canonical term)*: a specification — we'll use **Apache
> Iceberg** — that adds a metadata layer over your data files so a plain pile of
> files behaves like a transactional table.

Think of it as three layers stacked:

1. **Object store** — the raw bytes (SeaweedFS / S3).
2. **File format** — Parquet columnar files.
3. **Table format** — Iceberg metadata that says "these Parquet files, at this
   snapshot, are the table `orders`," and coordinates ACID writes, schema
   evolution, and time travel.

![The three-layer stack and the format choices](../figures/ch03/fig-3.2-table-formats.svg)

**Figure 3.2** — Object store → Parquet → table format; Iceberg vs. Delta vs. Hudi.

The three main open table formats:

- **Apache Iceberg** — engine-neutral by design (Spark, Flink, Trino, DuckDB, …),
  hidden partitioning, strong open governance. **Our choice.**
- **Delta Lake** — excellent on Spark, native to Databricks; historically
  Spark-centric.
- **Apache Hudi** — strong incremental upserts and CDC-oriented workloads; steeper
  learning curve.

All three solve the same problem. We pick **Iceberg** for its engine-neutrality —
the heart of the "open" promise, and what lets Chapter 10 serve the *same* tables
to multiple engines through Unity Catalog OSS.

### Under the hood — you're choosing now, building in Chapter 5

This chapter makes the two storage *decisions* — object store (SeaweedFS) and table
format (Iceberg). Chapter 5 does the hands-on Iceberg build — creating tables
through **Unity Catalog OSS**, evolving schema, and time-travel queries — so we
don't conflate the *choice* with the *mechanics*.

## 3.4 Build — start the object store and create the warehouse

Step 1 — start storage and confirm it's healthy:

```bash
./lakehouse start
./lakehouse status
```

Expected: the object store shows healthy in the status table. The S3 endpoint is
reachable at `http://localhost:8333` (containers reach it at
`http://host.docker.internal:8333`).

> Reminder from Chapter 2: the object store and Postgres run **natively on your
> host**, not as containers. If storage isn't healthy, confirm the host service is
> running before anything else.

Step 2 — confirm the storage credentials in your `.env`:

```bash
# .env (set in Chapter 2)
S3_ENDPOINT=http://host.docker.internal:8333
S3_ACCESS_KEY=<your-key>
S3_SECRET_KEY=<your-secret>
S3_BUCKET=lakehouse
S3_WAREHOUSE=s3a://lakehouse/warehouse
```

Step 3 — verify the warehouse bucket exists and is listable over the S3
API (using any S3 client, e.g. the AWS CLI pointed at the local endpoint):

```bash
aws --endpoint-url http://localhost:8333 s3 ls s3://lakehouse/
```

Expected: the `lakehouse` bucket lists successfully (empty is fine — we haven't
written tables yet). If the bucket doesn't exist, create it:

```bash
aws --endpoint-url http://localhost:8333 s3 mb s3://lakehouse
```

## 3.5 Checkpoint

- The object store is healthy in `./lakehouse status`.
- The `lakehouse` bucket exists and is listable over the S3 API.
- Your `.env` points `S3_WAREHOUSE` at `s3a://lakehouse/warehouse`.
- You can state, in one sentence each, **why SeaweedFS** and **why Iceberg**.
  (The catalog — Unity Catalog OSS — is confirmed in Chapter 5.)

## 3.6 Recap & what's next

- Storage is **two decisions**: an **object store** for the bytes and a chosen
  **open table format** for the structure.
- We run **SeaweedFS** (S3 API on `:8333`) and will use **Iceberg**.
- Our warehouse location is `s3a://lakehouse/warehouse`.
- **Next — Chapter 4, Compute:** bring up Spark and connect to it with a **Spark
  Connect** thin client (`sc://`) to run a first job against this storage.

![Progress: Storage complete, Compute next](../figures/ch03/fig-3.4-progress-storage-done.svg)

**Figure 3.4** — Progress map with **Storage ✓** and **Compute** highlighted next.

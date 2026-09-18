# Chapter 6 — Ingestion: landing raw data in the Bronze layer

## 6.0 What you'll build

The first data flow. You have storage (Ch 3), compute (Ch 4), and real tables
(Ch 5) — now you get *data into them*. You'll generate realistic order data and
**ingest** it as-is into a **Bronze** table: the raw, faithful landing zone that
every later transformation builds on. Along the way you'll meet the ingestion
vocabulary a practitioner uses every day — **batch**, **ELT**, and **idempotency**
— and see why "land raw first" is the modern default.

![Progress: Ingestion](../figures/ch06/fig-6.3-progress-ingestion.svg)

**Figure 6.3** — Progress map with **Ingestion** highlighted.

## 6.1 Learning objectives

By the end of this chapter you can:

- Define **ingestion** and explain the role of the **Bronze** layer.
- Contrast **ELT** with ETL and say why lakehouses default to ELT.
- Explain **idempotency** and why an ingestion job must be safely re-runnable.
- Generate test data and ingest it into an Iceberg Bronze table with Spark Connect.

## 6.2 What ingestion is

> **Ingestion** *(canonical term)*: getting raw data from a source system into the
> lakehouse, with as little transformation as possible.

The guiding principle of Bronze is **fidelity**: capture the source data *exactly
as it arrived* — same fields, same values, warts and all — plus a little metadata
about *when* and *how* you got it. You do not clean, join, or reshape here. Why?
Because raw data you've faithfully stored can always be re-processed later if your
logic changes; data you "cleaned" on the way in and threw away is gone forever.

> **Bronze / raw** *(canonical term)*: the first medallion tier — an
> as-faithful-as-possible copy of source data, the immutable foundation for
> everything downstream.

## 6.3 ELT, not ETL

> **ELT** *(canonical term)*: **E**xtract and **L**oad the raw data first, then
> **T**ransform it *inside* the lakehouse — as opposed to ETL, which transforms
> *before* loading.

The classic warehouse world did **ETL**: transform data in a separate tool, then
load only the polished result. The lakehouse inverts this. Storage is cheap and
compute is powerful and co-located, so you **load raw first** (that's Bronze) and
transform *in place* with Spark (that's Chapter 7). This is faster to build, keeps
the untouched source for re-processing, and means your transformations are just
more queries against tables you already have.

![ETL vs. ELT](../figures/ch06/fig-6.1-etl-vs-elt.svg)

**Figure 6.1** — ETL transforms before loading and discards the raw; ELT loads
raw into Bronze first, then transforms inside the lakehouse — keeping the source.

## 6.4 Idempotency: the property that makes ingestion safe

> **Idempotency** *(canonical term)*: re-running a job produces the same result as
> running it once — no duplicates, no drift.

Ingestion jobs fail and get retried — a network blip, a restarted worker, a manual
re-run. If a retry double-inserts yesterday's orders, your Bronze table is now
wrong, and so is everything built on it. An **idempotent** ingest is one you can
run twice safely. The usual techniques: ingest a bounded, identified batch (e.g.
"orders for 2026-01-05"), and either **overwrite that partition** or **merge on a
key** rather than blindly appending.

### Under the hood — why append-only bites you

A naive `INSERT INTO … SELECT * FROM source` is *not* idempotent: run it twice and
every row is duplicated. The fixes Iceberg gives you: `INSERT OVERWRITE` a specific
partition (replaces just that slice), or `MERGE INTO … WHEN MATCHED / WHEN NOT
MATCHED` on a business key (upsert). We use a partition overwrite below because
Bronze is organized by ingest date — re-running a day's ingest simply replaces
that day.

## 6.5 Build — generate data and ingest into Bronze

Step 1 — generate realistic order data with the CLI's built-in generator:

```bash
./lakehouse testdata generate
./lakehouse testdata stats
```

Expected: the generator writes a batch of synthetic order records (to a source
location / raw files) and `stats` prints how many records were produced. This is
our stand-in for a real source system.

Step 2 — connect a thin client and read the raw source into a DataFrame,
adding ingestion metadata (the "how/when we got it" that Bronze should carry):

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Read the raw generated orders (path/format per the generator's output).
raw = spark.read.json("s3a://lakehouse/raw/orders/")

bronze = (
    raw
    .withColumn("_ingested_at", F.current_timestamp())   # when we landed it
    .withColumn("_source",      F.lit("orders-generator")) # where it came from
    .withColumn("_ingest_date", F.to_date(F.current_timestamp()))  # partition key
)
```

Step 3 — write to the Bronze Iceberg table **idempotently**, partitioned
by ingest date so a re-run replaces just that day rather than duplicating it:

```python
# Create the Bronze table once, partitioned by ingest date.
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.bronze.orders_raw (
        order_id STRING, customer_id STRING, amount DOUBLE, status STRING,
        created_at TIMESTAMP, _ingested_at TIMESTAMP, _source STRING, _ingest_date DATE
    )
    USING iceberg
    PARTITIONED BY (_ingest_date)
""")

# Idempotent load: overwrite only the partition(s) present in this batch.
(bronze.writeTo("iceberg.bronze.orders_raw")
       .overwritePartitions())     # re-running the same day is safe
```

Step 4 — verify the landing, and prove idempotency by running Step 3
again and confirming the count does **not** change:

```python
spark.table("iceberg.bronze.orders_raw").count()
# run the write again … then:
spark.table("iceberg.bronze.orders_raw").count()   # same number — no duplicates
```

Expected: the second run leaves the row count unchanged — that's idempotency you
can see.

## 6.6 Batch now, streaming later

What you just built is **batch** ingestion:

> **Batch** *(canonical term)*: processing a bounded chunk of data (e.g. "today's
> orders") on a schedule or on demand.

Batch is the right default for most ingestion and the easiest to reason about. In
Chapter 8 you'll build the **streaming** counterpart — the *same* Bronze table fed
continuously from Kafka as events arrive — and see when each approach fits. For now,
bounded batches keep idempotency simple.

## 6.7 Checkpoint

- `./lakehouse testdata generate` produced records and `testdata stats` counted them.
- You ingested the raw orders into `iceberg.bronze.orders_raw` over `sc://`, adding
  `_ingested_at` / `_source` / `_ingest_date` metadata.
- The Bronze table is partitioned by `_ingest_date`.
- Running the write twice left the row count unchanged — the ingest is idempotent.

## 6.8 Recap & what's next

- **Ingestion** lands raw source data into **Bronze** with maximum fidelity and
  minimal transformation.
- Lakehouses default to **ELT**: load raw first, transform in place later.
- Ingestion must be **idempotent** — use partition overwrite or key-based merge, not
  blind append.
- This was **batch**; **streaming** ingestion of the same table comes in Chapter 8.
- **Next — Chapter 7, Transformation:** turn raw Bronze into clean Silver and
  business-ready Gold with **Spark Declarative Pipelines**.

![Progress: Ingestion complete, Transformation next](../figures/ch06/fig-6.4-progress-ingestion-done.svg)

**Figure 6.4** — Progress map with **Ingestion ✓** and **Transformation** next.

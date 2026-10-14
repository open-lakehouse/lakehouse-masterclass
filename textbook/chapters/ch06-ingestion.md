# Chapter 6: Ingestion

This is the first data flow, and a turning point in the course. You have storage (Chapter 3), compute (Chapter 4), and real tables (Chapter 5). Now you get data into them. You'll generate realistic order data, stand in for a source system, and ingest it as-is into a Bronze table: the raw, faithful landing zone that every later transformation builds on. Along the way you'll meet the ingestion vocabulary a practitioner uses every day, batch, ELT, and idempotency, and see why "land raw first" is the modern default.

Chapter 5's `bronze.orders` was a hands-on scratch table for learning Iceberg's mechanics (inserts, schema evolution, time travel, deliberately breaking it and rolling back). Here you build the pipeline's real Bronze table, `bronze.orders_raw`, the one Chapter 7 transforms into Silver and Gold and Chapter 8 streams into. From here on, `orders_raw` is the Bronze table that matters.

Everything before this was infrastructure: places to put data and engines to act on it. From here on, you're moving actual data through the system. Ingestion is where the lakehouse stops being a set of empty rooms and starts being a working home for your data. And the single most important habit you'll build here, landing raw data faithfully before touching it, is the one that separates engineers who sleep well from engineers who get paged.

![Progress: Ingestion](../figures/ch06/fig-6.1-progress-ingestion.svg)

**Figure 6.1**. Progress map with **Ingestion** highlighted.

## Learning objectives

By the end of this chapter you'll be able to:

- Define ingestion and explain the role of the Bronze layer.
- Contrast ELT with ETL and say why lakehouses default to ELT.
- Explain idempotency and why an ingestion job must be safely re-runnable.
- Write a reusable, date-parameterized `lakehouse.ingest.ingest_orders` module that lands one day's data into an Iceberg Bronze table idempotently, the same function Chapter 9 will schedule.
- Choose a partition spec that keeps several writers (a batch job and, in Chapter 8, a stream) from overwriting each other's rows.

## What ingestion is, and the golden rule of Bronze

> **Ingestion** *(canonical term)*: getting raw data from a source system into the lakehouse, with as little transformation as possible.

The guiding principle of Bronze is fidelity: capture the source data exactly as it arrived, same fields, same values, warts and all, plus a little metadata about when and how you got it. You do not clean, join, or reshape here. Why? Because raw data you've faithfully stored can always be re-processed later if your logic changes; data you "cleaned" on the way in and threw away is gone forever.

Let this sink in, because it's counterintuitive to beginners who want to "fix" data immediately. Imagine you ingest orders and, being helpful, you drop rows with a null `customer_id` because "those are obviously broken." Six months later you learn that a null `customer_id` actually meant "guest checkout," a huge, valuable segment. If you'd kept the raw data, you'd just re-run your transformations with the new understanding. But you threw those rows away at ingest, so they're gone. The golden rule of Bronze: never lose information you can't recreate. Clean data in later layers, where mistakes are reversible.

> **Bronze / raw** *(canonical term)*: the first medallion tier, an as-faithful-as-possible copy of source data, the immutable foundation for everything downstream.

Think of Bronze as the crime-scene photographer. The photographer doesn't tidy up the scene, move the furniture, or decide what's relevant; they capture exactly what's there, timestamped, so any later investigation can rely on a faithful record. Your Bronze table is that faithful record of what the source actually sent.

## ELT, not ETL

> **ELT** *(canonical term)*: extract and load the raw data first, then transform it inside the lakehouse, as opposed to ETL, which transforms before loading.

The classic warehouse world did ETL: transform data in a separate tool, then load only the polished result. The lakehouse inverts this. Storage is cheap and compute is powerful and co-located, so you load raw first (that's Bronze) and transform in place with Spark (that's Chapter 7). This is faster to build, keeps the untouched source for re-processing, and means your transformations are just more queries against tables you already have.

The reason the industry flipped from ETL to ELT is economics plus regret-avoidance. In the old world, storage was expensive, so you couldn't afford to keep raw data: you transformed it down to just what you needed and discarded the rest (and lived with the golden-rule violations that caused). In the lakehouse world, storage is so cheap that keeping every raw byte forever is affordable, and compute is powerful enough to transform on demand. So the calculus reversed: load everything raw, transform later, keep the raw around forever as insurance. Every serious lakehouse follows this pattern.

![ETL vs. ELT](../figures/ch06/fig-6.2-etl-vs-elt.svg)

**Figure 6.2**. ETL transforms before loading and discards the raw; ELT loads raw into Bronze first, then transforms inside the lakehouse, keeping the source.

## Idempotency: the property that makes ingestion safe

> **Idempotency** *(canonical term)*: re-running a job produces the same result as running it once, no duplicates, no drift.

Ingestion jobs fail and get retried: a network blip, a restarted worker, a manual re-run. If a retry double-inserts yesterday's orders, your Bronze table is now wrong, and so is everything built on it. An idempotent ingest is one you can run twice safely. The usual techniques: ingest a bounded, identified batch (for example, "orders for 2026-01-05"), and either overwrite that partition or merge on a key rather than blindly appending.

This is not a theoretical nicety; it's the difference between a pipeline you trust and one you babysit. Real pipelines fail constantly for boring reasons: a cloud region hiccups, a container gets OOM-killed, someone bumps a deploy. The pipelines that survive are the ones where "just run it again" is always safe. If re-running might duplicate data, then every failure becomes a delicate manual cleanup, and every 3 a.m. page becomes a judgment call about whether it's safe to retry. Idempotency turns retries from scary into routine. It's arguably the single most important property of a production data job.

A quick mental test for idempotency: "If I run this exact job twice in a row, is the table identical to running it once?" If yes, you're idempotent. If running it twice doubles rows, drifts counts, or changes results, you're not, and you have a latent bug waiting for the next retry.

### Under the hood: why append-only bites you

A naive `INSERT INTO ... SELECT * FROM source` is not idempotent: run it twice and every row is duplicated. The fixes Iceberg gives you: `INSERT OVERWRITE` a specific partition (replaces just that slice), or `MERGE INTO ... WHEN MATCHED / WHEN NOT MATCHED` on a business key (upsert). We use a partition overwrite below because Bronze is organized by ingest date (and by source, for a reason you'll see in Step 2), so re-running a day's ingest simply replaces that day.

When would you choose merge instead of partition overwrite? When you're ingesting updates to existing records rather than fresh daily batches, for example an order whose status changes from `pending` to `shipped`. A merge on `order_id` updates the matched row in place and inserts genuinely new ones, so you converge on the correct latest state no matter how many times you run it. Partition overwrite is simpler and perfect for append-mostly daily data; merge is the tool when records mutate. Both are idempotent; they just fit different shapes of source data.

## Build: generate data and ingest into Bronze

In production, the source would be an application database, an event stream, a partner's API export, or a file drop. To keep this chapter self-contained, you'll generate realistic, deliberately messy order records and write them to a raw location in object storage, standing in for a real source system. (When you have a real dataset, this is where it slots in: drop each day's export under `s3a://lakehouse/raw/orders/ingest_date=YYYY-MM-DD/` and the ingest below is unchanged.)

You'll write two real modules in your `lakehouse` package, not throwaway scripts: a generator and an ingest. The ingest in particular is the one your Airflow DAG imports and schedules in Chapter 9, so it's worth writing properly now.

First, add the raw location to your config so the generator and ingest agree on where raw data lands. Extend `lakehouse/config.py` from Chapter 4:

```python
# lakehouse/config.py  (add this line)
RAW_PREFIX = os.environ.get("RAW_PREFIX", "s3a://lakehouse/raw")
```

**Step 1: write the generator.** Three design choices in it are worth understanding before you read the code, because each one is there for a later chapter:

- **One folder per day.** The generator writes each day's export to its own folder, `raw/orders/ingest_date=2026-01-05/`. That is how real file drops usually arrive, and it is what lets the ingest pick up exactly one day's batch. Without it, the ingest would read every file ever dropped, and in Chapter 9 a backfill of seven days would copy the same records into seven partitions.
- **Seeded by the date.** The random generator is seeded with the date string, so `2026-01-05` always produces exactly the same records, on your machine and on ours. Re-running is reproducible (which makes the idempotency check below meaningful), and every count and revenue figure in this chapter and the next will match what you see.
- **Messy in the ways real exports are.** Each kind of mess is there so Chapter 7's cleaning has real work to do: a null `customer_id` (a guest checkout, which is valid and must survive cleaning), a broken `-1.0` amount, a record with no `order_id` at all, inconsistent casing (`PLACED` instead of `placed`), and a handful of records sent twice, the way an at-least-once source re-sends what it isn't sure landed.

Save this as `lakehouse/generate.py`:

```python
# lakehouse/generate.py
"""Stand-in for a source system: drops one day's raw order export into object storage."""
import random
from datetime import datetime, timedelta

from lakehouse.config import RAW_PREFIX
from lakehouse.session import get_spark

STATUSES = ["placed", "placed", "placed", "shipped", "cancelled"]
RAW_SCHEMA = "order_id STRING, customer_id STRING, amount DOUBLE, status STRING, created_at STRING"


def raw_path(ingest_date: str) -> str:
    """Where the source drops the export for one day. One folder per day is what
    lets the ingest (and later a backfill) pick up exactly one day's batch."""
    return f"{RAW_PREFIX}/orders/ingest_date={ingest_date}/"


def make_orders(ingest_date: str, n: int = 1000) -> list[tuple]:
    """Build one day's records, deliberately messy in the ways real exports are.
    Seeded by the date, so the same day always produces the same data: re-running
    is reproducible, and your counts will match the book's."""
    rng = random.Random(ingest_date)
    day = datetime.fromisoformat(ingest_date)
    first_id = 1000 + (day - datetime(2026, 1, 1)).days * 10_000   # ids never collide across days
    rows = []
    for i in range(n):
        order_id = f"o-{first_id + i}"
        customer = f"c-{rng.randint(1, 200)}"
        amount = round(rng.uniform(5, 500), 2)
        status = rng.choice(STATUSES)
        created = day + timedelta(hours=8, seconds=rng.randint(0, 12 * 3600))
        roll = rng.random()
        if roll < 0.10:
            customer = None                   # guest checkout: valid, keep it
        elif roll < 0.13:
            amount = -1.0                     # a broken amount from a flaky upstream
        elif roll < 0.15:
            order_id = None                   # a record with no key at all
        elif roll < 0.20:
            status = status.upper()           # inconsistent casing ("PLACED")
        rows.append((order_id, customer, amount, status, created.isoformat()))
    # At-least-once delivery: the source re-sent a few records it wasn't sure landed.
    rows += rng.sample([r for r in rows if r[0] is not None], k=n // 50)
    return rows


def generate_orders(ingest_date: str = "2026-01-05", n: int = 1000) -> int:
    """Write one day's raw export as JSON files under raw_path(ingest_date)."""
    spark = get_spark("generate-orders")
    df = spark.createDataFrame(make_orders(ingest_date, n), RAW_SCHEMA)
    df.write.mode("overwrite").json(raw_path(ingest_date))
    return df.count()


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-05"
    print("wrote", generate_orders(date), "raw order records for", date)
```

Run it as a module, passing the day you want:

```bash
python -m lakehouse.generate 2026-01-05
# -> wrote 1020 raw order records for 2026-01-05
```

**What just happened?** You stood in for a source system, but notice this is a set of *functions in your package*, not a loose script: `make_orders` can be called from a test without Spark at all, and `raw_path` is the single definition of where a day's drop lives, which the ingest imports rather than re-typing. Why 1020 and not 1000? The generator made 1000 records and then the "source" re-sent 20 of them. Those duplicates, plus the nulls, bad amounts, and odd casing, are exactly what Chapter 7 will clean out.

See what actually landed. The write didn't produce one file; Spark writes one file per partition of the DataFrame, and a DataFrame built from a local list is split into as many slices as your cluster has cores, so list the day's folder:

```bash
aws --endpoint-url http://localhost:8333 s3 ls s3://lakehouse/raw/orders/ingest_date=2026-01-05/
# 2026-10-12 05:03:31          0 _SUCCESS
# 2026-10-12 05:03:31       7060 part-00000-a30d0e41-....json
# 2026-10-12 05:03:31       7180 part-00001-a30d0e41-....json
# ...                                  (one part file per slice; the count depends on your cores)
```

The `_SUCCESS` marker is Spark's "this write finished cleanly" flag, and the `part-*` files hold the JSON, one record per line. Peek at one if you like (`aws ... s3 cp s3://lakehouse/raw/orders/ingest_date=2026-01-05/part-00000-<...>.json - | head -3`). This is a raw drop, plain JSON, no table semantics yet; that comes when you land it into Iceberg below.

**Step 2: write the ingest.** This module reads one day's raw drop, tags it with ingestion metadata (the "faithful capture" part of Bronze), and lands it into the Bronze Iceberg table idempotently. It's the one Chapter 9 schedules, so write it as a real, parameterized function. Save it as `lakehouse/ingest.py`:

```python
# lakehouse/ingest.py
import logging

from pyspark.sql import functions as F

from lakehouse.config import CATALOG
from lakehouse.generate import raw_path
from lakehouse.session import get_spark

log = logging.getLogger(__name__)

BRONZE = f"{CATALOG}.bronze.orders_raw"


def ingest_orders(ingest_date: str) -> int:
    """Land one day's raw orders into the Bronze table, idempotently.

    Reads only that day's raw drop and replaces only that day's partition, so a
    retry never duplicates and a backfill of any past day lands exactly that day.
    Returns the number of rows now in the day's partition."""
    spark = get_spark("ingest-orders")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
            order_id STRING, customer_id STRING, amount DOUBLE, status STRING,
            created_at STRING, _ingested_at TIMESTAMP, _source STRING, _ingest_date DATE
        ) USING iceberg PARTITIONED BY (_ingest_date, _source)
    """)

    # Faithful capture: keep the source columns as-is, add only ingestion metadata.
    # An explicit schema means a malformed file can't silently change column types.
    raw = spark.read.schema("order_id STRING, customer_id STRING, amount DOUBLE, "
                            "status STRING, created_at STRING").json(raw_path(ingest_date))
    bronze = (raw
        .withColumn("_ingested_at", F.current_timestamp())             # when we landed it
        .withColumn("_source",      F.lit("orders-generator"))         # where it came from
        .withColumn("_ingest_date", F.lit(ingest_date).cast("date")))  # which batch

    # Idempotent load: replace only the partitions present in `bronze`, which is
    # (this day, this source). The stream (Chapter 8) writes _source='orders-stream',
    # so its rows live in partitions this overwrite can never touch.
    bronze.writeTo(BRONZE).overwritePartitions()

    n = (spark.table(BRONZE)
         .where((F.col("_ingest_date") == ingest_date) & (F.col("_source") == "orders-generator"))
         .count())
    log.info("%s: %d rows in partition %s", BRONZE, n, ingest_date)
    return n


if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-01-05"
    print(ingest_orders(date), "rows in bronze.orders_raw for", date)
```

Run it for one day's batch:

```bash
python -m lakehouse.ingest 2026-01-05
# -> 1020 rows in bronze.orders_raw for 2026-01-05
```

**What just happened?** Four production habits worth calling out.

First, the three `_`-prefixed metadata columns answer questions you'll be grateful for later: when we landed it (`_ingested_at`), where it came from (`_source`), and which batch it is (`_ingest_date`). Recording the circumstances of ingestion is the one exception to "don't touch the data": it's faithful capture, not transformation.

Second, and this is the one that matters most, `ingest_date` is a *parameter*, not `today()`. The job lands "the 2026-01-05 batch" no matter when it runs, reading only that day's folder, which is exactly what makes re-running and backfilling correct. In Chapter 9 the orchestrator will pass a different date to backfill history, reusing this exact function unchanged.

Third, the read uses an explicit schema instead of letting Spark infer one from the JSON. Inference looks at whatever happens to be in the files, so one malformed export could silently turn `amount` into a string. With an explicit schema, the table's shape is a decision you made, not an accident of the data.

Fourth, the partitioning: `PARTITIONED BY (_ingest_date, _source)`. `overwritePartitions()` replaces every partition that appears in the DataFrame being written, and nothing else. Because this DataFrame only holds `_ingest_date = 2026-01-05` and `_source = 'orders-generator'`, that one slice is all it can ever replace. Why include `_source`? Because in Chapter 8 a streaming job will write into this same table, tagged `_source = 'orders-stream'` and dated with the day it arrived. If the table were partitioned by date alone, a batch run for today would replace the whole of today's partition, streamed rows included, and they would be gone. (We hit exactly this data loss while testing the course, before adding `_source` to the partition spec.) The rule it teaches is general: when several writers share a table and any of them overwrites, give each writer its own partitions.

**Step 3: prove idempotency** by running the ingest a second time and confirming the count does not change.

```bash
python -m lakehouse.ingest 2026-01-05    # run the exact same batch again
# -> 1020 rows in bronze.orders_raw for 2026-01-05      (unchanged)
```

Expected: the second run leaves the day's count at exactly `1020`. That's idempotency you can see, and because `ingest_orders` overwrites only the `2026-01-05` partition, running it ten more times still leaves 1020. This is worth doing deliberately once, because "the count didn't change when I ran it twice" is the most satisfying and concrete proof that your ingest is safe to retry. If you'd used a naive append, the count would have doubled to `2040`; try that too (in a scratch table) so you feel the difference.

Now look at how Iceberg laid the Bronze table out, because the partitioning is physical. Unity Catalog places each table under a catalog-assigned prefix (as you saw in Chapter 5), so filter the listing for the partition folders:

```bash
aws --endpoint-url http://localhost:8333 s3 ls --recursive s3://lakehouse/warehouse/ | grep "_ingest_date="
# .../data/_ingest_date=2026-01-05/_source=orders-generator/00000-....parquet
# .../data/_ingest_date=2026-01-05/_source=orders-generator/00000-....parquet
```

Notice the `_ingest_date=2026-01-05/_source=orders-generator/` folders in the path: that is `PARTITIONED BY (_ingest_date, _source)` made concrete. Each day and source lands under its own prefix, which is exactly why `overwritePartitions()` can replace one slice without touching another. And notice there are two Parquet files, one from each run. The second run didn't delete the first file; it wrote a new one and committed a snapshot that no longer references the old one. The old file stays in the bucket because the previous snapshot still points at it, which is the time-travel machinery from Chapter 5 at work. (Expiring old snapshots, a maintenance job, is what eventually deletes such files.)

Finally, look at the data itself, because Bronze should look exactly as messy as the source:

```python
from lakehouse.session import get_spark
spark = get_spark()
spark.sql("""
    SELECT order_id, customer_id, amount, status, created_at, _source
    FROM iceberg.bronze.orders_raw
    WHERE order_id IS NULL OR amount <= 0 OR status <> lower(status)
    LIMIT 5
""").show(truncate=False)
```

Expected (the first rows the query happens to return; yours may be a different five):

```
+--------+-----------+------+---------+-------------------+----------------+
|order_id|customer_id|amount|status   |created_at         |_source         |
+--------+-----------+------+---------+-------------------+----------------+
|o-41389 |c-123      |467.93|PLACED   |2026-01-05T13:39:30|orders-generator|
|o-41395 |c-142      |472.04|CANCELLED|2026-01-05T11:03:26|orders-generator|
|o-41408 |c-65       |130.87|PLACED   |2026-01-05T14:26:17|orders-generator|
|o-41412 |c-75       |225.9 |CANCELLED|2026-01-05T15:27:15|orders-generator|
|o-41428 |c-2        |-1.0  |shipped  |2026-01-05T12:33:29|orders-generator|
+--------+-----------+------+---------+-------------------+----------------+
```

Uppercase statuses, a `-1.0` amount, `created_at` still a string: Bronze kept every wart, which is the point. Nothing is lost, so Chapter 7 can decide what "clean" means and change its mind later.

## Batch now, streaming later

What you just built is batch ingestion:

> **Batch** *(canonical term)*: processing a bounded chunk of data (for example, "today's orders") on a schedule or on demand.

Batch is the right default for most ingestion and the easiest to reason about, precisely because "a bounded chunk with a clear identity" (today's orders) makes idempotency straightforward: you know exactly which partition to overwrite. In Chapter 8 you'll build the streaming counterpart, the same Bronze table fed continuously from Kafka as events arrive, and see when each approach fits. Streaming trades batch's simplicity for freshness, and it makes idempotency harder (there's no neat "today's batch" boundary), which is why we build batch first. For now, bounded batches keep the mental model clean.

## Troubleshooting

- **`spark.read.json` fails with "Path does not exist" or reads nothing.** You ingested a date you never generated. Run `python -m lakehouse.generate <date>` for that date first, and list `s3://lakehouse/raw/orders/` to see which days exist.
- **`ClassNotFoundException` for an S3A credentials provider, or `S3AFileSystem` not found.** The Connect server's `spark-defaults.conf` is wrong: `hadoop-aws` must match Spark's bundled Hadoop (3.4.2 for Spark 4.1.0) and the provider must be the AWS SDK v2 class from Chapter 4.
- **`Could not rename ... _temporary ...` when the generator writes.** Your object store mishandles the directory markers S3A uses during a commit. Use the SeaweedFS version pinned in Chapter 3; older releases have this bug.
- **Row count doubles on the second run.** You're appending, not overwriting. Confirm you're using `.overwritePartitions()` (or a merge), not `INSERT INTO` or `.append()`.
- **`CREATE TABLE` fails with a catalog error.** The Iceberg catalog isn't wired up or Unity Catalog OSS isn't running. Revisit Chapter 5: the catalog must be configured server-side and the service up before writes succeed.
- **Schema mismatch on write.** The DataFrame's columns don't line up with the table definition. Check that your added metadata columns match the `CREATE TABLE` schema exactly (names and types). If you created `orders_raw` with an older definition (for example partitioned by date only), drop it (`DROP TABLE iceberg.bronze.orders_raw PURGE`) and re-ingest; changing a partition spec is not something `CREATE TABLE IF NOT EXISTS` will do for you. Note `created_at` is kept as a `STRING` in Bronze: faithful capture means not reinterpreting the source's timestamp format yet; that typing happens in Silver.

## Checkpoint

- `lakehouse/generate.py` wrote 1020 raw order records to `s3a://lakehouse/raw/orders/ingest_date=2026-01-05/`.
- You ingested the raw orders into `iceberg.bronze.orders_raw` over `sc://`, adding `_ingested_at`, `_source`, and `_ingest_date` metadata.
- The Bronze table is partitioned by `_ingest_date` and `_source`, and you can explain why `_source` is in the spec.
- Running the write twice left the row count unchanged: the ingest is idempotent.
- You can explain the golden rule of Bronze and why it matters.

## Try it yourself

1. **Prove non-idempotency, safely.** In a scratch table, ingest the same batch with a plain append twice and watch the count double. Now you've felt why `overwritePartitions()` matters.
2. **Add a second day.** Generate and ingest the next day, then re-run the first day:

   ```bash
   python -m lakehouse.generate 2026-01-06
   python -m lakehouse.ingest 2026-01-06
   python -m lakehouse.ingest 2026-01-05     # re-run day one
   ```

   Then count per partition:

   ```python
   spark.sql("""SELECT _ingest_date, count(*) AS n, count(DISTINCT _ingested_at) AS runs
                FROM iceberg.bronze.orders_raw GROUP BY 1 ORDER BY 1""").show()
   ```

   Both days show 1020 rows. Re-running day one replaced only day one, which you can see because `2026-01-06` still carries its own, older `_ingested_at`.
3. **Inspect the metadata columns.** Query a few rows and look at `_ingested_at` and `_source`. Imagine debugging a data issue in six months: how would these columns help you?
4. **Design a merge.** Sketch (in SQL) how you'd ingest order status updates with a `MERGE INTO ... ON order_id` instead of a partition overwrite. When would you prefer each?

## Check your understanding

- State the golden rule of Bronze and give an example of violating it.
- Why did the industry shift from ETL to ELT? Name the two forces.
- What is idempotency, and what's the quick mental test for whether a job has it?
- When would you choose a key-based merge over a partition overwrite?

## Recap and what's next

- Ingestion lands raw source data into Bronze with maximum fidelity and minimal transformation: never lose information you can't recreate.
- Lakehouses default to ELT: load raw first, transform in place later, keep the raw forever.
- Ingestion must be idempotent: use partition overwrite or key-based merge, not blind append, so retries are always safe.
- You wrote it as real package modules (`lakehouse/generate.py`, `lakehouse/ingest.py`) with the ingest parameterized by date and reading one day's folder, so Chapter 9 can schedule and backfill it by importing the same function, not copying it.
- When several writers share a table, partition so each one owns its slice; that's why Bronze is partitioned by `_source` as well as date.
- This was batch; streaming ingestion of the same table comes in Chapter 8.
- **Next, Chapter 7, Transformation:** turn raw Bronze into clean Silver and business-ready Gold with Spark Declarative Pipelines.

![Progress: Ingestion complete, Transformation next](../figures/ch06/fig-6.3-progress-ingestion-done.svg)

**Figure 6.3**. Progress map with **Ingestion done** and **Transformation** next.

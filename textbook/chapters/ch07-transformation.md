# Chapter 7 — Transformation: the medallion with Spark Declarative Pipelines

## 7.0 What you'll build

The heart of the lakehouse. You have raw orders in **Bronze** (Ch 6); now you
refine them into clean **Silver** and business-ready **Gold** — the **medallion**
pattern. And you'll build it the modern way: with **Spark Declarative Pipelines
(SDP)**, where you *declare* each table and its dependencies and let the engine
figure out the execution graph, incremental refresh, and data-quality enforcement.
You describe the *what*; SDP handles the *how*.

![Progress: Transformation](../figures/ch07/fig-7.3-progress-transform.svg)

**Figure 7.3** — Progress map with **Transformation** highlighted.

## 7.1 Learning objectives

By the end of this chapter you can:

- Explain the **medallion** architecture: Bronze → Silver → Gold and what each tier
  is for.
- Explain what **Spark Declarative Pipelines** are and how they differ from
  hand-written pipeline scripts.
- Declare Silver and Gold tables with dependencies and let SDP derive the graph.
- Attach **data quality** expectations to a table and see them enforced.

## 7.2 The medallion: Bronze → Silver → Gold

> **Medallion (Bronze/Silver/Gold)** *(canonical term)*: a layered refinement
> pattern — raw (Bronze) → cleaned and conformed (Silver) → aggregated,
> business-ready (Gold).

Each tier has one job:

- **Bronze** — raw, faithful landing (built in Chapter 6). Never edited.
- **Silver** — *cleaned and conformed*: valid types, deduplicated, bad rows
  filtered, columns standardized. One trustworthy row per real-world event.
- **Gold** — *business-ready*: aggregated, joined, modeled for how people actually
  consume it (e.g. daily revenue by channel). This is what dashboards and models
  read.

You refine *forward* — each tier reads the one below it — so a logic change means
re-deriving Silver/Gold from the untouched Bronze, never re-fetching the source.

![The medallion: Bronze to Silver to Gold](../figures/ch07/fig-7.1-medallion.svg)

**Figure 7.1** — Bronze (raw) → Silver (clean/conform) → Gold (aggregate/model),
each tier reading the one below, with data-quality gates between them.

## 7.3 What Spark Declarative Pipelines are

> **Spark Declarative Pipelines (SDP)** *(canonical term)*: a framework
> (`pyspark.pipelines`) where you *declare* the datasets that make up a pipeline and
> their dependencies; the engine derives the execution order, handles incremental
> refresh, and enforces data-quality expectations.

With a hand-written script you'd have to sequence the steps yourself, wire up
dependencies, decide what to recompute, and bolt on your own quality checks. SDP
flips that: you write one function per table, decorated to say "this *is* the
`silver_orders` table," and reference other tables by name. SDP reads all the
declarations, builds the dependency graph, and runs them in the right order — the
same declarative model that makes tools like dbt so productive, but native to
Spark and built **on Spark Connect** (Ch 4).

### Under the hood — SDP runs on Spark Connect

`pyspark.pipelines` uses Spark Connect internally. That's why we standardized on
Connect back in Chapter 4: the transformation layer is built on it. Your pipeline
definitions are declarative Python; SDP compiles them to plans that execute on the
Connect server.

## 7.4 Build — declare the medallion pipeline

An SDP pipeline is a set of decorated Python functions plus a small YAML that tells
SDP where the definitions live. You declare tables; you never call them in order
yourself.

Step 1 — the pipeline definitions (`pipeline_sdp.py`). Each function
returns a DataFrame and is registered as a table; dependencies are expressed by
reading other declared tables:

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# SILVER — clean & conform the raw Bronze orders.
@dp.table(name="iceberg.silver.orders")
@dp.expect_or_drop("valid_amount", "amount > 0")          # data-quality gate
@dp.expect_or_drop("has_id", "order_id IS NOT NULL")
def silver_orders():
    return (
        dp.read("iceberg.bronze.orders_raw")
          .dropDuplicates(["order_id"])                    # one row per order
          .withColumn("status", F.lower(F.col("status")))  # conform
          .select("order_id", "customer_id", "amount", "status", "created_at")
    )

# GOLD — daily revenue by status, business-ready.
@dp.table(name="iceberg.gold.daily_revenue")
def gold_daily_revenue():
    return (
        dp.read("iceberg.silver.orders")
          .withColumn("order_date", F.to_date("created_at"))
          .groupBy("order_date", "status")
          .agg(F.sum("amount").alias("revenue"),
               F.count("*").alias("order_count"))
    )
```

Step 2 — the pipeline spec (`spark-pipeline.yml`) points SDP at the
definitions:

```yaml
name: lakehouse-medallion
definitions:
  - glob:
      include: pipeline_sdp.py
```

Step 3 — run the pipeline. SDP reads the declarations, builds the graph
(Bronze → Silver → Gold), and executes in dependency order:

```bash
spark-pipelines run --spec scripts/pipelines/spark-pipeline.yml
```

Expected: SDP reports the resolved graph and materializes `iceberg.silver.orders`
then `iceberg.gold.daily_revenue`. You did not sequence them — SDP inferred that
Gold depends on Silver, which depends on Bronze.

Step 4 — query the Gold table over a thin client:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.table("iceberg.gold.daily_revenue").orderBy("order_date", "status").show()
```

Expected output (shape):

```
+----------+---------+-------+-----------+
|order_date|   status|revenue|order_count|
+----------+---------+-------+-----------+
|2026-01-05|cancelled|   88.0|          1|
|2026-01-05|   placed|  117.7|          3|
+----------+---------+-------+-----------+
```

## 7.5 Data quality as a first-class citizen

> **Data quality** *(canonical term)*: checks that data meets expectations before
> it's trusted downstream.

Notice the `@dp.expect_or_drop(...)` decorators on Silver. In SDP, quality rules are
**declared on the table**, not bolted on afterward. Each expectation is a named
boolean condition; SDP evaluates it on every row and can **drop** violating rows
(as here), **warn**, or **fail** the pipeline outright. Because the rule lives with
the table definition, it's visible, versioned, and enforced every run — no separate
validation job to forget.

![Data-quality expectations on a table](../figures/ch07/fig-7.2-data-quality.svg)

**Figure 7.2** — Rows flow through declared expectations; violations are
dropped (or warned/failed) so only trustworthy rows reach Silver.

## 7.6 Checkpoint

- `pipeline_sdp.py` declares `iceberg.silver.orders` and `iceberg.gold.daily_revenue`
  with dependencies expressed by `dp.read(...)`.
- `spark-pipelines run` materialized both tables in the correct order without you
  sequencing them.
- Silver carries `@dp.expect_or_drop` data-quality gates and dropped invalid rows.
- Querying `iceberg.gold.daily_revenue` returns aggregated, business-ready results.

## 7.7 Recap & what's next

- The **medallion** refines data forward: **Bronze** (raw) → **Silver** (clean) →
  **Gold** (business-ready).
- **Spark Declarative Pipelines** let you *declare* tables and dependencies; the
  engine derives the graph, refresh, and quality enforcement — built on Spark
  Connect.
- **Data quality** lives on the table as declared expectations, enforced every run.
- **Next — Chapter 8, Streaming:** feed the *same* Bronze table continuously from
  Kafka, and contrast streaming with the batch pipeline you just built.

![Progress: Transformation complete, Streaming next](../figures/ch07/fig-7.4-progress-transform-done.svg)

**Figure 7.4** — Progress map with **Transformation ✓** and **Streaming** next.

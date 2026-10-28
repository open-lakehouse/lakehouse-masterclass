# Chapter 7: Transformation

This is the heart of the lakehouse. You have raw orders in Bronze (Chapter 6); now you refine them into clean Silver and business-ready Gold, the medallion pattern. And you'll build it the modern way, with Spark Declarative Pipelines (SDP), where you declare each table and its dependencies and let the engine figure out the execution graph and incremental refresh. You describe the what; SDP handles the how.

This is the chapter people remember, because it's where raw data becomes useful. Bronze is faithful but ugly: duplicates, nulls, inconsistent casing, no aggregation. Nobody builds a dashboard on Bronze. The medallion is the assembly line that turns that raw material into something a business analyst, a dashboard, or a model can actually trust and use. And by building it declaratively, you write dramatically less plumbing than the old hand-scripted way, which is exactly how modern data teams work.

![Progress: Transformation](../figures/ch07/fig-7.1-progress-transform.svg)

**Figure 7.1**. Progress map with **Transformation** highlighted.

## Learning objectives

By the end of this chapter you'll be able to:

- Explain the medallion architecture: Bronze to Silver to Gold, and what each tier is for.
- Explain what Spark Declarative Pipelines are and how they differ from hand-written pipeline scripts.
- Declare Silver and Gold datasets with dependencies and let SDP derive the graph.
- Enforce data quality inside a dataset definition, and know what OSS SDP does and does not give you here.
- Factor cleaning into a reusable `lakehouse.quality` function and unit-test it on its own, independent of the pipeline.

## The medallion: Bronze to Silver to Gold

> **Medallion (Bronze/Silver/Gold)** *(canonical term)*: a layered refinement pattern, raw (Bronze) to cleaned and conformed (Silver) to aggregated, business-ready (Gold).

Each tier has one job:

- **Bronze**: raw, faithful landing (built in Chapter 6). Never edited.
- **Silver**: cleaned and conformed. Valid types, deduplicated, bad rows filtered, columns standardized. One trustworthy row per real-world event.
- **Gold**: business-ready. Aggregated, joined, modeled for how people actually consume it (for example, daily revenue by status). This is what dashboards and models read.

Why three tiers and not one big "clean it all up" step? Because each tier serves a different audience and changes at a different rate, and separating them makes the whole pipeline more robust and reusable. Think of an ore refinery: Bronze is the raw ore straight from the mine (kept exactly as dug), Silver is the refined metal (impurities removed, standardized into ingots), and Gold is the finished product (shaped into exactly what a customer ordered). You could try to go from ore to finished product in one giant step, but then every product change means re-mining. By keeping distinct tiers, a new Gold table (say, "revenue by customer region") just reads the existing trustworthy Silver, with no re-cleaning and no re-ingesting.

There's a practical reuse point hiding here. Silver is shared infrastructure: many different Gold tables read from the same clean Silver. You clean and deduplicate orders once in Silver, and then build a dozen business views on top. If you cleaned inside each Gold table instead, you'd repeat that logic a dozen times and they'd inevitably drift apart. Clean once in Silver; aggregate many ways in Gold.

You refine forward, each tier reading the one below it, so a logic change means re-deriving Silver and Gold from the untouched Bronze, never re-fetching the source. This is the ELT golden rule from Chapter 6 paying off: because Bronze is a faithful, permanent record, you can rebuild everything above it whenever your understanding improves.

![The medallion: Bronze to Silver to Gold](../figures/ch07/fig-7.2-medallion.svg)

**Figure 7.2**. Bronze (raw) to Silver (clean/conform) to Gold (aggregate/model), each tier reading the one below, with quality filtering between them.

## What Spark Declarative Pipelines are

> **Spark Declarative Pipelines (SDP)** *(canonical term)*: a framework (`pyspark.pipelines`, shipped with Spark 4.1+) where you declare the datasets that make up a pipeline and their dependencies; the engine derives the execution order and handles incremental refresh.

With a hand-written script you'd have to sequence the steps yourself, wire up dependencies, and decide what to recompute. SDP flips that: you write one function per dataset, decorated to say "this is the `silver.orders` materialized view," and reference other tables by reading them. SDP reads all the declarations, builds the dependency graph, and runs them in the right order: the same declarative model that makes tools like dbt so productive, but native to Spark and built on Spark Connect (Chapter 4).

The distinction between imperative and declarative is worth making crisp, because it's the core idea of the chapter. Imperative means you write the exact steps in order: "first build Silver, then build Gold, and if Silver changed also rebuild Gold." You're the one holding the whole sequence in your head. Declarative means you describe the desired result, "Silver is Bronze cleaned; Gold is Silver aggregated," and let the engine figure out the order, the dependencies, and what needs recomputing. It's the difference between giving someone turn-by-turn driving directions (imperative) and just giving them the destination address and a GPS (declarative). As pipelines grow to dozens or hundreds of tables, holding the imperative sequence in your head becomes impossible; the declarative approach scales because the engine does the bookkeeping.

Two primitives cover almost everything you'll do:

- `@dp.materialized_view`: a batch dataset, fully recomputed from its inputs each run. This is what Silver and Gold are in a batch medallion.
- `@dp.table`: a streaming table, fed incrementally by a streaming read. You'll use this in Chapter 8 when the same Silver is fed from Kafka.

Dependencies are not passed as arguments and are never called by you. SDP infers them from the reads inside each function: if `gold.daily_revenue`'s function reads `iceberg.silver.orders`, SDP knows Gold depends on Silver.

### Under the hood: SDP runs on Spark Connect

`pyspark.pipelines` runs on Spark Connect machinery. That's why we standardized on Connect back in Chapter 4: the transformation layer is built on it. The `spark-pipelines` CLI accepts `--remote sc://host:15002` to target your running Connect server, so your pipeline definitions execute on the same remote engine as everything else. Everything you learned about the thin-client model applies directly.

## Build: declare the medallion pipeline

SDP projects have a standard shape, and the tooling scaffolds it for you. Back in Chapter 2 you left a `pipelines/` folder empty for exactly this moment.

**Step 1: install the tooling and scaffold the project.** Two prerequisites first. Install the pipelines extra on your host, pinned to match your 4.1.0 Connect server (a bare `pip install 'pyspark[pipelines]'` now pulls a newer release, and client/server skew causes confusing errors). And make sure you have a JDK (Java 17 or newer) on your host: `spark-pipelines` is launched through `spark-class`, so it starts a small JVM even though Spark itself runs in a container. This is the one host tool in the course that needs Java; `java -version` tells you whether you have it.

```bash
pip install 'pyspark[pipelines]==4.1.0'
java -version            # 17 or newer
cd pipelines
spark-pipelines init --name medallion
```

That creates a `medallion/` subdirectory with a `spark-pipeline.yml` spec and a `transformations/` folder holding two example definitions (one Python, one SQL). Delete both example files in `transformations/`; you'll write your own.

**Step 2: point the spec at your catalog.** Edit `medallion/spark-pipeline.yml` so the pipeline writes into your Iceberg catalog and finds your transformation files:

```yaml
name: medallion
catalog: iceberg
database: silver
storage: file:///tmp/medallion-storage
libraries:
  - glob:
      include: transformations/**
```

The `catalog` is the Unity Catalog OSS catalog you built in Chapter 5. `database` is the default schema for any dataset whose name you don't qualify; leave it out and SDP falls back to a schema called `default`, which doesn't exist in your catalog, and the run fails with `SCHEMA_NOT_FOUND`. SDP does not create schemas for you, so `iceberg.silver` and `iceberg.gold` must already exist (you created them at the end of Chapter 5; if not, run `CREATE NAMESPACE IF NOT EXISTS iceberg.silver` and the same for `gold` now). `storage` is where SDP keeps pipeline state such as streaming checkpoints; note that this path is resolved by the Connect server, not your laptop, so `file:///tmp/...` means the server container's `/tmp`. That's fine for the materialized views here, which keep no state between runs. `libraries` tells SDP where your dataset definitions live.

**Step 3: put reusable cleaning in the package, then declare the datasets.** The Silver cleaning (drop bad rows, dedupe, conform types) is exactly the kind of logic you want reusable and testable, not buried in a pipeline file. Put it in `lakehouse/quality.py` as a plain DataFrame-in, DataFrame-out function:

```python
# lakehouse/quality.py
from pyspark.sql import DataFrame, functions as F


def clean_orders(df: DataFrame) -> DataFrame:
    """Silver-grade cleaning: drop invalid rows, dedupe, conform types.
    A pure transformation (DataFrame in, DataFrame out), so it is reusable and
    unit-testable on its own, without running the whole pipeline."""
    return (df
        .filter("order_id IS NOT NULL")                         # must have a key
        .filter("amount > 0")                                   # drop bad amounts
        .dropDuplicates(["order_id"])                           # one row per order
        .withColumn("status", F.lower(F.col("status")))         # conform casing
        .withColumn("amount", F.col("amount").cast("decimal(10,2)"))  # money is exact, not float
        .withColumn("created_at", F.to_timestamp("created_at"))       # string -> real timestamp
        .select("order_id", "customer_id", "amount", "status", "created_at"))
```

One line deserves a pause: casting `amount` to `DECIMAL(10,2)`. In Bronze the amount is a `DOUBLE`, a binary floating-point number, and floats can't represent most cents exactly. Sum a few hundred of them and the error shows: an earlier version of this pipeline, which left `amount` as a double, reported one day's cancelled revenue as `48309.499999999985`. Nobody wants that in a finance dashboard. A `DECIMAL(10,2)` stores exactly two digits after the point, so sums are exact. Conforming types like this is precisely Silver's job: Bronze keeps what the source sent, Silver decides what it means.

Now create the SDP datasets in `medallion/transformations/medallion.py`. Each function returns a DataFrame and is registered as a dataset; dependencies are expressed by reading other tables with `spark.read.table(...)`. Silver is a one-liner that calls your reusable cleaner:

```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F
from lakehouse.quality import clean_orders

spark = SparkSession.active()


@dp.materialized_view(name="silver.orders")
def silver_orders():
    return clean_orders(spark.read.table("iceberg.bronze.orders_raw"))


@dp.materialized_view(name="gold.daily_revenue")
def gold_daily_revenue():
    return (
        spark.read.table("iceberg.silver.orders")
        # select, not withColumn, when reading another dataset of this pipeline
        # (see "Run it again, and verify" below for why)
        .select(F.to_date("created_at").alias("order_date"), "status", "amount")
        .groupBy("order_date", "status")
        .agg(F.sum("amount").alias("revenue"),
             F.count("*").alias("order_count"))
    )
```

SDP loads this file directly, which is why it binds `spark = SparkSession.active()` itself: nothing injects a `spark` variable into a transformations module for you. For the file to `import lakehouse.quality`, your package must be installed in the environment that runs `spark-pipelines`, which it is: you ran `pip install -e .` in Chapter 2.

**What just happened?** Read the two functions and notice what's absent: nowhere do you say "run Silver before Gold," and you never call these functions yourself. `gold_daily_revenue` reads `iceberg.silver.orders`, and that read is the dependency. SDP scans the declarations, sees that Gold depends on Silver (which depends on Bronze), and infers the whole graph. You wrote what each dataset is, not when to build it. That's the declarative model in one screen of code.

**Step 4: run the pipeline against your Connect server.** From inside the project, SDP auto-discovers the spec. Validate first with a dry run, which resolves the graph and checks schemas without writing anything, then run it for real:

```bash
cd medallion
spark-pipelines dry-run --remote sc://localhost:15002
spark-pipelines run --remote sc://localhost:15002
```

Expected (trimmed; timestamps will differ):

```
Loading pipeline spec from .../pipelines/medallion/spark-pipeline.yml...
Creating Spark session...
Creating dataflow graph...
Registering graph elements...
Found 1 files matching glob 'transformations/**/*'
Importing .../pipelines/medallion/transformations/medallion.py...
Starting run...
Flow iceberg.silver.orders is QUEUED.
Flow iceberg.gold.daily_revenue is QUEUED.
Flow iceberg.silver.orders is PLANNING.
Flow iceberg.silver.orders is STARTING.
Flow iceberg.silver.orders is RUNNING.
Flow iceberg.silver.orders has COMPLETED.
Flow iceberg.gold.daily_revenue is PLANNING.
Flow iceberg.gold.daily_revenue is STARTING.
Flow iceberg.gold.daily_revenue is RUNNING.
Flow iceberg.gold.daily_revenue has COMPLETED.
Run is COMPLETED.
```

**What just happened?** SDP did the orchestration you'd otherwise hand-code. It resolved the graph, ran Silver first (applying the quality filters), then ran Gold against the freshly-materialized Silver. Read the log as a graph walk: both flows are queued at once, but Gold doesn't start planning until Silver has completed. If you later add a third dataset that reads Gold, SDP slots it into the graph automatically.

**Step 5: query the Gold table over a thin client.**

```python
from lakehouse.session import get_spark
spark = get_spark()
spark.table("iceberg.gold.daily_revenue").orderBy("order_date", "status").show()
```

Expected output. Because Chapter 6's generator is seeded by the date, these are exactly the numbers you should see if you've ingested only `2026-01-05` (if you also did Chapter 6's "add a second day" exercise, you'll have a second date's rows too):

```
+----------+---------+---------+-----------+
|order_date|   status|  revenue|order_count|
+----------+---------+---------+-----------+
|2026-01-05|cancelled| 48309.50|        200|
|2026-01-05|   placed|134321.32|        552|
|2026-01-05|  shipped| 49997.14|        193|
+----------+---------+---------+-----------+
```

Check the type too: `spark.table("iceberg.gold.daily_revenue").dtypes` shows `revenue` as `decimal(20,2)`. Summing a `DECIMAL(10,2)` widens the precision so the total can't overflow, and keeps the two exact decimal places.

**Step 6: see the cleaning actually happen.** The quality filters aren't theoretical, they removed rows. Compare the counts across the tiers:

```python
spark.table("iceberg.bronze.orders_raw").count()  # 1020: everything, warts and all
spark.table("iceberg.silver.orders").count()      # 945: nulls, bad amounts, dupes dropped
```

The gap is 75 rows, and you can account for every one of them: 18 records with no `order_id`, 37 with a broken amount, and 20 re-sent duplicates, exactly the mess Chapter 6's generator injected. Note what was *not* dropped: guest checkouts with a null `customer_id` are valid orders, and they're still in Silver. And the uppercase statuses weren't dropped either, they were fixed (conformed to lowercase), which is why Gold shows three statuses, not six. Spot-check the result directly; this should return zero rows, because Silver is clean by construction:

```python
spark.sql("SELECT * FROM iceberg.silver.orders WHERE amount <= 0 OR order_id IS NULL").show()
# empty result: no bad rows survived into Silver
```

That empty result is the point of the medallion: Gold's revenue numbers are trustworthy because the bad data was filtered out one tier below, and you can prove it with a query rather than take it on faith. As a final cross-check, Gold's order counts sum to 945 (200 + 552 + 193), Silver's row count: every clean order is counted exactly once.

**What just happened?** You're reading a business-ready table: daily revenue and order counts by status. Trace its lineage in your head. These numbers came from Silver (clean, deduplicated orders), which came from Bronze (the raw landing), which came from the generator standing in for a source system. That full path, from raw event to business metric, is the entire data engineering flow in miniature, and you built it declaratively.

### Run it again, and verify: why Gold uses `select`, not `withColumn`

Here is where a habit you've been building pays off: never assume a pipeline worked because it said `COMPLETED`. Run the exact same command a second time and re-check the two tiers:

```bash
spark-pipelines run --remote sc://localhost:15002
```

```python
spark.table("iceberg.silver.orders").count()                                     # 945
spark.sql("SELECT sum(order_count) FROM iceberg.gold.daily_revenue").first()[0]  # 945
```

Gold still accounts for every Silver row, on this run and every run after it. That only holds because of the comment in `gold_daily_revenue`. Change its `.select(...)` line to the more common-looking

```python
        .withColumn("order_date", F.to_date("created_at"))
```

and run the pipeline twice more. The first of those runs is fine. On the second, the run still reports `COMPLETED` and Silver still has 945 rows, but Gold is empty. The run log shows why: instead of Silver finishing before Gold starts, both flows are queued and start together:

```
Flow iceberg.gold.daily_revenue is QUEUED.
Flow iceberg.silver.orders is QUEUED.
Flow iceberg.silver.orders is RUNNING.
Flow iceberg.gold.daily_revenue is RUNNING.
```

SDP no longer knows that Gold depends on Silver. Here is the mechanism, because it's a genuine gap in Spark rather than anything you did wrong (it reproduces on stock Spark 4.1.0, 4.1.3 and 4.2.0, and on the main branch as of this writing). SDP learns a flow's dependencies by finding the reads of other pipeline datasets in the flow's (unresolved) query plan. Over Spark Connect, though, `withColumn` is sent to the server as its own plan node, and the Connect server tries to analyze it eagerly the moment it arrives, which means looking `iceberg.silver.orders` up in the catalog. On the very first run Silver doesn't exist yet, the eager analysis fails, the server keeps the unresolved plan, and SDP finds the dependency. From the second run on Silver does exist, the read gets resolved straight to the catalog table, and SDP sees an ordinary external table instead of its own dataset. It then refreshes Gold alongside Silver, at the moment Silver has just been truncated for its own refresh (every run truncates the materialized views it rebuilds first), so Gold aggregates an empty table.

Be careful when you borrow from Spark's own SDP programming guide: its `daily_orders_by_state` example ends in `.withColumnRenamed("count", "order_count")` on a read of another pipeline dataset, so run over Connect a second time, it hits this same gap. Rewrite that last step as a `select` with an alias.

The bug is tracked upstream as [SPARK-59663](https://issues.apache.org/jira/browse/SPARK-59663), with a fix under review in [apache/spark#59100](https://github.com/apache/spark/pull/59100). The practical rule, until a Spark release carries that fix:

- **When a flow reads another dataset of the same pipeline, shape its columns with `select` (or `selectExpr`), `filter`, `groupBy`/`agg`, or SQL, not `withColumn`, `withColumnRenamed`, or `drop`.** Those last three go through the eager-analysis path. `drop`, which has no fallback at all, fails even the first run with `TABLE_OR_VIEW_NOT_FOUND`.
- **Reads of tables outside the pipeline are unaffected.** That's why `clean_orders` can use `withColumn` freely: Silver reads Bronze, which this pipeline doesn't define, so there's no dependency to lose.
- **Verify across runs, not just once.** A single green run proves very little here. The check above (Gold's counts summing to Silver's rows) is cheap, and it's exactly the kind of assertion worth adding to `tests/` or to the end of a scheduled run.

One related property is worth knowing. A materialized-view refresh here is two commits, a `delete` (the truncate) and then an `append`, not the single atomic commit you saw for `INSERT` and `DELETE` in Chapter 5. A reader that queries Gold in the moment between them sees an empty table. List Silver's snapshots with the helper from Chapter 5 (`print_snapshots("silver", "orders")`) and you'll see the `delete`/`append` pairs, one per refresh. For a local course that's harmless; for a dashboard refreshing during business hours, it's the kind of detail you'd want to know before production.

## Data quality in OSS Spark Declarative Pipelines

> **Data quality** *(canonical term)*: checks that data meets expectations before it's trusted downstream.

Look again at the Silver definition. The two `.filter(...)` calls are the data-quality enforcement: a row with no `order_id` or a non-positive `amount` never reaches Silver. The quality logic lives right in the dataset definition, so it's visible, versioned with the code, and enforced on every run. There's no separate validation job to forget.

It's worth being honest about a distinction here, because the Databricks documentation you'll find online describes something OSS doesn't have. Databricks' managed pipelines (the product formerly called DLT) ship a declarative expectations API: decorators like `@expect_or_drop` and `@expect_or_fail` that attach named rules to a dataset and track how many rows each one dropped. That API is Databricks-only. The open-source `pyspark.pipelines` that you're running does not include it. In OSS, you express the same intent with ordinary DataFrame operations: filter to drop bad rows, and raise an error in the function to fail the run.

The design decision behind the filters is the same one you'd make either way, so think it through:

- **Drop the row** (a `.filter(...)`, as above). Good when a few malformed records shouldn't block the whole pipeline and you're fine excluding them from downstream tables. Remember the dropped rows still exist in Bronze, which is exactly why Bronze fidelity matters.
- **Fail the run** (check a condition in the function and `raise` if it's violated). Reserve this for conditions that mean something is catastrophically wrong, for example a null primary key in a table that must have one, where continuing would corrupt everything downstream.

The instinct to reach for is: fail loud on things that must never happen; drop on messiness you expect. A null `order_id` where every order must have one could justify a hard failure; a negative `amount` from a known-flaky source is a drop. Encoding these decisions right in the Silver definition means the next engineer sees your data-quality intent without hunting through a separate script.

![Data-quality filtering in Silver](../figures/ch07/fig-7.3-data-quality.svg)

**Figure 7.3**. Rows flow through the Silver definition's filters; invalid rows are filtered out so only trustworthy rows reach Silver.

### Test the cleaning in isolation

Pulling `clean_orders` out into `lakehouse/quality.py` bought you something a buried `.filter()` chain can't: you can test the data-quality logic directly, with a handful of known-bad rows, without running the whole pipeline. That is what "production-ready" looks like at the code level, the rules that protect your Gold numbers are themselves verified. Save this as `tests/test_quality.py`:

```python
# tests/test_quality.py
from lakehouse.session import get_spark
from lakehouse.quality import clean_orders

def test_clean_orders_drops_bad_and_dedupes():
    spark = get_spark("test")
    df = spark.createDataFrame(
        [("o1", "c1", 10.0, "PLACED", "2026-01-05T09:00:00"),   # valid (status uppercased)
         ("o1", "c1", 10.0, "placed", "2026-01-05T09:00:00"),   # duplicate order_id
         (None, "c2", 10.0, "placed", "2026-01-05T09:00:00"),   # null key -> dropped
         ("o3", "c3", -1.0, "placed", "2026-01-05T09:00:00")],  # bad amount -> dropped
        ["order_id", "customer_id", "amount", "status", "created_at"])

    clean = clean_orders(df).collect()

    assert len(clean) == 1                 # only o1 survives, and only once (deduped)
    assert clean[0].order_id == "o1"
    assert clean[0].status == "placed"     # casing conformed


def test_clean_orders_conforms_types():
    spark = get_spark("test")
    df = spark.createDataFrame(
        [("o1", None, 19.99, "Shipped", "2026-01-05T09:00:00")],   # guest checkout: kept
        # An all-None column gives Spark nothing to infer a type from, so spell it out.
        "order_id STRING, customer_id STRING, amount DOUBLE, status STRING, created_at STRING")

    clean = clean_orders(df)
    types = dict(clean.dtypes)

    assert types["amount"] == "decimal(10,2)"     # exact money, not a float
    assert types["created_at"] == "timestamp"     # parsed, no longer a string
    row = clean.collect()[0]
    assert row.customer_id is None                # Silver keeps guest checkouts
    assert row.status == "shipped"
```

The first test covers what gets dropped, the second covers what gets *kept and conformed*, including the guest checkout that a careless filter on `customer_id` would have thrown away. Note the second test spells out its schema as a DDL string: its only `customer_id` is `None`, and Spark can't infer a column's type from nothing but nulls.

Run them:

```bash
pip install pytest        # a dev-only dependency
pytest tests/ -q
# 2 passed
```

The tests need the Connect server up (they use `get_spark()`), so they're really integration tests of your cleaning rules against a real engine. That's a deliberate trade for a lakehouse course: the rules are tested the way they actually run. Each new quality rule you add to `clean_orders` gets a case here, so a future edit that accidentally lets bad rows through fails the test instead of silently corrupting Gold.

## Troubleshooting

- **SDP reports a cycle or can't resolve the graph.** Two datasets read each other, or a function reads a table that isn't declared and doesn't exist. Check that every table you read either exists (like Bronze) or is declared in the pipeline.
- **A dataset materializes empty.** Often an over-strict filter removed every row, or the upstream table was empty. Check your filter conditions and confirm Bronze actually has data.
- **`ModuleNotFoundError: pyspark.pipelines` or `spark-pipelines: command not found`.** The pipelines extra isn't installed in the environment you're running from (activate your project's venv), or you're on a Spark older than 4.1. Run `pip install 'pyspark[pipelines]==4.1.0'`.
- **`spark-pipelines` fails to launch with a Java error.** The CLI starts a JVM under the hood (via `spark-class`), so it needs a JDK (Java 17+) on your host. See Step 1.
- **`SCHEMA_NOT_FOUND` for `iceberg`.`default` or `iceberg`.`silver`.** The spec is missing `database: silver`, or the `silver`/`gold` namespaces don't exist yet. SDP won't create schemas; create them with `CREATE NAMESPACE IF NOT EXISTS`.
- **Gold is empty after a run that said `COMPLETED`.** Gold uses `withColumn` (or `withColumnRenamed`) on its read of Silver, so from the second run on SDP no longer knows Gold depends on Silver and builds Gold from a just-truncated table. Shape the columns with `select` instead (see "Run it again, and verify"), then re-run and check that Gold's `order_count` sums to Silver's row count.
- **`Cannot change column type: amount: double -> decimal(10, 2)`.** You changed a column's type in a dataset that already exists. A refresh, even `--full-refresh-all`, keeps the table and only resets its data, and Iceberg can't change a double into a decimal in place. Drop the table (`DROP TABLE iceberg.silver.orders PURGE`, and Gold too) and run again. Iceberg can widen some types in place (`int` to `bigint`, `float` to `double`), but not this one.
- **Connect-style error during run.** SDP runs on Spark Connect, so failures surface as Connect errors; read the last line, as in Chapter 4. Confirm `--remote sc://localhost:15002` points at your running Connect server.
- **Tables land in the wrong catalog or schema.** Check `catalog:` in `spark-pipeline.yml` and the schema-qualified `name=` on each dataset (`silver.orders`, `gold.daily_revenue`).

## Checkpoint

- `spark-pipelines init` scaffolded `pipelines/medallion/`, and your spec sets `catalog: iceberg` and `database: silver`.
- `medallion.py` declares `silver.orders` and `gold.daily_revenue`, with dependencies expressed by `spark.read.table(...)`.
- `spark-pipelines run --remote sc://localhost:15002` materialized both datasets in the correct order without you sequencing them.
- You re-ran it and confirmed Gold still sums to Silver's row count, and you can explain why Gold uses `select` rather than `withColumn` on its read of Silver.
- Silver's `.filter(...)` calls dropped invalid rows.
- Querying `iceberg.gold.daily_revenue` returns aggregated, business-ready results with an exact `decimal` revenue, and `pytest tests/ -q` reports `2 passed`.
- You can explain the difference between imperative and declarative pipelines.

## Try it yourself

1. **Add a Gold table.** Declare a second Gold materialized view, say revenue by `customer_id`, that also reads Silver. Re-run the pipeline with the same command and confirm SDP builds it from your declaration alone, after Silver, without you touching the run command. Use `select`/`agg` on the Silver read, per the rule above. That's the "clean once, aggregate many ways" payoff. What should a guest checkout (null `customer_id`) count as here?
2. **Tighten a filter.** Add a quality filter to Silver (for example, `status IN ('placed', 'shipped', 'cancelled')`) and see how many rows it drops. Then relax it and compare.
3. **Make it fail loud.** In the Silver function, after the filters, check a must-hold condition (for example, that the row count is non-zero) and `raise` if it's violated. Introduce a scenario that trips it and watch the run stop. Reflect on when that behavior is what you want.
4. **Trace the lineage.** Pick one number in the Gold output and, in words, trace it back through Silver to Bronze to the generator. Being able to explain "where did this number come from?" is a core data-engineering skill.

## Check your understanding

- What is each medallion tier for, and why keep them separate instead of one big cleanup step?
- Explain "clean once in Silver, aggregate many ways in Gold."
- What's the difference between an imperative and a declarative pipeline? Give the GPS analogy.
- How do you enforce data quality in OSS Spark Declarative Pipelines, and what does OSS not give you that Databricks DLT does?

## Recap and what's next

- The medallion refines data forward: Bronze (raw) to Silver (clean) to Gold (business-ready). Clean once, aggregate many ways.
- Spark Declarative Pipelines let you declare datasets and dependencies; the engine derives the graph and refresh, built on Spark Connect.
- Data quality lives in the dataset definition as ordinary filters (drop) or raised errors (fail). The named-expectations API is a Databricks feature, not part of OSS.
- Cleaning logic lives in a reusable `lakehouse/quality.py` function that Silver calls and `tests/` verifies, so the rules protecting Gold are themselves tested. Money is conformed to an exact `DECIMAL`.
- "It said COMPLETED" is not verification: re-run and check the counts. In Spark 4.1 and 4.2, a flow that reads another pipeline dataset should shape columns with `select`, not `withColumn`, or SDP can lose the dependency.
- **Next, Chapter 8, Streaming:** feed the same Silver continuously from Kafka with a streaming table, and contrast streaming with the batch pipeline you just built.

![Progress: Transformation complete, Streaming next](../figures/ch07/fig-7.4-progress-transform-done.svg)

**Figure 7.4**. Progress map with **Transformation done** and **Streaming** next.

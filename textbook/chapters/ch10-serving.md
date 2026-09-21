# Chapter 10 — Serving: one open catalog, many engines

## 10.0 What you'll build

The payoff of "open." Your Gold tables are built, refreshed, and scheduled. Now you
**serve** them — make them available to whatever wants to read: a second query
engine, a BI tool, a notebook, a model. And here the open lakehouse earns its name:
because every table lives in **Unity Catalog OSS** as an open Iceberg table, *any*
Iceberg-aware engine reads the **same** data through **one** catalog endpoint — no
copies, no exports, no per-tool metastore. You'll prove it by querying your Gold
table from a *different* engine than the one that wrote it.

This is the chapter that justifies the word "open" in "open lakehouse," and it's
genuinely the thing a closed warehouse *cannot* do. Everything up to now you could,
in principle, have built inside a proprietary system. But the moment you want a
different tool to read your data without copying it out first, the closed systems
fall down and the open lakehouse shines. When you see DuckDB return the exact same
rows Spark wrote — with zero export step — the whole architecture's payoff clicks
into place.

![Progress: Serving](../figures/ch10/fig-10.3-progress-serving.svg)

**Figure 10.3** — Progress map with **Serving** highlighted.

## 10.1 Learning objectives

By the end of this chapter you can:

- Define the **serving** layer and who consumes it.
- Explain how one **Iceberg REST catalog** enables **multi-engine** access.
- Read a Gold table from a second engine and get identical results.
- Describe how serving differs from a closed warehouse's export-and-copy model.

## 10.2 What serving is

> **Serving** *(canonical term)*: making finished data available to consumers — BI
> tools, other query engines, apps, and models — ideally engine-neutrally.

Serving is a **read/consumption** concern, defined by the *consumer*, not the
producer. The test of a good serving layer: can something *other than* the tool that
produced the data get at it, easily and without a copy? In a closed warehouse the
answer is usually "export it first." In the open lakehouse the answer is "point your
engine at the catalog" — because the data was never locked in a proprietary format
to begin with.

It helps to see where serving sits in the story you've built. Ingestion brought data
*in*. Transformation refined it *up* through the medallion. Serving sends it *out* to
whoever needs it. It's the last mile — the point where all the pipeline work becomes
useful to an actual human looking at a dashboard, an analyst running a query, or a
model reading training data. A pipeline that never gets served is a tree falling in
an empty forest; serving is what makes the work *count*.

## 10.3 One catalog, many engines

The mechanism is the one you stood up in Chapter 5: Unity Catalog OSS's **Iceberg
REST catalog** at `http://localhost:8081/api/2.1/unity-catalog/iceberg`. It's a
*standard* protocol, so many engines speak it. Spark wrote the tables; DuckDB,
DataFusion, Trino, and others can read them — all pointing at that single URL,
all seeing the same current snapshot.

> **Multi-engine / interoperability** *(canonical term)*: the same open tables read
> by different engines, because they share one catalog and one open format.

Why is this such a big deal? Because different engines are good at different things,
and being able to use the right tool without copying data is transformative. Spark
is great for large distributed transformations. DuckDB is fantastic for fast local
analytical queries on your laptop. Trino excels at federated interactive SQL across
huge datasets. A BI tool renders dashboards. In a closed world you'd need the data
copied into each tool's preferred home. In the open lakehouse, they all read the
*same* Iceberg tables through the *same* catalog — you pick the best engine for each
job and none of them owns the data. The data sits still; the engines come to it.

![One catalog, many engines](../figures/ch10/fig-10.1-multi-engine.svg)

**Figure 10.1** — Spark, DuckDB, DataFusion, and BI tools all read the same
Iceberg Gold tables through the one Unity Catalog OSS REST endpoint — no data
copied.

### Under the hood — serving vs. a closed warehouse

In a closed warehouse, "giving another team access" often means an ETL job that
*copies* data out to wherever they can read it — and now you have two copies that
drift. The open lakehouse replaces the copy with a *pointer*: the catalog. Every
consumer reads the one authoritative table. Fewer copies, no drift, and governance
stays in one place (Chapter 12's agents and any BI tool hit the same governed
tables).

The "drift" problem is worth making vivid, because it's the quiet killer of data
trust in real organizations. Team A builds the revenue table. Team B needs it, so a
job copies it into Team B's system. Now there are two revenue tables. Team A fixes a
bug in theirs; Team B's copy still has the bug. Someone in a meeting says "revenue
was $2M" and someone else says "no, $1.8M" — and *both are reading a real table*,
just different copies that drifted apart. Multiply that across dozens of teams and
tables and you get the "which number is right?" chaos that plagues data teams. One
catalog, one authoritative table, many readers — that's the cure, and it's only
possible because the format is open and shareable.

## 10.4 Build — serve Gold to a second engine

The primary, always-available path is Spark itself reading through the catalog — but
the *point* of this chapter is a **different** engine, so we also read the same table
from DuckDB.

Step 1 — confirm Gold is present via the catalog (Spark Connect client):

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.table("iceberg.gold.daily_revenue").orderBy("order_date").show()
```

**What just happened?** Nothing new yet — this is Spark reading its own table
through the catalog, establishing the "before" baseline. Note the exact rows it
returns; you're about to get the *identical* rows from a completely different engine.

Step 2 — read the *same* table from **DuckDB**, pointing at the one Iceberg
REST catalog. DuckDB is not required infrastructure — it's a lightweight second
engine that proves interoperability:

```sql
-- In the DuckDB CLI / Python:
INSTALL iceberg; LOAD iceberg;

-- Attach the SAME Unity Catalog OSS Iceberg REST endpoint Spark uses.
ATTACH 'iceberg.gold' AS gold (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8081/api/2.1/unity-catalog/iceberg'
);

SELECT order_date, status, revenue, order_count
FROM gold.daily_revenue
ORDER BY order_date, status;
```

Expected: DuckDB returns **the same rows** Spark returned in Step 1 — read directly
from the Iceberg files, through the shared catalog, with no export step. That
identical result across two independent engines is the whole demonstration.

**What just happened?** Pause on this, because it's the intellectual climax of the
course. DuckDB — a completely separate engine that knows nothing about Spark — just
read the tables Spark wrote, and got byte-identical results. There was no export, no
copy, no "DuckDB format" conversion. DuckDB simply pointed at the same catalog URL,
found the same Iceberg tables, and read the same underlying Parquet files. *This* is
what "open" means in practice: your data isn't trapped in one engine's world. Every
choice earlier in the course — Iceberg over a proprietary format, Unity Catalog OSS
as a standard REST catalog — was made so this moment would work.

> Note: DuckDB / DataFusion are *optional garnish* — nice for proving the point and
> for lightweight local analytics. Unity Catalog OSS is the one catalog everything
> goes through; the second engine is interchangeable.

Step 3 — (optional) point a BI tool or notebook at the same catalog. Any
Iceberg-REST-aware client uses the identical endpoint and sees the identical tables.

## 10.5 Troubleshooting

- **DuckDB can't attach / "unknown catalog type iceberg."** Your DuckDB is too old or
  the iceberg extension didn't load. Run `INSTALL iceberg; LOAD iceberg;` and use a
  recent DuckDB.
- **DuckDB attaches but finds no tables.** The endpoint is wrong or the catalog is
  down. Confirm Unity Catalog OSS is healthy and the URL exactly matches what Spark
  uses (`.../api/2.1/unity-catalog/iceberg`).
- **Results differ between engines.** Almost always a stale snapshot or a caching
  issue — re-run the read. Because both engines read the same files through the same
  catalog, genuinely different results should be impossible; a mismatch means one
  engine is looking at old state.
- **"Connection refused" to :8081.** The catalog service isn't running. Start it and
  re-check `./lakehouse status`.

## 10.6 Checkpoint

- Your Gold table is visible through the Unity Catalog OSS Iceberg REST catalog.
- You queried `gold.daily_revenue` from a **second engine** (DuckDB) using the same
  endpoint Spark uses.
- The second engine returned **identical results** with no data copied or exported.
- You can explain why this beats a warehouse's export-and-copy model.
- You can describe the "drift" problem and how one catalog cures it.

## 10.7 Try it yourself

1. **Query differently in each engine.** Run a slightly different aggregation over
   `daily_revenue` in DuckDB than you did in Spark. Confirm both work against the same
   underlying data — proof that each engine brings its own SQL but shares the tables.
2. **Add a row in Spark, read it in DuckDB.** Write a new row from Spark, then re-run
   the DuckDB query. Watch the new row appear without any export — the pointer, not a
   copy.
3. **Explain the drift cure.** In your own words, write the two-sentence version of
   why "one catalog, many engines" prevents the "which revenue number is right?"
   problem.
4. **Map the consumers.** List every kind of consumer that might read your Gold
   tables (dashboard, notebook, model, another team's pipeline). Note that *all* of
   them use the same catalog endpoint.

## 10.8 Check your understanding

- What is the "test" of a good serving layer, and how does the open lakehouse pass it
  where a closed warehouse fails?
- Why is multi-engine access valuable — give two engines and what each is best at.
- Explain the data-drift problem and how a single catalog solves it.
- When DuckDB reads a table Spark wrote, what is it actually pointing at, and what
  did *not* happen (that a closed warehouse would require)?

## 10.9 Recap & what's next

- **Serving** exposes finished data to consumers, engine-neutrally — the last mile
  that makes the pipeline count.
- One **Iceberg REST catalog** (Unity Catalog OSS) lets **many engines** read the
  **same** tables — no copies, no drift, one authoritative version.
- DuckDB/DataFusion are optional second engines; the catalog is the constant.
- **Next — Chapter 11, AI:** train a model on the Gold table and track it with
  MLflow — the data feeding intelligence.

![Progress: Serving complete, AI next](../figures/ch10/fig-10.4-progress-serving-done.svg)

**Figure 10.4** — Progress map with **Serving ✓** and **AI** next.

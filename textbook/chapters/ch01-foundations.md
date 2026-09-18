# Chapter 1 — Foundations: What an Open Lakehouse Is and Why You'd Build One

## 1.0 What you'll build in this chapter

Nothing yet — and that's deliberate. Before you install a single tool, this
chapter gives you the **map**: what an open lakehouse actually is, why the
open-source approach is worth your time, and a labeled tour of the exact stack
you'll assemble over the next twelve chapters. Everything after this is
hands-on; this chapter is the mental model that makes the rest click.

![Progress map: the 10 layers you'll build](../figures/ch01/fig-1.1-progress-map.svg)

**Figure 1.1** — The recurring progress map, which we return to at the start of
every chapter with the current layer highlighted.

## 1.1 Learning objectives

By the end of this chapter you can:

- Explain the difference between a data warehouse, a data lake, and a **lakehouse**
  in one sentence each.
- Define **open table format** and why it's the piece that makes a lakehouse possible.
- Give three practitioner reasons to build on an open-source stack.
- Name every component in the stack and the one job each does.
- Describe the bottom-up build order and why each layer depends on the one below it.

## 1.2 The problem: two bad options

For most of data's history, you chose between two architectures, each with a
fatal trade-off.

**The data warehouse.** A closed, transactional system (think Teradata, or
classic Snowflake/BigQuery usage) where storage and compute are tightly coupled.
It's reliable — ACID transactions, enforced schemas, fast SQL — but it's
proprietary, and because storage and compute scale together, it gets expensive
quickly. Your data lives in *their* format; getting it out or querying it with
another tool is painful.

**The data lake.** The reaction to that: just dump files (CSV, JSON, Parquet)
into cheap **object storage** and query them with whatever engine you like. Cheap
and open — but you lose everything the warehouse gave you. No transactions, so
two jobs writing at once corrupt each other. No reliable schema. No easy way to
update or delete a row. Lakes routinely degraded into "data swamps."

`[TABLE 1.1]` — Warehouse vs. Lake vs. Lakehouse:

| Property | Data Warehouse | Data Lake | **Open Lakehouse** |
|---|---|---|---|
| Storage cost | High | Low | **Low** |
| Open format | No | Yes | **Yes** |
| ACID transactions | Yes | No | **Yes** |
| Schema enforcement/evolution | Yes | No | **Yes** |
| Time travel / rollback | Limited | No | **Yes** |
| Multi-engine access | No | Yes | **Yes** |
| Storage/compute decoupled | No | Yes | **Yes** |

## 1.3 The resolution: the open lakehouse

> **Open lakehouse** *(canonical term)*: warehouse-grade tables — ACID
> transactions, schema evolution, time travel — provided directly on cheap object
> storage, using open file and table formats.

The trick is a new layer. You still keep your data as cheap files in object
storage. But you place an **open table format** on top of those files.

> **Open table format** *(canonical term)*: a specification — we'll use **Apache
> Iceberg** — that adds a metadata layer over your data files so a plain pile of
> files behaves like a transactional table.

That metadata layer is what turns files into a real table: it tracks which files
belong to the table right now, records every write as a versioned **snapshot**
(enabling time travel and rollback), enforces and evolves schema, and coordinates
concurrent writers so they don't corrupt each other. Open format on the bottom,
warehouse guarantees on top, cheap storage throughout.

![Warehouse vs. Lake vs. Lakehouse](../figures/ch01/fig-1.2-warehouse-lake-lakehouse.svg)

**Figure 1.2** — The three-panel contrast: locked warehouse box → loose pile of
lake files → lakehouse (files + an "open table format: ACID · time travel ·
schema" layer drawn on top).

### Under the hood — why "table format," not "file format"

Parquet is a *file* format — it describes how one file's columns are stored. A
*table* format sits a level up: it's the metadata that says "these 240 Parquet
files, at these paths, as of this snapshot, are the table `orders`." Iceberg,
Delta Lake, and Apache Hudi are the three main open table formats. This course
uses Iceberg, but the concepts transfer.

## 1.4 Why open source for this build

Three practitioner reasons — not ideology, just pragmatics:

1. **No lock-in.** Every component is an open standard you can run on your laptop,
   on-prem, or on any cloud. Your data stays in open formats you own.
2. **No cloud bill to learn.** The whole system runs locally, so you can
   experiment freely, break things, and tear it all down — without a credit card
   or a running meter.
3. **The same stack underlies the paid platforms.** Databricks is built on Spark
   and Delta/Iceberg; managed services wrap Kafka and Airflow. Learn the open
   tools directly and moving to a managed service later is a small step — you
   already understand what's under the hood.

![The open-source stack and its three benefits](../figures/ch01/fig-1.3-stack-benefits.svg)

**Figure 1.3** — Stack component row (SeaweedFS, Spark, Iceberg, Kafka, Airflow,
DuckDB, MLflow) captioned "no lock-in · no cloud bill · portable skills."

## 1.5 The stack, layer by layer

Here's every component and the single job it does. We build **bottom-up**,
because each layer consumes the one beneath it — you can't transform data before
you can store and compute it.

`[TABLE 1.2]` — The stack and build order:

| Build order | Layer | Component | Its one job |
|---|---|---|---|
| 1 | **Storage** | SeaweedFS (S3 API) + Iceberg format | Hold data cheaply **and** decide how files become tables |
| 2 | **Compute** | Apache Spark 4.x via **Spark Connect** (`sc://`) | Read, write, and transform data from a thin remote client |
| 3 | **Tables & Catalog** | Apache Iceberg + **Unity Catalog OSS** | Create real Iceberg tables a catalog can track and any engine can find |
| 4 | **Ingestion** | Spark Connect → Iceberg | Land raw source data (Bronze) |
| 5 | **Transformation** | Spark Declarative Pipelines (SDP) | Build Bronze → Silver → Gold declaratively |
| 6 | **Streaming** | Kafka + Spark Structured Streaming | Handle real-time events |
| 7 | **Orchestration** | Apache Airflow | Schedule and wire pipelines together |
| 8 | **Serving** | Unity Catalog OSS (REST catalog) | Expose the same tables to any engine |
| 9 | **AI** | MLflow | Train, track, and register a model on the data |
| 10 | **Agents** | `./lakehouse` CLI + skills | Let an LLM operate the lakehouse |

A note on the two halves of storage. "Storage" in a lakehouse is really *two*
decisions stacked on top of each other, and Chapter 3 covers both: first, **where
the bytes live** — an **object store** like SeaweedFS, MinIO, or Amazon S3, all
speaking the same S3 API — and second, **how those files become a table** — an
**open table format** like Iceberg, Delta, or Hudi layered on top of the files. We
choose **SeaweedFS** for the object store (lightweight, laptop-friendly) and
**Iceberg** for the table format (engine-neutral — the heart of the "open"
promise). Chapter 5 then does the hands-on Iceberg build now that the *choice* is
made: creating tables through **Unity Catalog OSS**, schema evolution, and time
travel.

A note on compute. We drive Spark with **Spark Connect** — a thin client that
talks to a remote Spark cluster over `sc://` — rather than the classic model where
your program bundles the whole Spark engine. Chapter 4 explains why this is the
modern default (connect from anywhere, thin dependencies, client/server
isolation), and Spark Declarative Pipelines in Chapter 7 build on it.

![The open lakehouse architecture map](../figures/ch01/fig-1.4-architecture-map.svg)

**Figure 1.4** — The master architecture diagram: object storage at the base,
compute and table/catalog above it, the data-flow layers (ingestion,
transformation, streaming) in the middle, orchestration spanning them, and
serving/AI/agents at the top. **This is the single most important visual in the
course** — the progress map (Figure 1.1) is its condensed, recurring form.

## 1.6 The through-line: one dataset, built once

To keep the course coherent, we use one story throughout: a stream of realistic
**e-commerce order events**. You'll land those orders raw, clean and conform them,
aggregate them into business tables, schedule the whole pipeline, serve it to
multiple engines, train a model on it, and finally let an agent operate it. Every
chapter adds exactly one layer to a system you keep running — so by the end, the
finished lakehouse in the cold-open demo is entirely yours.

![One order event's journey through the stack](../figures/ch01/fig-1.5-order-event-journey.svg)

**Figure 1.5** — The order-event's journey across the layers (a single horizontal
"data flows through the stack" ribbon).

## 1.7 Checkpoint

No build this chapter. You're ready to proceed when you can, without looking:

- Draw the three-panel warehouse/lake/lakehouse contrast.
- Say what an open table format adds and why it matters.
- Name the ten layers in build order.

## 1.8 Recap & what's next

- A **warehouse** is reliable but closed and coupled; a **lake** is open and
  cheap but offers no guarantees; an **open lakehouse** puts warehouse guarantees
  on cheap open storage via an **open table format**.
- We build on open source for **no lock-in, no cloud bill, and transferable skills**.
- The stack has **ten layers**, built **bottom-up**, unified by one order-event
  dataset.
- **Next — Chapter 2, Setup:** install prerequisites, clone the project, and bring
  the environment up with a single command.

![Progress: Foundations complete, Setup next](../figures/ch01/fig-1.6-progress-foundations-done.svg)

**Figure 1.6** — Progress map with **Foundations ✓** checked and **Setup**
highlighted as next.

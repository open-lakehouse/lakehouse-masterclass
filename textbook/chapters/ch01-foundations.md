# Chapter 1: The Decisions You Make Before You Write Any Code

You've been handed a mandate: stand up a lakehouse for your organization, and you have weeks, not quarters. Before you install a single tool, you owe your future self a handful of decisions. Some are cheap to change later. A few are not. Get those wrong and you'll be migrating data formats or re-platforming a catalog six months in, on a Friday, while people wait on you.

This chapter is the decision record. It's the doc a senior engineer would hand you on day one: here are the calls you have to make, here's the call I'd make and why, here's when I'd choose differently, and here's how you prove each decision is production-ready before you build on top of it. The rest of the book is execution. This chapter is the architecture.

![The decisions map onto the layers you'll build](../figures/ch01/fig-1.1-progress-map.svg)

**Figure 1.1**. The layers you'll stand up, bottom-up. Every decision below maps to one of these. We return to this map at the start of every chapter.

## 1.1 What you'll decide in this chapter

By the end you'll have made, at least provisionally, every architecture decision that shapes the build:

- **Do you even need a lakehouse?** The gut-check that saves some readers months.
- **Table format:** Iceberg, Delta, or Hudi. The one hard-to-reverse choice.
- **Catalog:** where table metadata lives, and why a REST catalog now.
- **Object store:** where the bytes physically sit.
- **Compute and how you talk to it:** Spark, driven by Spark Connect.
- **Batch, streaming, or both on day one?**
- **Self-hosted vs. managed:** who runs it, and how that changes your build.
- **The week-one sequence:** what order to actually do this in.

## 1.2 Gut-check: do you actually need a lakehouse?

Before anything, be honest about whether this is the right tool, because "we should build a lakehouse" is sometimes cargo-culting. You need one when at least two of these are true:

- Your data is too big or too varied for a single Postgres or MySQL to serve analytics comfortably. You're into hundreds of GBs or TBs, or many source systems.
- You need more than one engine to touch the same data. A BI tool and Spark and maybe a notebook or a model, without maintaining copies.
- You need cheap retention of large raw history (compliance, replay, reprocessing) that a warehouse would make expensive.
- You want to avoid vendor lock-in on either storage or compute.

If you're a single team with a few dozen GB and one BI tool, a managed warehouse (BigQuery, Snowflake, even a big Postgres) may be the right answer and far less to operate. There's no prize for building infrastructure you didn't need. The lakehouse earns its complexity when you have multiple consumers, big cheap history, and a desire not to be trapped.

> **Practitioner note:** the honest version of "do we need this" is often "we will within a year, and migrating later is worse than building right now." That's a legitimate reason. Just make it a decision, not a default.

## 1.3 The shape: an open lakehouse in one diagram

Assuming you're building one, here's the reference architecture you're standing up. It's layered bottom-up because each layer consumes the one beneath it. You can't transform data you can't store and compute on.

![The open lakehouse architecture](../figures/ch01/fig-1.4-architecture-map.svg)

**Figure 1.4**. Object storage at the base; compute and the table/catalog layer above it; ingestion, transformation, and streaming in the middle; orchestration across them; serving, AI, and agents on top. This is the system you'll have running by the last chapter.

Here's the one-line version of what a lakehouse is, so we share vocabulary: warehouse-grade tables (ACID transactions, schema evolution, time travel) served directly on cheap object storage, using open formats. You get the reliability of a warehouse and the cost and openness of a lake, because a metadata layer (the table format) turns a pile of files into a real transactional table. That's the whole trick. The decisions below are about how you implement it without painting yourself into a corner.

![Warehouse vs. lake vs. lakehouse](../figures/ch01/fig-1.2-warehouse-lake-lakehouse.svg)

**Figure 1.2**. Why the lakehouse exists: a warehouse is reliable but closed and coupled; a lake is open and cheap but offers no guarantees; a lakehouse puts warehouse guarantees on cheap open storage via an open table format. If you already know this, skip ahead to the decisions. That's what you came for.

## 1.4 What each layer is for, and the simpler thing it replaces

Every layer in that diagram exists because a simpler approach breaks at a specific point. Knowing where each one breaks is what lets you defer a layer honestly when your scale hasn't hit that point yet, and defend it when someone asks why it's in the design. So for each layer: the simpler thing you'd reach for first, and the concrete failure that makes you reach for the layer instead.

**Object store, instead of files on a disk or a warehouse's internal storage.** The naive option is to keep data on a big disk, or inside a database that owns its own storage. Both weld your data to one machine or one engine. A disk runs out and doesn't scale horizontally; a warehouse only lets the warehouse read the data, so a second engine means a second copy. Object storage is cheap, effectively unbounded, and readable by anything that speaks the S3 API. Decoupling storage from compute is the property the entire lakehouse is built on, which is why this layer is the floor and not optional.

**Open table format, instead of reading Parquet straight from a bucket.** This is the question every engineer asks first: why not just point Spark at a folder of Parquet files in S3 and call it a table? It's worth answering properly, because Parquet is a genuinely good file format (columnar, compressed, carries per-file column stats) and a genuinely bad table. The gap is everything that makes a directory of files behave like a table:

- Nothing records which files are the table. "The table" is whatever is under the prefix when you list it, so every read starts with a `LIST`, which is slow across millions of objects and historically wasn't consistent on S3 either.
- Writes aren't atomic. If a job writes 200 files and dies after 120, a reader sees a half-written table as if it were real, because the filesystem contents are the only source of truth.
- Concurrent writers clobber each other. Two jobs writing the same prefix have no commit protocol to coordinate, so you silently lose or duplicate data with no error raised.
- Nothing enforces schema. Each Parquet file carries its own schema and nothing checks that Tuesday's files agree with Wednesday's. A column whose type drifts across files gives you a merge error at best and silent coercion at worst, and there is no safe way to rename or add a column across the set.
- There are no row-level updates or deletes. Parquet files are immutable, so removing one customer's rows for a compliance request means finding, rewriting, and swapping every file that contains them, by hand, while hoping nothing reads mid-swap.
- There is no history. Overwrite the files and the previous state is gone: no querying the table as of last Tuesday, no rolling back a bad load.
- Query planning is expensive. To find the rows you want, the engine has to list the prefix and open file footers, because nothing above the files knows their value ranges or row counts.

An open table format is exactly the metadata layer that closes that gap. Iceberg keeps a manifest of which files currently make up the table, with per-file statistics, and every write produces a new immutable snapshot and then atomically swaps a single pointer to it. That one indirection is what buys ACID commits, safe concurrent writers, schema evolution, row-level deletes, time travel, and file pruning without a directory listing. You are still storing Parquet underneath. The table format is the difference between "some files in a bucket" and "a table," and it's the layer you don't skip once correctness matters.

**Distributed compute, instead of pandas on one machine.** The simple option is to pull data into pandas or DuckDB on a single box, and for a few dozen GB that's the right call, not a compromise. It breaks when a dataset outgrows one machine's memory, or when a transformation is heavy enough that one core-count isn't enough. Spark is the default here because it scales across machines and does both batch and streaming with one engine. If your data comfortably fits one box, use the lighter tool; adopt Spark when you actually hit its wall.

**Ingestion as land-raw-first, instead of cleaning on the way in.** The tempting shortcut is to transform and filter during load, keeping only the rows you think you need. The failure is that you've thrown away information you can't recreate: the day you discover a bug in that cleaning logic, or a new question needs a field you dropped, the raw source may be gone. Landing raw first costs almost nothing and makes every future reprocessing possible. This layer is a discipline more than a tool, and it's never optional.

**Transformation as shared medallion tables, instead of every consumer cleaning for itself.** Without a transformation layer, each dashboard, model, and query re-derives its own cleaning and business logic, and those copies drift until two dashboards disagree on revenue and nobody can say which is right. Building Bronze to Silver to Gold once, in one place, is what makes a number trustworthy. Skippable only if your raw data is already clean and modeled, which it never is.

**Streaming, instead of running batch more often.** The simpler answer to "we need it fresher" is to run the batch job every 15 minutes. That covers a surprising amount of "real-time" requests. Streaming earns its extra operational cost (unbounded state, late and out-of-order events, checkpoints, exactly-once handling) only when freshness genuinely matters in seconds: fraud, live ops, alerting. This is the most deferrable layer. Add it for the specific pipelines that need it, not by default.

**Orchestration, instead of cron.** You can start a pipeline with cron and a shell script. It breaks the first time step three depends on step two finishing, or a task fails at 3am and everything downstream runs on stale data with no alert. An orchestrator runs tasks in dependency order with retries, backfills, and visibility into what failed and why. Optional while you're building by hand; not optional once the pipeline has to run reliably without you watching it.

**Serving through the catalog, instead of exporting copies.** The naive way to get data to a BI tool or another engine is to export it, which spawns duplicate tables that immediately start drifting from the source. In an open lakehouse this layer is nearly free: it's the same catalog you already stood up, now pointed at by more engines reading the same tables in place. Not optional the moment anyone besides your own pipeline needs the data.

**AI on the governed tables, instead of a separate ML data pile.** Optional entirely, and only relevant if you have a model to train. The point when you do have one is that the same clean, versioned Gold tables that serve BI also serve training, from a single source, with reproducibility for free, instead of maintaining a parallel dataset that no longer matches production.

**Agents, instead of a human at the CLI.** The newest and most optional layer: an LLM driving the platform through its command-line surface. The lasting lesson isn't the agent itself, it's that the properties that make a system agent-operable (one clean control surface, machine-readable status, documented procedures) are the same ones that make it pleasant for a human on call. Skip it and you lose nothing structural.

The pattern worth internalizing: the lower layers are mandatory and the upper layers are increasingly optional. Storage, table format, and ingestion are non-negotiable. Compute and transformation are near-mandatory. Streaming, orchestration, serving, AI, and agents are things you add when a real need crosses the breaking point described above. Build the floor first, and add each upper floor when someone is actually going to live on it.

## 1.5 Decision 1. Table format: Iceberg, Delta, or Hudi?

**The call: Apache Iceberg, unless your org is all-in on Databricks, in which case Delta.**

This is the decision that's genuinely hard to reverse, because it dictates how every byte you write is physically laid out and what can read it. Make this one deliberately.

| | **Iceberg** | Delta | Hudi |
|---|---|---|---|
| Engine neutrality | **Best**: Spark, Trino, Flink, DuckDB, Snowflake, BigQuery | Good; historically Spark-first | Good; Spark-centric |
| Best at | Broad interop, large-scale analytics | Databricks-native workloads | High-frequency upserts / CDC |
| Catalog | REST catalog standard (what we use) | Unity Catalog / Hive | Hive-based |
| Reversibility if you're wrong | **Low risk**: everyone reads it | Medium | Medium |

**Why Iceberg for most orgs:** it's the format least likely to trap you. If your future includes any engine besides Spark (a BI tool, DuckDB on an analyst's laptop, Snowflake, Trino) Iceberg reads everywhere with no migration. On a deadline, you are optimizing for "I will not have to redo this," and Iceberg is the safest bet on that axis. Its governance is a genuinely open, multi-vendor standard, not one company's roadmap.

**Pick Delta instead when:** your org already runs Databricks and will for the foreseeable future. Delta is the path of least resistance there and Databricks makes it excellent. Don't fight your platform to be a purist.

**Pick Hudi when:** your primary pattern is high-frequency upserts or change-data-capture, meaning constant streaming mutations to the same keys. It's built for that. If that's not your dominant workload, skip the added complexity.

> **Production gate, do this in week one and not month three:** write a 10-row table in your chosen format and read it from every consumer you'll have (Spark, your BI tool, DuckDB, whatever). If any consumer can't read it today, you've found a problem while it's cheap to fix. This course uses Iceberg.

## 1.6 Decision 2. Catalog: where does table metadata live?

**The call: a REST catalog, Unity Catalog OSS, from day one. Don't start on the Hive metastore.**

The catalog is the service that answers "what tables exist, and where are their files?" Every engine asks the catalog before it reads. This decision determines who can find your tables and how painful multi-engine access will be.

- **REST catalog (Iceberg REST / Unity Catalog OSS):** a modern, standard HTTP protocol many engines speak natively. One endpoint, many engines. This is where the ecosystem is going, and it's what makes "serve the same tables to Spark and DuckDB" a config line instead of a project.
- **Hive metastore:** the legacy default. Ubiquitous, but it's a heavier, older service, and you'll spend time on it you could spend elsewhere. Starting here in 2026 is choosing tech debt.

**Why Unity Catalog OSS:** it gives you the open Iceberg REST endpoint and a path to real governance (access control, lineage) as you grow, without committing you to a vendor. You'll wire it in Chapter 5 and lean on it for serving (Chapter 10) and agents (Chapter 12).

> **Production gate:** your catalog is a critical, stateful service. Decide now where its metadata database (Postgres) lives and how it's backed up. A lost catalog means "my files exist but nothing knows they're tables." Treat it like the production database it is.

## 1.7 Decision 3. Object store: where do the bytes live?

**The call: S3 in production; run SeaweedFS (or MinIO) locally to build against the same S3 API.**

Object storage is the cheap, effectively infinite foundation. The important practitioner insight: you build against the S3 API, not a specific product, so the store is swappable. Develop locally on SeaweedFS, deploy on Amazon S3, and your tables and pipelines don't change. Only an endpoint and credentials do.

- **Amazon S3 (or GCS/Azure equivalent):** the production answer. Durable, zero-ops, the default for real deployments.
- **MinIO:** self-hosted, fully S3-compatible; common on-prem and in enterprise labs.
- **SeaweedFS:** lightweight, tiny footprint. Our choice for local, because it gives you a real S3 endpoint without eating your laptop.

We go deep on this in Chapter 3, including why object storage's quirks (no real folders, no in-place edits) shape everything above it. For now the decision is: S3-API everywhere, a light local store for the build.

## 1.8 Decision 4. Compute, and how you talk to it

**The call: Apache Spark, driven by Spark Connect (`sc://`), not the classic embedded driver.**

Spark is the workhorse that ingests, transforms, and streams. The decision that matters here isn't "Spark or not" (it's the safe default for a lakehouse); it's how your code talks to Spark. Spark Connect makes your client a thin gRPC library that drives a remote cluster, instead of bundling the whole engine into your process.

Why it's the modern default, in one breath: thin clients, connect from anywhere with one URL, and the headline win, isolation, so one bad job can't take down the shared cluster. The trade-off you accept: the DataFrame/SQL surface only (no low-level RDD internals), and static config must be set server-side. Full treatment in Chapter 4. The newer Spark Declarative Pipelines (Chapter 7) are built on Connect, so choosing it now pays off later.

## 1.9 Decision 5. Batch, streaming, or both on day one?

**The call: batch first. Add streaming only when a real freshness requirement demands it, and be suspicious of "real-time" requirements.**

This is where teams over-build. Streaming is genuinely harder to operate: unbounded state, late and out-of-order events, checkpoints, exactly-once headaches. It's worth that cost when minutes or seconds matter (fraud, live ops, alerting). It is not worth it when "within an hour" or "nightly" is actually fine, which, if you ask hard, is most "real-time" requests.

Ship the batch medallion first (Chapters 6-7). It's simpler, easier to make idempotent, and covers the majority of needs. Layer streaming on (Chapter 8) for the specific pipelines that truly need freshness, feeding the same tables. Both paths coexist. You don't have to choose one forever.

> **Production gate:** for every "real-time" requirement, write down the actual tolerated latency and who needs it. Half will collapse into "batch is fine," and you'll have saved yourself an operational burden.

## 1.10 Decision 6. Self-hosted vs. managed: who runs this?

**The call: prototype self-hosted (this course), then decide managed-vs-self per component for production based on your team's operational capacity.**

Everything you build here is self-hosted, which is perfect for learning the guts and for a cost-controlled prototype. For production, the honest tradeoff is that managed buys time and costs money and control: a managed Spark, Kafka, or Airflow means you don't patch, scale, or get paged for infrastructure. You pay for that, in dollars and some flexibility.

Because you build on open standards, this isn't a one-way door. Your Iceberg tables, Spark jobs, SDP pipelines, and Airflow DAGs port to managed services as a lift, not a rewrite. So the pragmatic path is: build it yourself to understand it, then move the pieces your team can't afford to operate onto managed services, keeping the open formats so you're never locked in. Chapter 13 tours the cloud and Terraform path.

## 1.11 The through-line: one real dataset, carried all the way

To keep this concrete instead of abstract, the whole build uses one realistic dataset: a stream of e-commerce order events. You'll land them raw, clean and conform them, aggregate them into business tables, schedule the pipeline, serve them to multiple engines, train a model on them, and finally let an agent operate the whole thing. One honest dataset (messy, continuous, aggregatable, with signal to learn from) exercises every layer the way real work does.

![One order event's journey through the stack](../figures/ch01/fig-1.5-order-event-journey.svg)

**Figure 1.5**. The order event's path across the layers: this is the data you'll follow from raw landing to business metric to model.

## 1.12 Your week-one sequence

Decisions made, here's the order I'd actually execute in. Each step is a chapter, and each ends with something you can verify:

1. **Setup** (Ch 2): prerequisites and the one CLI you'll drive everything with.
2. **Storage** (Ch 3): object store up, warehouse bucket, format locked.
3. **Compute** (Ch 4): Spark plus Spark Connect, first remote job.
4. **Tables & Catalog** (Ch 5): real Iceberg tables through Unity Catalog OSS.
5. **Ingestion and Transformation** (Ch 6-7): batch medallion, Bronze to Silver to Gold.
6. **Streaming** (Ch 8): only if you need it.
7. **Orchestration** (Ch 9): schedule it so it runs without you.
8. **Serving, AI, Agents** (Ch 10-12): expose it, learn from it, operate it.
9. **Deploy and harden** (Ch 13): the path to production and clean teardown.

Bottom-up, because each layer is only testable once the one beneath it works.

## 1.13 Production-readiness checklist for architecture decisions

Every chapter from here ends with the bare minimum to call this layer production-ready. For the architecture itself:

- [ ] **Lakehouse justified:** you can name the two-plus reasons you need one (§1.2).
- [ ] **Table format chosen and proven:** a test table reads from every consumer you'll have (§1.5).
- [ ] **Catalog chosen:** REST catalog, with a decision on where its metadata DB lives and how it's backed up (§1.6).
- [ ] **Object store strategy:** S3-API everywhere; local store selected (§1.7).
- [ ] **Compute transport:** Spark Connect, with awareness of the server-side-config constraint (§1.8).
- [ ] **Streaming scoped honestly:** each "real-time" need has a written latency SLA; batch-first otherwise (§1.9).
- [ ] **Self-host vs. managed:** a per-component plan for production, open formats preserved so migration is a lift (§1.10).

If every box is checked, you have a defensible architecture you could put in a design doc and hand to your team. That's the deliverable of this chapter.

## 1.14 Recap & what's next

- The hard-to-reverse decisions are table format and catalog. Make those deliberately; the rest are cheaper to change.
- Default to Iceberg plus Unity Catalog OSS (REST) plus S3-API storage plus Spark Connect, batch-first, self-host-to-learn and managed-where-you-must, and deviate only with a reason.
- Build bottom-up, one verifiable layer at a time, following one real dataset.
- **Next, Chapter 2, Setup:** install the prerequisites and bring up the one control CLI you'll drive the whole stack with.

![Progress: Foundations complete, Setup next](../figures/ch01/fig-1.6-progress-foundations-done.svg)

**Figure 1.6**. Architecture decisions locked. Next: Setup.

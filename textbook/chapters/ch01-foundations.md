# Chapter 1: Foundations

A **data lakehouse** is one system that combines two things teams used to run separately: the low-cost, scale-out storage of a data lake, and the transactions, schema guarantees, and governance of a data warehouse. Instead of keeping raw files in a lake and copying the cleaned-up results into a warehouse, you apply warehouse-style management directly to cheap lake storage. One copy of the data, reliable enough to report on.

An **open lakehouse** adds one rule to that: every layer is built on open standards. The file format, the table format, the engine that processes the data, the catalog that governs it, and the ML and AI tooling on top are all open, so no single layer is locked to one vendor. The practical test is concrete, not philosophical: you can read your data with any compatible engine, and move from one tool to another without rewriting or re-exporting it.

That openness is the whole reason this course can exist. Because every layer is an open standard, you can stand the entire stack up yourself, on your own machine, and own it end to end. This chapter is about the architecture you're committing to when you do that: what each layer is, why it's there, and the handful of choices that are expensive to get wrong.

![The decisions map onto the layers you'll build](../figures/ch01/fig-1.1-progress-map.svg)

**Figure 1.1**. The layers you'll stand up, bottom-up. Every decision below maps to one of these. We return to this map at the start of every chapter.

## Learning objectives

By the end of this chapter you'll be able to:

- Explain what a lakehouse is, and what the word "open" adds, in terms someone on your team would accept.
- Describe each layer of the reference architecture and the specific problem it solves, so you can tell which layers your situation actually needs.
- Weigh a tool decision on its merits: identify the axes that matter for a given layer, judge the options against them, and defend the choice you land on rather than copying a stack.
- Judge whether your organization needs a lakehouse at all, and recognize when a warehouse is the better answer.
- Sequence the build bottom-up, and state how you'd verify each layer before building on top of it.

## Gut-check: do you actually need a lakehouse?

Before you commit, be honest about whether this is the right tool, because "we should build a lakehouse" is sometimes just following the crowd. You need one when at least two of these are true:

- Your data is too big or too varied for a single Postgres or MySQL to serve analytics comfortably. You're into hundreds of GBs or TBs, or many different source systems.
- More than one engine needs to touch the same data: a BI tool, Spark, maybe a notebook or a model, without each keeping its own copy.
- You need cheap retention of large raw history for compliance, replay, or reprocessing, which a warehouse would make expensive.
- You want the same governed data to feed AI as well as analytics: training sets, features, and increasingly the retrieval context and tools an LLM or agent reads from, without standing up a separate data stack for it.
- You want to avoid locking either your storage or your compute to one vendor.

If you're a single team with a few dozen GB and one BI tool, a managed warehouse like BigQuery or Snowflake, or even a well-tuned Postgres, is probably the better answer and far less to operate. There's no prize for running infrastructure you didn't need.

What you're actually signing up for when you say yes is not just the storage. It's a way of working with the data, and the shape of that work has a name: the **medallion architecture**. Raw data lands in a **Bronze** layer exactly as it arrived, unaltered, so you always have a faithful record you can go back to. Bronze gets cleaned, typed, and de-duplicated into a **Silver** layer that's trustworthy enough to build on. Silver gets aggregated and modeled into **Gold** tables shaped for a specific use: the revenue dashboard, the model's training set, the finance report. Data moves in one direction, Bronze to Silver to Gold, and each stage has a clear job. Increasingly that same Gold layer is also what feeds AI: not just training a model, but serving the clean, governed context an LLM or agent retrieves at query time. The advantage is one source of truth, so the numbers a dashboard shows and the facts an agent answers with come from the same tables, under the same governance, instead of a bolted-on AI pile that quietly drifts from production.

That pattern is the reason the checklist above tips toward a lakehouse. Multiple consumers stop arguing about whose numbers are right because they all read the same Gold tables. Cheap raw history is exactly what Bronze is, kept indefinitely because storage is cheap. Reprocessing is just rebuilding Silver and Gold from Bronze when you find a bug. If none of that describes your situation, you don't need this yet. If two or more of those pains are real, the medallion is what you're buying, and the rest of this book is how you build it.

## The shape: an open lakehouse in one diagram

Here's the reference architecture you're going to build. It's worth spending a minute on, because everything after this is just filling it in one layer at a time.

![The open lakehouse architecture](../figures/ch01/fig-1.4-architecture-map.svg)

**Figure 1.4**. Object storage at the base; compute and the table/catalog layer above it; ingestion, transformation, and streaming in the middle; orchestration across them; serving, AI, and agents on top. This is the system you'll have running by the last chapter.

Read it bottom-up, because that's both how it's built and how it depends on itself: each layer only works once the one beneath it does. Object storage holds the bytes. The table format turns those bytes into real tables, and the catalog keeps track of them. Compute reads and writes those tables. Ingestion, transformation, and streaming are the data flows that move order data through Bronze, Silver, and Gold. Orchestration runs those flows on a schedule. Serving, AI, and agents are the consumers on top, reading the Gold tables the layers below produced. You don't need to memorize this yet; the next section walks each layer in turn.

It helps to see why this shape beats the two things it replaces. A warehouse is reliable but closed, and it welds storage to compute. A lake is open and cheap but gives you no guarantees, so a pile of files is never quite a trustworthy table. The lakehouse takes the cheap, open storage of the lake and adds the guarantees of the warehouse back on top, through the table format.

![Warehouse vs. lake vs. lakehouse](../figures/ch01/fig-1.2-warehouse-lake-lakehouse.svg)

**Figure 1.2**. A warehouse is reliable but closed and coupled; a lake is open and cheap but offers no guarantees; a lakehouse puts warehouse guarantees on cheap open storage via an open table format.

## What each layer is for, and the simpler thing it replaces

You just saw what each layer does. This section is the harder question: how do you tell whether you actually need it? Every layer here exists because a simpler approach breaks at a specific point. If you know where each one breaks, you can defer a layer honestly when you haven't hit that point, and defend it when someone asks why it's in the design. So for each layer: the simpler thing you'd reach for first, and the concrete failure that pushes you to the layer instead.

**Object store, instead of files on a disk or a warehouse's internal storage.** The naive option is to keep data on a big disk, or inside a database that owns its own storage. Both weld your data to one place. A disk doesn't scale past one machine and fills up; a warehouse only lets the warehouse read the data, so the moment a second engine needs it you're maintaining a second copy that drifts. Object storage breaks that: it's cheap, effectively unbounded, and readable by anything that speaks the S3 API. That decoupling of storage from compute is the property the entire lakehouse is built on, which is why this layer is the floor and never optional.

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

### How this stack shows up as you grow

You rarely build all of this at once, and you shouldn't. The layers tend to arrive in a predictable order as an organization's data grows, and locating yourself on that path is a good sanity check on which layers you actually need yet:

| Stage | What's true | Layers you're adding |
|---|---|---|
| **One team, one source** | A few dashboards off a production database or some files | Object store, table format: get a real, trustworthy table |
| **Data outgrows one box** | Queries are slow, files pile up, more than one person needs them | Compute, ingestion, transformation: the batch medallion |
| **The pipeline has to be reliable** | People depend on the numbers being fresh each morning | Orchestration, and streaming for the pieces that truly need it |
| **Many consumers** | BI tools, analysts, other teams, all wanting the same data | Serving through the catalog, so nobody keeps a private copy |
| **Data feeds products, not just reports** | Models and agents read the same governed tables | AI and agents on top of Gold |

If you're early on this path, most of the upper layers are things to design for but not build yet. If you're already fighting several of these pains at once, that's the signal you need the full stack sooner rather than later. Either way, you build bottom-up: the lower layers are what everything above them stands on.

## Object storage and the files underneath

Object storage is the foundation the whole stack sits on, and there's little to decide here: your store is dictated by where you run. What matters is that you build against the **S3 API**, not a specific product, so the store is swappable. Your tables and pipelines target an S3 endpoint; moving from local to cloud changes an endpoint and credentials, not your code.

On a cloud provider you use the native store, Amazon S3, Google Cloud Storage, or Azure Data Lake Storage. They're durable, effectively infinite, zero-ops, and already integrated with everything else you run there. There's no reason to run your own object store on top of a cloud that already provides one.

Self-hosting, on-prem or in local development, you run an S3-compatible store instead. Two open-source options matter:

- **MinIO.** The common choice for real self-hosted and on-prem deployments. Fully S3-compatible and mature, with the operational features (erasure coding, replication) you want if this is actually holding your data.
- **SeaweedFS.** Lighter, with a small footprint. A good fit when you want a real S3 endpoint without much overhead, which is why this course uses it for local development.

Both speak the S3 API, so which one you run locally changes nothing above it.

Underneath the table format, the bytes themselves are **Parquet** files: a columnar, compressed, open file format. Columnar means a query that touches 3 of 40 columns reads only those 3. Compression works well because a column holds one type of value. Each file also carries footer statistics (per-column min/max and counts) that let an engine skip files that can't match a filter. The table format organizes these Parquet files into a table with history and transactions; Parquet is what's actually on disk. You don't pick it, Iceberg and Delta both write it, but knowing it's a columnar file with stats in the footer explains a lot of why lakehouse queries are fast. Chapter 3 goes deeper on both object storage and Parquet's internals.

## Table format

The table format dictates how every byte is physically laid out and which engines can read it, so it's worth reasoning about rather than defaulting to one. There are three serious open formats: Iceberg, Delta, and Hudi. They all do the core job from the last section, the metadata layer that makes files behave like a table, so you don't choose on "does it have ACID." They all do. You choose on the axes where they actually differ:

- **Engine neutrality.** How many engines read this format without a conversion step? This is your lock-in risk and how painful multi-engine serving will be later. Weight it heavily if you expect more than one consumer, which is most orgs.
- **Governance of the spec.** Is the format controlled by one vendor's roadmap, or a multi-party open standard? This is a bet on the next few years, not this quarter.
- **Workload fit.** Is your dominant pattern append-heavy analytics, or high-frequency upserts and change-data-capture? The formats optimize for different answers, and this is also where streaming write latency and small-file handling differ.
- **Ecosystem gravity.** What is your org already committed to? A choice that fights your existing platform costs more than its technical merits are worth.

Weigh the options against those axes:

| | Engine neutrality | Spec governance | Best-fit workload |
|---|---|---|---|
| **Iceberg** | Widest: Spark, Trino, Flink, DuckDB, Snowflake, BigQuery | Open, multi-vendor (Apache) | Broad interop, large-scale analytics |
| **Delta** | Good, historically Spark-first; opening up via Delta Kernel | Open, but Databricks is the primary driver | Databricks-native workloads |
| **Hudi** | Good, Spark-centric | Open (Apache) | High-frequency upserts / CDC |

In practice the real choice is Iceberg or Delta; Hudi is the right answer only when heavy upserts and CDC are your primary workload. If you weight engine neutrality and open governance highest, which is the right call when you can't fully predict your future consumers, you land on Iceberg, and that's what this course uses. If your center of gravity is Databricks, Delta is the low-friction choice. Either way the reasoning should be visible enough that you can redo it if your weights differ.

Three practical notes once you've chosen:

**Use one format.** Running Iceberg and Delta side by side doubles your catalog config, engine-compatibility testing, and operational surface for no real gain. Pick one and make it the default everywhere.

**Migration is cheaper on data than on history.** Both formats store the same Parquet files underneath, so switching mostly regenerates the metadata layer over data that stays in place; you rarely rewrite the bytes. What doesn't carry over cleanly is history: converting Iceberg to Delta brings the current table state, not the snapshot lineage, so time travel and audit history are effectively lost. That, plus repointing everything that reads the tables, is why this is the decision you least want to redo.

**The formats are converging, so check current versions.** Delta can expose Iceberg-readable metadata, translation tools exist in both directions, and each release narrows the gap on the workload and latency differences above. Compare the current versions rather than older write-ups. None of it changes the recommendation: for an open, multi-engine lakehouse, use Iceberg.

> **Prove it before you commit:** write a small table in your chosen format and read it from every consumer you'll have, Spark, your BI tool, DuckDB, whatever. If any of them can't read it today, you've found the problem while it's still cheap to fix.

## Catalog

Your organization needs a single, authoritative answer to "what tables exist, which is the current version, and who is allowed to read or write each one." That's a governance problem, and it's the one an open table format alone doesn't solve. Iceberg and Delta give you rich per-table history, but the format sitting in a bucket doesn't decide who can access a table, doesn't vend credentials to readers and writers, and doesn't carry the organizational metadata (tags, ownership, roles) you need to run this as a shared source of truth. Something has to own that, and that something is the catalog.

At its most basic, the catalog answers "what tables exist, and where are their files?" Every engine consults it before it reads: it maps a table name to its current metadata location, and the metadata points at the data files. That's also how it always resolves the latest version of a table, which a bare pile of files can't reliably do on its own. On top of that lookup, a real catalog is where access control, credential vending, tags, and ownership live, so the same governance applies no matter which engine is asking.

The question that matters is not whether you run a catalog, you need one, but which kind, because it decides how many engines can share your tables. This is where the Iceberg REST Catalog (IRC) comes in. IRC is a standard HTTP interface for catalogs: any engine that speaks it, Spark, Trino, DuckDB, Flink, can find and read your tables through the same endpoint, with no per-engine wiring. It's the piece that turns "our tables" into "our tables, readable by anything under one set of rules," which is the whole point of committing to an open format.

The older alternative is the Hive metastore: ubiquitous, but a heavier, Thrift-based service from the Hadoop era, without the open multi-engine story IRC gives you or a real governance model on top. It still works, and plenty of production runs on it, but starting a new lakehouse on it in 2026 buys you operational weight and an older interface for no upside.

This course uses Unity Catalog OSS, which exposes an Iceberg REST endpoint and adds the governance path, access control, credential vending, lineage, tags, as you grow, without locking you to a vendor. It's not the only IRC-compatible option (Apache Polaris, Project Nessie, and others fill the same role), and because they share the REST interface, the catalog layer is genuinely swappable. You'll stand it up in Chapter 5 and lean on it again for serving and agents.

> **Treat the catalog as a production database.** It's a critical, stateful service backed by its own metadata store (Postgres). Decide early where that lives and how it's backed up: lose the catalog and you have files nothing recognizes as tables.

## Compute

Object storage holds the bytes and the table format makes them a table, but nothing has read or written anything yet. That's the compute layer: the engine that actually executes work, scanning files, joining tables, aggregating, writing new snapshots. Storage is where data rests; compute is what moves it.

A compute engine takes a query, in SQL or in code, and turns it into an actual plan of work: which files to read, in what order, how to join and aggregate, and how to spread that across cores or machines. That last part is the point. On a single machine, a tool like pandas loads the data into one process's memory and works on it there, which is fine until the data is bigger than that machine's memory, or the job is too slow on one set of cores, or more than one person needs to run work at once. A distributed engine removes those ceilings: it splits a job into tasks that run in parallel across many workers, handles data far larger than any one machine, and reschedules work elsewhere if a worker fails. That is what you're buying that local doesn't give you, scale past one box, parallelism, and fault tolerance.

Keeping compute separate from storage matters for the same practical reasons. You can scale the engine up for a heavy job and back down when it's done without touching the data, and you can point more than one engine at the same tables. The engine interprets your queries, so a few things are worth deciding: whether you lead with SQL or a DataFrame API (most engines do both), how much control you want over how a job is executed and optimized, and how you run the engine, locally for development, or as a shared cluster once multiple people and jobs depend on it. Those needs change as you grow, so flexibility here is worth protecting.

For the processing layer, Apache Spark is the default. It's the mature distributed engine for exactly this: it handles data too big for one machine, runs the same code over gigabytes or terabytes, speaks both SQL and a rich DataFrame API, does batch and streaming, and connects to Iceberg, Delta, Kafka, and object storage natively. Lighter single-node engines like DuckDB and Polars are excellent in their place, which shows up later in serving, but Spark is the workhorse for the ingestion and transformation core.

The part worth understanding up front is not "Spark or not," it's how your code talks to Spark. The traditional model bundles the whole engine into your application process: your program is the Spark driver. That couples your client to the cluster, forces your code to run in the cluster's environment, and means one heavy client can destabilize the shared engine. Spark Connect changes that: your program becomes a thin client that sends its query plan over gRPC to a remote cluster, which does the work and streams results back. You get a lightweight client, a single URL to connect from anywhere, and isolation, so one bad job runs in its own session instead of taking down the cluster for everyone. The trade-off is that you work through the DataFrame and SQL surface rather than low-level engine internals, and some configuration is set server-side. For a lakehouse that's the right trade, and the declarative-pipeline approach you'll use for transformations is built on Connect.

## Batch and streaming

The difference people usually have in mind is about where the data comes from and what processes it. Batch means data at rest, files or tables you read on a schedule, transform, and write back, classically a Spark job over data in object storage. Streaming means data in motion, an unbounded feed of events you react to as they arrive, classically a Kafka topic processed by a dedicated stream engine like Flink. Two sources, two engines, two operational models. For years that meant maintaining two separate stacks that were supposed to agree and often didn't.

What's changed is convergence. Streaming and batch increasingly feed the same lakehouse rather than living in parallel systems. A common shape: events stream off Kafka straight into your Bronze layer as they arrive, and you still transform and serve from Silver and Gold the same way you would for batch data. The consumer reading a Gold table doesn't know or care whether the rows underneath arrived in a nightly batch or a live stream. And the engine has converged too. Spark Structured Streaming now handles the streaming job that used to require a separate Flink deployment, so the same engine, the same catalog, and the same governance cover both paths. That matters because the reason to keep streaming inside your lakehouse instead of off to the side is exactly governance and one source of truth, the same access control, lineage, and table definitions applying whether data is streamed or batched.

That's why this course builds streaming in natively rather than treating it as a separate topic. You'll land streaming data through Kafka into the same tables your batch pipelines write, on one Spark-based stack, so you can see what it looks like to have both without running two systems.

Streaming earns its place when freshness is a real requirement, not a preference. Fraud detection, live operational dashboards, and alerting need data in seconds or minutes, and there the added cost is justified. And it is added cost: streaming means managing unbounded state, late and out-of-order events, checkpoints, and exactly-once delivery, all of which are more to operate than a scheduled batch job. The trap is treating "real-time" as a default when nobody has quantified the latency they actually need. As a rule of thumb you start with batch, which covers most needs and is simpler to operate, and add streaming for the pipelines where freshness genuinely matters, both living in the same lakehouse.

## Serving

Everything up to here has been about producing trustworthy tables. Serving is the other side: getting those tables to the people and tools that consume them, BI dashboards, analysts writing SQL, notebooks, other engines, models. It's the layer where the lakehouse actually pays off, because data nobody can reach isn't worth the pipeline that built it.

The instinct most teams start with is to export: copy the finished data out to wherever the consumer lives, a warehouse for the BI tool, a CSV for the analyst, a separate store for the app. Every export is a second copy that starts drifting from the source the moment it's made, and soon you're maintaining a web of extracts that disagree with each other and with production. That's the problem serving through the lakehouse solves.

In an open lakehouse, serving is mostly something you already built. Because the tables live in an open format and are registered in the catalog, any engine that speaks the catalog's interface can read them in place, no export, no copy. It helps to separate two categories here. First, the engines that actually read the tables: a lightweight single-node engine like DuckDB is often the right tool for an analyst querying a Gold table on their laptop, while a distributed SQL engine like Trino fits when many people need to query shared tables at once. This is where those lighter engines earn their place, DuckDB does the read even though Spark did the heavy lifting upstream. Second, the BI and dashboard tools that sit on top of an engine rather than reading Iceberg themselves: something like Apache Superset or Metabase connects through one of those engines to visualize results. This course uses DuckDB as the concrete example, since it reads Iceberg directly with no server to stand up, which suits a laptop-scale build. The point in every case is the same: one copy of the data, many readers, the same governance applying to all of them.

The judgment in this layer is mostly about shaping Gold for its consumers and controlling access, not about moving data around. You model Gold tables for the questions people actually ask, and you use the catalog's access control to decide who can read what. Serving isn't a separate system you stand up so much as the natural consequence of having committed to open tables and a shared catalog in the first place.

## Self-hosted or managed

Every layer here can be run two ways: you host it yourself, or you pay a provider to run it for you. Managed Spark, managed Kafka, managed Airflow, they all exist, and the tradeoff is the same in each case. Self-hosting gives you full control and the lowest dollar cost, and it asks for your time: you patch, scale, monitor, and get paged when something breaks at 3am. Managed inverts that, you hand over the operational burden and pay for it in dollars and some loss of flexibility, but nobody on your team is responsible for keeping the service up.

Which way to lean depends on your team's operational capacity, not on principle. A small team without a platform group usually gets more value from managed services for the heavy, stateful pieces (a managed Kafka or a managed Spark), and self-hosting the rest. A team with real ops muscle might run more of it themselves to control cost and behavior. There's no single right answer, only the honest question of what your team can actually operate.

The reason this isn't a decision you have to get perfectly right up front is that open standards keep it reversible. Because your tables are Iceberg, your jobs are Spark, and your pipelines and DAGs are standard, moving a component onto a managed service later is a migration, not a rewrite. So the practical path, and the one this course takes, is to build it yourself first. You learn how every layer actually works, you run it at zero infrastructure cost while you learn, and then in production you move the pieces your team can't afford to operate onto managed services, keeping the open formats so you're never locked to one provider.

## The through-line: one real dataset, carried all the way

To keep this concrete instead of abstract, the whole build uses one realistic dataset: a stream of e-commerce order events. You'll land them raw, clean and conform them, aggregate them into business tables, schedule the pipeline, serve them to multiple engines, train a model on them, and finally let an agent operate the whole thing. One honest dataset (messy, continuous, aggregatable, with signal to learn from) exercises every layer the way real work does.

![One order event's journey through the stack](../figures/ch01/fig-1.5-order-event-journey.svg)

**Figure 1.5**. The order event's path across the layers: this is the data you'll follow from raw landing to business metric to model.

## Your week-one sequence

Decisions made, here's the order I'd actually execute in. Each step is a chapter, and each ends with something you can verify:

1. **Setup:** prerequisites and the one CLI you'll drive everything with.
2. **Storage:** object store up, warehouse bucket, format locked.
3. **Compute:** Spark plus Spark Connect, first remote job.
4. **Tables and Catalog:** real Iceberg tables through Unity Catalog OSS.
5. **Ingestion and Transformation:** batch medallion, Bronze to Silver to Gold.
6. **Streaming:** only if you need it.
7. **Orchestration:** schedule it so it runs without you.
8. **Serving, AI, Agents:** expose it, learn from it, operate it.
9. **Deploy and harden:** the path to production and clean teardown.

Bottom-up, because each layer is only testable once the one beneath it works.

## Production-readiness checklist for architecture decisions

Every chapter from here ends with the bare minimum to call this layer production-ready. For the architecture itself:

- [ ] **Lakehouse justified:** you can name the two-plus reasons you need one.
- [ ] **Table format chosen and proven:** a test table reads from every consumer you'll have.
- [ ] **Catalog chosen:** REST catalog, with a decision on where its metadata DB lives and how it's backed up.
- [ ] **Object store strategy:** S3-API everywhere; local store selected.
- [ ] **Compute transport:** Spark Connect, with awareness of the server-side-config constraint.
- [ ] **Streaming scoped honestly:** each "real-time" need has a written latency SLA; batch-first otherwise.
- [ ] **Self-host vs. managed:** a per-component plan for production, open formats preserved so migration is a lift.

If every box is checked, you have a defensible architecture you could put in a design doc and hand to your team. That's the deliverable of this chapter.

## Recap & what's next

- The hard-to-reverse decisions are table format and catalog. Make those deliberately; the rest are cheaper to change.
- Default to Iceberg plus Unity Catalog OSS (REST) plus S3-API storage plus Spark Connect, batch-first, self-host-to-learn and managed-where-you-must, and deviate only with a reason.
- Build bottom-up, one verifiable layer at a time, following one real dataset.
- **Next, Chapter 2, Setup:** install the prerequisites and bring up the one control CLI you'll drive the whole stack with.

![Progress: Foundations complete, Setup next](../figures/ch01/fig-1.6-progress-foundations-done.svg)

**Figure 1.6**. Architecture decisions locked. Next: Setup.

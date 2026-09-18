# 🏠 Build Your First Open Lakehouse — from Storage to Agents

**Everybody talks about the "modern data stack." This course makes you build one — the whole thing — on your own laptop.**

No cloud account. No credit card. No mystery managed service doing the interesting parts for you behind a paywall. Just you, your machine, and a stack of real, production-grade open-source tools that you'll wire together one layer at a time until you have a living data lakehouse that does **batch**, **streaming**, and **AI** — and can even be driven by an **AI agent** at the end.

By the last chapter, the intimidating architecture diagram you see in every "data engineering" blog post won't be intimidating anymore. It'll be *yours*. You'll have typed every command that built it.

---

## Wait — what's a "lakehouse," and why should I care?

Short version: for years you had to pick between two bad options.

- A **data warehouse** — reliable and fast, but closed, proprietary, and expensive. Your data lives in *their* box.
- A **data lake** — cheap and open, just files in storage… but no transactions, no reliable schema, no guarantees. It tends to rot into a "data swamp."

A **lakehouse** refuses to choose. You keep your data as cheap open files in object storage, then add an **open table format** (Apache Iceberg) on top that gives you warehouse-grade superpowers — ACID transactions, schema evolution, and *time travel* (yes, you can query your table as it looked last Tuesday, or undo a bad delete). Open format, cheap storage, warehouse guarantees. That's the whole magic trick, and you're going to build it from the ground up.

---

## Who this is for

This is for you if you can write a bit of **Python** and **SQL**, are comfortable in a **terminal**, and are tired of tutorials that stop at "and then the platform handles the rest." 🙂

You do **not** need to already know Spark, Docker internals, Kafka, or distributed systems. Every concept gets defined the moment you need it — right before you build the thing that uses it. We teach tools in service of the *idea*, never the other way around.

**What you'll need:** a laptop with ~16 GB of RAM (8 GB works if you're patient), Docker, and about an afternoon of curiosity. Everything runs locally and tears down with a single command when you're done — you get your machine back, no leftover mess.

---

## How the course works

The whole thing is written as a **textbook** — read it front to back, one chapter at a time. Every chapter builds **one layer** of the lakehouse on top of the last, so the system grows under your hands. We follow a single story throughout: a stream of realistic **e-commerce order events** that you'll ingest, clean, aggregate, schedule, serve, learn from, and eventually hand off to an agent.

There's a companion **code repository** too — [`COURSE.md`](COURSE.md) maps each chapter to a git tag, so you can check out the exact working state for any layer and pick up wherever you like.

Start here → **[textbook/README.md](textbook/README.md)**, then read **[textbook/chapters/](textbook/chapters/)** in order.

---

## 🗺️ The curriculum

You'll build the lakehouse **bottom-up** — because you can't transform data before you can store and compute it. Thirteen chapters, twelve layers, one working system.

| # | Chapter | The layer you build | The "aha" moment |
|---|---------|--------------------|------------------|
| 1 | **Foundations** | *(the map)* | See the finished lakehouse run, then understand the shape of everything you're about to build. |
| 2 | **Setup** | Your environment + the control CLI | One command (`./lakehouse setup`) and your whole workshop is ready. |
| 3 | **Storage** | Object storage + open table formats | Where the bytes live (S3 / MinIO / SeaweedFS) and how loose files become a *table* — meet Iceberg. |
| 4 | **Compute** | Apache Spark, via **Spark Connect** | Drive a real Spark cluster from a tiny client over `sc://` — no heavyweight driver bolted to your app. |
| 5 | **Tables & Catalog** | Iceberg through **Unity Catalog OSS** | Create a real transactional table, evolve its schema, then *time-travel* and roll back a mistake. ✨ |
| 6 | **Ingestion** | Landing raw data → **Bronze** | Load data the modern way (ELT), and make it safely re-runnable so retries never duplicate rows. |
| 7 | **Transformation** | The **medallion** with Spark Declarative Pipelines | Declare Bronze→Silver→Gold and let the engine figure out the order, the refresh, and the quality checks. |
| 8 | **Streaming** | Kafka → Structured Streaming → Bronze | The *same* table, now fed in real time — with watermarks and exactly-once dedup done honestly. |
| 9 | **Orchestration** | An **Airflow** DAG | Put the pipeline on a schedule with retries and backfills, so it runs itself while you sleep. |
| 10 | **Serving** | One open catalog, many engines | Query the *exact same* tables from a totally different engine — no copies, no exports. This is the payoff of "open." |
| 11 | **AI** | Train + track a model with **MLflow** | Feed your clean Gold tables to a model, and log *which snapshot* trained it — full reproducibility, for free. |
| 12 | **Agents** | An LLM that *operates* the lakehouse | Ask, in plain English, for the stack to spin up and the pipeline to run — and watch an agent actually do it. 🤖 |
| 13 | **Deploy & Teardown** | Cloud path + a clean exit | Tour the road to the cloud (without a bill), then tear it all down and see where to go next. |

**The stack you'll come out knowing:** SeaweedFS · Apache Spark (Spark Connect) · Apache Iceberg · Unity Catalog OSS · Apache Kafka · Apache Airflow · Spark Declarative Pipelines · MLflow.

And here's the quiet secret: **these are the same open tools the big paid platforms are built on.** Learn them here, directly, and moving to something like Databricks or EMR later is a small step — you'll already know what's under the hood.

---

## What's in this repo

```
lakehouse-masterclass/
├── README.md          ← you are here 👋
├── COURSE.md          ← chapter → code tag → commands → checkpoint map
└── textbook/          ← the course itself
    ├── README.md          the table of contents
    ├── chapters/          ch01 … ch13 — one Markdown file per chapter
    └── figures/           the diagrams (lots of them — this course is visual)
```

Every chapter is built the same way: a quick "here's what you'll build," clear objectives, each concept explained *right* before you use it, copy-pasteable build steps with the output you should expect, a checkpoint to prove the layer works, and a recap. Heavy on diagrams, light on jargon.

---

## Ready?

Grab a coffee, open a terminal, and head to **[the first chapter →](textbook/chapters/ch01-foundations.md)**.

By the end, you won't just *know about* data lakehouses. You'll have built one. 🚀

## License

See [`LICENSE`](LICENSE).

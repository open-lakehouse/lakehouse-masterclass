# Build Your First Open Lakehouse

![Build Your First Open Lakehouse, from Storage to Agents](img/banner.jpg)

**Everybody talks about the "modern data stack." This course makes you build one, the whole thing, on your own machine.**

No cloud account. No credit card. No managed service doing the interesting parts for you behind a paywall. Just you, your machine, and a stack of real, production-grade open-source tools that you wire together one layer at a time until you have a living data lakehouse that does batch, streaming, AI, and can even be driven by an AI agent at the end.

By the last chapter, the intimidating architecture diagram you see in every data-engineering blog post will be yours. You will have typed every command that built it, and you will understand every layer well enough to defend the choices to your team.

[Start with Chapter 1: Foundations](chapters/ch01-foundations.md){ .md-button .md-button--primary }

## Who this is for

You are comfortable with a terminal, you can read Python and SQL, and you want to understand a lakehouse by building one rather than by reading a vendor's marketing page. Maybe you have been handed the job of standing one up for your team. Either way, you will finish with a real stack and the judgment to run it.

## What you will build

One growing project, on one machine, using open standards the whole way down:

- **Storage** on an S3-compatible object store (SeaweedFS), with Apache Iceberg as the open table format
- **Compute** with Apache Spark, driven over Spark Connect
- **Tables and a catalog** with Unity Catalog's Iceberg REST catalog
- **Ingestion, transformation, and streaming** through the medallion layers, batch and Kafka on one stack
- **Orchestration** with Airflow, **serving** to any engine, **AI** with MLflow, and an **agent** that operates the whole thing

Everything runs locally in containers, and everything is open source, so the skills move straight to any cloud.

## The chapters

Read them in order. Each one builds a layer on top of the last.

1. [Foundations](chapters/ch01-foundations.md) — the decisions that are expensive to get wrong
2. [Setup](chapters/ch02-setup.md) — the environment, from scratch, with Docker Compose
3. [Storage](chapters/ch03-storage.md) — the object store and the open table format
4. [Compute](chapters/ch04-compute.md) — Spark, and why Spark Connect
5. [Tables and Catalog](chapters/ch05-tables-catalog.md) — Iceberg tables through Unity Catalog
6. [Ingestion](chapters/ch06-ingestion.md) — landing raw data into Bronze
7. [Transformation](chapters/ch07-transformation.md) — the medallion with Spark Declarative Pipelines
8. [Streaming](chapters/ch08-streaming.md) — Kafka into the same tables
9. [Orchestration](chapters/ch09-orchestration.md) — scheduling the pipeline with Airflow
10. [Serving](chapters/ch10-serving.md) — one catalog, many engines
11. [AI](chapters/ch11-ai.md) — training and tracking a model on the lakehouse
12. [Agents](chapters/ch12-agents.md) — an agent that operates the stack
13. [Deploy and Teardown](chapters/ch13-deploy-teardown.md) — where to go next

---

The full source, including the figure files and the finished code, lives in the [GitHub repository](https://github.com/open-lakehouse/lakehouse-masterclass).

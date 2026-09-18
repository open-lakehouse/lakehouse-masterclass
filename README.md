# Build Your First Open Lakehouse — Storage to Agents

A hands-on, open-source data-engineering course that walks you through building a
complete open data lakehouse on your own laptop — capable of **batch**,
**streaming**, and **AI**, and finally driven by an **AI agent** — using a proven
stack: SeaweedFS, Apache Spark (via **Spark Connect**), Apache Iceberg, **Unity
Catalog OSS**, Apache Kafka, Apache Airflow, MLflow, and Spark Declarative
Pipelines. No cloud account. No bill. 100% open source.

The course is written as a **textbook**: read it front to back, and each chapter
builds one layer of the lakehouse on top of the previous one.

## Repository layout

```
lakehouse-masterclass/
├── README.md          ← you are here
├── COURSE.md          ← chapter → code tag → commands → checkpoint map
└── textbook/          ← the course itself
    ├── README.md          table of contents
    ├── chapters/          ch01 … ch13 (one Markdown file per chapter)
    └── figures/           diagrams, organized per chapter (chNN/) + a generator
```

## How to read it

Start with [`textbook/README.md`](textbook/README.md) for the chapter list, then
read [`textbook/chapters/`](textbook/chapters/) in order. [`COURSE.md`](COURSE.md)
maps each chapter to a tag in the companion code repository so you can check out the
exact state for any layer.

## The twelve layers

Foundations → Setup → **Storage** → **Compute** → **Tables & Catalog** →
**Ingestion** → **Transformation** → **Streaming** → **Orchestration** →
**Serving** → **AI** → **Agents** → Deploy & Teardown.

## Conventions

- **One chapter = one layer**, built bottom-up.
- **Figures** live under `textbook/figures/chNN/` and are referenced with relative
  paths from each chapter (`../figures/chNN/…`).
- Every chapter shares the same structure — opener, objectives, concepts with
  diagrams, build steps, checkpoint, and recap.

## License

See [`LICENSE`](LICENSE).

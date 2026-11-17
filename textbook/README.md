# Textbook: table of contents

The book itself. Read the chapters in order; each builds one layer of the lakehouse
on top of the previous one. Diagrams live in `figures/chNN/`.

## Chapters

| # | Layer | File | Status |
|---|-------|------|--------|
| 1 | Foundations | `chapters/ch01-foundations.md` | ✅ drafted |
| 2 | Setup | `chapters/ch02-setup.md` | ✅ drafted |
| 3 | Agentic data engineering | `chapters/ch03-agentic-data-engineering.md` | ✅ drafted |
| 4 | Storage | `chapters/ch04-storage.md` | ✅ drafted |
| 5 | Compute (Spark Connect) | `chapters/ch05-compute.md` | ✅ drafted |
| 5.5 | Iceberg for Spark 4.3 (temporary) | `chapters/ch05-5-iceberg-runtime.md` | ✅ drafted |
| 6 | Tables & Catalog (Iceberg + Unity Catalog OSS) | `chapters/ch06-tables-catalog.md` | ✅ drafted |
| 7 | Ingestion | `chapters/ch07-ingestion.md` | ✅ drafted |
| 8 | Transformation (Spark Declarative Pipelines) | `chapters/ch08-transformation.md` | ✅ drafted |
| 9 | Streaming | `chapters/ch09-streaming.md` | ✅ drafted |
| 10 | Orchestration | `chapters/ch10-orchestration.md` | ✅ drafted |
| 11 | Serving | `chapters/ch11-serving.md` | ✅ drafted |
| 12 | AI | `chapters/ch12-ai.md` | ✅ drafted |
| 13 | Agents | `chapters/ch13-agents.md` | ✅ drafted |
| 14 | Deploy & Teardown | `chapters/ch14-deploy-teardown.md` | ✅ drafted |

## Figures

Organized per chapter under `figures/`:

- `figures/ch01/` … `figures/ch14/` holds the diagrams each chapter embeds.
- `figures/_tools/gen_progress_maps.py` stamps the recurring progress-map SVGs
  with the right layer highlighted; run it from anywhere, it writes into the
  correct `figures/chNN/` folders.

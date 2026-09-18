# Textbook — table of contents

The book itself. Read the chapters in order; each builds one layer of the lakehouse
on top of the previous one. Diagrams live in `figures/chNN/`.

## Chapters

| # | Layer | File | Status |
|---|-------|------|--------|
| 1 | Foundations | `chapters/ch01-foundations.md` | ✅ drafted |
| 2 | Setup | `chapters/ch02-setup.md` | ✅ drafted |
| 3 | Storage | `chapters/ch03-storage.md` | ✅ drafted |
| 4 | Compute (Spark Connect) | `chapters/ch04-compute.md` | ✅ drafted |
| 5 | Tables & Catalog (Iceberg + Unity Catalog OSS) | `chapters/ch05-tables-catalog.md` | ✅ drafted |
| 6 | Ingestion | `chapters/ch06-ingestion.md` | ✅ drafted |
| 7 | Transformation (Spark Declarative Pipelines) | `chapters/ch07-transformation.md` | ✅ drafted |
| 8 | Streaming | `chapters/ch08-streaming.md` | ✅ drafted |
| 9 | Orchestration | `chapters/ch09-orchestration.md` | ✅ drafted |
| 10 | Serving | `chapters/ch10-serving.md` | ✅ drafted |
| 11 | AI | `chapters/ch11-ai.md` | ✅ drafted |
| 12 | Agents | `chapters/ch12-agents.md` | ✅ drafted |
| 13 | Deploy & Teardown | `chapters/ch13-deploy-teardown.md` | ✅ drafted |

## Figures

Organized per chapter under `figures/`:

- `figures/ch01/` … `figures/ch13/` — the diagrams each chapter embeds.
- `figures/_tools/gen_progress_maps.py` — stamps the recurring progress-map SVGs
  with the right layer highlighted; run it from anywhere, it writes into the
  correct `figures/chNN/` folders.

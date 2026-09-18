# Companion code map

Each chapter of the textbook builds one layer of the lakehouse. This maps every
chapter to a **git tag** in the companion code repository, the **key commands** that
chapter runs, and the **checkpoint** that proves the layer works — so you can check
out any tag and have everything from chapter 1 up to that point running.

## Tag convention

- One tag per chapter end-state: `ch03-storage`, `ch06-ingestion`, … — check out
  `ch06-ingestion` and you have everything from chapters 1–6 working.
- `starter` = scaffolded but empty (prerequisites + CLI, no pipelines) for a clean start.
- `main` = the final, complete lakehouse (matches the Chapter 1 demo).

## Chapter → tag → commands → checkpoint

| Ch | Layer | Tag | Key commands | Checkpoint |
|----|-------|-----|--------------|------------|
| 1 | Foundations | `starter` | — (concepts only) | Can name the 10 layers |
| 2 | Setup | `ch02-setup` | `./lakehouse setup`, `./lakehouse status` | All prerequisites green |
| 3 | Storage | `ch03-storage` | start object storage; create the warehouse bucket | S3 bucket reachable; Iceberg chosen as the table format |
| 4 | Compute | `ch04-compute` | start Spark + the Connect server; connect a thin client via `sc://localhost:15002` | Spark UI up + first Connect job runs |
| 5 | Tables & Catalog | `ch05-tables-catalog` | create an Iceberg table; run a time-travel query | Snapshot rollback works |
| 6 | Ingestion | `ch06-ingestion` | `./lakehouse testdata generate`; batch load → Bronze | Bronze row count > 0; re-run is idempotent |
| 7 | Transformation | `ch07-transformation` | run the Bronze→Silver→Gold pipeline | Gold table populated |
| 8 | Streaming | `ch08-streaming` | `./lakehouse testdata stream`; streaming consumer | Live rows landing in Bronze |
| 9 | Orchestration | `ch09-orchestration` | start Airflow; trigger the DAG | DAG green in the UI |
| 10 | Serving | `ch10-serving` | expose tables via the Unity Catalog OSS REST catalog; read from a second engine | Same tables, multiple engines |
| 11 | AI | `ch11-ai` | train + track a model in MLflow; register it | Run + model in the MLflow UI |
| 12 | Agents | `ch12-agents` | an agent drives `./lakehouse` via its skills | Agent runs the demo end to end |
| 13 | Deploy & Teardown | `main` | cloud deploy tour; `./lakehouse stop all` | Clean teardown |

## Per-chapter READMEs

Each tag ships a `chapters/chNN/README.md` in the code repository that repeats the
exact commands and expected output from the matching textbook chapter, so the code
and the textbook never drift.

# Course map: chapter, commands, checkpoint

Each chapter of the textbook builds one layer of the lakehouse in a project you
create yourself (`open-lakehouse/`). This page maps every chapter to the **key
commands** it runs and the **checkpoint** that proves the layer works, so you can
see the whole build at a glance or find where a command comes from.

Commands assume you are in the project root with its virtual environment active
(`source .venv/bin/activate`). From Chapter 13 on, the same operations are also
`make` targets; they are listed where they apply.

## Chapter → commands → checkpoint

| Ch | Layer | Key commands | Checkpoint |
|----|-------|--------------|------------|
| 1 | Foundations | (concepts only) | Can name the layers and the decisions behind them |
| 2 | Setup | `python3 -m venv .venv`, `pip install -e .`, `docker run hello-world` | `import lakehouse` works; Docker runs a container; first commit is `.gitignore` |
| 3 | Storage | `docker compose up -d seaweedfs`; `aws --endpoint-url http://localhost:8333 s3 mb s3://lakehouse` | Bucket exists; a file round-trips through the S3 API; unauthenticated requests get 403 |
| 4 | Compute | `docker compose up -d`; `SparkSession.builder.remote("sc://localhost:15002")`; `lakehouse/session.py` | Spark UI shows an ALIVE worker; `spark.range(5).show()` runs remotely |
| 5 | Tables & Catalog | `docker compose up -d unity-catalog`; create the `iceberg` catalog (REST `POST .../catalogs`); `CREATE NAMESPACE` bronze/silver/gold | Iceberg table created, evolved, time-traveled, and rolled back through Unity Catalog |
| 6 | Ingestion | `python -m lakehouse.generate 2026-01-05`; `python -m lakehouse.ingest 2026-01-05` (`make ingest DATE=...`) | Bronze has the day's rows; re-running leaves the count unchanged |
| 7 | Transformation | `spark-pipelines run --remote sc://localhost:15002 --refresh silver.orders`, then `--refresh gold.daily_revenue` (`make pipeline`); `pytest tests/` (`make test`) | Silver is clean (no bad rows), Gold is populated, tests pass |
| 8 | Streaming | `docker compose up -d zookeeper kafka`; `python -m lakehouse.stream_source`; `python -m lakehouse.stream_ingest` | Bronze count climbs; stream rows have no duplicate `order_id`s; restart resumes from the checkpoint |
| 9 | Orchestration | `docker compose up -d --build airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor`; `airflow dags trigger ... --logical-date ...`; `airflow backfill create ... --max-active-runs 1` | DAG runs its tasks green in order; backfilled days appear in Gold |
| 10 | Serving | `pip install duckdb`; DuckDB `ATTACH 'iceberg' AS uc (TYPE iceberg, ...)` | DuckDB returns Gold rows identical to Spark's |
| 11 | AI | `docker compose up -d --build mlflow`; `python ml/train.py` (`make train`); `mlflow.register_model(...)` | Run with the Gold snapshot id in MLflow; registered model loads by name and version |
| 12 | Agents | write `AGENTS.md`; give a coding agent a goal | Agent operates the stack via `make` targets and its result checks out |
| 13 | Deploy & Teardown | `make down` / `make up`; `docker compose down -v` (destructive) | Data survives `down` + `up`; `down -v` is understood and gated |

## Companion code repository (planned)

The plan is a companion repository with one git tag per chapter end-state
(`ch03-storage`, `ch06-ingestion`, …, and `main` for the finished project), so you
could check out any layer and pick up from there. It does not exist yet. Until it
does, the textbook is self-contained: every file you need is written out in full in
the chapter that introduces it, and Chapter 13 shows the final project tree.

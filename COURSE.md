# Course map: chapter, commands, checkpoint

Each chapter of the textbook builds one layer of the lakehouse in a project you
create yourself (`open-lakehouse/`). This page maps every chapter to the **key
commands** it runs and the **checkpoint** that proves the layer works, so you can
see the whole build at a glance or find where a command comes from.

Commands assume you are in the project root with its virtual environment active
(`source .venv/bin/activate`). Chapter 14 adds a `Makefile` that wraps the common
operations; the matching target is shown in parentheses as "Ch14:", for when you
come back to a step later. Until Chapter 14, run the commands themselves.

## Chapter → commands → checkpoint

| Ch | Layer | Key commands | Checkpoint |
|----|-------|--------------|------------|
| 1 | Foundations | (concepts only) | Can name the layers and the decisions behind them |
| 2 | Setup | `python3.12 -m venv .venv`, `pip install -e .`, `docker run hello-world` | `import lakehouse` works; Docker runs a container; first commit is `.gitignore` |
| 3 | Agentic data engineering | `ollama create sdp-qwen3 -f agents/sdp_agent/Modelfile`; `pip install omnigent==0.22.0 pytest`; `pytest tests/test_policies.py -q`; `policy_modules` in `~/.omnigent/config.yaml`; `omnigent run agents/sdp_agent` | 5 policy tests pass; in a session, a `run` before a passing dry-run is DENYed with its reason, and a `collect()` gets an ASK |
| 4 | Storage | `docker compose up -d seaweedfs`; `aws --endpoint-url http://localhost:8333 s3 mb s3://lakehouse` | Bucket exists; a file round-trips through the S3 API; unauthenticated requests get 403 |
| 5 | Compute | `docker/spark/Dockerfile` (Python 3.12 + S3 jars); `docker compose build spark-connect`; `docker compose up -d`; `SparkSession.builder.remote("sc://localhost:15002")`; `lakehouse/session.py` | Spark UI shows an ALIVE worker; `spark.range(5).show()` runs remotely; a Python UDF reports the worker's Python as `3.12.x` |
| 5.5 | Iceberg for Spark 4.3 (temporary) | add the `iceberg` build stage and its `COPY` line to `docker/spark/Dockerfile`; `docker compose build spark-connect`; `docker compose up -d` | `/opt/spark/jars` holds `iceberg-spark-runtime-4.3_2.13.jar`, built from the pinned commit |
| 6 | Tables & Catalog | `docker compose up -d unity-catalog`; create the `iceberg` catalog (REST `POST .../catalogs`); `CREATE NAMESPACE` bronze/silver/gold, and `sandbox_sdp` for the agent | Iceberg table created, evolved, time-traveled, and rolled back through Unity Catalog |
| 7 | Ingestion | `python -m lakehouse.generate 2026-01-05`; `python -m lakehouse.ingest 2026-01-05` (Ch14: `make ingest DATE=...`) | Bronze has the day's rows; re-running leaves the count unchanged |
| 8 | Transformation | `spark-pipelines run --remote sc://localhost:15002` (Ch14: `make pipeline`), run twice to check Gold still sums to Silver; `pytest tests/` (Ch14: `make test`) | Silver is clean (no bad rows), Gold is populated, tests pass; the agent's `iceberg.sandbox_sdp.revenue_by_customer` matches the same numbers computed from Silver |
| 9 | Streaming | `docker compose up -d zookeeper kafka`; `python -m lakehouse.stream_source`; `python -m lakehouse.stream_ingest` | Bronze count climbs; stream rows have no duplicate `order_id`s; restart resumes from the checkpoint |
| 10 | Orchestration | `docker compose up -d --build airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor`; `airflow dags unpause lakehouse_medallion_pipeline`; `airflow dags trigger ... --logical-date ...`; `airflow backfill create ... --max-active-runs 1` | DAG runs its tasks green in order; backfilled days appear in Gold; Gold's order counts still sum to Silver's rows, and manual runs go through `airflow dags trigger` |
| 11 | Serving | `pip install duckdb`; DuckDB `ATTACH 'iceberg' AS uc (TYPE iceberg, ...)` | DuckDB returns Gold rows identical to Spark's |
| 12 | AI | `docker compose up -d --build mlflow`; `python ml/train.py` (Ch14: `make train`); `mlflow.register_model(...)` | Run with the Gold snapshot id in MLflow; registered model loads by name and version |
| 13 | Agents | write `AGENTS.md` and the Chapter 14 `Makefile`; give a coding agent a goal | Agent operates the stack via `make` targets and its result checks out |
| 14 | Deploy & Teardown | `make down` / `make up`; `docker compose down -v` (destructive) | Data survives `down` + `up`; `down -v` is understood and gated |

## Companion code repository (planned)

The plan is a companion repository with one git tag per chapter end-state
(`ch03-storage`, `ch06-ingestion`, …, and `main` for the finished project), so you
could check out any layer and pick up from there. It does not exist yet. Until it
does, the textbook is self-contained: every file you need is written out in full in
the chapter that introduces it, and Chapter 14 shows the final project tree.

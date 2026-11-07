# Chapter 9: Orchestration

This is the conductor. You can run the medallion pipeline by hand, but a real lakehouse runs it on a schedule, recovers from failures, and can re-run history on demand. That's orchestration. You'll wrap the batch medallion (Chapter 7) in an Apache Airflow DAG, give it a schedule and retries, and run a backfill to populate past dates. Airflow becomes the thing that keeps the lakehouse fed without you babysitting it.

Everything you've built so far, you've run by hand: typing commands, watching them finish. That's fine for learning, but no real data platform works that way. In production, pipelines run themselves: at 2 a.m., reliably, retrying transient failures, alerting a human only when something truly needs attention. Orchestration is what turns your hand-run pipeline into infrastructure that operates itself. It's the difference between a science project and a system.

![Progress: Orchestration](../figures/ch09/fig-9.1-progress-orchestration.svg)

**Figure 9.1**. Progress map with **Orchestration** highlighted.

## Learning objectives

By the end of this chapter you'll be able to:

- Explain what orchestration is and why a scheduler beats cron for pipelines.
- Read and reason about a DAG: tasks and their dependencies.
- Give a DAG a schedule and configure retries for resilience.
- Run a backfill to process historical dates.
- Explain what happens when two runs of the same pipeline overlap, and keep it from happening.
- Schedule Iceberg table maintenance (compaction and snapshot expiry) without losing the snapshots you need.

## What orchestration is, and why not just cron?

> **Orchestration** *(canonical term)*: scheduling tasks and wiring them into a dependency-aware pipeline that runs reliably, recovers from failure, and can be re-run over past time windows.

Scheduling alone (plain cron) fires a command at a time. Orchestration adds what production actually needs: dependencies (run Silver only after Bronze succeeds), retries (a transient failure shouldn't need a human), visibility (which runs passed, which failed, why), and backfills (re-run last month because logic changed). Airflow is the industry-standard tool for this.

It's worth dwelling on why cron isn't enough, because "just use cron" is a tempting simplification that falls apart quickly. Say you set three cron jobs: Bronze at 2:00, Silver at 2:30, Gold at 3:00. What happens when Bronze takes 45 minutes one night because there was more data? Silver fires at 2:30 against incomplete Bronze and produces wrong results, and cron has no idea anything went wrong. What happens when Silver fails? Gold runs at 3:00 anyway, on stale data. What happens when you need to see why last Tuesday's run failed? Cron kept no record. Cron knows about time; it knows nothing about dependencies, success, failure, or history. Orchestration is cron plus all the things you actually need to run a pipeline you can trust. The jump from cron to an orchestrator feels like overkill until the first time it saves you, and then you never go back.

## The DAG: tasks and dependencies

> **DAG** *(canonical term)*: a Directed Acyclic Graph, the pipeline's tasks and the dependency edges between them. "Directed" and "acyclic" mean work flows one way and never loops.

In Airflow you express your pipeline as a DAG: each step is a task, and you declare which tasks must finish before others start. Airflow figures out the order, runs independent tasks in parallel, and stops a downstream task if its upstream failed. Our medallion is a natural DAG: `land_raw` then `ingest_bronze` then `build_silver_gold`.

Let's unpack the jargon, because "Directed Acyclic Graph" sounds more intimidating than it is. A graph is just boxes (tasks) connected by arrows (dependencies). Directed means the arrows point one way: Bronze comes before Silver, not the reverse. Acyclic means no loops: you can't have Bronze depend on Gold which depends on Bronze, because then nothing could ever start. That's the whole concept: a set of tasks with one-way "must happen before" arrows and no circular waiting. Any pipeline you can draw as boxes-and-arrows-without-loops is a DAG, and Airflow's job is to run it correctly.

![The medallion as an Airflow DAG](../figures/ch09/fig-9.2-dag.svg)

**Figure 9.2**. The medallion DAG: `land_raw` to `ingest_bronze` to `build_silver_gold`, scheduled daily, each task with retries; a failed task blocks its downstream.

### Under the hood: the DAG triggers work, it isn't the work

A common confusion: the DAG doesn't contain your Spark logic, it triggers it. Each task typically calls the pipeline you already built (for example, runs the SDP pipeline or a Spark job over Connect). Keep heavy transformation in Spark and let Airflow do what it's good at: scheduling, ordering, retrying, and reporting. This separation is why the same pipeline runs identically whether you launch it by hand or Airflow launches it at 2 a.m.

This separation of concerns is a design principle, not just a detail. Airflow is a conductor, not a musician: it tells the orchestra when each section plays, but it doesn't play the instruments. Your Spark/SDP code is the musician that does the actual work. Keeping them separate means you can test your pipeline logic without Airflow, swap the orchestrator without rewriting your pipelines, and reason about each layer independently. A frequent beginner mistake is stuffing heavy data processing directly into Airflow tasks; resist it. Airflow triggers; Spark transforms.

## Build: a scheduled medallion DAG

Airflow is the most involved service in the stack: it is really several cooperating processes (a scheduler, an API server that also serves the UI, a DAG processor that parses your DAG files) plus a metadata database that records every run. Airflow publishes an official `docker-compose.yaml`, but it is built for a distributed CeleryExecutor setup with Redis, workers, and Flower, far more than a laptop needs, and adapting it by hand is where most people get stuck. So instead you'll write a small, complete version yourself, the same way you wrote every other service. It makes four decisions, and each one is worth understanding:

1. **LocalExecutor, not Celery.** With `LocalExecutor`, the scheduler runs tasks as subprocesses inside its own container. No broker, no workers, no Redis. That is plenty for one machine.
2. **A custom image, because the tasks call Spark.** The DAG imports your `lakehouse` package and runs `spark-pipelines`. That CLI is a JVM launcher under the hood (it starts `spark-class`), so the Airflow image needs a Java runtime, plus the Spark Connect client with the pipelines extra. The stock Airflow image has neither.
3. **Your code is mounted, not copied.** `dags/`, `lakehouse/`, and `pipelines/` are mounted into the containers and `PYTHONPATH` points at them, so `from lakehouse.ingest import ingest_orders` resolves to the exact file you edit on your host. Change the ingest and the next run uses the change, no image rebuild.
4. **In-network addresses.** Tasks run inside the Compose network, so they reach Spark at `sc://spark-connect:15002` and the catalog at `unity-catalog:8080`. Because `get_spark()` reads `SPARK_REMOTE` (Chapter 4), the identical ingest function runs from your host and from Airflow, differing only by one environment variable.

**Step 1: the Airflow image.** Create `docker/airflow/Dockerfile`:

```dockerfile
# docker/airflow/Dockerfile
# Airflow plus exactly what the medallion DAG calls: a JDK (spark-pipelines is a
# JVM launcher), the Spark Connect client with the pipelines extra, and the rest
# of the project's Python dependencies. The lakehouse package itself is mounted
# at /opt/lakehouse and put on PYTHONPATH by Compose, so code edits are live.
FROM apache/airflow:3.3.2-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && rm -rf /var/lib/apt/lists/*
# default-java is a symlink that exists on amd64 and arm64 (Apple Silicon) alike.
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "pyspark[connect,pipelines]==4.3.0"
```

Pinning `apache-airflow==3.3.2` in the same `pip install` stops pip from quietly upgrading Airflow while it resolves PySpark's dependencies. `default-jre-headless` and the `default-java` path are chosen so the same file builds on an Intel laptop and an Apple Silicon Mac. (Dockerfile `ENV` lines can't carry a trailing comment, which is why the explanation sits on its own line above it.)

**Step 2: secrets for Airflow.** Airflow needs a database password and a signing secret for the tokens its components exchange. They are secrets, so they go in `.env`, along with your user id so files Airflow writes into mounted folders are owned by you:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
echo "AIRFLOW_DB_PASSWORD=airflow-local" >> .env
echo "AIRFLOW_JWT_SECRET=$(openssl rand -hex 16)" >> .env
```

**Step 3: the Compose services.** Airflow's processes share almost all of their configuration, so define it once with a YAML anchor and reuse it. Put this block at the very top of `docker-compose.yml`, above `services:` (Compose ignores top-level keys that start with `x-`, which is exactly what makes them usable as anchors):

```yaml
x-airflow-common: &airflow-common
  build: ./docker/airflow
  image: open-lakehouse-airflow:3.3.2
  user: "${AIRFLOW_UID:-50000}:0"
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:${AIRFLOW_DB_PASSWORD}@airflow-db/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"   # nothing runs until you unpause it (Chapter 9)
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-apiserver:8080/execution/
    AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_JWT_SECRET}
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS: "true"   # local only: no login screen
    # The lakehouse package, reached the in-network way (Chapter 2's two-address rule):
    PYTHONPATH: /opt/lakehouse
    SPARK_REMOTE: sc://spark-connect:15002
    CATALOG_URI: http://unity-catalog:8080/api/2.1/unity-catalog/iceberg
  volumes:
    - ./dags:/opt/airflow/dags
    - ./lakehouse:/opt/lakehouse/lakehouse
    - ./pipelines:/opt/lakehouse/pipelines
  depends_on:
    airflow-db:
      condition: service_healthy
  networks:
    - lakehouse-network
```

Then add the services under `services:`:

```yaml
  airflow-db:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: ${AIRFLOW_DB_PASSWORD}
      POSTGRES_DB: airflow
    volumes:
      - airflow-db:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 10
    networks:
      - lakehouse-network

  airflow-init:
    <<: *airflow-common
    command: db migrate                     # create/upgrade Airflow's metadata tables, then exit
    restart: "no"

  airflow-apiserver:
    <<: *airflow-common
    command: api-server
    ports:
      - "8085:8080"                         # UI + REST API, at localhost:8085
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler                      # with LocalExecutor, tasks run inside this container
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-dag-processor:
    <<: *airflow-common
    command: dag-processor                  # parses dags/ (a separate process in Airflow 3)
    depends_on:
      airflow-init:
        condition: service_completed_successfully
```

And add `airflow-db:` to the top-level `volumes:` list, so run history survives a restart.

Read the dependency chain, because it encodes the startup order you'd otherwise have to babysit: the database must be healthy (`pg_isready`), then `airflow-init` runs the schema migration and exits, and only after it exits successfully do the long-running processes start. `<<: *airflow-common` pastes the shared block into each service; a service's own `depends_on` replaces the anchor's, which is why the long-running services list `airflow-init` (and inherit the database ordering through it). `SIMPLE_AUTH_MANAGER_ALL_ADMINS` turns off the login screen; it is a local-development convenience you would never ship. `DAGS_ARE_PAUSED_AT_CREATION: "true"` makes every new DAG start paused, so Airflow parses it but runs nothing until you unpause it in Step 6; the section "Two runs at once" below shows what an unpaused new DAG can do to the tables you built by hand.

**Step 4: build and start it.**

```bash
docker compose up -d --build airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor
docker compose ps -a | grep airflow
curl -s http://localhost:8085/api/v2/monitor/health
```

The first build installs a JDK and PySpark into the image, so give it a few minutes. Expected: `airflow-db` is `healthy`, `airflow-init` shows `Exited (0)` (it did its one job), the other three are `Up`, and the health endpoint reports each component:

```json
{"metadatabase":{"status":"healthy"},"scheduler":{"status":"healthy","latest_scheduler_heartbeat":"..."},"triggerer":{"status":null,"latest_triggerer_heartbeat":null},"dag_processor":{"status":"healthy","latest_dag_processor_heartbeat":"..."}}
```

The `triggerer` is `null` because we didn't start one; it only matters for deferrable operators, which this pipeline doesn't use.

**What just happened?** You started Airflow's cooperating processes: a scheduler that decides what runs when (and, with LocalExecutor, runs it), an API server for the UI and REST calls, a DAG processor that parses your DAG files, and a metadata database that records every run. Open `http://localhost:8085` in your browser. The UI is where orchestration stops being abstract: you can see your pipeline as a graph, watch runs succeed or fail, and drill into any task's logs.

**Step 5: write the DAG.** Save this as `dags/lakehouse_medallion_pipeline.py`:

```python
# dags/lakehouse_medallion_pipeline.py
import subprocess
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from lakehouse.generate import generate_orders   # Chapter 6: the stand-in source system
from lakehouse.ingest import ingest_orders       # Chapter 6: the SAME ingest you ran by hand

default_args = {
    "retries": 2,                          # transient failures auto-retry
    "retry_delay": timedelta(minutes=1),
}


def run_day(ctx) -> str:
    """The day this run is responsible for, as YYYY-MM-DD.
    Scheduled and backfill runs carry a logical date (the interval they cover).
    In Airflow 3 a manual trigger has none unless you pass --logical-date, so fall
    back to when the run was requested."""
    when = ctx.get("logical_date") or ctx["dag_run"].run_after
    return when.strftime("%Y-%m-%d")


def land_raw(**ctx):
    # Stand-in for the upstream system delivering that day's export. In production
    # this task would not exist (the file would just arrive), or it would be a
    # sensor that waits for the file.
    generate_orders(run_day(ctx))


def ingest_bronze(**ctx):
    # A backfill run for 2026-01-03 gets "2026-01-03", so the ingest lands exactly
    # that day's partition, using the same function you ran by hand.
    ingest_orders(run_day(ctx))


def build_silver_gold(**_):
    # Run the Chapter 7 SDP project against the in-network Connect server. SDP works out
    # that Gold reads Silver and builds them in that order; Airflow only sequences
    # "ingest, then transform".
    subprocess.run(
        ["spark-pipelines", "run", "--remote", "sc://spark-connect:15002"],
        cwd="/opt/lakehouse/pipelines/medallion", check=True,
    )


with DAG(
    dag_id="lakehouse_medallion_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",                      # run once a day
    catchup=False,                          # don't auto-run all history on deploy
    max_active_runs=1,                      # one refresh of silver/gold at a time
    default_args=default_args,
) as dag:
    t_raw = PythonOperator(task_id="land_raw", python_callable=land_raw)
    t_bronze = PythonOperator(task_id="ingest_bronze", python_callable=ingest_bronze)
    t_silver_gold = PythonOperator(task_id="build_silver_gold", python_callable=build_silver_gold)

    t_raw >> t_bronze >> t_silver_gold      # the dependency edges = the DAG
```

**What just happened?** The last line is the heart of it: `t_raw >> t_bronze >> t_silver_gold` literally draws the arrows of the DAG. Several details are worth slowing down for:

- **The imports are Airflow 3's.** `DAG` comes from `airflow.sdk` and `PythonOperator` from the standard provider. Older tutorials import `airflow.operators.python`; that path belongs to Airflow 2.
- **`run_day()` answers "which day is this run for?"** Scheduled and backfill runs carry a *logical date*, the interval they cover, and that date is what makes retries and backfills correct: the run for January 3 always lands January 3, whenever it actually executes. Airflow 3 changed one thing here: a run you trigger by hand has *no* logical date unless you give it one, so code that reaches for `ctx["ds"]` (or takes `ds` as a parameter) fails on manual runs with `missing 1 required positional argument: 'ds'`. `run_day()` uses the logical date when there is one and falls back to when the run was requested.
- **`land_raw` stands in for the source.** In production the day's export would simply arrive (or a sensor task would wait for it). Here the generator plays that role, so a backfill of a past day has raw data to ingest. Because the generator is seeded by date, the January 3 drop is identical every time it is produced.
- **The tasks are thin.** `ingest_bronze` calls the exact `ingest_orders` you wrote in Chapter 6, not a copy. There is one definition of "land Bronze" in the whole system; fix a bug in `lakehouse/ingest.py` and both your manual runs and the scheduled runs get the fix.
- **Silver and Gold are one task.** `build_silver_gold` runs the whole Chapter 7 SDP project, and SDP works out that Gold reads Silver and builds them in that order. Airflow only has to sequence "ingest, then transform". This is the conductor-and-musician split again: the orchestrator orders the steps it owns and leaves the ordering inside a declarative pipeline to the pipeline.
- **`catchup=False` and `max_active_runs=1`.** Without `catchup=False`, deploying a DAG whose `start_date` is in the past makes Airflow immediately try to run every missed day, a classic surprise that floods a new pipeline with runs. `max_active_runs=1` tells the scheduler to start at most one run of this DAG at a time; a second run it creates waits, queued, until the first finishes. It only governs runs Airflow starts. A `spark-pipelines run` you type in a terminal never passes through Airflow, and the limit can't see it.

Within a few seconds of saving the file, the DAG processor picks it up. Because the Compose file sets `DAGS_ARE_PAUSED_AT_CREATION` to `"true"`, the DAG starts paused: it is listed, but nothing is scheduled yet. Check that the file parsed cleanly:

```bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected: `lakehouse_medallion_pipeline` is listed, and the import-errors command prints `No data found`.

**Step 6: unpause the DAG, trigger a run, and watch the tasks go green in order.** First hand the pipeline to the scheduler. Make sure no `spark-pipelines run` of your own is still going in another terminal, then unpause the DAG (the toggle next to its name in the UI does the same):

```bash
docker compose exec airflow-scheduler airflow dags unpause lakehouse_medallion_pipeline
```

With `catchup=False`, Airflow now schedules exactly one run, for the most recent daily interval. That run is expected; it's the scheduler doing its job. From here on Airflow owns this pipeline (the reason that matters is the next section). Now trigger a run of your own, from the UI's trigger button or from the CLI inside the scheduler container. Give it a logical date so it lands a specific day:

```bash
docker compose exec airflow-scheduler \
    airflow dags trigger lakehouse_medallion_pipeline --logical-date 2026-01-08
```

If the scheduled run is still going, yours waits in `queued` until it finishes: that is `max_active_runs=1` at work.

Expected: `land_raw` runs first, then `ingest_bronze`, then `build_silver_gold`, each starting only after the one before it succeeds. In the UI's graph view the tasks turn green in sequence. You can confirm the same thing without the UI, which is how you'd check it in a script or an alert (in Airflow 3 the DAG id is a positional argument):

```bash
docker compose exec airflow-scheduler airflow dags list-runs lakehouse_medallion_pipeline
```

A `state` of `success` for the run means all three tasks finished in order; `failed` or `running` tells you to open that run's task logs in the UI. The task list for the run, in the order the tasks ran, from a verification run:

```
land_raw           success
ingest_bronze      success
build_silver_gold  success
```

Then confirm the data, not just the green boxes:

```python
from lakehouse.session import get_spark
spark = get_spark()
spark.sql("SELECT order_date, sum(order_count) AS orders FROM iceberg.gold.daily_revenue "
          "GROUP BY order_date ORDER BY order_date").show()
```

Expected: a row for `2026-01-08` has appeared alongside the days you loaded by hand.

**What just happened?** You watched dependency enforcement in action. The transform did not start until the ingest succeeded (and inside it, SDP built Silver before Gold); if the ingest had failed, nothing downstream would have run, sparing you a pile of wrong data. That "don't run downstream if upstream failed" behavior is exactly what cron can't do and what makes an orchestrated pipeline trustworthy.

## Two runs at once: the refresh race

With the DAG unpaused, the medallion has two ways to start: the scheduler, and you typing `spark-pipelines run` in a terminal as you did in Chapter 7. Nothing stops both from happening at the same moment, and when they do, every signal you've learned to read says success.

**What happens.** In a verification run on the course stack, with several days of orders loaded, two `spark-pipelines run` commands of the medallion were started together, which is what a scheduled run and a manual run amount to. Both printed `Run is COMPLETED`. Silver went from 3271 rows to 6542, every order twice. Gold went from 9 rows to 18, and its `order_count` column summed to 9813.

**Why.** Chapter 7 showed that a materialized-view refresh is two commits: a `delete` that empties the table, then an `append` of the recomputed result. Two refreshes at once interleave those commits. Here is Silver's commit log from that run, read from the catalog's Iceberg REST endpoint (the same table metadata `lakehouse/catalog.py` reads), with the commit time, the operation, the rows each commit added and deleted, and the table's total afterwards:

```
commits on silver.orders after snapshot 1175202214321322638:
  22:16:02.637  delete    added=   0 deleted=3271 total=0
  22:16:02.801  delete    added=   0 deleted=   0 total=0
  22:16:04.644  append    added=3271 deleted=   0 total=3271
  22:16:04.782  append    added=3271 deleted=   0 total=6542
```

The first run emptied Silver. The second run emptied a table that was already empty. Then each appended its own result, both computed from the same Bronze, so the second append is a full copy of the first. Iceberg accepted all four commits because each one is valid on its own; it rejects a commit only when it conflicts with another one, two appends never conflict, and nothing tells Iceberg that these four commits are two refreshes. Gold went through the same sequence, and its two refreshes read Silver at different moments, one after Silver's first append (3271 orders) and one after the second (6542), which is where 9813 comes from. In between, for about two seconds, anyone reading Silver saw an empty table.

This isn't hypothetical. It first happened in this course by accident, back when the Compose file let new DAGs start unpaused: the DAG's first scheduled run started while a Chapter 7 run was still going, and Silver went from 1899 rows to 3798. That's why `DAGS_ARE_PAUSED_AT_CREATION` is now `"true"` and Step 6 has you unpause by hand.

**Why `max_active_runs=1` doesn't prevent it.** That setting belongs to the DAG, and the scheduler enforces it on the runs the scheduler creates: scheduled, triggered and backfill runs of this DAG queue behind each other. A `spark-pipelines run` from your terminal goes straight to the Connect server and never passes through Airflow, so the scheduler doesn't know it exists. SDP doesn't stop it either: neither the spec nor the Connect server refuses a second run of a pipeline that is already running.

**The rule: one entry point per pipeline.** Once Airflow owns the pipeline, start every run through Airflow, including the ones you want right now:

```bash
docker compose exec airflow-scheduler \
    airflow dags trigger lakehouse_medallion_pipeline --logical-date 2026-01-08
```

A triggered run queues behind any active one, so it can never overlap another run of the DAG. The same goes for anything else that can start the pipeline. The `make pipeline` target in Chapter 13 calls `spark-pipelines run` directly (and the agent instructions in Chapter 12 carry this rule for that reason), so use it only on a stack where Airflow isn't scheduling this pipeline, or pause the DAG first (`airflow dags pause lakehouse_medallion_pipeline`) and confirm with `airflow dags list-runs lakehouse_medallion_pipeline` that no run is still `running`.

**The check that catches it.** Every task was green and both runs said `COMPLETED`, so the status you get from Airflow and SDP can't catch this. The check from Chapter 7 can: Gold's order counts must sum to Silver's row count, and Silver must hold one row per `order_id`:

```python
from lakehouse.session import get_spark
spark = get_spark()
silver = spark.table("iceberg.silver.orders").count()
ids = spark.sql("SELECT count(DISTINCT order_id) FROM iceberg.silver.orders").first()[0]
gold = spark.sql("SELECT sum(order_count) FROM iceberg.gold.daily_revenue").first()[0]
print(silver, ids, gold)   # all three equal on a healthy run
```

After the race above they were 6542, 3271 and 9813. The Gold sum caught both races described here; the distinct count is the direct test for the doubling, because a race could leave Gold's sum equal to Silver's row count if both Gold refreshes happened to read Silver between its two appends. To repair the tables, run the pipeline once more through Airflow: each refresh empties Silver and Gold and recomputes them from Bronze, so one run on its own puts them back.

## Retries and backfills: the production payoff

**Retries.** With `retries: 2`, a task that hits a transient error (a momentary storage blip, the Connect server restarting) is retried automatically before the run is marked failed. You wake up to a green pipeline, not a 2 a.m. page. This only works safely because your tasks are idempotent (Chapter 6): a retry re-runs the task, and idempotency guarantees that re-running doesn't duplicate data. Retries and idempotency are a team: retries give you automatic recovery, idempotency makes that recovery safe. Neither is much use without the other.

> **Backfill** *(canonical term)*: running a pipeline over past dates to populate or re-populate history.

**Backfill.** Suppose you fixed a Gold aggregation and need last week re-computed, or you are standing up the pipeline and want history. Because each run is tied to a logical date, you can backfill a range and Airflow creates one run per day in that window. In Airflow 3 that's the `backfill create` command:

```bash
docker compose exec airflow-scheduler \
    airflow backfill create --dag-id lakehouse_medallion_pipeline \
    --from-date 2026-01-01 --to-date 2026-01-07 --max-active-runs 1
```

(Airflow 2's `airflow dags backfill` no longer exists. The UI's backfill dialog does the same thing as this command.)

Expected: one run per day, `backfill__2026-01-01...` through `backfill__2026-01-07...`, executing one after another. Each lands its own day's raw drop, overwrites exactly that day's Bronze partition, and refreshes Silver and Gold. Re-running a range you already backfilled needs `--reprocess-behavior completed`, which tells Airflow to redo days that already succeeded; because every task is idempotent, the result is identical.

**Don't drop `--max-active-runs 1`.** It looks like a tuning knob; it is a correctness setting. A backfill has its own concurrency limit, separate from the DAG's `max_active_runs`, and by default it starts many days at once. When that happens here, several runs refresh Silver and Gold simultaneously, each one resetting the table and inserting into it, and the writes interleave. In verification, an unbounded seven-day backfill left Silver at 30744 rows, four times the real count, because overlapping inserts stacked up, with every task green. The orchestrator reported success on corrupted data. Running one day at a time removes the overlap, and the counts come out right. The general lesson outlives this pipeline: parallel runs are only safe for tasks that don't write the same table.

**What just happened?** This is where several earlier chapters' decisions pay a dividend at once. You needed a week of history. Because each day's raw drop is addressable by date and the ingest overwrites only that day's partition (Chapter 6), each run touched exactly its own slice of Bronze. Because each run is idempotent, re-running a day overwrites cleanly instead of duplicating. And because Airflow ties runs to dates, "redo January 1 to 7" is one command. A backfill that would have been a terrifying manual data-surgery operation in a fragile system is routine here. (The streaming job from Chapter 8 can keep running throughout: its rows live in `_source = 'orders-stream'` partitions, which a batch overwrite never touches.)

## Table maintenance: compaction and snapshot expiry

The Chapter 8 stream commits to `bronze.orders_raw` every 10 seconds. Each micro-batch that carries rows writes at least one new Parquet file into the `_source = 'orders-stream'` partition, and each commit adds a manifest, a manifest list, a `metadata.json`, and a snapshot. Nothing in Iceberg merges those files or removes those snapshots on its own. In a verification run, 57 streamed orders left six stream files of a few kilobytes each beside the three batch partitions' files, and the table had 12 snapshots. A stream with rows in every micro-batch commits 8,640 times a day. As the files pile up, every query plans over more manifests and opens more tiny files, and every file a later write replaced (the overwritten Bronze partitions from Chapter 6, for one) stays in the bucket for as long as an old snapshot still lists it.

Table maintenance is four Iceberg procedures, run in this order:

1. **`rewrite_data_files`** bin-packs small files in each partition into fewer, larger ones and commits the result as one `replace` snapshot. The rows don't change; the old files stay in the bucket, because the earlier snapshots still list them.
2. **`rewrite_manifests`** regroups the manifests that every commit adds, so planning reads a few instead of one per commit.
3. **`expire_snapshots`** removes snapshots past the retention window, then deletes every data file, manifest, and manifest list that only the removed snapshots referenced. This is the step that frees storage, which is why it runs after compaction.
4. **`remove_orphan_files`** lists the table's location and deletes files no snapshot references at all, such as the files a write had uploaded when its commit failed or the process writing it died.

These are the same kind of task as the medallion DAG's: Airflow sends a statement over Spark Connect, and the work runs on the Spark cluster. Save this as `dags/lakehouse_table_maintenance.py`:

```python
# dags/lakehouse_table_maintenance.py
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

from lakehouse.config import CATALOG
from lakehouse.session import get_spark

TABLE = "bronze.orders_raw"     # the table the Chapter 8 stream commits to every 10 seconds


def days_ago(days: int) -> str:
    # A TIMESTAMP literal for the procedures' older_than argument. CALL arguments must
    # be constants (current_timestamp() - INTERVAL ... is refused), so compute it here,
    # in UTC like the Connect server's session time zone.
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S'")


def call(sql: str) -> None:
    # Iceberg's maintenance procedures run inside the Connect server, like every
    # other statement; the task only sends the CALL and logs the result row.
    spark = get_spark("table-maintenance")
    for row in spark.sql(sql).collect():
        print(row.asDict())


def compact(**_):
    # Rewrite small data files into fewer, larger ones (target: the table's
    # write.target-file-size-bytes, 512 MB by default). min-input-files=2 lets a
    # partition with only a couple of small files qualify; the default is 5.
    call(f"CALL {CATALOG}.system.rewrite_data_files(table => '{TABLE}', "
         f"options => map('min-input-files', '2'))")


def rewrite_manifests(**_):
    # Every commit adds a manifest; regroup them so planning reads a few, not hundreds.
    call(f"CALL {CATALOG}.system.rewrite_manifests('{TABLE}')")


def expire_snapshots(**ctx):
    # Drop snapshots older than keep_days, but always keep the last retain_last on main,
    # and never a snapshot a tag or branch still points at (a ref is kept until its own
    # RETAIN age passes). Files that only expired snapshots referenced are deleted.
    keep_days, retain_last = ctx["params"]["keep_days"], ctx["params"]["retain_last"]
    call(f"CALL {CATALOG}.system.expire_snapshots(table => '{TABLE}', "
         f"older_than => {days_ago(keep_days)}, retain_last => {retain_last})")


def remove_orphan_files(**_):
    # Delete files under the table's location that no snapshot references (left by
    # failed or killed writes). older_than must be at least 24 hours: a younger file
    # may belong to a commit still in flight. prefix_listing lists the bucket through
    # Iceberg's own S3 client; without it the listing goes through Hadoop, which has
    # no FileSystem for the s3:// paths Unity Catalog gives tables.
    call(f"CALL {CATALOG}.system.remove_orphan_files(table => '{TABLE}', "
         f"older_than => {days_ago(3)}, prefix_listing => true)")


with DAG(
    dag_id="lakehouse_table_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={
        "keep_days": Param(5, type="integer", minimum=0),     # Iceberg's own default age
        "retain_last": Param(10, type="integer", minimum=1),
    },
) as dag:
    t_compact = PythonOperator(task_id="rewrite_data_files", python_callable=compact)
    t_manifests = PythonOperator(task_id="rewrite_manifests", python_callable=rewrite_manifests)
    t_expire = PythonOperator(task_id="expire_snapshots", python_callable=expire_snapshots)
    t_orphans = PythonOperator(task_id="remove_orphan_files", python_callable=remove_orphan_files)

    t_compact >> t_manifests >> t_expire >> t_orphans
```

**What just happened?** The DAG is four thin tasks in a chain, each sending one `CALL` to the Connect server. A few details decide whether it is safe:

- **`retain_last` and `keep_days`.** `expire_snapshots` removes snapshots older than the cutoff, but always keeps the newest `retain_last` snapshots of `main`, and never removes a snapshot a tag or branch points at until that ref's own `RETAIN` age has passed. That last rule is what makes the job safe to point at any table: Chapter 11 tags each model's training snapshot on Gold, so if you add Gold to this job, the tagged snapshots stay. The defaults here (five days, Iceberg's own default age, and the last ten snapshots) are params, so a run can override them.
- **`days_ago()`.** `CALL` arguments must be constants. `older_than => current_timestamp() - INTERVAL 5 DAYS` fails with `requirement failed: number of args and params must match after binding`, so the task computes the cutoff in Python and passes a `TIMESTAMP` literal, in UTC like the Connect server's session time zone (Chapter 7).
- **`min-input-files`.** `rewrite_data_files` only rewrites a partition's files once there are at least 5 of them by default. Lowering it to 2 lets the stream partition qualify sooner; the size target is the table's `write.target-file-size-bytes` (512 MB by default).
- **`remove_orphan_files` refuses a recent cutoff.** With `older_than` under 24 hours it fails with `Cannot remove orphan files with an interval less than 24 hours`, because a file that young may belong to a commit still in flight. The task uses three days, the procedure's own default.
- **`prefix_listing => true` is required on this stack.** Without it, Iceberg lists the table location through Hadoop's file system API, which has no implementation for the `s3://` paths Unity Catalog gives tables, and the call fails with `UnsupportedFileSystemException: No FileSystem for scheme "s3"`. With it, the listing goes through Iceberg's own S3 client, the one that wrote the files.
- **Concurrent writes.** The stream can keep running: a rewrite commit only conflicts with a write that removed the files it rewrote, and appends remove nothing. A batch overwrite of the same partition in between does conflict; the rewrite's commit fails and the task's retry plans it again.

The DAG starts paused, like the medallion DAG. Unpause it and trigger one run with retention overrides, so that snapshots minutes old count as expired (on a real schedule, leave the defaults):

```bash
docker compose exec airflow-scheduler airflow dags unpause lakehouse_table_maintenance
docker compose exec airflow-scheduler \
    airflow dags trigger lakehouse_table_maintenance --conf '{"keep_days": 0, "retain_last": 1}'
```

Unpausing also schedules one run for the latest daily interval, with the default params. Both runs go through `max_active_runs=1` one at a time; the first compacts, and the triggered one expires. Count the table's data files and snapshots before and after, straight from the catalog:

```python
from lakehouse.catalog import table_metadata

meta = table_metadata("bronze", "orders_raw")
current = [s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"]][0]
print("data files:", current["summary"]["total-data-files"], " snapshots:", len(meta["snapshots"]))
```

From the verification run:

```
before   data files: 9  snapshots: 12
after    data files: 4  snapshots: 1
```

The six stream files became one 7.7 KB file (the three batch partitions already had one file each), `rewrite_manifests` folded ten manifests into one, and `expire_snapshots` left only the current snapshot and deleted the files that only the expired snapshots listed: the bucket now holds exactly the table's four Parquet files. `remove_orphan_files` returned no rows, because every file in the bucket was minutes old, inside its three-day cutoff. The rows themselves didn't change; a `replace` commit adds and removes files, never rows.

## Troubleshooting

- **The DAG doesn't appear in the UI.** Airflow didn't parse the file. Check for Python syntax errors in the DAG file and confirm it's in the mounted `dags/` folder Airflow scans; the UI often shows an import-error banner.
- **Tasks can't reach Spark.** They're using `localhost` instead of the in-network name. Airflow tasks run in the Airflow container, so Spark Connect is `spark-connect:15002`, not `localhost:15002`.
- **On deploy, Airflow runs a flood of past dates.** You forgot `catchup=False` (or set it `True`) with a past `start_date`. Set `catchup=False` unless you genuinely want automatic historical runs.
- **A task fails but downstream runs anyway.** You wired the dependencies wrong. Confirm the `>>` chain reflects the real order; a missing edge lets tasks run independently.
- **Retries make duplicate data.** Your task isn't idempotent. Revisit Chapter 6; retries are only safe on idempotent tasks.
- **Backfill does nothing or errors.** Check the date range and that the DAG's `start_date` precedes it; Airflow won't run logical dates before `start_date`. If the days already ran successfully, add `--reprocess-behavior completed` to redo them.
- **`airflow dags backfill`: invalid choice.** That is the Airflow 2 command. Airflow 3 uses `airflow backfill create --dag-id ... --from-date ... --to-date ...`.
- **Every task is green (or the run said `COMPLETED`) but Silver has a multiple of the rows it should, the same `order_id` appears more than once, and Gold's `order_count` no longer sums to Silver's row count.** Two refreshes overlapped and both appended (see "Two runs at once"). Either a backfill ran without `--max-active-runs 1`, or a `spark-pipelines run` (or `make pipeline`) started outside Airflow while a DAG run was refreshing. Re-run once through Airflow (`airflow dags trigger`), or re-run a backfill range with `--max-active-runs 1 --reprocess-behavior completed`, and from then on start every run through Airflow.
- **The DAG is listed but never runs, and the trigger button's runs stay `queued`.** The DAG is paused, which is how every new DAG starts in this stack. Unpause it with `airflow dags unpause lakehouse_medallion_pipeline` (Step 6).
- **A manually triggered run fails with `missing 1 required positional argument: 'ds'` or `KeyError: 'ds'`.** In Airflow 3 a manual run has no logical date. Use the `run_day()` helper from the DAG above, or trigger with `--logical-date YYYY-MM-DD`.
- **`build_silver_gold` fails with `java: not found` or `JAVA_HOME is not set`.** The scheduler is running the stock Airflow image. `spark-pipelines` needs the JDK baked into `docker/airflow/Dockerfile`; rebuild with `docker compose up -d --build airflow-scheduler`.
- **`ModuleNotFoundError: No module named 'lakehouse'` in a task.** The mounts or `PYTHONPATH` are missing from the service. Every Airflow service inherits them from `x-airflow-common`; check that the service has `<<: *airflow-common`.
- **`airflow-init` exits non-zero.** Usually the database password in `.env` changed after `airflow-db` first initialized its volume. Postgres only reads `POSTGRES_PASSWORD` the first time; put the original back (or, if the Airflow history is disposable, remove just the `airflow-db` volume).

## Checkpoint

- `docker compose ps` shows the Airflow services healthy; the UI opens on 8085.
- The `lakehouse_medallion_pipeline` DAG shows `land_raw` then `ingest_bronze` then `build_silver_gold`, with no import errors.
- You unpaused the DAG yourself, and a triggered run turns the tasks green in dependency order.
- After every run, Silver's row count, its distinct `order_id` count and Gold's `order_count` sum are equal, and you can explain why a manual `spark-pipelines run` during a DAG run breaks that and `max_active_runs=1` doesn't stop it.
- `retries` are configured, and you ran a backfill over a date range with `--max-active-runs 1`, then confirmed every backfilled day appears in Gold.
- You can explain why cron is insufficient and what "DAG" means.

## Try it yourself

1. **Force a failure.** Make `build_silver_gold` raise an error on purpose (for example, point its `cwd` at a directory that doesn't exist), trigger the DAG, and confirm the run stops there. Then fix it and re-run. That's dependency enforcement protecting you.
2. **Watch retries.** Make a task fail intermittently (fail the first attempt), and watch Airflow retry it per your `retries` setting before giving up.
3. **Backfill and verify.** Backfill a three-day range and confirm each day's Gold was re-derived. Check that running the backfill twice doesn't duplicate data (idempotency plus orchestration together).
4. **Make the check a task.** Add a fourth task after `build_silver_gold` that runs the three-number check from "Two runs at once" and raises if the numbers differ, so a corrupted refresh turns the run red instead of green.
5. **Draw your own DAG.** Sketch the DAG you'd build if Gold had two independent downstream tables (say, a dashboard export and a model-training trigger). Which tasks could run in parallel?

## Check your understanding

- Give three things orchestration provides that plain cron does not.
- What do "directed" and "acyclic" mean in DAG, and why does acyclic matter?
- Explain "Airflow is a conductor, not a musician." What belongs in the DAG versus in Spark?
- Why are retries and idempotency described as a team? What goes wrong if you have one without the other?
- Two refreshes of the medallion overlap. Walk through the four commits on Silver and say why each run reported success.

## Recap and what's next

- Orchestration schedules, orders, retries, and reports on pipeline tasks, everything cron can't.
- A DAG expresses tasks and dependencies; Airflow derives order and parallelism.
- The DAG triggers your Spark/SDP work; it doesn't replace it. Conductor, not musician.
- Retries give resilience; backfills re-run history, made safe by idempotency.
- One entry point per pipeline: once Airflow owns it, start every run through Airflow, and check after each run that Gold's order counts sum to Silver's rows.
- Streaming leaves small files and many snapshots; a maintenance DAG compacts them and expires old snapshots, and tags keep the snapshots you must not lose.
- **Next, Chapter 10, Serving:** expose the finished Gold tables to other engines through the one open catalog.

![Progress: Orchestration complete, Serving next](../figures/ch09/fig-9.3-progress-orchestration-done.svg)

**Figure 9.3**. Progress map with **Orchestration done** and **Serving** next.

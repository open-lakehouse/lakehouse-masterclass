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

## What orchestration is, and why not just cron?

> **Orchestration** *(canonical term)*: scheduling tasks and wiring them into a dependency-aware pipeline that runs reliably, recovers from failure, and can be re-run over past time windows.

Scheduling alone (plain cron) fires a command at a time. Orchestration adds what production actually needs: dependencies (run Silver only after Bronze succeeds), retries (a transient failure shouldn't need a human), visibility (which runs passed, which failed, why), and backfills (re-run last month because logic changed). Airflow is the industry-standard tool for this.

It's worth dwelling on why cron isn't enough, because "just use cron" is a tempting simplification that falls apart quickly. Say you set three cron jobs: Bronze at 2:00, Silver at 2:30, Gold at 3:00. What happens when Bronze takes 45 minutes one night because there was more data? Silver fires at 2:30 against incomplete Bronze and produces wrong results, and cron has no idea anything went wrong. What happens when Silver fails? Gold runs at 3:00 anyway, on stale data. What happens when you need to see why last Tuesday's run failed? Cron kept no record. Cron knows about time; it knows nothing about dependencies, success, failure, or history. Orchestration is cron plus all the things you actually need to run a pipeline you can trust. The jump from cron to an orchestrator feels like overkill until the first time it saves you, and then you never go back.

## The DAG: tasks and dependencies

> **DAG** *(canonical term)*: a Directed Acyclic Graph, the pipeline's tasks and the dependency edges between them. "Directed" and "acyclic" mean work flows one way and never loops.

In Airflow you express your pipeline as a DAG: each step is a task, and you declare which tasks must finish before others start. Airflow figures out the order, runs independent tasks in parallel, and stops a downstream task if its upstream failed. Our medallion is a natural DAG: `land_raw` then `ingest_bronze` then `build_silver` then `build_gold`.

Let's unpack the jargon, because "Directed Acyclic Graph" sounds more intimidating than it is. A graph is just boxes (tasks) connected by arrows (dependencies). Directed means the arrows point one way: Bronze comes before Silver, not the reverse. Acyclic means no loops: you can't have Bronze depend on Gold which depends on Bronze, because then nothing could ever start. That's the whole concept: a set of tasks with one-way "must happen before" arrows and no circular waiting. Any pipeline you can draw as boxes-and-arrows-without-loops is a DAG, and Airflow's job is to run it correctly.

![The medallion as an Airflow DAG](../figures/ch09/fig-9.2-dag.svg)

**Figure 9.2**. The medallion DAG: `ingest_bronze` to `build_silver` to `build_gold`, scheduled daily, each task with retries; a failed task blocks its downstream.

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
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "pyspark[connect,pipelines]==4.1.0"
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
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"
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

Read the dependency chain, because it encodes the startup order you'd otherwise have to babysit: the database must be healthy (`pg_isready`), then `airflow-init` runs the schema migration and exits, and only after it exits successfully do the long-running processes start. `<<: *airflow-common` pastes the shared block into each service; a service's own `depends_on` replaces the anchor's, which is why the long-running services list `airflow-init` (and inherit the database ordering through it). `SIMPLE_AUTH_MANAGER_ALL_ADMINS` turns off the login screen; it is a local-development convenience you would never ship.

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


def refresh(dataset: str):
    """Refresh one dataset of the Chapter 7 SDP project on the in-network Connect server."""
    subprocess.run(
        ["spark-pipelines", "run", "--remote", "sc://spark-connect:15002", "--refresh", dataset],
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
    t_silver = PythonOperator(task_id="build_silver", python_callable=refresh,
                              op_kwargs={"dataset": "silver.orders"})
    t_gold = PythonOperator(task_id="build_gold", python_callable=refresh,
                            op_kwargs={"dataset": "gold.daily_revenue"})

    t_raw >> t_bronze >> t_silver >> t_gold      # the dependency edges = the DAG
```

**What just happened?** The last line is the heart of it: `t_raw >> t_bronze >> t_silver >> t_gold` literally draws the arrows of the DAG. Several details are worth slowing down for:

- **The imports are Airflow 3's.** `DAG` comes from `airflow.sdk` and `PythonOperator` from the standard provider. Older tutorials import `airflow.operators.python`; that path belongs to Airflow 2.
- **`run_day()` answers "which day is this run for?"** Scheduled and backfill runs carry a *logical date*, the interval they cover, and that date is what makes retries and backfills correct: the run for January 3 always lands January 3, whenever it actually executes. Airflow 3 changed one thing here: a run you trigger by hand has *no* logical date unless you give it one, so code that reaches for `ctx["ds"]` (or takes `ds` as a parameter) fails on manual runs with `missing 1 required positional argument: 'ds'`. `run_day()` uses the logical date when there is one and falls back to when the run was requested.
- **`land_raw` stands in for the source.** In production the day's export would simply arrive (or a sensor task would wait for it). Here the generator plays that role, so a backfill of a past day has raw data to ingest. Because the generator is seeded by date, the January 3 drop is identical every time it is produced.
- **The tasks are thin.** `ingest_bronze` calls the exact `ingest_orders` you wrote in Chapter 6, not a copy. There is one definition of "land Bronze" in the whole system; fix a bug in `lakehouse/ingest.py` and both your manual runs and the scheduled runs get the fix.
- **Silver and Gold are two tasks.** Chapter 7 explains why: with open-source SDP 4.1 on Iceberg, refreshing a materialized view and another one that reads it in the same run leaves the downstream one empty, so Silver and Gold are refreshed as separate steps. Sequencing steps is exactly the orchestrator's job, and splitting them gives you per-step retries and logs for free.
- **`catchup=False` and `max_active_runs=1`.** Without `catchup=False`, deploying a DAG whose `start_date` is in the past makes Airflow immediately try to run every missed day, a classic surprise that floods a new pipeline with runs. `max_active_runs=1` keeps two scheduled runs from refreshing Silver and Gold at the same time.

Within a few seconds of saving the file, the DAG processor picks it up. Because the Compose file sets `DAGS_ARE_PAUSED_AT_CREATION` to `false`, the DAG starts unpaused, and with `catchup=False` Airflow schedules exactly one run, for the most recent daily interval. That first run is expected; it's the scheduler doing its job. Check that the file parsed cleanly:

```bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected: `lakehouse_medallion_pipeline` is listed, and the import-errors command prints `No data found`.

**Step 6: trigger a run and watch the tasks go green in order.** From the UI's trigger button, or from the CLI inside the scheduler container. Give it a logical date so it lands a specific day:

```bash
docker compose exec airflow-scheduler \
    airflow dags trigger lakehouse_medallion_pipeline --logical-date 2026-01-08
```

Expected: `land_raw` runs first, then `ingest_bronze`, then `build_silver`, then `build_gold`, each starting only after the one before it succeeds. In the UI's graph view the tasks turn green in sequence. You can confirm the same thing without the UI, which is how you'd check it in a script or an alert (in Airflow 3 the DAG id is a positional argument):

```bash
docker compose exec airflow-scheduler airflow dags list-runs lakehouse_medallion_pipeline
```

A `state` of `success` for the run means all four tasks finished in order; `failed` or `running` tells you to open that run's task logs in the UI. The task timeline from a verification run looks like this, each task starting seconds after the previous one finished:

```
land_raw       success  12:22:35
ingest_bronze  success  12:22:39
build_silver   success  12:22:42
build_gold     success  12:22:48
```

Then confirm the data, not just the green boxes:

```python
from lakehouse.session import get_spark
spark = get_spark()
spark.sql("SELECT order_date, sum(order_count) AS orders FROM iceberg.gold.daily_revenue "
          "GROUP BY order_date ORDER BY order_date").show()
```

Expected: a row for `2026-01-08` has appeared alongside the days you loaded by hand.

**What just happened?** You watched dependency enforcement in action. Gold did not refresh until Silver had, and Silver did not refresh until the ingest succeeded; if the ingest had failed, nothing downstream would have run, sparing you a pile of wrong data. That "don't run downstream if upstream failed" behavior is exactly what cron can't do and what makes an orchestrated pipeline trustworthy.

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

**Don't drop `--max-active-runs 1`.** It looks like a tuning knob; it is a correctness setting. A backfill has its own concurrency limit, separate from the DAG's `max_active_runs`, and by default it starts many days at once. When that happens here, several runs refresh Silver and Gold simultaneously, each one resetting the table and inserting into it, and the writes interleave. In verification, an unbounded seven-day backfill left Silver at 30744 rows (four times the real count, because overlapping inserts stacked up) and Gold empty, with every task green. The orchestrator reported success on corrupted data. Running one day at a time removes the overlap, and the counts come out right. The general lesson outlives this pipeline: parallel runs are only safe for tasks that don't write the same table.

**What just happened?** This is where several earlier chapters' decisions pay a dividend at once. You needed a week of history. Because each day's raw drop is addressable by date and the ingest overwrites only that day's partition (Chapter 6), each run touched exactly its own slice of Bronze. Because each run is idempotent, re-running a day overwrites cleanly instead of duplicating. And because Airflow ties runs to dates, "redo January 1 to 7" is one command. A backfill that would have been a terrifying manual data-surgery operation in a fragile system is routine here. (The streaming job from Chapter 8 can keep running throughout: its rows live in `_source = 'orders-stream'` partitions, which a batch overwrite never touches.)

## Troubleshooting

- **The DAG doesn't appear in the UI.** Airflow didn't parse the file. Check for Python syntax errors in the DAG file and confirm it's in the mounted `dags/` folder Airflow scans; the UI often shows an import-error banner.
- **Tasks can't reach Spark.** They're using `localhost` instead of the in-network name. Airflow tasks run in the Airflow container, so Spark Connect is `spark-connect:15002`, not `localhost:15002`.
- **On deploy, Airflow runs a flood of past dates.** You forgot `catchup=False` (or set it `True`) with a past `start_date`. Set `catchup=False` unless you genuinely want automatic historical runs.
- **A task fails but downstream runs anyway.** You wired the dependencies wrong. Confirm the `>>` chain reflects the real order; a missing edge lets tasks run independently.
- **Retries make duplicate data.** Your task isn't idempotent. Revisit Chapter 6; retries are only safe on idempotent tasks.
- **Backfill does nothing or errors.** Check the date range and that the DAG's `start_date` precedes it; Airflow won't run logical dates before `start_date`. If the days already ran successfully, add `--reprocess-behavior completed` to redo them.
- **`airflow dags backfill`: invalid choice.** That is the Airflow 2 command. Airflow 3 uses `airflow backfill create --dag-id ... --from-date ... --to-date ...`.
- **Every task is green but Silver has several times too many rows and Gold is empty.** Runs overlapped. A backfill without `--max-active-runs 1` refreshes Silver and Gold from several runs at once. Re-run the range with `--max-active-runs 1 --reprocess-behavior completed`.
- **A manually triggered run fails with `missing 1 required positional argument: 'ds'` or `KeyError: 'ds'`.** In Airflow 3 a manual run has no logical date. Use the `run_day()` helper from the DAG above, or trigger with `--logical-date YYYY-MM-DD`.
- **`build_silver` fails with `java: not found` or `JAVA_HOME is not set`.** The scheduler is running the stock Airflow image. `spark-pipelines` needs the JDK baked into `docker/airflow/Dockerfile`; rebuild with `docker compose up -d --build airflow-scheduler`.
- **`ModuleNotFoundError: No module named 'lakehouse'` in a task.** The mounts or `PYTHONPATH` are missing from the service. Every Airflow service inherits them from `x-airflow-common`; check that the service has `<<: *airflow-common`.
- **`airflow-init` exits non-zero.** Usually the database password in `.env` changed after `airflow-db` first initialized its volume. Postgres only reads `POSTGRES_PASSWORD` the first time; put the original back (or, if the Airflow history is disposable, remove just the `airflow-db` volume).

## Checkpoint

- `docker compose ps` shows the Airflow services healthy; the UI opens on 8085.
- The `lakehouse_medallion_pipeline` DAG shows `land_raw` then `ingest_bronze` then `build_silver` then `build_gold`, with no import errors.
- A triggered run turns the tasks green in dependency order.
- `retries` are configured, and you ran a backfill over a date range with `--max-active-runs 1`, then confirmed every backfilled day appears in Gold.
- You can explain why cron is insufficient and what "DAG" means.

## Try it yourself

1. **Force a failure.** Make `build_silver` raise an error on purpose (for example, point `refresh` at a dataset name that doesn't exist), trigger the DAG, and confirm the run stops there. Then fix it and re-run. That's dependency enforcement protecting you.
2. **Watch retries.** Make a task fail intermittently (fail the first attempt), and watch Airflow retry it per your `retries` setting before giving up.
3. **Backfill and verify.** Backfill a three-day range and confirm each day's Gold was re-derived. Check that running the backfill twice doesn't duplicate data (idempotency plus orchestration together).
4. **Draw your own DAG.** Sketch the DAG you'd build if Gold had two independent downstream tables (say, a dashboard export and a model-training trigger). Which tasks could run in parallel?

## Check your understanding

- Give three things orchestration provides that plain cron does not.
- What do "directed" and "acyclic" mean in DAG, and why does acyclic matter?
- Explain "Airflow is a conductor, not a musician." What belongs in the DAG versus in Spark?
- Why are retries and idempotency described as a team? What goes wrong if you have one without the other?

## Recap and what's next

- Orchestration schedules, orders, retries, and reports on pipeline tasks, everything cron can't.
- A DAG expresses tasks and dependencies; Airflow derives order and parallelism.
- The DAG triggers your Spark/SDP work; it doesn't replace it. Conductor, not musician.
- Retries give resilience; backfills re-run history, made safe by idempotency.
- **Next, Chapter 10, Serving:** expose the finished Gold tables to other engines through the one open catalog.

![Progress: Orchestration complete, Serving next](../figures/ch09/fig-9.3-progress-orchestration-done.svg)

**Figure 9.3**. Progress map with **Orchestration done** and **Serving** next.

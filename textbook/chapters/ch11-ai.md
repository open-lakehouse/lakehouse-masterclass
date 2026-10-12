# Chapter 11: AI

This is the lakehouse feeding intelligence. Your Gold tables are clean, served, and governed, exactly the shape a model wants. In this chapter you read a Gold table as training data, train a simple model, and record the run with MLflow: experiment tracking for params and metrics, and a model registry for versioned, promotable models. The point isn't the model, it's the pattern: the same open tables that serve BI also serve AI, from one governed source.

Let's set expectations honestly up front: this is not a machine-learning course, and the model you train will be almost comically simple. That's deliberate. The lesson here isn't "how to build a good model," it's "how the lakehouse is the right foundation for AI, and how you track model work with the same discipline you brought to your data." The ML world is drowning in un-reproducible experiments and mystery models; the lakehouse plus MLflow is how you bring order to that chaos. Focus on the workflow, not the algorithm.

![Progress: AI](../figures/ch11/fig-11.1-progress-ai.svg)

**Figure 11.1**. Progress map with **AI** highlighted.

## Learning objectives

By the end of this chapter you'll be able to:

- Read a Gold table as training data for a model.
- Use MLflow experiment tracking to log params, metrics, and artifacts.
- Register a trained model in the MLflow model registry and version it.
- Explain why a lakehouse is a natural foundation for AI workloads.

## Why the lakehouse is a good home for AI

Models are only as good as their data, and the lakehouse already solved the hard data problems: Gold tables are clean and conformed (Chapter 7), versioned via snapshots (Chapter 5) so you can reproduce exactly which data trained a model, served openly (Chapter 10) so training reads the same governed tables as everything else, and refreshed on a schedule (Chapter 9). "Train on Gold" gives you reproducibility and lineage for free: you can always answer "what data produced this model?"

That last point is worth dwelling on, because it's a genuine pain in real ML work. Ask a typical ML team "what exact data did this production model train on?" and the honest answer is often a shrug: a CSV someone exported months ago, since overwritten, from a query nobody saved. When something goes wrong, you can't reproduce the training set, so you can't diagnose the model. The lakehouse dissolves this problem: your training data is a versioned Iceberg table. You can point at the exact snapshot, time-travel back to it (Chapter 5), and reconstruct the precise rows that trained any model. Reproducible data is the foundation of trustworthy ML, and you already have it.

> **Training data** *(canonical term)*: the historical, feature-shaped data a model learns from, here rows from a Gold table.

![Gold feeds the model; MLflow tracks it](../figures/ch11/fig-11.2-ai-flow.svg)

**Figure 11.2**. Gold table to training data to model; every run's params, metrics, and the model artifact are logged to MLflow and versioned in the registry.

## What MLflow gives you

> **Experiment tracking** *(canonical term)*: recording each model run's parameters, metrics, and artifacts so runs are comparable and reproducible.

> **Model registry** *(canonical term)*: a versioned store of trained models, promoted by pointing named aliases (for example, `staging`, then `production`) at specific versions.

Without tracking, model development is a mess of notebooks and "which version was that?" MLflow fixes both ends: every training run is logged (what params, what score, what artifact), and the good models get registered under a name with versions you can promote or roll back, the same disciplined versioning Iceberg gave your data, now for your models.

Picture the world without MLflow, because most people have lived it. You train a model in a notebook, tweak a parameter, train again, tweak, train again, twenty times. Which run was best? You're not sure; the numbers scrolled off the screen. Can you recreate the good one? Not really. Which model file is deployed? The one called `model_final_v2_REAL.pkl`, probably. This is the actual state of a lot of ML work, and it's a reproducibility disaster. MLflow replaces it with a system: every run is recorded with its params and metrics so you can sort by score and find the best; every model is versioned in the registry so "what's in production" is a fact, not a guess. It brings the same rigor to models that Iceberg brought to your data.

### Under the hood: reproducibility from snapshots

Because you train on an Iceberg Gold table, you can log the table's snapshot id as a run parameter. That means a year later you can reconstruct the exact training set by time-traveling to that snapshot (Chapter 5). Data versioning plus model versioning equals end-to-end reproducibility, something classic ML setups struggle to guarantee. This is the single most powerful idea in the chapter: when your data lives in a versioned table and your models live in a versioned registry, the entire chain from raw data to deployed model is reproducible. That's rare, and it's a direct consequence of building on a lakehouse.

## Build: train, track, register

**Step 1: add MLflow to your Compose file.** The tracking server records every run and stores model artifacts. Its artifact store is your SeaweedFS bucket (in-network address `seaweedfs:8333`), and its run metadata goes in a simple SQLite file (Postgres in production). One wrinkle first: the official MLflow image ships without `boto3`, the AWS SDK for Python, which the server needs to write artifacts to S3-compatible storage. Without it, the server starts fine and then fails the first time a model is logged. So build a two-line image on top of it. Create `docker/mlflow/Dockerfile`:

```dockerfile
# docker/mlflow/Dockerfile
# The official image ships without boto3, which the server needs to store
# artifacts in S3-compatible storage (our SeaweedFS bucket).
FROM ghcr.io/mlflow/mlflow:v3.1.0
RUN pip install --no-cache-dir boto3
```

Then add the service:

```yaml
  mlflow:
    build: ./docker/mlflow
    image: open-lakehouse-mlflow:3.1.0
    depends_on:
      - seaweedfs
    environment:
      MLFLOW_S3_ENDPOINT_URL: http://seaweedfs:8333     # artifacts live in SeaweedFS
      AWS_ACCESS_KEY_ID: ${S3_ACCESS_KEY}
      AWS_SECRET_ACCESS_KEY: ${S3_SECRET_KEY}
    command: >
      mlflow server --host 0.0.0.0 --port 5000
      --backend-store-uri sqlite:////mlflow/mlflow.db
      --artifacts-destination s3://lakehouse/mlflow-artifacts
      --serve-artifacts
    volumes:
      - mlflow-db:/mlflow          # run + registry metadata; without it, history resets on recreate
    ports:
      - "5000:5000"
    networks:
      - lakehouse-network
```

and add `mlflow-db:` to the top-level `volumes:` list.

Two lines deserve a closer look. `--serve-artifacts` makes the tracking server a proxy for artifacts: your training script uploads the model to `http://localhost:5000`, and the *server* writes it to SeaweedFS. So the S3 credentials live in exactly one place, the server's environment, and your laptop never needs them. And the `mlflow-db` volume is the same lesson as Unity Catalog's database in Chapter 5: the SQLite file holds every run and every registered model version, and without a volume it lives inside the container, so the next `docker compose up` that recreates the container would quietly erase your experiment history.

Bring it up and check it:

```bash
docker compose up -d --build mlflow
docker compose ps mlflow
curl -s http://localhost:5000/health      # -> OK
```

**What just happened?** You started the MLflow tracking server, the service that records every training run and stores registered models. The UI on `http://localhost:5000` is where model work becomes visible and comparable, the same way the Airflow UI made orchestration visible. Keep it open as you train; watching runs appear is how the tracking concept becomes concrete.

**Step 2: read Gold as training data, then train and log with MLflow.** Install the client libraries into your project environment (pin MLflow to the server's version):

```bash
pip install mlflow==3.1.0 scikit-learn
```

Put the training code in `ml/train.py` (it lives in the repo like everything else, not in a scratch notebook). It reuses the project's `get_spark()` factory, so it reaches Spark the same way every other module does, and it reaches MLflow at `http://localhost:5000`. The model predicts a row's order count from its revenue: trivial on purpose; the workflow is the lesson.

```python
# ml/train.py  --  python ml/train.py [feature ...]
import os
import sys

import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression

from lakehouse.config import CATALOG
from lakehouse.session import get_spark

GOLD = f"{CATALOG}.gold.daily_revenue"
MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")


def load_training_data(spark):
    """Pin the read to one snapshot and return (snapshot_id, pandas DataFrame).
    Reading *at* the snapshot we log, rather than "whatever is current", is what
    makes the logged lineage true even if Gold is refreshed mid-run."""
    snap = spark.sql(f"SHOW TBLPROPERTIES {GOLD} ('current-snapshot-id')").collect()[0]["value"]
    pdf = (spark.read.option("versionAsOf", snap).table(GOLD)
                .selectExpr("order_date", "status",
                            "CAST(revenue AS DOUBLE) AS revenue",   # DECIMAL -> float for sklearn
                            "order_count")
                .toPandas())
    return snap, pdf


def train(features=("revenue",)):
    spark = get_spark("train-order-count")
    snap, pdf = load_training_data(spark)
    pdf["is_cancelled"] = (pdf["status"] == "cancelled").astype(int)   # a second candidate feature
    X, y = pdf[list(features)], pdf["order_count"]

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("orders-baseline")
    with mlflow.start_run() as run:
        model = LinearRegression().fit(X, y)
        mlflow.log_param("gold_table", GOLD)
        mlflow.log_param("gold_snapshot_id", snap)          # data lineage
        mlflow.log_param("features", ",".join(features))
        mlflow.log_param("training_rows", len(pdf))
        mlflow.log_metric("r2", model.score(X, y))          # a metric to compare runs
        info = mlflow.sklearn.log_model(model, name="model", input_example=X.head(3))
    print(f"run {run.info.run_id}: r2={model.score(X, y):.4f} on {len(pdf)} rows, snapshot {snap}")
    print(f"model uri: {info.model_uri}")
    return info.model_uri


if __name__ == "__main__":
    train(tuple(sys.argv[1:]) or ("revenue",))
```

Run it:

```bash
python ml/train.py
```

Expected: MLflow prints links to the run and the experiment, then the script's own summary. From a verification run with the Chapter 9 backfill loaded (more days means more Gold rows; with only one day loaded you'll have far fewer):

```
run f384b1c2...: r2=0.9984 on 27 rows, snapshot 4778004762130462748
model uri: models:/m-4d6e08c2...
```

In the UI, a new run appears under the `orders-baseline` experiment, showing the `r2` metric, the params (including `gold_snapshot_id` and `training_rows`), and the saved model with its files (`MLmodel`, `model.pkl`, the environment files, an input example). Those files are real objects in your bucket, under `s3://lakehouse/mlflow-artifacts/`; list them with the S3 CLI from Chapter 3 if you want to see them.

**What just happened?** Look at `load_training_data`, because it is the reproducibility idea from the last section done carefully. It asks Iceberg for the table's current snapshot id, then reads the table *at that snapshot* (`versionAsOf`) and logs the same id on the run. The order matters. If you logged "the latest snapshot" and separately read "the table", a Gold refresh landing between the two calls (your Airflow DAG runs daily, after all) would leave the run claiming data it never trained on. Pinning the read to the logged snapshot makes the lineage true by construction. A year from now, "what data trained this model?" has an exact answer, and `spark.read.option("versionAsOf", <that id>)` gets you the identical rows.

Three smaller details:

- **Why `SHOW TBLPROPERTIES`, not the `.snapshots` metadata table?** Iceberg's metadata tables are the usual way to ask for snapshot ids, but with Unity Catalog 0.7 they fail through the REST catalog (Chapter 5 explains why). Iceberg also publishes the current snapshot id as a table property, which works everywhere.
- **The `CAST(revenue AS DOUBLE)`.** Silver made money an exact `DECIMAL` (Chapter 7). That's right for revenue reports, but scikit-learn wants plain floats, so the cast happens at the edge, where the data enters the model.
- **`log_model(..., name="model", input_example=...)`.** MLflow 3 treats a logged model as a first-class object with its own id and URI (`models:/m-...`), which the script returns. The input example records what a valid input looks like, so anyone loading the model later knows its expected columns.

**Step 3: register the model to version and promote it.** Registering gives a logged model a name and a version number:

```python
import mlflow
import sys
sys.path.insert(0, "ml")
from train import train

mlflow.set_tracking_uri("http://localhost:5000")
model_uri = train()                                   # train and log a fresh run
result = mlflow.register_model(model_uri, "orders-order-count")
print("registered version", result.version)
```

Expected: `Successfully registered model 'orders-order-count'` and `Created version '1'` (a higher number if you register again). The model shows up in the MLflow Models tab as a named, versioned entry.

You'll see older tutorials register with `runs:/<run_id>/model`. MLflow 3 still accepts that, but only by falling back with a warning (`Run ... has no artifacts at artifact path 'model', registering model based on models:/m-... instead`), because models are no longer stored under the run. Registering the `model_uri` that `log_model` returned is the direct path.

**What just happened?** You promoted a run's model into the registry, a named, versioned home for models you might actually use. This is the difference between "a model file somewhere" and "version 3 of `orders-order-count`, which we can promote to production or roll back." The registry is to models what the catalog is to tables: the one authoritative, versioned source of truth.

**Step 3b: load the registered model back and use it.** A registered model is only worth something if you can pull it out and run it. Fetch the exact version you just registered and predict:

```python
import mlflow.sklearn
import pandas as pd

model = mlflow.sklearn.load_model(f"models:/orders-order-count/{result.version}")
print(model.predict(pd.DataFrame({"revenue": [40000.0]})))
```

Expected: a predicted `order_count` for a Gold row with $40,000 of revenue. From the verification run above it was about `[161.8]`. That makes sense once you remember each Gold row is one *status* on one day, not a whole day: a cancelled-orders row with $40,000 of revenue had roughly 160 orders. The number itself is uninteresting; what matters is the round-trip. You loaded a model *by name and version* from the registry and ran it, with no file paths, no "which .pkl was it," no guesswork. That is exactly how a serving job or batch-scoring pipeline would load it in production: `models:/orders-order-count/<version>`. (Passing a DataFrame with the `revenue` column, rather than a bare list, matches the input example the model was logged with and avoids a feature-name warning.)

**Step 4: compare runs.** Train again with a second feature and compare:

```bash
python ml/train.py revenue is_cancelled
```

In the UI, select both runs in the `orders-baseline` experiment and compare them, or sort the run list by `r2`. In verification, adding `is_cancelled` nudged `r2` from 0.9984 to 0.9985 on the same snapshot: a real but tiny gain. Because both runs logged the same `gold_snapshot_id`, you know the comparison is fair: same data, different features. That's the quiet power of logging lineage; without it, you couldn't tell whether a better score came from a better model or different data.

**What just happened?** You just experienced the entire point of experiment tracking. Two runs, side by side, sorted by score, provably on the same data: you can see which was better instead of guessing. Scale that from two runs to two hundred and you understand why no serious ML team works without tracking. The best run isn't the one you remember; it's the one the data says is best.

A note on honesty about the model itself: a regression with a near-perfect `r2` on a couple of dozen rows is not impressive, it's expected, because order count and revenue rise together almost by definition. Treat it as a stand-in. The lakehouse half of this chapter (pinned snapshots, lineage, a registry) is exactly what you'd keep when the model becomes a real one.

## Troubleshooting

- **The log says it "found environment variables used during model inference" and names one of your API keys.** MLflow scans your shell's environment for variable names that look like credentials and writes the *names* (never the values) to `environment_variables.txt` in the model's artifacts, as a hint for whoever deploys it. Nothing secret is stored, but if you'd rather it not record them, `export MLFLOW_RECORD_ENV_VARS_IN_MODEL_LOGGING=false` before training.
- **`mlflow` can't connect to the tracking server.** The tracking URI is wrong or the server is down. Confirm `docker compose ps` shows MLflow running and `curl http://localhost:5000/health` returns `OK`.
- **Logging the model fails with `No module named 'boto3'` (in the server's logs) or an S3 error.** The server is running the stock image. Build the `docker/mlflow` image (`docker compose up -d --build mlflow`) and confirm `MLFLOW_S3_ENDPOINT_URL` points at `http://seaweedfs:8333`.
- **Your runs and registered models disappeared after a restart.** The `mlflow-db` volume is missing, so the SQLite file lived inside the container and went away with it. Add the volume; history from before is gone.
- **`NoSuchNamespaceException: Schema not found: iceberg.gold.daily_revenue`.** You queried the `.snapshots` metadata table (`iceberg.gold.daily_revenue.snapshots`). With Unity Catalog 0.7 these lookups fail (Chapter 5). Use `SHOW TBLPROPERTIES ... ('current-snapshot-id')` as `train.py` does.
- **`Time travel option snapshot-id is no longer supported`.** Use `versionAsOf`, as in `load_training_data`.
- **scikit-learn complains about `Decimal` values.** The `revenue` column reached pandas as `DECIMAL`. Cast it to `DOUBLE` in the Spark read, as `train.py` does.
- **`r2` is `nan` or the fit fails.** Gold has too few rows (fewer than two). Load more days (`make ingest DATE=...` or the Chapter 9 backfill) and refresh Silver and Gold.
- **`.toPandas()` is slow or runs out of memory.** You're pulling a table into local memory. Fine for a small Gold table like ours; for big data you'd sample or train distributed.
- **`register_model` warns that the run has no artifacts at path `model`.** You registered `runs:/<run_id>/model`. It still works through MLflow 3's fallback; register the `model_uri` returned by `log_model` instead.

## Checkpoint

- MLflow is healthy; the UI opens on 5000.
- You read `iceberg.gold.daily_revenue` as training data over `sc://`.
- A run logged params (including the Gold snapshot id it actually read), the `r2` metric, and a model whose files are in `s3://lakehouse/mlflow-artifacts/`.
- You registered the model, can see a versioned entry in the registry, and loaded it back by `models:/orders-order-count/<version>`.
- You can explain how snapshots make the training set reproducible.

## Try it yourself

1. **Run the comparison for real.** Train three times with different features or parameters, then sort the runs by `r2` in the UI and identify the best. You've now used tracking the way real teams do.
2. **Prove reproducibility.** Copy the `gold_snapshot_id` from a run in the UI, refresh Gold (re-run Chapter 7's two refreshes, or let the DAG run), then read `spark.read.option("versionAsOf", <id>).table("iceberg.gold.daily_revenue")` and compare it to what the run trained on (`training_rows`). The current table moved on; the snapshot didn't.
3. **Promote a version.** MLflow 3 promotes models with *aliases* rather than the older fixed stages. Point an alias at a version with `mlflow.MlflowClient().set_registered_model_alias("orders-order-count", "staging", <version>)`, then load it by role instead of number: `mlflow.sklearn.load_model("models:/orders-order-count@staging")`. Move the alias to a newer version and the same load picks it up. "What's the current staging model?" is now a precise answer, not a guess.
4. **Explain the pattern.** In two sentences, explain to an imaginary ML colleague why training on a Gold Iceberg table beats training on an exported CSV.

## Check your understanding

- Why is a lakehouse a good foundation for AI? Name at least three properties Gold tables already have that models want.
- What are the two things MLflow tracks, and what problem does each solve?
- How does logging the Gold snapshot id give you end-to-end reproducibility?
- What's the difference between a logged run and a registered model?

## Recap and what's next

- The lakehouse is a natural AI foundation: clean, versioned, served, scheduled data, reproducible training data for free.
- Experiment tracking logs params, metrics, and artifacts so runs are comparable; the model registry versions and promotes models.
- Logging the Gold snapshot id ties model lineage to exact data, full reproducibility from raw data to deployed model.
- **Next, Chapter 12, Agents:** let an LLM agent operate the lakehouse through the same interfaces you've used, SQL over Spark Connect and the catalog.

![Progress: AI complete, Agents next](../figures/ch11/fig-11.3-progress-ai-done.svg)

**Figure 11.3**. Progress map with **AI done** and **Agents** next.

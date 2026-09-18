# Chapter 11 — AI: training and tracking a model on the lakehouse

## 11.0 What you'll build

The lakehouse feeding intelligence. Your Gold tables are clean, served, and
governed — exactly the shape a model wants. In this chapter you read a Gold table as
**training data**, train a simple model, and record the run with **MLflow**:
**experiment tracking** for params and metrics, and a **model registry** for
versioned, promotable models. The point isn't the model — it's the *pattern*: the
same open tables that serve BI also serve AI, from one governed source.

![Progress: AI](../figures/ch11/fig-11.3-progress-ai.svg)

**Figure 11.3** — Progress map with **AI** highlighted.

## 11.1 Learning objectives

By the end of this chapter you can:

- Read a Gold table as **training data** for a model.
- Use MLflow **experiment tracking** to log params, metrics, and artifacts.
- Register a trained model in the MLflow **model registry** and version it.
- Explain why a lakehouse is a natural foundation for AI workloads.

## 11.2 Why the lakehouse is a good home for AI

Models are only as good as their data, and the lakehouse already solved the hard
data problems: Gold tables are clean and conformed (Ch 7), versioned via snapshots
(Ch 5) so you can reproduce *exactly which data* trained a model, served openly
(Ch 10) so training reads the same governed tables as everything else, and
refreshed on a schedule (Ch 9). "Train on Gold" gives you reproducibility and
lineage for free — you can always answer "what data produced this model?"

> **Training data** *(canonical term)*: the historical, feature-shaped data a model
> learns from — here, rows from a Gold table.

![Gold feeds the model; MLflow tracks it](../figures/ch11/fig-11.1-ai-flow.svg)

**Figure 11.1** — Gold table → training data → model; every run's params,
metrics, and the model artifact are logged to MLflow and versioned in the registry.

## 11.3 What MLflow gives you

> **Experiment tracking** *(canonical term)*: recording each model run's parameters,
> metrics, and artifacts so runs are comparable and reproducible.

> **Model registry** *(canonical term)*: a versioned store of trained models,
> promotable through stages (e.g. staging → production).

Without tracking, model development is a mess of notebooks and "which version was
that?" MLflow fixes both ends: every training run is logged (what params, what
score, what artifact), and the good models get registered under a name with
versions you can promote or roll back — the same disciplined versioning Iceberg gave
your data, now for your models.

### Under the hood — reproducibility from snapshots

Because you train on an Iceberg Gold table, you can log the table's **snapshot id**
as a run parameter. That means a year later you can reconstruct the *exact* training
set by time-traveling to that snapshot (Ch 5). Data versioning + model versioning =
end-to-end reproducibility, something classic ML setups struggle to guarantee.

## 11.4 Build — train, track, register

Step 1 — start MLflow and open its UI:

```bash
./lakehouse start mlflow
./lakehouse status
```

Expected: MLflow tracking server healthy; UI available on port **5000**.

Step 2 — read Gold as training data over Spark Connect, then train and log
with MLflow. We fit a small model that predicts order count from revenue — trivial
on purpose; the *workflow* is the lesson:

```python
import mlflow, mlflow.sklearn
from sklearn.linear_model import LinearRegression
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Record WHICH data trained the model (snapshot id → reproducibility).
snap = spark.sql("SELECT max(snapshot_id) s FROM iceberg.gold.daily_revenue.snapshots").collect()[0]["s"]
pdf = spark.table("iceberg.gold.daily_revenue").toPandas()
X, y = pdf[["revenue"]], pdf["order_count"]

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("orders-baseline")

with mlflow.start_run():
    model = LinearRegression().fit(X, y)
    mlflow.log_param("gold_snapshot_id", snap)     # data lineage
    mlflow.log_param("features", "revenue")
    mlflow.log_metric("r2", model.score(X, y))     # a metric to compare runs
    mlflow.sklearn.log_model(model, "model")       # the artifact
```

Expected: a new run appears under the `orders-baseline` experiment in the MLflow UI,
showing the `r2` metric, the logged params (including the Gold snapshot id), and the
saved model artifact.

Step 3 — register the model to version and promote it:

```python
result = mlflow.register_model(
    model_uri=f"runs:/{mlflow.last_active_run().info.run_id}/model",
    name="orders-order-count",
)
# The registry now holds version N of "orders-order-count", ready to promote.
```

Expected: the model shows up in the MLflow **Models** tab as a named, versioned
entry you can move through stages.

Step 4 — compare runs: train again with a tweak (e.g. a different feature)
and view both runs side by side in the UI, sorted by `r2`. This is why tracking
exists — objective comparison instead of guesswork.

## 11.5 Checkpoint

- MLflow is healthy; the UI opens on **5000**.
- You read `iceberg.gold.daily_revenue` as training data over `sc://`.
- A run logged params (including the Gold **snapshot id**), the `r2` metric, and a
  model artifact.
- You **registered** the model and can see a versioned entry in the registry.
- You can explain how snapshots make the training set reproducible.

## 11.6 Recap & what's next

- The lakehouse is a natural AI foundation: clean, versioned, served, scheduled data.
- **Experiment tracking** logs params/metrics/artifacts; the **model registry**
  versions and promotes models.
- Logging the Gold **snapshot id** ties model lineage to exact data — full
  reproducibility.
- **Next — Chapter 12, Agents:** let an LLM agent operate the whole lakehouse
  through its CLI and skills.

![Progress: AI complete, Agents next](../figures/ch11/fig-11.4-progress-ai-done.svg)

**Figure 11.4** — Progress map with **AI ✓** and **Agents** next.

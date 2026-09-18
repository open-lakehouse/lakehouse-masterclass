# Chapter 4 — Compute: Spark, driven by Spark Connect

## 4.0 What you'll build

The engine of the lakehouse — Apache Spark — and the modern way to drive it: **Spark
Connect**. You'll start a Spark cluster and its Connect server, then run your first
job from a **thin client** over `sc://`, without bundling the whole Spark engine
into your program. This is the transport every later chapter uses.

![Progress: Compute](../figures/ch04/fig-4.3-progress-compute.svg)

**Figure 4.3** — Progress map with **Compute** highlighted.

## 4.1 Learning objectives

By the end of this chapter you can:

- Explain what **Spark** does in the lakehouse (distributed compute).
- Explain what **Spark Connect** is and how it differs from the classic embedded
  driver model.
- List the practitioner advantages of Spark Connect — and its honest trade-offs.
- Connect a Python client to a remote Spark cluster with `sc://` and run a job.

## 4.2 What Spark is (briefly)

> **Compute** *(canonical term)*: the engine that reads, writes, and transforms
> data — separate from storage. Ours is Apache Spark 4.x.

Spark is a distributed compute engine: it splits work across executors so you can
process far more data than fits on one machine, using a familiar DataFrame and SQL
API. In a lakehouse, Spark is the workhorse that lands data (Chapter 6), transforms
it (Chapter 7), and streams it (Chapter 8). Storage (Chapter 3) holds the bytes;
Spark is what *acts* on them.

## 4.3 The old way vs. the modern way

Historically, your program *was* the Spark driver: you bundled a full Spark JVM —
the `SparkContext`, the Catalyst optimizer, the scheduler — into your process and
either ran `spark-submit` or built a local `SparkSession`. That works, but it makes
your client heavy, pins it to one exact Spark and JVM version, and means a crash in
your code can take the whole driver (and job) down with it.

> **Spark Connect** *(canonical term)*: a thin client that drives a remote Spark
> cluster over gRPC at `sc://host:15002`, decoupled from the driver.

With Spark Connect, your client becomes a **thin library**. It builds an unresolved
logical plan from your DataFrame/SQL calls and ships it as a **protobuf message over
gRPC** to a remote **Spark Connect server** — which *is* the real driver, running
on the cluster. The server analyzes, optimizes, and executes the plan, then streams
results back as **Apache Arrow** batches. The API you write is identical; what
changes is *where the driver lives*.

![Spark Connect architecture: thin client, remote driver](../figures/ch04/fig-4.1-spark-connect-architecture.svg)

**Figure 4.1** — Classic embedded-driver model vs. Spark Connect's thin client
over gRPC, with the `sc://` client call.

## 4.4 Why Spark Connect (and the honest trade-offs)

This is the reason we standardize on Connect for the whole course. The advantages
are concrete:

- **Thin client** — your app links a small client library and talks gRPC; it
  doesn't ship a multi-hundred-megabyte Spark JVM.
- **Language & version independence** — the client isn't pinned to the server's
  exact Spark/JVM build. Upgrade the server without rebuilding every client.
- **Connect from anywhere via `sc://`** — an IDE, a notebook, a web app, or a
  long-running service attaches to a remote cluster with one URL. No jar packaging,
  no cluster-side submission.
- **Stability & isolation** *(the headline win)* — a buggy or out-of-memory client
  can't take down the driver or the cluster; the server keeps running and other
  sessions are unaffected.
- **Many concurrent, isolated sessions** — one Connect server multiplexes many
  users and apps sharing the same cluster compute.
- **Interactive development & embeddability** — fast local REPL/notebook loops
  against real cluster data, and a backend service can hold a Spark session as just
  another remote dependency.

But teach it honestly — Connect exposes the DataFrame / Spark SQL / Structured
Streaming surface, **not** the low-level driver internals:

- **No RDD API / no `SparkContext`** — the biggest gap. Code that drops to
  `spark.sparkContext` or RDDs won't work. For a modern DataFrame/SQL lakehouse
  this rarely bites, but know it.
- **UDF constraints** — Python/pandas UDFs work but serialize differently; the
  client and server Python environments must be compatible.
- **Version-specific gaps** — Connect gained streaming and more of the ML API over
  releases; verify features against the Spark version you run.
- **Different debugging** — errors arrive as `pyspark.errors.exceptions.connect.*`;
  the useful line is usually the last one.
- **`CANNOT_MODIFY_STATIC_CONFIG`** — static settings (SQL extensions, jars, the
  warehouse dir, the gRPC port) must be set **server-side** in `spark-defaults.conf`;
  a client session cannot inject them. We'll hit this in Chapter 5 when wiring the
  catalog.

![Spark Connect advantages vs. trade-offs](../figures/ch04/fig-4.2-spark-connect-advantages.svg)

**Figure 4.2** — The advantages (left) and the honest trade-offs (right).

## 4.5 Build — start Spark and connect a thin client

Step 1 — start the Spark cluster (master, worker, and the Connect server):

```bash
./lakehouse start spark
./lakehouse status
```

Expected: the Spark master, worker, and Connect server show healthy. The Spark UI
is available in your browser (master UI on the Spark 4.1 default), and the Connect
server is listening on gRPC port **15002**.

Step 2 — open the Spark UI to see the cluster:

Visit the master UI in your browser. You should see one worker registered and no
applications running yet.

Step 3 — connect a thin client and run your first job:

```python
from pyspark.sql import SparkSession

# The whole point: we attach to a REMOTE Spark over sc://, not a local driver.
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

df = spark.range(5)
df.show()
```

Expected output:

```
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
```

Step 4 — point the client somewhere else without changing code, using an
environment variable or URL parameters:

```bash
# Local, via env var (PySpark reads SPARK_REMOTE):
export SPARK_REMOTE="sc://localhost:15002"

# Remote with TLS + a token — same client code, different URL:
#   sc://connect.example.com:443/;use_ssl=true;token=<bearer>
```

This portability — same code, different endpoint — is exactly why we can develop
locally and later point at a bigger cluster with a one-line change.

### Under the hood — SDP is built on Connect

Spark Declarative Pipelines (`pyspark.pipelines`), which we use to build the
medallion in Chapter 7, **use Spark Connect internally**. Standardizing on Connect
now isn't just a compute choice — it's the foundation the transformation layer is
built on.

## 4.6 Checkpoint

- `./lakehouse status` shows Spark master, worker, and the Connect server healthy.
- The Spark UI shows a registered worker.
- Your Python client connected via `sc://localhost:15002` and `spark.range(5).show()`
  returned five rows.
- You can name three advantages of Spark Connect and at least one trade-off.

## 4.7 Recap & what's next

- **Spark** is the compute engine of the lakehouse.
- We drive it with **Spark Connect** — a thin client over `sc://` — for portability,
  isolation, and thin dependencies, accepting the DataFrame/SQL surface in exchange.
- Static config lives **server-side**; remember `CANNOT_MODIFY_STATIC_CONFIG`.
- **Next — Chapter 5, Tables & Catalog:** create real Iceberg tables through **Unity
  Catalog OSS**, evolve their schema, and travel through their snapshots.

![Progress: Compute complete, Tables next](../figures/ch04/fig-4.4-progress-compute-done.svg)

**Figure 4.4** — Progress map with **Compute ✓** and **Tables & Catalog** next.

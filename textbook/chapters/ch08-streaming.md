# Chapter 8 — Streaming: real-time ingestion with Kafka and Structured Streaming

## 8.0 What you'll build

The real-time path. In Chapter 6 you ingested a bounded **batch** of orders. Now
you'll feed the *same* Bronze table **continuously**: orders arrive as an **event
stream** on Kafka, and a Spark **Structured Streaming** job lands them into Iceberg
as they happen. You'll handle the hard parts of streaming honestly — **unbounded**
data, late events with **watermarks**, and exactly-once **deduplication** — and see
exactly when to reach for streaming instead of batch.

Streaming is where a lot of learners get intimidated, and understandably: "process
data that never stops arriving, in the right order, exactly once, even when things
fail" sounds daunting. It's not, once you have the right mental model — and building
it here demystifies a topic that reads like black magic in job descriptions. The key
reassurance up front: streaming in Spark uses the *same* DataFrame API you already
know. If you can write a batch transformation, you're most of the way to a streaming
one.

![Progress: Streaming](../figures/ch08/fig-8.3-progress-streaming.svg)

**Figure 8.3** — Progress map with **Streaming** highlighted.

## 8.1 Learning objectives

By the end of this chapter you can:

- Explain what an **event stream** and **unbounded** data are.
- Read from a Kafka topic with Spark **Structured Streaming** and write to Iceberg.
- Use a **watermark** to bound state and handle late-arriving events.
- Deduplicate a stream so the same event isn't landed twice.
- Decide when **streaming** is the right tool versus **batch**.

## 8.2 Streaming vs. batch

> **Streaming** *(canonical term)*: processing **unbounded** data continuously, as
> each event arrives, rather than in scheduled bounded chunks.

> **Event stream** *(canonical term)*: an unbounded, ordered sequence of records —
> here, a Kafka topic of order events.

Batch asks "process today's orders." Streaming asks "process each order the moment
it appears, forever." The data never ends — it's **unbounded** — so a streaming job
is a long-running process, not a script that finishes. You choose streaming when
freshness matters (fraud checks, live dashboards, alerting); you choose batch when
periodic is fine and simplicity wins. Crucially, both can feed the *same* Bronze
table — streaming is a different *how*, not a different destination.

The honest trade-off, stated plainly: **streaming buys you freshness and costs you
simplicity.** A batch job starts, processes a known chunk, and stops — easy to
reason about, easy to retry, easy to test. A streaming job runs forever, holds
state, must cope with events arriving out of order and late, and can never just
"start over" cleanly. That complexity is worth paying when minutes or seconds
matter — a fraud check that runs nightly is useless. It's *not* worth paying when a
daily refresh is fine. A common and healthy instinct in the industry: **default to
batch; reach for streaming only when the freshness requirement genuinely demands
it.** Plenty of "real-time" requirements turn out to be "within an hour is fine,"
which batch handles with far less operational burden.

![Batch vs. streaming into the same Bronze](../figures/ch08/fig-8.1-batch-vs-streaming.svg)

**Figure 8.1** — Batch lands bounded chunks on a schedule; streaming lands
individual events continuously — both writing to the same Iceberg Bronze table.

### Under the hood — a stream is really a table that never stops growing

Spark's Structured Streaming has a beautifully simple core idea: treat the
unbounded stream as an **unbounded table** that new rows keep getting appended to.
Your streaming query is conceptually the *same* query you'd write against a static
table; Spark just re-runs it incrementally as new rows arrive, in small
**micro-batches**. This is why the API is identical to batch — because in Spark's
model, a stream literally *is* a table, just one that's never finished. Hold onto
this mental image: it's what makes streaming feel manageable instead of alien.

## 8.3 The streaming challenges: order, lateness, duplicates

Real streams are messy. Events arrive **out of order** and **late** (a phone was
offline; a retry fired an hour later), and the *same* event can arrive **twice** (an
at-least-once producer). A correct streaming job must handle all three.

To make this concrete: imagine a customer places an order on a train, loses signal
in a tunnel, and their phone re-sends the order three times over the next ten
minutes as it reconnects. Your stream now has the *same* order arriving three times,
out of order relative to other customers' orders, and later than when it was
actually placed. A naive job would land three duplicate orders. A correct job lands
exactly one, dated to when the order was really placed. The tools for that are
watermarks and deduplication.

> **Watermark** *(canonical term)*: the streaming engine's moving clock — "I will
> not wait for events older than this" — which bounds how much state is kept and
> decides when late data is too late to include.

> **Dedup / idempotency** *(canonical term)*: dropping repeated events (by key +
> event time) so each real event lands exactly once.

Without a watermark, the engine would have to remember every event forever to catch
possible late-arrivers — unbounded state, eventual crash. The watermark says "after
this much lateness, stop waiting," letting Spark drop old state safely.

Think of the watermark as a restaurant's kitchen closing time. The kitchen will
happily serve you if you order a little late, but at some point it *closes* — it
can't stay open all night on the chance one more diner shows up. The watermark is
that closing time for late data: "I'll accept events up to 10 minutes late, but
after that the kitchen's closed and I've cleaned up." Without a closing time, the
kitchen (your job's memory of past events) would have to stay staffed forever,
which is impossible. The watermark is the promise that lets Spark forget old state
and keep running indefinitely.

### Under the hood — Structured Streaming on Connect

Structured Streaming runs through the same Spark Connect server you started in
Chapter 4 (streaming support was added to Connect in the Spark 4.x line). The
streaming DataFrame API is identical to batch — `readStream`/`writeStream` instead
of `read`/`write` — so everything you learned transfers directly. This is the
promise from the chapter intro made concrete: your streaming code is a thin client
shipping a plan to the same remote engine, just one that runs continuously.

## 8.4 Build — stream orders from Kafka into Bronze

Step 1 — start Kafka and begin producing a live stream of order events:

```bash
./lakehouse start kafka
./lakehouse testdata stream --speed 5     # emit order events to the Kafka topic
```

Expected: Kafka is healthy on **9092**, and the generator publishes order events
continuously to the orders topic. (`./lakehouse producer` does the same;
`./lakehouse consumer` runs a ready-made Structured Streaming consumer if you want
to see one immediately.)

**What just happened?** You started **Kafka** — the event streaming platform that
holds the `orders` topic — and turned on a producer that publishes order events into
it continuously. Kafka is the "pipe" between the source and your streaming job: the
generator writes events in one end, and your Spark job (next steps) reads them out
the other. Kafka durably holds events so that if your consumer is down for a while,
the events wait for it rather than being lost.

Step 2 — connect a thin client and define the streaming read from Kafka:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

schema = StructType([
    StructField("order_id", StringType()), StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),   StructField("status", StringType()),
    StructField("created_at", TimestampType()),
])

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "orders")
       .option("startingOffsets", "latest")
       .load())

# Kafka values are bytes; parse the JSON payload into columns.
events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
             .select("e.*"))
```

**What just happened?** Two things worth noticing. First, `readStream` instead of
`read` — that one word is the difference between a batch and a streaming source;
everything after it is the familiar DataFrame API. Second, Kafka hands you raw bytes
in a `value` column, so you parse the JSON payload into real typed columns with
`from_json` and your declared `schema`. Kafka doesn't know or care what's inside your
messages — it just moves bytes — so giving the data structure is your job on the
read side.

Step 3 — watermark and deduplicate, then write the stream into the Bronze
Iceberg table:

```python
deduped = (events
    .withWatermark("created_at", "10 minutes")          # tolerate 10 min lateness
    .dropDuplicatesWithinWatermark(["order_id"]))        # exactly-once per order

query = (deduped.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://lakehouse/_checkpoints/bronze_orders_stream")
    .toTable("iceberg.bronze.orders_raw"))               # SAME Bronze table as batch

query.awaitTermination()   # long-running: this job doesn't "finish"
```

**What just happened?** This is the train-in-the-tunnel scenario handled in three
lines. `withWatermark("created_at", "10 minutes")` sets the kitchen's closing time —
accept events up to 10 minutes late. `dropDuplicatesWithinWatermark(["order_id"])`
lands each `order_id` exactly once even if the producer sent it three times. And
`toTable("iceberg.bronze.orders_raw")` writes to the *same* Bronze table your batch
job used — streaming is a different *how*, same destination. Finally,
`awaitTermination()` reflects that this job never finishes; it runs until you stop
it.

Step 4 — from another client, watch the Bronze count climb as events flow:

```python
spark.table("iceberg.bronze.orders_raw").count()   # run repeatedly — it grows
```

**What just happened?** Running that count repeatedly and watching it climb is your
proof that the stream is live — events are flowing from the generator, through
Kafka, through your Structured Streaming job, into Iceberg, in near-real-time. That
growing number is the whole chapter working end to end.

## 8.5 Why the checkpoint matters

The `checkpointLocation` is what makes a streaming job restartable and idempotent
across failures: it durably records which Kafka offsets have been processed and the
watermark state. Kill the job and restart it, and it resumes from the last committed
offset — it will not skip events or double-write them. Delete the checkpoint and it
starts over. Treat the checkpoint as part of your data: one checkpoint per streaming
query, never shared.

This is the streaming equivalent of the idempotency you built in Chapter 6, and it
matters for the same reason: things fail, and recovery must be safe. A batch job
retries by re-running a bounded chunk. A streaming job can't "re-run" — it never
stopped — so instead it remembers its exact position (the Kafka offsets) and its
in-flight state (the watermark) in the checkpoint. On restart, it reads that
bookmark and continues precisely where it left off. Without the checkpoint, a
restart would either re-process events (duplicates) or skip them (data loss).
**The checkpoint is what turns a fragile long-running process into a reliable one.**
Never point two different queries at the same checkpoint location, and never delete
it unless you truly mean "start this stream over from scratch."

## 8.6 Troubleshooting

- **The streaming query starts but no rows land.** With `startingOffsets: latest`,
  the job only sees events produced *after* it started. Confirm `testdata stream` is
  actively producing, or use `earliest` to read from the start of the topic.
- **"Cannot connect to Kafka" / broker errors.** Kafka isn't up or you're on the
  wrong address. Check `./lakehouse status` and use `localhost:9092` from the host.
- **Row count doesn't grow.** Either the producer stopped, the schema doesn't match
  the JSON (so `from_json` yields nulls), or the watermark/dedup is dropping
  everything. Inspect a few parsed rows before the write.
- **"Checkpoint location already exists" or corrupt-state errors on restart.** You
  changed the query in an incompatible way, or two queries share a checkpoint. Use a
  fresh checkpoint path for a genuinely new query.
- **Streaming Connect errors.** As in Chapter 4, read the last line of the
  Connect-style traceback first.

## 8.7 Checkpoint (yours, not the stream's)

- Kafka is healthy on **9092** and `testdata stream` is publishing order events.
- Your Structured Streaming job reads the `orders` topic over `sc://` and appends to
  `iceberg.bronze.orders_raw`.
- The job uses a **watermark** and `dropDuplicatesWithinWatermark` for exactly-once.
- A `checkpointLocation` is set, and the Bronze row count grows as events arrive.
- You can state one case where streaming beats batch and one where it doesn't.

## 8.8 Try it yourself

1. **Kill and restart.** Stop the streaming query, note the Bronze count, restart the
   same job (same checkpoint), and confirm it resumes without duplicating or skipping.
   That's the checkpoint earning its keep.
2. **Read from the beginning.** Start a fresh query with `startingOffsets: earliest`
   and a *new* checkpoint path, and watch it backfill the whole topic. Compare with
   `latest`.
3. **Tune the watermark.** Change the watermark from 10 minutes to 1 minute and
   reason about what would happen to a genuinely late event. What are you trading?
4. **Make the batch-vs-streaming call.** For three imaginary requirements ("nightly
   finance report," "live fraud alerts," "hourly inventory sync"), decide batch or
   streaming and justify each in one sentence.

## 8.9 Check your understanding

- What does "unbounded" mean, and why does it make a streaming job a long-running
  process rather than a script?
- Explain the "a stream is a table that never stops growing" model.
- What problem does a watermark solve, and what's the kitchen-closing-time analogy?
- What does the checkpoint store, and what breaks if you delete or share it?

## 8.10 Recap & what's next

- **Streaming** processes **unbounded** event streams continuously; batch processes
  bounded chunks. Both can feed the same Bronze table.
- Streaming buys freshness and costs simplicity — default to batch, reach for
  streaming when freshness genuinely demands it.
- **Watermarks** bound state and handle lateness; **dedup within the watermark**
  gives exactly-once landing.
- **Checkpoints** make streaming jobs restartable and idempotent.
- Structured Streaming uses the same Connect transport and DataFrame API as batch.
- **Next — Chapter 9, Orchestration:** schedule the batch medallion pipeline as a
  DAG with retries and backfills.

![Progress: Streaming complete, Orchestration next](../figures/ch08/fig-8.4-progress-streaming-done.svg)

**Figure 8.4** — Progress map with **Streaming ✓** and **Orchestration** next.

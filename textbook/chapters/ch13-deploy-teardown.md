# Chapter 13 — Deploy, Teardown & Where to Go Next

## 13.0 What you'll build

The wrap-up. You've built the whole lakehouse — storage to agents — on your laptop.
This final chapter zooms out: a **narrated tour** of taking it to the cloud (so you
understand the path without spending a dollar), a clean **teardown** that gives your
machine back, and a roadmap of where to grow next. No new required infrastructure —
this is about understanding the full lifecycle and leaving you with next steps.

Take a moment first to appreciate what you've done. Thirteen chapters ago, "open
lakehouse" was probably a buzzword. Now you've built one — every layer, by hand,
from the object store up to an AI agent operating the whole thing. That's not a toy;
it's the same architecture, the same open components, that power real data platforms
at real companies. This chapter is about landing the plane: understanding how what
you built relates to production, cleaning up, and knowing where to go from here.

![Progress: complete](../figures/ch13/fig-13.3-progress-complete.svg)

**Figure 13.3** — Progress map: every layer complete.

## 13.1 Learning objectives

By the end of this chapter you can:

- Contrast **self-hosted** and **managed** deployments and pick sensibly.
- Describe the cloud deploy path (Terraform) at a high level.
- Tear the local lakehouse down cleanly, understanding what's preserved vs. wiped.
- Name concrete next steps to deepen each layer.

## 13.2 Self-hosted vs. managed

> **Self-hosted vs. managed** *(canonical term)*: you run the infrastructure
> yourself (this whole course) versus a cloud vendor runs it for you (EMR,
> Databricks, MSK, managed Airflow).

Everything you built is **self-hosted** — you own every process. The exact same
open components also exist as **managed** services, and because you built on open
standards, moving is a *lift*, not a *rewrite*: your Iceberg tables, Spark jobs, SDP
pipelines, and Airflow DAGs port over. That's the whole payoff of the open-source
choice from Chapter 1 — the skills and the artifacts transfer.

So which should *you* use in real life? The honest tradeoff: **managed services buy
you time and cost you money and control.** A managed Spark or Airflow means you don't
babysit servers, patch security holes, or get paged when a node dies — the vendor
does that. You pay for it, both in dollars and in some loss of control and
flexibility. Self-hosting is cheaper and infinitely tweakable but puts the
operational burden on you. Most teams end up managed for production (their engineers'
time is worth more than the server savings) and value people who understand the
open guts underneath — which, having built it all yourself, you now do. That
understanding is exactly what makes you effective on a managed platform: you know
what the buttons actually do.

![Self-hosted vs. managed — same open stack](../figures/ch13/fig-13.1-selfhosted-vs-managed.svg)

**Figure 13.1** — The same open components, self-hosted (you run them) vs.
managed (a vendor runs them); the open formats and code carry across.

## 13.3 The cloud path (a tour, not a bill)

The reference architecture ships Terraform for cloud deployment. We *tour* it —
reading the shape, not running it — so you don't incur cloud costs to learn the
path:

- **`terraform/`** — self-hosted-on-cloud: Spark on ECS/EMR, object storage on S3,
  Postgres on RDS. You still run the open components, just on cloud infrastructure.
- **`terraform-databricks/`** — the fully managed path on Databricks.

> **Infrastructure as code** — the deployment is *declared* in Terraform and applied
> reproducibly, the same declarative philosophy you saw in SDP (Ch 7) and Airflow
> DAGs (Ch 9): describe the desired state, let the tool converge to it.

Notice the pattern repeating one more time: **declarative** shows up at every layer
of this course. SDP declares tables and lets the engine derive the build order.
Airflow declares task dependencies and derives the run order. Terraform declares
cloud infrastructure and derives the steps to create it. Once you internalize
"describe the desired end state, let the tool figure out how to get there," you have
a mental model that transfers across the entire modern data and infrastructure
stack. It's arguably the single most portable idea in the whole book.

### Under the hood — why "lift, not rewrite" is true

Your data is in **Iceberg**, an open table format any engine and any cloud reads.
Your catalog speaks the **Iceberg REST** protocol. Your transforms are **Spark**.
None of these are proprietary, so a managed service consumes them directly. Contrast
a closed warehouse, where migrating means exporting and re-modeling everything. Open
standards are what make your work portable — that was the bet in Chapter 1, and this
is where it pays off. The bet has a name in the industry: *avoiding lock-in*. You
made it deliberately in Chapter 1, carried it through every layer, and now it means
you're never trapped — your data and pipelines can move to any engine, any cloud,
any vendor, because none of them owns the format.

## 13.4 Teardown — give your machine back

Step 1 — stop everything (preserves your data volumes):

```bash
./lakehouse stop all
./lakehouse status
```

Expected: all services stop; `status` shows nothing running. Your Iceberg tables and
catalog data remain on disk, so a later `start` resumes where you left off.

**What just happened?** `stop all` is the gentle option — it halts the running
services but leaves your data volumes intact. Think of it as closing the shop for the
night, not demolishing it. When you come back and `start`, your Iceberg tables,
catalog entries, and everything else are exactly where you left them. This is the
command for "I'm done for today," not "I'm done forever."

Step 2 — full reset, only if you want a clean slate (this **wipes data**):

```bash
./lakehouse reset --all      # destructive: removes data + metadata
```

Expected: volumes and state are cleared. Use this to reclaim disk or start fresh —
and note this is exactly the kind of destructive operation that should require human
confirmation when an agent (Ch 12) is at the controls.

**What just happened?** This is the demolition option — it removes the data volumes,
not just the running services. Everything is gone: tables, catalog, checkpoints. Use
it deliberately when you want to reclaim disk space or begin completely fresh, and
*never* casually. It's the perfect example of the "human-in-the-loop for destructive
steps" guardrail from Chapter 12: a fast operator (human or agent) should have to
consciously confirm before wiping data. Reversible operations can be automatic;
irreversible ones deserve a pause.

> Teardown discipline is a feature, not an afterthought: because everything runs in
> containers with explicit volumes, "give my laptop back" is one command, and there
> are no stray processes left behind. This clean lifecycle — stand it all up with one
> command, tear it all down with another — is one of the quiet luxuries of building
> on containers, and it's why you could experiment fearlessly throughout the course.

## 13.5 Where to go next

You've built the core. Concrete ways to deepen each layer:

- **Storage / Tables** — table maintenance: compaction, snapshot expiration, and
  partition evolution on your Iceberg tables.
- **Transformation** — add a real data-quality suite and more Gold marts; explore
  incremental SDP refresh in depth.
- **Streaming** — exactly-once end-to-end, schema-registry-backed Kafka, and
  windowed aggregations in the stream.
- **Serving** — wire a BI tool (or a semantic layer) onto the Gold tables; try
  Trino as a third engine.
- **AI** — batch inference writing predictions back to an Iceberg table, and model
  promotion workflows in the registry.
- **Agents** — a retrieval/analytics agent that answers questions by querying Gold
  through the catalog (text-to-SQL over the lakehouse).
- **Governance** — deepen Unity Catalog OSS: access control, lineage, and audit.

Pick one that excites you and go deep, rather than trying to do all seven. The
fastest way to solidify what you've learned is to *change* the system — add a Gold
table, wire in a new engine, extend the agent — because that forces you to
understand how the layers connect, not just how each works alone. You have a
complete, working lakehouse to experiment on; use it as a sandbox.

## 13.6 Checkpoint — you built an open lakehouse

Look back at the cold open from Chapter 1. You now have:

- **Storage** (object store) holding open **Iceberg** tables via **Unity Catalog OSS**.
- **Compute** driven by **Spark Connect**.
- **Ingestion** (batch) and **Streaming** (Kafka) both feeding **Bronze**.
- **Transformation** into Silver/Gold with **Spark Declarative Pipelines**.
- **Orchestration** via **Airflow**.
- **Serving** to multiple engines through one open catalog.
- **AI** trained and tracked with **MLflow**.
- **Agents** operating it all through the CLI.

Every layer, built by you, running on your own machine, entirely open source. If you
can look at that list and, for each item, explain *what it does, why it's there, and
roughly how you built it* — you don't just have a running lakehouse, you have the
mental model of a data engineer. That's the real deliverable of this course.

## 13.7 Try it yourself

1. **Do a full round-trip.** `stop all`, confirm everything's down, then `start` the
   stack again and verify your Gold table survived. Feel the difference between
   stopping and resetting.
2. **Tour the Terraform.** Open the `terraform/` files and read them like a map.
   Match each resource (S3, RDS, EMR/ECS) to the local component it replaces. You're
   reading a production deployment without paying for one.
3. **Write your own roadmap.** From the "where to go next" list, pick one item and
   write down the first three concrete steps you'd take. Commit to actually doing it.
4. **Teach it back.** The ultimate test: explain the whole architecture — storage to
   agents — to someone else (or to a rubber duck) in five minutes, using the Chapter
   1 architecture diagram. If you can, you've truly got it.

## 13.8 Check your understanding

- What's the real tradeoff between self-hosted and managed, and why do most
  production teams choose managed?
- Why is migrating this lakehouse to the cloud a "lift, not a rewrite"?
- Where does the "declarative" idea appear across the course? Name at least three
  places.
- What's the difference between `stop all` and `reset --all`, and which deserves a
  confirmation gate?

## 13.9 Recap

- **Self-hosted** (this course) and **managed** run the same open stack; open
  standards make migration a lift, not a rewrite — the anti-lock-in bet from Chapter
  1 paying off.
- The cloud path is Terraform — declarative infrastructure, toured here without cost;
  "declare the end state, let the tool converge" recurs at every layer.
- **Teardown** is one command; `stop all` preserves data, `reset --all` wipes it —
  reversible steps can be automatic, irreversible ones deserve a pause.
- The roadmap deepens every layer — you have the foundation, and the mental model, to
  grow.

![Progress: course complete](../figures/ch13/fig-13.4-progress-complete.svg)

**Figure 13.4** — Every layer checked: the finished open lakehouse.

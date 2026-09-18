# Chapter 2 — Setup: Prerequisites and the Control CLI

## 2.0 What you'll build

A working environment and the one tool you'll use to drive everything else: the
`./lakehouse` control CLI. By the end you'll have the prerequisites installed, the
project cloned, your configuration in place, and a green `./lakehouse status`.

![Progress: Setup](../figures/ch02/fig-2.1-progress-setup.svg)

**Figure 2.1** — Progress map with **Setup** highlighted.

## 2.1 Learning objectives

By the end of this chapter you can:

- Install the prerequisites (Docker, Java, Python, Poetry) on macOS, Ubuntu, or
  Windows/WSL2.
- Explain the two **host-native** dependencies (Postgres and the object store) and
  why they're not containers.
- Clone the project and create your `.env` configuration.
- Use the `./lakehouse` CLI to validate and inspect the stack.

## 2.2 Prerequisites (be honest about hardware)

This whole lakehouse runs on your machine, so set expectations up front.

`[TABLE 2.1]` — Hardware and software:

| Resource | Minimum | Recommended |
|---|---|---|
| RAM | 8 GB | 16 GB |
| Disk | 20 GB free | 50 GB free |
| CPU | 4 cores | 8 cores |

Software to install first:

- **Docker** (with Docker Compose) — runs the services.
- **Java 17+** (Java **21** is required for Spark 4.1, our default) — Spark runs on
  the JVM.
- **Python 3.10+** — the pipelines and tooling.
- **Poetry** — Python dependency management for the project.

Supported operating systems: **macOS**, **Ubuntu/Debian**, and **Windows via
WSL2**. On Windows, do everything inside your WSL2 Linux shell — treat it as Ubuntu.

### Under the hood — two dependencies that are NOT containers

Most of the stack runs in Docker, but **two foundational services run natively on
your host**: **PostgreSQL** (the catalog's metadata database) and the **object
store** (covered in Chapter 3). The containers reach them over
`host.docker.internal`. This trips up a lot of beginners — if a service can't find
its database, it's almost always because Postgres isn't running on the host. We
call this out again in Chapter 3, because it's the single most common setup snag.

## 2.3 Get the code

```bash
git clone <the-course-companion-repo>.git
cd <companion-repo>
```

The project root contains the `./lakehouse` script — your control surface for the
entire course — plus per-service Docker Compose files, the pipelines, and the
test-data generator.

## 2.4 Configure your environment

The project ships an example environment file. Copy it and fill in the blanks.

```bash
cp .env.example .env
```

Open `.env` and set the credentials. The important variables you'll see:

`[TABLE 2.2]` — Key environment variables (Storage credentials, detailed in Ch 3):

| Variable | Purpose |
|---|---|
| `S3_ENDPOINT` | Where the object store listens (e.g. `http://host.docker.internal:8333`) |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | Object-store credentials |
| `S3_BUCKET` | The bucket name (e.g. `lakehouse`) |
| `S3_WAREHOUSE` | The warehouse path (e.g. `s3a://lakehouse/warehouse`) |
| `POSTGRES_*` | Catalog metadata database connection |

> Security note: `.env` holds secrets and is gitignored. Never commit it. This is
> the course's first **security** practice — keep credentials out of version control.

## 2.5 Meet the control CLI

Everything in this course runs through one command. Get its help first:

```bash
./lakehouse help
```

`[TABLE 2.3]` — The commands you'll use most:

| Command | What it does |
|---|---|
| `./lakehouse setup` | Validate the environment and install dependencies (downloads Spark JARs) |
| `./lakehouse check-config` | Validate credentials are consistent between `.env` and Spark config |
| `./lakehouse preflight` | Run pre-start checks |
| `./lakehouse status` | Show the health of every service (add `--json` for machine output) |
| `./lakehouse start [service]` | Start a service (`spark`, `kafka`, `airflow`, `unity-catalog`, `all`) |
| `./lakehouse stop [service]` | Stop a service |
| `./lakehouse logs [service]` | Tail a service's logs |
| `./lakehouse testdata <cmd>` | Generate / stream / load / inspect test data |

> Note the `--version 4.0|4.1` option: it selects the Spark version. We use the
> default (**4.1**, on Java 21) throughout, because it's the version with the
> modern Spark Connect and Declarative Pipelines features this course leans on.

## 2.6 Build — bring the environment up to "ready"

Step 1 — validate and install:

```bash
./lakehouse setup
```

Expected: environment checks pass (Docker, Java, Python, Poetry found), Python
dependencies install, and the Spark JARs download (this is a large, one-time
network step — be patient).

Step 2 — confirm configuration is consistent:

```bash
./lakehouse check-config
```

Expected: credentials in `.env` and the Spark config agree; no mismatches reported.

Step 3 — check status (nothing started yet):

```bash
./lakehouse status
```

Expected: the CLI lists each service and shows it as not-yet-running. That's fine —
we start services layer by layer beginning in Chapter 3.

## 2.7 Checkpoint

You're ready to move on when:

- `./lakehouse setup` completed without errors.
- `./lakehouse check-config` reports consistent credentials.
- `./lakehouse status` runs and prints a service table.
- You know where your `.env` is and that it must never be committed.

## 2.8 Recap & what's next

- The stack needs **Docker, Java 21, Python 3.10+, and Poetry**; plan for ~16 GB RAM.
- **Postgres and the object store run natively on your host**, not in containers —
  remember this when debugging.
- The **`./lakehouse` CLI** is your single control surface for the whole course.
- **Next — Chapter 3, Storage:** start the object store, create the warehouse
  bucket, and lock in Iceberg as the table format.

![Progress: Setup complete, Storage next](../figures/ch02/fig-2.2-progress-setup-done.svg)

**Figure 2.2** — Progress map with **Setup ✓** and **Storage** highlighted next.

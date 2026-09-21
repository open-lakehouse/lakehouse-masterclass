# Chapter 2 — Setup: Prerequisites and the Control CLI

## 2.0 What you'll build

A working environment and the one tool you'll use to drive everything else: the
`./lakehouse` control CLI. By the end you'll have the prerequisites installed, the
project cloned, your configuration in place, and a green `./lakehouse status`.

This is the chapter with the highest "give up" rate in any hands-on course,
because environment setup is where enthusiasm meets reality. So we're going to be
unusually thorough — not because setup is hard, but because a confident setup is
the difference between enjoying the next eleven chapters and fighting your laptop
the whole way. Ten extra minutes here saves hours later.

![Progress: Setup](../figures/ch02/fig-2.1-progress-setup.svg)

**Figure 2.1** — Progress map with **Setup** highlighted.

## 2.1 Learning objectives

By the end of this chapter you can:

- Install the prerequisites (Docker, Java, Python, Poetry) on macOS, Ubuntu, or
  Windows/WSL2 — and explain what each one is *for*.
- Explain the two **host-native** dependencies (Postgres and the object store) and
  why they're not containers.
- Clone the project and create your `.env` configuration.
- Use the `./lakehouse` CLI to validate and inspect the stack, and read its status
  output confidently.

## 2.2 The mental model: one control surface, many services

Before any installs, understand the shape of what you're setting up. A lakehouse
is not one program — it's *many* independent services (an object store, a Spark
cluster, a catalog, Kafka, Airflow, and more) that have to be started in the right
order, pointed at each other, and health-checked. Managing that by hand — a dozen
`docker` commands, a tangle of ports and environment variables — is exactly the
kind of error-prone busywork that makes people hate infrastructure.

So this project follows a principle you'll see in every well-run data platform:
**there is one control surface, and everything goes through it.** That control
surface is the `./lakehouse` script. You will almost never type a raw `docker`
command in this course. Instead you'll say `./lakehouse start spark`,
`./lakehouse status`, `./lakehouse logs airflow` — and the script translates those
into the right underlying operations. This is the "infrastructure as a single
front door" idea, and it's worth internalizing: when a system is complex, the
kindest thing you can build is a simple, consistent way to drive it.

### Under the hood — this is "infrastructure as code," in miniature

The `./lakehouse` script plus the per-service Docker Compose files *are* your
infrastructure, written down as code you can read, version, and re-run. There's no
click-here-then-there ritual to remember and no "works on my machine" mystery —
the environment is defined in files, so anyone who clones the repo gets the exact
same stack. That's the whole promise of infrastructure-as-code, and you get it for
free just by using the CLI instead of running things by hand.

## 2.3 Prerequisites (be honest about hardware)

This whole lakehouse runs on your machine, so set expectations up front. This is
real distributed-systems software running locally; it is not lightweight.

**Table 2.1** — Hardware and software:

| Resource | Minimum | Recommended |
|---|---|---|
| RAM | 8 GB | 16 GB |
| Disk | 20 GB free | 50 GB free |
| CPU | 4 cores | 8 cores |

At 8 GB you can run the stack, but you'll want to start services one at a time and
stop what you're not using (the CLI makes this easy). At 16 GB the whole thing runs
comfortably at once. If you're near the minimum, that's fine — the course is
designed so you only ever *need* a few services running for any given chapter.

Software to install first, and — importantly — *why each one*:

- **Docker** (with Docker Compose) — runs the services. Nearly every component
  (Spark, Kafka, Airflow, the catalog) runs in a container so you don't have to
  install each one natively. Docker is the engine that runs those containers.
- **Java 17+** (Java **21** is required for Spark 4.1, our default) — Spark is a
  JVM application. Even though you'll mostly *drive* Spark from Python, the engine
  itself runs on Java, so the right Java version has to be present.
- **Python 3.10+** — the language of the pipelines, the test-data generator, and
  the tooling. It's also how you talk to Spark (via the Spark Connect client in
  Chapter 4).
- **Poetry** — Python dependency management. It reads the project's declared
  dependencies and installs the exact, consistent set into an isolated environment,
  so you don't pollute your system Python or fight version conflicts.

Supported operating systems: **macOS**, **Ubuntu/Debian**, and **Windows via
WSL2**. On Windows, do everything inside your WSL2 Linux shell — treat it as Ubuntu.
Do **not** try to run the stack from Windows PowerShell directly; the tooling
assumes a Unix shell, and WSL2 gives you exactly that.

### Installing on each OS

The exact commands vary, but the shape is the same everywhere. Rough guide:

- **macOS:** install [Docker Desktop](https://www.docker.com/products/docker-desktop/),
  then use Homebrew for the rest: `brew install openjdk@21 python@3.12 poetry`.
  In Docker Desktop's settings, give it at least 8 GB of memory (Settings →
  Resources) — the default is often too low for Spark.
- **Ubuntu/Debian:** install Docker Engine and the Compose plugin from Docker's
  official apt repository, then `sudo apt install openjdk-21-jdk python3.12` and
  install Poetry with its official installer script.
- **Windows (WSL2):** install WSL2 with an Ubuntu distribution, enable the WSL2
  backend in Docker Desktop, then follow the Ubuntu steps *inside* your WSL2 shell.

After installing, sanity-check each tool exists before going further:

```bash
docker --version
java -version      # should report 21.x for the default Spark 4.1
python3 --version  # 3.10 or newer
poetry --version
```

If any of these four commands errors or reports the wrong version, stop and fix it
now — the CLI's own checks in section 2.6 will fail otherwise, and it's much easier
to diagnose one missing tool here than to untangle a half-configured stack later.

### Under the hood — two dependencies that are NOT containers

Most of the stack runs in Docker, but **two foundational services run natively on
your host**: **PostgreSQL** (the catalog's metadata database) and the **object
store** (covered in Chapter 3). The containers reach them over
`host.docker.internal`, a special hostname that means "the host machine, from
inside a container."

Why not containerize these too? Two reasons. First, they're the most
*stateful* parts of the system — the things you least want to accidentally destroy
when you tear down containers — so keeping them on the host protects your data.
Second, running them natively mirrors how real deployments work: your object store
and your metadata database are almost always *external managed services*, not part
of your compute cluster.

This design trips up a lot of beginners — if a container can't find its database,
it's almost always because Postgres isn't running on the host, or
`host.docker.internal` isn't resolving. We call this out again in Chapter 3,
because it's the single most common setup snag. Commit it to memory now: **when a
service can't reach its dependency, check the two host-native services first.**

## 2.4 Get the code

```bash
git clone <the-course-companion-repo>.git
cd <companion-repo>
```

The project root contains the `./lakehouse` script — your control surface for the
entire course — plus per-service Docker Compose files, the pipelines, and the
test-data generator. Take a moment to look around: `ls` the root and skim the
top-level folders. You don't need to understand it all yet, but knowing roughly
where things live pays off when a later chapter says "edit the pipeline
definition" and you already have a sense of the layout.

## 2.5 Configure your environment

The project ships an example environment file. Copy it and fill in the blanks.

```bash
cp .env.example .env
```

Open `.env` and set the credentials. The important variables you'll see:

**Table 2.2** — Key environment variables (Storage credentials, detailed in Ch 3):

| Variable | Purpose |
|---|---|
| `S3_ENDPOINT` | Where the object store listens (e.g. `http://host.docker.internal:8333`) |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | Object-store credentials |
| `S3_BUCKET` | The bucket name (e.g. `lakehouse`) |
| `S3_WAREHOUSE` | The warehouse path (e.g. `s3a://lakehouse/warehouse`) |
| `POSTGRES_*` | Catalog metadata database connection |

Notice that `S3_ENDPOINT` uses `host.docker.internal` — that's the host-native
object store from the last section. The value you put here is the same address the
Spark containers will use to reach storage, which is why consistency between
`.env` and the Spark config matters (the CLI checks exactly this in section 2.6).

> Security note: `.env` holds secrets and is gitignored. Never commit it. This is
> the course's first **security** practice — keep credentials out of version
> control. It's a small habit that prevents a very large class of real-world
> disasters (leaked keys in public repos are one of the most common security
> incidents in the industry).

## 2.6 Meet the control CLI

Everything in this course runs through one command. Get its help first:

```bash
./lakehouse help
```

**Table 2.3** — The commands you'll use most:

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

Two commands deserve special mention because you'll lean on them constantly:

- **`./lakehouse status`** is your dashboard. Any time something feels off, this is
  the first thing to run. It tells you which services are up, which are down, and
  which are unhealthy. Its `--json` mode is handy when you want to check status
  from a script.
- **`./lakehouse logs [service]`** is your debugger. When a service is unhealthy,
  its logs almost always say why. Getting comfortable reading logs is one of the
  most valuable habits in all of data engineering — the answer is nearly always in
  there.

> Note the `--version 4.0|4.1` option: it selects the Spark version. We use the
> default (**4.1**, on Java 21) throughout, because it's the version with the
> modern Spark Connect and Declarative Pipelines features this course leans on.

## 2.7 Build — bring the environment up to "ready"

Step 1 — validate and install:

```bash
./lakehouse setup
```

Expected: environment checks pass (Docker, Java, Python, Poetry found), Python
dependencies install, and the Spark JARs download (this is a large, one-time
network step — be patient; it can take several minutes on a normal connection).

**What just happened?** `setup` did three things: confirmed your four prerequisites
are present and the right versions, used Poetry to install the project's Python
dependencies into an isolated environment, and downloaded the Spark engine plus the
connector JARs (the plugins that let Spark talk to the object store and Iceberg).
Those JARs are large, which is why this step is slow the first time and instant
afterward.

Step 2 — confirm configuration is consistent:

```bash
./lakehouse check-config
```

Expected: credentials in `.env` and the Spark config agree; no mismatches reported.
This catches the classic mistake of setting a password in one place but not the
other — a mismatch here would surface much later as a confusing "access denied"
when Spark tries to reach storage, so it's worth the ten seconds now.

Step 3 — check status (nothing started yet):

```bash
./lakehouse status
```

Expected: the CLI lists each service and shows it as not-yet-running. That's fine —
we start services layer by layer beginning in Chapter 3. Seeing an all-stopped
status table is actually the *correct* end state for this chapter; you've built the
control surface, not the services.

## 2.8 Troubleshooting

The most common setup snags and what they mean:

- **`./lakehouse setup` fails on the Java check.** You have Java, but the wrong
  version, or multiple Javas and the wrong one is first on your `PATH`. Run
  `java -version`; if it's not 21.x, install/select Java 21. On macOS, Homebrew's
  `openjdk@21` sometimes needs to be symlinked or added to `PATH` — the install
  output tells you how.
- **`docker` command works but `setup` says Docker isn't running.** The Docker CLI
  is installed but the Docker *daemon/desktop* isn't started. Launch Docker Desktop
  (macOS/Windows) or `sudo systemctl start docker` (Linux), then retry.
- **The Spark JAR download stalls or fails.** It's a large download; a flaky
  connection can interrupt it. Re-run `./lakehouse setup` — it resumes rather than
  re-downloading everything.
- **`check-config` reports a mismatch.** A credential differs between `.env` and the
  Spark config. Re-open `.env`, confirm each value, and re-run. Copy-paste errors in
  the secret key are the usual culprit.
- **Everything installs but you're on Windows PowerShell.** Stop — move into your
  WSL2 Ubuntu shell and run everything there. The tooling needs a Unix shell.

Whenever a step fails, the pattern is the same: read the error, run
`./lakehouse status` and `./lakehouse logs` to see the details, and fix one thing
at a time. Resist the urge to change five things at once — you'll lose track of
what actually fixed it.

## 2.9 Checkpoint

You're ready to move on when:

- `./lakehouse setup` completed without errors.
- `./lakehouse check-config` reports consistent credentials.
- `./lakehouse status` runs and prints a service table (all stopped is correct).
- You know where your `.env` is and that it must never be committed.
- You can name the two host-native services and say why they're not containers.

## 2.10 Try it yourself

1. **Read the help end to end.** Run `./lakehouse help` and read every command,
   even the ones we haven't used. You'll recognize them as we go, and you'll know
   the tool has more to offer than the handful we lean on.
2. **Break it on purpose.** Temporarily rename a value in `.env` to something wrong,
   run `./lakehouse check-config`, and read the error. Then fix it. Deliberately
   causing (and reading) an error is the fastest way to learn what a healthy state
   looks like by contrast.
3. **Inspect the JSON.** Run `./lakehouse status --json` and look at the structure.
   Imagine you were writing a script to alert you when a service goes down — which
   field would you check?
4. **Find the Compose files.** Locate the per-service Docker Compose files in the
   repo and open one. You don't need to understand every line — just confirm to
   yourself that "the infrastructure is written down as code," as section 2.2
   claimed.

## 2.11 Check your understanding

- Why does this project insist on a single control CLI instead of raw `docker`
  commands? What does that buy you?
- Which two services run natively on the host rather than in containers, and what
  are the two reasons given for that choice?
- What does `host.docker.internal` mean, and why does the object-store endpoint use
  it?
- When a containerized service can't reach its database, what should you check
  first?

## 2.12 Recap & what's next

- The stack needs **Docker, Java 21, Python 3.10+, and Poetry**; plan for ~16 GB
  RAM (8 GB works if you run services one at a time).
- There is **one control surface** — the `./lakehouse` CLI — and everything goes
  through it. `status` is your dashboard; `logs` is your debugger.
- **Postgres and the object store run natively on your host**, not in containers —
  remember this when debugging.
- The infrastructure is **written down as code**, so every clone gets the same
  stack.
- **Next — Chapter 3, Storage:** start the object store, create the warehouse
  bucket, and lock in Iceberg as the table format.

![Progress: Setup complete, Storage next](../figures/ch02/fig-2.2-progress-setup-done.svg)

**Figure 2.2** — Progress map with **Setup ✓** and **Storage** highlighted next.

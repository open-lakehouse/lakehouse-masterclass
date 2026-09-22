# Chapter 2: Setup

Chapter 1 was decisions. This is where you start running things. By the end you'll have the foundation in place on your machine: the tools installed, the project laid out, and Docker confirmed working, ready to add the first service to.

A lakehouse is a handful of services (an object store, a Spark cluster, a catalog and its database, and later Kafka and an orchestrator), their configuration, and the wiring that lets them find each other. You're going to build that yourself, one service at a time, so you understand exactly what's running and can fix it when something breaks. The services run as containers, and Docker Compose is what defines and runs them, so that's the tool at the center of this chapter.

This chapter does the groundwork the rest of the build stands on: install the prerequisites, lay out the project directory, set up configuration and secrets properly before there are any secrets to leak, and confirm Docker actually works. You won't stand up a lakehouse service yet. That starts in the next chapter with the object store. What you'll have when you finish here is a clean, working foundation to build on.

![Progress: Setup](../figures/ch02/fig-2.1-progress-setup.svg)

**Figure 2.1**. Progress map with **Setup** highlighted.

## Learning objectives

By the end of this chapter you'll be able to:

- Install the prerequisites (Docker with Compose, Python, and the JDK) and say what each one is for in the stack.
- Explain what Docker and Docker Compose actually give you here: reproducible services defined in a file, rather than software installed by hand on your machine.
- Lay out a project directory that will hold the service definitions, configuration, and pipeline code you build over the rest of the course.
- Keep configuration and secrets in a `.env` file that stays out of version control, and explain why that habit matters before you have any real credentials.
- Confirm your setup works by bringing up a throwaway container with Compose, so you know the pattern the later chapters rely on.

## The mental model: services in containers, defined in a file

A lakehouse is several independent services that have to run at once and find each other: an object store, a Spark cluster, a catalog and its database, and later Kafka and an orchestrator. You could install each one directly on your machine, but that road is painful and you've probably been down it: version conflicts, half-uninstalled leftovers, and a setup that works on your laptop and nowhere else. It also doesn't resemble how any of this runs in production.

So you don't install these services on your host. You run each one as a **container**: a packaged, isolated copy of the software with its own dependencies baked in, that runs the same way on any machine. Docker is what runs containers. That's the first tool you install, and for most of this build it's the only thing that actually touches your host system. The object store, Spark, the catalog, Kafka, they all run as containers on top of Docker, so your machine stays clean and every reader ends up with the same stack.

Running one container by hand is a long command with a lot of flags. Running six of them, wired together with shared networks and consistent settings, is unmanageable that way. That's what **Docker Compose** solves. Compose lets you describe your services in a single file, `docker-compose.yml`, one block per service, saying which image it runs, what ports it exposes, and how it connects to the others. Then one command brings the whole set up, and another tears it down. The file is the source of truth: it's readable, you keep it in version control, and it *is* your infrastructure, written down rather than assembled by memory.

That is the pattern for the rest of the course. Each chapter adds one service to this Compose file, brings it up, and confirms it's healthy, so by the end you have the whole stack described in one file you wrote and understand line by line. This chapter just gets Docker and Compose in place and proves they work.

## Prerequisites

The point of running everything in containers is that your host stays clean, so the list of things you install directly is short. Three tools:

- **Docker, with the Compose plugin.** This is the one that matters. It runs every service in the stack. Install Docker Desktop on macOS or Windows, or Docker Engine on Linux; recent versions include Compose as `docker compose`. Everything else in the course runs on top of this.
- **Python 3.10 or newer.** You'll write pipeline code and talk to Spark from Python, from your host, so you need it locally. This is the language you'll actually work in day to day.
- **Git.** To version your project: the Compose file, config, and pipeline code you build. Assume you have it; if not, install it.

Notice what's *not* here. You don't install Spark, or a JVM, or a database on your host. Spark is a JVM application, but its Java lives inside the Spark container you'll define in the Compute chapter, so there's nothing to set up for it now. The catalog's database is a container too. Keeping these off your host is the whole point of the container approach: the only things touching your machine are Docker, Python, and Git.

A note on hardware, because this is real distributed-systems software running locally. Plan for around 16 GB of RAM to run the full stack comfortably; 8 GB works if you bring services up one at a time and stop what you're not using, which the one-service-per-chapter structure makes easy. Give Docker a generous memory allowance in its settings (on Docker Desktop, under Resources), since the default is often too low for Spark. Budget 20 to 50 GB of free disk for container images and data.

On operating systems: macOS, Linux, and Windows via WSL2 all work. On Windows, do everything inside your WSL2 Linux shell and treat it as Linux; don't run the stack from PowerShell directly, since the tooling assumes a Unix shell.

After installing, confirm the tools are present:

```bash
docker --version
docker compose version
python3 --version
git --version
```

If any of these errors, fix it before going further. A missing tool is far easier to diagnose now than halfway through bringing up your first service.

## Lay out the project

You're building this from nothing, so start with an empty directory and put it under version control:

```bash
mkdir open-lakehouse && cd open-lakehouse
git init
```

You'll grow a specific structure over the course, but a little intention now saves cleanup later. Create these top-level folders:

```bash
mkdir compose config pipelines data
```

What each is for:

- **`compose/`** holds the service definitions. You can keep everything in one `docker-compose.yml` at the root, but as the stack grows it's cleaner to split it, one file per service, and combine them. Either way, this is where your infrastructure lives.
- **`config/`** holds service configuration that isn't secret: the settings files each service reads. It gets populated as you add services.
- **`pipelines/`** is where your data pipelines will live. You won't hand-build its internal structure: in the Transformation chapter you'll run Spark Declarative Pipelines' own initializer (`spark-pipelines init --name <project>`), which scaffolds a project subdirectory here containing a `spark-pipeline.yml` spec and a `transformations/` folder with example definitions. For now it's just an empty home waiting for that.
- **`data/`** is a local scratch space for sample data and anything you don't want committed.

None of this is load-bearing yet. The point is that you have a home for each kind of thing you'll create, so when a later chapter says "add the Spark service" or "initialize the pipeline project," you already know where it goes. We'll create the actual `docker-compose.yml` in the next chapter, when there's a first service to put in it.

## Configuration and secrets

Services need configuration, and some of it is sensitive: access keys, database passwords. The rule you adopt now, before you have a single real credential, is that secrets never go in version control. Getting this habit in place while the stakes are zero is the point; it's much harder to retrofit after a key has already been committed and lives in your git history forever.

Two kinds of configuration, kept separate:

- **Non-secret config** (ports, service names, non-sensitive settings) can live in files you commit, so anyone with your project gets a working setup.
- **Secrets** (credentials, keys) go in a single `.env` file that you never commit. Services and Compose read values from it at startup.

Create the `.env` file and, in the same breath, make sure git will ignore it:

```bash
touch .env
echo ".env" >> .gitignore
echo "data/" >> .gitignore
git add .gitignore
git commit -m "Ignore secrets and local data"
```

That is deliberately the first thing you commit: the rule that keeps secrets out. The `.env` file is empty for now; you'll add values to it as you bring up services that need credentials, starting with the object store's access keys in the next chapter. Compose automatically reads a `.env` file sitting next to your `docker-compose.yml`, so any variable you define there is available to your service definitions without extra wiring.

One habit worth adopting alongside this: commit an example file, `.env.example`, that lists the *names* of the variables with placeholder values, and do commit that one. It documents what configuration the project expects without exposing any real secret, so someone setting up the project later knows exactly what to fill in.

## Prove it works

You won't stand up a real lakehouse service yet, that starts next chapter, but before you finish here you want proof that Docker and Compose actually run on your machine. It's worth catching a broken Docker install now, with a trivial throwaway service, rather than while you're also trying to learn the object store.

Create a temporary `docker-compose.yml` at the project root with a single tiny service:

```yaml
services:
  hello:
    image: hello-world
```

Bring it up:

```bash
docker compose up
```

You should see Docker pull the `hello-world` image and print a "Hello from Docker!" message confirming your installation is working, then the container exits. If you see that message, Docker can pull images and Compose can read your file and run a service, which is everything the rest of the course depends on.

Now tear it down and remove the throwaway file:

```bash
docker compose down
rm docker-compose.yml
```

You deleted it because the real `docker-compose.yml` gets created in the next chapter with your first actual service, the object store. This one was only ever a smoke test.

**What just happened?** You wrote a service definition in a Compose file, and `docker compose up` read it, pulled the image, and ran the container; `docker compose down` stopped and cleaned it up. That pull, run, stop cycle is the exact pattern every later chapter uses, just with real services and more of them. If it worked for `hello-world`, your foundation is solid.

If `docker compose up` failed, the usual causes are: Docker isn't actually running (start Docker Desktop, or `sudo systemctl start docker` on Linux), you're in a Windows PowerShell shell instead of WSL2, or a networking restriction is blocking the image pull. Fix the cause and re-run; don't move on until you see the hello message.

## Checkpoint

You're ready to move on when:

- Docker, Compose, Python, and Git are installed and their version commands all work.
- You have a project directory under version control, with the `compose/`, `config/`, `pipelines/`, and `data/` folders laid out.
- Your first commit is a `.gitignore` that excludes `.env` and `data/`, and you understand why secrets stay out of version control.
- `docker compose up` on the throwaway `hello-world` service printed the hello message, and you tore it back down.

## Try it yourself

1. **Read a Compose file's shape.** Look up the reference for a `docker-compose.yml` service block and note the common keys (`image`, `ports`, `environment`, `volumes`, `depends_on`). You'll use every one of these as you add real services.
2. **Break the smoke test on purpose.** Put a typo in the throwaway Compose file (misspell `image`, say), run `docker compose up`, and read the error. Then fix it. Reading a failure now teaches you what a healthy run looks like by contrast.
3. **Reference a secret from Compose.** Add a line like `GREETING=hello` to your `.env`, then write a tiny Compose service that echoes `${GREETING}`, and confirm the value flows through. This is exactly how service credentials will reach your real services later.

## Check your understanding

- Why run each service in a container instead of installing it on your host? Give two concrete reasons.
- What does Docker Compose add on top of Docker, and why does that matter once you have more than one service?
- Why is the very first commit a `.gitignore` that excludes `.env`? What problem does that habit prevent?
- What does the throwaway `hello-world` run actually prove about your setup?

## Recap and what's next

- The only tools on your host are **Docker (with Compose), Python, and Git**; everything else runs in containers. Plan for around 16 GB of RAM to run the full stack comfortably.
- Services run as **containers defined in a Compose file**, which is your infrastructure written down: readable, versioned, reproducible.
- Secrets live in a gitignored **`.env`**, and keeping them out of version control is the first habit you commit.
- You proved Docker and Compose work with a throwaway service, and laid out a project directory ready for its first real service.
- **Next, Chapter 3, Storage:** create your first real service, an S3-compatible object store, in the `docker-compose.yml` you'll start for real, and set up the warehouse it holds.

![Progress: Setup complete, Storage next](../figures/ch02/fig-2.2-progress-setup-done.svg)

**Figure 2.2**. Progress map with **Setup ✓** and **Storage** highlighted next.

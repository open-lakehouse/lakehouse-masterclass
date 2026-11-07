# Chapter 4.5: The Spark Image

Chapter 4 ran Spark from the official `apache/spark` image, and for everything Chapter 4 does that image is enough. The next chapters ask two things of the cluster that it can't give. Chapter 5 creates Iceberg tables, which needs an Iceberg runtime built for Spark 4.3, and no Iceberg release ships one yet. Chapter 8 runs Python code on the cluster itself, which needs a Python on the workers that matches yours. This short chapter builds the course's own Spark image to supply both, swaps your three Spark services over to it, and checks the result.

It's also the place to learn how an image is put together, because sooner or later every platform team builds its own engine image: to pin a connector, to add a library the base image lacks, or to carry a fix before it's released. The habits here, pinning, checksums, multi-stage builds and knowing which classloader loads a jar, are what keep that image from becoming the mystery box nobody dares rebuild.

## Learning objectives

By the end of this chapter you'll be able to:

- Show, with a failing job, why the official image can't run this course's Python code on the cluster.
- Read a multi-stage Dockerfile and say what each stage contributes to the final image.
- Explain why jars that call each other belong on Spark's own classpath, loaded by one classloader, rather than fetched through `spark.jars.packages`.
- Build the image, point Compose at it, and verify the Iceberg runtime, the S3 jars and the worker Python are in place.

## What the official image is missing

Two gaps, and you can see both from the stack you have right now.

**Python on the cluster.** Most of your Python code never leaves your laptop: the client turns DataFrame calls into a plan and the JVM runs it. A Python UDF is different. Spark ships the function to the cluster and runs it in a Python worker process that the Spark executor starts, and PySpark refuses to run it unless that worker's Python minor version equals the client's. Try it, with the Chapter 4 stack still up:

```python
from pyspark.sql import functions as F
from lakehouse.session import get_spark

spark = get_spark("python-check")

@F.udf("string")
def worker_python(_):
    import sys
    return sys.version.split()[0]

spark.range(1).select(worker_python("id")).first()[0]
```

On the official image it fails, and the last lines of the traceback say why:

```
pyspark.errors.exceptions.base.PySparkRuntimeError: [PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.10 than that in driver: 3.12, PySpark cannot run with different minor versions.
Please check environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON are correctly set.
```

The "driver" here is your client's Python, 3.12 (Chapter 2). The official image with Python, `apache/spark:4.3.0-scala2.13-java21-python3-ubuntu`, is built on Ubuntu 22.04, whose Python is 3.10, and it has no pyarrow or pandas, which Arrow-based Python code on the workers needs. The stateful processor in Chapter 8 runs exactly this way, so it would fail the same way.

**An Iceberg runtime for Spark 4.3.** Iceberg publishes one Spark runtime jar per Spark minor version. List what Maven Central has:

```bash
curl -s https://repo1.maven.org/maven2/org/apache/iceberg/ | grep -o 'iceberg-spark-runtime-4\.[0-9]_2\.13' | sort -u
# iceberg-spark-runtime-4.0_2.13
# iceberg-spark-runtime-4.1_2.13
```

There is no `4.3`. Support for Spark 4.3 exists as an open pull request, [apache/iceberg#18749](https://github.com/apache/iceberg/pull/18749), so the course builds the runtime from that pull request, pinned to one commit, until a release ships it ([course issue #8](https://github.com/open-lakehouse/lakehouse-masterclass/issues/8) tracks the swap). A runtime built for Spark 4.1 is not a substitute: Iceberg keeps a separate module per Spark version precisely because the Spark APIs it plugs into change between minor releases.

## How the image is built

> **Multi-stage build** *(canonical term)*: a Dockerfile with more than one `FROM`. Each stage is its own image; a later stage copies only the files it needs out of an earlier one, so the tools used to build something (a JDK, Gradle, git) never reach the final image.

The course image uses two stages. The first is a build environment that produces jars. The second starts from the official Spark image and adds those jars and a Python. Only the second stage becomes `open-lakehouse-spark:4.3.0`.

**Step 1: write the Dockerfile.** Create `docker/spark/Dockerfile`:

```dockerfile
# docker/spark/Dockerfile
# The course's Spark image: Apache Spark 4.3.0 plus the two things the official
# image lacks for this stack.
#
#  1. An Iceberg runtime for Spark 4.3. No Iceberg release ships one yet, so the
#     first stage builds it from the pull request that adds Spark 4.3 support
#     (apache/iceberg#18749), pinned to one commit. When a release ships
#     iceberg-spark-runtime-4.3_2.13, this stage becomes one line in
#     spark-defaults.conf (spark.jars.packages) and the stage goes away.
#  2. Python 3.12 with pyarrow, pandas and protobuf. Python code that runs on
#     the cluster (UDFs, and the stateful processor in Chapter 8) executes in a
#     Python worker here, and Spark Connect requires that worker to run the same
#     Python minor version as your client.

# ---- stage 1: build the Iceberg runtime jar (several minutes, once) ----------
FROM eclipse-temurin:17-jdk AS iceberg
ARG ICEBERG_COMMIT=b1eab64f8d304344a22301084f841fa69a376d86
RUN apt-get update && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN git init -q . \
 && git fetch -q --depth 1 https://github.com/apache/iceberg.git "$ICEBERG_COMMIT" \
 && git checkout -q FETCH_HEAD
RUN ./gradlew --no-daemon -q -x test \
      -DsparkVersions=4.3 -DscalaVersion=2.13 -DflinkVersions= -DkafkaVersions= \
      :iceberg-spark:iceberg-spark-runtime-4.3_2.13:shadowJar \
 && cp spark/v4.3/spark-runtime/build/libs/iceberg-spark-runtime-4.3_2.13-*.jar \
       /iceberg-spark-runtime-4.3_2.13.jar
# S3 access for both Iceberg (S3FileIO) and Hadoop's S3A filesystem (Spark
# checkpoints, raw files). One AWS SDK for both, on Spark's own classpath:
# when the two arrive through different classloaders, or as two SDK versions,
# calls between them fail with NoClassDefFoundError or NoSuchMethodError.
# hadoop-aws 3.5.0 matches the Hadoop that Spark 4.3.0 ships; SDK 2.56.0 is the
# one this Iceberg commit builds against (2.x stays backward compatible).
WORKDIR /jars
RUN set -e; M=https://repo1.maven.org/maven2; \
    curl -fsSLO $M/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar; \
    curl -fsSLO $M/software/amazon/awssdk/bundle/2.56.0/bundle-2.56.0.jar; \
    curl -fsSLO $M/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar; \
    printf '%s\n' \
      "66bd9eefb938b1a1657487324b63c13df2d8b17164a1dd24e0ef37b905bc5baa  hadoop-aws-3.5.0.jar" \
      "ae515d6745874faa1942aa47a2750d4ba333609db31457ac1e61f7fe91fc8985  bundle-2.56.0.jar" \
      "4959a9747cbb479c73c3ffbcd479bee5b014f8b2880994585a8693fb9d24675e  analyticsaccelerator-s3-1.3.1.jar" \
      | sha256sum -c -; \
    cp /iceberg-spark-runtime-4.3_2.13.jar .

# ---- stage 2: the image every Spark service runs --------------------------
FROM apache/spark:4.3.0-scala2.13-java21-ubuntu
USER root
COPY --from=ghcr.io/astral-sh/uv:0.11.10 /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN uv venv --python 3.12 /opt/venv \
 && uv pip install --python /opt/venv/bin/python --no-cache \
      pyarrow==20.0.0 pandas==2.3.3 numpy==2.5.4 protobuf==6.33.6 \
      grpcio==1.84.0 grpcio-status==1.84.0 googleapis-common-protos==1.75.5 \
      zstandard==0.25.0
# Spark starts its Python workers with this interpreter.
ENV PYSPARK_PYTHON=/opt/venv/bin/python
COPY --from=iceberg /jars/ /opt/spark/jars/
USER spark
```

Each part is there for a reason you can name:

- **Stage 1 builds the Iceberg runtime.** It starts from a JDK 17 image, fetches the one pinned commit of apache/iceberg#18749 (a shallow fetch, so only that commit's files come down), and runs Gradle for just the Spark 4.3, Scala 2.13 runtime jar, skipping the tests and the Flink and Kafka Connect modules. Pinning the commit means everyone builds the same jar, and the jar's own metadata says which commit it came from (Chapter 8 shows it in a snapshot summary: `Apache Iceberg b1eab64`). When an Iceberg release includes the 4.3 runtime, this whole step becomes a checksummed download like the ones below.
- **The S3 jars, with checksums.** `hadoop-aws` is Hadoop's S3A connector, the code behind every `s3a://` path (Spark checkpoints, raw files), and it must match the Hadoop that Spark ships: Spark 4.3.0 includes `hadoop-client-api-3.5.0.jar`, so this is `hadoop-aws` 3.5.0. `analyticsaccelerator-s3` 1.3.1 is a library `hadoop-aws` 3.5.0 depends on. `bundle-2.56.0.jar` is the AWS SDK v2, which both S3A and Iceberg's own S3 client (`S3FileIO`) call. The `sha256sum -c` line stops the build if any download differs from the file the course was verified with.
- **One AWS SDK, on Spark's own classpath.** All of these jars land in `/opt/spark/jars`, next to Spark's own. In Chapter 4, `hadoop-aws` came through `spark.jars.packages` instead, and that was fine while nothing else used the AWS SDK. Iceberg does, and the next section shows what goes wrong when the two arrive by different routes.
- **Stage 2 is the image every Spark service runs.** It starts from the official image without Python, `apache/spark:4.3.0-scala2.13-java21-ubuntu`, copies in `uv` (a Python installer, from its own pinned image), and uses it to create a Python 3.12 virtual environment at `/opt/venv` with pinned pyarrow, pandas, protobuf and gRPC packages. `PYSPARK_PYTHON` tells Spark to start its Python workers with that interpreter. The pins are the versions the course's client environment was verified with, so both ends of a Python UDF run the same libraries.
- **`USER root`, then `USER spark`.** Installing needs root; the last line switches back to the unprivileged `spark` user the official image runs as.

### Under the hood: why the jars can't come through `spark.jars.packages`

`spark.jars.packages` is convenient: list Maven coordinates and Spark downloads them at startup. But those jars are loaded by a separate, child classloader, which can see Spark's own classes while Spark's own classes can't see it. Code in `/opt/spark/jars` that calls into a package therefore can't find it, and two copies of a library in the two places can both get loaded, each half of a program picking up a different one. On this stack, both arrangements that look reasonable fail:

- **Iceberg runtime baked in, its AWS SDK (`iceberg-aws-bundle`) through packages.** The first table access fails with `Cannot initialize FileIO implementation org.apache.iceberg.aws.s3.S3FileIO`, caused by `NoClassDefFoundError: software/amazon/awssdk/core/exception/SdkException`. The runtime, on Spark's classpath, can't see the SDK in the child classloader.
- **`iceberg-aws-bundle` baked in, `hadoop-aws` through packages.** Ivy resolves `hadoop-aws` 3.5.0 together with the SDK version it was built against, 2.35.4, so two SDK versions are now loaded. Tables work, but in Chapter 8 a streaming query that checkpoints its state over S3A fails with `NoSuchMethodError: 'software.amazon.awssdk.thirdparty.org.slf4j.Logger software.amazon.awssdk.utils.Logger.logger()'`: S3A called a method of one SDK version on a class loaded from the other.

The image avoids both by giving every caller the same single copy: one SDK, 2.56.0, the version this Iceberg commit builds against, loaded by the same classloader as everything that calls it. S3A was built against 2.35.4 and runs on 2.56.0 because AWS keeps SDK 2.x releases backward compatible. `spark.jars.packages` keeps the one jar that calls no AWS code: the Kafka connector.

**Step 2: build it.**

```bash
docker compose build spark-connect
docker image ls open-lakehouse-spark
# REPOSITORY             TAG     IMAGE ID       CREATED          SIZE
# open-lakehouse-spark   4.3.0   f646e30bceb7   ...       2.22GB
```

This takes a while the first time: Gradle compiles Iceberg, which takes several minutes on a laptop. Later builds reuse the cached layers and finish in seconds, unless you change a line above them. The image is about twice the size of its base (`apache/spark:4.3.0-scala2.13-java21-ubuntu` is about 1 GB): roughly 380 MB of Python and 760 MB of jars, most of it the AWS SDK bundle, which carries every AWS service's client. `docker image history open-lakehouse-spark:4.3.0` shows the size of each layer.

**Step 3: point the Spark services at it.** In `docker-compose.yml`, replace the `image:` line of `spark-master`, `spark-worker` and `spark-connect` with these two lines (the rest of each service stays as it is):

```yaml
    build: ./docker/spark
    image: open-lakehouse-spark:4.3.0
```

`build:` tells Compose where the Dockerfile lives, and `image:` names the result, so all three services share the one image you just built. `docker compose up` uses the tagged image rather than building again; after you edit the Dockerfile, rebuild with `docker compose build spark-connect`.

**Step 4: update `spark-defaults.conf`.** S3A now comes from the image, so take `hadoop-aws` out of `spark.jars.packages`; and since Iceberg is on the classpath, turn on its SQL extensions. The file becomes:

```properties
# config/spark/spark-defaults.conf  (read by the Connect server at startup)

# The Kafka connector (Chapter 8). Iceberg, S3A and the AWS SDK are baked into
# the image instead (docker/spark/Dockerfile).
spark.jars.packages      org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0
# The image runs as user "spark" whose HOME is /nonexistent, so point Ivy (the
# jar downloader) at a writable directory or the server dies at startup.
spark.jars.ivy           /tmp/.ivy2
spark.sql.extensions     org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
# Every timestamp without an offset is read and bucketed as UTC, whatever the
# server's or your laptop's clock says. Without this, to_date() near midnight
# depends on the machine the job ran on.
spark.sql.session.timeZone  UTC

# Reach SeaweedFS over the S3 API, with path-style addressing and env-var creds.
spark.hadoop.fs.s3a.endpoint                  http://seaweedfs:8333
spark.hadoop.fs.s3a.path.style.access         true
spark.hadoop.fs.s3a.aws.credentials.provider  software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider

# Iceberg's S3FileIO and the AWS SDK v2 both require a region, even against
# SeaweedFS; it is supplied by the AWS_REGION env var on the Spark services.
spark.sql.catalog.iceberg.s3.region           us-east-1
```

What changed since Chapter 4:

- **`spark.jars.packages` holds only the Kafka connector.** Leaving `hadoop-aws` in it would bring the second AWS SDK described above.
- **`spark.sql.extensions`** loads `IcebergSparkSessionExtensions`, which adds Iceberg's SQL to Spark: `CALL` procedures (rollback in Chapter 5, maintenance in Chapter 9), `MERGE`-related rewrites, and the branch and tag DDL. It's a static setting, so it belongs here, read at server start.
- **`spark.sql.catalog.iceberg.s3.region`** gives Iceberg's S3 client a region. The AWS SDK refuses to work without one, even against SeaweedFS, which doesn't care; the value is supplied to the Spark services as `AWS_REGION` too. The rest of the catalog's settings come in Chapter 5.

**Step 5: recreate the Spark services and wait for the server.**

```bash
docker compose up -d
until docker compose logs --since 2m spark-connect | grep -q "Spark Connect server started"; do sleep 3; done
docker compose images | grep spark
# ...-spark-connect-1   open-lakehouse-spark   4.3.0   linux/amd64   f646e30bceb7   2.22GB   ...
# ...-spark-master-1    open-lakehouse-spark   4.3.0   linux/amd64   f646e30bceb7   2.22GB   ...
# ...-spark-worker-1    open-lakehouse-spark   4.3.0   linux/amd64   f646e30bceb7   2.22GB   ...
```

Compose sees that the three services' image changed and recreates them; SeaweedFS is untouched.

**Step 6: check what's inside.** First the jars, on the server's own classpath:

```bash
docker compose exec spark-connect ls /opt/spark/jars | grep -E 'iceberg|hadoop-aws|bundle|analytics'
# analyticsaccelerator-s3-1.3.1.jar
# bundle-2.56.0.jar
# hadoop-aws-3.5.0.jar
# iceberg-spark-runtime-4.3_2.13.jar
```

Then run the UDF from the start of the chapter again, in a fresh Python session:

```python
spark.range(1).select(worker_python("id")).first()[0]
# '3.12.13'
spark.conf.get("spark.sql.extensions")
# 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'
```

The same code that failed now returns the worker's Python version, 3.12, the same minor version as your client, and the server has loaded Iceberg's extensions.

**What just happened?** You replaced the engine's image without touching a line of application code: the client still connects to `sc://localhost:15002`, and your `lakehouse` package doesn't know anything changed. That separation is the point. The image is platform: which Spark, which connectors, which Python. Your code is the application on top. When Iceberg releases the 4.3 runtime, or a new Spark patch release comes out, the change lands in `docker/spark/Dockerfile`, gets rebuilt and verified once, and every chapter keeps working.

## Troubleshooting

- **`docker compose build spark-connect` fails in stage 1.** The Iceberg build needs network access to GitHub and Maven Central, and several gigabytes free on the disk Docker uses. A `sha256sum: WARNING: ... computed checksum did NOT match` line means a jar download was cut short or changed; re-run the build, and Docker resumes from the last cached layer.
- **The services still run the official image after `up`.** Check `docker compose images`. If the `image:` lines still name `apache/spark`, Step 3 wasn't applied to all three services; a mix of images across master, worker and Connect server is worse than either one.
- **`[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.12 than that in driver: 3.11`** (or another pair). The workers now run 3.12; your client doesn't. Recreate `.venv` with `python3.12` (Chapter 2).
- **`Cannot initialize FileIO implementation org.apache.iceberg.aws.s3.S3FileIO` with `NoClassDefFoundError: software/amazon/awssdk/core/exception/SdkException`** (on the first table access, in Chapter 5). An AWS SDK reached Spark through `spark.jars.packages` while the Iceberg runtime sits on Spark's own classpath. Keep `spark.jars.packages` to the Kafka connector.
- **`NoSuchMethodError` on `software.amazon.awssdk.utils.Logger.logger()`** (in Chapter 8, when a stateful stream checkpoints over S3A). Two AWS SDK versions are loaded, typically because `hadoop-aws` is still in `spark.jars.packages`. Remove it and restart `spark-connect`.
- **`WARN SparkSession: Cannot use org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions to configure session extensions`, with a `ClassNotFoundException`, in the `spark-connect` log.** The `spark.sql.extensions` line is in the config but the service runs an image without the Iceberg runtime. The server keeps running without Iceberg's extensions, so this only shows up later, when an Iceberg statement fails. Check `docker compose images`.

## Checkpoint

- `docker image ls open-lakehouse-spark` lists the `4.3.0` image, and `docker compose images` shows all three Spark services running it.
- `/opt/spark/jars` in `spark-connect` holds the Iceberg runtime, `hadoop-aws` 3.5.0, the AWS SDK bundle and `analyticsaccelerator-s3`.
- The UDF that failed with `PYTHON_VERSION_MISMATCH` on the official image returns `3.12.x`.
- `spark.jars.packages` lists only the Kafka connector, and `spark.sql.extensions` names Iceberg's extensions.
- You can explain why a jar on Spark's classpath can't see a jar from `spark.jars.packages`.

## Try it yourself

1. **Read the layers.** Run `docker image history open-lakehouse-spark:4.3.0` and match each large layer to a line of the Dockerfile. Which layer would a change to the pinned Python packages invalidate, and which would it leave cached?
2. **Find the commit in the jar.** The runtime records where it was built from. Copy it out of the container (`docker compose cp spark-connect:/opt/spark/jars/iceberg-spark-runtime-4.3_2.13.jar .`) and print `unzip -p iceberg-spark-runtime-4.3_2.13.jar iceberg-build.properties`: `git.commit.id` is the commit the Dockerfile pins. Then find the same id in a table's snapshot summary in Chapter 8.
3. **Break it on purpose, then fix it.** Add `org.apache.hadoop:hadoop-aws:3.5.0` back to `spark.jars.packages`, restart `spark-connect`, and list what Ivy downloaded (`docker compose exec spark-connect ls /tmp/.ivy2/jars`). Find the second AWS SDK in that list. Put the file back afterwards.

## Check your understanding

- What does a Python UDF need from the cluster that a DataFrame transformation doesn't?
- What does stage 1 of the Dockerfile contribute to the final image, and what does it leave behind?
- Why does the course check a SHA-256 for each downloaded jar, and pin a commit for the one it builds?
- Why can't the Iceberg runtime in `/opt/spark/jars` use an AWS SDK fetched through `spark.jars.packages`?

## Recap and what's next

- The official Spark 4.3.0 image runs Chapter 4, but its workers run Python 3.10 without pyarrow, and no Iceberg release has a Spark 4.3 runtime yet.
- The course image is a two-stage build: stage 1 compiles the Iceberg runtime from a pinned commit of apache/iceberg#18749 and downloads the S3 jars with checksums; stage 2 adds those jars and a Python 3.12 environment to the official image.
- Jars that call each other go in `/opt/spark/jars` together, with one AWS SDK; `spark.jars.packages` keeps only the Kafka connector.
- Swapping the image changed nothing in your application code: the image is platform, your package is application.
- **Next, Chapter 5, Tables and Catalog:** create real Iceberg tables through Unity Catalog OSS, evolve their schema, and travel through their snapshots.

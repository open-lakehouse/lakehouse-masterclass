# Chapter 4.5: Iceberg for Spark 4.3

Chapter 5 creates Iceberg tables, so Spark needs Iceberg's runtime: the jar that teaches Spark to read and write Iceberg tables and to talk to an Iceberg catalog. Iceberg publishes one runtime per Spark minor version, and the release for Spark 4.3 hasn't shipped yet. This short chapter builds it from source and adds it to the course's Spark image. It's temporary on purpose: when an Iceberg release includes the Spark 4.3 runtime, this chapter goes away and the image downloads the released jar instead ([course issue #8](https://github.com/open-lakehouse/lakehouse-masterclass/issues/8)). Nothing else in the course depends on how the jar got into the image.

## Learning objectives

By the end of this chapter you'll be able to:

- Check which Iceberg Spark runtimes are released, and explain why one built for another Spark version won't do.
- Add a build stage to the course's Dockerfile that compiles the runtime from a pinned commit.
- Verify which commit the runtime in your image was built from.

## The runtime that isn't released yet

List the Spark runtimes Iceberg has published to Maven Central:

```bash
curl -s https://repo1.maven.org/maven2/org/apache/iceberg/ | grep -o 'iceberg-spark-runtime-4\.[0-9]_2\.13' | sort -u
# iceberg-spark-runtime-4.0_2.13
# iceberg-spark-runtime-4.1_2.13
```

There is no `4.3`. A runtime built for Spark 4.1 is not a substitute: Iceberg keeps a separate module per Spark minor version because the Spark interfaces it plugs into change between minor releases. Support for Spark 4.3 exists as a pull request, [apache/iceberg#18749](https://github.com/apache/iceberg/pull/18749), so the course builds the runtime from that pull request, pinned to one commit, so that everyone builds the same jar.

## Build: add the Iceberg stage to the image

Three additions to the `docker/spark/Dockerfile` you wrote in Chapter 4.

**Step 1: add the build stage.** Paste it between the `jars` stage and the final `FROM apache/spark:...` line:

```dockerfile
# ---- Iceberg runtime for Spark 4.3, built from source (Chapter 4.5) ---------
# Temporary: no Iceberg release ships iceberg-spark-runtime-4.3_2.13 yet, so this
# builds it from the pull request that adds Spark 4.3 support
# (apache/iceberg#18749), pinned to one commit. Several minutes, once.
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
```

It starts from a JDK 17 image, fetches the one pinned commit (a shallow fetch, so only that commit's files come down), and runs Gradle for just the Spark 4.3, Scala 2.13 runtime jar, skipping the tests and the Flink and Kafka Connect modules. The JDK, Gradle and the Iceberg sources stay in this stage; only the finished jar leaves it.

**Step 2: copy the jar into the final stage,** next to the S3 jars, so it shares Spark's classloader and the one AWS SDK (Chapter 4 explains why that matters). Add this line after the `COPY --from=jars` line:

```dockerfile
COPY --from=iceberg /iceberg-spark-runtime-4.3_2.13.jar /opt/spark/jars/
```

**Step 3: note it in the header.** Add a third item to the comment at the top of the file, so the next person to read the file knows this part is temporary:

```dockerfile
#  3. The Iceberg runtime for Spark 4.3, built from source until an Iceberg
#     release ships it (Chapter 4.5).
```

The whole file now reads:

```dockerfile
# docker/spark/Dockerfile
# The course's Spark image: Apache Spark 4.3.0 plus what the official image
# lacks for this stack.
#
#  1. Python 3.12 with pyarrow, pandas and protobuf. Python code that runs on
#     the cluster (UDFs, and the stateful processor in Chapter 8) executes in a
#     Python worker here, and PySpark requires that worker to run the same
#     Python minor version as your client.
#  2. S3 access for Hadoop's S3A filesystem and for Iceberg, as one AWS SDK on
#     Spark's own classpath (Chapter 4 explains why).
#  3. The Iceberg runtime for Spark 4.3, built from source until an Iceberg
#     release ships it (Chapter 4.5).

# ---- S3 jars, checksummed (Chapter 4) --------------------------------------
# hadoop-aws 3.5.0 matches the Hadoop that Spark 4.3.0 ships; analyticsaccelerator-s3
# is a library it needs; the AWS SDK v2 bundle is the one SDK every caller shares.
FROM alpine:3.20 AS jars
WORKDIR /jars
RUN set -e; M=https://repo1.maven.org/maven2; \
    wget -q $M/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar; \
    wget -q $M/software/amazon/awssdk/bundle/2.56.0/bundle-2.56.0.jar; \
    wget -q $M/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar; \
    printf '%s\n' \
      "66bd9eefb938b1a1657487324b63c13df2d8b17164a1dd24e0ef37b905bc5baa  hadoop-aws-3.5.0.jar" \
      "ae515d6745874faa1942aa47a2750d4ba333609db31457ac1e61f7fe91fc8985  bundle-2.56.0.jar" \
      "4959a9747cbb479c73c3ffbcd479bee5b014f8b2880994585a8693fb9d24675e  analyticsaccelerator-s3-1.3.1.jar" \
      | sha256sum -c -

# ---- Iceberg runtime for Spark 4.3, built from source (Chapter 4.5) ---------
# Temporary: no Iceberg release ships iceberg-spark-runtime-4.3_2.13 yet, so this
# builds it from the pull request that adds Spark 4.3 support
# (apache/iceberg#18749), pinned to one commit. Several minutes, once.
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

# ---- the image every Spark service runs ------------------------------------
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
COPY --from=jars /jars/ /opt/spark/jars/
COPY --from=iceberg /iceberg-spark-runtime-4.3_2.13.jar /opt/spark/jars/
USER spark
```

**Step 4: rebuild and recreate the Spark services.**

```bash
docker compose build spark-connect
docker compose up -d
until docker compose logs --since 2m spark-connect | grep -q "Spark Connect server started"; do sleep 3; done
```

This build takes a while the first time: Gradle compiles Iceberg, which takes several minutes on a laptop and needs a few gigabytes free on the disk Docker uses. The `jars` stage and the Python environment come from the cache. `up -d` sees the new image and recreates the three Spark services; SeaweedFS is untouched.

**Step 5: check the jar and where it came from.**

```bash
docker compose exec spark-connect ls /opt/spark/jars | grep -E 'iceberg|hadoop-aws|bundle'
# bundle-2.56.0.jar
# hadoop-aws-3.5.0.jar
# iceberg-spark-runtime-4.3_2.13.jar
docker compose exec spark-connect /opt/venv/bin/python -c \
  "import zipfile; print(zipfile.ZipFile('/opt/spark/jars/iceberg-spark-runtime-4.3_2.13.jar').read('iceberg-build.properties').decode())" \
  | grep 'git.commit.id='
# git.commit.id=b1eab64f8d304344a22301084f841fa69a376d86
```

The runtime records the commit it was built from in a small properties file inside the jar (read here with the image's own Python, since the image has no `unzip`), and it matches `ICEBERG_COMMIT` in the Dockerfile. Iceberg also stamps it on every commit: each snapshot's summary, which Chapter 8 reads through the catalog's REST API, carries `iceberg-version: Apache Iceberg b1eab64 (commit b1eab64f8d304344a22301084f841fa69a376d86)`.

**What just happened?** The image now carries everything Chapter 5 needs from Spark's side. Nothing uses the runtime yet: Spark loads Iceberg's code when Chapter 5's settings in `spark-defaults.conf` name its SQL extensions and its catalog. Because the jar sits in `/opt/spark/jars` like the S3 jars, those settings don't care whether it was built here or downloaded from a release.

## When this chapter goes away

When an Iceberg release ships `iceberg-spark-runtime-4.3_2.13`, the `iceberg` stage and its `COPY` line are replaced by one more download in Chapter 4's `jars` stage, checksummed like the others, and this chapter is removed. Chapter 5 onward is unchanged. Course issue #8 lists the steps.

## Troubleshooting

- **The build fails in the `iceberg` stage.** It needs network access to GitHub and Maven Central (Gradle downloads its own dependencies) and a few gigabytes of free disk. Re-run the build; Docker resumes from the last cached layer. If the `git fetch` step fails while the network is fine, the pinned commit may no longer be fetchable (a pull request's history can be rewritten); check course issue #8 for the current commit, or for the released runtime if it exists by now.
- **`WARN SparkSession: Cannot use org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions to configure session extensions`, with a `ClassNotFoundException`, in the `spark-connect` log** (once Chapter 5's settings are in). The Spark services run an image without the runtime: you rebuilt but didn't recreate, or one service still runs an older image. Check `docker compose images`. The server keeps running without Iceberg's extensions, so this otherwise only shows up when an Iceberg statement fails.

## Checkpoint

- `/opt/spark/jars` in `spark-connect` holds `iceberg-spark-runtime-4.3_2.13.jar` next to the S3 jars.
- The jar's `git.commit.id` matches `ICEBERG_COMMIT` in the Dockerfile.

## Recap and what's next

- No Iceberg release has a Spark 4.3 runtime yet, so the course builds one from a pinned commit of apache/iceberg#18749, in its own build stage.
- The jar goes into `/opt/spark/jars` with the S3 jars, so it shares Spark's classloader and the one AWS SDK.
- This chapter is temporary: when the runtime is released, a checksummed download replaces the build stage and nothing after it changes.
- **Next, Chapter 5, Tables and Catalog:** turn on Iceberg's SQL extensions, create real Iceberg tables through Unity Catalog OSS, evolve their schema, and travel through their snapshots.

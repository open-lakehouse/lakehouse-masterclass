# Film: the refresh race (terminal)

**Story.** Chapter 9, "Two runs at once: the refresh race". Two refreshes of the same
Spark Declarative Pipeline started together (what a scheduled run plus a manual run
amount to) both report `Run is COMPLETED`, while Silver ends up with every order twice
and Gold's `order_count` sums to more again. Silver's commit log, read from Unity
Catalog's Iceberg REST endpoint, shows why: delete, delete, append, append. The beats
follow the chapter section in order; nothing in the film is staged, the race runs live
against the dev stack.

**Tape:** `refresh-race.tape`. **Needs:** a checkout of the finished course project (the code from every chapter, Chapter 13's tree) with
seaweedfs, spark-master, spark-worker, spark-connect and unity-catalog up, and Silver
and Gold freshly refreshed by one run on its own (Silver = distinct ids = Gold sum).
Render with `--demo <that checkout>`. Every render runs the
race for real, so restore the tables before and after each one:
`cd pipelines/medallion && spark-pipelines run --remote $SPARK_REMOTE`.

## Beats

There is no video script for this section, so every VO line is a **draft**, taken from
the chapter's sentences and checked against the run. The numbers in the VO are left out
on purpose: each render is its own run, and the screen shows that run's numbers.

| beat | shows | VO | zoom |
|---|---|---|---|
| `before` | Caption, then the chapter's three-number check as one query: Silver rows, distinct order ids, Gold's summed order_count, all equal | Once Airflow schedules the medallion, it has two ways to start: the scheduler, and you typing spark-pipelines run. First, the healthy state. Silver's rows, its distinct order ids, and Gold's summed order count are the same number. | `counts 20-end-1` |
| `race` | Caption, two `spark-pipelines run` commands in the background on one line, `wait`, then `grep "Run is"` on both logs: two `Run is COMPLETED` | Now both at once, which is what a scheduled run and a manual run amount to. Neither the spec nor the Connect server refuses a second run of a pipeline that is already running. Both runs finish, and both say COMPLETED. | `done 16-end-1` |
| `after` | Caption, the same check recalled with Ctrl+R: Silver doubled, ids unchanged, Gold's sum higher still | Same three numbers. Silver holds every order twice, the distinct count hasn't moved, and Gold's order count no longer matches Silver at all. Every status said success. | `counts2 6-end-1` |
| `commits` | Caption, curl of Silver's table metadata from the catalog's Iceberg REST endpoint piped into Python: the last four snapshots, delete, delete, append, append | Here is why. A materialized view refresh is two commits, a delete that empties the table, then an append of the result. The first run emptied Silver, the second emptied an empty table, then each appended its own full copy. Iceberg accepted all four, because each commit is valid on its own. | `log 24-end-1` |

## Zoom regions

Boxes are pixels of the 1920x1080 landscape render, measured on its frames. The vertical
cut isn't zoomed.

| region | landscape | vertical |
|---|---|---|
| `counts` | 0,180,700,170 | - |
| `counts2` | 0,150,700,170 | - |
| `done` | 0,210,1200,140 | - |
| `log` | 0,320,900,150 | - |

## What has to be readable

- `before`: the three equal numbers under `silver`, `ids`, `gold`.
- `race`: both commands on one line, and two `Run is COMPLETED` lines, one per log.
- `after`: Silver twice the `before` value, `ids` unchanged, `gold` different from both.
- `commits`: the four operations in order (delete, delete, append, append) with their
  added, deleted and total counts.

## Not filmed here

Title card, the Airflow side (the rule "one entry point per pipeline" and
`airflow dags trigger`), and the restore run, which happens off camera after each take.

# Film: the sessions job survives a crash (terminal)

**Story.** Chapter 8, "Stateful streaming: one row per customer visit", Steps 8 to 10.
The producer runs in one terminal and `lakehouse/customer_sessions.py` (transformWithState,
one row per customer visit) in a second. A minute later the Gold table holds finished
visits; the `statestore` data source shows the visits still open (the `visit` value
state, the `orders` map, the timers). Then the crash: `docker compose restart
spark-connect` under the running query, the client dies, the same command starts it
again on the same checkpoint, and a minute later rows equal distinct visits. The beats
follow the chapter's steps in order, live against the dev stack.

**Tape:** `sessions-crash.tape`. **Needs:** a checkout of the finished course project (the code from every chapter, Chapter 13's tree) with
seaweedfs, spark-master, spark-worker, spark-connect, unity-catalog, zookeeper and kafka
up. `film-setup.sh <format>` resets the sessions table and checkpoint (as
`data/replay-ch8.sh` does) and builds three tmux panes on their own server (producer,
customer_sessions, query); the tape types into them, so the producer and the job keep
running across beats. `film-teardown.sh` stops everything afterwards and restarts
spark-connect. A take is one continuous run: render the whole tape, never `--only`.

Two departures from the chapter's text, for the vertical cut's 62 columns: the Gold query
shows the visit's start and end as `HH:mm:ss` and leaves out `closed_by`, and the three
`statestore` reads drop the `partition_id` column (it is 0 for every row here).

## Beats

There is no video script for this section, so every VO line is a **draft**, taken from
the chapter's sentences and checked against the code. Numbers are left out of the VO; the
screen shows each take's own.

| beat | shows | VO | zoom |
|---|---|---|---|
| `producer` | Producer pane: caption, `python -m lakehouse.stream_source`, sent / RESEND / late lines scrolling | The producer publishes an order event about every two hundred milliseconds, re-sends and three-minute-late events included. | |
| `job` | Job pane: caption, `python -m lakehouse.customer_sessions`, the "sessionizing into" line | The job keeps each customer's open visit in the state store, and closes it once the watermark passes fifteen quiet seconds. | |
| `gold` | Query pane: Python, the Gold query, four visits with three or more orders | A minute later, the Gold table has rows. Each one is a visit, written only after it ended: when it started and ended, how many orders, the revenue from orders that weren't cancelled, and how many were. | `gold 14-end-1` |
| `state` | The `statestore` source: open visits (start and latest order in epoch ms), their orders map, their timers | The state is data too. The state data source reads it from the checkpoint while the query runs. These are visits still open: the visit's start and latest order, its orders so far, and a timer set to the latest order plus fifteen seconds. | `timers 20-end-1` |
| `crash` | `docker compose restart spark-connect` under the running query | Now the crash: restart the Connect server under the running query. The driver and its state store go with it. | |
| `restart` | The server's "started" log line; the job pane's client error (`INVALID_HANDLE.SESSION_NOT_FOUND`, raised once the server is back); the same command again | When the server is back, start the job again with the same command. It finds its checkpoint, restores every open visit, map and timer from the last committed micro-batch, and carries on from those Kafka offsets. | |
| `check` | Python again: `count(*)` against distinct `(customer_id, session_start)`, equal | A minute later, count rows against distinct visits. They're equal. No visit was written twice, and none was lost, because Iceberg skips any micro-batch it has already committed. | `check 12-end-1` |
| `stop` | Ctrl-C in the job pane (query.stop()), then in the producer | Control-C in the job calls query stop, which ends the query on the server too. | |

## Zoom regions

Boxes are pixels of the 1920x1080 landscape render, measured on its frames. The vertical
cut isn't zoomed.

| region | landscape | vertical |
|---|---|---|
| `gold` | 830,290,900,260 | - |
| `timers` | 830,780,900,220 | - |
| `check` | 830,470,700,160 | - |

## What has to be readable

- `producer`: `sent`, `RESEND` and `late` lines with order id, customer and created_at.
- `job`: `sessionizing into iceberg.gold.customer_sessions`.
- `gold`: four rows: customer, start, end, orders (3 or more), revenue, cancelled.
- `state`: the three tables' headers and rows. Each read is of the latest committed
  micro-batch, which can move between reads, so not every customer lines up across the
  three; at least one should (timer = the visit's latest order plus 15000).
- `crash`: the restart command and `Started`.
- `restart`: the "Spark Connect server started" line, the client's traceback, and the second "sessionizing into".
- `check`: `rows` equal to `visits`.

## Not filmed here

Title card, the processor's code (Step 7; the animation in `media/anim/stateful-sessions`
covers it), the Bronze stream, and the offline check against the producer's log
(`data/check_sessions.py`), which runs after each take.

# The sessions job survives a crash (Chapter 8, "Stateful streaming: one row per customer
# visit", Steps 8 to 10). Storyboard: storyboard.md.
# One take is one continuous run: the producer and the job keep running between beats in
# tmux panes on their own server, and every clip attaches to it. So never --only a beat;
# render the whole tape, between the off-camera helpers:
#   film-setup.sh landscape && film render sessions-crash.tape --demo <course project>; film-teardown.sh
#   film-setup.sh vertical  && film render sessions-crash.tape --demo <course project> --format vertical; film-teardown.sh
#   film-setup.sh chapter   && film render sessions-crash.tape --demo <course project> --format chapter; film-teardown.sh
# The waits (visits closing, the server coming back) are cut with Hide/Sleep at the ends
# of beats.
Set TypingSpeed 20ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip: attach to the panes film-setup.sh built.
Hide
Type "tmux -L film2 attach -t film" Enter
Sleep 1500ms
Show

# beat: producer | The producer
Sleep 500ms
Alt+p
Sleep 300ms
Type "# Terminal 1: the order producer (re-sends and late events included)" Enter
Sleep 300ms
Type "python -m lakehouse.stream_source" Enter
Sleep 6s

# beat: job | The sessions job
Sleep 300ms
Alt+j
Sleep 300ms
Type "# Terminal 2: the transformWithState job, one row per visit" Enter
Sleep 300ms
Type "python -m lakehouse.customer_sessions" Enter
Wait+Screen@90s /sessionizing into/
Sleep 5s
Hide
Sleep 60s

# beat: gold | A minute later: the Gold table
Sleep 300ms
Alt+q
Sleep 300ms
Type "# A minute later, from Python: the visits that have closed" Enter
Type "python -q" Enter
Sleep 1s
Type "from lakehouse.session import get_spark; spark = get_spark()" Enter
Sleep 300ms
Type `spark.sql("SELECT customer_id, date_format(session_start, 'HH:mm:ss') AS started, date_format(session_end, 'HH:mm:ss') AS ended, orders, revenue, cancelled FROM iceberg.gold.customer_sessions WHERE orders >= 3 ORDER BY session_start LIMIT 4").show()` Enter
Wait+Screen@90s /cancelled\|/
Sleep 8s

# beat: state | Inside the state store
Sleep 300ms
Ctrl+L
Sleep 300ms
Type "# The visits still open, read from the checkpoint (statestore source)" Enter
Type `cp = "s3a://lakehouse/_checkpoints/gold_customer_sessions"` Enter
Type `spark.read.format("statestore").option("stateVarName", "visit").load(cp).drop("partition_id").show(3, truncate=False)` Enter
Wait+Screen@90s /\|value/
Sleep 1s
Type `spark.read.format("statestore").option("stateVarName", "orders").load(cp).drop("partition_id").show(3, truncate=False)` Enter
Wait+Screen@90s /user_map_value/
Sleep 1s
Type `spark.read.format("statestore").option("readRegisteredTimers", "true").load(cp).drop("partition_id").show(3, truncate=False)` Enter
Wait+Screen@90s /expiration_timestamp_ms/
Sleep 9s

# beat: crash | Crash: restart the Connect server
Sleep 300ms
Type "exit()" Enter
Sleep 300ms
Ctrl+L
Type "# Step 10: crash it. Restart the Connect server under the running query" Enter
Sleep 300ms
Type "docker compose restart spark-connect" Enter
Sleep 2s
Hide
Wait@90s
Show
Sleep 4s

# beat: restart | Start the job again
Hide
Sleep 15s
Show
Sleep 300ms
Type "docker compose logs --since 1m spark-connect | grep 'server started'" Enter
Wait
Sleep 1s
Alt+j
Sleep 4s
Ctrl+L
Type "# Same command, same checkpoint" Enter
Type "python -m lakehouse.customer_sessions" Enter
Wait+Screen@90s /sessionizing into/
Sleep 5s
Hide
Sleep 60s

# beat: check | Rows equal visits
Sleep 300ms
Alt+q
Sleep 300ms
Type "# A minute later: no visit written twice" Enter
Type "python -q" Enter
Sleep 1s
Type "from lakehouse.session import get_spark; spark = get_spark()" Enter
Sleep 300ms
Type `spark.sql("SELECT count(*) AS rows, count(DISTINCT customer_id, session_start) AS visits FROM iceberg.gold.customer_sessions").show()` Enter
Wait+Screen@90s /visits\|/
Sleep 7s

# beat: stop | Ctrl-C stops the query on the server
Sleep 300ms
Alt+j
Sleep 500ms
Ctrl+C
Sleep 3s
Type "# Ctrl-C: query.stop() ends the query on the server too" Enter
Sleep 300ms
Alt+p
Sleep 300ms
Ctrl+C
Sleep 4s

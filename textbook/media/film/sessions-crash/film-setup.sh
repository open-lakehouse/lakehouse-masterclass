#!/usr/bin/env bash
# Off camera, before every take of sessions-crash.tape: reset the sessions table and its
# checkpoint the way data/replay-ch8.sh does, then build the three tmux panes the tape
# types into (producer, customer_sessions, query) on their own tmux server (-L film2).
#   film-setup.sh landscape|vertical|chapter
set -euo pipefail
FMT=${1:-landscape}
D=${FILM_DEMO:?set FILM_DEMO to a checkout of the finished course project}
HERE=$(cd "$(dirname "$0")" && pwd)
LOG=${FILM_PRODUCER_LOG:-$HERE/renders/producer-$FMT.log}
mkdir -p "$HERE/renders"
cd "$D"
. data/env.sh

docker compose up -d zookeeper kafka >/dev/null 2>&1
# A query whose client exited without stop() keeps running on the server, and executors
# keep state stores loaded after a query ends: restart both before deleting the checkpoint.
docker compose restart spark-worker spark-connect >/dev/null 2>&1
until docker compose logs --since 30s spark-connect 2>&1 | grep -q "Spark Connect server started"; do sleep 2; done
python -c "from lakehouse.session import get_spark; get_spark('reset').sql('DROP TABLE IF EXISTS iceberg.gold.customer_sessions PURGE')"
# Through SeaweedFS itself: `aws s3 rm --recursive` leaves empty directories behind.
docker compose exec -T seaweedfs sh -c 'echo "fs.rm -r /buckets/lakehouse/_checkpoints/gold_customer_sessions" | weed shell' >/dev/null 2>&1 || true
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic orders --partitions 1 --replication-factor 1 >/dev/null

T="tmux -L film2 -f $HERE/tmux.conf"
$T kill-server 2>/dev/null || true
if [[ $FMT == vertical ]]; then
    # 62x52 cells: producer, job, query stacked.
    $T new-session -d -s film -x 62 -y 52 -c "$D" "zsh -f"
    $T split-window -v -t film:0.0 -l 35 -c "$D" "zsh -f"
    $T split-window -v -t film:0.0 -l 9 -c "$D" "zsh -f"
elif [[ $FMT == chapter ]]; then
    # 95x24 cells (1280x720 at 20px, for the textbook's embedded clips): a wider query pane,
    # since the producer pane is only there to show that events are flowing.
    $T new-session -d -s film -x 95 -y 24 -c "$D" "zsh -f"
    $T split-window -h -t film:0.0 -l 60 -c "$D" "zsh -f"
    $T split-window -v -t film:0.0 -c "$D" "zsh -f"
else
    # 131x34 cells: producer over job on the left, query on the right.
    $T new-session -d -s film -x 131 -y 34 -c "$D" "zsh -f"
    $T split-window -h -t film:0.0 -l 74 -c "$D" "zsh -f"
    $T split-window -v -t film:0.0 -c "$D" "zsh -f"
fi
titles=(producer customer_sessions query)
for i in 0 1 2; do
    $T select-pane -t "film:0.$i" -T "${titles[$i]}"
    $T send-keys -t "film:0.$i" " VIRTUAL_ENV_DISABLE_PROMPT=1 . data/env.sh; setopt interactivecomments histignorespace; PROMPT='%F{cyan}open-lakehouse%f %F{magenta}❯%f '; clear" Enter
done
# The producer's lines, for checking the visits afterwards (data/check_sessions.py).
: > "$LOG"
$T pipe-pane -t film:0.0 "tr -d '\r' >> '$LOG'"
$T select-pane -t film:0.0
sleep 1
$T list-panes -t film -F '#{pane_index} #{pane_title} #{pane_width}x#{pane_height}'

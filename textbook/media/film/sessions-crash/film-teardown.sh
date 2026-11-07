#!/usr/bin/env bash
# Off camera, after every take: stop anything still running in the panes, restart the
# Connect server (which stops any query whose client left without stop()), close tmux.
set -uo pipefail
D=${FILM_DEMO:?set FILM_DEMO to a checkout of the finished course project}
HERE=$(cd "$(dirname "$0")" && pwd)
T="tmux -L film2 -f $HERE/tmux.conf"
for i in 0 1 2; do $T send-keys -t "film:0.$i" C-c 2>/dev/null; done
sleep 5
cd "$D"
docker compose restart spark-connect >/dev/null 2>&1
until docker compose logs --since 30s spark-connect 2>&1 | grep -q "Spark Connect server started"; do sleep 2; done
$T kill-server 2>/dev/null || true
echo "torn down"

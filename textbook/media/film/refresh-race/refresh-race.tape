# The refresh race (Chapter 9, "Two runs at once"): two refreshes of the medallion
# pipeline at once, both COMPLETED, Silver doubled. Storyboard: storyboard.md.
#   film render textbook/media/film/refresh-race/refresh-race.tape --demo ~/masterclass-student/open-lakehouse-dev [--format vertical]
# Every render runs the race for real: restore Silver and Gold with one pipeline run
# before and after each render (see storyboard.md), and delete renders/*/film.hist.
Set TypingSpeed 35ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip: the dev stack's environment (env.sh sets
# SPARK_REMOTE, CATALOG_URI and the venv), the pipeline folder, comments allowed, emacs
# keys for Ctrl+R, and a history file shared by the clips (in renders/, so the `after`
# beat can recall the `before` beat's query). Lines starting with a space stay out of it.
Hide
Type " H=$PWD/film.hist; cd $FILM_DEMO && VIRTUAL_ENV_DISABLE_PROMPT=1 . data/env.sh && cd pipelines/medallion" Enter
Type " setopt interactivecomments histignorespace incappendhistory; bindkey -e; HISTFILE=$H HISTSIZE=200 SAVEHIST=200; touch $H; fc -R $H" Enter
Type " [[ -n $FILM_REAL ]] || PROMPT='%F{cyan}pipelines/medallion%f %F{magenta}❯%f ' PS2='> '; clear" Enter
Sleep 1s
Show

# beat: before | Silver and Gold before
Sleep 600ms
Type "# Chapter 9: one pipeline, two ways to start it. First, the healthy state:" Enter
Type "# Silver's rows, its distinct order ids, and Gold's summed order_count" Enter
Sleep 400ms
Type "python -c "
Type `"from lakehouse.session import get_spark; get_spark().sql('SELECT (SELECT count(*) FROM iceberg.silver.orders) AS silver, (SELECT count(DISTINCT order_id) FROM iceberg.silver.orders) AS ids, (SELECT sum(order_count) FROM iceberg.gold.daily_revenue) AS gold').show()"`
Sleep 300ms
Enter
Wait
Sleep 8s

# beat: race | Two runs at once
Sleep 600ms
Type "# A scheduled run and a manual run of the same pipeline, started together:" Enter
Sleep 400ms
Type "spark-pipelines run --remote $SPARK_REMOTE > run-a.log 2>&1 & spark-pipelines run --remote $SPARK_REMOTE > run-b.log 2>&1 &"
Sleep 300ms
Enter
Sleep 500ms
Type "wait; grep 'Run is' run-a.log run-b.log"
Sleep 200ms
Enter
Wait
Sleep 8s

# beat: after | Silver and Gold after
Sleep 600ms
Type "# Both COMPLETED. The same three numbers again:" Enter
Sleep 400ms
Ctrl+R
Sleep 300ms
Type "AS ids"
Sleep 1.2s
Enter
Wait
Sleep 7s

# beat: commits | Silver's commit log
Sleep 600ms
Type "# Why: Silver's last four commits, from Unity Catalog's Iceberg REST endpoint" Enter
Sleep 400ms
Type "curl -s $CATALOG_URI/v1/catalogs/iceberg/namespaces/silver/tables/orders | python -c "
Type '"' Enter
Type "import sys, json, datetime" Enter
Type "for s in json.load(sys.stdin)['metadata']['snapshots'][-4:]:" Enter
Type "    t = datetime.datetime.fromtimestamp(s['timestamp-ms'] / 1000, datetime.UTC).strftime('%H:%M:%S.%f')[:-3]" Enter
Type "    su = s['summary']" Enter
Type "    print(t, su['operation'].ljust(7), 'added', su.get('added-records', 0), 'deleted', su.get('deleted-records', 0), 'total', su['total-records'])" Enter
Type '"'
Sleep 300ms
Enter
Wait
Sleep 14s

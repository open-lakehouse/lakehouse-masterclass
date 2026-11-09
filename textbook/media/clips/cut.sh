#!/usr/bin/env bash
# Cut the short muted clips the chapters embed, plus their poster stills.
#
# The sources are the full renders from textbook/media/anim (create-animation)
# and textbook/media/film (film-demo), which are kept out of git. Start and end
# times in clips.tsv are the beat boundaries from each film's cue sheet.
#
# The MP4s are not committed either. Upload them as assets of the GitHub
# release media-v1, which the chapters link to:
#   gh release upload media-v1 out/*.mp4 --clobber
# The posters (small WebP stills) are committed next to this script.
#
# Usage: cut.sh <renders-dir>   (the dir holding anim/ and film/)
set -euo pipefail

RENDERS=${1:?usage: cut.sh <renders-dir>}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT="$HERE/out"
mkdir -p "$OUT"

tail -n +2 "$HERE/clips.tsv" | while IFS=$'\t' read -r clip source start end poster_at chapter; do
  ffmpeg -nostdin -v error -y -ss "$start" -to "$end" -i "$RENDERS/$source" -an \
    -vf "scale=1280:-2,fps=25" -c:v libx264 -preset slow -crf 28 \
    -pix_fmt yuv420p -movflags +faststart "$OUT/$clip.mp4"
  ffmpeg -nostdin -v error -y -ss "$poster_at" -i "$OUT/$clip.mp4" -frames:v 1 \
    -c:v libwebp -quality 70 "$HERE/$clip.webp"
  echo "$clip ($chapter): $(du -h "$OUT/$clip.mp4" | cut -f1)"
done

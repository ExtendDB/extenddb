#!/usr/bin/env bash
# Copyright 2026 ExtendDB contributors
# SPDX-License-Identifier: Apache-2.0
#
# Regenerate every released PostgreSQL fixture, a few at a time, with
# regenerate.sh. Logs go to <state>.log beside each fixture directory's
# parent unless LOGDIR is set.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
LOGDIR="${LOGDIR:-$HERE}"
STATES=(0.1.5 0.1.6 0.1.8 0.1.10 0.1.11 0.1.12 0.1.13 0.1.14 pv-0.1.11 pv-0.1.14 v0.1.0 v0.1.2)
PARALLEL="${PARALLEL:-4}"
n=0
pids=()
for state in "${STATES[@]}"; do
  n=$((n + 1))
  bash "$HERE/regenerate.sh" "$state" "$n" > "$LOGDIR/$state.log" 2>&1 &
  pids+=("$!")
  if [ "${#pids[@]}" -ge "$PARALLEL" ]; then
    wait "${pids[0]}"; echo "$(tail -n 1 "$LOGDIR/${STATES[$((n - ${#pids[@]}))]}.log")"
    pids=("${pids[@]:1}")
  fi
done
for pid in "${pids[@]}"; do wait "$pid"; done
for state in "${STATES[@]}"; do echo "$state: $(tail -n 1 "$LOGDIR/$state.log")"; done

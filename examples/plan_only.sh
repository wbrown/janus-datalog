#!/usr/bin/env bash
# plan_only.sh — show what planning does to the complex checkpoint query,
# without executing it: the compiled relational algebra, every rewrite
# decision the optimization passes made (applied, or declined with the failed
# precondition), the optimized tree, the Datalog it decompiles back to, and
# the physical plan.
#
# Usage:
#   examples/plan_only.sh                     # algebra optimizer on (default)
#   examples/plan_only.sh -optimize=false     # baseline planner
#
# Extra arguments pass through to the CLI. Planning needs no data, so the
# script runs against an empty temporary database; note that a schemaless
# database plans with schemaless defaults — point -db at a real database
# (extra args win over the script's default) to see schema-aware planning.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

echo "Building cmd/datalog from the working tree..." >&2
go build -C "$repo_root" -o "$workdir/datalog" ./cmd/datalog

# An empty EDN dump loads into a temporary database; -plan-only never writes.
touch "$workdir/empty.edn"

# The complex checkpoint query (BenchmarkComplexQueryCheckpoint's shape):
# scenario rollups through correlated aggregate subqueries, get-else
# defaults, or-default fallbacks, a nested argmax subquery, and bounded
# ordered finalization.
query='[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens ?totalDuration ?complete ?lastKey ?lastUpdatedAt
  :where
  [?scenario :entity/type :entity.type/scenario]
  [?scenario :scenario/title ?title]
  [?scenario :scenario/created-at ?createdAt]
  (or-default [(q [:find (count ?t) (sum ?tok) (sum ?dur)
           :in $ ?s
           :where [?t :task/root ?s]
                  [?t :task/status :status/complete]
                  [(get-else $ ?t :task/token-count 0) ?tok]
                  [(get-else $ ?t :task/duration 0) ?dur]]
          $ ?scenario) [[?taskCount ?totalTokens ?totalDuration]]]
      [(ground [0 0 0]) [[?taskCount ?totalTokens ?totalDuration]]])
  (or-default [(q [:find (count ?t)
           :in $ ?s
           :where [?t :task/root ?s]
                  [?t :task/key :task/opening]
                  [?t :task/status :status/complete]]
          $ ?scenario) [[?openingCount]]]
      [(ground 0) ?openingCount])
  [[(> ?openingCount 0)] ?complete]
  (or-default [(q [:find ?key ?ca
           :in $ ?s
           :where [?t :task/root ?s]
                  [?t :task/status :status/complete]
                  [?t :task/completed-at ?ca]
                  [?t :task/key ?key]
                  [(q [:find (max ?ca)
                       :in $ ?s
                       :where [?t :task/root ?s]
                              [?t :task/status :status/complete]
                              [?t :task/completed-at ?ca]]
                      $ ?s) [[?maxCa]]]
                  [(= ?ca ?maxCa)]]
          $ ?scenario) [[?lastKey ?lastUpdatedAt]]]
      [(ground [:none :none]) [[?lastKey ?lastUpdatedAt]]])
  :order-by [[?lastUpdatedAt :desc]]
  :limit 25]'

exec "$workdir/datalog" -db "$workdir/empty.edn" -plan-only -query "$query" "$@"

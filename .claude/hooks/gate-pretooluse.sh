#!/bin/bash
# gate-pretooluse.sh — one-mutating-call-per-turn gate (PreToolUse).
#
# Catches tool-call CHAINING: emitting a 2nd mutating call in the same assistant
# turn, authored before the 1st call's result is seen. The repo's other hooks
# review commit reasoning and edit reasoning, but none catch chaining itself —
# the failure mode where an agent commits/acts on an unread result.
#
# Rule (strict), within one assistant turn:
#   - Edit/Write/NotebookEdit may batch freely AMONG THEMSELVES (deterministic;
#     no result to interpret before the next action).
#   - Bash must be SOLO: denied if any gated call already ran this turn, and any
#     edit after a Bash is denied. A command produces a result that must be read
#     and interpreted before the next decision, so it cannot share a turn.
#
#   Allowed:  [Edit,Edit,Edit] | [Bash]
#   Denied:   [Edit,Bash] (deny Bash) | [Bash,Edit] (deny Edit) | [Bash,Bash] (deny 2nd)
#
# Mechanism: two atomic mkdir markers under .claude/.gate-locks/, cleared between
# batches by gate-reset.sh (PostToolBatch + UserPromptSubmit + Stop). No TTL: a
# time-based reclaim would let a chained call through whenever the first call runs
# longer than the TTL (e.g. a slow `go test`) — exactly the case this blocks.
# Deny via permissionDecision:"deny" — a programmatic block, verified NOT subject
# to the permission-dialog timeout-proceed behavior. Only denials are logged.

set -euo pipefail

# Resolve the lock dir relative to this script (<proj>/.claude/hooks/<this>),
# independent of CLAUDE_PROJECT_DIR being exported.
HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCK_ROOT="$(dirname "$HOOK_DIR")/.gate-locks"   # <proj>/.claude/.gate-locks
LOG="$LOCK_ROOT/denials.log"
BATCH="$LOCK_ROOT/.batch"
CMD="$LOCK_ROOT/.cmd"
mkdir -p "$LOCK_ROOT"

input="$(cat)"
tool="$(printf '%s' "$input" | jq -r '.tool_name // ""')"

deny() {  # $1=reason-tag $2=message
  printf '%s deny tool=%s reason=%s\n' "$(date +%s)" "$tool" "$1" >> "$LOG"
  jq -n --arg r "$2" '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: $r
    }
  }'
  exit 0
}

if [[ "$tool" == "Bash" ]]; then
  if mkdir "$BATCH" 2>/dev/null; then
    mkdir "$CMD" 2>/dev/null || true   # mark that a command ran this turn
    exit 0                             # first gated call — allow
  fi
  deny "bash-not-solo" "A command must be the only mutating tool call in its turn. Run this Bash alone, read its result, then continue next turn. (Edits may batch together, but not with a command.)"
fi

# Edit / Write / NotebookEdit
if [[ -d "$CMD" ]]; then
  deny "edit-after-cmd" "A command already ran this turn; do not follow it with an edit in the same turn. Read the command's result first, then edit next turn."
fi
mkdir "$BATCH" 2>/dev/null || true   # idempotent: edits may batch among themselves
exit 0

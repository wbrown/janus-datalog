#!/bin/bash
# review-commit.sh — Reviews git commit reasoning to catch premature victory.
# Fires on Bash commands that look like git commits.

set -euo pipefail

INPUT=$(cat)
TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name')
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // ""')
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')

# Only fire on git commit commands
if ! echo "$COMMAND" | grep -qE '^\s*git commit'; then
    exit 0
fi

# Need transcript
if [ -z "$TRANSCRIPT_PATH" ] || [ ! -f "$TRANSCRIPT_PATH" ]; then
    exit 0
fi

# Extract recent reasoning + user messages
REASONING=$(tail -300 "$TRANSCRIPT_PATH" | \
    jq -r 'select(.type == "assistant" or .type == "human" or .type == "user") |
        .type as $type |
        .message.content[]? |
        select(.type == "thinking" or .type == "text") |
        "\($type): \(.thinking // .text)"' 2>/dev/null | tail -75) || true

if [ -z "$REASONING" ]; then
    exit 0
fi

# Get the commit message
COMMIT_MSG="$COMMAND"

SYSTEM_PROMPT='You are a commit auditor. You review an AI coding agent'"'"'s reasoning chain before it commits code to catch PREMATURE VICTORY — the pattern where the agent declares work "done" or "complete" while known problems remain unsolved.

You receive the agent'"'"'s recent reasoning (including user messages) and the git commit command.

Pay particular attention to user messages that authorize the commit. The user'"'"'s decisions are authoritative — if the user says "commit", "let'"'"'s commit", or similar, the commit is authorized regardless of unsolved problems. The user decides when work is done, not the agent.

## Failure modes to detect:

### 1. DECLARING VICTORY WITH KNOWN UNSOLVED PROBLEMS
The agent commits while acknowledging unsolved performance issues, correctness bugs, or missing features with language like:
- "The remaining bottleneck is X — needs a different approach"
- "This is a separate optimization"
- "Future work"
- "That'"'"'s the next step"
- "Running low on context"
This is the agent treating difficulty as a signal to stop instead of a signal to think harder.

### 2. COMMITTING BROKEN INTERMEDIATE STATE
The agent commits code with known test failures, planning to "fix them in the next commit." Senior engineers finish the work, verify it passes, then commit once.

### 3. COMMITTING TO AVOID ACCOUNTABILITY
The agent commits after user criticism to "save progress" rather than addressing the criticism.

## What is NOT a failure:
- Committing after all tests pass and the feature works correctly
- Committing after the user explicitly says to commit
- Committing a checkpoint the user requested

## Response format:

Think through your analysis. Then emit your verdict as the FINAL line.

CRITICAL: Do NOT emit [BLOCKED] or [ALLOWED] anywhere except your final verdict line.

Final line format:
VERDICT: <brief reason> [ALLOWED]
VERDICT: <brief reason> [BLOCKED]

When in doubt, allow — only block clear premature victory patterns.'

REVIEW_PROMPT="AGENT REASONING BEFORE COMMIT:
$REASONING

---

COMMIT COMMAND: $COMMIT_MSG"

START_TIME=$(date +%s)
REVIEW_RESULT=$(echo "$REVIEW_PROMPT" | env -u CLAUDECODE claude --effort low -p --model sonnet --system-prompt "$SYSTEM_PROMPT" --no-session-persistence --tools "" "Review whether this commit is premature victory — is the agent declaring done while known problems remain?" 2>/dev/null) || {
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    echo "[${ELAPSED}.0s] COMMIT HOOK FAILED (timeout or API error)" >> /tmp/commit_audit_log.txt
    exit 0
}
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "[${ELAPSED}.0s] $REVIEW_RESULT" >> /tmp/commit_audit_log.txt

LAST_LINE=$(echo "$REVIEW_RESULT" | tail -1)
if echo "$LAST_LINE" | grep -q "\[BLOCKED\]"; then
    echo "COMMIT AUDIT: $REVIEW_RESULT" >&2
    exit 2
fi

CONTEXT_MSG="COMMIT AUDIT [${ELAPSED}s]: $REVIEW_RESULT"
jq -n --arg ctx "$CONTEXT_MSG" '{
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    permissionDecision: "allow",
    permissionDecisionReason: "Commit audit passed",
    additionalContext: $ctx
  }
}'

exit 0

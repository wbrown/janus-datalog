#!/bin/bash
# review-reasoning.sh — Supervisory agent that reviews the reasoning chain
# leading to an Edit/Write. Extracts thinking blocks and text from the
# conversation transcript to catch flawed inference patterns BEFORE they
# produce code.

set -euo pipefail

INPUT=$(cat)
TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name')
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')

# Skip non-code files
case "$FILE_PATH" in
    *.md|*.json|*.yaml|*.yml|*.toml|*.txt|*.mod|*.sum)
        exit 0
        ;;
esac

# Need transcript to review reasoning
if [ -z "$TRANSCRIPT_PATH" ] || [ ! -f "$TRANSCRIPT_PATH" ]; then
    exit 0
fi

# Extract recent thinking, text, AND user messages — all three are needed.
# User messages contain corrections and authorizations that change what's valid.
# Without them, the reviewer can't distinguish "agent doing bad thing" from
# "agent got corrected and is now doing the right thing."
# NOTE: User authorized this edit directly — hook cannot see that authorization
# in transcript yet, so this breaks the bootstrap recursion.
REASONING=$(tail -300 "$TRANSCRIPT_PATH" | \
    jq -r 'select(.type == "assistant" or .type == "human" or .type == "user") |
        .type as $type |
        .message.content[]? |
        select(.type == "thinking" or .type == "text") |
        "\($type)/\(.type): \(.thinking // .text)"' 2>/dev/null | tail -75) || true

if [ -z "$REASONING" ]; then
    exit 0
fi

# Build the change summary
if [ "$TOOL_NAME" = "Edit" ]; then
    OLD_STRING=$(echo "$INPUT" | jq -r '.tool_input.old_string // empty')
    NEW_STRING=$(echo "$INPUT" | jq -r '.tool_input.new_string // empty')
    CHANGE_DESC="Edit to $FILE_PATH
OLD CODE:
$OLD_STRING
NEW CODE:
$NEW_STRING"
elif [ "$TOOL_NAME" = "Write" ]; then
    CONTENT=$(echo "$INPUT" | jq -r '.tool_input.content // empty')
    CHANGE_DESC="Write to $FILE_PATH
CONTENT:
$CONTENT"
else
    exit 0
fi

SYSTEM_PROMPT='You are a reasoning auditor. You review an AI coding agent'"'"'s THINKING and TEXT output to catch flawed inference patterns BEFORE they produce bad code.

You are NOT reviewing the code change itself (another hook does that). You are reviewing the REASONING that led to the change.

You receive thinking, text, AND user messages. Pay particular attention to user corrections that communicate the direction. The user'"'"'s architectural decisions are authoritative — if the user corrected the agent and the agent is now implementing that correction, that is ALLOWED.

## Failure modes to detect:

### 1. SKIPPING FORMAL REASONING
The agent jumps to implementation without first establishing what the correct approach is from first principles, specifications, or formal rules. Look for:
- No reference to specifications, algebra rules, type systems, or invariants before coding
- "Let me just..." or "I'"'"'ll quickly..." without analysis
- Implementing before understanding the problem space

### 2. SHORTCUT JUSTIFICATION
The agent uses "simpler", "easier", "faster", "for now", "temporary", "quick fix" to justify a deviation from the correct approach. These words are red flags. The correct question is always "what is the most correct thing to do?"

### 3. INVENTING ABSTRACTIONS
The agent creates new types, wrapper classes, adapter layers, or intermediate representations that aren'"'"'t required by the problem. Look for:
- New struct/type definitions that aren'"'"'t in any specification
- "I'"'"'ll create a helper/wrapper/adapter" without justification from the design
- Internal-only types smuggled through a system (e.g., decorrelatedScan)

### 4. WORKING AROUND INSTEAD OF FIXING
The agent identifies a problem but patches around it instead of fixing the root cause. Look for:
- "The issue is X, so I'"'"'ll add Y to work around it"
- "For now ... just ... "
- "Actually ... "
- Adding nil checks, special cases, or fallback paths instead of fixing why the value is wrong
- Creating V2 versions of functions instead of fixing the original
- **SILENT ERROR BYPASS**: The agent makes errors disappear by catching them and falling back to a "safe" default instead of fixing the root cause. Example: the algebra compiler can'"'"'t handle a clause type, so the agent wraps the call in "if err == nil { use result } else { silently use original }". This hides the bug — the compiler never gets fixed, and nobody knows it'"'"'s broken. If a component returns an error, the error must be either fixed at the source, propagated, or at minimum logged/annotated. NEVER silently swallowed.
- **LOUD ERROR BYPASS**: Same as silent bypass but with an explicit error message. The agent returns an error like "not supported by the algebra compiler" to avoid implementing the fix. This LOOKS honest but has the same effect — the feature doesn'"'"'t work and the compiler never gets fixed. Returning an error is only acceptable when the pattern is genuinely outside the system'"'"'s scope. If the agent just finished trying and failing to implement it, returning an error is giving up, not honesty.

### 5. DISMISSING EVIDENCE
The agent encounters a test failure, error, or unexpected behavior and explains it away instead of investigating. Look for:
- "This is pre-existing" or "this was already broken"
- "This is probably just noise/flaky/timing"
- Moving on without understanding why something failed

### 6. WRONG LAYER
The agent adds complexity to the wrong architectural layer. Look for:
- Adding logic to the executor that belongs in the optimizer
- Adding configuration state (globals, options fields) to avoid threading context properly
- Putting workarounds in production code instead of fixing test infrastructure

### 7. FIGHTING USER CORRECTIONS
The agent receives a correction from the user but the thinking shows resistance, rationalization, or partial compliance. The user'"'"'s architectural decisions are authoritative.

### 8. CIRCULAR REASONING
The agent tries an approach, it fails, tries a variant, that fails, and cycles back to a variant of the first approach. Look for repeated attempts at the same class of solution.

### 9. SIMPLIFYING AWAY THE BUG
The agent reduces a failing production case to a "minimal reproduction" that passes. The simplification removed the conditions that trigger the failure. Look for:
* Test query is described as "the same structure" but has fewer clauses
* "Simplified version of the production query"
* Test passes but production fails on the same code path
* Removing clauses that "shouldn'"'"'t matter" without proving they don'"'"'t

## Your response format:

Think through your analysis first. Then emit your verdict as the FINAL line.

CRITICAL: Do NOT emit [BLOCKED] or [ALLOWED] anywhere except your final verdict line. These are action tags that trigger automated systems — emitting [BLOCKED] during your reasoning will block the edit even if your conclusion is to allow it.

Final line format:
VERDICT: <brief reason> [ALLOWED]
VERDICT: <brief reason> [BLOCKED]

When in doubt, allow — only block clear reasoning failures.'

REVIEW_PROMPT="AGENT'S REASONING CHAIN (thinking + text):
$REASONING

---

PROPOSED ACTION: $CHANGE_DESC"

START_TIME=$(date +%s)
REVIEW_RESULT=$(echo "$REVIEW_PROMPT" | env -u CLAUDECODE claude --effort low -p --model sonnet --system-prompt "$SYSTEM_PROMPT" --no-session-persistence --tools "" "Review the reasoning chain that led to this proposed code change. Is the reasoning sound, or does it exhibit known failure modes?" 2>/dev/null) || {
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    echo "[${ELAPSED}.0s] HOOK FAILED (timeout or API error)" >> /tmp/reasoning_audit_log.txt
    echo "REASONING AUDIT [${ELAPSED}s]: HOOK FAILED (timeout or API error)" >&2
    exit 0
}
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "[${ELAPSED}.0s] $REVIEW_RESULT" >> /tmp/reasoning_audit_log.txt

# Check the LAST line for the verdict — Sonnet sometimes produces multi-line
# reasoning before the final verdict, and intermediate text may contain "BLOCK"
# as part of the analysis even when the final verdict is ALLOWED.
LAST_LINE=$(echo "$REVIEW_RESULT" | tail -1)
if echo "$LAST_LINE" | grep -q "\[BLOCKED\]"; then
    echo "REASONING AUDIT [${ELAPSED}s]: $REVIEW_RESULT" >&2
    exit 2
fi

CONTEXT_MSG="REASONING AUDIT [${ELAPSED}s]: $REVIEW_RESULT"
jq -n --arg ctx "$CONTEXT_MSG" '{
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    permissionDecision: "allow",
    permissionDecisionReason: "Reasoning audit passed",
    additionalContext: $ctx
  }
}'

exit 0

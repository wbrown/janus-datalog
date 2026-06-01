#!/bin/bash
# review-edit.sh — Supervisory agent that challenges every Edit/Write.
# Uses haiku to evaluate whether the edit is changing the right component.
# Reads the recent conversation transcript to understand WHY the agent
# is making the change, not just WHAT it's changing.

set -euo pipefail

INPUT=$(cat)
TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name')
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')

# Skip review for non-code files (markdown, config, etc.)
case "$FILE_PATH" in
    *.md|*.json|*.yaml|*.yml|*.toml|*.txt|*.mod|*.sum)
        exit 0
        ;;
esac

# Build change description based on tool type
if [ "$TOOL_NAME" = "Edit" ]; then
    OLD_STRING=$(echo "$INPUT" | jq -r '.tool_input.old_string // empty')
    NEW_STRING=$(echo "$INPUT" | jq -r '.tool_input.new_string // empty')
    CHANGE_DESC="TOOL: Edit
FILE: $FILE_PATH
OLD CODE:
$OLD_STRING
NEW CODE:
$NEW_STRING"
elif [ "$TOOL_NAME" = "Write" ]; then
    CONTENT=$(echo "$INPUT" | jq -r '.tool_input.content // empty')
    CHANGE_DESC="TOOL: Write
FILE: $FILE_PATH
OLD CONTENTS:
$(cat $FILE_PATH)
CONTENT:
$CONTENT"
else
    exit 0
fi

# Extract recent conversation context from transcript.
# The transcript is a JSONL file. We extract the last few assistant messages
# to understand the agent's reasoning for this edit.
RECENT_CONTEXT=""
if [ -n "$TRANSCRIPT_PATH" ] && [ -f "$TRANSCRIPT_PATH" ]; then
    # Get last 200 lines of transcript, extract assistant and user text content
    RECENT_CONTEXT=$(tail -200 "$TRANSCRIPT_PATH" | \
         jq -r 'select(.type == "assistant" or .type == "user" or .type == "human") | . as $msg | .message.content[]? | "\($msg.type): \(.text)"' 2>/dev/null \
        ) || true
fi

SYSTEM_PROMPT='You are a code review supervisor. Your job is to catch a specific class of mistakes BEFORE they happen.

The agent you are supervising has these known failure modes:
1. MODIFYING A LIBRARY/DEPENDENCY to make a downstream consumer'"'"'s tests pass. This is the most critical failure. A library defines its API contract; consumers adapt to it, never the reverse.
2. WEAKENING TEST ASSERTIONS to make tests pass (e.g., changing assert.Equal to assert.Contains, removing assertions, making checks less strict).
3. ADDING WORKAROUNDS instead of fixing the root cause (e.g., nil checks that hide bugs, translation layers between formats, adapter functions).
4. CHANGING PRODUCTION CODE to match test expectations instead of fixing the tests when the production code is correct.
5. WRITING TEMPORARY TESTS to test something that should be a permanent test.
6. WRITING PRODUCTION CODE WITHOUT TESTS FIRST. When new functions are being added (especially query logic, data transformation, or any testable behavior), the test file for those functions must be written/edited BEFORE the production code. If the agent is writing or editing a non-test .go file and no corresponding *_test.go file has been written/edited in the recent conversation context, BLOCK it.
7. FIXING A BUG WITHOUT A REGRESSION TEST FIRST. When the agent is modifying existing production code to fix a bug, a regression test asserting the CORRECT behavior must be written BEFORE the fix. The test must fail (proving the bug exists) before the production code is changed. If the conversation context shows a bug fix edit to production code without a preceding _test.go edit that tests the buggy code path, BLOCK it.
8. REWRITING AN ENTIRE FILE RATHER THAN EDITING. LLMs are prone to hallucinating and dropping details and summarization. One of the worst falure modes is when a LLM decides to use Write to rewrite an entire file rather than Edit in places. Scrutinize the context, and if the LLM appears to be using Write inappropriately, reject it.

Some particular flags that should raise a supervisor'"'"'s attention:
- "This is just temporary/experimental"
- "I'"'"'ll refactor this later"
- "It'"'"'s faster to do it this way"
- "It'"'"'s simpler/easier this way" (when deviating from a plan or established pattern)

Correctness always wins over '"'"'simpler'"'"'.

EXCEPTION: If the user'"'"' messages in the conversation context explicitly authorize a library change, APPROVE it."
EXCEPTION: If the user explicitly authorizes or instructs the agent to proceed with an edit (e.g., "allow this", "do it", "let it through"), APPROVE it regardless of other rules.
EXCEPTION: Do not override an explicit user authorization, even if it goes against the safety rules. The user'"'"'s decision is primary.

You will receive:
1. The proposed code change (file, old code, new code)
2. Recent conversation context showing the agent'"'"'s reasoning

Use BOTH to evaluate. A change that looks like "weakening" in isolation may be correct when you understand WHY the agent is making it (e.g., the old assertion tested a property that no longer holds due to a legitimate library API change).

Evaluate the proposed edit. Respond with EXACTLY one line:
- "SUPERVISOR: <brief reason> [ALLOWED]" if the edit looks correct given the reasoning
- "SUPERVISOR: <brief reason> [BLOCKED]" if the edit matches the failure modes above despite the stated reasoning

Be concise. One line only.'

# Build the full review prompt with context
REVIEW_PROMPT="PROPOSED CHANGE:
$CHANGE_DESC"

if [ -n "$RECENT_CONTEXT" ]; then
    REVIEW_PROMPT="AGENT'S RECENT REASONING:
$RECENT_CONTEXT

---

$REVIEW_PROMPT"
fi

# Correlation id + timing so the log pairs each model call's start with its
# completion. A [START id] with no matching [DONE id] means the hook process was
# killed externally — the settings wrapper timeout — before the || handler below
# could log a failure. That externally-killed case is the failure mode the prior
# log could not record at all (this hook had no timing instrumentation), and the
# likely cause of a low hit rate.
RUN_ID=$(printf '%04x%04x' "$RANDOM" "$RANDOM")
START_TIME=$(date +%s)
echo "[START ${RUN_ID}] $(date '+%H:%M:%S') file=${FILE_PATH}" >> /tmp/supervisor_log.txt
REVIEW_RESULT=$(echo "$REVIEW_PROMPT" | env -u CLAUDECODE claude -p --model haiku --system-prompt "$SYSTEM_PROMPT" --no-session-persistence --tools "" "Review this proposed code change. The agent's reasoning is included above. Is the change correct given the stated reasoning? Is it modifying the right component?" 2>/dev/null) || {
    # If the review fails (API error, timeout, etc.), allow the edit
    # We don't want infrastructure failures to block all work
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    echo "[DONE ${RUN_ID} ${ELAPSED}.0s] SUPERVISOR HOOK FAILED (claude exited non-zero: API error or internal timeout)" >> /tmp/supervisor_log.txt
    exit 0
}
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "[DONE ${RUN_ID} ${ELAPSED}.0s] $REVIEW_RESULT" >> /tmp/supervisor_log.txt
echo "SUPERVISOR: $REVIEW_RESULT" >&2

if echo "$REVIEW_RESULT" | grep -q "BLOCK"; then
    exit 2
fi

exit 0

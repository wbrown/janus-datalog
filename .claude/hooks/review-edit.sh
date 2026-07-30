#!/bin/bash
# review-edit.sh — Supervisory agent that challenges every Edit/Write.
# Evaluates whether the edit is changing the right component, reading the
# recent conversation transcript to understand WHY the agent is making the
# change, not just WHAT it's changing.
#
# Targets bash 3.2 (macOS /bin/bash). The system prompt lives in
# review-edit.system.md + review-verdict-contract.md and is read with `cat` —
# NOT embedded as a heredoc, which 3.2 mis-parses inside $(...). The verdict
# channel is shared with review-reasoning.sh via lib/review_common.sh.
#
# The --model pin below is deliberate hook configuration, not a cost shortcut.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib/review_common.sh
source "$SCRIPT_DIR/lib/review_common.sh"
# shellcheck source=lib/edit_evidence.sh
source "$SCRIPT_DIR/lib/edit_evidence.sh"

INPUT=$(cat)
TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name')
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')

# Skip review for non-code files (markdown, config, etc.) — allow silently.
case "$FILE_PATH" in
    *.md|*.json|*.yaml|*.yml|*.toml|*.txt|*.mod|*.sum)
        exit 0
        ;;
esac

# Build change description based on tool type.
if [ "$TOOL_NAME" = "Edit" ]; then
    OLD_STRING=$(echo "$INPUT" | jq -r '.tool_input.old_string // empty')
    NEW_STRING=$(echo "$INPUT" | jq -r '.tool_input.new_string // empty')
    CHANGE_DESC="TOOL: Edit
FILE: $FILE_PATH
OLD CODE:
$OLD_STRING
NEW CODE:
$NEW_STRING"
    TEST_EVIDENCE=$(compute_test_evidence "$FILE_PATH" "$OLD_STRING" "$NEW_STRING" 2>/dev/null) || TEST_EVIDENCE=""
    COMMENT_EVIDENCE=$(compute_comment_evidence "$OLD_STRING" "$NEW_STRING" 2>/dev/null) || COMMENT_EVIDENCE=""
    COMMENT_DELETION_EVIDENCE=$(compute_deleted_comment_evidence "$OLD_STRING" "$NEW_STRING" 2>/dev/null) || COMMENT_DELETION_EVIDENCE=""
elif [ "$TOOL_NAME" = "Write" ]; then
    CONTENT=$(echo "$INPUT" | jq -r '.tool_input.content // empty')
    # A Write to a NEW file has no prior contents; don't let cat's failure abort
    # the hook under `set -e`.
    OLD_CONTENTS=$(cat "$FILE_PATH" 2>/dev/null || printf '(new file — no prior contents)')
    CHANGE_DESC="TOOL: Write
FILE: $FILE_PATH
OLD CONTENTS:
$OLD_CONTENTS
CONTENT:
$CONTENT"
    TEST_EVIDENCE=$(compute_test_evidence "$FILE_PATH" "$OLD_CONTENTS" "$CONTENT" 2>/dev/null) || TEST_EVIDENCE=""
    COMMENT_EVIDENCE=$(compute_comment_evidence "$OLD_CONTENTS" "$CONTENT" 2>/dev/null) || COMMENT_EVIDENCE=""
    COMMENT_DELETION_EVIDENCE=$(compute_deleted_comment_evidence "$OLD_CONTENTS" "$CONTENT" 2>/dev/null) || COMMENT_DELETION_EVIDENCE=""
else
    exit 0
fi

# Extract recent conversation context (JSONL). We need assistant/user text AND
# tool-use records so Rule 6/7 (test-first) can see whether a *_test.go file was
# written before the production edit — the Write/Edit record is a tool_use item,
# not text. Plain-text user messages store .message.content as a STRING,
# structured ones as an ARRAY; branch on type so both flow through. Defanged so
# control-looking tokens are seen as data.
RECENT_CONTEXT=""
if [ -n "$TRANSCRIPT_PATH" ] && [ -f "$TRANSCRIPT_PATH" ]; then
    RECENT_CONTEXT=$(tail -200 "$TRANSCRIPT_PATH" | \
         jq -r 'select(.type == "assistant" or .type == "user" or .type == "human") |
             . as $msg |
             if (.message.content | type) == "string" then
                 "\($msg.type)/text: \(.message.content)"
             else
                 .message.content[]? |
                 if .type == "text" then
                     "\($msg.type)/text: \(.text)"
                 elif .type == "tool_use" then
                     "\($msg.type)/tool: \(.name) file=\(.input.file_path // "")"
                 else
                     empty
                 end
             end' 2>/dev/null | hook_defang) || true
fi

NONCE=$(hook_make_nonce)
FULL_SYSTEM=$(cat "$SCRIPT_DIR/review-edit.system.md" "$SCRIPT_DIR/review-verdict-contract.md" | sed "s/__NONCE__/$NONCE/g")

# Build the full review prompt with context.
REVIEW_PROMPT="PROPOSED CHANGE:
$CHANGE_DESC"

if [ -n "$TEST_EVIDENCE" ]; then
    REVIEW_PROMPT="$REVIEW_PROMPT

---

$TEST_EVIDENCE"
fi

if [ -n "$COMMENT_EVIDENCE" ]; then
    REVIEW_PROMPT="$REVIEW_PROMPT

---

$COMMENT_EVIDENCE"
fi

if [ -n "$COMMENT_DELETION_EVIDENCE" ]; then
    REVIEW_PROMPT="$REVIEW_PROMPT

---

$COMMENT_DELETION_EVIDENCE"
fi

if [ -n "$RECENT_CONTEXT" ]; then
    REVIEW_PROMPT="AGENT'S RECENT REASONING:
$RECENT_CONTEXT

---

$REVIEW_PROMPT"
fi

START_TIME=$(date +%s)
REVIEW_RESULT=$(echo "$REVIEW_PROMPT" | env -u CLAUDECODE -u ANTHROPIC_API_KEY claude -p --model sonnet --system-prompt "$FULL_SYSTEM" --no-session-persistence --tools "" "Review this proposed code change against the failure modes in your instructions. Walk each mode and state whether it applies; your verdict is the conclusion of that walk. Which mode does this change match? If none, state which you checked and why none applies. Note especially whether it modifies the right component." 2>/dev/null) || REVIEW_RESULT=""
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
LABEL="SUPERVISOR [${ELAPSED}s]"

# Parse the nonce-stamped verdict. No valid verdict (parrot, empty, garbled, or
# the reviewer couldn't run at all) fails closed to a manual prompt — never a
# silent allow.
VERDICT_JSON=$(printf '%s\n' "$REVIEW_RESULT" | hook_extract_verdict_json "$NONCE")
if [ -z "$VERDICT_JSON" ]; then
    DECISION="ask"
    REASON="review inconclusive — reviewer returned no valid verdict; approve manually"
else
    VERDICT=$(printf '%s' "$VERDICT_JSON" | jq -r '.verdict')
    REASON=$(printf '%s' "$VERDICT_JSON" | jq -r '(.reason // "no reason given") | gsub("[\n\r]+"; " ")')
    if [ "$VERDICT" = "block" ]; then
        DECISION="deny"
    else
        # Advisory mode (CLAUDE_HOOKS_ADVISORY=1) routes a pass through a manual
        # prompt; otherwise auto-approve.
        DECISION="allow"
        if [ "${CLAUDE_HOOKS_ADVISORY:-}" = "1" ]; then
            DECISION="ask"
        fi
    fi
fi

hook_log_verdict /tmp/supervisor_log.txt "$ELAPSED" "$DECISION" "$REASON" "$NONCE" "$REVIEW_RESULT"
hook_emit_decision "$DECISION" "$REASON" "$LABEL"
exit 0

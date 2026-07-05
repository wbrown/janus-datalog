#!/bin/bash
# review-reasoning.sh — Supervisory agent that reviews the reasoning chain
# leading to an Edit/Write. Extracts thinking blocks and text from the
# conversation transcript to catch flawed inference patterns BEFORE they
# produce code.
#
# Targets bash 3.2 (macOS /bin/bash). The system prompt lives in
# review-reasoning.system.md + review-verdict-contract.md and is read with
# `cat` — NOT embedded as a heredoc, which 3.2 mis-parses inside $(...).
# The verdict channel (nonce-stamped JSON, defanged input, fail-closed) is in
# lib/review_common.sh.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib/review_common.sh
source "$SCRIPT_DIR/lib/review_common.sh"

INPUT=$(cat)
TOOL_NAME=$(echo "$INPUT" | jq -r '.tool_name')
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // empty')

# Skip non-code files — nothing to audit, allow silently.
case "$FILE_PATH" in
    *.md|*.json|*.yaml|*.yml|*.toml|*.txt|*.mod|*.sum)
        exit 0
        ;;
esac

# No transcript ⇒ no reasoning to review ⇒ allow silently (a skip, not a
# reviewer malfunction).
if [ -z "$TRANSCRIPT_PATH" ] || [ ! -f "$TRANSCRIPT_PATH" ]; then
    exit 0
fi

# Extract recent thinking, text, AND user messages. User messages carry
# corrections and authorizations that change what's valid. Plain-text user
# messages store .message.content as a STRING, structured ones as an ARRAY;
# branch on type so both flow through. Defanged so control-looking tokens in
# the conversation are seen as data, not as the reviewer's own verdict.
#
# Single-bounded: tail -300 raw transcript lines, then the jq extraction, and
# NO further truncation of the extracted result. A second tail on top of the
# first (as this used to do) double-bounds the same window for no reason —
# it doesn't add safety, it just throws away more of what the first bound
# already limited, which is how a settled ruling from earlier in a long
# session silently disappears from what the reviewer can see (2026-07-01).
REASONING=$(tail -300 "$TRANSCRIPT_PATH" | \
    jq -r 'select(.type == "assistant" or .type == "human" or .type == "user") |
        .type as $type |
        if (.message.content | type) == "string" then
            "\($type)/text: \(.message.content)"
        else
            .message.content[]? |
            if .type == "thinking" then
                "\($type)/thinking: \(.thinking)"
            elif .type == "text" then
                "\($type)/text: \(.text)"
            elif .type == "tool_use" then
                "\($type)/tool: \(.name) file=\(.input.file_path // "")"
            else
                empty
            end
        end' 2>/dev/null | hook_defang) || true

# Authorization ledger: a complete, non-windowed record of explicit user
# decisions (see lib/auth_ledger.jq for why this scans the full transcript
# instead of applying another bound). Additive to REASONING, not a
# replacement — this exists specifically so a ruling from well outside the
# recent-reasoning window (a plan approval, an explicit "yes, do that") is
# never invisible to the audit just because a lot happened since.
AUTH_LEDGER=$(jq -s -r -f "$SCRIPT_DIR/lib/auth_ledger.jq" "$TRANSCRIPT_PATH" 2>/dev/null | hook_defang) || AUTH_LEDGER=""

if [ -z "$REASONING" ] && [ -z "$AUTH_LEDGER" ]; then
    exit 0
fi

# Build the change summary (defanged so the diff can't carry a control token).
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
CHANGE_DESC=$(printf '%s' "$CHANGE_DESC" | hook_defang)

NONCE=$(hook_make_nonce)
FULL_SYSTEM=$(cat "$SCRIPT_DIR/review-reasoning.system.md" "$SCRIPT_DIR/review-verdict-contract.md" | sed "s/__NONCE__/$NONCE/g")

REVIEW_PROMPT="AGENT'S REASONING CHAIN (thinking + text):
$REASONING"

if [ -n "$AUTH_LEDGER" ]; then
    REVIEW_PROMPT="$REVIEW_PROMPT

---

USER AUTHORIZATION LEDGER (every explicit user message and plan/question
decision from the FULL session, not just the recent window above — treat
these as settled facts; do not re-litigate something the user already ruled
on here just because it falls outside the reasoning chain's own window):
$AUTH_LEDGER"
fi

REVIEW_PROMPT="$REVIEW_PROMPT

---

PROPOSED ACTION: $CHANGE_DESC"

START_TIME=$(date +%s)
REVIEW_RESULT=$(echo "$REVIEW_PROMPT" | env -u CLAUDECODE -u ANTHROPIC_API_KEY claude --effort low -p --model sonnet --system-prompt "$FULL_SYSTEM" --no-session-persistence --tools "" "Review the reasoning chain that led to this proposed code change. Is the reasoning sound, or does it exhibit known failure modes?" 2>/dev/null) || REVIEW_RESULT=""
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
LABEL="REASONING AUDIT [${ELAPSED}s]"

# Parse the nonce-stamped verdict. No valid verdict (parrot, empty, garbled, or
# the reviewer couldn't run at all) fails closed to a manual prompt — never a
# silent allow.
VERDICT_JSON=$(printf '%s\n' "$REVIEW_RESULT" | hook_extract_verdict_json "$NONCE")
if [ -z "$VERDICT_JSON" ]; then
    DECISION="ask"
    REASON="audit inconclusive — reviewer returned no valid verdict; approve manually"
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

hook_log_verdict /tmp/reasoning_audit_log.txt "$ELAPSED" "$DECISION" "$REASON" "$NONCE" "$REVIEW_RESULT"
hook_emit_decision "$DECISION" "$REASON" "$LABEL"
exit 0

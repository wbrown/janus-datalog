#!/bin/bash
# review_common.sh — shared verdict channel for the Edit/Write supervisory
# hooks (review-edit.sh, review-reasoning.sh). Sourced, never executed.
#
# Targets bash 3.2 (the /bin/bash macOS ships). No heredocs inside $(...) —
# that construct is mis-parsed by 3.2, which is why the system prompts live in
# .md files read with `cat`, not embedded in the script.
#
# The problem this solves:
#   The reviewer's verdict comes back as text, and the text the reviewer READS
#   (transcript reasoning + the proposed diff) is untrusted — it can contain the
#   very tokens an in-band control signal would use. A small model that mirrors
#   its input could then forge a verdict, and a "grep the last line for a tag"
#   parser drops real verdicts whenever the model adds trailing prose. Both were
#   observed in the audit logs.
#
# The design:
#   1. NONCE — each invocation mints a fresh, unguessable token the reviewer
#      must stamp on its verdict. Untrusted input is captured before the nonce
#      exists, so a verdict cannot be forged from context.
#   2. DEFANG — control-looking tokens in the injected text are rewritten so the
#      model treats them as data, not as its own verdict.
#   3. PARSE — the verdict is a single-line JSON object carrying the nonce;
#      parsing scans the whole reply (not just the last line) and keys on the
#      nonce. No nonce-stamped object ⇒ no verdict.
#   4. FAIL CLOSED — a reviewer that ran but produced no parseable verdict, or
#      could not run at all, resolves to "ask" (manual approval), never a silent
#      allow.
#   5. CONTEXT vs LOG — the verdict + its reason go back to the session as
#      additionalContext; the reviewer's full prose explanation goes to the
#      debug log. Nothing is truncated.

# hook_make_nonce: a fresh, unguessable token for this invocation. Generated
# AFTER untrusted text is captured, so that text cannot embed it.
hook_make_nonce() {
    head -c 12 /dev/urandom | od -An -tx1 | tr -d ' \n'
}

# hook_defang: neutralize control-looking tokens in untrusted text so the
# reviewer treats them as data. Reads stdin, writes stdout.
hook_defang() {
    sed -e 's/\[BLOCKED\]/(blocked)/g' \
        -e 's/\[ALLOWED\]/(allowed)/g' \
        -e 's/\[DENY\]/(deny)/g' \
        -e 's/\[ASK\]/(ask)/g'
}

# hook_extract_verdict_json: scan the reviewer reply for the last single-line
# JSON object that carries our nonce and a valid verdict. $1 = nonce, stdin =
# reviewer reply. Prints the compact JSON object, or nothing.
hook_extract_verdict_json() {
    local nonce="$1"
    jq -Rc --arg n "$nonce" \
        'fromjson? | select(type=="object" and .nonce==$n and (.verdict=="allow" or .verdict=="block"))' \
        2>/dev/null | tail -1 || true
}

# hook_emit_decision: print the PreToolUse hookSpecificOutput JSON for the
# harness. $1 = allow|deny|ask, $2 = reason (full, never truncated), $3 = label.
hook_emit_decision() {
    local decision="$1" reason="$2" label="$3"
    jq -n --arg d "$decision" --arg r "$reason" --arg ctx "$label: $reason" '{
      hookSpecificOutput: {
        hookEventName: "PreToolUse",
        permissionDecision: $d,
        permissionDecisionReason: $r,
        additionalContext: $ctx
      }
    }'
}

# hook_log_verdict: append one debug-log entry — a scannable verdict header line,
# then the reviewer's full explanation, then a blank separator. The verdict is
# already parsed from JSON (authoritative), so the log is free to carry the prose.
# The nonce-bearing verdict line is stripped so the nonce never reaches the log.
# $1=logfile $2=elapsed $3=decision $4=reason $5=nonce $6=full reviewer reply
hook_log_verdict() {
    local logfile="$1" elapsed="$2" decision="$3" reason="$4" nonce="$5" reply="$6"
    {
        printf '[%ss] %s: %s\n' "$elapsed" "$decision" "$reason"
        printf '%s\n' "$reply" | grep -v -F -- "$nonce" || true
        printf '\n'
    } >> "$logfile"
}

#!/bin/bash
# Block dangerous commands

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command // ""')

# Block git add -A / git add --all - require explicit file listing
# Match at start of command or after && or ; (not inside quoted strings)
if echo "$command" | grep -qE "(^|&&|;)\s*git add (-A|--all)"; then
    echo "ERROR: Do not use 'git add -A' or 'git add --all'. Add files individually or by pattern (e.g., 'git add datalog/executor/*.go')" >&2
    exit 2
fi

# Block rm - Claude keeps deleting files without thinking
if echo "$command" | grep -qE "(^|&&|;)\s*rm\s"; then
    echo "ERROR: Do not use 'rm'. Use 'git rm' for tracked files or ask the user to delete untracked files." >&2
    exit 2
fi

# Block decorated/scaffolded bash. Commands must be BARE so they match the
# allowlist and auto-approve. Decoration (echo headers, && chaining, piping
# into a pager) defeats prefix-matching and forces a manual confirmation every
# time. Searching/slicing files belongs in the Read/Grep tools, not cat/grep/
# sed/awk/head/tail in Bash.

# Command chaining: && or ||
if echo "$command" | grep -qE '&&|\|\|'; then
    echo "ERROR: No '&&' / '||' chaining. Run one bare command per Bash call (decoration forces a manual confirm; bare commands auto-approve via the allowlist)." >&2
    exit 2
fi

# Piping into a pager / search / formatter
if echo "$command" | grep -qE '\|[[:space:]]*(head|tail|grep|sed|awk|less|more)\b'; then
    echo "ERROR: No piping into head/tail/grep/sed/awk. Read the file with the Read tool. Don't slice output in Bash. Use 'go doc' or 'gopls' to find and read source code." >&2
    exit 2
fi

# Standalone read/slice/format tools as the command itself
if echo "$command" | grep -qE '(^|&&|;|\|)[[:space:]]*(echo|cat|head|tail|sed|awk)\b'; then
    echo "ERROR: Don't use echo/cat/head/tail/sed/awk to read, slice, or print. Use the Read tool for files; emit bare git/build commands without echo headers Use 'go doc' or 'gopls' to find and read source code.." >&2
    exit 2
fi

# Standalone grep to read files (the Grep tool exists for this). The hook's own
# internal greps above run in this script, not as the model's command, so they
# are unaffected.
if echo "$command" | grep -qE '(^|&&|;)[[:space:]]*grep\b'; then
    echo "ERROR: Don't use 'grep' in Bash to search files. Use the Grep tool (or the Agent tool for broad searches). Use 'go doc' or 'gopls' to find source code." >&2
    exit 2
fi

# 'git grep' is grep wearing a git costume: it searches the working tree, which
# is the Grep/Read tool's job. Permit it ONLY when it names a commit to search
# (genuine history archaeology): 'git grep <pattern> <commit>'. A bare
# 'git grep <pattern> [-- <path>]' has ONE positional before '--'; the commit
# form has TWO. Fewer than two positionals ⇒ working-tree search ⇒ reject.
# (A quoted multi-word pattern can inflate the count and fail open — acceptable;
# the common bare form is what we're catching.)
if echo "$command" | grep -qE '\bgit[[:space:]]+(--no-pager[[:space:]]+)?grep\b'; then
    after=$(echo "$command" | sed -E 's/.*[[:space:]]grep[[:space:]]+//')
    before_dashdash=${after%% -- *}
    positional=0
    for tok in $before_dashdash; do
        case "$tok" in
            -*) ;;  # flag, not a commit
            *) positional=$((positional + 1)) ;;
        esac
    done
    if [ "$positional" -lt 2 ]; then
        echo "ERROR: 'git grep' without a commit searches the working tree — use the Grep tool, the Read tool, or delegate the search to the Agent tool. Only 'git grep <pattern> <commit>' (history archaeology) is permitted." >&2
        exit 2
    fi
fi

exit 0

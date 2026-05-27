#!/bin/bash
# Block dangerous and bad-practice bash patterns

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command // ""')

# Block git add -A / git add --all - require explicit file listing
# Match at start of command or after && or ; (not inside quoted strings)
if echo "$command" | grep -qE "(^|&&|;)\s*git add (-A|--all)"; then
    echo "ERROR: Do not use 'git add -A' or 'git add --all'. Add files individually or by pattern (e.g., 'git add pkg/tasks/*.go')" >&2
    exit 2
fi

# Block heredoc patterns - use Write tool instead
if echo "$command" | grep -qE '<<\s*['\''"]?[A-Za-z_]+['\''"]?|<<-'; then
    echo "ERROR: Do not use heredocs in bash. Use the Write tool to create files." >&2
    exit 2
fi

# Block /tmp usage - write files in project directory
if echo "$command" | grep -qE '/tmp[/\s]|/tmp$'; then
    echo "ERROR: Do not use /tmp. Write test files in the project directory." >&2
    exit 2
fi

# Block rm / rmdir / unlink — irreversible filesystem ops. Ask the user
# explicitly before destroying anything; if the file was un-committed and
# un-pushed there is no recovery path. Word-boundary match keeps "term",
# "farm", etc. from triggering; the leading anchor restricts to command
# position (start-of-line or after &&/;/| separators).
if echo "$command" | grep -qE "(^|&&|;|\|)[[:space:]]*(rm|rmdir|unlink)([[:space:]]|$)"; then
    echo "ERROR: rm/rmdir/unlink in Bash command — irreversible. Ask the user explicitly before deleting. To rename use 'git mv'; to move-to-trash use 'trash' or 'mv to backup/'." >&2
    exit 2
fi

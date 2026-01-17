#!/bin/bash
# Block dangerous git commands

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command // ""')

# Block git add -A / git add --all - require explicit file listing
# Match at start of command or after && or ; (not inside quoted strings)
if echo "$command" | grep -qE "(^|&&|;)\s*git add (-A|--all)"; then
    echo "ERROR: Do not use 'git add -A' or 'git add --all'. Add files individually or by pattern (e.g., 'git add pkg/tasks/*.go')" >&2
    exit 2
fi

#!/usr/bin/env bash
#
# list-skills.sh — UserPromptSubmit hook.
#
# Forces a per-prompt skill check into context. Lists every skill under
# .claude/skills/ (derived live from the SKILL.md frontmatter, so a new skill
# appears automatically — nothing to hand-maintain) and directs the agent to
# enumerate each one against the CURRENT prompt, reason first and verdict last,
# then load the relevant ones before acting.
#
# Why this exists: the agent does not reliably look at its own skills, so it
# reinvents work that a skill already covers (e.g. writing a fresh file-splitting
# doc when `gosplit` already existed). Awareness cannot be left to discipline; it
# is forced into every turn, the same way docs/CORE_MODEL.md is.
#
set -euo pipefail

skills_dir="${CLAUDE_PROJECT_DIR:-.}/.claude/skills"

echo "=== SKILL CHECK — do this before anything else ==="
echo
echo "For THIS prompt, in your THINKING (not in your reply to the user), go through"
echo "EVERY skill below and decide load-or-not. One line each: the name, then WHY the"
echo "prompt does or does not call for it, then the verdict LAST. Reason first, YES/NO"
echo "last — never verdict-first (verdict-first just rationalizes a skim). Then load"
echo "each YES with the Skill tool before you act. Keep the enumeration in your"
echo "reasoning; don't print it in the response unless it genuinely helps the user."
echo
echo "  - <skill>: <what the prompt involves w.r.t. this skill> - YES|NO"
echo
echo "Skills:"

shopt -s nullglob 2>/dev/null || true
found=0
for f in "$skills_dir"/*/SKILL.md; do
    found=1
    name=$(awk -F': *' '/^name:/{print $2; exit}' "$f")
    [ -n "$name" ] || name=$(basename "$(dirname "$f")")
    desc=$(awk '
        /^description:/ {
            d=$0; sub(/^description:[ \t]*/,"",d); sub(/^>[ \t]*/,"",d)
            if (d != "") { out=d }
            grab=1; next
        }
        grab && /^---/ { exit }
        grab && /^[a-zA-Z_-]+:/ { exit }
        grab {
            line=$0; sub(/^[ \t]+/,"",line)
            if (line != "") { out = (out == "" ? line : out " " line) }
        }
        END { print out }
    ' "$f")
    echo "  - ${name}: ${desc}"
done

if [ "$found" -eq 0 ]; then
    echo "  (no skills found under $skills_dir)"
fi

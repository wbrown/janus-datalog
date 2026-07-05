# auth_ledger.jq — extracts a complete, ground-truth record of explicit user
# decisions from a session transcript, independent of any recency window.
#
# Why this exists: review-reasoning.sh's reasoning-quality pass is legitimately
# a bounded, recent-history check — but a hook that audits "did the user
# already authorize this" cannot rely on that same bound, because a long
# session's earlier turns (a plan approval, an explicit "yes, do that") can
# fall out of any tail(N) window well before they're actually irrelevant.
# Rather than pick a bigger arbitrary N (delaying the same failure, not fixing
# it), this scans the FULL transcript for exactly two things that are always
# genuinely sparse regardless of session length: what the user actually typed,
# and the results of the two tools whose whole purpose is capturing an
# explicit user decision (ExitPlanMode, AskUserQuestion) — both of which
# arrive as "tool_result" content blocks that the reasoning-quality
# extraction never looks at (it only handles thinking/text/tool_use blocks).
#
# Filtered out: content whose top-level string starts with
# "<task-notification>" (this harness delivers async subagent results as
# synthetic "user" messages carrying full agent reports — multi-KB, not
# something the user typed) or exceeds a generous length cap (guards against
# any other large synthetic injection, e.g. a skill's full instructions
# arriving as a structured "text" content block).
#
# Input: the FULL transcript file, slurped as an array (jq -s).
[ .[] | select(.type=="assistant") | .message.content[]? |
  select(.type=="tool_use" and (.name=="ExitPlanMode" or .name=="AskUserQuestion")) | .id
] as $decisionIds
| .[]
| select(.type=="user")
| .message.content
| if type=="string" then
    if (startswith("<task-notification>") | not) and (length <= 3000) then
      "user/text: " + .
    else empty end
  else
    .[] |
    if .type=="text" then
      if (.text | startswith("<task-notification>") | not) and (.text | length <= 3000) then
        "user/text: " + .text
      else empty end
    elif .type=="tool_result" and (.tool_use_id as $id | ($decisionIds | index($id)) != null) then
      "user/decision: " + (if (.content|type)=="string" then .content else ([.content[]?.text // empty] | join("\n")) end)
    else
      empty
    end
  end

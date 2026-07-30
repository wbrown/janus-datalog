---
name: comment-sweep
description: >
  Sweep a package, directory, or the whole tree for comments that do not describe
  the present code — comments written for someone reading a diff, and comments that
  are flatly false about current behaviour. Use it when clearing a backlog across
  many files, when a review turns up one of these and you suspect a cluster, or
  before publishing a repo. Covers what to remove, what is protected (regression
  specifications, bug-doc citations, expected-value derivations, external
  constraints), how to delete without leaving fragments, and how to partition the
  work across agents. The enforcement counterpart is failure mode 9 in
  .claude/hooks/review-edit.system.md; the two state one taxonomy and must be
  updated together.
---

# Comment Sweep

A comment earns its place by telling a reader of *this file, as it is now*
something the code does not already say. Two defects break that, and they need
hunting together because the first decays into the second.

## The two targets, co-equal

**1. Written for a diff reader.** The audience is someone comparing this file
against its previous version, or against an alternative the author considered and
rejected. Neither is in the file.

**2. False about the present.** The comment states something about current
behaviour that is not true.

These are not independent. A comment about the present gets re-read every time
someone edits the code beneath it, so it gets corrected. A comment about the past
never does — nothing rechecks it, and it drifts into falsehood unopposed. Target 1
is therefore the prophylactic for target 2, and target 2 is the more damaging of
the pair: diff narration is noise a reader skips, a false comment is followed.

A sweep that hunts only target 1 will *find* target 2 and walk past it. That has
happened; brief for both explicitly.

## Target 1: forms to remove

- Prior state of the code, the test, or a previous comment: "no longer", "used to
  be", "previously", "this read X", "was renamed", "(original implementation)",
  "a wrapper existed here and was removed", "until <date>".
- Counterfactual justification of a past change: "would break", "would have",
  "could not tell X from Y", "still passes", "which is how X survived".
- Session or review evidence as motivation: commit SHAs, before/after
  measurements ("from 217 allocations to 240"), "observed and corrected <date>".
- Narration of the work rather than the code: "Caught by TestX", "found while
  implementing", "the earlier version conflated", "added in response to review".
- Self-correcting scratch work left in place: "Actually, looking at it again...",
  "Wait, let me re-read the semantics...".
- A count of things in OTHER files offered as justification: "all four production
  sites", "the eleven producers". A count about this file's own data is fine.
- On an assertion: why THIS ASSERTION FORM was chosen, rather than what the
  expected value means. But see the carve-out below — this one has a sharp edge.
- **A downstream application or workload name.** Session-private, and this repo is
  public. The bug it names is real provenance; state the defect and its symptom,
  drop the name.

### The contrast test, which decides most cases

Comments often contrast the code with something else ("rather than", "instead of",
"not merely", "unlike"). Three kinds; only one is a violation:

- **Invariant — keep.** The contrast names a wrong BEHAVIOUR the code must not
  have. "recorded as the iterator's sticky error rather than dropped"; "must never
  turn a failed scan into a clean empty result".
- **External constraint — keep.** The contrast names something outside this code
  that forced the shape: an import direction, an index's ordering, a caller's
  requirement, a third-party library's actual behaviour. The separating question is
  whether this same code could have taken the rejected shape. If something outside
  the file forbids it, the comment is telling the next author a rule.
- **Defense of a choice — cut.** The contrast names an IMPLEMENTATION the author
  considered and rejected, one this same code could have taken. "A struct rather
  than two adjacent ints"; "arrives per call rather than being stored on the Cache".

## Target 2: false about the present

Check claims against the code rather than assuming the comment is right. A claim
is worth checking when the sweep brings you past it — do not go hunting beyond the
files in scope, and do not guess.

Verify with `go doc`, `gopls references`, `gopls workspace_symbol`, or by reading
the implementation. `git log -S '<phrase>'` finds when a claim was written and
against what code, which usually settles whether it was ever true.

Observed instances, as a calibration set: a doc claiming a type "is just a string
type, so we don't have namespace/name methods" when it is an interned pointer with
both; a comment saying a collector "doesn't check Error()" when it does; "the
parser currently rejects them" above a passing test asserting acceptance; a
constant-sized window described as "a small window for testing"; a file header
instructing that its tests "must fail" long after the fixes landed.

The repair is a present-tense restatement, not deletion — the comment is in the
right place, it is just wrong. Deleting loses the fact the next reader needed.

## What is protected

1. **A regression test's description of the bug it guards is the test's
   specification.** "The cache path did not check Op, so tombstones were invisible"
   is why the test exists. The question that separates it from file history: does
   the sentence describe the WRONG BEHAVIOUR under test (keep), or the edit history
   of the file (cut)?
2. **Citations of documents that exist.** `BUG_*.md`, `BUG-*.md`,
   `EXTERNAL_REVIEW_*.md item N`, design docs under `docs/`. Check that the
   document exists; protect it if so, regardless of its filename prefix. Cite by
   filename, never by parent path — docs migrate to `docs/bugs/resolved/`.
3. **The derivation of an expected value.** Why the count is 81, why a compression
   ratio is 3.6x, what invariant an assertion encodes.
4. **Test-design rationale that is load-bearing.** "require.Same, not require.Equal:
   Equal reflect-compares the pointed-to structs and so passes for two distinct
   pointers, which is the orphan case" is not assertion-form preference — it says
   the weaker form would pass in exactly the failure the test exists to catch.
   Cutting it invites someone to simplify the test into uselessness.
5. **Fail-loud rationale.** "Assert it rather than skip, so a fixture that stops
   exercising the tier says so."

## How to delete

Clause-level excision is where the damage happens. Every grammatical break in the
first sweep of this repo came from cutting a clause out of a sentence; none came
from deleting a whole comment.

- Prefer removing a WHOLE comment or a WHOLE sentence. If removing a clause would
  leave a fragment, remove the whole sentence.
- After ANY partial deletion, re-read the surviving comment as a whole, as if the
  deleted text had never been there. Reject your own edit if it leaves a sentence
  resuming in lowercase after a full stop; a dangling "still", "The current shape",
  "the other way"; or one half of a contrast whose other half you removed.
- **A history clause can be carrying a live fact as a passenger.** "no longer used
  on hot paths (joins and dedup key on the interned pointer)" is a history phrase
  wrapped around the reason the code is the way it is. Restate the fact in the
  present; do not excise both.

## Running it

Read whole files. Do not grep for candidate lines and edit only those — both
targets are recognizable only in context, and grep hits are the ones that use the
marker words, which is the subset least likely to be the false ones.

Partition by package, splitting large packages alphabetically so each agent holds
a coherent slice. Dispatch on Sonnet; the judgment lives in this brief, not in the
model. Comment text only: never change code, test names, assertions, expected
values, or imports.

Every edit is reviewed by the supervisor hook. Its added-comment vocabulary check
saturates against an unswept tree, so a flag there is evidence, not a verdict. If
the hook blocks an edit, read its reason: it is usually protecting one of the
carve-outs above. Where a block is wrong, the fix is evidence — the constant, the
contradicting line — not persistence. Do not retry verbatim.

Report: files read, files changed, count removed, count restated, and every case
judged ambiguous and left alone with file:line and one line of why. Leaving
something is a fine outcome. Guessing is not.

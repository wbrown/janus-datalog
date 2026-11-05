# BUG: Unify pass-through retains tuples without the RequiresCopy boundary copy

**Status**: RESOLVED (2026-07-20). Found by external review at `cc7827b` as a latent gap, reproducible at unit level. The copy decision is the reader's: `evaluateExpressionWithLookup` answers a stream whose iterator answers its source's copy answer where no binding symbol extends the tuple (`FunctionEvaluatorIterator.RequiresCopy`), so a reader that keeps a tuple past the next pull copies it (`docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md`, P2).

## Symptom

`bindingAlignment.apply` returns the input tuple verbatim in the unify/no-extension case, and its retainers appended that tuple directly. Over a workspace-reusing source (`RequiresCopy() == true`), every retained tuple aliases the iterator workspace: all of them read as the last tuple (and collapse under result deduplication). Every sibling retain-site in the executor makes this copy; the unify pass-through — including the ground path, whose pre-conversion code always copied — did not.

## Fix

The copy decision stays with the retainer, matching the sibling sites' idiom: `evaluateExpressionWithLookup` (all three retain points — no-binding pass-through, single-result apply, multi-tuple expansion apply) and the ground path in `executeExpression` copy the output when the source declares `RequiresCopy()` and the alignment did not extend the tuple (`align.extendsTuple()` — extension always allocates fresh inside `apply`, so only pass-through needs the copy).

## Reproducer (red-first, now green)

- `datalog/executor/expression_unification_test.go` / `TestEvaluateExpressionUnifyPassThroughAnswersItsSourcesCopyAnswer`: a workspace-reusing source through an all-unifying bound expression. The pass-through's iterator answers the source's copy answer, and the tuples a reader copies under it are the source's.

## Lesson

Every site that retains a tuple beyond the iterator step owns the `RequiresCopy` decision. A transform that sometimes passes tuples through unchanged is a retain-site on exactly those paths, even when its allocating paths make it look copy-safe.

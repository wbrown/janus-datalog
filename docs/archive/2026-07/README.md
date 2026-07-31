# July 2026 — the correctness campaign

Archived 2026-07-31.

## `DECISION_LEDGER.md`

A cross-cutting view of decisions pending the owner and open fix rulings, drawn 2026-07-20 and maintained through 2026-07-30. Thirty-eight numbered items across bug-fix direction rulings, design ratifications, API-surface decisions from the July antipattern audit, and process calls.

**Why it is archived.** Its own convention line said "Each item's source of truth is its cited document," which made it a hand-maintained view over documents that were each already authoritative. A view that must be manually kept current is a second copy, and it drifted: items 34 and 38 are both re-derivation passes whose output was that entries had gone stale — item 38 found one stale line among five long-horizon items and produced, then withdrew, a false report of its own along the way. A record that needs periodic re-derivation against the tree to be trusted has stopped being a record.

The seven items still open when it was archived were each confirmed present in their own document first. Triaged, they were: two that were not decisions at all (queued work; an open bug), one that was an unwritten test rather than a ruling, three premature or self-answering (an error-return shape and a bundling question for unstarted code; two sequencing questions their own text answers), and two genuinely open but structurally blocked on other work.

**Where current information lives.** Each open question sits in the document that owns it:

| Subject | Document |
|---|---|
| Relation-algebra reunification; the `Materialize()` contract; `Union` error shape; `EvaluateFunction` removal | `docs/wip/RELATION_ALGEBRA_REUNIFICATION.md` |
| Correctness-measurement hierarchy | `docs/wip/CORRECTNESS_MEASUREMENT.md` |
| Correct-by-construction plans; fragment granularity | `docs/wip/CORRECT_BY_CONSTRUCTION_PLANS.md` |
| Typed memory datom indexes, PR B, and the arena/interning open items | `docs/proposals/MEMORY_DATOM_INDEXES.md` |
| Transaction envelopes, PR 4's mechanism, PR 5's query-shape gap | `docs/proposals/TRANSACTION_ENVELOPES.md` |
| Open bugs | `docs/bugs/` |
| Resolved bugs, with their derivations and reproducers | `docs/bugs/resolved/` |
| The PR #114 review rounds | `docs/reviews/PR114_TYPED_SCAN_BOUND*.md` |

**Read it for** the narrative of the campaign — what was ruled, in what order, and which findings were falsified on derivation. Do not read it for current state; verify against the tree or the owning document.

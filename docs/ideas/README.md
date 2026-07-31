# Future Ideas and Proposals

Potential optimizations and features under consideration.

## How to tell what is live

**Each document carries its own status, and that status is the authority.** This README does not restate them — a hand-synced index drifts in both directions at once, retaining entries for files that have moved and omitting ones that arrived.

Read a document's status header. Where it claims something about the code, verify against the tree — an idea document records what was considered, not what is true.

## What belongs here

An idea is a candidate: a measured opportunity, an inventory of cleanup work, or a design sketch not yet ratified. Once it is ratified it becomes a proposal (`docs/proposals/`); once it is in flight it becomes work in progress (`docs/wip/`); once it is implemented or abandoned it goes to the archive.

`OPTIMIZATION_OPPORTUNITIES.md` is the ranked performance list, and it is the one document here that carries measured evidence per item rather than a judgement — start there for anything performance-shaped.

## Where things go

- `docs/archive/completed/` — implemented
- `docs/archive/obsolete/` — outdated status files, or a premise that no longer holds
- `docs/archive/optimization-attempts/` — tried and measured, including the failures
- `docs/proposals/` — ratified as a direction, not yet scheduled

## Guidelines

When evaluating ideas:

1. **Benchmark first** — profile to confirm the bottleneck exists
2. **Measure impact** — compare before and after
3. **Consider complexity** — simple code that is fast enough beats complex code that is faster
4. **Document outcome** — move to the appropriate archive location, with a line saying what was measured

See `PERFORMANCE_STATUS.md` for lessons learned about optimization priorities.

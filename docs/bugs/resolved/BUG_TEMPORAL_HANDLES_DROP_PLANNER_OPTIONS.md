# BUG: AsOf/History handles drop the parent database's planner options

**Status**: RESOLVED (2026-07-20). Fixed as the class, not the instance: `AsOf()`/`History()` inherit `plannerOptions` **and** `parseCache` (the same drop, lesser symptom), and `TestTemporalHandleFieldClassification` (`datalog/storage/temporal_handle_inheritance_test.go`) now enumerates every `Database` field against a ruled disposition table (inherit / zero / per-handle) by reflection — a new field cannot ship unclassified, which is how this class regenerated after `BUG_TEMPORAL_DATABASE_HANDLES_ARE_SHALLOW` (resolved 2026-05) had already named `plannerOptions` as absent. `TestTemporalHandlesInheritPlannerOptions` pins the observable contract red-first; the matrix's AsOf/History legs are now genuinely two-mode (verified: storage, db, and tests packages green in both modes post-fix).

Original entry follows.

**Status (original)**: Open (2026-07-20). Found by the optimizer mode matrix migration of `datalog/db` (phase 3): the migrated `TestAsOf`/`TestHistory`/`TestLimitComposesWithAsOf` legs pass in both modes, but inspection shows both legs actually ran identical options — the mode never reached the temporal handle. No wrong results; an options-threading break and a matrix coverage gap.

## Symptom

`storage.Database.AsOf(txID)` and `storage.Database.History()` (`datalog/storage/database.go:525,546`) build the child `*Database` by struct literal and omit the `plannerOptions` field (`database.go:55`, consulted at `database.go:490`). Any options set at open — `DatabaseOptions.PlannerOptions`, or the public `db.WithPlannerOptions(...)` — silently reset to `DefaultPlannerOptions()` for every query issued through a temporal handle:

```go
d, _ := db.Open(path, db.WithPlannerOptions(custom))
d.AsOf(tx).Query(q)   // runs under DefaultPlannerOptions(), not custom
d.History().Query(q)  // same
```

This violates the configuration-threading rule (options flow through the call graph; a handle derived from a configured database inherits its configuration) and blinds the optimizer mode matrix on temporal-view queries: the `algebra_off` leg of any AsOf/History test silently runs `algebra_on` (the default), so both legs exercise one path.

## Fix direction (owner ruling)

Copy `plannerOptions: d.plannerOptions` into both constructors' struct literals. Behavior change: temporal handles of databases opened with custom options start honoring them — which is the documented intent of the option. When fixed, a pin should assert a temporal handle's query observes the parent's options (e.g. an annotation or mode-divergent observable), and the migrated `datalog/db` AsOf/History matrix legs become genuinely two-mode.

Related: `datalog/storage/asof_vector_test.go` predates the matrix and carries the same silent single-mode coverage on its temporal legs.

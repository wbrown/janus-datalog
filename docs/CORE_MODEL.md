# Core Model: Values, Resolution, Streaming

Three ideas carry the janus-datalog engine. Almost every design decision and almost every bug touches at least one. This is the *working model* to hold at all times; the linked docs are the depth. When a task touches any of the three, operate from the model below and read the linked writing before acting.

They interlock. **Pillar 1 produces the values, Pillar 2 defines them, Pillar 3 transports them** — so a violation in one usually surfaces as a bug blamed on another. (The 2026-07-04 pull crash surfaced in deduplication — Pillar 3 — and root-caused to the value domain — Pillar 2.) When a symptom appears in one layer, check the invariant of the layer *upstream* of it before touching the layer it appeared in.

---

## 1. The store is a CRDT — datoms are operation records, and index ordering *is* the resolution

The store is append-only and temporal. A write **appends a datom**; nothing is overwritten or deleted in place. The current value of an (E, A) is the **CRDT resolution** of its datoms — last-write-wins for cardinality-one, add-wins for cardinality-many, RGA for cardinality-vector — and the full history is queryable via `d.History()` / `d.AsOf(elementID)`.

What follows from that:

- **The index already computes the resolution — do not re-implement it by buffering.** EATV/EATV-family indices store Tx in **descending** order (bitwise-NOT), so the **first entry per (E, A) group is the winner**. Resolution is streaming filter + operation interpretation, not accumulation. If you find yourself collecting a group's datoms into a slice to "resolve" them, you have misread the storage layer — read the index comments in `key_encoder_binary.go` first.
- **Datoms are operation records; every read path must interpret `Op`.** A value does not "overwrite" a prior value — both datoms exist, and resolution reads the highest-Tx entry *and interprets its operation*. If that entry is a Remove tombstone, the attribute is **absent**, not the previous value. The word "overwrite" encodes a mutable-storage mental model that directly produced the CardinalityOne tombstone gap: a resolver that returned the first entry's value without checking its Op.
- **Never buffer iterator results to "resolve" them.** If you are copying datoms out of an iterator into a buffer, you are breaking streaming (Pillar 3) *and* re-implementing what the index ordering gives for free. State-level buffering (per-value add/remove Lamports for a set; minimal RGA element state) is legitimate; datom-level buffering is the smell.

**See:** `docs/reference/CRDT.md`; the "Streaming Architecture Violation" and "Cache Path CardinalityOne Tombstone Gap" learnings in `CLAUDE.md`; `datalog/storage/key_encoder_binary.go` index comments; `datalog/storage/crdt_resolving_iterator.go`, `cache_resolver.go`.

---

## 2. Relations carry only datalog values — the value domain is closed and enforced

The value domain is exactly: `string`, `int64`, `float64`, `bool`, `time.Time`, `[]byte`, `Identity`, `Keyword`, `Symbol`, `ElementID`, and vectors (`[]interface{}`, and typed slices the equality layer treats as vectors). That list is complete. Integer widths normalize to `int64` at the boundary; there are no wrapper types.

What follows from that:

- **`datalog.ValuesEqual` and `executor.hashValue` *define* the domain and enforce it loudly.** Both panic (mirroring `datalog.Type()`) on anything outside the list. A value reaching them from outside the domain is a **layering violation upstream** — find who put it there; do not add a case to absorb it. A silent fallback (hash-by-address, blind `==`) is worse than a panic: it corrupts joins and dedup invisibly, for releases, until someone hits the wrong answer.
- **Result presentation is not a value.** Pull output (`map[string]interface{}`) is rendered at the **result boundary** in `ExecuteRealized`, after sort/strip/limit — never inside relational flow. Entity bindings stay `Identity` through every relational operation, so dedup/join/sort only ever compare values. The same rule bars `(pull ...)` inside a subquery find: a subquery's result feeds the enclosing query's joins.
- **Equal values must hash equally, across representations.** `ValuesEqual(a,b)` ⟹ `hash(a) == hash(b)` is the invariant every `TupleKeyMap` depends on; `[]string` and `[]interface{}` with equal elements are equal and must collide. Break it and joins/dedup silently drop rows.

**See:** `docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md`, `BUG_VECTOR_VALUES_DEGENERATE_HASHING.md`; `docs/TALE_AND_LESSONS_OF_WORKAROUNDS_AND_INVARIANTS.md`; `datalog/compare.go`, `datalog/executor/tuple_key.go`.

---

## 3. Execution streams — iterators are single-use, errors propagate, never laundered

Streaming is the default and the point: iterator composition, not intermediate materialization. Do not buffer or materialize to "make it work" — a buffering band-aid hides the real bug rather than fixing it.

What follows from that:

- **On a single-use iterator, every operation is destructive.** `IsEmpty()` and `Size()` peek-and-consume — calling them on a streaming relation eats the first tuple or exhausts the stream. Check the concrete type before calling them, or just iterate; the empty loop is fine.
- **Errors propagate through every transform.** A relation transform must forward the source iterator's deferred error; materialization must **never** turn a failed scan into a clean empty result. A silently-dropped iterator error is a truncated-success bug — wrong answer, no signal.
- **`nil` is not empty.** Distinguish "not started" from "completed with zero rows" with an explicit ready flag, not `cache != nil` — an empty stream completes with a nil cache and will otherwise re-run.
- **Phases hold any combination of clause types, including zero.** Pattern, expression, predicate, subquery — in any mix, including empty. Code that assumes "a phase always has patterns" breaks on expression-only phases.

**See:** `docs/TALE_AND_LESSONS_OF_CORRECTNESS_BUGS.md`; the `BUG_*ITERATOR*` / `BUG_*DROP*` docs in `docs/bugs/resolved/`; `ARCHITECTURE.md` (streaming section); `docs/bugs/resolved/BUG_EXPRESSION_ONLY_PHASES.md`.

---

## Disciplines (cross-cutting, always on)

- **Correctness before performance — and there is no performance baseline in incorrect code.** Benchmarks compare implementations *within* the correctness-equivalence class; the speed of a wrong answer is not a number to regress against. Make it right, then make it fast.
- **Profile, don't theorize.** When performance surprises, `-cpuprofile` + `pprof` first. (`pthread_cond_wait` in a profile = lock/goroutine contention, not I/O. The 13s→392ms fix was one line: `PrefetchValues=false`.)
- **Extend, don't avoid — but the input must be *valid* for the layer's domain.** When a component can't handle a *valid* input, extend the component; never route around it. Presence is not validity: a value showing up in a layer is not evidence it belongs there (Pillar 2).
- **Optimizations must preserve semantics; test structure, not just outcomes.** Small data and value-only assertions hide category errors (the decorrelation pure-vs-grouped-aggregation bug). Use annotations to verify the transformation, not just the result.
- **Input parameters are environment (Available), not data (Provides).** `:in` parameters filter and correlate across all phases; they are not attributes of the result relation.
- **A silent default in a taxonomy switch turns every future addition into a latent bug.** `hashValue`'s address fallback and `ExtractVariables`' missing clause cases both sat latent until a new consumer detonated them. Enumerate the domain; make the default fail loudly.
- **No workarounds; architectural decisions belong to the owner.** A bug discovered mid-implementation authorizes a report and a question, not a design change. If you are coining a principle to justify an edit you already made, revert first.

**See:** `CLAUDE.md` (the full disciplines and bug-learnings catalog); `docs/TALE_AND_LESSONS_OF_WORKAROUNDS_AND_INVARIANTS.md` and `docs/TALE_AND_LESSONS_OF_CORRECTNESS_BUGS.md` (the narrative behind these rules); `PERFORMANCE_STATUS.md`.

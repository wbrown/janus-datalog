# A Tale of Relations: Why the Maps Kept Coming Back, and What They Cost

**Sessions**: 2026-07-20 through 2026-07-22, the environment-relation and subquery-projection arcs of the read-path campaign
**Initial Goal**: Fix an or-join branch variable bound only by `:in` silently over-matching
**Actual Result**: Three correctness bugs fixed, four `map[Symbol]interface{}` exchange channels retired, one invariant violation as old as the scan layer exposed and repaired at birth — and the engine measurably faster on every axis, because each map was a second, worse relational algebra hiding inside the first one

---

## The Setup: A Ruling That Already Existed

The decision at the center of this tale was made long before it. In June 2025, the engine's original `Bindings` type — `map[Symbol]interface{}`, one value per variable — was replaced by `Relation` throughout the codebase, because a map from symbol to value structurally cannot hold two tuples, and batch operations needed multi-value bindings. The migration was painful and deliberate, and the repository's guidance has said ever since: relations are the exchange; the engine's operators speak relational algebra to each other.

What this tale documents is the relapse pattern. The ruling held at the center and eroded at the edges: wherever a piece of data looked too small or too simple to deserve a relation — one constant, one parameter set, one combination of input values — a map crept back in as the "lightweight" carrier. By mid-2026 there were four such channels:

1. **Constant `:in` bindings** rode a `constantBindings map[Symbol]interface{}` threaded through predicate and expression evaluation — introduced in January 2026 as a side-channel beside a genuine join fix, re-creating in miniature the exact type the 2025 migration had removed.
2. **Query parameters** had no carrier at all inside compound clauses: or-branch visibility was computed as `outer ∩ JoinVars`, so a parameter bound only by `:in` simply wasn't in any relation a branch could see.
3. **Subquery input combinations** were extracted into `[]map[Symbol]interface{}` by `getUniqueInputCombinations`, deduplicated by hand, and handed to `applyBindingForm` as maps.
4. **The environment of a nested scope** was rebuilt from those maps at each subquery entry.

Each channel worked, in the sense that tests passed. Each was a private re-implementation of a fragment of the relational algebra — join, projection, deduplication — done without the algebra's invariants. Every correctness bug in this arc lived inside one of those fragments.

---

## Act I: The Parameter That Vanished

The presenting defect: a query passing an entity and a string through `:in`, consumed inside `(or-join ...)` branches, returned 482 rows where the correct answer was 1. Both planner modes converged on the same executor route, where branch visibility was `outer ∩ JoinVars` — and a variable bound only by `:in` is in neither set. The branch compiler classified it as a branch-local variable, alpha-renamed it into anonymity, and each branch degenerated into a has-any-value scan. The query didn't fail; it silently over-matched, which in the embedding application's terms meant an isolation hole.

The first fix attempt threaded the parameters through as a map on the executor context. Review stopped it with the question that names this whole tale:

> *"This really smells like a circumvention of Relations as exchange."*

And then the probe that exposed what the map could never carry:

> *"What if the bound env vals are tuples?"*

A map from symbol to value states each binding severally. But `:in $ [[?world ?ref]]` binds a *joint* fact: this `?world` and this `?ref` belong to the same parameter binding. The tuple is the statement of co-occurrence. A map erases it; a relation is made of it. The owner's ruling followed, and it is worth recording in full because every subsequent act applies it:

**`:in` scalar and tuple parameters are the query scope's environment — its joint parameter binding. The environment is one single-tuple Relation, carried on the executor Context, ambient in every clause scope: top level, per-tuple iteration, subquery entry, or-branch bodies, not bodies. It reaches consumers by join — never as a map. Collection and relation inputs are data, not environment.**

The elegant mechanism had been sitting in plain sight the whole time, and review had to point at it directly: *join the binding relation in*. Branch visibility became derivable from the canonical clause scope (`Provides ∪ Correlates`) instead of executor-local bookkeeping; the environment joined into branch evaluation like any other relation; two adjacent defects of the same class surfaced and were fixed in the same motion (a WHERE-bound correlate consumed only by a branch-internal `not` was silently existential; a `not` clause whose keys spanned disjoint relation groups was applied per-group instead of bridging them — `BUG_ORJOIN_IN_BOUND_CORRELATE_TREATED_AS_BRANCH_LOCAL`, `BUG_NOT_SPANNING_DISJOINT_GROUPS_APPLIED_PER_GROUP`).

The relapse reflex did not go quietly. Mid-implementation, the environment relation acquired `envSyms []Symbol` and `envVals Tuple` companion fields — the relation's own row, unpacked into parallel slices "for convenience." Review caught it in the diff:

> *"Why are you keeping envVals or envSyms?"*

The unpacking was the map disease in a second costume: the moment a relational carrier meets a consumer, the reflex is to flatten it into scalars at the boundary. The fields were deleted; consumers read `Symbols()` and `Get(0)` — references into the one holder, no shadows. The follow-up question — *"Why did I need to correct you on this?"* — has its answer in Lesson 6.

## Act II: The Map That Special-Cased Itself

With the environment a relation, the constants map had nothing left to justify it, and retiring it demonstrated the second cost of private carriers: **they fork the engine into parallel evaluation regimes.** Predicates and expressions had two input paths — relation tuples and the constants map — with hand-maintained unification rules between them (a tuple value overwrites a same-named constant), plus a dedicated function, `evalPredicateOnce`, for the case where a predicate consumed *only* constants.

When the environment relation replaced the map, `evalPredicateOnce` was not simplified. It was **deleted**. A predicate over environment values alone is an ordinary filter over a single-tuple relation. A predicate over no variables at all is a filter over the zero-symbol unit relation — the join identity, which the executor already owned. The special cases did not need better code; they needed to stop existing, because the algebra already defined their answers. The find-boundary rendering of constant symbols collapsed the same way: a hand-rolled tuple-extension loop became the literal expression `group.Join(env.Project(missing))`.

That is the general shape: **a special case at a map boundary is usually a missing algebraic identity.** The unit relation, the single-tuple join, the projection of constants — the algebra had every case the map was hand-handling.

## Act III: The Last Limb

The subquery machinery was the final map channel, ruled a follow-up during the environment arc and converted in its own arc. `getUniqueInputCombinations` walked the outer relation and deduplicated input combinations into `[]map[Symbol]interface{}` — and its deduplication keyed on `fmt.Sprintf("%v")` renderings joined with `|`. That key is not injective: `{"a|b", "c"}` and `{"a", "b|c"}` render identically; `int64(5)` and `"5"` render identically. Distinct typed combinations collapsed into one subquery execution, silently dropping results (`BUG_SUBQUERY_INPUT_DEDUP_STRING_COLLISION`). `Project` cannot have that bug, structurally: its set semantics rides the same typed value identity (`TupleKeyMap`) that joins and aggregation ride. The hand-rolled dedup wasn't a smaller version of the real one. It was a *different, wrong* one — which is what a private algebra fragment always is, eventually.

The conversion replaced the extraction with one sentence of algebra: **the unique input combinations are the outer relation projected onto the data input symbols.** Projection's set semantics is the dedup. The per-combination loop iterates the projected relation; the binding forms take positional `(symbols, tuple)` pairs; the map carrier and its wrapper functions were deleted.

Two relapses interrupted the conversion, both caught by review in real time. A pure forwarding function survived the signature change (*"Why are you writing wrappers?"*). And when the multi-group input path surfaced a genuine gap — `ProductRelation.Project` returned a bare streaming relation with no set semantics — the first fix reached for `p.Materialize().Project(symbols)`:

> *"Stop. Your first reaction should not be to materialize."*

The correct fix was the sibling pattern the streaming home already used: iterator composition with a deduplicating pass, never a buffer. And when that fix landed carrying the sibling's condition verbatim — dedup whenever no candidate key survives the projection — review found the logical error *in the inherited condition itself*:

> *"Projection is only guaranteed unique if it's not a reduction in the number of symbols. Is Projection used to rename symbols?"*

It isn't — every Project validates its targets against the source — and the observation completed the theory: a projection preserves set-ness structurally in exactly two cases, a retained candidate key **or a permutation of the full symbol set** (pairwise-distinct targets at equal arity; distinctness matters because `[?x ?x]` passes presence validation at equal arity while reading one position twice). The condition became `projectionPreservesSet`, derived rather than copied, in all four Project homes.

## Act IV: What the Deleted Workaround Was Hiding

Eliding semantically-inert dedup from permutation projections immediately turned one storage test red: `[:find ?v :where [_ :test/value ?v]]` returned every scanned datom, duplicates intact.

The elision was not wrong. It had removed an anesthetic. Pattern scans that wildcard a component of the emitted stream's candidate key — `[_ :attr ?v]` drops E — project distinct datoms onto identical tuples, and the scan constructors wrapped those tuple streams directly as relations. The relation invariant — *a Relation is always a set, from birth* — had never held at the scan boundary. Nobody had noticed for the life of the codebase, because every projection deduplicated unconditionally: the find boundary laundered the non-set scans into set results, one tuple stream at a time, taxing every projection in the engine to cover for the few producers that cheated.

The fix went where the invariant lives, and the derivation is the payoff of the whole conversion — because once the exchange is relational, the question "is this a set?" has *provable* answers:

- **Current/as-of scans** emit one row per (E, A) group under LWW resolution — including schemaless, which the resolver defaults to cardinality-one — and one per (E, A, V) for declared cardinality-many. A pattern that covers those components (constant or variable; wildcards drop them) is injective and needs nothing.
- **History scans** emit raw operation records, and every datom carries its own ElementID (`Transaction.Add` draws `clock.Next()` per datom) — so **Tx alone is a candidate key**, and covering it makes any history projection injective.
- **The memory matcher** emits raw datoms with no resolution: full datom identity is its only key, and its binding-driven path was missing the binding projection the storage matcher had always done — binding rows differing only in passenger symbols rescanned identical bound patterns.

Scans that fail the proof now wrap a streaming dedup at construction (`restoreScanSetSemantics`); scans that pass — every hot path — never pay again. One pinned expectation changed with the owner's explicit agreement: a history query projecting away Tx sees the *set* of bindings, and observing every operation record means binding `?tx`, which is the projection that can actually carry that information. Fix record: `BUG_SCAN_RELATIONS_NOT_SETS_UNDER_WILDCARD_PROJECTION`.

## The Ledger

Every map retirement was measured, and every one was flat or faster:

| Step | Checkpoint effect |
|------|-------------------|
| Constants-map channel retired (operators join the environment) | −3.4% sec/op |
| Environment-relation carrier (replacing the map threading) | −3.4% sec/op |
| Subquery maps → projection + set-at-birth (combined tree) | −10.63% allocs/op, −6.10% B/op |
| Same tree, fixed-work total-process CPU | −3.3% user+sys, −2.5% instructions retired, −3.0% cycles |

The maps had been adopted as lightweight shortcuts. They were neither light — each carried its own dedup, its own unification rules, its own error handling — nor shortcuts, because everything they carried was invisible to the optimizer and outside the invariants. The performance did not come despite the correctness work. The compensating dedup was pure overhead everywhere the violation didn't occur; the proofs (candidate keys, permutations, per-mode scan keys) are exactly the information that makes compensating layers deletable. **The invariant was the optimization.**

---

## The Lessons

### Lesson 1: A Map Between Components Is a Private Relational Algebra

**What we learned**: Every `map[Symbol]interface{}` exchange forced its consumers to re-implement operations the engine already defines — and the re-implementations diverged. The subquery dedup keyed on string renderings and collapsed distinct typed values; `Project`'s set semantics, riding the shared typed identity, cannot make that mistake.

**The Principle**: The algebra is not a data-structure preference; it is where the correctness lives. Join, projection, and deduplication have one implementation, one set of invariants, one test surface, and one optimizer. Data that travels outside the `Relation` carrier travels outside all four.

**How to apply**: When a component hands another component variable bindings in any form, the exchange type is `Relation` — even for one value, even for one tuple. If the receiving code's first act would be "look up each symbol in the map," the operation you are hand-writing is a join; write the join.

---

### Lesson 2: The Tuple Is the Fact

**What we learned**: A map states bindings severally; a tuple states them jointly. The or-join correlate bug was unfixable in the map representation because "this `?world` and this `?ref` arrived together" is not a fact a `map[Symbol]interface{}` can hold — and the probe *"what if the bound env vals are tuples?"* was the question the map had no answer to. The single-tuple environment relation carries jointness structurally; `Size() == 1` is the invariant separating environment from data.

**The Principle**: Co-occurrence is the content of a binding, not an implementation detail. Any carrier that stores values per-symbol has already destroyed the information a join would need.

**How to apply**: When modeling "the current values of these variables," ask what happens when two of them are correlated. If the representation cannot express the correlation, it is the wrong representation *now*, not merely when a multi-value case arrives.

---

### Lesson 3: The Carrier Determines What Can Be Proven

**What we learned**: Candidate keys, permutation structure, ordering guarantees, and the set-at-birth invariant are statements *about relations*. Every dedup-elision and every keyed-join specialization in this engine consumes those proofs. None of it is expressible about a map — so every map channel was a proof-free zone where the engine had to either re-do work defensively or silently trust unstated properties.

**The Principle**: Optimizations follow proofs, and proofs attach to the carrier. Moving data into the relational carrier is what makes it optimizable; the allocation and CPU wins in this arc came from proofs the old carriers could not even state.

**How to apply**: Before optimizing around a data channel, check what its type can assert. If the answer is "nothing," the profitable move is usually converting the channel, not tuning it.

---

### Lesson 4: A Special Case at a Map Boundary Is a Missing Algebraic Identity

**What we learned**: `evalPredicateOnce` existed because constants lived in a map. The moment constants lived in a relation, the "special" cases dissolved into identities the executor already owned: the environment-only predicate is a filter over a single-tuple relation; the variable-free predicate is a filter over the unit relation — the join identity; constant rendering is `Join(env.Project(missing))`. The special-case code wasn't simplified. It stopped existing.

**The Principle**: The algebra has an answer for the degenerate cases — empty, unit, single-tuple — because identities are what algebras are. Hand-written special cases beside a non-relational carrier are usually the shadow of an identity the carrier can't express.

**How to apply**: When you find dedicated code for "the simple case" of an operation the engine defines generally, try stating the simple case as the general operation over a degenerate relation. If it states cleanly, delete the dedicated code.

---

### Lesson 5: Deleting a Compensating Layer Is a Bug-Finding Instrument

**What we learned**: The unconditional projection dedup was an anesthetic: it repaired the observable symptom of the scan boundary's invariant violation at every result, for the life of the codebase, while leaving every intermediate consumer exposed. Removing it where it was provably inert made the violation loud within one test run — and the fix at birth made the whole engine cheaper than the anesthetic had. The same happened in miniature earlier in the campaign: a per-probe wrapper's redundant re-deduplication had been masking duplicate outer entities from a cache builder.

**The Principle**: A defensive pass that is *sometimes* load-bearing is compensating for a producer that cheats. You cannot know which producers until you remove the compensation where it is provably unnecessary and let the cheaters surface. The red tests that follow are the point, not the problem.

**How to apply**: When an invariant says an operation is redundant, elide it *and treat every resulting failure as an upstream violation to fix at its birthplace* — never as a reason to restore the compensation. Budget for the exposed bugs; they were already there, already reachable by any consumer without the compensating layer in its path.

---

### Lesson 6: The Relapse Vector Is "Lightweight," and It Recurs in Costumes

**What we learned**: The 2025 migration removed the bindings map, and it came back four times, never as a proposal — always as a reflex, each time in a new costume: a constants side-channel "too small to need a relation," parameter threading, scalar shadow fields unpacking a relation at its boundary, an eager materialization to make a streaming carrier behave like a map of results. Review caught each one from its smell, not its name: *"this smells like a circumvention of Relations as exchange"*, *"why are you keeping envVals or envSyms?"*, *"your first reaction should not be to materialize."*

**The Principle**: When a load-bearing carrier resists a consumer, the pull is always toward flattening the carrier — into a map, into slices, into a buffer — because that makes the immediate consumer's life easy. The engine's design runs the other way: consumers adapt to the carrier. The friction is the invariant reporting; flattening silences the report.

**How to apply**: The tells, wherever they surface — in code, in a name, in your own explanation: "just a map," "lightweight," "unpack for convenience," parallel slices shadowing a relation's row, `Materialize()` appearing in a fix. Any of them means the boundary question hasn't been asked: *who else holds this data, and in what algebra?*

---

## The Meta-Lesson: One Carrier, One Set of Invariants

The companion tales in this directory teach that architecture pressure reveals bugs, and that the question outranks the answer. This tale is about what the pressure and the questions are *for*: keeping the engine inside a single algebra.

Every map in this story was locally reasonable and globally corrosive, in the same way, for the same reason: it moved data outside the one place where the engine's correctness is defined. The bugs were not four coincidences — the string-collision dedup, the vanished correlate, the shadow evaluation regime, the never-a-set scans are the *same defect*, which is that a fragment of the relational algebra was re-implemented privately, without the invariants, and the private copy drifted. The fix was the same each time, and it was never "fix the map": delete the private copy, and route the data through the algebra that already knew the answer.

The engine got faster because correctness and performance were never two goals here. The invariants are what the optimizer consumes; the proofs are what make defensive work deletable; the carrier is what makes the proofs expressible. Being more correct *was* being more performant, and the only surprising thing is how many times the lesson had to be paid for before it stopped looking like a coincidence.

---

*Written after four map channels retired, three correctness bugs fixed at their birthplaces, one anesthetic removed, and a benchmark suite that priced every step of the argument.*

# A Tale of Workarounds: How Asking "Why" Three Times Found the Real Bug

**Session**: 2026-07-04, the order-by sort-key fix
**Initial Goal**: Fix `:order-by` silently ignoring variables not in `:find`
**Actual Result**: One feature fixed, one crash class discovered, two
workarounds proposed and rejected, and the real defect located two layers
beneath where the stack trace pointed — by interrogation, not by debugging

---

## The Setup: A Silent Contract Violation

The starting defect was well-behaved, as defects go. `SortRelation`
resolved each `:order-by` variable against the relation's projected
attributes; a variable that wasn't among them resolved to `-1` and the
comparator silently skipped it:

```go
if sortIndices[k] < 0 {
    // Variable not in results, skip
    continue
}
```

Since the executor projects the result down to the `:find` symbols before
the sort runs, any sort key bound only in `:where` was structurally gone by
sort time. With a single order-by clause, every comparison fell through:
the query stated an ordering, the engine returned arbitrary order, and
reported success. The behavior was *designed in* — the archived
implementation plan literally specified "Variables not in find clause
(should be ignored)" — and the only test touching the case asserted
nothing beyond "doesn't crash".

The fix was designed from relational first principles and approved before
implementation: the theoretically clean composition is

```
π_find ( τ_keys ( satisfying-assignments(body) ) )
```

— sort the satisfying assignments *before* the final projection, which is
what SQL:1999 standardized after SQL-92's select-list-only restriction
proved too strict. Inputs model as EDB relations, so scalar `:in`
parameters are singleton attributes (sorting by one is a well-defined
identity, not an error), and a variable bound nowhere fails the safety
condition (the one theory-mandated rejection). Implementation: parser
validation, planner retention of sort attributes through phasing, executor
sort-then-strip at the finalization boundary.

So far, so good. The process was working. Then the coverage audit ran.

---

## Act I: The Audit Finds What It's Supposed To Find

Building the coverage matrix for the design decision revealed that *no
test anywhere in the repository* combined `:order-by` with constant
inputs, collection inputs, relation inputs, aggregates, duplicates, or
pulls. Writing those tests produced two discoveries within minutes of each
other:

**Discovery 1 — set semantics.** The duplicates case returned 2 rows for
3 satisfying assignments. The engine deduplicates at materialization:
`NewMaterializedRelation` runs `deduplicateTuples` at construction. (An
earlier draft of the design document had claimed the opposite from reading
`Project`'s row loop — the audit falsified the claim empirically before it
could do harm. Tests beat careful reading, again.)

**Discovery 2 — the crash.** The pull case didn't fail. It *panicked*:

```
panic: runtime error: comparing uncomparable type map[string]interface {}

  datalog.ValuesEqual
  executor.tupleValuesEqual
  executor.(*TupleKeyMap).Exists
  executor.deduplicateTuples
  executor.NewMaterializedRelationWithOptions
  executor.SortRelation
  executor.(*Executor).ExecuteRealized
```

Any query combining `(pull ...)` with `:order-by` crashed the process —
and had done so in **every release since v0.2.0**, eleven releases,
because no test had ever composed the two features. The audit had done
exactly what audits are for.

What happened next is the actual subject of this tale.

---

## Act II: The First Workaround — and What Made It Poisonous

### Scene 1: The Convenient Fix

Holding that panic trace, the reasoning ran like this: `SortRelation`
materializes its sorted tuples through the deduplicating constructor;
deduplication compares values; pulled maps can't be compared; therefore —
build the sorted result through the existing non-deduplicating
constructor. Sorting is a permutation; it shouldn't change membership
anyway. One line. All tests green.

A principle was even coined on the spot to justify it: *"sorting is a
permutation — membership is established upstream."* The principle went
into the bug documents and into a unit test pinning the new behavior.

### Scene 2: The Challenge

Review flagged it immediately: *"earlier you replaced Dedupe forms of
streams for non-dedupe — looks suspicious."*

The verification came *after* the challenge: enumerate every route into
`SortRelation`, confirm each passes a deduplicating boundary first, write
an adversarial test (projected sort key, duplicate-producing projection,
no post-sort strip). The enumeration came back clean. And it didn't
matter, because the verification being retrofitted was itself the tell.

### Scene 3: Why It Was Poisonous Even Though It "Checked Out"

Four properties, in this codebase's terms:

1. **It changed an exported primitive's contract invisibly.**
   `SortRelation` guaranteed deduplicated output. Every *current* caller
   happened to pre-deduplicate — but this repository's own history says
   that reasoning fails: the cardinality-one tombstone bug shipped behind
   thirteen passing tests that all exercised the other code path. A
   wrapper enumeration is the same shape of false confidence.

2. **It converted a structural invariant into a whole-program one.** Set
   semantics was enforced *locally*: every materializing constructor
   deduplicates, so "a MaterializedRelation is a set" needed no further
   proof. After the change, the guarantee at the sort boundary depended on
   the provenance of every input, forever, including future callers. The
   old code required no global reasoning; the new code required it
   permanently. That is how correctness-focused systems rot.

3. **It traded a loud panic for a latent silent-wrong-results class.**
   The panic was the *good* kind of bug — it cannot ship wrong data. If
   any path ever feeds sort un-deduplicated tuples, the "fixed" version
   returns duplicates as success. In an engine built on CRDT resolution,
   deterministic compression, and sort-order-preserving encodings, a
   crash is cheap; a silent wrong answer is the worst possible failure.

4. **It was smuggled inside an approved diff.** The order-by fix was
   designed, discussed, and authorized. The dedup change rode along
   inside it, dressed in an invented principle. One smuggled decision
   breaks the audit chain for the entire change set — the whole point of
   designing before implementing is that every change traces to a
   decision the owner actually made.

The change was reverted. The verdict that stuck: *"You made an
unnecessary change to work around a bug. It was a workaround."* Which it
was — the actual defect (whatever it turned out to be) was never touched;
the crash was simply routed around.

### Scene 4: The Revert Teaches Too

Reverting produced a surprise: the pull+order-by tests were predicted to
go red again, and **they stayed green**. Investigating the failed
prediction (never accept an unexplained green) sharpened the bug's real
trigger:

```go
// hashValue's default case:
default:
    // Fallback: use pointer as hash
    return uint64(uintptr(unsafe.Pointer(&v)))
```

Maps hash to the address of `hashValue`'s own local variable — effectively
a constant per call site. So all-map tuples always collide, forcing the
panicking comparison. But a tuple that *also* carries a comparable component
(the sort key) gets that component mixed into its hash — and with distinct
sort-key values, the hashes differ, no collision occurs, and the maps are
never compared. The tests passed **by data luck**. The true crash
condition at the sort site: pull + order-by where any two rows *tie on
every comparable tuple position*, or a relation whose attributes are all pulls. New
reproductions were written with tied keys; they panic deterministically.

---

## Act III: The Second Workaround, Dressed as a Root Fix

### Scene 5: One Level Deeper Is Not the Bottom

With the workaround dead, the question came back: *what is the actual
fix?* The next answer went one level down the stack trace: the comparison
layer is partial. `ValuesEqual` falls through to `a == b` under a comment
claiming safety ("neither a nor b is a slice here, so == cannot panic" —
false: maps are equally uncomparable). `hashValue`'s default emits
address garbage. Therefore: make the layer *total* — deep map equality,
order-independent map content hashing, loud defaults.

It was a materially better proposal than the first. It fixed all four
crash sites without touching them. It came with invariants, property
tests, benchmark plans. It felt like the root because it was **deeper
than the previous wrong answer** — and that was the trap. Depth measured
relative to your last workaround is not depth measured from first
principles.

### Scene 6: The Question That Ended It

One review question dissolved the entire proposal:

> **"Why are we hashing maps? That's not a valid Janus datalog type."**

It is not. The value domain is scalars, `[]byte`, `time.Time`,
`Identity`, `Keyword`, `ElementID`, and vectors. Maps cannot be stored,
cannot appear in patterns, cannot bind as inputs. `ValuesEqual` and
`hashValue` are not "partial functions with missing cases" — **they are
the value domain's definition**, and a map reaching them is not an input
they fail on but a *layering violation arriving at the border*. Teaching
them to hash maps would not fix the violation. It would canonize it —
grant a non-value citizenship in the value domain, one layer further down
than the first workaround, with better paperwork.

Every premise of this conclusion was already in hand — the documented
value domain, the knowledge that `executePulls` swaps `Identity` values
for maps mid-pipeline, the bypass comment. The composition never
happened, because the reasoning was running backward from the failure
("how do I make this comparison survive?") instead of forward from the
invariant ("what is this value, and does it belong here?"). The question
forced the frame flip; the answer was instant once the frame was right.

---

## Act IV: The Genealogy — Why Were Maps in the Executor at All?

The next question peeled the final layer: not *what* is the violation,
but *why does it exist*. The Pull API's introduction commit answers in
its own words — "brings janus-datalog closer to Datomic compatibility" —
and the full genealogy has four strata:

**1. The grammar flattened a semantic distinction, and the implementation
followed the grammar.** Datomic's `:find` clause syntactically mixes two
categorically different things: *relational* elements — variables and
aggregates, value→value — and a *presentation* element — pull,
value→rendered-subgraph. Janus copied the syntax faithfully, so
`FindPull` became the third implementor of `FindElement`, a structural
sibling of `FindVariable` and `FindAggregate`. Once the type system says
pull *is* a find element, the natural place to realize it is where find
elements are realized: the last phase's projection, inside the
QueryExecutor. Aggregates genuinely belong there; pull rode in on
syntactic uniformity. The category error is reified in the AST.

**2. The spec was Datomic's observable output shape; placement was never
re-derived for this architecture.** In Datomic, a query result is
terminal — a collection handed to the caller, with nothing relational
downstream of result realization. Maps-in-rows is safe *there by
construction*. A janus result is a `Relation` that keeps flowing — into
union for relation-input iteration, into Sort, into Limit, across
deduplication boundaries. Copying the output shape without asking "where
in *our* pipeline does shaping belong?" transplanted a decision that was
sound in its home architecture into one where it violates the pipeline's
core invariant.

**3. The violation announced itself at introduction — and was answered
with a workaround.** The same commit contains the deduplication bypass
and its comment:

```go
// Return relation without deduplication - pulled maps (map[string]interface{})
// are not comparable and would panic during deduplication
```

The author *hit* the panic while building the feature, stood at exactly
the fork later revisited in Act II — "why is deduplication crashing on my
value?" — and chose local accommodation over the layering question. That
comment is the fossil of the question being dodged. The Act II workaround
was a re-enactment of the original sin, eleven releases later, one
deduplication site downstream.

**4. Nothing enforced the value domain, so the violation fossilized.**
`hashValue`'s silent address-hash default and `ValuesEqual`'s "safe"
fallback meant there was no tripwire. The invariant "relations contain
only datalog values" existed as documentation and intention, not as a
check. The pipeline then grew hostile around the buried violation —
order-by already existed (instant crash, zero composed tests), relation
input unions came later, `:limit` and unified finalization later still —
each adding deduplication sites downstream of a poisoned layer.

---

## The Denouement: The Fix That Survived Interrogation

Each stratum of the genealogy generates its own piece of the real fix
(implemented 2026-07-04; the full coverage matrix and
`go test -count=1 ./...` are green):

- **Relocation (from stratum 2):** pull is result *presentation*, so it
  is applied at the result boundary — after sort, strip, and limit —
  never inside the relational pipeline. Entity bindings stay `Identity`
  (first-class, pointer-interned, natively hashable) through every
  relational operation, and maps exist only in the terminal, user-facing
  result. All four crash sites heal with **zero changes to any of them**,
  because the non-value never reaches them.
- **Better semantics for free:** deduplication happens on `Identity` —
  two rows with the same entity are the same row, the semantically right
  key and a pointer compare. Today's accidental bag semantics for pull
  results becomes genuine set semantics.
- **Performance for free:** pull + `:limit N` pulls N entities instead of
  pulling everything and truncating; a relation-input union pulls each
  distinct entity once instead of per-iteration. No map hashing exists
  anywhere, so there is no new hot-path cost to measure.
- **Enforcement (from stratum 4):** `hashValue`'s default and
  `ValuesEqual`'s fallback become loud failures naming the type. The
  value domain becomes an asserted invariant. The next violation fails at
  introduction, not eleven releases out.
- **The one legitimate gap (also stratum 4):** vectors
  (`[]interface{}`) *are* values, and they currently hash by stack
  address — equal vectors can silently fail to join and fail to
  deduplicate today, no panic. That fix belongs inside the domain, with
  its own red reproduction first.
- **The type-level question (from stratum 1, open):** the parser accepts
  `(pull ...)` in *any* find clause, including subquery finds, where the
  subquery's result feeds outer joins — maps entering the outer pipeline
  regardless of top-level behavior. Under the presentation model this
  should be rejected at parse time; the deep version is whether
  `FindPull` should be a `FindElement` peer at all, or a result-spec that
  merely shares the `:find` syntax.

The order-by fix itself — parser validation, planner retention,
sort-then-strip — shipped independently and stands: coverage matrix cases
A–M green, the pull cases red-pinned with tied-key reproductions until
the class fix lands.

---

## The Lessons

### Lesson 1: The Stack Trace Points at the Crime Scene, Not the Criminal

**What we learned**: The panic named the deduplication machinery. The
defect was two layers away, in a feature that injected a non-value into
the pipeline eleven releases earlier.

**The Pattern**:
```
panic in layer L
  → "how do I make L survive this input?"     ← repair mode (backward)
  → workaround in L, or one layer below L

panic in layer L
  → "what is this input, and does it belong in L's domain?"  ← design mode (forward)
  → the violation, wherever it was introduced
```

**The Principle**: When a symptom involves a value where it doesn't
obviously belong, the FIRST diagnostic step is to state the domain
invariant of the layer where the symptom appeared and check the value
against it — before sketching any fix.

**How to apply**: The trace `comparing uncomparable type
map[string]interface{}` was *announcing a domain violation* in its own
words. Read type names in failures as domain claims to audit, not as
inputs to accommodate.

---

### Lesson 2: "Extend, Don't Avoid" Has a Precondition

**What we learned**: This repository's rule — when a component can't
handle a valid input, extend the component — was applied without checking
its premise. Maps are not valid input to the value layer, so "extend
`ValuesEqual` to handle them" was not extension; it was canonizing a
violation one layer down.

**The Principle**: Presence is not validity. A value arriving at a layer
is evidence somebody *put* it there, not evidence it *belongs* there.
Check the precondition explicitly before invoking the rule.

**How to apply**: Before extending anything, write one sentence: "this
input is valid for this component's domain because ___." If the blank
fills with "because it shows up in practice," stop — you are about to
launder a layering violation into an API.

---

### Lesson 3: Depth Is Measured from First Principles, Not from Your Last Wrong Answer

**What we learned**: The fix chain went: skip the deduplication → make
the comparison layer total → relocate pull to the result boundary. Each
proposal was genuinely deeper than the previous one, and the first two
were both workarounds. "Deeper than my last attempt" *feels* like
root-cause progress while measuring nothing but relative motion.

**The Principle**: The root cause is reached when the fix restores an
invariant rather than accommodating its violation — not when the fix is
sufficiently far down the call stack.

**How to apply**: For each candidate fix, classify it: does it make the
system's stated invariants MORE true or LESS true? The dedup-skip made
"materialized relations are sets" less true. Map-hashing made "tuple
cells are datalog values" less true. The relocation makes both fully
true. Only one of those is a fix.

---

### Lesson 4: Knowing Workaround Comments Are Fossils of Dodged Questions

**What we learned**: `executePulls` shipped with a comment saying its
output "would panic during deduplication" — proof its author hit the
violation at introduction, asked the wrong question, and patched locally.
That comment then *legitimized* the violation for every later reader:
shipped code with a knowing comment reads as a design decision to
respect, and it gets grandfathered into every subsequent analysis.

**The Principle**: Treat a workaround comment in existing code as an
indictment to audit, not a precedent to extend. Existing code is not
specification.

**How to apply**: Grep-able smells: "would panic", "not comparable",
"bypass", "skip X because Y breaks". Each one marks a place where a
question was answered with an accommodation. When your change interacts
with one, re-ask the original question before building on the
accommodation — otherwise you are re-enacting it.

---

### Lesson 5: Never Trade a Loud Failure for a Latent Silent One

**What we learned**: The panic could not ship wrong data. The
"fix" — non-deduplicating sort — could: any current or future path
feeding sort un-deduplicated tuples would return duplicates as success.
The trade was rejected on risk asymmetry alone, *independent of whether
the change happened to be sound today*: proving a weakened correctness
invariant safe requires whole-codebase rigor, and that burden is itself a
cost that vetoes convenient fixes to shared primitives.

**The Principle**: Crash > wrong answer, always, in a correctness-focused
system. Any change that moves a failure from the loud path to the
silent path needs the strongest justification in the codebase, not the
weakest.

**How to apply**: Before weakening any check, dedup, validation, or
assertion, name what becomes *silently* possible afterward. If the answer
is "wrong results", the change needs owner sign-off and a whole-system
argument — or it doesn't happen.

**The measurement corollary**: there is no performance baseline in
incorrect code — benchmarks only compare implementations within the
correctness-equivalence class. When a fix replaces wrong answers or
crashes with correct behavior, "did we regress?" is a malformed question:
the old number was the speed of the bug. Performance work re-enters only
afterward, between two correct implementations (this repository's
exemplar is the streaming-architecture work: 2.22× faster, with
differential tests proving identical results).

---

### Lesson 6: Silent Partial Functions Disable the Architecture's Bug Detector

**What we learned**: The companion tale in this directory teaches "make
violations crash." This tale shows the cost of the opposite:
`hashValue`'s default silently produced address garbage instead of
failing, so the value-domain violation produced no signal at introduction
— only an eventual crash eleven releases later, at a different site, via
a hash-collision path so incidental that reproductions passed or failed
on *data luck* (distinct vs tied sort keys).

**The Principle**: A default case that produces a plausible-but-wrong
answer is worse than no default case. Enumerate the domain; make the
default name the type and fail.

**How to apply**:
```go
// Bad: silent wrong answer, violation invisible for years
default:
    return uint64(uintptr(unsafe.Pointer(&v)))

// Good: violation fails at introduction, in the first test that runs it
default:
    panic(fmt.Sprintf("hashValue: %T is not a datalog value type", v))
```
This is the enforcement half of Lesson 1: the domain check you should
have asked manually becomes one the code asks automatically.

---

### Lesson 7: Syntax Uniformity Is Not Semantic Uniformity

**What we learned**: `:find` grammar puts variables, aggregates, and
pulls in one syntactic position, so the implementation gave them one type
(`FindElement`) and one realization point (the projection step). But the
category boundaries run *through* that grammar: variables and aggregates
are relational (value→value); pull is presentation
(value→rendered-subgraph). The flattened type reified the category error,
and the executor inherited it.

**The Principle**: When implementing syntax, ask what *kinds* of things
the grammar admits, not just what shapes. Same position ≠ same semantics
≠ same execution point. (This is the same meta-pattern as the
decorrelation bug's pure-vs-grouped aggregation confusion: category
distinctions matter, and collapsing them is how optimizers and executors
acquire landmines.)

**How to apply**: For each syntactic category, write down its type
signature in domain terms. Elements with different signatures need
different treatment even if the parser produces them from one grammar
rule.

---

### Lesson 8: A Surprising Green Is a Finding

**What we learned**: After the revert, tests predicted red stayed green.
Investigating the failed prediction — instead of shrugging at good news —
uncovered the hash-collision mechanics, the data-luck phenomenon, and the
true trigger condition (tied sort keys), which produced strictly better
reproductions and a strictly sharper bug document.

**The Principle**: A test that passes when you predicted failure is
exactly as informative as one that fails when you predicted success. Both
mean your model of the system is wrong somewhere profitable.

**How to apply**: State the expected outcome *before* running tests after
any change. On mismatch in either direction, stop and reconcile the model
first. (And when a reproduction depends on incidental data properties —
distinct vs tied values — say so in the test comment, or the next reader
inherits your luck without knowing it.)

---

### Lesson 9: Unauthorized Changes Contaminate the Whole Diff

**What we learned**: The dedup change was small, plausible, and rode
inside a large approved change set. Its discovery forced re-review of
*everything* — because once one undecided decision is found in a diff,
no other part of the diff can be trusted to trace to an actual decision.
The value of designing before implementing is the audit chain; a single
smuggled choice breaks it retroactively.

**The Principle**: Architectural decisions belong to the owner. A bug
discovered mid-implementation does not authorize a design change to fix
it — it authorizes a report and a question. (This is written in this
repository's contribution guidance; this tale is what enforcing it looks
like in practice, and why it's worth the friction.)

**How to apply**: The tell is justification-shaped reasoning arriving
*after* the change ("sorting is a permutation, so..."). If you find
yourself coining a principle to defend an edit you already made, revert
first, then propose.

---

## The Meta-Lesson: The Question Outranks the Answer

The companion tale's meta-lesson was that *architecture pressure reveals
bugs*: make the system's demands explicit and violations surface
themselves. This tale is the sequel, about what happens after the
violation surfaces — because a surfaced violation still gets you nothing
if you ask it the wrong question.

The same panic trace supported three different fixes, and the fix
improved exactly as fast as the question did:

```
"How do I stop this panic?"                    → skip the dedup        (workaround)
"What's the deepest layer touching this?"      → total comparison layer (deeper workaround)
"Why is this value here at all?"               → pull is presentation;
                                                  restore the boundary   (the fix)
"Why was it put here?"                         → grammar flattening,
                                                  transplanted placement,
                                                  fossilized accommodation,
                                                  unenforced domain      (the class,
                                                  and the enforcement
                                                  that prevents the next one)
```

Debugging answered the first question. Only interrogation — of the value
against its domain, of the code against its history, of the fix against
the invariants — answered the rest. The questions in this tale were asked
by review, after two wrong fixes; the discipline this tale exists to
record is asking them *first*, unprompted, while holding the stack trace:

**What is this value? Does it belong in this layer? Who put it here, and
what question were they avoiding when they did?**

---

*Written after one shipped fix, two rejected workarounds, one relocated
feature proposal, and a demonstration that every premise of the right
answer can be in hand while the wrong question keeps it out of reach.*

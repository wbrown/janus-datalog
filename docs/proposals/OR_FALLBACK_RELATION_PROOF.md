# `OrFallbackRelation` as a proof

## Sources

- The code at commit `dffcb050cbc4c159cc6b355298d0284bf97d6992`: `datalog/executor/or_fallback_relation.go`, `datalog/executor/scope.go`, `datalog/executor/join_index.go`, `findOuterRelation`, `orProperties` and `deriveInvocation` in `datalog/executor`, `TupleKeyMap.PutIfAbsentPositions` (returns `existed`), and the doc comment of `ruleEvaluation` in `datalog/executor/rules.go`.
- The design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` (SCOPE) §4.1 to §4.9, `docs/proposals/NESTED_SCOPES_AS_QUERIES.md` (NESTED) §2.2, §2.3, §2.5.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **S**, the subject: `outerRel`, a relation over the symbol list **O** (`outerSyms`). Its tuples are s_1, …, s_m in the order its one iterator yields them.
- **env**: the environment's one tuple over the symbols **E**, from `environmentTuple`; absent when the context carries none.
- **R**, **U**: the header's required run (`required`) and outputs (`outputs`).
- **C**, the carried outputs (`carried`): the u ∈ U with u ∈ O (read at `outerPos`) or u ∈ E (read as `envValue`).
- **G**, what a branch is given (`givenSymbols`): R under the fallback, R followed by C under the union.
- **runSyms**: the o ∈ O with o ∈ G, in O's order, then the g ∈ G with g ∉ O.
- **J**, the probe key (`joinSyms`): the prefix of runSyms that O carries.
- **Σ**, the result's symbols (`symbols`): O, then the u ∈ U with u ∉ O.
- **B_1 … B_n**: the branches, in order.
- **frame_i = (req_i, out_i)** and **plan_i**: `planBranches`' frame and plan for B_i. **header_i** = req_i followed by out_i, the symbols of the branch's result.
- **run(plan, X)**: `runPlan`, the primitive's second operation: the plan executed with `scope/required` bound to X, a relation over the plan's req, the enclosing context's derived relations held, no environment.
- **κ_f(t)**, a frame's key of a subject tuple: the tuple over req_f carrying t's value at each req symbol O holds and env's value at the rest (`windowRun`'s construction of `key`).
- **K_f(W)** for a window W: { κ_f(t) : t ∈ W }, a set.
- **cb_i**: branch i's kept index (`cachedBranch`), a `JoinIndex` whose key **K_i** is the symbols of J that header_i carries, read at O's positions on the probe side and at header_i's on the stored side (`newCachedBranch`).
- **A_i(t)**, the language's answer of branch i for a subject tuple: run over {π_R(t) with env's values at R \ O} of the branch framed over R alone, a relation over R ∪ U (NESTED §2.2: a branch sees the required run and nothing else).

## 2. Preconditions

The machine assumes these; each is established before `NewOrFallbackRelation` runs, and each is a contract the substrate would state to its caller after a split.

- **P1.** Every symbol of runSyms that O lacks is bound by env. `windowFrames` reads `envRel.Get(0)` for such a symbol, and `planBranches` frames a branch reading nothing over `fromEnvironment`. Established by the planner's refusal of a required run nothing binds (NESTED §2.4's last bullet, SCOPE §2.2) and, for C \ O, by `carried`'s construction.
- **P2.** S is a set: no two of s_1 … s_m are equal over O. The relational invariant of the janus `CLAUDE.md`.
- **P3.** The primitive's identity (SCOPE §4.2): run(plan, X ∪ Y) = run(plan, X) ∪ run(plan, Y), and every tuple of run(plan, X) carries its own run tuple at the req positions (`align.apply`). It holds for every clause a branch may hold, an invocation under a magic rewrite included.
- **P4.** Every branch binds every output (the header check at `Prepare`). Hence U ⊆ header_i for every i, and header_i ⊆ R ∪ C ∪ U.
- **P5.** The seek equivalence (SCOPE §4.4): a branch holding no invocation, run over the required run with the carried outputs bound in, answers the same tuples as the branch run over the required run alone joined with the outputs after.

## 3. The theorem

For a subject tuple t, define the form's answer **Ans(t)**:

- Union: Ans(t) = ⋃_i { π_Σ(t ⋈ b ⋈ env) : b ∈ A_i(t) }, the natural join on every shared symbol, so b agrees with t on R ∩ O and on U ∩ O and with env on U ∩ E.
- Fallback: let i*(t) be the least i with A_i(t) ≠ ∅; Ans(t) = { π_Σ(t ⋈ b ⋈ env) : b ∈ A_{i*(t)}(t) }, empty when no branch answers, and empty when every b of the answering branch disagrees with t on C.

**Theorem.** The set of tuples `OrFallbackIterator` yields over its lifetime is ⋃_{t ∈ S} Ans(t); it pulls each s_q from S's iterator once, in order; its first tuple follows the first pull; and the tuples it yields appear in S's order of the subject tuples they came from.

## 4. Lemmas

Each lemma names the code it is about, what it reads, what it concludes, and the test that pins it. "Reads the form" marks a lemma whose statement depends on `shortCircuit`.

### L1. The window reader (`fillWindow`, the second arm of `Next`)

Reads: `outerIter`, `windowSize`, `windowCap`, `windowGrowth`, `copies`. Does not read the form or the branches.

Let w_1 = 1 and w_{k+1} = min(w_k · growth, cap) while w_k < cap, else w_k. Let p_1 = 0. The k-th call of `fillWindow` with S not exhausted sets `window` to s_{p_k+1} … s_{p_k+n_k} with n_k = min(w_k, m − p_k), sets `windows` to k, emits `OrFallbackWindow`, and calls `runWindow` before returning; p_{k+1} = p_k + n_k. Each subject tuple is pulled once: `outerIter` is opened once in `Iterator` and `Next` is called on it once per tuple. The workspace's capacity is at most cap, since it is reallocated only to `windowSize`, which never exceeds cap. The call that finds S exhausted with an empty window emits `OrFallbackOuterExhausted`, completes an owned continuation, and ends the iterator. A failure of S's iterator becomes `it.err` with the iterator done. A tuple is copied on entry when `copies`.

Pinned by `TestFillWindow_ReadsTheSubjectInWindowsDoublingToTheCap`, `TestFillWindow_CopiesATupleFromASubjectThatReusesItsWorkspace`, `TestFillWindow_ASubjectsFailureIsTheIteratorsError`, `TestWindowGrowsFromOneTupleToTheCap`.

### L2. The frames (`windowFrames`, `windowFrame.windowRun`, `windowFrame.environmentRun`)

Reads: the plans' req lists, O, `envRel`. Does not read the form, the branches' clauses, or any branch's result.

- (a) The frames are the distinct req lists among the plans, compared in order (`symbolsMatch`); `frameOf[i]` names branch i's.
- (b) For a frame f carrying a subject symbol, let run_{f,k} = `windowRun(W_k, k)`. Then run_{f,k} = K_f(W_k) \ ⋃_{j<k} run_{f,j}, with no duplicate, and ⋃_{j≤k} run_{f,j} = K_f(W_1 ∪ … ∪ W_k). Proof by induction on k: before window k, `ran` holds exactly the keys of ⋃_{j<k} run_{f,j}; `PutIfAbsentPositions` returns `existed` and inserts when absent, so a key is kept at its first sight in the window and skipped at every later sight. A second call with the same ordinal returns the memoized `run`.
- (c) For a frame carrying no subject symbol, `environmentRun` is the one tuple of env's values over req_f, or nil, the unit relation, when req_f is empty; `runWindow` builds such a frame's branches at window 1 alone.

Precondition P1 is where `envValues` is filled. Pinned by `TestWindowFrames_SharesAFrameAmongBranchesFramedAlike`, `TestWindowFrames_ResolvesEachRequiredSymbolToTheSubjectOrTheEnvironment`, `TestWindowFrame_AFrameOfNoSubjectSymbolRunsOverTheEnvironmentsTuple`, `TestWindowFrame_WindowRunIsTheWindowsDistinctKeysLessThoseRunBefore`.

### L3a. The index (`JoinIndex.Add`, `JoinIndex.Probe`)

Reads: nothing of the executor.

After Add(b_1) … Add(b_n), Probe(t) is the b_j whose values at the stored positions equal t's at the probe positions, position by position under `datalog.ValuesEqual`, in added order, and nil when none does; `Len` is n; `KeysUnique` holds iff every group has one tuple; with no key position every tuple is one group and every probe returns it. Pinned by `join_index_test.go`.

### L3b. The build (`buildBranch`, `runBranch`, `collectBranch`, `newCachedBranch`, `cache`)

Reads: plan_i, the run X, the context, J, O. Reads the form at two hooks only: the EA gate (`narrowable`, L7) and the answered record (L3c).

`buildBranch(i, X)` creates cb_i at the first build with key K_i and adds to it exactly the tuples of run(plan_i, X), read whole by `collectBranch` and copied when the result reuses its workspace, or the EA arm's tuples under L7. header_i is the result's symbols. A failure sets `it.err` and returns false. `OrFallbackCacheBuild` reports the count. So after windows 1 … k, cb_i holds ⋃_j run(plan_i, X_{i,j}) over the runs X_{i,j} branch i was built over.

With L2: for a keyed branch under the union, X_{i,j} = run_{f(i),j}, so cb_i holds run(plan_i, K_{f(i)}(W_1 ∪ … ∪ W_k)) by P3. For a branch reading nothing, cb_i holds run(plan_i, environmentRun), built once.

Pinned by `TestRunWindow_BuildsEachBranchOverItsRun`, `TestRunWindow_ABranchsFailureIsTheIteratorsError`, `TestCollectBranchPreservesIteratorAndCloseErrors`, `TestCollectBranchCopiesFromAnUnsafeBranchResult`, `TestKeyAnsweredByAnEarlierWindowIsProbedNotEvaluated`.

### L3c. The fallback's answered keys (`recordAnswered`, `unanswered`, `runWindow`'s order) — reads the form

Under the fallback G = R, so runSyms ⊆ R and `planBranches` frames every keyed branch, invocation or not, over runSyms; there is at most one keyed frame and at most one frame of no subject symbol.

- answered_i, for a branch whose header carries some of J: the projection of cb_i's tuples onto K_i, read at a run tuple's leading positions (`runPos`), since a keyed run tuple begins with J in O's order. A branch whose header carries none of J is unrecorded.
- `unanswered(X, i)` is empty when some unrecorded j < i has a non-empty index, and otherwise X without the tuples whose K_j projection some recorded j < i answered.
- `runWindow` builds the branches in index order and `buildBranch` records answered_j before `unanswered(·, i)` is computed for i > j.

Hence, for every key κ ∈ K_f(W_1 ∪ … ∪ W_k) and branch i: branch i was built over κ in the window κ first appeared iff no earlier branch answered κ by then, where "j answered κ" means run(plan_j, {κ}) ≠ ∅ for a recorded j and a non-empty subject-independent result for an unrecorded j. A branch with a partial key (a branch reading nothing that provides a symbol of J) answers a key by that symbol alone, which is exact because its result does not depend on the rest of the key.

Pinned by `TestRunWindow_AFallbacksLaterBranchRunsOverTheUnansweredKeys`, `TestRunWindow_ABranchReadingNothingWhoseResultCarriesTheKeyAnswersTheKeysItHolds`, `TestEveryWindowSizeAnswersTheSameTuples`.

### L4. The emit (`nextShortCircuit`, `nextCorrelatedUnion`, `agreeing`, `agrees`, `emitCachedMatches`, `projectionPlan.project`, the drain arm of `Next`) — reads the form in its rule

`agrees(b, t)` holds iff b[u] = t[u] for every u ∈ C ∩ header_i with u ∈ O and b[u] = env[u] for every u ∈ C ∩ header_i with u ∉ O. `project_i(b, t)` fills each σ ∈ Σ from b when header_i carries σ, else from t.

- Union: for a window tuple t and each built branch i in order, the iterator yields project_i(b, t) for every b ∈ Probe_i(t) with agrees(b, t).
- Fallback: for the first built branch i with Probe_i(t) ≠ ∅, the iterator yields project_i(b, t) for every b ∈ Probe_i(t) with agrees(b, t), and nothing of the later branches, whether or not any b agreed (`break` after a false emit).

Claim: project_i(b, t) = π_Σ(t ⋈ b ⋈ env) for b ∈ Probe_i(t) with agrees(b, t). The symbols b and t share are O ∩ header_i ⊆ (O ∩ R) ∪ (O ∩ U) by P4, O ∩ R ⊆ J, and O ∩ U = O ∩ C; a symbol of K_i agrees by the probe, and a symbol of C agrees by `agrees`. So every shared value is t's, and the projection is the join's. A symbol of Σ \ O is in U ⊆ header_i by P4, so no position stays unset.

Pinned by `TestNextShortCircuitDecidesBeforeTheCarriedOutputFilter`, `TestNextCorrelatedUnionTakesEveryBranchPerSubjectTuple`, `TestAgreeingKeepsTheMatchesAgreeingOnTheCarriedOutputs`, `TestEmitCachedMatchesSetsTheFirstTupleAndTheDrainState`, `TestProjectionPlan`, `TestFallbackDecisionReadsTheBranchBeforeTheCarriedOutputFilter`.

### L5. Deduplication (`seen` in `Next`, `deduplicate` in `NewOrFallbackRelation`) — reads the form

When `deduplicate`, a yielded tuple equal to an earlier one under `NewTupleKeyFull` is skipped. The rule: `len(branches) > 1` under the union; `len(outerRel.Properties().Keys) == 0` under the fallback.

What the stream can repeat before the filter:

- Union, n ≥ 2: two branches can yield one tuple for one t, so the filter is needed.
- Union, n = 1: for one t the b are distinct over header_1, header_1's part outside Σ is env-bound and so constant, and project keeps b's part inside Σ, so the projections are distinct; for t ≠ t' the projections differ on O ⊆ Σ, since project keeps t[O] by L4's claim. No repeat.
- Fallback, any n: one branch answers each t, its b are distinct as under n = 1, and distinct t give distinct projections as above. No repeat under P2.

So under P2 the fallback's filter over a keyless subject skips nothing; it is reached only by a subject that is not a set, which P2 excludes. The arm's justification in SCOPE §4.3 argues why a keyed subject needs no filter and states no case for the keyless one.

Pinned by `TestDeduplicationFollowsTheFormAndTheSubjectsKey`, `TestCorrelatedUnionDeduplicatesAcrossBranches`.

### L6. The continuations (`Iterator`'s ownership, `deriveInvocation`, `ruleContinuations`, `ruleEvaluation`)

Reads: `ctx.continued`. Does not read the form.

`Iterator` takes the context's continuations or creates its own, recording `ownsContinued`, and the derived contexts `runPlan` makes carry them (`WithEnvironment`, `WithDerived`). `deriveInvocation` at an invocation clause reads `byClause[inv]`, keyed on the clause pointer, which is stable because plan_i is made once at the first iterator and kept; it creates the evaluation at the clause's first run and extends it with every later run's seed, completing it at once only when the context carries no continuations. `ruleEvaluation`'s doc states the rest: the strata's sets and the derived count are carried from window to window, a window's seed enters the seed predicate's stratum as a delta, the strata run again from the lowest one that changed, and the evaluation derives each tuple once however the seeds are split across the windows. By SCOPE §4.2's induction the predicate's relation after window k is the fixpoint over the seeds of windows 1 … k, and the seed decides which keys the adorned predicate carries and never what a key carries, so run(plan_i, X_k) under the continuation equals the same run under a fresh evaluation. An owned continuation completes at exhaustion and at `Close`; a nested form's iterator finds the context's and shares it.

Pinned by `TestDeriveInvocation_ContinuesTheClausesEvaluationUnderContinuations`, `TestFixpointContinuedAcrossWindowsDerivesEachTupleOnce`, `TestFormInsideAFormContinuesOneEvaluationAcrossTheOuterWindows`, `TestOrFallbackIteratorClosedBeforeExhaustionCompletesItsEvaluations`, `TestIterationBoundIsCountedPerWindowAndTheTupleBoundIsCarried`.

### L7. The EA arm (`buildBranchFromEACache`, the gate in `buildBranch`) — reads the form in its gate

Under the gate (`shortCircuit`, `isDataPatternOnlyBranch`, `prefetched`) and the arm's own conditions (one pattern on the default source, E a variable of J, A a constant keyword, V a variable, a matcher with `LookupAttribute` whose `CanFuseAttributeFetch` holds for A), the arm returns {(e, v) : e a distinct identity at the run's E position, `LookupAttribute(e, A)` found v}, narrowed to env's V value when `envRel` carries V. header_i is then [E V].

Claim: this equals run(plan_i, X) projected onto [E V], and the index over K_i = J ∩ [E V] answers every probe as the scan path's index would. Cardinality one on a current handle gives one v per e, the contract `CanFuseAttributeFetch` states. The pattern reads no run symbol but E and V, so its result for a run tuple depends on e alone, or on (e, v) when V ∈ J, and probing by K_i alone returns the same tuples for every value of the other run symbols, which `project` fills from t. The narrowing by env is sound because `envRel` carries a symbol only when runSyms holds it and O lacks it, so under the fallback V is then required and the run X carries env's value at V, and the scan path is narrowed by the run to the same value.

`OrFallbackRelation.prefetched` has two references on the tree: its copy into the iterator in `Iterator`, and the write in `or_outer_selection_test.go`. No production path writes it, so the arm runs under that test alone.

### L8. The properties (`orProperties`, `orBranchesEmitAtMostOne`) — reads the form

The result's ordering is the subject's, which L1 and L4 deliver (window by window, tuple by tuple, in S's order). The result's keys: the subject's own under the fallback when every branch answers at most one tuple per subject tuple, judged against the header symbols the subject carries through the matcher's capability; otherwise the composite of each subject key with the produced outputs; under the union always the composite. The derivation emits `OrPropertiesDerived`. This lemma states the contract readers use and is not part of the set theorem.

### L9. Failure

A planning or projection failure in `Iterator` sets `it.err` with the iterator done before S is opened; a subject failure surfaces in L1; a branch failure in L3b; `Close` returns the subject iterator's close error. No failure surfaces from `NewOrFallbackRelation`.

## 5. Proof of the theorem

Fix t = s_q ∈ S. By L1, t lies in exactly one window W_k, pulled once, and `runWindow` for W_k has run before any probe of t. For a branch i with frame f, let κ = κ_f(t) ∈ K_f(W_k); by L2 κ lies in run_{f,j} for exactly one j ≤ k, its first appearance.

**Union.** Three cases on branch i.

- A seek branch, req_i = runSyms: by L3b and P3, cb_i holds run(plan_i, {κ}); K_i = J; a held tuple whose J part equals t's has run tuple κ, since κ's part outside J is env's one tuple, so Probe_i(t) = run(plan_i, {κ}), which by P5 is A_i(t) joined with t on C ∩ O.
- An invocation branch, req_i = the R part of runSyms: K_i = J, which carries C ∩ O beside R ∩ O; a held tuple matching t on K_i has run tuple π_R(t) with env's values and agrees with t on C ∩ O; so Probe_i(t) = A_i(t) joined with t on C ∩ O, the join through the index's key.
- A branch reading nothing: cb_i holds run(plan_i, environmentRun), which does not depend on t, so A_i(t) is that relation for every t; Probe_i(t) keeps the tuples agreeing with t on K_i ⊆ J, the join on those symbols.

In each case L4 then yields π_Σ(t ⋈ b ⋈ env) for the b agreeing with t on the rest of C, so the yield for t and i is { π_Σ(t ⋈ b ⋈ env) : b ∈ A_i(t) }, and over the branches it is Ans(t). L5 makes the union over t a set, and by the n = 1 case of L5 nothing is lost when the filter is off. Over all t, the yield is ⋃_t Ans(t).

**Fallback.** By L3c, the least i* with A_{i*}(t) ≠ ∅ was built over κ in κ's first window, and cb_{i*} holds its tuples for κ; every j < i* holds none for κ: a recorded j was built over κ with an empty result or not at all, and an unrecorded j with a non-empty result would answer every t, contradicting the choice of i*. `nextShortCircuit` takes the first built branch with a non-empty probe, which is therefore i*; for a j < i* whose header carries all of J a non-empty probe would mean a held tuple with run tuple κ, and for one with a partial key its probe is non-empty iff its subject-independent result holds t's values there, which is A_j(t) ≠ ∅. L4 then yields the agreeing tuples of A_{i*}(t) and drops t when none agrees, which is Ans(t). L5 skips nothing under P2.

**Read once, first result, order.** L1 gives the single pull per tuple; W_1 holds one tuple, so the first result follows the first pull; L1 and L4 yield in S's order.

## 6. What the proof finds

- **F1.** The lemmas that never read the form: L1 (window reader), L2 (frames), L3a (index), L3b (build, outside its two hooks), L6 (continuations), L9's mechanics, and `projectionPlan`. The lemmas that do: L3c (answered keys, fallback only), L4's rule (which branches, first or all), L5's rule, L7's gate, L8, and `planBranches`' framing policy with `givenSymbols`.
- **F2.** The only place the form reaches inside the build is the fallback: `unanswered` narrows a branch's run on earlier branches' results, and `recordAnswered` writes them. Both sit between L2's run and L3b's build, so they are a step the fallback inserts, not a property of the build.
- **F3.** Under P2 the fallback's deduplication filter over a keyless subject never skips a tuple (L5). SCOPE §4.3 justifies the keyed case's absence of a filter and states no case for the keyless one.
- **F4.** The EA arm's narrowing by env is sound only because `envRel` holds required symbols under the fallback (L7). Under the union `envRel` can hold a carried output, and a narrowing before the decision would then be a join before the decision; the gate's `shortCircuit` keeps the arm off that path today. A form-free gate, which SCOPE §4.6 states, must keep the narrowing after the decision or restrict it to required symbols.
- **F5.** The subquery's seams all sit in the framing and the key, L0's `scope.query` and L2's key construction: the aggregating find with inputs as group keys, the call's substitution, each inner `:in` symbol unified with the argument at its place and each inner `:find` element with the binding variable at its place, under which the run is presented by symbol and the answer renamed by symbol, and a collection input expanding one subject tuple into several keys. None sits in L1, L3, L4 or L6. A pure aggregate is L2(c)'s frame of no subject symbol, built once and probed with no key, which is SCOPE §4.4's "paired with every run tuple".
- **F6.** The `not-join` is the union with one body, U = ∅, C = ∅, and the emit rule inverted: yield t iff Probe(t) = ∅. Its environment-only header is L2(c)'s frame over the unit subject: the one tuple is dropped iff the body has a result, and the find boundary's verdict (SCOPE §3) then empties the result, in place of `executeNotJoinClause`'s emptying of every group.
- **F7.** P1 through P5 are the substrate's contract with its caller and are not checked at `NewOrFallbackRelation`. P1's violation is a nil dereference in `windowFrames`; the planner's refusal is what keeps it out.

## 7. The split the lemmas give

Each module is one lemma or one pair, with the interface the proof used.

1. **Window reader** (L1): over an `Iterator`, a cap, a growth and `copies`; yields `(window []Tuple, ordinal int)` per fill and the exhaustion; owns `OrFallbackOuterTuple`, `OrFallbackOuterExhausted` and `OrFallbackWindow` (the distinct-key count needs the key positions, given at construction). Form-free.
2. **Frames** (L2): `windowFrames` over the req lists, O and env; `windowRun(window, ordinal)` and `environmentRun`. Form-free.
3. **Kept branch** (L3a, L3b): a `cachedBranch` with `build(plan, run)` through `runPlan` and `collectBranch`, `Probe`, and `OrFallbackCacheBuild`; the EA build of L7 as a second build strategy chosen by its own conditions. Form-free once the two fallback hooks leave it.
4. **Continuations** (L6): already its own piece (`ruleContinuations`, the context); the ownership rule stays with the outermost windowed relation. Form-free.
5. **Per form**, four small policies over the modules above:
   - the framing (`planBranches` and `givenSymbols`): the union's seek and its pre-plan over R, the fallback's R alone, the `not-join`'s all-required header, the subquery's inputs as req with its find shape, its collection expansion and the call's substitution, under which its run is presented and its answer renamed by symbol;
   - the fallback's `unanswered` and `recordAnswered` (L3c), applied between modules 2 and 3;
   - the emit rule (L4): all branches, the first, the anti (none), or the one body joined; `agreeing` and `projectionPlan` serve the three forms with outputs;
   - the dedup rule (L5) and the properties (L8).

The composition in `Next` is then: fill a window (1), per frame compute the run (2), per branch narrow the run under the form's rule and build (3), per window tuple emit under the form's rule (4), deduplicate under the form's rule. The theorem's proof is the same text with each lemma supplied by its module, and each module's tests are the lemma's pins already in the tree.

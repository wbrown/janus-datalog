# `OrFallbackRelation` as a proof

## Sources

- The code, in `datalog/executor`: `window_reader.go` (`windowReader`), `window_frame.go` (`windowFrame`, `windowFrames`, `windowRun`, `environmentRun`, `keySymbols`, `environmentRelation`), `kept_branch.go` (`cachedBranch`, `newCachedBranch`, `buildBranchTuples`, `framing.present`, `collectBranch`), `join_index.go` (`JoinIndex`), `windowed_form.go` (`framing`, `frameBranches`, `formPolicy`, `windowedIterator`, `continuationsFor`), `match_emit.go` (`matchEmit`, `projectionPlan`), `or_fallback_relation.go` (`OrFallbackRelation`, `OrFallbackIterator`, `OrFallbackRelation.framing`, `planBranches`, `orBranchesEmitAtMostOne`, `prefetchWindow`, `buildFromCache`, `buildBranchFromEACache`, `recordAnswered`, `unanswered`, `nextShortCircuit`, `nextCorrelatedUnion`, `emitCachedMatches`), `relation_properties.go` (`orProperties`), `scope.go` (`runPlan`), `executor_utils.go` (`environmentTuple`), `query_executor.go` (`findOuterRelation`), `rules.go` (`ruleContinuations`, `ruleEvaluation`, `deriveInvocation`) and `tuple_key.go` (`TupleKeyMap.PutIfAbsentPositions`, which returns whether the key existed); and in `datalog/planner`, `framing.go` (`GivenSymbols`, `Framing.Branches`, `Framing.CarriedBy`, `ReadSubqueryFrame`).
- The design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` (SCOPE) §4.1 to §4.9, `docs/proposals/NESTED_SCOPES_AS_QUERIES.md` (NESTED) §2.2, §2.3, §2.5.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **S**, the subject: `outerRel`, a relation over the symbol list **O** (the subject's `Symbols()`). Its tuples are s_1, …, s_m in the order its one iterator yields them.
- **env**: the environment's one tuple over the symbols **E**, from `environmentTuple`. It is absent when the context carries none.
- **R**, **U**: the header's required run (`required`) and outputs (`outputs`).
- **C**, the carried outputs (`carried`): the u ∈ U with u ∈ O, read at the subject position `outerPos`, or u ∈ E, read as `envValue`.
- **G**, what a branch is given (`planner.GivenSymbols`, the framing's `given`): R under the fallback, and R followed by C under the union.
- **runSyms** (`Framing.Branches`, the frame's `Required`): G in the header's order, the symbols a branch holding no invocation is framed over; a branch holding an invocation is framed over R alone, and a branch reading nothing over the r ∈ R with r ∉ O.
- **J**, the probe key (`joinSyms`, the framing's `key`, `keySymbols`): the g ∈ G with g ∈ O, in O's order.
- **Σ**, the result's symbols (`symbols`): O, then the u ∈ U with u ∉ O.
- **B_1 … B_n**: the branches, in order.
- **frame_i = (req_i, out_i)** and **plan_i**: `frameBranches`' frame and plan for B_i. **header_i** is req_i followed by out_i, the symbols of the branch's result.
- **run(plan, X)**: the primitive's second operation. `runPlan` executes the plan with `scope/required` bound to X, a relation over the plan's req, with the enclosing context's derived relations held and no environment.
- **κ_f(t)**, a frame's key of a subject tuple: the tuple over req_f carrying t's value at each req symbol O holds and env's value at the rest (`windowRun`'s construction of `key`).
- **K_f(W)** for a window W: { κ_f(t) : t ∈ W }, a set.
- **cb_i**: branch i's kept index (`cachedBranch`), a `JoinIndex` whose key **K_i** is the symbols of J that header_i carries. The key is read at O's positions on the probe side and at header_i's on the stored side (`newCachedBranch`).
- **A_i(t)**, the language's answer of branch i for a subject tuple: the branch framed over R alone, run over {π_R(t) with env's values at R \ O}. It is a relation over R ∪ U (NESTED §2.2: a branch sees the required run and nothing else).

## 2. Preconditions

The machine assumes these. Each is established before `NewOrFallbackRelation` runs, and each is a contract the modules state to their caller.

- **P1.** Every symbol of runSyms that O lacks is bound by env. `windowFrames` reads `f.environment.Get(0)` for such a symbol, and `frameBranches` frames a branch reading nothing over `fromEnvironment`. The planner's refusal of a required run nothing binds establishes it (NESTED §2.4's last bullet, SCOPE §2.2), and `carried`'s construction establishes it for C \ O.
- **P2.** S is a set: no two of s_1 … s_m are equal over O. That is the relational invariant of the janus `CLAUDE.md`.
- **P3.** The primitive's identity (SCOPE §4.2): run(plan, X ∪ Y) = run(plan, X) ∪ run(plan, Y), and every tuple of run(plan, X) carries its own run tuple at the req positions. It holds for every clause a branch may hold, an invocation under a magic rewrite included.
- **P4.** Every branch binds every output (the header check at `Prepare`). Hence U ⊆ header_i for every i, and header_i ⊆ R ∪ C ∪ U.
- **P5.** The seek equivalence (SCOPE §4.4): a branch holding no invocation, run over the required run with the carried outputs bound in, answers the same tuples as the branch run over the required run alone and joined with the outputs after.

## 3. The theorem

For a subject tuple t, define the form's answer **Ans(t)**:

- Union: Ans(t) = ⋃_i { π_Σ(t ⋈ b ⋈ env) : b ∈ A_i(t) }. The join is the natural join on every shared symbol, so b agrees with t on R ∩ O and on U ∩ O, and with env on U ∩ E.
- Fallback: let i*(t) be the least i with A_i(t) ≠ ∅. Then Ans(t) = { π_Σ(t ⋈ b ⋈ env) : b ∈ A_{i*(t)}(t) }. It is empty when no branch answers, and empty when every b of the answering branch disagrees with t on C.

**Theorem.** The set of tuples `OrFallbackIterator` yields over its lifetime is ⋃_{t ∈ S} Ans(t). It pulls each s_q from S's iterator once, in order. Its first tuple follows the first pull. The tuples it yields appear in S's order of the subject tuples they came from.

## 4. Lemmas

Each lemma names the code it is about, what it reads, what it concludes, and the tests that cover it. "Reads the form" marks a lemma whose statement depends on the form's policy.

### L1. The window reader (`windowReader.next`, `windowedIterator.nextWindow`)

Reads: the subject's iterator, `size`, `cap`, `growth`, `copies`. It does not read the form or the branches.

Let w_1 = 1, and w_{k+1} = min(w_k · growth, cap) while w_k < cap, else w_k. Let p_1 = 0. The k-th call of `next` with S not exhausted sets `window` to s_{p_k+1} … s_{p_k+n_k} with n_k = min(w_k, m − p_k), sets `ordinal` to k, and emits `ScopeWindow`. Then p_{k+1} = p_k + n_k. Each subject tuple is pulled once: `Next` is called on the subject's iterator once per tuple, and the iterator is opened once in `newWindowedIterator`. The workspace's capacity never exceeds cap, since it is reallocated only to `size`, which never exceeds cap. The call that finds S exhausted with an empty window emits `ScopeSubjectExhausted` and returns false. `nextWindow` then completes an owned continuation and ends the iterator. A failure of S's iterator becomes the reader's `err`, which `nextWindow` copies to the iterator's, with the iterator done. A tuple is copied on entry when `copies`. `nextWindow` calls `runWindow` on each window read, before any probe of it.

Its tests: `TestWindowReader_ReadsTheSubjectInWindowsDoublingToTheCap`, `TestWindowReader_CopiesATupleFromASubjectThatReusesItsWorkspace`, `TestWindowReader_ASubjectsFailureIsTheReadersError` and `TestWindowGrowsFromOneTupleToTheCap`.

### L2. The frames (`windowFrames`, `windowFrame.windowRun`, `windowFrame.environmentRun`)

Reads: the plans' req lists, O, and the frames' environment relation. It does not read the form, the branches' clauses, or any branch's result.

- (a) The frames are the distinct req lists among the plans, compared in order (`symbolsMatch`). `frameOf[i]` names branch i's.
- (b) For a frame f carrying a subject symbol, let run_{f,k} = `windowRun(W_k, k)`. Then run_{f,k} = K_f(W_k) \ ⋃*{j<k} run*{f,j}, with no duplicate, and ⋃*{j≤k} run*{f,j} = K_f(W_1 ∪ … ∪ W_k). The proof is by induction on k. Before window k, `ran` holds exactly the keys of ⋃*{j<k} run*{f,j}. `PutIfAbsentPositions` reports whether the key existed and inserts it when absent. So a key is kept at its first sight in the window and skipped at every later sight. A second call with the same ordinal returns the memoized `run`.
- (c) For a frame carrying no subject symbol, `environmentRun` is the one tuple of env's values over req_f, or nil, the unit relation, when req_f is empty. `runWindow` builds such a frame's branches at window 1 alone.

P1 is where `envValues` is filled. Its tests: `TestWindowFrames_SharesAFrameAmongBranchesFramedAlike`, `TestWindowFrames_ResolvesEachRequiredSymbolToTheSubjectOrTheEnvironment`, `TestWindowFrame_AFrameOfNoSubjectSymbolRunsOverTheEnvironmentsTuple` and `TestWindowFrame_WindowRunIsTheWindowsDistinctKeysLessThoseRunBefore`.

### L3a. The index (`JoinIndex.Add`, `JoinIndex.Probe`)

Reads: nothing of the executor.

After Add(b_1) … Add(b_n), Probe(t) is the b_j whose values at the stored positions equal t's at the probe positions, position by position under `datalog.ValuesEqual`, in added order. It is nil when none does. `Len` is n. `KeysUnique` holds iff every group has one tuple. With no key position every tuple is one group, and every probe returns it. Its tests are in `join_index_test.go`.

### L3b. The build (`windowedIterator.buildBranch`, `buildBranchTuples`, `collectBranch`, `newCachedBranch`)

Reads: plan_i, the run X, the context, J, O. It reads the form at two hooks only: the policy's `build`, which is the EA arm (L7), and the policy's `built`, which is the answered record (L3c).

`buildBranch(i, X)` creates cb_i at the first build, with key K_i. It adds to cb_i exactly the tuples of run(plan_i, X), read whole by `collectBranch`, or the EA arm's tuples under L7. A tuple is copied when the result reuses its workspace. header_i is the result's symbols. A failure sets the iterator's `err` and returns false. `ScopeBranchBuild` reports the count. So after windows 1 … k, cb_i holds ⋃*j run(plan_i, X*{i,j}) over the runs X_{i,j} branch i was built over.

With L2: for a keyed branch under the union, X_{i,j} = run_{f(i),j}, so cb_i holds run(plan_i, K_{f(i)}(W_1 ∪ … ∪ W_k)) by P3. For a branch reading nothing, cb_i holds run(plan_i, environmentRun), built once.

Its tests: `TestRunWindow_BuildsEachBranchOverItsRun`, `TestRunWindow_ABranchsFailureIsTheIteratorsError`, `TestCollectBranchPreservesIteratorAndCloseErrors`, `TestCollectBranchCopiesFromAnUnsafeBranchResult` and `TestKeyAnsweredByAnEarlierWindowIsProbedNotEvaluated`.

### L3c. The fallback's answered keys (`recordAnswered`, `unanswered`, `runWindow`'s order). Reads the form.

Under the fallback G = R, so runSyms ⊆ R, and `frameBranches` frames every keyed branch, invocation or not, over runSyms. So there is at most one keyed frame and at most one frame of no subject symbol.

- answered_i, for a branch whose header carries some of J, is the projection of cb_i's tuples onto K_i. It is recorded at the branch result's positions of K_i and read at the positions the run's own symbols give K_i (`unanswered`'s `runPos`). A branch whose header carries none of J is unrecorded.
- `unanswered(X, i)` is empty when some unrecorded j < i has a non-empty index. Otherwise it is X without the tuples whose K_j projection some recorded j < i answered.
- `runWindow` builds the branches in index order. The policy's `narrow` runs `unanswered` before branch i is built, and the policy's `built` runs `recordAnswered` after branch j is built, so answered_j is recorded before `unanswered(·, i)` is computed for i > j.

Hence, for every key κ ∈ K_f(W_1 ∪ … ∪ W_k) and branch i: branch i was built over κ in the window κ first appeared iff no earlier branch answered κ by then. Here "j answered κ" means run(plan_j, {κ}) ≠ ∅ for a recorded j, and a non-empty subject-independent result for an unrecorded j. A branch with a partial key, a branch reading nothing that provides a symbol of J, answers a key by that symbol alone. That is exact, because its result does not depend on the rest of the key.

Its tests: `TestRunWindow_AFallbacksLaterBranchRunsOverTheUnansweredKeys`, `TestRunWindow_ABranchReadingNothingWhoseResultCarriesTheKeyAnswersTheKeysItHolds` and `TestEveryWindowSizeAnswersTheSameTuples`.

### L4. The emit (`nextShortCircuit`, `nextCorrelatedUnion`, `matchEmit.agreeing`, `matchEmit.agrees`, `emitCachedMatches`, `projectionPlan.project`, the drain in `answer`). Reads the form in its rule.

`agrees(b, t)` holds iff b[u] = t[u] for every u ∈ C ∩ header_i with u ∈ O, and b[u] = env[u] for every u ∈ C ∩ header_i with u ∉ O. `project_i(b, t)` fills each σ ∈ Σ from b when header_i carries σ, and from t otherwise.

- Union: for a window tuple t and each built branch i in order, the iterator yields project_i(b, t) for every b ∈ Probe_i(t) with agrees(b, t).
- Fallback: for the first built branch i with Probe_i(t) ≠ ∅, the iterator yields project_i(b, t) for every b ∈ Probe_i(t) with agrees(b, t), and nothing of the later branches, whether or not any b agreed (the `break` after a false emit).

Claim: project_i(b, t) = π_Σ(t ⋈ b ⋈ env) for b ∈ Probe_i(t) with agrees(b, t). The symbols b and t share are O ∩ header_i ⊆ (O ∩ R) ∪ (O ∩ U) by P4. O ∩ R ⊆ J, and O ∩ U = O ∩ C. A symbol of K_i agrees by the probe, and a symbol of C agrees by `agrees`. So every shared value is t's, and the projection is the join's. A symbol of Σ \ O is in U ⊆ header_i by P4, so no position stays unset.

Its tests: `TestNextShortCircuitDecidesBeforeTheCarriedOutputFilter`, `TestNextCorrelatedUnionTakesEveryBranchPerSubjectTuple`, `TestAgreeingKeepsTheMatchesAgreeingOnTheCarriedOutputs`, `TestEmitCachedMatchesSetsTheFirstTupleAndTheDrainState`, `TestProjectionPlan` and `TestFallbackDecisionReadsTheBranchBeforeTheCarriedOutputFilter`.

### L5. Deduplication (`seen` in `answer`, `deduplicate` in `NewOrFallbackRelation`). Reads the form.

When `deduplicate`, a yielded tuple equal to an earlier one under `NewTupleKeyFull` is skipped. The rule: `len(branches) > 1` under the union, and `len(outerRel.Properties().Keys) == 0` under the fallback.

What the stream can repeat before the filter:

- Union, n ≥ 2: two branches can yield one tuple for one t, so the filter is needed.
- Union, n = 1: for one t the b are distinct over header_1. header_1's part outside Σ is env-bound and so constant, and project keeps b's part inside Σ, so the projections are distinct. For t ≠ t' the projections differ on O ⊆ Σ, since project keeps t[O] by L4's claim. No repeat.
- Fallback, any n: one branch answers each t, its b are distinct as under n = 1, and distinct t give distinct projections as above. No repeat under P2.

So under P2 the fallback's filter over a keyless subject skips nothing. Only a subject that is not a set reaches it, which P2 excludes.

Its tests: `TestDeduplicationFollowsTheFormAndTheSubjectsKey` and `TestCorrelatedUnionDeduplicatesAcrossBranches`.

### L6. The continuations (`continuationsFor`, `ownsContinued`, `deriveInvocation`, `ruleContinuations`, `ruleEvaluation`)

Reads: `ctx.continued`. It does not read the form.

`continuationsFor` takes the context's continuations or creates new ones, recording `ownsContinued`. The derived contexts `runPlan` makes carry them (`WithEnvironment`, `WithDerived`). `deriveInvocation` at an invocation clause reads `byClause[inv]`, keyed on the clause pointer. The pointer is stable because plan_i is made once, at the first iterator, and kept. It creates the evaluation at the clause's first run and extends it with every later run's seed, completing it at once only when the context carries no continuations. `ruleEvaluation`'s doc states the rest. The strata's sets and the derived count are carried from window to window. A window's seed enters the seed predicate's stratum as a delta. The strata run again from the lowest one that changed. The evaluation derives each tuple once, however the seeds are split across the windows. By SCOPE §4.2's induction the predicate's relation after window k is the fixpoint over the seeds of windows 1 … k. The seed decides which keys the adorned predicate carries and never what a key carries, so run(plan_i, X_k) under the continuation equals the same run under a fresh evaluation. An owned continuation completes at exhaustion (`nextWindow`) and at `Close`. A nested form's iterator finds the context's and shares it.

Its tests: `TestDeriveInvocation_ContinuesTheClausesEvaluationUnderContinuations`, `TestFixpointContinuedAcrossWindowsDerivesEachTupleOnce`, `TestFormInsideAFormContinuesOneEvaluationAcrossTheOuterWindows`, `TestOrFallbackIteratorClosedBeforeExhaustionCompletesItsEvaluations` and `TestIterationBoundIsCountedPerWindowAndTheTupleBoundIsCarried`.

### L7. The EA arm (`prefetchWindow`, `buildFromCache`, `buildBranchFromEACache`). Reads the form in its gate.

The arm is the fallback's policy. `Iterator` sets the policy's `build` to `buildFromCache` under the fallback alone, and sets the policy's `window` to `prefetchWindow` when the matcher is an `EntityPrefetcher`, `EnableEntityPrefetch` is on, and some branch has the arm's shape. `prefetchWindow` collects a window's distinct entities at the arm positions. When they exceed `prefetchEntityThreshold`, it prefetches them through the matcher, reports `PrefetchTrigger` under the form, and sets `prefetched`. `buildFromCache` answers with no symbols, so that the composition builds from the plan, for a branch holding anything but data patterns (`isDataPatternOnlyBranch`) and while `prefetched` is false.

Once `prefetched` holds, `buildBranchFromEACache` builds a branch of the arm's shape (`eaArmShape`: one pattern on the default source, E a variable of J, A a constant keyword, V a variable), given a matcher with `LookupAttribute` whose `CanFuseAttributeFetch` holds for A. It returns {(e, v) : e a distinct identity at the run's E position, `LookupAttribute(e, A)` found v}, narrowed to env's V value when the framing's environment carries V. header_i is then [E V].

Claim: this equals run(plan_i, X) projected onto [E V], and the index over K_i = J ∩ [E V] answers every probe as the scan path's index would. Cardinality one on a current handle gives one v per e, the contract `CanFuseAttributeFetch` states. The pattern reads no run symbol but E and V, so its result for a run tuple depends on e alone, or on (e, v) when V ∈ J. Probing by K_i alone returns the same tuples for every value of the other run symbols, which `project` fills from t. The narrowing by env is sound because the framing's environment carries a symbol only when runSyms holds it and O lacks it. Under the fallback runSyms is R, so V is then required, the run X carries env's value at V, and the scan path is narrowed by the run to the same value.

### L8. The properties (`orProperties`, `orBranchesEmitAtMostOne`). Reads the form.

The result's ordering is the subject's, which L1 and L4 deliver: window by window, tuple by tuple, in S's order. The result's keys are the subject's own under the fallback when every branch answers at most one tuple per subject tuple, judged against the header symbols the subject carries through the matcher's capability. Otherwise the keys are the composite of each subject key with the produced outputs. Under the union the keys are always the composite. The derivation emits `OrPropertiesDerived`. This lemma states the contract readers use and is not part of the set theorem.

### L9. Failure

A planning failure in `Iterator` sets the iterator's `err` with the iterator done, before S is opened. A subject failure surfaces in L1. A branch failure surfaces in L3b. `Close` returns the subject iterator's close error. No failure surfaces from `NewOrFallbackRelation`.

## 5. Proof of the theorem

Fix t = s_q ∈ S. By L1, t lies in exactly one window W_k, pulled once, and `runWindow` for W_k has run before any probe of t. For a branch i with frame f, let κ = κ_f(t) ∈ K_f(W_k). By L2, κ lies in run_{f,j} for exactly one j ≤ k, its first appearance.

**Union.** Three cases on branch i.

- A seek branch, req_i = runSyms. By L3b and P3, cb_i holds run(plan_i, {κ}), and K_i = J. A held tuple whose J part equals t's has run tuple κ, since κ's part outside J is env's one tuple. So Probe_i(t) = run(plan_i, {κ}), which by P5 is A_i(t) joined with t on C ∩ O.
- An invocation branch, req_i = the R part of runSyms. K_i = J, which carries C ∩ O beside R ∩ O. A held tuple matching t on K_i has run tuple π_R(t) with env's values, and it agrees with t on C ∩ O. So Probe_i(t) = A_i(t) joined with t on C ∩ O, the join through the index's key.
- A branch reading nothing. cb_i holds run(plan_i, environmentRun), which does not depend on t, so A_i(t) is that relation for every t. Probe_i(t) keeps the tuples agreeing with t on K_i ⊆ J, the join on those symbols.

In each case L4 then yields π_Σ(t ⋈ b ⋈ env) for the b agreeing with t on the rest of C. So the yield for t and i is { π_Σ(t ⋈ b ⋈ env) : b ∈ A_i(t) }, and over the branches it is Ans(t). L5 makes the union over t a set, and by the n = 1 case of L5 nothing is lost when the filter is off. Over all t, the yield is ⋃_t Ans(t).

**Fallback.** By L3c, the least i* with A_{i*}(t) ≠ ∅ was built over κ in κ's first window, and cb_{i*} holds its tuples for κ. Every j < i* holds none for κ. A recorded j was built over κ with an empty result, or not at all. An unrecorded j with a non-empty result would answer every t, contradicting the choice of i*. `nextShortCircuit` takes the first built branch with a non-empty probe, which is therefore i*. For a j < i* whose header carries all of J, a non-empty probe would mean a held tuple with run tuple κ. For one with a partial key, its probe is non-empty iff its subject-independent result holds t's values there, which is A_j(t) ≠ ∅. L4 then yields the agreeing tuples of A_{i*}(t) and drops t when none agrees, which is Ans(t). L5 skips nothing under P2.

**Read once, first result, order.** L1 gives the single pull per tuple. W_1 holds one tuple, so the first result follows the first pull. L1 and L4 yield in S's order.

## 6. What the proof finds

- **F1.** The lemmas that never read the form: L1 (the window reader), L2 (the frames), L3a (the index), L3b (the build, outside its two hooks), L6 (the continuations), L9's mechanics, and `projectionPlan`. The lemmas that do: L3c (the answered keys, fallback only), L4's rule (which branches, first or all), L5's rule, L7's gate, L8, and `frameBranches`' framing policy with `planner.GivenSymbols`.
- **F2.** The only place the form reaches inside the build is the fallback. `unanswered` narrows a branch's run on earlier branches' results, and `recordAnswered` writes them. Both are policy steps between L2's run and L3b's build, the `narrow` and `built` hooks of `formPolicy`, not a property of the build.
- **F3.** Under P2 the fallback's deduplication filter over a keyless subject never skips a tuple (L5). The filter costs one key per yielded tuple on a keyless subject and nothing on a keyed one.
- **F4.** The EA arm's narrowing by env is sound because the framing's environment holds required symbols under the fallback (L7). Under the union the environment relation can hold a carried output, and a narrowing before the decision would be a join before the decision. The arm is the fallback's policy alone, which `Iterator` wires only under `shortCircuit`, so no union path reaches it.
- **F5.** The subquery's seams all sit in the framing and the key: the framing's `subst` and `standsFor` in `windowed_form.go`, `present` in `kept_branch.go`, and the opening identity `ReadSubqueryFrame` writes over the holder `ScopeInput` mints (`datalog/planner/framing.go`). Those are the aggregating find with inputs as group keys, the call's substitution, each inner `:in` symbol standing for the argument at its place and each inner `:find` element for the binding variable at its place, under which the run is read by symbol and the answer presented by symbol, and a collection input bound as one group inside the scope from the holder its one run position carries (SCOPE §4.4). None sits in L1, L3, L4 or L6. A pure aggregate is L2(c)'s frame of no subject symbol, built once and probed with no key, which is SCOPE §4.4's "paired with every run tuple".
- **F6.** The `not-join` is the union with one body, U = ∅, C = ∅, and the emit rule inverted: yield t iff Probe(t) = ∅ (`executeNotJoinClause`, SCOPE §4.4). Its environment-only header is L2(c)'s frame over the unit subject. The one tuple is dropped iff the body has a result, and the find boundary's verdict (SCOPE §3) then empties the result.
- **F7.** P1 through P5 are the modules' contract with their caller, and they are not checked at `NewOrFallbackRelation`. P1's violation is a nil dereference in `windowFrames`. The planner's refusal is what keeps it out.

## 7. The modules

Each module is one lemma or one pair, with the interface the proof used.

1. **The window reader** (L1), `window_reader.go`. `windowReader` is over an `Iterator`, a cap, a growth and `copies`. `next` reads one window per call and reports the exhaustion. It owns `ScopeSubjectTuple`, `ScopeSubjectExhausted` and `ScopeWindow`, and counts the window event's distinct keys over the key positions given at construction. It is form-free.
2. **The frames** (L2), `window_frame.go`. `windowFrames` is over the req lists, O and the environment relation. `windowRun(window, ordinal)` and `environmentRun` give each frame's run. It is form-free.
3. **The kept branch** (L3a, L3b), `join_index.go` and `kept_branch.go`. `cachedBranch` is a `JoinIndex` over the branch's result symbols. `buildBranchTuples` runs a plan through `runPlan`, presents the answer under the enclosing names, and reads it through `collectBranch`. It is form-free. The EA build of L7 is a second build strategy the policy supplies.
4. **The continuations** (L6), `rules.go`. `ruleContinuations` is the evaluations a form execution continues across its windows. `continuationsFor` in `windowed_form.go` gives the ownership rule: the outermost windowed form's iterator owns them. It is form-free.
5. **The composition** (`windowed_form.go`). `framing` is the one value the composition reads: the form's name, the subject, the header's required run and find, what a branch is given, per given symbol the enclosing symbol it stands for, the key, the environment relation, a subquery's substitution, and whether a branch is first planned over the required run alone. `frameBranches` frames and plans each branch. `formPolicy` is the five hooks a form supplies: `window`, `narrow`, `build`, `built` and `answer`. `windowedIterator` composes modules 1 to 4 under it: fill a window (1), per frame compute the run (2), per branch narrow the run under the form's rule and build (3), per window tuple answer under the form's rule (6), and deduplicate under the form's rule.
6. **The emit** (L4), `match_emit.go`. `matchEmit` filters a probe's matches to those agreeing with the subject tuple on the carried outputs, projects them onto the result's symbols, and drains them one tuple at a time. The forms with outputs share it.
7. **The branch forms' policy** (L3c, L4's rule, L5, L7, L8), `or_fallback_relation.go`. `OrFallbackRelation` holds the header, the branches, the subject and the plans, derives the properties (`orProperties`, `orBranchesEmitAtMostOne`) and the deduplication rule, and frames its branches through `framing` over `planner.GivenSymbols`: the union's seek over R and C, and the fallback's R alone. `OrFallbackIterator` supplies the policy: `unanswered` as `narrow`, `buildFromCache` as `build`, `recordAnswered` as `built`, `prefetchWindow` as `window` under the fallback with prefetch enabled, and `answer` with `nextShortCircuit` or `nextCorrelatedUnion` as the emit rule.

The theorem's proof is the same text with each lemma supplied by its module, and each module's tests are the lemma's tests.

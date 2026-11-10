package executor_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// countingPlanner counts the plans asked of the planner it wraps.
type countingPlanner struct {
	planner.QueryPlanner
	queries, scopes int
}

func (p *countingPlanner) PlanQuery(q *query.Query, bindings query.Bindings, handler annotations.Handler) (*planner.RealizedPlan, error) {
	p.queries++
	return p.QueryPlanner.PlanQuery(q, bindings, handler)
}

func (p *countingPlanner) PlanScope(s planner.Scope, bindings query.Bindings, handler annotations.Handler) (*planner.RealizedPlan, error) {
	p.scopes++
	return p.QueryPlanner.PlanScope(s, bindings, handler)
}

// markedPageGraph links page1 to page2 and page2 to page3 and page7 by
// :page/a, page3 to page9 and page7 to page8 by :page/b, and marks page7 by
// :page/c, so a predicate s over the mark holds of page7 alone: a path
// through page7 that a negation of s guards is cut, and one a positive s
// guards is the only one kept.
func markedPageGraph() []datalog.Datom {
	pageA, pageB, pageC := datalog.NewKeyword(":page/a"), datalog.NewKeyword(":page/b"), datalog.NewKeyword(":page/c")
	stamp := func(i int) datalog.ElementID { return datalog.ElementID{Lamport: uint64(i), ReplicaID: 1} }
	return []datalog.Datom{
		{E: Page(1), A: pageA, V: Page(2), Tx: stamp(1)},
		{E: Page(2), A: pageA, V: Page(3), Tx: stamp(2)},
		{E: Page(2), A: pageA, V: Page(7), Tx: stamp(3)},
		{E: Page(3), A: pageB, V: Page(9), Tx: stamp(4)},
		{E: Page(7), A: pageB, V: Page(8), Tx: stamp(5)},
		{E: Page(7), A: pageC, V: "marked", Tx: stamp(6)},
	}
}

// tuplesBoundTo runs a query whose one scalar input binds an invocation's
// argument to start, and returns its tuples.
func tuplesBoundTo(t *testing.T, exec *Executor, rs *query.RuleSet, q *query.Query, start datalog.Identity) []Tuple {
	t.Helper()
	inputs, err := CreateInputRelationsFromValuesWithOptions(q,
		[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), start}, exec.Options())
	if err != nil {
		t.Fatalf("binding: %v", err)
	}
	rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs)
	if err != nil {
		t.Fatalf("bound to %s: %v", start, err)
	}
	var tuples []Tuple
	if err := CollectTuplesInto(&tuples, rel); err != nil {
		t.Fatalf("bound to %s: %v", start, err)
	}
	return tuples
}

// requireSamePages asserts that a result over one symbol holds exactly the
// pages given.
func requireSamePages(t *testing.T, label string, got []Tuple, want ...datalog.Identity) {
	t.Helper()
	have := NewTupleSet([]int{0}, []int{0}, len(got))
	for _, tuple := range got {
		if _, hash, held := have.Find(tuple); !held {
			have.Add(tuple, hash)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%s = %v, want %v", label, got, want)
		return
	}
	for _, p := range want {
		if !have.Probe(Tuple{p}) {
			t.Errorf("%s = %v, want %v", label, got, want)
			return
		}
	}
}

// Executing a planned query plans no body: the planner is asked for the
// query's plan once, and the bodies the query's invocations run, free and
// bound, are read from that plan, so no body is planned and no set is
// rewritten at evaluation. An invocation a streaming group binds is framed
// over the group at the handler, the one scope planned at evaluation
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.1), and the frame's
// plan carries what the invocation inside it runs.
func TestRules_ExecutingAPlannedQueryPlansNoBody(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(6))
	rs := MustRules(t, FollowsRulesText)
	queries := []struct {
		name   string
		q      *query.Query
		inputs []interface{}
		scopes int
	}{
		{"free", MustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`), []interface{}{datalog.SymDollar, datalog.NewSymbol("%")}, 0},
		{"bound by the environment", MustParse(t, `[:find ?y :in $ % ?x :where (follows ?x ?y)]`), []interface{}{datalog.SymDollar, datalog.NewSymbol("%"), Page(1)}, 0},
		{"bound by a streaming group", MustParse(t, `[:find ?y :in $ % :where [?x :page/kind "leaf"] (follows ?x ?y)]`), []interface{}{datalog.SymDollar, datalog.NewSymbol("%")}, 1},
	}
	for _, mode := range LegsFor(t, FusionOption) {
		for _, tc := range queries {
			t.Run(mode.Name()+"/"+tc.name, func(t *testing.T) {
				exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
				counting := &countingPlanner{QueryPlanner: exec.GetPlanner()}
				exec.SetPlanner(counting)
				inputs, err := CreateInputRelationsFromValuesWithOptions(tc.q, tc.inputs, exec.Options())
				require.NoError(t, err)
				result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), tc.q, inputs)
				require.NoError(t, err)
				_, err = CollectTuples(result, nil)
				require.NoError(t, err)
				require.Equal(t, 1, counting.queries, "the planner is asked for the query's plan once; every body is read from it")
				require.Equal(t, tc.scopes, counting.scopes, "the scopes planned at evaluation: the frame of an invocation a streaming group binds, and no other")
			})
		}
	}
}

func TestRules_TransitiveClosureOnChain(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(6))
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			if err != nil {
				t.Fatalf("closure query: %v", err)
			}
			RequireSamePairs(t, IdentityPairs(t, result), ChainClosure(6))
		})
	}
}

func TestEvaluateRules_SemiNaiveStopsWhenDeltaEmpty(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(5))
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			var iterations []annotations.Event
			opts := mode.PlannerOptions()
			opts.Handler = mode.Recording(func(e annotations.Event) {
				if e.Name == annotations.RulesIteration {
					iterations = append(iterations, e)
				}
			})
			exec := NewExecutorWithOptions(matcher, nil, opts)
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			if err != nil {
				t.Fatalf("closure query: %v", err)
			}
			RequireSamePairs(t, IdentityPairs(t, result), ChainClosure(5))
			// The longest path has four edges: iteration 0 derives the edges,
			// iterations 1 to 3 the longer paths, and iteration 4 finds nothing
			// new and ends the stratum.
			if len(iterations) != 5 {
				t.Errorf("observed %d iteration events, want 5 (the longest path's edge count plus the empty one): %v", len(iterations), iterations)
			}
		})
	}
}

func TestEvaluateRules_TwoRecursiveOccurrences(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(6))
	rs := MustRules(t, `[[(path ?a ?b) [?a :page/next ?b]]
	                     [(path ?a ?c) (path ?a ?b) (path ?b ?c)]]`)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (path ?x ?y)]`)
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			if err != nil {
				t.Fatalf("doubling closure: %v", err)
			}
			RequireSamePairs(t, IdentityPairs(t, result), ChainClosure(6))
		})
	}
}

// A pass runs each delta copy whose marked predicate gained tuples since the
// copy last ran, in the pass order, and no other copy
// (docs/proposals/A_COPY_READS_THE_TUPLES_ADDED_SINCE_IT_RAN_PROOF.md, T5).
// Over two mutually recursive predicates, each body's one delta copy marked
// on the other, the pass order is even's copy, marked on odd, then odd's
// copy, marked on even, so a pass begins even's copy's phases when the pass
// before added to odd, and odd's copy's phases when the pass itself added to
// even.
func TestRunStratum_RunsTheCopiesOfThePredicatesThatGainedTuples(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(6))
	rs := MustRules(t, `[[(odd ?a ?b) [?a :page/next ?b]]
	                     [(even ?a ?c) (odd ?a ?b) [?b :page/next ?c]]
	                     [(odd ?a ?c) (even ?a ?b) [?b :page/next ?c]]]`)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (odd ?x ?y)]`)
	odd, even := datalog.NewSymbol("odd"), datalog.NewSymbol("even")
	oddPairs := map[[2]datalog.Identity]bool{}
	for i := 1; i <= 6; i++ {
		for j := i + 1; j <= 6; j += 2 {
			oddPairs[[2]datalog.Identity{Page(i), Page(j)}] = true
		}
	}
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			var events []annotations.Event
			opts := mode.PlannerOptions()
			opts.Handler = mode.Recording(func(e annotations.Event) { events = append(events, e) })
			exec := NewExecutorWithOptions(matcher, nil, opts)
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			require.NoError(t, err)
			RequireSamePairs(t, IdentityPairs(t, result), oddPairs)

			// The phases of each copy, by the predicate the copy is marked
			// on: the copy of even's body reads odd's delta, and the delta
			// copy of odd's second body reads even's.
			bodies, err := planner.PlanStratum(exec.GetPlanner(), rs, rs.Strata()[0], nil)
			require.NoError(t, err)
			require.Len(t, bodies.Delta, 2, "one delta copy per body with an occurrence")
			phases := map[query.Symbol]int{}
			for _, body := range bodies.Delta {
				marked := odd
				if body.Rule.Name == odd {
					marked = even
				}
				phases[marked] += len(body.Plan.Phases)
			}

			var previous map[query.Symbol]int
			begun, compared := 0, 0
			for _, e := range events {
				switch e.Name {
				case annotations.PhaseBegin:
					begun++
				case annotations.RulesIteration:
					added := e.Data["delta"].(map[query.Symbol]int)
					if previous != nil {
						want := 0
						if previous[odd] > 0 {
							want += phases[odd]
						}
						if added[even] > 0 {
							want += phases[even]
						}
						require.Equal(t, want, begun, "pass %v: the phases begun are even's copy's when the pass before added to odd and odd's copy's when this pass added to even; before %v, added %v", e.Data["iteration"], previous, added)
						compared++
					}
					previous = added
					begun = 0
				}
			}
			require.GreaterOrEqual(t, compared, 3, "the chain's odd and even paths alternate over several passes")
		})
	}
}

func TestEvaluateRules_StratifiedNegation(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(4))
	rs := MustRules(t, `[[(follows ?a ?b) [?a :page/next ?b]]
	                     [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]
	                     [(first ?p) [?p :page/kind _] (not (follows _ ?p))]]`)
	q := MustParse(t, `[:find ?p :in $ % :where (first ?p)]`)
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil))
			if err != nil {
				t.Fatalf("first query: %v", err)
			}
			if len(tuples) != 1 || tuples[0][0] != Page(1) {
				t.Errorf("first = %v, want page1 alone: the one page no page follows", tuples)
			}
		})
	}
}

func TestEvaluateRules_NaiveStepEqualsSemiNaive(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(6))
	rs := MustRules(t, FollowsRulesText)
	follows := datalog.NewSymbol("follows")
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			ctx := NewContext().WithRules(rs)
			stratum := rs.Strata()[0]
			bodies, err := planner.PlanStratum(exec.GetPlanner(), rs, stratum, nil)
			if err != nil {
				t.Fatalf("PlanStratum: %v", err)
			}
			eval := exec.PrepareStratum(rs, planner.PlannedStratum{Stratum: stratum, Bodies: bodies})
			full := map[query.Symbol]*DerivedSet{follows: NewDerivedSet(eval.Symbols(follows))}
			seed, err := eval.Run(ctx, nil, eval.Bodies().Plain)
			if err != nil {
				t.Fatalf("seeding: %v", err)
			}
			for _, rel := range seed[follows] {
				if _, err := full[follows].Absorb(rel); err != nil {
					t.Fatalf("seeding: %v", err)
				}
			}
			// Naive: every occurrence reads the full relation, the delta
			// symbol's among them, and the loop ends when a pass derives
			// nothing new.
			for grew := true; grew; {
				whole := full[follows].Relation(exec.Options())
				view := map[query.Symbol]Relation{follows: whole, query.DeltaSymbol(follows): whole}
				got, err := eval.Run(ctx, view, eval.Bodies().Delta)
				if err != nil {
					t.Fatalf("naive pass: %v", err)
				}
				grew = false
				for _, rel := range got[follows] {
					added, err := full[follows].Absorb(rel)
					if err != nil {
						t.Fatalf("naive pass: %v", err)
					}
					if len(added) > 0 {
						grew = true
					}
				}
			}
			RequireSamePairs(t, IdentityPairs(t, full[follows].Relation(exec.Options())), ChainClosure(6))
		})
	}
}

func TestEvaluateRules_IterationLimit(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(6))
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			opts := mode.PlannerOptions()
			opts.MaxRuleIterations = 2
			exec := NewExecutorWithOptions(matcher, nil, opts)
			_, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			var limit *query.RuleIterationLimitError
			if !errors.As(err, &limit) {
				t.Fatalf("want *query.RuleIterationLimitError, got %v", err)
			}
			if len(limit.Predicates) != 1 || limit.Predicates[0] != datalog.NewSymbol("follows") || limit.Limit != 2 || limit.Reached != 3 {
				t.Errorf("limit error = %+v, want follows, limit 2, reached 3", limit)
			}
		})
	}
}

func TestEvaluateRules_DerivedTupleLimit(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(6))
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			opts := mode.PlannerOptions()
			opts.MaxDerivedTuples = 7
			exec := NewExecutorWithOptions(matcher, nil, opts)
			_, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			var limit *query.DerivedTupleLimitError
			if !errors.As(err, &limit) {
				t.Fatalf("want *query.DerivedTupleLimitError, got %v", err)
			}
			if len(limit.Predicates) != 1 || limit.Predicates[0] != datalog.NewSymbol("follows") || limit.Limit != 7 || limit.Reached <= 7 {
				t.Errorf("limit error = %+v, want follows, limit 7, more than 7 reached", limit)
			}
		})
	}
}

// TestExecuteWithRelations_BoundInvocationDerivesOnlyWhatItReaches binds the
// start of a closure over ten chains and caps the derived tuples between one
// chain's closure and the graph's: the invocation derives the chain its start
// reaches and nothing of the other nine.
func TestExecuteWithRelations_BoundInvocationDerivesOnlyWhatItReaches(t *testing.T) {
	matcher := factsDatabase(t, chainsDatoms(100))
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?y :in $ % ?x :where (follows ?x ?y)]`)
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			opts := mode.PlannerOptions()
			opts.MaxDerivedTuples = 100
			exec := NewExecutorWithOptions(matcher, nil, opts)
			inputs, err := CreateInputRelationsFromValuesWithOptions(q,
				[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), Page(0)}, exec.Options())
			if err != nil {
				t.Fatalf("binding: %v", err)
			}
			tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs))
			if err != nil {
				t.Fatalf("a bound invocation derives only what its start reaches: %v", err)
			}
			if len(tuples) != 10 {
				t.Errorf("got %d pages, want the ten page0 reaches", len(tuples))
			}
		})
	}
}

func TestRules_SubqueryPassesRulesThrough(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(4))
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?w
	                    :in $ %
	                    :where [?x :page/kind _]
	                           [(q [:find ?w :in $ % ?x :where (follows ?x ?w)] $ % ?x) [[?w] ...]]]`)
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			if err != nil {
				t.Fatalf("subquery invocation: %v", err)
			}
			RequireSamePairs(t, IdentityPairs(t, result), ChainClosure(4))
		})
	}
}

// A rule body written with its recursive invocation before the pattern that
// binds the invocation's argument is rewritten in the planner's order, where
// the pattern seeks on the required run first and the invocation is bound by
// what it provides, so a bound invocation derives what its binding reaches
// and never the predicate whole (REWRITE_ORDER_PROOF.md T4, NESTED §5.5).
func TestDeriveInvocation_ABodyWrittenBeforeItsBinderDerivesWhatTheBindingReaches(t *testing.T) {
	rs := MustRules(t, `[[(follows ?a ?b) [?a :page/next ?b]]
	                     [(follows ?a ?c) (follows ?b ?c) [?a :page/next ?b]]]`)
	q := MustParse(t, `[:find ?y :in $ % ?x :where (follows ?x ?y)]`)
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			opts := mode.PlannerOptions()
			opts.MaxDerivedTuples = 100
			exec := NewExecutorWithOptions(factsDatabase(t, chainsDatoms(1000)), nil, opts)
			inputs, err := CreateInputRelationsFromValuesWithOptions(q, []interface{}{datalog.SymDollar, datalog.NewSymbol("%"), Page(0)}, exec.Options())
			require.NoError(t, err)
			tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs))
			require.NoError(t, err, "the bound invocation derives the chain its start reaches under the cap")
			require.Len(t, tuples, 10, "the ten pages page0 reaches")
		})
	}
}

func TestEvaluateRules_DerivesOnlyTheNeededStrata(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(4))
	rs := MustRules(t, `[[(follows ?a ?b) [?a :page/next ?b]]
	                     [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]
	                     [(marked ?p) [?p :page/kind "marked"]]]`)
	exec := NewExecutor(matcher, nil)
	planned, err := planner.PlanEvaluation(exec.GetPlanner(), rs, datalog.NewSymbol("follows"), []bool{false, false}, nil)
	if err != nil {
		t.Fatalf("PlanEvaluation: %v", err)
	}
	ev := NewRuleEvaluation(exec, planned)
	if _, err := ev.Extend(NewContext().WithRules(rs), nil); err != nil {
		t.Fatalf("extend: %v", err)
	}
	ev.Complete()
	rel, ok := ev.Derived()[datalog.NewSymbol("follows")]
	if !ok {
		t.Fatal("the invoked predicate is derived")
	}
	RequireSamePairs(t, IdentityPairs(t, rel), ChainClosure(4))
	if _, ok := ev.Derived()[datalog.NewSymbol("marked")]; ok {
		t.Error("a predicate nothing depends on is not derived")
	}
	if ev.Total() != len(ChainClosure(4)) {
		t.Errorf("the evaluation counted %d tuples, want %d: the needed stratum's alone", ev.Total(), len(ChainClosure(4)))
	}
}

// TestExecuteWithRelations_SeededEqualsWhole is the rewrite's
// semantic-preservation test: for every rules fixture and every argument
// position, the invocation bound at that position through an input returns
// the whole invocation's tuples carrying that value there.
func TestExecuteWithRelations_SeededEqualsWhole(t *testing.T) {
	fixtures := []struct {
		name, rules, predicate string
		arity                  int
	}{
		{"closure", FollowsRulesText, "follows", 2},
		{"doubling", `[[(path ?a ?b) [?a :page/next ?b]]
		               [(path ?a ?c) (path ?a ?b) (path ?b ?c)]]`, "path", 2},
		{"negation", `[[(follows ?a ?b) [?a :page/next ?b]]
		               [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]
		               [(first ?p) [?p :page/kind _] (not (follows _ ?p))]]`, "first", 1},
		{"or-join", `[[(follows ?a ?b) [?a :page/next ?b]]
		              [(follows ?a ?c) [?a :page/next ?b] (or-join [?b ?c] (follows ?b ?c) [?b :page/next ?c])]]`, "follows", 2},
		{"not-join", `[[(follows ?a ?b) [?a :page/next ?b]]
		               [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]
		               [(last ?p) [?p :page/kind _] (not-join [?p] (follows ?p _))]]`, "last", 1},
	}
	matcher := factsDatabase(t, ChainDatoms(6))
	for _, f := range fixtures {
		rs := MustRules(t, f.rules)
		vars := make([]string, f.arity)
		head := ""
		for i := range vars {
			vars[i] = fmt.Sprintf("?v%d", i)
			if i > 0 {
				head += " "
			}
			head += vars[i]
		}
		whole := MustParse(t, fmt.Sprintf(`[:find %s :in $ %% :where (%s %s)]`, head, f.predicate, head))
		for _, mode := range LegsFor(t, SharingOption, FusionOption) {
			t.Run(f.name+"/"+mode.Name(), func(t *testing.T) {
				exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
				all, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), whole, nil))
				if err != nil {
					t.Fatalf("whole invocation: %v", err)
				}
				if len(all) == 0 {
					t.Fatal("the fixture derives nothing, so the contract is untestable")
				}
				for i := 0; i < f.arity; i++ {
					bound := MustParse(t, fmt.Sprintf(`[:find %s :in $ %% %s :where (%s %s)]`, head, vars[i], f.predicate, head))
					values := NewTupleSet([]int{i}, []int{i}, 0)
					for _, tuple := range all {
						_, hash, held := values.Find(tuple)
						if held {
							continue
						}
						values.Add(tuple, hash)
						v := tuple[i]
						positions := AllPositions(len(tuple))
						want := NewTupleSet(positions, positions, 0)
						wantCount := 0
						for _, w := range all {
							if datalog.ValuesEqual(w[i], v) {
								want.Add(w, HashTuplePositions(w, positions))
								wantCount++
							}
						}
						inputs, err := CreateInputRelationsFromValuesWithOptions(bound,
							[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), v}, exec.Options())
						if err != nil {
							t.Fatalf("binding position %d: %v", i, err)
						}
						got, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), bound, inputs))
						if err != nil {
							t.Fatalf("position %d bound to %v: %v", i, v, err)
						}
						if len(got) != wantCount {
							t.Errorf("position %d bound to %v: got %d tuples, want %d", i, v, len(got), wantCount)
						}
						for _, g := range got {
							if !want.Probe(g) {
								t.Errorf("position %d bound to %v: %v is not among the whole invocation's tuples", i, v, g)
							}
						}
					}
				}
			})
		}
	}
}

// TestExecuteWithRelations_NegationAfterARecursiveInvocationRewrites binds an
// invocation of q whose recursive rule negates s after the recursive
// invocation of q. The binding that invocation provides does not reach the
// negation, so the magic rule of s reads no adorned q, the rewritten set
// stratifies, and each query records one rewrite of q under bf and derives
// what the whole predicate filtered to its start holds.
func TestExecuteWithRelations_NegationAfterARecursiveInvocationRewrites(t *testing.T) {
	matcher := factsDatabase(t, markedPageGraph())
	rs := MustRules(t, `[[(q ?x ?z) [?x :page/a ?z]]
	                     [(q ?x ?z) [?x :page/a ?y] (q ?y ?w) (not (s ?w)) [?w :page/b ?z]]
	                     [(s ?w) [?w :page/c _]]]`)
	q := MustParse(t, `[:find ?z :in $ % ?x :where (q ?x ?z)]`)
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			var rewrites []annotations.Event
			opts := mode.PlannerOptions()
			opts.Handler = mode.Recording(func(e annotations.Event) {
				if e.Name == annotations.RulesMagicSets {
					rewrites = append(rewrites, e)
				}
			})
			exec := NewExecutorWithOptions(matcher, nil, opts)
			// q(1, 9) holds through q(2, 3), since s(3) does not hold and
			// b(3, 9); q(1, 8) does not, since s(7) holds.
			requireSamePages(t, "q bound to page1", tuplesBoundTo(t, exec, rs, q, Page(1)), Page(2), Page(9))
			requireSamePages(t, "q bound to page2", tuplesBoundTo(t, exec, rs, q, Page(2)), Page(3), Page(7))
			if len(rewrites) != 2 {
				t.Fatalf("recorded %d rewrites, want one per invocation: %v", len(rewrites), rewrites)
			}
			for _, r := range rewrites {
				adornment, _ := r.Data["adornment"].([]bool)
				if r.Data["predicate"] != datalog.NewSymbol("q") || len(adornment) != 2 || !adornment[0] || adornment[1] {
					t.Errorf("rewrite %v, want q under bf", r.Data)
				}
			}
		})
	}
}

// A negated site whose adornment would not stratify is left unadorned and
// the rewrite is accepted: q's second rule invokes s after the recursive
// invocation of q, so the magic rule of adorned s reads the adorned q, and
// q's third rule negates s, where adorning it would close a cycle through
// the negation. The query answers, and the rewrite made for each invocation
// is recorded with the predicate and its adornment
// (NON_MONOTONE_FORMS_PROOF.md T6).
func TestExecuteWithRelations_ANegatedSiteLeftUnadornedAnswers(t *testing.T) {
	matcher := factsDatabase(t, markedPageGraph())
	rs := MustRules(t, `[[(q ?x ?z) [?x :page/a ?z]]
	                     [(q ?x ?z) [?x :page/a ?y] (q ?y ?w) (s ?w) [?w :page/b ?z]]
	                     [(q ?x ?z) [?x :page/a ?w] (not (s ?w)) [?w :page/b ?z]]
	                     [(s ?w) [?w :page/c _]]]`)
	q := MustParse(t, `[:find ?z :in $ % ?x :where (q ?x ?z)]`)
	for _, mode := range LegsFor(t, SharingOption, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			var rewrites []annotations.Event
			opts := mode.PlannerOptions()
			opts.Handler = mode.Recording(func(e annotations.Event) {
				if e.Name == annotations.RulesMagicSets {
					rewrites = append(rewrites, e)
				}
			})
			exec := NewExecutorWithOptions(matcher, nil, opts)
			// q(1, 8) holds through q(2, 7), since s(7) holds and b(7, 8);
			// q(2, 9) holds by the third rule, since s(3) does not hold and
			// b(3, 9).
			requireSamePages(t, "q bound to page1", tuplesBoundTo(t, exec, rs, q, Page(1)), Page(2), Page(8))
			requireSamePages(t, "q bound to page2", tuplesBoundTo(t, exec, rs, q, Page(2)), Page(3), Page(7), Page(9))
			if len(rewrites) != 2 {
				t.Fatalf("recorded %d rewrites, want one per invocation: %v", len(rewrites), rewrites)
			}
			for _, r := range rewrites {
				adornment, _ := r.Data["adornment"].([]bool)
				if r.Data["predicate"] != datalog.NewSymbol("q") || len(adornment) != 2 || !adornment[0] || adornment[1] {
					t.Errorf("rewrite %v, want q under bf", r.Data)
				}
			}
		})
	}
}

// The same shape with path requiring its first position: the rewrite is
// accepted with the negated site unadorned, and the query answers, where a
// predicate requiring bindings has no whole evaluation.
func TestExecuteWithRelations_ARequiredPredicateWithANegatedSiteLeftUnadornedAnswers(t *testing.T) {
	matcher := factsDatabase(t, markedPageGraph())
	rs := MustRules(t, `[[(path [?x] ?z) [?x :page/a ?z]]
	                     [(path [?x] ?z) [?x :page/a ?y] (path ?y ?w) (s ?w) [?w :page/b ?z]]
	                     [(path [?x] ?z) [?x :page/a ?w] (not (s ?w)) [?w :page/b ?z]]
	                     [(s ?w) [?w :page/c _]]]`)
	q := MustParse(t, `[:find ?z :in $ % ?x :where (path ?x ?z)]`)
	for _, mode := range LegsFor(t, SharingOption, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			requireSamePages(t, "path bound to page1", tuplesBoundTo(t, exec, rs, q, Page(1)), Page(2), Page(8))
			requireSamePages(t, "path bound to page2", tuplesBoundTo(t, exec, rs, q, Page(2)), Page(3), Page(7), Page(9))
		})
	}
}

// An invocation under a form evaluates its predicate once per query: the
// rewrite and the strata an invocation under not runs number what the
// top-level invocation's do, with the sink of a chain of ten bound.
func TestExecuteWithRelations_InvocationUnderAFormEvaluatesOnce(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(10))
	rs := MustRules(t, FollowsRulesText)
	top := MustParse(t, `[:find ?x :in $ % ?s :where [?x :page/kind "leaf"] (follows ?x ?s)]`)
	under := MustParse(t, `[:find ?x :in $ % ?s :where [?x :page/kind "leaf"] (not (follows ?x ?s))]`)
	for _, mode := range LegsFor(t, SharingOption, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			var rewrites, strata int
			opts := mode.PlannerOptions()
			opts.Handler = mode.Recording(func(e annotations.Event) {
				switch e.Name {
				case annotations.RulesMagicSets:
					rewrites++
				case annotations.RulesStratumBegin:
					strata++
				}
			})
			exec := NewExecutorWithOptions(matcher, nil, opts)
			run := func(q *query.Query) int {
				inputs, err := CreateInputRelationsFromValuesWithOptions(q,
					[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), Page(10)}, exec.Options())
				if err != nil {
					t.Fatalf("binding: %v", err)
				}
				rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs)
				if err != nil {
					t.Fatal(err)
				}
				var tuples []Tuple
				if err := CollectTuplesInto(&tuples, rel); err != nil {
					t.Fatal(err)
				}
				return len(tuples)
			}
			if got := run(top); got != 9 {
				t.Errorf("top level: %d pages reach page10, want 9", got)
			}
			atTop := [2]int{rewrites, strata}
			rewrites, strata = 0, 0
			if got := run(under); got != 1 {
				t.Errorf("under not: %d pages do not reach page10, want page10 alone", got)
			}
			if got := [2]int{rewrites, strata}; got != atTop {
				t.Errorf("under not: %d rewrites and %d strata, want the top level's %d and %d", got[0], got[1], atTop[0], atTop[1])
			}
		})
	}
}

// Two invocations of one predicate under one form, bound at the same position
// to different values, each read the tuples their own binding reaches: on the
// chain of ten, page 5 and page 2 both reach page 6, and page 2 is not
// reached from page 5, so a not over both invocations excludes the pair
// only when the second invocation reads what its own binding reaches.
func TestExecuteWithRelations_TwoInvocationsUnderAFormKeepTheirOwnSeeds(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(10))
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?z :in $ % ?x ?z
	                     :where [?x :page/kind "leaf"] [?z :page/kind "leaf"]
	                            (not (follows ?x ?w) (follows ?z ?w))]`)
	for _, mode := range LegsFor(t, SharingOption, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			inputs, err := CreateInputRelationsFromValuesWithOptions(q,
				[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), Page(5), Page(2)}, exec.Options())
			if err != nil {
				t.Fatalf("binding: %v", err)
			}
			rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs)
			if err != nil {
				t.Fatal(err)
			}
			var tuples []Tuple
			if err := CollectTuplesInto(&tuples, rel); err != nil {
				t.Fatal(err)
			}
			if len(tuples) != 0 {
				t.Errorf("page 5 and page 2 both reach page 6, so the not excludes the pair; got %v", tuples)
			}
		})
	}
}

// TestEvaluateRules_SeedAbsorbedAtIterationZero seeds a rewritten closure's
// magic predicate directly: the seed's tuples are the magic stratum's
// iteration zero, and the adorned closure holds the pairs the seed reaches.
func TestEvaluateRules_SeedAbsorbedAtIterationZero(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(6))
	rs := MustRules(t, FollowsRulesText)
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			var iterations []annotations.Event
			opts := mode.PlannerOptions()
			opts.Handler = mode.Recording(func(e annotations.Event) {
				if e.Name == annotations.RulesIteration {
					iterations = append(iterations, e)
				}
			})
			exec := NewExecutorWithOptions(matcher, nil, opts)
			planned, err := planner.PlanEvaluation(exec.GetPlanner(), rs, datalog.NewSymbol("follows"), []bool{true, false}, nil)
			if err != nil {
				t.Fatalf("PlanEvaluation: %v", err)
			}
			magic := planned.Magic
			seedHead, _ := magic.Rules.Head(magic.Seed)
			seed := NewMaterializedRelation(seedHead, []Tuple{{Page(3)}})
			ev := NewRuleEvaluation(exec, planned)
			if _, err := ev.Extend(NewContext().WithRules(rs), seed); err != nil {
				t.Fatalf("extend: %v", err)
			}
			ev.Complete()
			want := map[[2]datalog.Identity]bool{}
			for i := 3; i <= 6; i++ {
				for j := i + 1; j <= 6; j++ {
					want[[2]datalog.Identity{Page(i), Page(j)}] = true
				}
			}
			RequireSamePairs(t, IdentityPairs(t, ev.Derived()[magic.Predicate]), want)
			if len(iterations) == 0 {
				t.Fatal("no iteration event was emitted")
			}
			first := iterations[0]
			predicates, _ := first.Data["predicates"].([]query.Symbol)
			delta, _ := first.Data["delta"].(map[query.Symbol]int)
			if first.Data["iteration"] != 0 || len(predicates) != 1 || predicates[0] != magic.Seed || delta[magic.Seed] != 1 {
				t.Errorf("first iteration event = %v, want iteration 0 of %s with the seed's one tuple as its delta", first.Data, magic.Seed)
			}
		})
	}
}

// TestExecuteWithRelations_VectorConstantArgument: a vector literal is an
// argument like any constant. It fixes its position to the vector, so the
// invocation keeps the tuples whose position holds an equal vector; it is a
// bound position, so the rewrite applies; and it seeds the magic predicate,
// so the adorned predicate derives the tuple the vector reaches.
func TestExecuteWithRelations_VectorConstantArgument(t *testing.T) {
	pageTags := datalog.NewKeyword(":page/tags")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	matcher := factsDatabase(t, []datalog.Datom{
		{E: Page(1), A: pageTags, V: []interface{}{int64(1), int64(2)}, Tx: tx},
		{E: Page(2), A: pageTags, V: []interface{}{int64(1), int64(2), int64(3)}, Tx: tx},
		{E: Page(3), A: pageTags, V: []interface{}{int64(2), int64(1)}, Tx: tx},
	})

	rs := MustRules(t, `[[(tagged ?p ?tags) [?p :page/tags ?tags]]]`)
	q := MustParse(t, `[:find ?p :in $ % :where (tagged ?p [1 2])]`)
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			var rewrites []annotations.Event
			opts := mode.PlannerOptions()
			opts.Handler = mode.Recording(func(e annotations.Event) {
				if e.Name == annotations.RulesMagicSets {
					rewrites = append(rewrites, e)
				}
			})
			exec := NewExecutorWithOptions(matcher, nil, opts)
			tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil))
			if err != nil {
				t.Fatalf("invocation with a vector argument: %v", err)
			}
			if len(tuples) != 1 || len(tuples[0]) != 1 || tuples[0][0] != Page(1) {
				t.Errorf("got %v, want the one page whose tags equal [1 2]", tuples)
			}
			if len(rewrites) != 1 {
				t.Fatalf("observed %d magic-set events, want one for the invocation", len(rewrites))
			}
			adornment, _ := rewrites[0].Data["adornment"].([]bool)
			if len(adornment) != 2 || adornment[0] || !adornment[1] {
				t.Errorf("magic-set event = %v, want the rewrite made with the vector's position bound", rewrites[0].Data)
			}
		})
	}
}

// TestPlan_PhasesParseBackToThemselves: the planner's output is a sequence
// of Datalog queries. Every phase of a query's plan, and every phase of every
// body plan the fixpoint prepares, is text the parser reads back to itself.
func TestPlan_PhasesParseBackToThemselves(t *testing.T) {
	matcher := factsDatabase(t, ChainDatoms(4))
	closure := MustRules(t, FollowsRulesText)
	successor := MustRules(t, SuccessorRulesText)
	rewritePlanner := planner.CreatePlanner(nil, DefaultPlannerOptions())
	closureMagic, err := planner.Rewrite(rewritePlanner, closure, datalog.NewSymbol("follows"), []bool{true, false}, nil)
	if err != nil {
		t.Fatalf("Rewrite(follows): %v", err)
	}
	successorMagic, err := planner.Rewrite(rewritePlanner, successor, datalog.NewSymbol("succ"), []bool{true, false}, nil)
	if err != nil {
		t.Fatalf("Rewrite(succ): %v", err)
	}
	queries := []struct {
		name  string
		text  string
		rules *query.RuleSet
		bound []string
	}{
		{"plain", `[:find ?x ?k :where [?x :page/next ?y] [?y :page/kind ?k]]`, nil, nil},
		{"whole", `[:find ?x ?y :in $ % :where (follows ?x ?y)]`, closure, nil},
		{"bound", `[:find ?y :in $ % ?x :where (follows ?x ?y)]`, closure, []string{"?x"}},
		{"required", `[:find ?m :in $ % :where (succ ?n ?m) [(ground 5) ?n]]`, successor, nil},
	}
	sets := []struct {
		name  string
		rules *query.RuleSet
	}{
		{"closure", closure},
		{"closure rewritten", closureMagic.Rules},
		{"successor rewritten", successorMagic.Rules},
	}
	type labelledPlan struct {
		label string
		plan  *planner.RealizedPlan
	}
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			var plans []labelledPlan
			for _, c := range queries {
				bindings := query.Bindings{Symbols: map[query.Symbol]bool{}, Rules: c.rules}
				for _, b := range c.bound {
					bindings.Symbols[datalog.NewSymbol(b)] = true
				}
				plan, err := exec.GetPlanner().PlanQuery(MustParse(t, c.text), bindings, nil)
				if err != nil {
					t.Fatalf("%s: planning: %v", c.name, err)
				}
				plans = append(plans, labelledPlan{c.name, plan})
			}
			for _, s := range sets {
				for _, stratum := range s.rules.Strata() {
					planned, err := planner.PlanStratum(exec.GetPlanner(), s.rules, stratum, nil)
					if err != nil {
						t.Fatalf("%s: PlanStratum %v: %v", s.name, stratum.Predicates, err)
					}
					for _, bodies := range [][]planner.PlannedBody{planned.Plain, planned.Delta, planned.Lower} {
						for _, body := range bodies {
							plans = append(plans, labelledPlan{s.name + ", body of " + body.Rule.Name.String(), body.Plan})
						}
					}
				}
			}
			for _, p := range plans {
				for i, phase := range p.plan.Phases {
					text := phase.Query.String()
					parsed, err := parser.ParseQuery(text)
					if err != nil {
						t.Errorf("%s: phase %d does not parse: %v\n%s", p.label, i+1, err, text)
						continue
					}
					if got := parsed.String(); got != phase.Query.String() {
						t.Errorf("%s: phase %d renders differently once parsed back:\nplanned:\n%s\nparsed:\n%s", p.label, i+1, phase.Query.String(), got)
					}
				}
			}
		})
	}
}

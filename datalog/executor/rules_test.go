package executor

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

var (
	pageNext = datalog.NewKeyword(":page/next")
	pageKind = datalog.NewKeyword(":page/kind")
)

func page(i int) datalog.Identity { return datalog.NewIdentity(fmt.Sprintf("page%d", i)) }

// chainDatoms links page1 through pageN by :page/next and marks every page
// with :page/kind.
func chainDatoms(n int) []datalog.Datom {
	var datoms []datalog.Datom
	for i := 1; i <= n; i++ {
		tx := datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}
		datoms = append(datoms, datalog.Datom{E: page(i), A: pageKind, V: "leaf", Tx: tx})
		if i < n {
			datoms = append(datoms, datalog.Datom{E: page(i), A: pageNext, V: page(i + 1), Tx: tx})
		}
	}
	return datoms
}

const followsRulesText = `[[(follows ?a ?b) [?a :page/next ?b]]
                          [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]]`

func mustRules(t testing.TB, text string) *query.RuleSet {
	t.Helper()
	rs, err := parser.ParseRules(text)
	if err != nil {
		t.Fatalf("ParseRules: %v", err)
	}
	return rs
}

func mustParse(t testing.TB, text string) *query.Query {
	t.Helper()
	q, err := parser.ParseQuery(text)
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	return q
}

// identityPairs reads a relation of two identities into a set.
func identityPairs(t testing.TB, rel Relation) map[[2]datalog.Identity]bool {
	t.Helper()
	pairs := map[[2]datalog.Identity]bool{}
	err := ForEach(rel, func(tuple Tuple) error {
		a, aok := tuple[0].(datalog.Identity)
		b, bok := tuple[1].(datalog.Identity)
		if !aok || !bok {
			return fmt.Errorf("tuple %v is not a pair of identities", tuple)
		}
		pairs[[2]datalog.Identity{a, b}] = true
		return nil
	})
	if err != nil {
		t.Fatalf("reading pairs: %v", err)
	}
	return pairs
}

// chainClosure is every pair (i, j) with i < j: the transitive closure of a
// chain of n pages computed by hand.
func chainClosure(n int) map[[2]datalog.Identity]bool {
	pairs := map[[2]datalog.Identity]bool{}
	for i := 1; i <= n; i++ {
		for j := i + 1; j <= n; j++ {
			pairs[[2]datalog.Identity{page(i), page(j)}] = true
		}
	}
	return pairs
}

func requireSamePairs(t *testing.T, got, want map[[2]datalog.Identity]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("got %d pairs, want %d", len(got), len(want))
	}
	for pair := range want {
		if !got[pair] {
			t.Errorf("missing %s → %s", pair[0], pair[1])
		}
	}
	for pair := range got {
		if !want[pair] {
			t.Errorf("unexpected %s → %s", pair[0], pair[1])
		}
	}
}

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

// Executing a planned query plans no body: the planner is asked for the
// query's plan once, and the bodies the query's invocations run, free and
// bound, are read from that plan, so no body is planned and no set is
// rewritten at evaluation. An invocation a streaming group binds is framed
// over the group at the handler, the one scope planned at evaluation
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.1), and the frame's
// plan carries what the invocation inside it runs.
func TestRules_ExecutingAPlannedQueryPlansNoBody(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(6))
	rs := mustRules(t, followsRulesText)
	queries := []struct {
		name   string
		q      *query.Query
		inputs []interface{}
		scopes int
	}{
		{"free", mustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`), []interface{}{datalog.SymDollar, datalog.NewSymbol("%")}, 0},
		{"bound by the environment", mustParse(t, `[:find ?y :in $ % ?x :where (follows ?x ?y)]`), []interface{}{datalog.SymDollar, datalog.NewSymbol("%"), page(1)}, 0},
		{"bound by a streaming group", mustParse(t, `[:find ?y :in $ % :where [?x :page/kind "leaf"] (follows ?x ?y)]`), []interface{}{datalog.SymDollar, datalog.NewSymbol("%")}, 1},
	}
	for _, mode := range optimizerModes {
		for _, tc := range queries {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
				counting := &countingPlanner{QueryPlanner: exec.planner}
				exec.planner = counting
				inputs, err := createInputRelationsFromValuesWithOptions(tc.q, tc.inputs, exec.options)
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
	matcher := NewMemoryPatternMatcher(chainDatoms(6))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			if err != nil {
				t.Fatalf("closure query: %v", err)
			}
			requireSamePairs(t, identityPairs(t, result), chainClosure(6))
		})
	}
}

func TestEvaluateRules_SemiNaiveStopsWhenDeltaEmpty(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(5))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var iterations []annotations.Event
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) {
				if e.Name == annotations.RulesIteration {
					iterations = append(iterations, e)
				}
			}
			exec := NewExecutorWithOptions(matcher, nil, opts)
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			if err != nil {
				t.Fatalf("closure query: %v", err)
			}
			requireSamePairs(t, identityPairs(t, result), chainClosure(5))
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
	matcher := NewMemoryPatternMatcher(chainDatoms(6))
	rs := mustRules(t, `[[(path ?a ?b) [?a :page/next ?b]]
	                     [(path ?a ?c) (path ?a ?b) (path ?b ?c)]]`)
	q := mustParse(t, `[:find ?x ?y :in $ % :where (path ?x ?y)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			if err != nil {
				t.Fatalf("doubling closure: %v", err)
			}
			requireSamePairs(t, identityPairs(t, result), chainClosure(6))
		})
	}
}

func TestEvaluateRules_StratifiedNegation(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(4))
	rs := mustRules(t, `[[(follows ?a ?b) [?a :page/next ?b]]
	                     [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]
	                     [(first ?p) [?p :page/kind _] (not (follows _ ?p))]]`)
	q := mustParse(t, `[:find ?p :in $ % :where (first ?p)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil))
			if err != nil {
				t.Fatalf("first query: %v", err)
			}
			if len(tuples) != 1 || tuples[0][0] != page(1) {
				t.Errorf("first = %v, want page1 alone: the one page no page follows", tuples)
			}
		})
	}
}

func TestEvaluateRules_NaiveStepEqualsSemiNaive(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(6))
	rs := mustRules(t, followsRulesText)
	follows := datalog.NewSymbol("follows")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			ctx := NewContext().WithRules(rs)
			stratum := rs.Strata()[0]
			bodies, err := planner.PlanStratum(exec.planner, rs, stratum, nil)
			if err != nil {
				t.Fatalf("PlanStratum: %v", err)
			}
			eval := exec.prepareStratum(rs, planner.PlannedStratum{Stratum: stratum, Bodies: bodies})
			full := map[query.Symbol]*derivedSet{follows: newDerivedSet(eval.symbols[follows])}
			seed, err := eval.run(ctx, nil, eval.bodies.Plain)
			if err != nil {
				t.Fatalf("seeding: %v", err)
			}
			for _, rel := range seed[follows] {
				if _, err := full[follows].absorb(rel); err != nil {
					t.Fatalf("seeding: %v", err)
				}
			}
			// Naive: every occurrence reads the full relation, the delta
			// symbol's among them, and the loop ends when a pass derives
			// nothing new.
			for grew := true; grew; {
				whole := full[follows].relation(exec.options)
				view := map[query.Symbol]Relation{follows: whole, query.DeltaSymbol(follows): whole}
				got, err := eval.run(ctx, view, eval.bodies.Delta)
				if err != nil {
					t.Fatalf("naive pass: %v", err)
				}
				grew = false
				for _, rel := range got[follows] {
					added, err := full[follows].absorb(rel)
					if err != nil {
						t.Fatalf("naive pass: %v", err)
					}
					if len(added) > 0 {
						grew = true
					}
				}
			}
			requireSamePairs(t, identityPairs(t, full[follows].relation(exec.options)), chainClosure(6))
		})
	}
}

func TestExecuteRuleInvocation_ProjectsDerivedRelation(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	derived := NewMaterializedRelation([]query.Symbol{a, b}, []Tuple{
		{page(1), page(2)}, {page(1), page(3)}, {page(2), page(3)}, {page(3), page(3)},
	})
	delta := NewMaterializedRelation([]query.Symbol{a, b}, []Tuple{{page(2), page(3)}})
	follows := datalog.NewSymbol("follows")
	ctx := NewContext().WithRules(mustRules(t, followsRulesText)).WithDerived(map[query.Symbol]Relation{
		follows:                    derived,
		query.DeltaSymbol(follows): delta,
	})
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})
	invoke := func(args ...query.PatternElement) *query.RuleInvocation {
		return &query.RuleInvocation{Name: follows, Args: args}
	}
	variable := func(s query.Symbol) query.PatternElement { return query.Variable{Name: s} }

	t.Run("renames the positions to the arguments", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(variable(x), variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		if syms := rel.Symbols(); len(syms) != 2 || syms[0] != x || syms[1] != y {
			t.Errorf("symbols = %v, want ?x ?y", syms)
		}
		if rel.Size() != 4 {
			t.Errorf("size = %d, want every derived tuple", rel.Size())
		}
	})
	t.Run("a constant filters its position", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(variable(x), query.Constant{Value: page(3)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		tuples, _ := CollectTuples(rel, nil)
		if len(tuples) != 3 {
			t.Errorf("got %v, want the three tuples whose second position is page3", tuples)
		}
		if syms := rel.Symbols(); len(syms) != 1 || syms[0] != x {
			t.Errorf("symbols = %v, want ?x alone", syms)
		}
	})
	t.Run("a repeated variable filters to equal positions", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(variable(x), variable(x)), nil)
		if err != nil {
			t.Fatal(err)
		}
		tuples, _ := CollectTuples(rel, nil)
		if len(tuples) != 1 || tuples[0][0] != page(3) {
			t.Errorf("got %v, want page3 alone", tuples)
		}
	})
	t.Run("a blank is projected away", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(query.Blank{}, variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		if syms := rel.Symbols(); len(syms) != 1 || syms[0] != y {
			t.Errorf("symbols = %v, want ?y alone", syms)
		}
		tuples, err := CollectTuples(rel, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples) != 2 {
			t.Errorf("got %v, want the two distinct second positions", tuples)
		}
	})
	t.Run("an environment-bound argument filters to its value", func(t *testing.T) {
		env := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{page(1)}})
		rel, err := exec.executeRuleInvocation(ctx.WithEnvironment(env), invoke(variable(x), variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		tuples, _ := CollectTuples(rel, nil)
		if len(tuples) != 2 {
			t.Errorf("got %v, want the two tuples starting at page1", tuples)
		}
	})
	t.Run("an invocation of the delta symbol reads the delta", func(t *testing.T) {
		inv := &query.RuleInvocation{Name: query.DeltaSymbol(follows), Args: []query.PatternElement{variable(x), variable(y)}}
		rel, err := exec.executeRuleInvocation(ctx, inv, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rel.Size() != 1 {
			t.Errorf("size = %d, want the delta's one tuple", rel.Size())
		}
	})
	t.Run("an all-constant invocation is a verdict", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(query.Constant{Value: page(1)}, query.Constant{Value: page(2)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rel.Symbols()) != 0 || rel.Size() != 1 {
			t.Errorf("a held fact is the zero-symbol unit, got %v with %d tuples", rel.Symbols(), rel.Size())
		}
		rel, err = exec.executeRuleInvocation(ctx, invoke(query.Constant{Value: page(2)}, query.Constant{Value: page(1)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rel.Symbols()) != 0 || rel.Size() != 0 {
			t.Errorf("an absent fact is the zero-symbol empty relation, got %v with %d tuples", rel.Symbols(), rel.Size())
		}
	})
	t.Run("a predicate no scope derived and no rule set can derive is an error", func(t *testing.T) {
		_, err := exec.executeRuleInvocation(NewContext(), invoke(variable(x), variable(y)), nil)
		if err == nil {
			t.Error("expected an error naming the invocation and the missing rule set")
		}
	})
}

func TestExecuteRuleInvocation_ReadsThePredicateInPlace(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	follows := datalog.NewSymbol("follows")
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})
	variable := func(s query.Symbol) query.PatternElement { return query.Variable{Name: s} }
	// derivedOf is a scope whose follows relation holds n pairs, each ending
	// at page0, so a constant page0 at the second position keeps every pair.
	rs := mustRules(t, followsRulesText)
	derivedOf := func(n int) *Context {
		tuples := make([]Tuple, n)
		for i := range tuples {
			tuples[i] = Tuple{page(i + 1), page(0)}
		}
		return NewContext().WithRules(rs).WithDerived(
			map[query.Symbol]Relation{follows: NewMaterializedRelation([]query.Symbol{a, b}, tuples)})
	}
	allocations := func(ctx *Context, inv *query.RuleInvocation) float64 {
		return testing.AllocsPerRun(10, func() {
			if _, err := exec.executeRuleInvocation(ctx, inv, nil); err != nil {
				t.Fatal(err)
			}
		})
	}
	small, large := derivedOf(8), derivedOf(8192)
	cases := map[string]*query.RuleInvocation{
		"renamed":  {Name: follows, Args: []query.PatternElement{variable(x), variable(y)}},
		"filtered": {Name: follows, Args: []query.PatternElement{variable(x), query.Constant{Value: page(0)}}},
	}
	for name, inv := range cases {
		t.Run(name, func(t *testing.T) {
			s, l := allocations(small, inv), allocations(large, inv)
			if l > s {
				t.Errorf("invoking over 8192 tuples allocates %v times and over 8 tuples %v times: the invocation copies its predicate instead of reading it in place", l, s)
			}
		})
	}
}

func TestDerivedSet_AbsorbCopiesOnlyNewTuples(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	symbols := []query.Symbol{a, b}
	pairs := func(n int) []Tuple {
		tuples := make([]Tuple, n)
		for i := range tuples {
			tuples[i] = Tuple{page(i + 1), page(0)}
		}
		return tuples
	}
	// streamOf yields the tuples through a relation that, like a body's
	// result, warrants nothing about its tuples' memory.
	streamOf := func(tuples []Tuple) Relation {
		return NewStreamingRelationWithOptions(symbols, &sliceIterator{tuples: tuples, pos: -1}, ExecutorOptions{})
	}

	t.Run("a tuple the set holds is not copied", func(t *testing.T) {
		held := pairs(8)
		allocations := func(repeat int) float64 {
			set := newDerivedSet(symbols)
			if _, err := set.absorb(streamOf(held)); err != nil {
				t.Fatal(err)
			}
			var again []Tuple
			for i := 0; i < repeat; i++ {
				again = append(again, held...)
			}
			return testing.AllocsPerRun(1, func() {
				if _, err := set.absorb(streamOf(again)); err != nil {
					t.Fatal(err)
				}
			})
		}
		once, many := allocations(1), allocations(1000)
		if many > once {
			t.Errorf("absorbing 8000 held tuples allocates %v times and 8 held tuples %v times: a tuple the set holds is copied before it is rejected", many, once)
		}
	})

	t.Run("a tuple a stable source yields is held, not copied", func(t *testing.T) {
		held := pairs(8)
		set := newDerivedSet(symbols)
		added, err := set.absorb(NewMaterializedRelation(symbols, held))
		if err != nil {
			t.Fatal(err)
		}
		if len(added) != len(held) {
			t.Fatalf("absorbed %d tuples, want %d", len(added), len(held))
		}
		for i := range held {
			if &added[i][0] != &held[i][0] {
				t.Errorf("tuple %d was copied on absorbing: a materialized source's tuples are stable and are held as they are", i)
			}
		}
	})
}

func TestExecuteWithRelations_UnknownRuleAndArity(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(3))
	rs := mustRules(t, followsRulesText)
	exec := NewExecutor(matcher, nil)

	_, err := exec.ExecuteWithRelations(NewContext().WithRules(rs),
		mustParse(t, `[:find ?x ?y :in $ % :where (precedes ?x ?y)]`), nil)
	var unknown *query.UnknownRuleError
	if !errors.As(err, &unknown) || unknown.Name != datalog.NewSymbol("precedes") {
		t.Errorf("want *query.UnknownRuleError naming precedes, got %v", err)
	}

	_, err = exec.ExecuteWithRelations(NewContext().WithRules(rs),
		mustParse(t, `[:find ?x :in $ % :where (follows ?x)]`), nil)
	var arity *query.RuleArityError
	if !errors.As(err, &arity) || arity.Expected != 2 || arity.Got != 1 {
		t.Errorf("want *query.RuleArityError expecting 2 and getting 1, got %v", err)
	}

	_, err = exec.ExecuteWithRelations(NewContext(),
		mustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`), nil)
	if err == nil {
		t.Error("an invocation with no rule set on the context must be an error")
	}
}

func TestEvaluateRules_IterationLimit(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(6))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
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
	matcher := NewMemoryPatternMatcher(chainDatoms(6))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
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
	matcher := NewMemoryPatternMatcher(chainsDatoms(100))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?y :in $ % ?x :where (follows ?x ?y)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.MaxDerivedTuples = 100
			exec := NewExecutorWithOptions(matcher, nil, opts)
			inputs, err := createInputRelationsFromValuesWithOptions(q,
				[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), page(0)}, exec.options)
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
	matcher := NewMemoryPatternMatcher(chainDatoms(4))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?x ?w
	                    :in $ %
	                    :where [?x :page/kind _]
	                           [(q [:find ?w :in $ % ?x :where (follows ?x ?w)] $ % ?x) [[?w] ...]]]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			if err != nil {
				t.Fatalf("subquery invocation: %v", err)
			}
			requireSamePairs(t, identityPairs(t, result), chainClosure(4))
		})
	}
}

func TestCreateInputRelationsFromValues_RulesInputConsumesNoRelation(t *testing.T) {
	q := mustParse(t, `[:find ?y :in $ % ?x :where (follows ?x ?y)]`)
	relations, err := createInputRelationsFromValuesWithOptions(q,
		[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), page(1)}, ExecutorOptions{})
	if err != nil {
		t.Fatalf("binding: %v", err)
	}
	if len(relations) != 1 || relations[0].Symbols()[0] != datalog.NewSymbol("?x") {
		t.Errorf("relations = %v, want one relation over ?x", relations)
	}
	_, err = createInputRelationsFromValuesWithOptions(q,
		[]interface{}{datalog.SymDollar, page(1), page(2)}, ExecutorOptions{})
	if err == nil {
		t.Error("a value other than % at the rules position must be an error")
	}
}

// A rule body written with its recursive invocation before the pattern that
// binds the invocation's argument is rewritten in the planner's order, where
// the pattern seeks on the required run first and the invocation is bound by
// what it provides, so a bound invocation derives what its binding reaches
// and never the predicate whole (REWRITE_ORDER_PROOF.md T4, NESTED §5.5).
func TestDeriveInvocation_ABodyWrittenBeforeItsBinderDerivesWhatTheBindingReaches(t *testing.T) {
	rs := mustRules(t, `[[(follows ?a ?b) [?a :page/next ?b]]
	                     [(follows ?a ?c) (follows ?b ?c) [?a :page/next ?b]]]`)
	q := mustParse(t, `[:find ?y :in $ % ?x :where (follows ?x ?y)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			opts.MaxDerivedTuples = 100
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(chainsDatoms(1000)), nil, opts)
			inputs, err := createInputRelationsFromValuesWithOptions(q, []interface{}{datalog.SymDollar, datalog.NewSymbol("%"), page(0)}, exec.options)
			require.NoError(t, err)
			tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs))
			require.NoError(t, err, "the bound invocation derives the chain its start reaches under the cap")
			require.Len(t, tuples, 10, "the ten pages page0 reaches")
		})
	}
}

func TestEvaluateRules_DerivesOnlyTheNeededStrata(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(4))
	rs := mustRules(t, `[[(follows ?a ?b) [?a :page/next ?b]]
	                     [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]
	                     [(marked ?p) [?p :page/kind "marked"]]]`)
	exec := NewExecutor(matcher, nil)
	planned, err := planner.PlanEvaluation(exec.planner, rs, datalog.NewSymbol("follows"), []bool{false, false}, nil)
	if err != nil {
		t.Fatalf("PlanEvaluation: %v", err)
	}
	ev := newRuleEvaluation(exec, planned)
	if _, err := ev.extend(NewContext().WithRules(rs), nil); err != nil {
		t.Fatalf("extend: %v", err)
	}
	ev.complete()
	rel, ok := ev.derived[datalog.NewSymbol("follows")]
	if !ok {
		t.Fatal("the invoked predicate is derived")
	}
	requireSamePairs(t, identityPairs(t, rel), chainClosure(4))
	if _, ok := ev.derived[datalog.NewSymbol("marked")]; ok {
		t.Error("a predicate nothing depends on is not derived")
	}
	if ev.total != len(chainClosure(4)) {
		t.Errorf("the evaluation counted %d tuples, want %d: the needed stratum's alone", ev.total, len(chainClosure(4)))
	}
}

// BindQueryInputs takes one relation per :in entry that binds, and the
// rules input binds none, so a scalar written after % reads the first
// relation given.
func TestBindQueryInputs_SkipsRulesInput(t *testing.T) {
	x := datalog.NewSymbol("?x")
	q := &query.Query{In: []query.InputSpec{
		query.DatabaseInput{Name: datalog.SymDollar},
		query.RulesInput{},
		query.ScalarInput{Symbol: x},
	}}
	bound, err := BindQueryInputs(q, []Relation{
		NewMaterializedRelation([]query.Symbol{x}, []Tuple{{page(1)}}),
	})
	if err != nil {
		t.Fatalf("the rules input consumes no relation, got error %v", err)
	}
	if len(bound.Symbols()) != 1 || bound.Symbols()[0] != x || bound.Size() != 1 || bound.Get(0)[0] != page(1) {
		t.Errorf("bound group = %v, want ?x bound to page1: the rules input consumes no relation", bound)
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
		{"closure", followsRulesText, "follows", 2},
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
	matcher := NewMemoryPatternMatcher(chainDatoms(6))
	for _, f := range fixtures {
		rs := mustRules(t, f.rules)
		vars := make([]string, f.arity)
		head := ""
		for i := range vars {
			vars[i] = fmt.Sprintf("?v%d", i)
			if i > 0 {
				head += " "
			}
			head += vars[i]
		}
		whole := mustParse(t, fmt.Sprintf(`[:find %s :in $ %% :where (%s %s)]`, head, f.predicate, head))
		for _, mode := range optimizerModes {
			t.Run(f.name+"/"+mode.name, func(t *testing.T) {
				exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
				all, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), whole, nil))
				if err != nil {
					t.Fatalf("whole invocation: %v", err)
				}
				if len(all) == 0 {
					t.Fatal("the fixture derives nothing, so the contract is untestable")
				}
				for i := 0; i < f.arity; i++ {
					bound := mustParse(t, fmt.Sprintf(`[:find %s :in $ %% %s :where (%s %s)]`, head, vars[i], f.predicate, head))
					values := NewTupleSet([]int{i}, []int{i}, 0)
					for _, tuple := range all {
						if values.Probe(tuple) {
							continue
						}
						values.Add(tuple)
						v := tuple[i]
						positions := allPositions(len(tuple))
						want := NewTupleSet(positions, positions, 0)
						wantCount := 0
						for _, w := range all {
							if datalog.ValuesEqual(w[i], v) {
								want.Add(w)
								wantCount++
							}
						}
						inputs, err := createInputRelationsFromValuesWithOptions(bound,
							[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), v}, exec.options)
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

// markedPageGraph links page1 to page2 and page2 to page3 and page7 by
// :page/a, page3 to page9 and page7 to page8 by :page/b, and marks page7 by
// :page/c, so a predicate s over the mark holds of page7 alone: a path
// through page7 that a negation of s guards is cut, and one a positive s
// guards is the only one kept.
func markedPageGraph() []datalog.Datom {
	pageA, pageB, pageC := datalog.NewKeyword(":page/a"), datalog.NewKeyword(":page/b"), datalog.NewKeyword(":page/c")
	stamp := func(i int) datalog.ElementID { return datalog.ElementID{Lamport: uint64(i), ReplicaID: 1} }
	return []datalog.Datom{
		{E: page(1), A: pageA, V: page(2), Tx: stamp(1)},
		{E: page(2), A: pageA, V: page(3), Tx: stamp(2)},
		{E: page(2), A: pageA, V: page(7), Tx: stamp(3)},
		{E: page(3), A: pageB, V: page(9), Tx: stamp(4)},
		{E: page(7), A: pageB, V: page(8), Tx: stamp(5)},
		{E: page(7), A: pageC, V: "marked", Tx: stamp(6)},
	}
}

// tuplesBoundTo runs a query whose one scalar input binds an invocation's
// argument to start, and returns its tuples.
func tuplesBoundTo(t *testing.T, exec *Executor, rs *query.RuleSet, q *query.Query, start datalog.Identity) []Tuple {
	t.Helper()
	inputs, err := createInputRelationsFromValuesWithOptions(q,
		[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), start}, exec.options)
	if err != nil {
		t.Fatalf("binding: %v", err)
	}
	rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs)
	if err != nil {
		t.Fatalf("bound to %s: %v", start, err)
	}
	var tuples []Tuple
	if err := collectTuplesInto(&tuples, rel); err != nil {
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
		if !have.Probe(tuple) {
			have.Add(tuple)
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

// TestExecuteWithRelations_NegationAfterARecursiveInvocationRewrites binds an
// invocation of q whose recursive rule negates s after the recursive
// invocation of q. The binding that invocation provides does not reach the
// negation, so the magic rule of s reads no adorned q, the rewritten set
// stratifies, and each query records one rewrite of q under bf and derives
// what the whole predicate filtered to its start holds.
func TestExecuteWithRelations_NegationAfterARecursiveInvocationRewrites(t *testing.T) {
	matcher := NewMemoryPatternMatcher(markedPageGraph())
	rs := mustRules(t, `[[(q ?x ?z) [?x :page/a ?z]]
	                     [(q ?x ?z) [?x :page/a ?y] (q ?y ?w) (not (s ?w)) [?w :page/b ?z]]
	                     [(s ?w) [?w :page/c _]]]`)
	q := mustParse(t, `[:find ?z :in $ % ?x :where (q ?x ?z)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var rewrites []annotations.Event
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) {
				if e.Name == annotations.RulesMagicSets {
					rewrites = append(rewrites, e)
				}
			}
			exec := NewExecutorWithOptions(matcher, nil, opts)
			// q(1, 9) holds through q(2, 3), since s(3) does not hold and
			// b(3, 9); q(1, 8) does not, since s(7) holds.
			requireSamePages(t, "q bound to page1", tuplesBoundTo(t, exec, rs, q, page(1)), page(2), page(9))
			requireSamePages(t, "q bound to page2", tuplesBoundTo(t, exec, rs, q, page(2)), page(3), page(7))
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
	matcher := NewMemoryPatternMatcher(markedPageGraph())
	rs := mustRules(t, `[[(q ?x ?z) [?x :page/a ?z]]
	                     [(q ?x ?z) [?x :page/a ?y] (q ?y ?w) (s ?w) [?w :page/b ?z]]
	                     [(q ?x ?z) [?x :page/a ?w] (not (s ?w)) [?w :page/b ?z]]
	                     [(s ?w) [?w :page/c _]]]`)
	q := mustParse(t, `[:find ?z :in $ % ?x :where (q ?x ?z)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var rewrites []annotations.Event
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) {
				if e.Name == annotations.RulesMagicSets {
					rewrites = append(rewrites, e)
				}
			}
			exec := NewExecutorWithOptions(matcher, nil, opts)
			// q(1, 8) holds through q(2, 7), since s(7) holds and b(7, 8);
			// q(2, 9) holds by the third rule, since s(3) does not hold and
			// b(3, 9).
			requireSamePages(t, "q bound to page1", tuplesBoundTo(t, exec, rs, q, page(1)), page(2), page(8))
			requireSamePages(t, "q bound to page2", tuplesBoundTo(t, exec, rs, q, page(2)), page(3), page(7), page(9))
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
	matcher := NewMemoryPatternMatcher(markedPageGraph())
	rs := mustRules(t, `[[(path [?x] ?z) [?x :page/a ?z]]
	                     [(path [?x] ?z) [?x :page/a ?y] (path ?y ?w) (s ?w) [?w :page/b ?z]]
	                     [(path [?x] ?z) [?x :page/a ?w] (not (s ?w)) [?w :page/b ?z]]
	                     [(s ?w) [?w :page/c _]]]`)
	q := mustParse(t, `[:find ?z :in $ % ?x :where (path ?x ?z)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			requireSamePages(t, "path bound to page1", tuplesBoundTo(t, exec, rs, q, page(1)), page(2), page(8))
			requireSamePages(t, "path bound to page2", tuplesBoundTo(t, exec, rs, q, page(2)), page(3), page(7), page(9))
		})
	}
}

// An invocation under a form evaluates its predicate once per query: the
// rewrite and the strata an invocation under not runs number what the
// top-level invocation's do, with the sink of a chain of ten bound.
func TestExecuteWithRelations_InvocationUnderAFormEvaluatesOnce(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(10))
	rs := mustRules(t, followsRulesText)
	top := mustParse(t, `[:find ?x :in $ % ?s :where [?x :page/kind "leaf"] (follows ?x ?s)]`)
	under := mustParse(t, `[:find ?x :in $ % ?s :where [?x :page/kind "leaf"] (not (follows ?x ?s))]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var rewrites, strata int
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) {
				switch e.Name {
				case annotations.RulesMagicSets:
					rewrites++
				case annotations.RulesStratumBegin:
					strata++
				}
			}
			exec := NewExecutorWithOptions(matcher, nil, opts)
			run := func(q *query.Query) int {
				inputs, err := createInputRelationsFromValuesWithOptions(q,
					[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), page(10)}, exec.options)
				if err != nil {
					t.Fatalf("binding: %v", err)
				}
				rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs)
				if err != nil {
					t.Fatal(err)
				}
				var tuples []Tuple
				if err := collectTuplesInto(&tuples, rel); err != nil {
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
	matcher := NewMemoryPatternMatcher(chainDatoms(10))
	rs := mustRules(t, followsRulesText)
	q := mustParse(t, `[:find ?x ?z :in $ % ?x ?z
	                     :where [?x :page/kind "leaf"] [?z :page/kind "leaf"]
	                            (not (follows ?x ?w) (follows ?z ?w))]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			inputs, err := createInputRelationsFromValuesWithOptions(q,
				[]interface{}{datalog.SymDollar, datalog.NewSymbol("%"), page(5), page(2)}, exec.options)
			if err != nil {
				t.Fatalf("binding: %v", err)
			}
			rel, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, inputs)
			if err != nil {
				t.Fatal(err)
			}
			var tuples []Tuple
			if err := collectTuplesInto(&tuples, rel); err != nil {
				t.Fatal(err)
			}
			if len(tuples) != 0 {
				t.Errorf("page 5 and page 2 both reach page 6, so the not excludes the pair; got %v", tuples)
			}
		})
	}
}

const successorRulesText = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`

// TestExecuteWithRelations_RequiredBindingSchedulesAfterItsProvider writes
// the invocation of a predicate requiring its first position bound before
// the expression that binds it; the planner orders the invocation after its
// provider, since the required position is the invocation's correlate.
func TestExecuteWithRelations_RequiredBindingSchedulesAfterItsProvider(t *testing.T) {
	rs := mustRules(t, successorRulesText)
	q := mustParse(t, `[:find ?m :in $ % :where (succ ?n ?m) [(ground 5) ?n]]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(nil), nil, mode.plannerOptions())
			tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil))
			if err != nil {
				t.Fatalf("the invocation runs after its provider: %v", err)
			}
			if len(tuples) != 1 || tuples[0][0] != int64(6) {
				t.Errorf("got %v, want the successor of 5", tuples)
			}
		})
	}
}

// TestExecuteWithRelations_RequiredBindingNothingBinds: nothing in the
// query binds the required position, and the error names the predicate and
// the variable.
func TestExecuteWithRelations_RequiredBindingNothingBinds(t *testing.T) {
	rs := mustRules(t, successorRulesText)
	q := mustParse(t, `[:find ?m :in $ % :where (succ ?n ?m)]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(nil), nil, mode.plannerOptions())
			_, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			if err == nil {
				t.Fatal("expected an error naming succ and ?n")
			}
			if !strings.Contains(err.Error(), "succ") || !strings.Contains(err.Error(), "?n") {
				t.Errorf("error %q must name the predicate and the required variable", err)
			}
		})
	}
}

// TestExecuteWithRelations_RequiredBindingInSubqueryBodyIsOrderedAfterItsBinder:
// a subquery body is ordered like any scope's, so an invocation of a
// predicate requiring its first position bound, written before the
// expression binding it, runs after its binder and answers the successor.
func TestExecuteWithRelations_RequiredBindingInSubqueryBodyIsOrderedAfterItsBinder(t *testing.T) {
	rs := mustRules(t, successorRulesText)
	q := mustParse(t, `[:find ?m
	                    :in $ %
	                    :where [(q [:find ?m
	                                :in $ %
	                                :where (succ ?n ?m)
	                                       [(ground 5) ?n]]
	                               $ %) [[?m] ...]]]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(nil), nil, mode.plannerOptions())
			tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil))
			if err != nil {
				t.Fatalf("the body's invocation runs after its binder: %v", err)
			}
			if len(tuples) != 1 || tuples[0][0] != int64(6) {
				t.Errorf("got %v, want the successor of 5", tuples)
			}
		})
	}
}

// TestEvaluateRules_SeedAbsorbedAtIterationZero seeds a rewritten closure's
// magic predicate directly: the seed's tuples are the magic stratum's
// iteration zero, and the adorned closure holds the pairs the seed reaches.
func TestEvaluateRules_SeedAbsorbedAtIterationZero(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(6))
	rs := mustRules(t, followsRulesText)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var iterations []annotations.Event
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) {
				if e.Name == annotations.RulesIteration {
					iterations = append(iterations, e)
				}
			}
			exec := NewExecutorWithOptions(matcher, nil, opts)
			planned, err := planner.PlanEvaluation(exec.planner, rs, datalog.NewSymbol("follows"), []bool{true, false}, nil)
			if err != nil {
				t.Fatalf("PlanEvaluation: %v", err)
			}
			magic := planned.Magic
			seedHead, _ := magic.Rules.Head(magic.Seed)
			seed := NewMaterializedRelation(seedHead, []Tuple{{page(3)}})
			ev := newRuleEvaluation(exec, planned)
			if _, err := ev.extend(NewContext().WithRules(rs), seed); err != nil {
				t.Fatalf("extend: %v", err)
			}
			ev.complete()
			want := map[[2]datalog.Identity]bool{}
			for i := 3; i <= 6; i++ {
				for j := i + 1; j <= 6; j++ {
					want[[2]datalog.Identity{page(i), page(j)}] = true
				}
			}
			requireSamePairs(t, identityPairs(t, ev.derived[magic.Predicate]), want)
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
	matcher := NewMemoryPatternMatcher([]datalog.Datom{
		{E: page(1), A: pageTags, V: []interface{}{int64(1), int64(2)}, Tx: tx},
		{E: page(2), A: pageTags, V: []interface{}{int64(1), int64(2), int64(3)}, Tx: tx},
		{E: page(3), A: pageTags, V: []interface{}{int64(2), int64(1)}, Tx: tx},
	})
	rs := mustRules(t, `[[(tagged ?p ?tags) [?p :page/tags ?tags]]]`)
	q := mustParse(t, `[:find ?p :in $ % :where (tagged ?p [1 2])]`)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var rewrites []annotations.Event
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) {
				if e.Name == annotations.RulesMagicSets {
					rewrites = append(rewrites, e)
				}
			}
			exec := NewExecutorWithOptions(matcher, nil, opts)
			tuples, err := CollectTuples(exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil))
			if err != nil {
				t.Fatalf("invocation with a vector argument: %v", err)
			}
			if len(tuples) != 1 || len(tuples[0]) != 1 || tuples[0][0] != page(1) {
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
	matcher := NewMemoryPatternMatcher(chainDatoms(4))
	closure := mustRules(t, followsRulesText)
	successor := mustRules(t, successorRulesText)
	rewritePlanner := planner.CreatePlanner(nil, defaultPlannerOptions())
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
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			var plans []labelledPlan
			for _, c := range queries {
				bindings := query.Bindings{Symbols: map[query.Symbol]bool{}, Rules: c.rules}
				for _, b := range c.bound {
					bindings.Symbols[datalog.NewSymbol(b)] = true
				}
				plan, err := exec.planner.PlanQuery(mustParse(t, c.text), bindings, nil)
				if err != nil {
					t.Fatalf("%s: planning: %v", c.name, err)
				}
				plans = append(plans, labelledPlan{c.name, plan})
			}
			for _, s := range sets {
				for _, stratum := range s.rules.Strata() {
					planned, err := planner.PlanStratum(exec.planner, s.rules, stratum, nil)
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

// pairsBySymbol reads a relation's ?x and ?y attributes into a set of pairs,
// each looked up by its symbol in the relation in hand.
func pairsBySymbol(t *testing.T, rel Relation) map[[2]datalog.Identity]bool {
	t.Helper()
	positions, err := query.SymbolIndexTable(rel.Symbols(), []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")})
	if err != nil {
		t.Fatalf("the result's symbols: %v", err)
	}
	pairs := map[[2]datalog.Identity]bool{}
	err = ForEach(rel, func(tuple Tuple) error {
		pairs[[2]datalog.Identity{tuple[positions[0]].(datalog.Identity), tuple[positions[1]].(datalog.Identity)}] = true
		return nil
	})
	if err != nil {
		t.Fatalf("reading pairs: %v", err)
	}
	return pairs
}

// An invocation finds each head symbol in its source by symbol
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7): a relation held under
// a predicate's name with its symbols in another order than the head's binds
// each argument to the attribute the head pairs it with, whatever the
// invocation's shape, and the required run is read by its own symbols.
func TestInvocationBindsEachArgumentByTheHeadsSymbolWhateverTheSourcesLayout(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	follows := datalog.NewSymbol("follows")
	rs := mustRules(t, followsRulesText)
	// The relations are laid out [?b ?a], the reverse of the head (follows ?a ?b):
	// follows holds page1→page2, page2→page3 and page3→page3, and the delta
	// holds page2→page3.
	held := NewMaterializedRelation([]query.Symbol{b, a}, []Tuple{{page(2), page(1)}, {page(3), page(2)}, {page(3), page(3)}})
	delta := NewMaterializedRelation([]query.Symbol{b, a}, []Tuple{{page(3), page(2)}})
	ctx := NewContext().WithRules(rs).WithDerived(map[query.Symbol]Relation{follows: held, query.DeltaSymbol(follows): delta})
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})
	variable := func(s query.Symbol) query.PatternElement { return query.Variable{Name: s} }
	invoke := func(name query.Symbol, args ...query.PatternElement) *query.RuleInvocation {
		return &query.RuleInvocation{Name: name, Args: args}
	}
	every := map[[2]datalog.Identity]bool{{page(1), page(2)}: true, {page(2), page(3)}: true, {page(3), page(3)}: true}

	t.Run("distinct free variables over a materialized source", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(follows, variable(x), variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		requireSamePairs(t, pairsBySymbol(t, rel), every)
	})
	t.Run("distinct free variables over a streaming source", func(t *testing.T) {
		streaming := NewContext().WithRules(rs).WithDerived(map[query.Symbol]Relation{follows: NewStreamingRelation([]query.Symbol{b, a}, held.Iterator())})
		rel, err := exec.executeRuleInvocation(streaming, invoke(follows, variable(x), variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		requireSamePairs(t, pairsBySymbol(t, rel), every)
	})
	t.Run("a constant fixes the head's position", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(follows, variable(x), query.Constant{Value: page(3)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		tuples, err := CollectTuples(rel, nil)
		if err != nil {
			t.Fatal(err)
		}
		got := map[datalog.Identity]bool{}
		for _, tuple := range tuples {
			got[tuple[0].(datalog.Identity)] = true
		}
		if len(got) != 2 || !got[page(2)] || !got[page(3)] {
			t.Errorf("got %v, want page2 and page3, the ?a values whose ?b is page3", tuples)
		}
	})
	t.Run("a repeated variable equates the head's positions", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(follows, variable(x), variable(x)), nil)
		if err != nil {
			t.Fatal(err)
		}
		tuples, err := CollectTuples(rel, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(tuples) != 1 || tuples[0][0] != page(3) {
			t.Errorf("got %v, want page3 alone, the one pair whose ?a and ?b agree", tuples)
		}
	})
	t.Run("an all-constant invocation is the verdict on the head's positions", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(follows, query.Constant{Value: page(1)}, query.Constant{Value: page(2)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if rel.Size() != 1 {
			t.Errorf("page1→page2 is held, so the verdict is the unit relation, got %d tuples", rel.Size())
		}
		rel, err = exec.executeRuleInvocation(ctx, invoke(follows, query.Constant{Value: page(2)}, query.Constant{Value: page(1)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if rel.Size() != 0 {
			t.Errorf("page2→page1 is not held, so the verdict is empty, got %d tuples", rel.Size())
		}
	})
	t.Run("the delta symbol is read by the predicate's head", func(t *testing.T) {
		rel, err := exec.executeRuleInvocation(ctx, invoke(query.DeltaSymbol(follows), variable(x), variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		requireSamePairs(t, pairsBySymbol(t, rel), map[[2]datalog.Identity]bool{{page(2), page(3)}: true})
	})
	t.Run("the required run is read by its own symbols", func(t *testing.T) {
		run := NewMaterializedRelation([]query.Symbol{y, x}, []Tuple{{page(2), page(1)}})
		scoped := NewContext().WithRules(rs).WithDerived(map[query.Symbol]Relation{query.ScopeRequired: run})
		rel, err := exec.executeRuleInvocation(scoped, invoke(query.ScopeRequired, variable(x), variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		requireSamePairs(t, pairsBySymbol(t, rel), map[[2]datalog.Identity]bool{{page(1), page(2)}: true})
	})
}

// A source lacking a head symbol is the invocation's error naming the
// symbol, never a misread (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md
// §7), for a predicate's relation and for the required run alike.
func TestInvocationErrorsOnASourceLackingAHeadSymbol(t *testing.T) {
	p, q := datalog.NewSymbol("?p"), datalog.NewSymbol("?q")
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	follows := datalog.NewSymbol("follows")
	rs := mustRules(t, followsRulesText)
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})
	args := []query.PatternElement{query.Variable{Name: x}, query.Variable{Name: y}}

	t.Run("a predicate's relation", func(t *testing.T) {
		held := NewMaterializedRelation([]query.Symbol{p, q}, []Tuple{{page(1), page(2)}})
		ctx := NewContext().WithRules(rs).WithDerived(map[query.Symbol]Relation{follows: held})
		_, err := exec.executeRuleInvocation(ctx, &query.RuleInvocation{Name: follows, Args: args}, nil)
		if err == nil {
			t.Fatal("a relation over [?p ?q] held under follows, whose head is (follows ?a ?b), lacks ?a: expected an error naming it")
		}
		if !strings.Contains(err.Error(), "?a") {
			t.Errorf("error %q does not name the missing head symbol ?a", err)
		}
	})
	t.Run("the required run", func(t *testing.T) {
		run := NewMaterializedRelation([]query.Symbol{x}, []Tuple{{page(1)}})
		ctx := NewContext().WithRules(rs).WithDerived(map[query.Symbol]Relation{query.ScopeRequired: run})
		_, err := exec.executeRuleInvocation(ctx, &query.RuleInvocation{Name: query.ScopeRequired, Args: args}, nil)
		if err == nil {
			t.Fatal("a run over [?x] under (scope/required ?x ?y) lacks ?y: expected an error naming it")
		}
		if !strings.Contains(err.Error(), "?y") {
			t.Errorf("error %q does not name the missing required symbol ?y", err)
		}
	})
}

// The derived set of a fixpoint absorbs a body's result by symbol
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7): a result laid out in
// another order than the predicate's canonical symbols is held under the
// right attributes, one fact under two layouts is one tuple, and a result
// lacking a canonical symbol is the set's error naming it.
func TestDerivedSetAbsorbsABodyResultLaidOutInAnotherOrder(t *testing.T) {
	a, b, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")
	set := newDerivedSet([]query.Symbol{a, b})

	added, err := set.absorb(NewMaterializedRelation([]query.Symbol{b, a}, []Tuple{{page(2), page(1)}}))
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 1 {
		t.Fatalf("added %v, want the one pair", added)
	}
	rel := set.relation(ExecutorOptions{})
	positions, err := query.SymbolIndexTable(rel.Symbols(), []query.Symbol{a, b})
	if err != nil {
		t.Fatal(err)
	}
	tuples, err := CollectTuples(rel, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tuples) != 1 || tuples[0][positions[0]] != page(1) || tuples[0][positions[1]] != page(2) {
		t.Errorf("the set holds %v over %v, want ?a page1 and ?b page2", tuples, rel.Symbols())
	}

	added, err = set.absorb(NewMaterializedRelation([]query.Symbol{a, b}, []Tuple{{page(1), page(2)}}))
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 0 {
		t.Errorf("the same fact under the canonical layout is held already, got %v added", added)
	}

	_, err = set.absorb(NewMaterializedRelation([]query.Symbol{a, c}, []Tuple{{page(1), page(3)}}))
	if err == nil {
		t.Fatal("a result over [?a ?c] lacks the set's ?b: expected an error naming it")
	}
	if !strings.Contains(err.Error(), "?b") {
		t.Errorf("error %q does not name the missing symbol ?b", err)
	}
}

package executor

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
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
	exec := newQueryExecutor(nil, nil, ExecutorOptions{})
	invoke := func(args ...query.PatternElement) *query.RuleInvocation {
		return &query.RuleInvocation{Name: follows, Args: args}
	}
	variable := func(s query.Symbol) query.PatternElement { return query.Variable{Name: s} }

	t.Run("renames the positions to the arguments", func(t *testing.T) {
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(variable(x), variable(y)), nil)
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
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(variable(x), query.Constant{Value: page(3)}), nil)
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
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(variable(x), variable(x)), nil)
		if err != nil {
			t.Fatal(err)
		}
		tuples, _ := CollectTuples(rel, nil)
		if len(tuples) != 1 || tuples[0][0] != page(3) {
			t.Errorf("got %v, want page3 alone", tuples)
		}
	})
	t.Run("a blank is projected away", func(t *testing.T) {
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(query.Blank{}, variable(y)), nil)
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
		rel, _, err := exec.executeRuleInvocation(ctx.WithEnvironment(env), invoke(variable(x), variable(y)), nil)
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
		rel, _, err := exec.executeRuleInvocation(ctx, inv, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rel.Size() != 1 {
			t.Errorf("size = %d, want the delta's one tuple", rel.Size())
		}
	})
	t.Run("an all-constant invocation is a verdict", func(t *testing.T) {
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(query.Constant{Value: page(1)}, query.Constant{Value: page(2)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rel.Symbols()) != 0 || rel.Size() != 1 {
			t.Errorf("a held fact is the zero-symbol unit, got %v with %d tuples", rel.Symbols(), rel.Size())
		}
		rel, _, err = exec.executeRuleInvocation(ctx, invoke(query.Constant{Value: page(2)}, query.Constant{Value: page(1)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(rel.Symbols()) != 0 || rel.Size() != 0 {
			t.Errorf("an absent fact is the zero-symbol empty relation, got %v with %d tuples", rel.Symbols(), rel.Size())
		}
	})
	t.Run("a predicate no scope derived and no rule set can derive is an error", func(t *testing.T) {
		_, _, err := exec.executeRuleInvocation(NewContext(), invoke(variable(x), variable(y)), nil)
		if err == nil {
			t.Error("expected an error naming the invocation and the missing rule set")
		}
	})
}

func TestExecuteRuleInvocation_ReadsThePredicateInPlace(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	x, y := datalog.NewSymbol("?x"), datalog.NewSymbol("?y")
	follows := datalog.NewSymbol("follows")
	exec := newQueryExecutor(nil, nil, ExecutorOptions{})
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
			if _, _, err := exec.executeRuleInvocation(ctx, inv, nil); err != nil {
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
	var matcher PatternMatcher
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

const successorRulesText = `[[(succ [?n] ?m) [(+ ?n 1) ?m]]]`

// TestExecuteWithRelations_RequiredBindingSchedulesAfterItsProvider writes
// the invocation of a predicate requiring its first position bound before
// the expression that binds it; the planner orders the invocation after its
// provider, since the required position is the invocation's correlate.
func TestExecuteWithRelations_RequiredBindingSchedulesAfterItsProvider(t *testing.T) {
	rs := mustRules(t, successorRulesText)
	q := mustParse(t, `[:find ?m :in $ % :where (succ ?n ?m) [(ground 5) ?n]]`)
	for _, mode := range legsFor(t) {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(nil, nil, mode.plannerOptions())
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
	for _, mode := range legsFor(t) {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(nil, nil, mode.plannerOptions())
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
	for _, mode := range legsFor(t) {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(nil, nil, mode.plannerOptions())
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
	exec := newQueryExecutor(nil, nil, ExecutorOptions{})
	variable := func(s query.Symbol) query.PatternElement { return query.Variable{Name: s} }
	invoke := func(name query.Symbol, args ...query.PatternElement) *query.RuleInvocation {
		return &query.RuleInvocation{Name: name, Args: args}
	}
	every := map[[2]datalog.Identity]bool{{page(1), page(2)}: true, {page(2), page(3)}: true, {page(3), page(3)}: true}

	t.Run("distinct free variables over a materialized source", func(t *testing.T) {
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(follows, variable(x), variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		requireSamePairs(t, pairsBySymbol(t, rel), every)
	})
	t.Run("distinct free variables over a streaming source", func(t *testing.T) {
		streaming := NewContext().WithRules(rs).WithDerived(map[query.Symbol]Relation{follows: NewStreamingRelation([]query.Symbol{b, a}, held.Iterator())})
		rel, _, err := exec.executeRuleInvocation(streaming, invoke(follows, variable(x), variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		requireSamePairs(t, pairsBySymbol(t, rel), every)
	})
	t.Run("a constant fixes the head's position", func(t *testing.T) {
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(follows, variable(x), query.Constant{Value: page(3)}), nil)
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
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(follows, variable(x), variable(x)), nil)
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
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(follows, query.Constant{Value: page(1)}, query.Constant{Value: page(2)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if rel.Size() != 1 {
			t.Errorf("page1→page2 is held, so the verdict is the unit relation, got %d tuples", rel.Size())
		}
		rel, _, err = exec.executeRuleInvocation(ctx, invoke(follows, query.Constant{Value: page(2)}, query.Constant{Value: page(1)}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if rel.Size() != 0 {
			t.Errorf("page2→page1 is not held, so the verdict is empty, got %d tuples", rel.Size())
		}
	})
	t.Run("the delta symbol is read by the predicate's head", func(t *testing.T) {
		rel, _, err := exec.executeRuleInvocation(ctx, invoke(query.DeltaSymbol(follows), variable(x), variable(y)), nil)
		if err != nil {
			t.Fatal(err)
		}
		requireSamePairs(t, pairsBySymbol(t, rel), map[[2]datalog.Identity]bool{{page(2), page(3)}: true})
	})
	t.Run("the required run is read by its own symbols", func(t *testing.T) {
		run := NewMaterializedRelation([]query.Symbol{y, x}, []Tuple{{page(2), page(1)}})
		scoped := NewContext().WithRules(rs).WithDerived(map[query.Symbol]Relation{query.ScopeRequired: run})
		rel, _, err := exec.executeRuleInvocation(scoped, invoke(query.ScopeRequired, variable(x), variable(y)), nil)
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
	exec := newQueryExecutor(nil, nil, ExecutorOptions{})
	args := []query.PatternElement{query.Variable{Name: x}, query.Variable{Name: y}}

	t.Run("a predicate's relation", func(t *testing.T) {
		held := NewMaterializedRelation([]query.Symbol{p, q}, []Tuple{{page(1), page(2)}})
		ctx := NewContext().WithRules(rs).WithDerived(map[query.Symbol]Relation{follows: held})
		_, _, err := exec.executeRuleInvocation(ctx, &query.RuleInvocation{Name: follows, Args: args}, nil)
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
		_, _, err := exec.executeRuleInvocation(ctx, &query.RuleInvocation{Name: query.ScopeRequired, Args: args}, nil)
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

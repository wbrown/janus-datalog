package planner

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// invocation builds a rule invocation whose "?x" arguments are variables and
// whose other arguments are string constants.
func invocation(name string, args ...string) *query.RuleInvocation {
	inv := &query.RuleInvocation{Name: datalog.NewSymbol(name)}
	for _, arg := range args {
		if arg[0] == '?' {
			inv.Args = append(inv.Args, query.Variable{Name: datalog.NewSymbol(arg)})
		} else {
			inv.Args = append(inv.Args, query.Constant{Value: arg})
		}
	}
	return inv
}

func TestScoreClauseRuleInvocation(t *testing.T) {
	none := map[query.Symbol]bool{}
	unbound := scoreClause(invocation("follows", "?x", "?y"), none, query.Bindings{})
	bound := scoreClause(invocation("follows", "?x", "?y"), map[query.Symbol]bool{datalog.NewSymbol("?x"): true}, query.Bindings{})
	if bound <= unbound {
		t.Errorf("an invocation with a bound argument scores %d, not above the unbound %d", bound, unbound)
	}
	constant := scoreClause(invocation("follows", "?x", "end"), none, query.Bindings{})
	if constant <= unbound {
		t.Errorf("an invocation with a constant argument scores %d, not above the unbound %d", constant, unbound)
	}
	vector := scoreClause(&query.RuleInvocation{Name: datalog.NewSymbol("follows"), Args: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?x")},
		query.VectorConstant{Values: []interface{}{int64(1), int64(2)}},
	}}, none, query.Bindings{})
	if vector <= unbound {
		t.Errorf("an invocation with a vector constant argument scores %d, not above the unbound %d: the executor binds that position, so the planner counts it as a constant", vector, unbound)
	}
	pattern := scoreClause(&query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?x")},
		query.Constant{Value: datalog.NewKeyword(":page/next")},
		query.Variable{Name: datalog.NewSymbol("?y")},
	}}, none, query.Bindings{})
	if unbound >= pattern {
		t.Errorf("an unbound invocation scores %d, not below a pattern with a constant attribute at %d", unbound, pattern)
	}

	// An invocation of a predicate the seed restricts, a magic or adorned
	// predicate or the delta of one, reads a relation holding only what the
	// seed reaches: it schedules before every pattern, which then seeks on
	// what it binds, and after a ready predicate, which narrows the relation
	// first. The delta of a user predicate is unrestricted and keeps a
	// pattern's place.
	restricted := scoreClause(invocation("delta.magic.bf/follows", "?x"), none, query.Bindings{})
	if restricted <= pattern {
		t.Errorf("an invocation of a seed-restricted predicate scores %d, not above a pattern with a constant attribute at %d", restricted, pattern)
	}
	ready := scoreClause(&query.Comparison{
		Op:    datalog.SymLT,
		Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
		Right: query.ConstantTerm{Value: int64(10)},
	}, map[query.Symbol]bool{datalog.NewSymbol("?x"): true}, query.Bindings{})
	if restricted >= ready {
		t.Errorf("an invocation of a seed-restricted predicate scores %d, not below a ready predicate at %d", restricted, ready)
	}
	unrestricted := scoreClause(invocation("delta/follows", "?x", "?y"), none, query.Bindings{})
	if unrestricted >= pattern {
		t.Errorf("an invocation of a user predicate's delta scores %d, not below a pattern with a constant attribute at %d", unrestricted, pattern)
	}

	// A scope's required run enters its query as a relation held under
	// scope/required, which holds only the tuples the scope runs over, so its
	// invocation leads the patterns as a seed-restricted predicate's does.
	scoped := scoreClause(invocation("scope/required", "?x"), none, query.Bindings{})
	if scoped <= pattern {
		t.Errorf("an invocation of scope/required scores %d, not above a pattern with a constant attribute at %d", scoped, pattern)
	}
}

func TestPlanCacheKeyDistinguishesRulesInput(t *testing.T) {
	inv := invocation("follows", "?x", "?y")
	find := []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?y")}}
	with := &query.Query{
		Find:  find,
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}},
		Where: []query.Clause{inv},
	}
	without := &query.Query{
		Find:  find,
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
		Where: []query.Clause{inv},
	}
	cache := &PlanCache{}
	if cache.computeKeyWithOptions(with, query.Bindings{}, PlannerOptions{}) == cache.computeKeyWithOptions(without, query.Bindings{}, PlannerOptions{}) {
		t.Error("a query with a rules input shares its plan-cache key with the same query without one")
	}
}

// TestPlanCacheKeyDistinguishesRequiredPositions: the positions a rule set
// requires of an invoked predicate decide the invocation's correlates and so
// its phase, and the clause text does not carry them, so one query text under
// two rule sets requiring different positions of its predicate never shares
// a plan, at the top level or inside a form.
func TestPlanCacheKeyDistinguishesRequiredPositions(t *testing.T) {
	find := []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?m")}}
	in := []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}}
	succ := func(required ...string) *query.RuleSet {
		rule := &query.Rule{
			Name: datalog.NewSymbol("succ"),
			Head: []query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?m")},
			Body: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?n")},
				query.Constant{Value: datalog.NewKeyword(":page/next")},
				query.Variable{Name: datalog.NewSymbol("?m")},
			}}},
		}
		for _, r := range required {
			rule.Required = append(rule.Required, datalog.NewSymbol(r))
		}
		rs, err := query.NewRuleSet([]*query.Rule{rule})
		if err != nil {
			t.Fatalf("NewRuleSet: %v", err)
		}
		return rs
	}
	free, requiring := succ(), succ("?n")
	cache := &PlanCache{}
	top := &query.Query{Find: find, In: in, Where: []query.Clause{invocation("succ", "?n", "?m")}}
	if cache.computeKeyWithOptions(top, query.Bindings{Rules: free}, PlannerOptions{}) == cache.computeKeyWithOptions(top, query.Bindings{Rules: requiring}, PlannerOptions{}) {
		t.Error("a query under a rule set requiring the invoked predicate's first position shares its plan-cache key with the query under one requiring nothing")
	}

	branch := &query.Query{Find: find, In: in, Where: []query.Clause{
		&query.OrJoinClause{OutputVars: []query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?m")}, Branches: [][]query.Clause{{invocation("succ", "?n", "?m")}}},
	}}
	if cache.computeKeyWithOptions(branch, query.Bindings{Rules: free}, PlannerOptions{}) == cache.computeKeyWithOptions(branch, query.Bindings{Rules: requiring}, PlannerOptions{}) {
		t.Error("an invocation inside an or-join branch shares its plan-cache key across rule sets requiring different positions")
	}
}

// TestPlanCacheKeyDistinguishesRuleBodies: a plan is the walk over the bodies
// the query's invocations run, so one query text under two rule sets whose
// predicate has different bodies never shares a plan, at the top level or
// inside a form, and two sets built from one text share one.
func TestPlanCacheKeyDistinguishesRuleBodies(t *testing.T) {
	find := []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?m")}}
	in := []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}}
	next := func(from, to string) *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol(from)},
			query.Constant{Value: datalog.NewKeyword(":page/next")},
			query.Variable{Name: datalog.NewSymbol(to)},
		}}
	}
	succ := func(body ...query.Clause) *query.RuleSet {
		rs, err := query.NewRuleSet([]*query.Rule{{
			Name: datalog.NewSymbol("succ"),
			Head: []query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?m")},
			Body: body,
		}})
		if err != nil {
			t.Fatalf("NewRuleSet: %v", err)
		}
		return rs
	}
	oneHop, twoHops, oneHopAgain := succ(next("?n", "?m")), succ(next("?n", "?k"), next("?k", "?m")), succ(next("?n", "?m"))
	cache := &PlanCache{}
	top := &query.Query{Find: find, In: in, Where: []query.Clause{invocation("succ", "?n", "?m")}}
	branch := &query.Query{Find: find, In: in, Where: []query.Clause{
		&query.OrJoinClause{OutputVars: []query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?m")}, Branches: [][]query.Clause{{invocation("succ", "?n", "?m")}}},
	}}
	for _, q := range []*query.Query{top, branch} {
		if cache.computeKeyWithOptions(q, query.Bindings{Rules: oneHop}, PlannerOptions{}) == cache.computeKeyWithOptions(q, query.Bindings{Rules: twoHops}, PlannerOptions{}) {
			t.Errorf("%s under a rule set whose body is one hop shares its plan-cache key with the query under one whose body is two hops", q)
		}
		if cache.computeKeyWithOptions(q, query.Bindings{Rules: oneHop}, PlannerOptions{}) != cache.computeKeyWithOptions(q, query.Bindings{Rules: oneHopAgain}, PlannerOptions{}) {
			t.Errorf("%s under two rule sets built from one text has two plan-cache keys", q)
		}
	}
}

func TestAnalyzeClausesForExplainRecordsInvocation(t *testing.T) {
	inv := invocation("follows", "?x", "?y")
	phase := RealizedPhase{Query: &query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?y")}},
		Where: []query.Clause{inv},
	}}
	available := map[query.Symbol]bool{datalog.NewSymbol("?x"): true}
	analyzeClausesForExplain(&phase, []query.Clause{inv}, available, nil, nil)
	if len(phase.Invocations) != 1 {
		t.Fatalf("phase records %d invocations, want 1", len(phase.Invocations))
	}
	plan := phase.Invocations[0]
	if plan.Invocation != inv {
		t.Errorf("recorded invocation is %s, want %s", plan.Invocation, inv)
	}
	if len(plan.Bound) != 1 || plan.Bound[0] != datalog.NewSymbol("?x") {
		t.Errorf("Bound = %v, want the argument available before the clause, ?x", plan.Bound)
	}
	if !available[datalog.NewSymbol("?y")] {
		t.Error("an invocation's variable arguments become available to the clauses after it")
	}
	rendered := phase.String()
	if !strings.Contains(rendered, "(follows ?x ?y)") {
		t.Errorf("the phase rendering omits the invocation:\n%s", rendered)
	}
	if !strings.Contains(rendered, "Bound: [?x]") {
		t.Errorf("the phase rendering omits the bound arguments:\n%s", rendered)
	}
}

// What an expression binds is available to the clauses after it under every
// binding form: the variables of a relation-bound enumerate bind the entity
// position of the pattern that follows.
func TestAnalyzeClausesForExplainReadsARelationBoundExpression(t *testing.T) {
	vec, idx, item, c := datalog.NewSymbol("?vec"), datalog.NewSymbol("?idx"), datalog.NewSymbol("?item"), datalog.NewSymbol("?c")
	enumerate := &query.Expression{
		Function: query.EnumerateFunction{VecTerm: query.VariableTerm{Symbol: vec}},
		Binding:  query.RelationBinding{Variables: []query.Symbol{idx, item}},
	}
	color := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: item}, query.Constant{Value: datalog.NewKeyword(":item/color")}, query.Variable{Name: c},
	}}
	phase := RealizedPhase{Query: &query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: c}},
		Where: []query.Clause{enumerate, color},
	}}
	available := map[query.Symbol]bool{vec: true}
	analyzeClausesForExplain(&phase, []query.Clause{enumerate, color}, available, nil, nil)
	if len(phase.Patterns) != 1 {
		t.Fatalf("phase records %d patterns, want 1", len(phase.Patterns))
	}
	if !phase.Patterns[0].BoundMask.E {
		t.Errorf("the pattern's entity ?item is unbound in the plan; the relation-bound enumerate before it binds ?idx and ?item")
	}
	if !available[idx] || !available[item] {
		t.Errorf("available = %v, want ?idx and ?item, the relation binding's variables", available)
	}
}

// Explain's record of an invocation names every argument the clauses before
// it bind, an or and an or-join among the binders: what a form provides is
// available to the clauses after it.
func TestAnalyzeClausesForExplainBoundNamesAnArgumentAnOrBinds(t *testing.T) {
	edge := func(e, v string) *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol(e)},
			query.Constant{Value: datalog.NewKeyword(":edge")},
			query.Variable{Name: datalog.NewSymbol(v)},
		}}
	}
	branches := [][]query.Clause{{edge("?x", "?z")}, {edge("?z", "?x")}}
	forms := []struct {
		name   string
		clause query.Clause
	}{
		{"or", &query.OrClause{Branches: branches}},
		{"or-join", &query.OrJoinClause{OutputVars: []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?z")}, Branches: branches}},
	}
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			inv := invocation("reach", "?x", "?y")
			inferred, err := query.InferHeaders(&query.Query{
				Find:  []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?y")}},
				Where: []query.Clause{form.clause, inv},
			}, nil)
			if err != nil {
				t.Fatalf("InferHeaders: %v", err)
			}
			phase := RealizedPhase{Query: inferred}
			analyzeClausesForExplain(&phase, inferred.Where, map[query.Symbol]bool{}, nil, nil)
			if len(phase.Invocations) != 1 {
				t.Fatalf("phase records %d invocations, want 1", len(phase.Invocations))
			}
			if !query.ContainsSymbol(phase.Invocations[0].Bound, datalog.NewSymbol("?x")) {
				t.Errorf("Bound = %v, want ?x, which the %s before the invocation binds", phase.Invocations[0].Bound, form.name)
			}
		})
	}
}

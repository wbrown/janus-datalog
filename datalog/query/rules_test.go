package query

import (
	"errors"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// A rule set declares predicates, each rule one way to derive a predicate's
// tuples from clauses, and NewRuleSet refuses a set it cannot evaluate: heads
// of one predicate disagreeing on arity, an unsafe rule, an invocation of an
// undefined predicate, a cycle through negation, or a recursive invocation
// where evaluation is not monotone. The strata it computes put every
// predicate after the predicates its bodies invoke.

// invoke builds a rule invocation. A "?x" argument is a variable, "_" a
// blank, and anything else a string constant.
func invoke(name string, args ...string) *RuleInvocation {
	inv := &RuleInvocation{Name: datalog.NewSymbol(name)}
	for _, arg := range args {
		switch {
		case arg == "_":
			inv.Args = append(inv.Args, Blank{})
		case arg[0] == '?':
			inv.Args = append(inv.Args, Variable{Name: datalog.NewSymbol(arg)})
		default:
			inv.Args = append(inv.Args, Constant{Value: arg})
		}
	}
	return inv
}

// defineRule builds a rule from its name, its head variables and its body.
func defineRule(name string, head []string, body ...Clause) *Rule {
	r := &Rule{Name: datalog.NewSymbol(name), Body: body}
	for _, h := range head {
		r.Head = append(r.Head, datalog.NewSymbol(h))
	}
	return r
}

// followsRules is the closure over :page/next: a page follows the page its
// next names, and follows whatever that page follows.
func followsRules() []*Rule {
	return []*Rule{
		defineRule("follows", []string{"?a", "?b"},
			scopePattern("?a", "page/next", "?b")),
		defineRule("follows", []string{"?a", "?c"},
			scopePattern("?a", "page/next", "?b"),
			invoke("follows", "?b", "?c")),
	}
}

func sym(name string) Symbol { return datalog.NewSymbol(name) }

func mustRuleSet(t *testing.T, rules []*Rule) *RuleSet {
	t.Helper()
	rs, err := NewRuleSet(rules)
	if err != nil {
		t.Fatalf("NewRuleSet: %v", err)
	}
	return rs
}

func TestNewRuleSet_GroupsAlternativesInOrder(t *testing.T) {
	rules := append(followsRules(),
		defineRule("page", []string{"?p"}, scopePattern("?p", "page/kind", "_")))
	rs, err := NewRuleSet(rules)
	if err != nil {
		t.Fatalf("NewRuleSet: %v", err)
	}
	preds := rs.Predicates()
	assertSymbols(t, "Predicates", preds, "follows", "page")
	if preds[0] != sym("follows") {
		t.Errorf("first predicate is %s, want follows: predicates keep first-appearance order", preds[0])
	}
	got := rs.Rules(sym("follows"))
	if len(got) != 2 || got[0] != rules[0] || got[1] != rules[1] {
		t.Errorf("Rules(follows) = %v, want the two follows rules in declaration order", got)
	}
	if arity, ok := rs.Arity(sym("follows")); !ok || arity != 2 {
		t.Errorf("Arity(follows) = %d, %v; want 2, true", arity, ok)
	}
	if _, ok := rs.Arity(sym("absent")); ok {
		t.Error("Arity of an undefined predicate must report false")
	}
	if err := rs.CheckInvocation(invoke("follows", "?x", "?y")); err != nil {
		t.Errorf("CheckInvocation of a defined predicate at its arity: %v", err)
	}
}

func TestNewRuleSet_RefusesArityDisagreement(t *testing.T) {
	_, err := NewRuleSet([]*Rule{
		defineRule("follows", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b")),
		defineRule("follows", []string{"?a"}, scopePattern("?a", "page/next", "_")),
	})
	var arity *RuleArityError
	if !errors.As(err, &arity) {
		t.Fatalf("want *RuleArityError, got %v", err)
	}
	if arity.Name != sym("follows") || arity.Expected != 2 || arity.Got != 1 {
		t.Errorf("RuleArityError = %+v, want follows expecting 2, got 1", arity)
	}
}

func TestNewRuleSet_RefusesInvocationArityMismatch(t *testing.T) {
	rs, err := NewRuleSet(followsRules())
	if err != nil {
		t.Fatalf("NewRuleSet: %v", err)
	}
	err = rs.CheckInvocation(invoke("follows", "?x"))
	var arity *RuleArityError
	if !errors.As(err, &arity) {
		t.Fatalf("want *RuleArityError, got %v", err)
	}
	if arity.Name != sym("follows") || arity.Expected != 2 || arity.Got != 1 {
		t.Errorf("RuleArityError = %+v, want follows expecting 2, got 1", arity)
	}
	_, err = NewRuleSet([]*Rule{
		defineRule("follows", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b")),
		defineRule("follows", []string{"?a", "?c"}, scopePattern("?a", "page/next", "?b"), invoke("follows", "?b")),
	})
	if !errors.As(err, &arity) {
		t.Fatalf("a body invocation at the wrong arity: want *RuleArityError, got %v", err)
	}
}

func TestNewRuleSet_RefusesUnsafeHead(t *testing.T) {
	_, err := NewRuleSet([]*Rule{
		defineRule("follows", []string{"?a", "?b"}, scopePattern("?a", "page/next", "_")),
	})
	var unsafe *UnsafeRuleError
	if !errors.As(err, &unsafe) {
		t.Fatalf("want *UnsafeRuleError, got %v", err)
	}
	if unsafe.Rule != sym("follows") || unsafe.Symbol != sym("?b") {
		t.Errorf("UnsafeRuleError = %+v, want rule follows and symbol ?b", unsafe)
	}
}

func TestNewRuleSet_RefusesUnsafePredicateSymbol(t *testing.T) {
	_, err := NewRuleSet([]*Rule{
		defineRule("follows", []string{"?a", "?b"},
			scopePattern("?a", "page/next", "?b"),
			&Comparison{
				Op:    datalog.SymLT,
				Left:  VariableTerm{Symbol: sym("?n")},
				Right: ConstantTerm{Value: int64(10)},
			}),
	})
	var unsafe *UnsafeRuleError
	if !errors.As(err, &unsafe) {
		t.Fatalf("want *UnsafeRuleError, got %v", err)
	}
	if unsafe.Rule != sym("follows") || unsafe.Symbol != sym("?n") {
		t.Errorf("UnsafeRuleError = %+v, want rule follows and symbol ?n", unsafe)
	}
}

func TestNewRuleSet_RefusesUnknownInvocation(t *testing.T) {
	_, err := NewRuleSet([]*Rule{
		defineRule("follows", []string{"?a", "?b"},
			scopePattern("?a", "page/next", "?b"),
			invoke("precedes", "?b", "?a")),
	})
	var unknown *UnknownRuleError
	if !errors.As(err, &unknown) {
		t.Fatalf("want *UnknownRuleError, got %v", err)
	}
	if unknown.Name != sym("precedes") {
		t.Errorf("UnknownRuleError names %s, want precedes", unknown.Name)
	}
	assertSymbols(t, "Defined", unknown.Defined, "follows")
}

func TestNewRuleSet_RefusesNegativeCycle(t *testing.T) {
	_, err := NewRuleSet([]*Rule{
		defineRule("p", []string{"?x"},
			scopePattern("?x", "page/kind", "_"),
			&NotClause{Clauses: []Clause{invoke("q", "?x")}}),
		defineRule("q", []string{"?x"},
			scopePattern("?x", "page/kind", "_"),
			&NotClause{Clauses: []Clause{invoke("p", "?x")}}),
	})
	var cycle *UnstratifiableError
	if !errors.As(err, &cycle) {
		t.Fatalf("want *UnstratifiableError, got %v", err)
	}
	assertSymbols(t, "Predicates", cycle.Predicates, "p", "q")
}

func TestNewRuleSet_OrdersStrata(t *testing.T) {
	// Declared out of dependency order: final needs derived, which needs
	// base and excluded.
	rs, err := NewRuleSet([]*Rule{
		defineRule("final", []string{"?x"},
			invoke("derived", "?x"),
			scopePattern("?x", "page/kind", "?k")),
		defineRule("derived", []string{"?x"},
			invoke("base", "?x"),
			&NotClause{Clauses: []Clause{invoke("excluded", "?x")}}),
		defineRule("base", []string{"?x"}, scopePattern("?x", "page/kind", "_")),
		defineRule("excluded", []string{"?x"}, scopePattern("?x", "page/spoilt", "_")),
	})
	if err != nil {
		t.Fatalf("NewRuleSet: %v", err)
	}
	strata := rs.Strata()
	if len(strata) != 4 {
		t.Fatalf("got %d strata, want 4 (one per predicate): %v", len(strata), strata)
	}
	position := map[Symbol]int{}
	for i, stratum := range strata {
		if len(stratum.Predicates) != 1 {
			t.Errorf("stratum %d holds %v, want one predicate", i, stratum.Predicates)
			continue
		}
		if stratum.Recursive {
			t.Errorf("stratum %d (%v) is marked recursive; no predicate invokes itself", i, stratum.Predicates)
		}
		position[stratum.Predicates[0]] = i
	}
	before := func(earlier, later string) {
		t.Helper()
		if position[sym(earlier)] >= position[sym(later)] {
			t.Errorf("%s (stratum %d) must come before %s (stratum %d)", earlier, position[sym(earlier)], later, position[sym(later)])
		}
	}
	before("base", "derived")
	before("excluded", "derived")
	before("derived", "final")

	closure, err := NewRuleSet(followsRules())
	if err != nil {
		t.Fatalf("NewRuleSet(follows): %v", err)
	}
	strata = closure.Strata()
	if len(strata) != 1 || !strata[0].Recursive {
		t.Fatalf("a closure is one recursive stratum, got %+v", strata)
	}
	assertSymbols(t, "Predicates", strata[0].Predicates, "follows")
}

// A rule body's static shapes are read at the rule set's boundary as a
// query's are at the text boundaries, through the one walk
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §5.1).
func TestNewRuleSet_RefusesABodyShapeTheTextBoundariesRefuse(t *testing.T) {
	_, err := NewRuleSet([]*Rule{
		defineRule("counted", []string{"?a", "?n"},
			scopePattern("?a", "page/next", "?b"),
			&SubqueryPattern{
				Query: &Query{
					Find:  []FindElement{FindAggregate{Function: datalog.SymCount, Arg: sym("?c")}},
					In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: sym("?b")}},
					Where: []Clause{scopePattern("?b", "page/next", "?c")},
				},
				Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Variable{Name: sym("?b")}},
				Binding: RelationBinding{Variables: []Symbol{sym("?n"), sym("?m")}},
			}),
	})
	if err == nil {
		t.Fatal("NewRuleSet accepts the body; its subquery's binding declares two symbols for a find of one")
	}
	for _, part := range []string{"rule counted", "subquery relation binding declares 2 symbol(s), but the inner :find has 1 element(s)"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("the refusal %q does not carry %q", err, part)
		}
	}
}

func TestNewRuleSet_RefusesRecursionUnderFallbackAndSubquery(t *testing.T) {
	cases := []struct {
		form   string
		want   string
		clause Clause
	}{
		{"or-default", "or-default-join", &OrDefaultClause{Branches: [][]Clause{
			{invoke("follows", "?b", "?c")},
			{&Expression{Function: &GroundFunction{Value: "end"}, Binding: sym("?c")}},
		}}},
		{"or-default-join", "or-default-join", &OrDefaultJoinClause{
			RequiredVars: []Symbol{sym("?b")},
			OutputVars:   []Symbol{sym("?c")},
			Branches: [][]Clause{
				{invoke("follows", "?b", "?c")},
				{&Expression{Function: &GroundFunction{Value: "end"}, Binding: sym("?c")}},
			},
		}},
		{"aggregating subquery", "subquery", &SubqueryPattern{
			Query: &Query{
				Find:  []FindElement{FindAggregate{Function: datalog.SymCount, Arg: sym("?w")}},
				In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, RulesInput{}, ScalarInput{Symbol: sym("?b")}},
				Where: []Clause{invoke("follows", "?b", "?w")},
			},
			Inputs: []PatternElement{
				Constant{Value: datalog.SymDollar},
				Constant{Value: datalog.NewSymbol("%")},
				Variable{Name: sym("?b")},
			},
			Binding: RelationBinding{Variables: []Symbol{sym("?c")}},
		}},
		// A fallback enclosing a subquery whose inner query does not
		// aggregate is refused naming the fallback: the admitted form inside
		// does not hide the one outside.
		{"a fallback enclosing a non-aggregating subquery", "or-default-join", &OrDefaultClause{Branches: [][]Clause{
			{&SubqueryPattern{
				Query: &Query{
					Find:  []FindElement{FindVariable{Symbol: sym("?c")}},
					In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, RulesInput{}, ScalarInput{Symbol: sym("?b")}},
					Where: []Clause{invoke("follows", "?b", "?c")},
				},
				Inputs: []PatternElement{
					Constant{Value: datalog.SymDollar},
					Constant{Value: datalog.NewSymbol("%")},
					Variable{Name: sym("?b")},
				},
				Binding: RelationBinding{Variables: []Symbol{sym("?c")}},
			}},
			{&Expression{Function: &GroundFunction{Value: "end"}, Binding: sym("?c")}},
		}}},
		// An aggregating subquery enclosing a fallback is refused naming the
		// innermost form whose evaluation is not monotone, the fallback.
		{"an aggregating subquery enclosing a fallback", "or-default-join", &SubqueryPattern{
			Query: &Query{
				Find: []FindElement{FindAggregate{Function: datalog.SymCount, Arg: sym("?w")}},
				In:   []InputSpec{DatabaseInput{Name: datalog.SymDollar}, RulesInput{}, ScalarInput{Symbol: sym("?b")}},
				Where: []Clause{&OrDefaultClause{Branches: [][]Clause{
					{invoke("follows", "?b", "?w")},
					{&Expression{Function: &GroundFunction{Value: "end"}, Binding: sym("?w")}},
				}}},
			},
			Inputs: []PatternElement{
				Constant{Value: datalog.SymDollar},
				Constant{Value: datalog.NewSymbol("%")},
				Variable{Name: sym("?b")},
			},
			Binding: RelationBinding{Variables: []Symbol{sym("?c")}},
		}},
	}
	for _, c := range cases {
		t.Run(c.form, func(t *testing.T) {
			_, err := NewRuleSet([]*Rule{
				defineRule("follows", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b")),
				defineRule("follows", []string{"?a", "?c"}, scopePattern("?a", "page/next", "?b"), c.clause),
			})
			var nested *NestedRecursionError
			if !errors.As(err, &nested) {
				t.Fatalf("want *NestedRecursionError, got %v", err)
			}
			if nested.Rule != sym("follows") || nested.Predicate != sym("follows") || nested.Form != c.want {
				t.Errorf("NestedRecursionError = %+v, want rule follows, predicate follows, form %s", nested, c.want)
			}
		})
	}

	t.Run("or-join is monotone and accepted", func(t *testing.T) {
		rs, err := NewRuleSet([]*Rule{
			defineRule("follows", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b")),
			defineRule("follows", []string{"?a", "?c"},
				scopePattern("?a", "page/next", "?b"),
				&OrJoinClause{
					OutputVars: []Symbol{sym("?b"), sym("?c")},
					Branches: [][]Clause{
						{invoke("follows", "?b", "?c")},
						{scopePattern("?b", "page/next", "?c")},
					},
				}),
		})
		if err != nil {
			t.Fatalf("a recursive invocation inside an or-join branch distributes over union: %v", err)
		}
		if strata := rs.Strata(); len(strata) != 1 || !strata[0].Recursive {
			t.Errorf("strata = %+v, want one recursive stratum", strata)
		}
	})

	t.Run("a non-aggregating subquery is monotone and accepted", func(t *testing.T) {
		rs, err := NewRuleSet([]*Rule{
			defineRule("follows", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b")),
			defineRule("follows", []string{"?a", "?c"},
				scopePattern("?a", "page/next", "?b"),
				&SubqueryPattern{
					Query: &Query{
						Find:  []FindElement{FindVariable{Symbol: sym("?c")}},
						In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, RulesInput{}, ScalarInput{Symbol: sym("?b")}},
						Where: []Clause{invoke("follows", "?b", "?c")},
					},
					Inputs: []PatternElement{
						Constant{Value: datalog.SymDollar},
						Constant{Value: datalog.NewSymbol("%")},
						Variable{Name: sym("?b")},
					},
					Binding: RelationBinding{Variables: []Symbol{sym("?c")}},
				}),
		})
		if err != nil {
			t.Fatalf("a recursive invocation inside a non-aggregating inner query joins its result with the scope over the combinations, monotone as a branch: %v", err)
		}
		if strata := rs.Strata(); len(strata) != 1 || !strata[0].Recursive {
			t.Errorf("strata = %+v, want one recursive stratum", strata)
		}
	})
}

func TestInvocationsDescendsEveryForm(t *testing.T) {
	inner := invoke("follows", "?b", "?c")
	sub := invoke("follows", "?c", "?d")
	found := Invocations([]Clause{
		scopePattern("?a", "page/next", "?b"),
		&OrJoinClause{OutputVars: []Symbol{sym("?b")}, Branches: [][]Clause{{inner}}},
		&SubqueryPattern{
			Query:   &Query{Where: []Clause{sub}},
			Binding: RelationBinding{Variables: []Symbol{sym("?d")}},
		},
	})
	if len(found) != 2 || found[0] != inner || found[1] != sub {
		t.Errorf("Invocations = %v, want the or-join branch's then the subquery body's", found)
	}
}

// RulesUse finds the first clause using the query's own %: an invocation at
// any depth short of a subquery body, or a subquery call passing % through.
// A subquery call passing no % uses nothing of the outer query's, whatever
// its body does with the inner query's own %.
func TestRulesUse(t *testing.T) {
	inner := invoke("follows", "?b", "?c")
	passing := &SubqueryPattern{
		Query: &Query{
			In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, RulesInput{}},
			Where: []Clause{invoke("follows", "?c", "?d")},
		},
		Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Constant{Value: datalog.SymRules}},
		Binding: RelationBinding{Variables: []Symbol{sym("?d")}},
	}
	own := &SubqueryPattern{
		Query: &Query{
			In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}},
			Where: []Clause{scopePattern("?c", "page/next", "?d")},
		},
		Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}},
		Binding: RelationBinding{Variables: []Symbol{sym("?d")}},
	}
	cases := []struct {
		name    string
		clauses []Clause
		want    Clause
	}{
		{"an invocation in an or-join branch", []Clause{
			scopePattern("?a", "page/next", "?b"),
			&OrJoinClause{OutputVars: []Symbol{sym("?b")}, Branches: [][]Clause{{inner}}},
		}, inner},
		{"a subquery call passing %", []Clause{scopePattern("?a", "page/next", "?b"), passing}, passing},
		{"a subquery call passing no %", []Clause{scopePattern("?a", "page/next", "?b"), own}, nil},
		{"patterns alone", []Clause{scopePattern("?a", "page/next", "?b")}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := RulesUse(c.clauses); got != c.want {
				t.Errorf("RulesUse = %v, want %v", got, c.want)
			}
		})
	}
}

func TestRuleSetDefaultSourceAttributes(t *testing.T) {
	rs, err := NewRuleSet([]*Rule{
		defineRule("follows", []string{"?a", "?b"},
			scopePattern("?a", "page/next", "?b"),
			scopePattern("?b", "page/kind", "_")),
		defineRule("follows", []string{"?a", "?c"},
			scopePattern("?a", "page/next", "?b"),
			invoke("follows", "?b", "?c"),
			&SubqueryPattern{
				Query: &Query{
					Find:  []FindElement{FindVariable{Symbol: sym("?t")}},
					In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: sym("?c")}},
					Where: []Clause{scopePattern("?c", "page/title", "?t")},
				},
				Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Variable{Name: sym("?c")}},
				Binding: ScalarBinding{Variable: sym("?t")},
			},
			&Expression{
				Function: &GetElseFunction{
					Entity:  VariableTerm{Symbol: sym("?c")},
					Attr:    datalog.NewKeyword(":page/label"),
					Default: "none",
				},
				Binding: sym("?l"),
			}),
	})
	if err != nil {
		t.Fatalf("NewRuleSet: %v", err)
	}
	first := rs.SourceAttributes(datalog.SymDollar)
	want := []datalog.Keyword{
		datalog.NewKeyword(":page/next"),
		datalog.NewKeyword(":page/kind"),
		datalog.NewKeyword(":page/title"),
		datalog.NewKeyword(":page/label"),
	}
	if len(first) != len(want) {
		t.Fatalf("roster = %v, want %v", first, want)
	}
	for i := range want {
		if first[i] != want[i] {
			t.Errorf("roster[%d] = %s, want %s (every attribute once, in first-appearance order)", i, first[i], want[i])
		}
	}
	second := rs.SourceAttributes(datalog.SymDollar)
	if &first[0] != &second[0] {
		t.Error("the roster is collected once; a second call returns the same slice")
	}
}

// attrVarPattern builds [?e ?a _] on the default source: a pattern whose
// attribute is the variable named.
func attrVarPattern(e, a string) *DataPattern {
	return &DataPattern{Elements: []PatternElement{Variable{Name: sym(e)}, Variable{Name: sym(a)}, Blank{}}}
}

func TestScopeOfRuleInvocation(t *testing.T) {
	inv := invoke("follows", "?a", "x", "_", "?a")
	scope := ScopeOf(inv, nil)
	assertSymbols(t, "Provides", scope.Provides, "?a")
	assertSymbols(t, "Correlates", scope.Correlates)
	assertSymbols(t, "Variables", inv.Variables(), "?a")
	if got, want := inv.String(), `(follows ?a "x" _ ?a)`; got != want {
		t.Errorf("String() = %s, want %s", got, want)
	}

	// Read with the rule set, an invocation of a predicate with required
	// positions correlates on the variables there and provides the rest.
	// Read with no rule set, it provides every variable.
	succ := defineRule("succ", []string{"?n", "?m"}, scopePattern("?n", "page/next", "?m"))
	succ.Required = []Symbol{sym("?n")}
	rs := mustRuleSet(t, []*Rule{succ})
	scope = ScopeOf(invoke("succ", "?n", "?m"), rs)
	assertSymbols(t, "Provides under the rule set", scope.Provides, "?m")
	assertSymbols(t, "Correlates under the rule set", scope.Correlates, "?n")
	scope = ScopeOf(invoke("succ", "?n", "?m"), nil)
	assertSymbols(t, "Provides with no rule set", scope.Provides, "?n", "?m")
	assertSymbols(t, "Correlates with no rule set", scope.Correlates)
}

// small holds of a number below ten and requires the caller to bind it: its
// body consumes ?n and nothing in it provides ?n.
func smallRule() *Rule {
	r := defineRule("small", []string{"?n"}, &Comparison{
		Op:    datalog.SymLT,
		Left:  VariableTerm{Symbol: sym("?n")},
		Right: ConstantTerm{Value: int64(10)},
	})
	r.Required = []Symbol{sym("?n")}
	return r
}

func TestNewRuleSet_RequiredBindings(t *testing.T) {
	t.Run("a required binding supplies the head symbol and the predicate's correlate", func(t *testing.T) {
		rs, err := NewRuleSet([]*Rule{smallRule()})
		if err != nil {
			t.Fatalf("a rule whose only binding of ?n is the caller's is safe: %v", err)
		}
		if required := rs.Required(sym("small")); len(required) != 1 || !required[0] {
			t.Errorf("Required(small) = %v, want position 0 required", required)
		}
		if required := rs.Required(sym("absent")); required != nil {
			t.Errorf("Required of an undefined predicate = %v, want nil", required)
		}
	})
	t.Run("without the required run the same rule is unsafe", func(t *testing.T) {
		r := smallRule()
		r.Required = nil
		_, err := NewRuleSet([]*Rule{r})
		var unsafe *UnsafeRuleError
		if !errors.As(err, &unsafe) || unsafe.Symbol != sym("?n") {
			t.Fatalf("want *UnsafeRuleError naming ?n, got %v", err)
		}
	})
	t.Run("a required run that does not lead the head is refused", func(t *testing.T) {
		r := defineRule("succ", []string{"?n", "?m"}, scopePattern("?n", "page/next", "?m"))
		r.Required = []Symbol{sym("?m")}
		_, err := NewRuleSet([]*Rule{r})
		if err == nil || !strings.Contains(err.Error(), "succ") || !strings.Contains(err.Error(), "?m") {
			t.Fatalf("want a refusal naming succ and ?m, got %v", err)
		}
	})
	t.Run("the rules of one predicate agree on the required run", func(t *testing.T) {
		a := defineRule("succ", []string{"?n", "?m"}, scopePattern("?n", "page/next", "?m"))
		a.Required = []Symbol{sym("?n")}
		b := defineRule("succ", []string{"?n", "?m"}, scopePattern("?n", "page/prev", "?m"))
		_, err := NewRuleSet([]*Rule{a, b})
		if err == nil || !strings.Contains(err.Error(), "succ") {
			t.Fatalf("want a refusal naming succ, got %v", err)
		}
	})
	t.Run("a body invocation's required positions are correlates the body must bind", func(t *testing.T) {
		_, err := NewRuleSet([]*Rule{
			smallRule(),
			defineRule("holds", []string{"?x"}, scopePattern("?x", "page/n", "?n"), invoke("small", "?n")),
		})
		if err != nil {
			t.Fatalf("a body binding the required position is safe: %v", err)
		}
		_, err = NewRuleSet([]*Rule{
			smallRule(),
			defineRule("loose", []string{"?x"}, scopePattern("?x", "page/kind", "_"), invoke("small", "?y")),
		})
		var unsafe *UnsafeRuleError
		if !errors.As(err, &unsafe) || unsafe.Rule != sym("loose") || unsafe.Symbol != sym("?y") {
			t.Fatalf("want *UnsafeRuleError naming loose and ?y, got %v", err)
		}
	})
	t.Run("a required predicate recursive with one that is not is accepted", func(t *testing.T) {
		down := defineRule("down", []string{"?x", "?y"}, invoke("up", "?x", "?y"))
		down.Required = []Symbol{sym("?x")}
		_, err := NewRuleSet([]*Rule{
			defineRule("up", []string{"?x", "?y"}, scopePattern("?x", "page/kind", "_"), invoke("down", "?x", "?y")),
			down,
		})
		if err != nil {
			t.Fatalf("a free invocation of up is rewritten with its head all free, adorning down from the clauses before it: %v", err)
		}
	})
	t.Run("required predicates recursive with each other are accepted", func(t *testing.T) {
		upBase := defineRule("up", []string{"?n", "?m"}, scopePattern("?n", "page/next", "?m"))
		upBase.Required = []Symbol{sym("?n")}
		up := defineRule("up", []string{"?n", "?m"}, scopePattern("?n", "page/next", "?k"), invoke("down", "?k", "?m"))
		up.Required = []Symbol{sym("?n")}
		downBase := defineRule("down", []string{"?n", "?m"}, scopePattern("?n", "page/prev", "?m"))
		downBase.Required = []Symbol{sym("?n")}
		down := defineRule("down", []string{"?n", "?m"}, scopePattern("?n", "page/prev", "?k"), invoke("up", "?k", "?m"))
		down.Required = []Symbol{sym("?n")}
		if _, err := NewRuleSet([]*Rule{upBase, up, downBase, down}); err != nil {
			t.Fatalf("required predicates recursive with each other evaluate through the rewrite: %v", err)
		}
	})
}

// A symbol a clause consumes is bound before the clause runs, whatever form
// holds the clause: an input of a predicate, an expression or a subquery, or a
// required position, that nothing in the rule binds is refused at build under
// not and or-default as at the top level, naming the rule and the symbol.
func TestNewRuleSet_RefusesAnUnboundInputUnderNotAndOrDefault(t *testing.T) {
	below := func(w, v string) *Comparison {
		return &Comparison{Op: datalog.SymLT, Left: VariableTerm{Symbol: sym(w)}, Right: VariableTerm{Symbol: sym(v)}}
	}
	lookup := func(w, v string) *Expression {
		return &Expression{
			Function: &GetElseFunction{Entity: VariableTerm{Symbol: sym(w)}, Attr: datalog.NewKeyword(":page/kind"), Default: "none"},
			Binding:  sym(v),
		}
	}
	subquery := func(w, z string) *SubqueryPattern {
		return &SubqueryPattern{
			Query: &Query{
				Find:  []FindElement{FindVariable{Symbol: sym(z)}},
				In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: sym(w)}},
				Where: []Clause{scopePattern(z, "page/kind", w)},
			},
			Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Variable{Name: sym(w)}},
			Binding: RelationBinding{Variables: []Symbol{sym(z)}},
		}
	}
	cases := []struct {
		name   string
		clause Clause
		unsafe string
	}{
		{"a predicate under not", &NotClause{Clauses: []Clause{scopePattern("?x", "page/n", "?v"), below("?w", "?v")}}, "?w"},
		{"a predicate under or-default", &OrDefaultClause{Branches: [][]Clause{
			{scopePattern("?x", "page/n", "?v"), below("?w", "?v")},
			{scopePattern("?x", "page/kind", "_")},
		}}, "?w"},
		{"an expression under not", &NotClause{Clauses: []Clause{lookup("?w", "?v"), scopePattern("?x", "page/n", "?v")}}, "?w"},
		{"a subquery under or-default", &OrDefaultClause{Branches: [][]Clause{
			{scopePattern("?x", "page/n", "?v"), subquery("?w", "?z")},
			{scopePattern("?x", "page/kind", "_")},
		}}, "?w"},
		{"a required position under not", &NotClause{Clauses: []Clause{invoke("small", "?z")}}, "?z"},
		{"a required position under or-default", &OrDefaultClause{Branches: [][]Clause{
			{invoke("small", "?z")},
			{scopePattern("?x", "page/kind", "_")},
		}}, "?z"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewRuleSet([]*Rule{
				smallRule(),
				defineRule("r", []string{"?x"}, scopePattern("?x", "page/kind", "_"), c.clause),
			})
			var unsafe *UnsafeRuleError
			if !errors.As(err, &unsafe) {
				t.Fatalf("want *UnsafeRuleError naming r and %s, got %v", c.unsafe, err)
			}
			if unsafe.Rule != sym("r") || unsafe.Symbol != sym(c.unsafe) {
				t.Errorf("UnsafeRuleError = %+v, want rule r and symbol %s", unsafe, c.unsafe)
			}
		})
	}
}

// A rule's head carries at least one variable, the form Datomic's rule
// grammar gives it, so a head with none is refused at build, naming the
// rule, whether or not the rule has a body.
func TestNewRuleSet_RefusesAHeadWithNoVariable(t *testing.T) {
	cases := []struct {
		name string
		rule *Rule
	}{
		{"with a body", defineRule("always", nil, scopePattern("?x", "page/kind", "_"))},
		{"with no body", defineRule("always", nil)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewRuleSet([]*Rule{c.rule})
			if err == nil || !strings.Contains(err.Error(), "always") {
				t.Fatalf("want a refusal naming always, got %v", err)
			}
		})
	}
}

// A predicate's name is a symbol the parser reads as an invocation at the
// head of a list clause: neither a reserved clause form nor a variable. A
// rule named for one is refused at build, naming the rule.
func TestNewRuleSet_RefusesAReservedFormOrVariableAsAPredicateName(t *testing.T) {
	for _, name := range []string{"not", "not-join", "or", "or-join", "or-default", "or-default-join", "and", "?r"} {
		t.Run(name, func(t *testing.T) {
			_, err := NewRuleSet([]*Rule{
				defineRule(name, []string{"?x"}, scopePattern("?x", "page/kind", "_")),
			})
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("want a refusal naming %s, got %v", name, err)
			}
		})
	}
}

// A rule with no name is refused at build with an error saying so, as every
// other malformed rule is.
func TestNewRuleSet_RefusesARuleWithNoName(t *testing.T) {
	_, err := NewRuleSet([]*Rule{{Head: []Symbol{sym("?x")}, Body: []Clause{scopePattern("?x", "page/kind", "_")}}})
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("want a refusal saying the rule has no name, got %v", err)
	}
}

// A query declares % at most once, a rule of the :in clause alone, so every
// boundary refuses the same shape.
func TestValidateInputShapes(t *testing.T) {
	db := DatabaseInput{Name: datalog.SymDollar}
	if err := ValidateInputShapes([]InputSpec{db, ScalarInput{Symbol: sym("?x")}}); err != nil {
		t.Errorf("no %% is valid, got %v", err)
	}
	if err := ValidateInputShapes([]InputSpec{db, RulesInput{}, ScalarInput{Symbol: sym("?x")}}); err != nil {
		t.Errorf("one %% is valid, got %v", err)
	}
	err := ValidateInputShapes([]InputSpec{db, RulesInput{}, RulesInput{}})
	if err == nil || !strings.Contains(err.Error(), "%") {
		t.Fatalf("want a refusal naming %%, got %v", err)
	}
}

// A clause using the query's own % needs % declared in its :in: an
// invocation at any depth short of a subquery body, or a subquery call
// passing % through, is refused naming the clause when the :in declares
// none, and a subquery body's invocation is the inner query's use.
func TestValidateRulesInput(t *testing.T) {
	db := DatabaseInput{Name: datalog.SymDollar}
	inv := invoke("follows", "?a", "?b")
	inner := findIn([]string{"?b"}, []InputSpec{db, RulesInput{}, ScalarInput{Symbol: sym("?a")}}, invoke("follows", "?a", "?b"))
	passing := &SubqueryPattern{
		Query:   inner,
		Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Constant{Value: datalog.SymRules}, Variable{Name: sym("?a")}},
		Binding: RelationBinding{Variables: []Symbol{sym("?b")}},
	}
	notPassing := &SubqueryPattern{
		Query:   findIn([]string{"?b"}, []InputSpec{db, ScalarInput{Symbol: sym("?a")}}, scopePattern("?a", "page/next", "?b")),
		Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Variable{Name: sym("?a")}},
		Binding: RelationBinding{Variables: []Symbol{sym("?b")}},
	}

	if err := ValidateRulesInput([]InputSpec{db}, []Clause{scopePattern("?a", "page/next", "?b"), notPassing}); err != nil {
		t.Errorf("a query using no %% is valid, got %v", err)
	}
	if err := ValidateRulesInput([]InputSpec{db, RulesInput{}}, []Clause{inv, passing}); err != nil {
		t.Errorf("a query declaring %% is valid, got %v", err)
	}
	err := ValidateRulesInput([]InputSpec{db}, []Clause{scopePattern("?a", "page/next", "?b"), inv})
	if err == nil || !strings.Contains(err.Error(), inv.String()) || !strings.Contains(err.Error(), ":in") {
		t.Errorf("want a refusal naming the invocation and the :in, got %v", err)
	}
	err = ValidateRulesInput([]InputSpec{db}, []Clause{scopePattern("?a", "page/next", "?b"), passing})
	if err == nil || !strings.Contains(err.Error(), passing.String()) || !strings.Contains(err.Error(), "passes %") {
		t.Errorf("want a refusal naming the subquery call that passes %%, got %v", err)
	}
	bodyUse := &SubqueryPattern{
		Query:   inner,
		Inputs:  []PatternElement{Constant{Value: datalog.SymDollar}, Variable{Name: sym("?rules")}, Variable{Name: sym("?a")}},
		Binding: RelationBinding{Variables: []Symbol{sym("?b")}},
	}
	if err := ValidateRulesInput([]InputSpec{db, ScalarInput{Symbol: sym("?rules")}}, []Clause{scopePattern("?a", "page/next", "?b"), bodyUse}); err != nil {
		t.Errorf("an invocation inside a subquery body is the inner query's use of its own %%, got %v", err)
	}
}

// TestNewRuleSet_HoldsInferredBodies: a rule set holds each rule with its
// body's forms inferred, as a query finding the head with the required run
// as its inputs, the rule given untouched; a rule whose body is header forms
// is held as the pointer given.
func TestNewRuleSet_HoldsInferredBodies(t *testing.T) {
	plain := defineRule("tagged", []string{"?e", "?x"},
		scopePattern("?e", "page/n", "_"),
		&OrClause{Branches: [][]Clause{{scopePattern("?e", "page/p", "?x")}, {scopePattern("?e", "page/q", "?x")}}})
	given := defineRule("holds", []string{"?x"}, scopePattern("?x", "page/n", "?n"), invoke("small", "?n"))
	rs := mustRuleSet(t, []*Rule{smallRule(), plain, given})

	if got := rs.Rules(sym("holds")); len(got) != 1 || got[0] != given {
		t.Errorf("Rules(holds) = %v, want the rule given, itself", got)
	}
	got := rs.Rules(sym("tagged"))
	if len(got) != 1 || got[0] == plain {
		t.Fatalf("Rules(tagged) = %v, want one rule, a copy of the given with its body inferred", got)
	}
	if got[0].Name != plain.Name || len(got[0].Head) != 2 || got[0].Head[0] != plain.Head[0] || got[0].Head[1] != plain.Head[1] {
		t.Errorf("the inferred rule's head is %s %v, want the given %s %v", got[0].Name, got[0].Head, plain.Name, plain.Head)
	}
	want := "(or-join [?e ?x] [?e :page/p ?x] [?e :page/q ?x])"
	if len(got[0].Body) != 2 || got[0].Body[1].String() != want {
		t.Errorf("the inferred body's or = %v, want %s, ?x shared through the head", got[0].Body, want)
	}
	if _, isOr := plain.Body[1].(*OrClause); !isOr {
		t.Errorf("the rule given is rewritten in place: its or is %s", plain.Body[1])
	}
}

func TestWalkClausesVisitsRuleInvocation(t *testing.T) {
	inv := invoke("follows", "?a", "?b")
	var visited []Clause
	WalkClauses([]Clause{
		&OrJoinClause{OutputVars: []Symbol{sym("?a")}, Branches: [][]Clause{{inv}}},
	}, func(c Clause) bool {
		visited = append(visited, c)
		return true
	})
	if len(visited) != 2 || visited[1] != Clause(inv) {
		t.Errorf("visited %v, want the or-join then the invocation it holds", visited)
	}
}

// A rule set is the one home of a predicate's head, the symbols its relation
// carries and an invocation's arguments unify with by position: the first
// rule's head for a predicate with rules, the magic rule's head for a seed
// predicate a magic rule derives into, the minted per-position symbols for a
// seed no rule derives, the predicate's own head for a delta symbol, and
// absent for a name the set does not declare.
func TestRuleSetHead(t *testing.T) {
	closure, err := NewRuleSet(followsRules())
	if err != nil {
		t.Fatal(err)
	}
	// The closure rewritten for a bound first position, as the planner's
	// rewrite builds it: the adorned closure guarded by its magic predicate,
	// which the recursive rule's magic rule derives into.
	rewritten, err := NewSeededRuleSet([]*Rule{
		defineRule("adorned.bf/follows", []string{"?a", "?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
		defineRule("adorned.bf/follows", []string{"?a", "?c"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b"),
			invoke("adorned.bf/follows", "?b", "?c")),
		defineRule("magic.bf/follows", []string{"?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
	}, []SeedPredicate{{Name: sym("magic.bf/follows"), Arity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	// An edge predicate rewritten the same way: no magic rule derives into
	// its seed, which the caller alone fills.
	edgesRewritten, err := NewSeededRuleSet([]*Rule{
		defineRule("adorned.bf/edge", []string{"?a", "?b"},
			invoke("magic.bf/edge", "?a"),
			scopePattern("?a", "page/next", "?b")),
	}, []SeedPredicate{{Name: sym("magic.bf/edge"), Arity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		label string
		rs    *RuleSet
		name  Symbol
		want  []string
	}{
		{"a predicate with rules", closure, sym("follows"), []string{"?a", "?b"}},
		{"the delta of a predicate with rules", closure, DeltaSymbol(sym("follows")), []string{"?a", "?b"}},
		{"the adorned predicate", rewritten, sym("adorned.bf/follows"), []string{"?a", "?b"}},
		{"a seed predicate a magic rule derives into", rewritten, sym("magic.bf/follows"), []string{"?b"}},
		{"the delta of a seed predicate a magic rule derives into", rewritten, DeltaSymbol(sym("magic.bf/follows")), []string{"?b"}},
		{"a seed predicate no rule derives", edgesRewritten, sym("magic.bf/edge"), []string{"?magic.bf/edge.0"}},
		{"the delta of a seed predicate no rule derives", edgesRewritten, DeltaSymbol(sym("magic.bf/edge")), []string{"?magic.bf/edge.0"}},
	}
	for _, c := range cases {
		got, ok := c.rs.Head(c.name)
		if !ok {
			t.Errorf("%s: Head(%s) reports no head", c.label, c.name)
			continue
		}
		want := make([]Symbol, len(c.want))
		for i, name := range c.want {
			want[i] = sym(name)
		}
		if len(got) != len(want) {
			t.Errorf("%s: Head(%s) = %v, want %v", c.label, c.name, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s: Head(%s) = %v, want %v", c.label, c.name, got, want)
				break
			}
		}
	}
	for _, name := range []Symbol{sym("absent"), DeltaSymbol(sym("absent")), ScopeRequired} {
		if got, ok := closure.Head(name); ok {
			t.Errorf("Head(%s) = %v, true, want no head for a name the set does not declare", name, got)
		}
	}
}

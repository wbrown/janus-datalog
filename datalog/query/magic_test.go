package query

import (
	"errors"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// A rule set rewritten for an invocation with bound arguments derives only
// the tuples those arguments reach: each predicate the bindings reach gets a
// copy adorned with its binding pattern and guarded by a magic predicate
// holding the bindings it is asked with, and the bindings pass left to right
// through each body into the invocations after them. An invocation nothing
// binds keeps the original rules, a subquery body is not entered, and a
// rewrite that does not stratify is declined.

// assertRules compares two rule lists rendered rule by rule.
func assertRules(t *testing.T, label string, got, want []*Rule) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d rules, want %d:\n%s\n--\n%s", label, len(got), len(want), renderRules(got), renderRules(want))
	}
	for i := range want {
		if got[i].String() != want[i].String() {
			t.Errorf("%s rule %d:\n got %s\nwant %s", label, i, got[i], want[i])
		}
	}
}

func renderRules(rules []*Rule) string {
	lines := make([]string, len(rules))
	for i, r := range rules {
		lines[i] = r.String()
	}
	return strings.Join(lines, "\n")
}

func mustRuleSet(t *testing.T, rules []*Rule) *RuleSet {
	t.Helper()
	rs, err := NewRuleSet(rules)
	if err != nil {
		t.Fatalf("NewRuleSet: %v", err)
	}
	return rs
}

func mustMagic(t *testing.T, rs *RuleSet, name string, bound ...bool) *Magic {
	t.Helper()
	magic, err := rs.Magic(sym(name), bound)
	if err != nil {
		t.Fatalf("Magic(%s): %v", name, err)
	}
	return magic
}

func TestMagic_ClosureBoundFirst(t *testing.T) {
	rs := mustRuleSet(t, followsRules())
	magic := mustMagic(t, rs, "follows", true, false)
	if magic.Predicate != sym("adorned.bf/follows") || magic.Seed != sym("magic.bf/follows") {
		t.Errorf("Predicate %s, Seed %s; want adorned.bf/follows and magic.bf/follows", magic.Predicate, magic.Seed)
	}
	assertRules(t, "adorned.bf/follows", magic.Rules.Rules(sym("adorned.bf/follows")), []*Rule{
		defineRule("adorned.bf/follows", []string{"?a", "?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
		defineRule("adorned.bf/follows", []string{"?a", "?c"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b"),
			invoke("adorned.bf/follows", "?b", "?c")),
	})
	assertRules(t, "magic.bf/follows", magic.Rules.Rules(sym("magic.bf/follows")), []*Rule{
		defineRule("magic.bf/follows", []string{"?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
	})
	if arity, ok := magic.Rules.Arity(sym("magic.bf/follows")); !ok || arity != 1 {
		t.Errorf("Arity(magic.bf/follows) = %d, %v; want 1, true: the seed predicate is declared", arity, ok)
	}
	// The magic rule reads only its own predicate and the edge, so the magic
	// set is a recursive stratum of its own, complete before the closure
	// restricted to it runs.
	strata := magic.Rules.Strata()
	if len(strata) != 2 || !strata[0].Recursive || !strata[1].Recursive {
		t.Fatalf("strata = %+v, want two recursive strata, the magic set then the closure", strata)
	}
	assertSymbols(t, "first stratum", strata[0].Predicates, "magic.bf/follows")
	assertSymbols(t, "second stratum", strata[1].Predicates, "adorned.bf/follows")
	assertRules(t, "the original set", rs.Rules(sym("follows")), followsRules())
}

func TestMagic_ConstantAndRepeatedArguments(t *testing.T) {
	rs := mustRuleSet(t, []*Rule{
		defineRule("pair", []string{"?x", "?y"}, scopePattern("?x", "page/next", "?y")),
		defineRule("twin", []string{"?x"}, invoke("pair", "?x", "k"), invoke("pair", "?x", "?x")),
	})
	magic := mustMagic(t, rs, "twin", true)
	assertRules(t, "adorned.b/twin", magic.Rules.Rules(sym("adorned.b/twin")), []*Rule{
		defineRule("adorned.b/twin", []string{"?x"},
			invoke("magic.b/twin", "?x"),
			invoke("adorned.bb/pair", "?x", "k"),
			invoke("adorned.bf/pair", "?x", "?x")),
	})
	assertRules(t, "adorned.bb/pair", magic.Rules.Rules(sym("adorned.bb/pair")), []*Rule{
		defineRule("adorned.bb/pair", []string{"?x", "?y"},
			invoke("magic.bb/pair", "?x", "?y"),
			scopePattern("?x", "page/next", "?y")),
	})
	assertRules(t, "adorned.bf/pair", magic.Rules.Rules(sym("adorned.bf/pair")), []*Rule{
		defineRule("adorned.bf/pair", []string{"?x", "?y"},
			invoke("magic.bf/pair", "?x"),
			scopePattern("?x", "page/next", "?y")),
	})
	// A constant at a bound position is a fresh head variable a ground
	// expression binds; a variable repeated across positions is bound at its
	// first position only, so the second stays free.
	assertRules(t, "magic.bb/pair", magic.Rules.Rules(sym("magic.bb/pair")), []*Rule{
		defineRule("magic.bb/pair", []string{"?x", "?magic.bb/pair.1"},
			invoke("magic.b/twin", "?x"),
			&Expression{Function: &GroundFunction{Value: "k"}, Binding: sym("?magic.bb/pair.1")}),
	})
	assertRules(t, "magic.bf/pair", magic.Rules.Rules(sym("magic.bf/pair")), []*Rule{
		defineRule("magic.bf/pair", []string{"?x"},
			invoke("magic.b/twin", "?x"),
			invoke("adorned.bb/pair", "?x", "k")),
	})
}

func TestMagic_InvocationInsideOrJoinBranch(t *testing.T) {
	orJoin := &OrJoinClause{JoinVars: []Symbol{sym("?b"), sym("?c")}, Branches: [][]Clause{
		{invoke("follows", "?b", "?c")},
		{scopePattern("?b", "page/next", "?c")},
	}}
	rs := mustRuleSet(t, []*Rule{
		defineRule("follows", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b")),
		defineRule("follows", []string{"?a", "?c"}, scopePattern("?a", "page/next", "?b"), orJoin),
	})
	magic := mustMagic(t, rs, "follows", true, false)
	got := magic.Rules.Rules(sym("adorned.bf/follows"))
	assertRules(t, "adorned.bf/follows", got, []*Rule{
		defineRule("adorned.bf/follows", []string{"?a", "?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
		defineRule("adorned.bf/follows", []string{"?a", "?c"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b"),
			&OrJoinClause{JoinVars: []Symbol{sym("?b"), sym("?c")}, Branches: [][]Clause{
				{invoke("adorned.bf/follows", "?b", "?c")},
				{scopePattern("?b", "page/next", "?c")},
			}}),
	})
	assertRules(t, "magic.bf/follows", magic.Rules.Rules(sym("magic.bf/follows")), []*Rule{
		defineRule("magic.bf/follows", []string{"?b"},
			invoke("magic.bf/follows", "?a"),
			scopePattern("?a", "page/next", "?b")),
	})
	if len(got) == 2 && len(got[1].Body) == 3 {
		rewritten, ok := got[1].Body[2].(*OrJoinClause)
		if !ok || rewritten == orJoin {
			t.Error("the or-join on the path to the adorned invocation is copied, not shared")
		} else if rewritten.Branches[1][0] != orJoin.Branches[1][0] {
			t.Error("a branch holding no invocation shares its clauses")
		}
	}
}

func TestMagic_UnboundInvocationKeepsOriginalRules(t *testing.T) {
	follows := followsRules()
	rs := mustRuleSet(t, append(follows,
		defineRule("any", []string{"?x"}, invoke("follows", "?y", "?z"), scopePattern("?y", "page/kind", "?x"))))
	magic := mustMagic(t, rs, "any", true)
	assertRules(t, "adorned.b/any", magic.Rules.Rules(sym("adorned.b/any")), []*Rule{
		defineRule("adorned.b/any", []string{"?x"},
			invoke("magic.b/any", "?x"),
			invoke("follows", "?y", "?z"),
			scopePattern("?y", "page/kind", "?x")),
	})
	got := magic.Rules.Rules(sym("follows"))
	if len(got) != 2 || got[0] != follows[0] || got[1] != follows[1] {
		t.Errorf("Rules(follows) = %v, want the two original rules unchanged: nothing binds the invocation", got)
	}
	assertSymbols(t, "Predicates", magic.Rules.Predicates(), "magic.b/any", "adorned.b/any", "follows")
	if rules := magic.Rules.Rules(sym("magic.b/any")); len(rules) != 0 {
		t.Errorf("the seed predicate of a non-recursive rule has no rules, got %v", rules)
	}
	if arity, ok := magic.Rules.Arity(sym("magic.b/any")); !ok || arity != 1 {
		t.Errorf("Arity(magic.b/any) = %d, %v; want 1, true", arity, ok)
	}
}

func TestMagic_SubqueryBodyNotRewritten(t *testing.T) {
	sub := &SubqueryPattern{
		Query: &Query{
			Find:  []FindElement{FindVariable{Symbol: sym("?y")}},
			In:    []InputSpec{DatabaseInput{Name: datalog.SymDollar}, RulesInput{}, ScalarInput{Symbol: sym("?x")}},
			Where: []Clause{invoke("follows", "?x", "?y")},
		},
		Inputs: []PatternElement{
			Constant{Value: datalog.SymDollar},
			Constant{Value: datalog.NewSymbol("%")},
			Variable{Name: sym("?x")},
		},
		Binding: RelationBinding{Variables: []Symbol{sym("?y")}},
	}
	follows := followsRules()
	rs := mustRuleSet(t, append(follows,
		defineRule("reached", []string{"?x", "?y"}, scopePattern("?x", "page/kind", "_"), sub)))
	magic := mustMagic(t, rs, "reached", true, false)
	got := magic.Rules.Rules(sym("adorned.bf/reached"))
	assertRules(t, "adorned.bf/reached", got, []*Rule{
		defineRule("adorned.bf/reached", []string{"?x", "?y"},
			invoke("magic.bf/reached", "?x"),
			scopePattern("?x", "page/kind", "_"),
			sub),
	})
	if len(got) == 1 && len(got[0].Body) == 3 && got[0].Body[2] != Clause(sub) {
		t.Error("a subquery is not entered: the clause is shared")
	}
	if original := magic.Rules.Rules(sym("follows")); len(original) != 2 || original[0] != follows[0] {
		t.Errorf("the predicate the subquery body invokes keeps its original rules, got %v", original)
	}
	// A subquery body's invocation is not adorned: the set holds the seed, the
	// adorned predicate, and follows as it was.
	assertSymbols(t, "Predicates", magic.Rules.Predicates(), "magic.bf/reached", "adorned.bf/reached", "follows")
}

// TestMagic_RequiredPositionLeftFree: the bindings pass left to right, so an
// invocation of a predicate with a required position is adorned by what the
// clauses before it bind. A rule binding the position after the invocation
// is refused naming the predicate, the head variable and the rule; one
// binding it before adorns the invocation.
func TestMagic_RequiredPositionLeftFree(t *testing.T) {
	rs := mustRuleSet(t, []*Rule{
		smallRule(),
		defineRule("holds", []string{"?x"}, scopePattern("?x", "page/n", "?n"), invoke("small", "?n")),
		defineRule("loose", []string{"?x"}, invoke("small", "?n"), scopePattern("?x", "page/n", "?n")),
	})
	_, err := rs.Magic(sym("loose"), []bool{true})
	var required *RequiredBindingError
	if !errors.As(err, &required) {
		t.Fatalf("want *RequiredBindingError, got %v", err)
	}
	if required.Name != sym("small") || required.Symbol != sym("?n") || required.Rule != sym("loose") {
		t.Errorf("RequiredBindingError = %+v, want predicate small, variable ?n, rule loose", required)
	}

	magic := mustMagic(t, rs, "holds", true)
	assertRules(t, "adorned.b/holds", magic.Rules.Rules(sym("adorned.b/holds")), []*Rule{
		defineRule("adorned.b/holds", []string{"?x"},
			invoke("magic.b/holds", "?x"),
			scopePattern("?x", "page/n", "?n"),
			invoke("adorned.b/small", "?n")),
	})
	if adorned := magic.Rules.Rules(sym("adorned.b/small")); len(adorned) != 1 || len(adorned[0].Required) != 1 {
		t.Errorf("adorned.b/small keeps small's required run, got %v", adorned)
	}
}

// TestMagic_DeclinesUnstratifiableRewrite: q's magic rule for s reads the
// negation of t that precedes the invocation, and s invokes q, so the magic
// predicates close a cycle through that negation which the original set, a
// cycle of q and s beside a separate t, does not have.
func TestMagic_DeclinesUnstratifiableRewrite(t *testing.T) {
	rs := mustRuleSet(t, []*Rule{
		defineRule("q", []string{"?x"},
			scopePattern("?x", "page/a", "?y"),
			&NotClause{Clauses: []Clause{invoke("t", "?y")}},
			invoke("s", "?y")),
		defineRule("s", []string{"?y"},
			scopePattern("?y", "page/b", "?z"),
			invoke("q", "?z")),
		defineRule("t", []string{"?y"}, scopePattern("?y", "page/c", "_")),
	})
	_, err := rs.Magic(sym("q"), []bool{true})
	var unstratifiable *UnstratifiableError
	if !errors.As(err, &unstratifiable) {
		t.Fatalf("want *UnstratifiableError, the decline, got %v", err)
	}
}

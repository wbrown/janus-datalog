package query

import (
	"strings"
	"testing"
)

// The engine mints predicates beside a rule set's own: a predicate adorned
// with a binding pattern, the magic predicate holding the bindings it is
// asked with, and the delta of a predicate under fixpoint evaluation. Each is
// a symbol in a namespace of the engine's, so the text of a body that invokes
// it is Datalog the parser reads, and a rule set may not declare a predicate
// there.

func TestMintedPredicateSymbols(t *testing.T) {
	bf := []bool{true, false}
	cases := []struct {
		label string
		got   Symbol
		want  string
	}{
		{"adorned", AdornedSymbol(sym("follows"), bf), "adorned.bf/follows"},
		{"magic", MagicSymbol(sym("follows"), bf), "magic.bf/follows"},
		{"delta", DeltaSymbol(sym("follows")), "delta/follows"},
		{"delta of an adorned predicate", DeltaSymbol(AdornedSymbol(sym("follows"), bf)), "delta.adorned.bf/follows"},
		{"delta of a magic predicate", DeltaSymbol(MagicSymbol(sym("follows"), bf)), "delta.magic.bf/follows"},
		{"adorned, the predicate in a namespace of the caller's", AdornedSymbol(sym("mine/follows"), bf), "adorned.bf.mine/follows"},
		{"a scope's collection input at its call position", ScopeInput(2), "?scope/input.2"},
		{"a pattern's later occurrence of a variable", UnifiedVariable(sym("?x"), 2), "?unify/x.2"},
		{"a pattern's later occurrence of a namespaced variable", UnifiedVariable(sym("?mine/x"), 1), "?unify.mine/x.1"},
	}
	for _, c := range cases {
		if c.got != sym(c.want) {
			t.Errorf("%s: %s, want %s", c.label, c.got, c.want)
		}
	}
}

func TestAdornment(t *testing.T) {
	if got := adornment([]bool{true, false, false}); got != "bff" {
		t.Errorf("adornment(b f f) = %q, want bff", got)
	}
	if got := adornment(nil); got != "" {
		t.Errorf("adornment of no positions = %q, want the empty pattern", got)
	}
}

func TestEngineNamespace(t *testing.T) {
	for _, name := range []string{"delta/follows", "delta.adorned.bf/follows", "magic.b/follows", "adorned.bf/follows", "scope/required", "unify/follows",
		"?unify/e.2", "?unify.mine/x.1", "?scope/input.1", "?magic.bf/reach.0", "?adorned.bf/x", "?delta/x"} {
		if !engineNamespace(sym(name)) {
			t.Errorf("%s lies in a namespace the engine mints into", name)
		}
	}
	for _, name := range []string{"follows", "mine/follows", "deltas/follows", "mine.delta/follows", "scopes/required",
		"?e", "?unify", "?unify-me", "?scoped/x", "?x.unify/y", "?mine/unify"} {
		if engineNamespace(sym(name)) {
			t.Errorf("%s lies in no namespace the engine mints into", name)
		}
	}
}

// A rule naming a variable in a namespace the engine mints into, at its head
// or anywhere in its body, is refused naming the rule and the variable, since
// the inference writes the engine's own variables into the body and a user's
// would join them; a variable outside those namespaces is accepted.
func TestNewRuleSet_RefusesAVariableInAnEngineNamespace(t *testing.T) {
	inner := func(where ...Clause) *SubqueryPattern {
		return &SubqueryPattern{
			Query:   &Query{Find: []FindElement{FindVariable{Symbol: sym("?y")}}, In: []InputSpec{DatabaseInput{Name: sym("$")}, ScalarInput{Symbol: sym("?x")}}, Where: where},
			Inputs:  []PatternElement{Constant{Value: sym("$")}, Variable{Name: sym("?e")}},
			Binding: RelationBinding{Variables: []Symbol{sym("?n")}},
		}
	}
	cases := []struct {
		name     string
		rule     *Rule
		variable string
	}{
		{"at the head", defineRule("r", []string{"?unify/n.2"}, scopePattern("?e", "p", "?unify/n.2")), "?unify/n.2"},
		{"in a body pattern", defineRule("r", []string{"?n"}, scopePattern("?e", "p", "?n"), scopePattern("?e", "q", "?scope/input.1")), "?scope/input.1"},
		{"in an or branch", defineRule("r", []string{"?n"}, scopePattern("?e", "p", "?n"),
			&OrClause{Branches: [][]Clause{{scopePattern("?e", "q", "?magic.bf/x.0")}, {scopePattern("?e", "r", "?magic.bf/x.0")}}}), "?magic.bf/x.0"},
		{"in a subquery's inner query", defineRule("r", []string{"?n"}, scopePattern("?e", "p", "?w"),
			inner(scopePattern("?x", "q", "?unify/y.2"), &Expression{Function: IdentityFunction{Arg: VariableTerm{Symbol: sym("?unify/y.2")}}, Binding: sym("?y")})), "?unify/y.2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewRuleSet([]*Rule{c.rule})
			refused, ok := err.(*EngineNamespaceError)
			if !ok {
				t.Fatalf("want an EngineNamespaceError naming %s, got %v", c.variable, err)
			}
			if refused.Rule != sym("r") || refused.Variable != sym(c.variable) {
				t.Errorf("the refusal names rule %s and variable %s, want r and %s", refused.Rule, refused.Variable, c.variable)
			}
			if !strings.Contains(err.Error(), "namespace") || !strings.Contains(err.Error(), "rename") {
				t.Errorf("the refusal names the namespace and the repair: %v", err)
			}
		})
	}
	for _, name := range []string{"?unify", "?unify-me", "?scoped/x", "?x.unify/y", "?mine/unify"} {
		if _, err := NewRuleSet([]*Rule{defineRule("r", []string{"?n"}, scopePattern("?e", "p", "?n"), scopePattern("?e", "q", name))}); err != nil {
			t.Errorf("%s lies in no namespace the engine mints into and is accepted: %v", name, err)
		}
	}
}

func TestSeedRestricted(t *testing.T) {
	for _, name := range []string{"magic.bf/follows", "adorned.bf/follows", "delta.magic.bf/follows", "delta.adorned.bf/follows", "adorned.bf.mine/follows", "scope/required"} {
		if !SeedRestricted(sym(name)) {
			t.Errorf("%s holds only what a seed reaches", name)
		}
	}
	for _, name := range []string{"follows", "mine/follows", "delta/follows", "delta.mine/follows", "magical/follows", "scopes/required"} {
		if SeedRestricted(sym(name)) {
			t.Errorf("%s is not restricted by a seed", name)
		}
	}
}

func TestNewRuleSet_RefusesEngineNamespaces(t *testing.T) {
	for _, name := range []string{"delta/follows", "delta.adorned.bf/follows", "magic.b/follows", "adorned.bf/follows", "scope/required"} {
		_, err := NewRuleSet([]*Rule{defineRule(name, []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b"))})
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("a rule named %s: want a refusal naming it, got %v", name, err)
		}
	}
	if _, err := NewRuleSet([]*Rule{defineRule("mine/follows", []string{"?a", "?b"}, scopePattern("?a", "page/next", "?b"))}); err != nil {
		t.Errorf("a namespace of the caller's own is accepted: %v", err)
	}
}

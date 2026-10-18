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
		{"adorned", adornedSymbol(sym("follows"), bf), "adorned.bf/follows"},
		{"magic", magicSymbol(sym("follows"), bf), "magic.bf/follows"},
		{"delta", DeltaSymbol(sym("follows")), "delta/follows"},
		{"delta of an adorned predicate", DeltaSymbol(adornedSymbol(sym("follows"), bf)), "delta.adorned.bf/follows"},
		{"delta of a magic predicate", DeltaSymbol(magicSymbol(sym("follows"), bf)), "delta.magic.bf/follows"},
		{"adorned, the predicate in a namespace of the caller's", adornedSymbol(sym("mine/follows"), bf), "adorned.bf.mine/follows"},
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
	for _, name := range []string{"delta/follows", "delta.adorned.bf/follows", "magic.b/follows", "adorned.bf/follows", "scope/required"} {
		if !engineNamespace(sym(name)) {
			t.Errorf("%s lies in a namespace the engine mints into", name)
		}
	}
	for _, name := range []string{"follows", "mine/follows", "deltas/follows", "mine.delta/follows", "scopes/required"} {
		if engineNamespace(sym(name)) {
			t.Errorf("%s lies in no namespace the engine mints into", name)
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

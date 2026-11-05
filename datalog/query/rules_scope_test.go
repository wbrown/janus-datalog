package query

import "testing"

// A not inside an or branch may read a variable that occurs nowhere else:
// the variable is the not's own existential, local to its body, so the rule
// is safe whether the not stands at the body's top level or inside a branch.
func TestNewRuleSet_AcceptsAnExistentialUnderANotInsideAnOrBranch(t *testing.T) {
	existential := &NotClause{Clauses: []Clause{scopePattern("?x", "e", "?w"), scopePattern("?w", "c", "zero")}}
	rules := []*Rule{
		defineRule("top", []string{"?x"}, scopePattern("?x", "t", "node"), existential),
		defineRule("branch", []string{"?x"},
			scopePattern("?x", "t", "node"),
			&OrClause{Branches: [][]Clause{
				{scopePattern("?x", "c", "one")},
				{scopePattern("?x", "c", "two"), existential},
			}}),
	}
	for _, rule := range rules {
		rs, err := NewRuleSet([]*Rule{rule})
		if err != nil {
			t.Errorf("rule %s is refused: %v; a not's existential is local to its body wherever the not stands", rule.Name, err)
			continue
		}
		// The accepted set defines the rule's predicate, of arity one, by the
		// one rule given.
		if got := rs.Predicates(); len(got) != 1 || got[0] != rule.Name {
			t.Errorf("rule %s: the set defines %v, want only %s", rule.Name, got, rule.Name)
		}
		if arity, ok := rs.Arity(rule.Name); !ok || arity != 1 {
			t.Errorf("rule %s: arity %d (defined: %v), want 1", rule.Name, arity, ok)
		}
		if n := len(rs.Rules(rule.Name)); n != 1 {
			t.Errorf("rule %s: the set holds %d rules for it, want 1", rule.Name, n)
		}
	}
}

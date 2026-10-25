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
		if _, err := NewRuleSet([]*Rule{rule}); err != nil {
			t.Errorf("rule %s is refused: %v; a not's existential is local to its body wherever the not stands", rule.Name, err)
		}
	}
}

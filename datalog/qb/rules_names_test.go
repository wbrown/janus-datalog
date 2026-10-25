package qb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// RuleSet refuses a predicate named for a clause form, one named as a
// variable and a head naming no variable with the message NewRuleSet gives
// (docs/reference/RULES.md, the four plain refusals), and ParseRules refuses
// the same rule set's text with the same message.
func TestRuleSet_RefusesClauseFormNamesVariableNamesAndAnEmptyHead(t *testing.T) {
	x := NewVar("x")
	cases := []struct {
		name, predicate, text, message string
	}{
		{"not", "not", `[[(not ?x) [?x :a _]]]`, "heads a clause form"},
		{"not-join", "not-join", `[[(not-join ?x) [?x :a _]]]`, "heads a clause form"},
		{"or", "or", `[[(or ?x) [?x :a _]]]`, "heads a clause form"},
		{"or-join", "or-join", `[[(or-join ?x) [?x :a _]]]`, "heads a clause form"},
		{"or-default", "or-default", `[[(or-default ?x) [?x :a _]]]`, "heads a clause form"},
		{"or-default-join", "or-default-join", `[[(or-default-join ?x) [?x :a _]]]`, "heads a clause form"},
		{"and", "and", `[[(and ?x) [?x :a _]]]`, "heads a clause form"},
		{"a variable", "?r", `[[(?r ?x) [?x :a _]]]`, "a predicate's name is not a variable"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := RuleSet(DefineRule(c.predicate, x).Where(Pat(x, Kw(":a"), Blank())))
			require.Error(t, err)
			require.Contains(t, err.Error(), c.message)
			_, parseErr := parser.ParseRules(c.text)
			require.Error(t, parseErr)
			require.Equal(t, parseErr.Error(), err.Error(), "the builder and the parser refuse with one message")
		})
	}
	t.Run("an empty head", func(t *testing.T) {
		_, err := RuleSet(DefineRule("always").Where(Pat(x, Kw(":a"), Blank())))
		require.Error(t, err)
		require.Contains(t, err.Error(), "a head names at least one variable")
		_, parseErr := parser.ParseRules(`[[(always) [?x :a _]]]`)
		require.Error(t, parseErr)
		require.Equal(t, parseErr.Error(), err.Error(), "the builder and the parser refuse with one message")
	})
}

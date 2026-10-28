package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ParseRules refuses rule text holding a constant outside the value domain,
// a NaN at a pattern element, a ground or a predicate term, naming the rule
// and the value, as ParseQuery refuses the same constant in a query's
// :where.
func TestParseRulesRefusesAConstantOutsideTheDomain(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"a NaN ground", `[[(r ?x) [?e :t/a _] [(ground ##NaN) ?x]]]`},
		{"a NaN pattern element", `[[(r ?e) [?e :t/a ##NaN]]]`},
		{"a NaN predicate term", `[[(r ?e) [?e :t/a ?v] [(> ?v ##NaN)]]]`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseRules(c.text)
			require.ErrorContains(t, err, "rule r", "the refusal names the rule: %s", c.text)
			require.ErrorContains(t, err, "NaN is not a datalog value", "the refusal names the value: %s", c.text)
		})
	}
}

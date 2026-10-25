package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A subquery binding spelled as the tuple form, [?a ?b], is an expression's
// destructuring and no subquery binding
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.5), and its refusal names the
// repair, the relation form over the author's own symbols, [[?a ?b] ...]
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md goal 6).
func TestParse_ATupleSpelledSubqueryBindingNamesTheRelationForm(t *testing.T) {
	cases := []struct {
		name, text, repair string
	}{
		{
			name:   "two symbols",
			text:   `[:find ?a :where [?e :t/n ?n] [(q [:find ?x ?y :in $ ?n :where [?x :t/m ?n] [?x :t/k ?y]] $ ?n) [?a ?b]]]`,
			repair: "[[?a ?b] ...]",
		},
		{
			name:   "one symbol",
			text:   `[:find ?a :where [?e :t/n ?n] [(q [:find ?x :in $ ?n :where [?x :t/m ?n]] $ ?n) [?a]]]`,
			repair: "[[?a] ...]",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseQuery(tc.text)
			require.ErrorContains(t, err, tc.repair, "the refusal names the relation form over the author's symbols")
		})
	}

	t.Run("the relation form over the same symbols is accepted", func(t *testing.T) {
		_, err := ParseQuery(`[:find ?a :where [?e :t/n ?n] [(q [:find ?x ?y :in $ ?n :where [?x :t/m ?n] [?x :t/k ?y]] $ ?n) [[?a ?b] ...]]]`)
		require.NoError(t, err)
	})
}

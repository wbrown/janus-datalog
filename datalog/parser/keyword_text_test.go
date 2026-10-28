package parser

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A database function's attribute given as a string is read as the keyword
// it names, and a string that is no keyword, one with no name after the colon
// or one holding a space, is refused by the text boundary with an error
// naming the text, never by a panic, on every route that reads the string:
// get-else, missing? as a function and as a predicate, get-some, through
// ParseQuery and through a rule body under ParseRules.
func TestParseQueryRefusesAStringAttributeThatIsNoKeyword(t *testing.T) {
	routes := []struct {
		name  string
		query func(attribute string) string
	}{
		{"get-else", func(a string) string {
			return fmt.Sprintf(`[:find ?v :where [?e :t/name _] [(get-else $ ?e %q "none") ?v]]`, a)
		}},
		{"missing? as a function", func(a string) string {
			return fmt.Sprintf(`[:find ?m :where [?e :t/name _] [(missing? $ ?e %q) ?m]]`, a)
		}},
		{"missing? as a predicate", func(a string) string {
			return fmt.Sprintf(`[:find ?e :where [?e :t/name _] [(missing? $ ?e %q)]]`, a)
		}},
		{"get-some", func(a string) string {
			return fmt.Sprintf(`[:find ?a ?v :where [?e :t/name _] [(get-some $ ?e %q) [?a ?v]]]`, a)
		}},
	}
	for _, r := range routes {
		for _, attribute := range []string{":t/first name", ":"} {
			t.Run(r.name+" given "+strconv.Quote(attribute), func(t *testing.T) {
				text := r.query(attribute)
				var err error
				require.NotPanics(t, func() { _, err = ParseQuery(text) }, text)
				require.Error(t, err, "%s is refused", text)
				require.ErrorContains(t, err, strconv.Quote(attribute), "the refusal names the text: %s", text)
			})
		}
	}

	t.Run("inside a rule body", func(t *testing.T) {
		text := `[[(named ?e ?v) [?e :t/name _] [(get-else $ ?e ":t/first name" "none") ?v]]]`
		var err error
		require.NotPanics(t, func() { _, err = ParseRules(text) }, text)
		require.Error(t, err, "%s is refused", text)
		require.ErrorContains(t, err, strconv.Quote(":t/first name"), "the refusal names the text: %s", text)
	})

	t.Run("a string naming a keyword is read as it", func(t *testing.T) {
		q, err := ParseQuery(`[:find ?v :where [?e :t/name _] [(get-else $ ?e ":t/name" "none") ?v]]`)
		require.NoError(t, err)
		expression, ok := q.Where[1].(*query.Expression)
		require.True(t, ok, "the clause parses to %T, want an expression", q.Where[1])
		getElse, ok := expression.Function.(*query.GetElseFunction)
		require.True(t, ok, "the expression's function is %T, want get-else", expression.Function)
		require.Equal(t, datalog.NewKeyword(":t/name"), getElse.Attr)
	})
}

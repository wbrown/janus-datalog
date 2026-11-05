package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A database function's first argument is the source it reads, $ or a named
// source, as in Datomic's grammar (NESTED_SCOPES_AS_QUERIES.md goal 12). The
// query renders it and parses back to the same query.
func TestDatabaseFunction_ParsesTheSourceItNames(t *testing.T) {
	texts := []struct{ name, text, form string }{
		{"get-else", `[:find ?v :in $ $src :where [?e :t/n _] [(get-else $src ?e :t/a "d") ?v]]`, `(get-else $src ?e :t/a "d")`},
		{"get-some", `[:find ?v :in $ $src :where [?e :t/n _] [(get-some $src ?e :t/a :t/b) [?attr ?v]]]`, `(get-some $src ?e :t/a :t/b)`},
		{"missing? bound", `[:find ?m :in $ $src :where [?e :t/n _] [(missing? $src ?e :t/a) ?m]]`, `(missing? $src ?e :t/a)`},
		{"missing? as a predicate", `[:find ?e :in $ $src :where [?e :t/n _] [(missing? $src ?e :t/a)]]`, `(missing? $src ?e :t/a)`},
	}
	for _, tc := range texts {
		t.Run(tc.name, func(t *testing.T) {
			q, err := ParseQuery(tc.text)
			require.NoError(t, err)
			rendered := q.String()
			require.Contains(t, rendered, tc.form, "the rendering names the source the function reads")
			back, err := ParseQuery(rendered)
			require.NoError(t, err)
			require.True(t, q.Equal(back), "the rendering parses back to the same query")
			require.Equal(t, q.Hash(), back.Hash())
		})
	}
}

// Two texts that differ only in a database function's source are two queries.
func TestDatabaseFunction_SourceDistinguishesTwoQueries(t *testing.T) {
	onDefault, err := ParseQuery(`[:find ?v :in $ $src :where [?e :t/n _] [(get-else $ ?e :t/a "d") ?v]]`)
	require.NoError(t, err)
	onSrc, err := ParseQuery(`[:find ?v :in $ $src :where [?e :t/n _] [(get-else $src ?e :t/a "d") ?v]]`)
	require.NoError(t, err)
	require.False(t, onDefault.Equal(onSrc))
	require.NotEqual(t, onDefault.Hash(), onSrc.Hash())
}

// A first argument that names no source is refused, naming what it found.
func TestDatabaseFunction_FirstArgumentNamesASource(t *testing.T) {
	for _, text := range []string{
		`[:find ?v :where [?e :t/n _] [(get-else ?x ?e :t/a "d") ?v]]`,
		`[:find ?v :where [?e :t/n _] [(get-some :t/x ?e :t/a) ?v]]`,
		`[:find ?e :where [?e :t/n _] [(missing? "src" ?e :t/a)]]`,
	} {
		_, err := ParseQuery(text)
		require.Error(t, err, "query: %s", text)
		require.Contains(t, err.Error(), "source", "query: %s", text)
	}
}

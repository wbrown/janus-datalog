package qb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A database function built on a named source builds the query its EDN form
// parses to, as PatFrom does for a data pattern.
func TestDatabaseFunctionFrom_BuildsTheQueryTheEDNParsesTo(t *testing.T) {
	src := Source("$src")
	e, x, v, m, at := NewVar("e"), NewVar("x"), NewVar("v"), NewVar("m"), NewVar("at")
	n, a := Kw(":t/n"), Kw(":t/a")
	cases := []struct {
		name  string
		built func() (*query.Query, error)
		text  string
	}{
		{
			"get-some binding the attribute and the value",
			func() (*query.Query, error) {
				return Query().Find(v).In(src).Where(Pat(e, n, x), GetSomeFrom(src, e, a, n).As(at, v)).Build()
			},
			`[:find ?v :in $src :where [?e :t/n ?x] [(get-some $src ?e :t/a :t/n) [?at ?v]]]`,
		},
		{
			"get-some on $",
			func() (*query.Query, error) {
				return Query().Find(v).Where(Pat(e, n, x), GetSome(e, a, n).As(at, v)).Build()
			},
			`[:find ?v :where [?e :t/n ?x] [(get-some $ ?e :t/a :t/n) [?at ?v]]]`,
		},
		{
			"get-else",
			func() (*query.Query, error) {
				return Query().Find(v).In(src).Where(Pat(e, n, x), GetElseFrom(src, e, a, "d").As(v)).Build()
			},
			`[:find ?v :in $src :where [?e :t/n ?x] [(get-else $src ?e :t/a "d") ?v]]`,
		},
		{
			"missing? bound",
			func() (*query.Query, error) {
				return Query().Find(m).In(src).Where(Pat(e, n, x), MissingFrom(src, e, a).As(m)).Build()
			},
			`[:find ?m :in $src :where [?e :t/n ?x] [(missing? $src ?e :t/a) ?m]]`,
		},
		{
			"missing? as a predicate",
			func() (*query.Query, error) {
				return Query().Find(e).In(src).Where(Pat(e, n, x), MissingFrom(src, e, a)).Build()
			},
			`[:find ?e :in $src :where [?e :t/n ?x] [(missing? $src ?e :t/a)]]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			built, err := tc.built()
			require.NoError(t, err)
			parsed, err := parser.ParseQuery(tc.text)
			require.NoError(t, err)
			require.True(t, built.Equal(parsed), "built %s, parsed %s", built, parsed)
		})
	}
}

package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// plainDatabaseHolding opens a database refusing nothing in which holder
// carries the :t/string value refusingDB gives it.
func plainDatabaseHolding(t *testing.T, mode optimizerMode, holder datalog.Identity) *Database {
	d := createOptimizerModeDB(t, mode, DatabaseOptions{})
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(holder, kw(":t/string"), "held"))
	_, err := tx.Commit()
	require.NoError(t, err)
	return d
}

// A refusing database passed as $other to a plain database's query, the
// query's :in declaring $other or not: each reading of the undeclared
// attribute :t/strnig on $other is refused
// (docs/reference/SCHEMA.md, "Schema as Facts": the attributes of get-else,
// get-some and missing? are checked by the source each function names, and a
// database passed as a named source refuses as it does as $).
func TestRefuteParse_UndeclaredSourceIsAskedAboutItsAttributes(t *testing.T) {
	cases := []struct {
		name  string
		text  string
		rules string
	}{
		{"data pattern", `[:find ?v :in $ ?e :where [$other ?e :t/strnig ?v]]`, ""},
		{"get-else", `[:find ?v :in $ ?e :where [(get-else $other ?e :t/strnig "x") ?v]]`, ""},
		{"missing? predicate", `[:find ?e :in $ ?e :where [(missing? $other ?e :t/strnig)]]`, ""},
		{"get-some", `[:find ?v :in $ ?e :where [(get-some $other ?e :t/strnig) [?at ?v]]]`, ""},
		{"get-else in an or-join", `[:find ?v :in $ ?e :where (or-join [[?e] ?v] [(get-else $other ?e :t/strnig "x") ?v] [?e :t/string ?v])]`, ""},
		{"missing? in a not-join", `[:find ?e :in $ ?e :where [?e :t/string _] (not-join [?e] [(missing? $other ?e :t/strnig)])]`, ""},
		{"data pattern in a subquery body", `[:find ?v :in $ ?e :where [(q [:find ?x :in $ ?e :where [$other ?e :t/strnig ?x]] $ ?e) [[?v]]]]`, ""},
		{"get-else in a subquery body", `[:find ?v :in $ ?e :where [(q [:find ?x :in $ ?e :where [(get-else $other ?e :t/strnig "x") ?x]] $ ?e) [[?v]]]]`, ""},
		{"data pattern in a rule body", `[:find ?v :in $ ?e % :where (rv ?e ?v)]`, `[[(rv ?e ?v) [$other ?e :t/strnig ?v]]]`},
		{"get-else in a rule body", `[:find ?v :in $ ?e % :where (rv ?e ?v)]`, `[[(rv ?e ?v) [?e :t/string _] [(get-else $other ?e :t/strnig "x") ?v]]]`},
		{"missing? in a rule body", `[:find ?e :in $ ?e % :where (rm ?e)]`, `[[(rm ?e) [?e :t/string _] [(missing? $other ?e :t/strnig)]]]`},
	}
	// Each case runs with $other undeclared, as written, and declared in :in.
	headers := []struct{ name, in string }{
		{"undeclared", ":in $ ?e"},
		{"declared", ":in $ $other ?e"},
	}
	for _, mode := range legsFor(optimizerModes, algebraOption) {
		for _, header := range headers {
			for _, tc := range cases {
				t.Run(mode.name+"/"+header.name+"/"+tc.name, func(t *testing.T) {
					refusing, holder := refusingDB(t, mode)
					plain := plainDatabaseHolding(t, mode, holder)
					text := strings.Replace(tc.text, ":in $ ?e", header.in, 1)
					inputs := []interface{}{holder}
					if tc.rules != "" {
						inputs = append(inputs, tc.rules)
					}
					inputs = append(inputs, WithSources(map[datalog.Symbol]executor.PatternMatcher{datalog.NewSymbol("$other"): refusing}))
					tuples, err := executor.CollectTuples(plain.Query(text, inputs...))
					requireUnknown(t, err, kw(":t/strnig"))
					require.Empty(t, tuples, "a refused query answers nothing, got %v", tuples)
				})
			}
		}
	}
}

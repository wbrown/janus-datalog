package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A hand-built query holding a shape the parser refuses is refused by Query
// with the parser's message, in both optimizer modes: a data pattern of five
// elements, an or with a branch of no clause, an or-default with one branch,
// and a negative limit.
func TestQuery_RefusesTheShapesTheParserRefuses(t *testing.T) {
	e, v, w := datalog.NewSymbol("?e"), datalog.NewSymbol("?v"), datalog.NewSymbol("?w")
	n, m := datalog.NewKeyword(":t/n"), datalog.NewKeyword(":t/m")
	first := &query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: e}, query.Constant{Value: n}, query.Variable{Name: v}}}
	second := &query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: e}, query.Constant{Value: m}, query.Variable{Name: w}}}
	find := []query.FindElement{query.FindVariable{Symbol: e}}
	limit := -1
	shapes := []struct {
		name  string
		q     *query.Query
		names string
	}{
		{"a data pattern of five elements",
			&query.Query{Find: find, Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: e}, query.Constant{Value: n}, query.Variable{Name: v}, query.Variable{Name: w}, query.Variable{Name: w}}}}},
			"data pattern must have 1 to 4 elements, got 5"},
		{"an or with a branch of no clause",
			&query.Query{Find: find, Where: []query.Clause{&query.OrClause{Branches: [][]query.Clause{{}, {first}}}}},
			"or branch 1 must have at least one clause"},
		{"an or-default with one branch",
			&query.Query{Find: find, Where: []query.Clause{first, &query.OrDefaultClause{Branches: [][]query.Clause{{second}}}}},
			"or-default clause must have at least two branches"},
		{"a negative limit",
			&query.Query{Find: find, Where: []query.Clause{first}, Limit: &limit},
			":limit must be non-negative, got -1"},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, []fact{{named("e1"), ":t/n", int64(1)}, {named("e1"), ":t/m", int64(2)}})
			for _, s := range shapes {
				t.Run(s.name, func(t *testing.T) {
					_, err := d.Query(s.q)
					require.ErrorContains(t, err, s.names)
				})
			}
		})
	}
}

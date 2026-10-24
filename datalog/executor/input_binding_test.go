package executor

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An input relation binds the query's :in symbols from its own symbols of
// those names, and every bound input is a relation the query joins
// (docs/proposals/RELATION_INPUT_PROOF.md §8): two relation inputs are
// joined, a symbol written twice in a tuple or relation input unifies, a
// relation whose symbols are permuted answers as the :in names them and one
// lacking a symbol is the query's error, and a symbol written twice in an
// expression binding unifies under every form.

// bindingEntities are two entities, x at :a 1 and :b 10, and y at :a 2 and
// :b 20, as datoms and as names by identity.
func bindingEntities() ([]datalog.Datom, map[datalog.Identity]string) {
	x, y := datalog.NewIdentity("x"), datalog.NewIdentity("y")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: x, A: datalog.NewKeyword(":a"), V: int64(1), Tx: tx},
		{E: x, A: datalog.NewKeyword(":b"), V: int64(10), Tx: tx},
		{E: y, A: datalog.NewKeyword(":a"), V: int64(2), Tx: tx},
		{E: y, A: datalog.NewKeyword(":b"), V: int64(20), Tx: tx},
	}
	return datoms, map[datalog.Identity]string{x: "x", y: "y"}
}

// renderedTuples reads a result into one string per tuple, an identity by
// its name in names and any other value by fmt, sorted.
func renderedTuples(t *testing.T, rel Relation, names map[datalog.Identity]string) []string {
	t.Helper()
	var rows []string
	require.NoError(t, ForEach(rel, func(tuple Tuple) error {
		parts := make([]string, len(tuple))
		for i, v := range tuple {
			if id, ok := v.(datalog.Identity); ok {
				parts[i] = names[id]
			} else {
				parts[i] = fmt.Sprint(v)
			}
		}
		rows = append(rows, strings.Join(parts, " "))
		return nil
	}))
	sort.Strings(rows)
	return rows
}

// relationOver is a materialized relation over the named symbols holding
// the tuples.
func relationOver(symbols []string, tuples ...Tuple) Relation {
	syms := make([]query.Symbol, len(symbols))
	for i, s := range symbols {
		syms[i] = datalog.NewSymbol(s)
	}
	return NewMaterializedRelation(syms, tuples)
}

// parsedQuery parses text or fails the test.
func parsedQuery(t *testing.T, text string) *query.Query {
	t.Helper()
	q, err := parser.ParseQuery(text)
	require.NoError(t, err, text)
	return q
}

// Two relation inputs are the join of the two, a product when they share
// no symbol: over [?j ?v] and [?f ?e] the pattern on ?j keeps both values of
// ?j beside the one ?f; over [?j ?v] and [?v ?f] the join on ?v keeps the ?j
// whose ?v the second relation holds.
func TestRelationInputs_TwoAreJoined(t *testing.T) {
	datoms, names := bindingEntities()
	matcher := NewMemoryPatternMatcher(datoms)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			first := relationOver([]string{"?j", "?v"}, Tuple{int64(1), int64(10)}, Tuple{int64(2), int64(20)})
			var rows []string
			require.NotPanics(t, func() {
				rel, err := exec.ExecuteWithRelations(NewContext(), parsedQuery(t, `[:find ?j ?f :in $ [[?j ?v] ...] [[?f ?e] ...] :where [?x :a ?j]]`),
					[]Relation{first, relationOver([]string{"?f", "?e"}, Tuple{int64(7), int64(70)})})
				require.NoError(t, err)
				rows = renderedTuples(t, rel, names)
			})
			require.Equal(t, []string{"1 7", "2 7"}, rows, "sharing no symbol, the product")
			require.NotPanics(t, func() {
				rel, err := exec.ExecuteWithRelations(NewContext(), parsedQuery(t, `[:find ?j ?f :in $ [[?j ?v] ...] [[?v ?f] ...] :where [?x :a ?j]]`),
					[]Relation{first, relationOver([]string{"?v", "?f"}, Tuple{int64(10), int64(7)}, Tuple{int64(30), int64(9)})})
				require.NoError(t, err)
				rows = renderedTuples(t, rel, names)
			})
			require.Equal(t, []string{"1 7"}, rows, "sharing ?v, the join")
		})
	}
}

// A symbol written twice in an input unifies, selecting the tuples whose
// values agree: the tuple input [?w ?w] given 1 and 2 binds nothing and
// given 1 and 1 binds 1, and the relation input [[?w ?w] ...] given (1 2) and
// (2 2) binds 2 alone.
func TestInput_ARepeatedSymbolUnifies(t *testing.T) {
	datoms, names := bindingEntities()
	matcher := NewMemoryPatternMatcher(datoms)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			tupleQuery := parsedQuery(t, `[:find ?x :in $ [?w ?w] :where [?x :a ?w]]`)
			var rows []string
			require.NotPanics(t, func() {
				rel, err := exec.ExecuteWithRelations(NewContext(), tupleQuery, []Relation{relationOver([]string{"?w", "?w"}, Tuple{int64(1), int64(2)})})
				require.NoError(t, err)
				rows = renderedTuples(t, rel, names)
			})
			require.Empty(t, rows, "a tuple whose two values differ binds nothing")
			require.NotPanics(t, func() {
				rel, err := exec.ExecuteWithRelations(NewContext(), tupleQuery, []Relation{relationOver([]string{"?w", "?w"}, Tuple{int64(1), int64(1)})})
				require.NoError(t, err)
				rows = renderedTuples(t, rel, names)
			})
			require.Equal(t, []string{"x"}, rows, "a tuple whose two values agree binds them")
			require.NotPanics(t, func() {
				rel, err := exec.ExecuteWithRelations(NewContext(), parsedQuery(t, `[:find ?x :in $ [[?w ?w] ...] :where [?x :a ?w]]`),
					[]Relation{relationOver([]string{"?w", "?w"}, Tuple{int64(1), int64(2)}, Tuple{int64(2), int64(2)})})
				require.NoError(t, err)
				rows = renderedTuples(t, rel, names)
			})
			require.Equal(t, []string{"y"}, rows, "of a relation's tuples, those whose two values agree bind")
		})
	}
}

// A relation given to ExecuteWithRelations binds each :in symbol from the
// relation's own symbol of that name: a tuple or relation input whose
// relation is permuted answers as the :in names them, and a relation lacking
// a symbol the :in names is the query's error.
func TestExecuteWithRelations_BindsARelationByItsSymbols(t *testing.T) {
	datoms, names := bindingEntities()
	matcher := NewMemoryPatternMatcher(datoms)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			permuted := relationOver([]string{"?q", "?p"}, Tuple{int64(10), int64(1)})
			rel, err := exec.ExecuteWithRelations(NewContext(), parsedQuery(t, `[:find ?x :in $ [?p ?q] :where [?x :a ?p] [?x :b ?q]]`), []Relation{permuted})
			require.NoError(t, err)
			require.Equal(t, []string{"x"}, renderedTuples(t, rel, names), "a permuted tuple input")
			rel, err = exec.ExecuteWithRelations(NewContext(), parsedQuery(t, `[:find ?x :in $ [[?p ?q] ...] :where [?x :a ?p] [?x :b ?q]]`), []Relation{permuted})
			require.NoError(t, err)
			require.Equal(t, []string{"x"}, renderedTuples(t, rel, names), "a permuted relation input")
			lacking := relationOver([]string{"?p"}, Tuple{int64(1)})
			_, err = exec.ExecuteWithRelations(NewContext(), parsedQuery(t, `[:find ?x :in $ [?p ?q] :where [?x :a ?p] [?x :b ?q]]`), []Relation{lacking})
			require.Error(t, err, "a tuple input's relation lacking ?q")
			_, err = exec.ExecuteWithRelations(NewContext(), parsedQuery(t, `[:find ?x :in $ [[?p ?q] ...] :where [?x :a ?p] [?x :b ?q]]`), []Relation{lacking})
			require.Error(t, err, "a relation input's relation lacking ?q")
		})
	}
}

// A symbol written twice in an expression binding unifies, selecting the
// tuples whose values agree, under the tuple form and the relation form, at
// the top level, inside a branch and inside a subquery: a ground of 1 and 2
// into [?a ?a] binds nothing, and a ground of 1 and 1 binds 1 beside every
// entity.
func TestExpressionBinding_ARepeatedSymbolUnifies(t *testing.T) {
	datoms, names := bindingEntities()
	matcher := NewMemoryPatternMatcher(datoms)
	shapes := []struct {
		name, text string
		want       []string
	}{
		{"tuple form, differing", `[:find ?x ?a :where [?x :a _] [(ground [1 2]) [?a ?a]]]`, nil},
		{"tuple form, agreeing", `[:find ?x ?a :where [?x :a _] [(ground [1 1]) [?a ?a]]]`, []string{"x 1", "y 1"}},
		{"relation form", `[:find ?x ?a :where [?x :a _] [(ground [[1 2] [3 3]]) [[?a ?a] ...]]]`, []string{"x 3", "y 3"}},
		{"inside a branch, differing", `[:find ?x ?a :where [?x :a _] (or-join [[?x] ?a] [(ground [1 2]) [?a ?a]])]`, nil},
		{"inside a branch, agreeing", `[:find ?x ?a :where [?x :a _] (or-join [[?x] ?a] [(ground [1 1]) [?a ?a]])]`, []string{"x 1", "y 1"}},
		{"inside a subquery, differing", `[:find ?x ?a :where [?x :a _] [(q [:find ?v :where [(ground [1 2]) [?v ?v]]] $) [[?a] ...]]]`, nil},
		{"inside a subquery, agreeing", `[:find ?x ?a :where [?x :a _] [(q [:find ?v :where [(ground [1 1]) [?v ?v]]] $) [[?a] ...]]]`, []string{"x 1", "y 1"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
			for _, shape := range shapes {
				rel, err := exec.Execute(parsedQuery(t, shape.text))
				require.NoError(t, err, shape.name)
				rows := renderedTuples(t, rel, names)
				if shape.want == nil {
					require.Empty(t, rows, shape.name)
					continue
				}
				require.Equal(t, shape.want, rows, shape.name)
			}
		})
	}
}

package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// reviewRouteSpelling is how a query names the in-memory source: at the head
// a named source, $m, declared beside $.
var reviewRouteSpelling = struct {
	sym string // the symbol the source is passed under
	in  string // the :in sources
}{sym: "$m", in: "$ $m"}

// A get-else on a memory database answers, for an entity holding two
// values at an attribute no schema declares, the value of the latest
// transaction, the rule storage's lookup applies to such an attribute
// (PatternMatcher.LookupAttribute), whichever shape the query takes and
// whichever optimizer path runs it.
func TestReviewRoute_MemorySourceGetElseAnswersOneValueOnEveryRoute(t *testing.T) {
	one := datalog.NewIdentity("review-route:one")
	two := datalog.NewIdentity("review-route:two")
	facts := []datalog.Datom{
		{E: one, A: kw(":item/name"), V: "one", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: one, A: kw(":item/tag"), V: "red", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: one, A: kw(":item/tag"), V: "blue", Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
		{E: two, A: kw(":item/name"), V: "two", Tx: datalog.ElementID{Lamport: 4, ReplicaID: 1}},
	}
	src := reviewRouteSpelling.sym
	cases := []struct {
		name   string
		text   string
		inputs []interface{}
		want   [][]interface{}
	}{
		{
			name: "entity bound by a pattern on the source",
			text: fmt.Sprintf(`[:find ?n ?v :in %s :where [%s ?e :item/name ?n] [(get-else %s ?e :item/tag "none") ?v]]`,
				reviewRouteSpelling.in, src, src),
			want: [][]interface{}{{"one", "blue"}, {"two", "none"}},
		},
		{
			name: "entity bound by the :in",
			text: fmt.Sprintf(`[:find ?v :in %s ?e :where [(get-else %s ?e :item/tag "none") ?v]]`,
				reviewRouteSpelling.in, src),
			inputs: []interface{}{one},
			want:   [][]interface{}{{"blue"}},
		},
		{
			name: "inside a subquery the source is passed to",
			text: fmt.Sprintf(`[:find ?n ?v :in %s :where [%s ?e :item/name ?n] [(q [:find ?x :in $ ?e :where [$ ?e :item/name _] [(get-else $ ?e :item/tag "none") ?x]] %s ?e) [[?v]]]]`,
				reviewRouteSpelling.in, src, src),
			want: [][]interface{}{{"one", "blue"}, {"two", "none"}},
		},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				memory := logDatabase(t, facts)
				inputs := append([]interface{}{WithSources(map[datalog.Symbol]executor.PatternMatcher{
					datalog.NewSymbol(src): memory,
				})}, c.inputs...)
				tuples, err := executor.CollectTuples(d.Query(c.text, inputs...))
				require.NoError(t, err, "query: %s", c.text)
				sort.Slice(tuples, func(i, j int) bool {
					return fmt.Sprint(tuples[i]) < fmt.Sprint(tuples[j])
				})
				require.Equal(t, c.want, tuples, "query: %s", c.text)
			})
		}
	}
}

// A database function on a slice source, a database holding the slice's
// facts, answers from those facts on every optimizer path, here with every
// entity holding :item/tag: its tag, the default at an attribute no item
// holds, and no entity missing the tag.
func TestReviewRoute_SliceSourceDatabaseFunctionsAnswerTheSlicesFacts(t *testing.T) {
	type item struct {
		Name string
		Tag  string
	}
	items := []item{{Name: "one", Tag: "red"}, {Name: "two", Tag: "blue"}}
	src := reviewRouteSpelling.sym
	cases := []struct {
		name string
		text string
		want [][]interface{}
	}{
		{
			name: "get-else",
			text: fmt.Sprintf(`[:find ?n ?v :in %s :where [%s ?e :item/name ?n] [(get-else %s ?e :item/tag "none") ?v]]`,
				reviewRouteSpelling.in, src, src),
			want: [][]interface{}{{"one", "red"}, {"two", "blue"}},
		},
		{
			name: "get-else on an attribute the AttributeSchema does not hold",
			text: fmt.Sprintf(`[:find ?n ?v :in %s :where [%s ?e :item/name ?n] [(get-else %s ?e :item/color "none") ?v]]`,
				reviewRouteSpelling.in, src, src),
			want: [][]interface{}{{"one", "none"}, {"two", "none"}},
		},
		{
			name: "missing?",
			text: fmt.Sprintf(`[:find ?n :in %s :where [%s ?e :item/name ?n] [(missing? %s ?e :item/tag)]]`,
				reviewRouteSpelling.in, src, src),
			want: [][]interface{}{},
		},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				slice := sliceDatabase(t, items, AttributeSchema[item]{
					kw(":item/name"): func(i item) any { return i.Name },
					kw(":item/tag"):  func(i item) any { return i.Tag },
				})
				tuples, err := executor.CollectTuples(d.Query(c.text, WithSources(map[datalog.Symbol]executor.PatternMatcher{
					datalog.NewSymbol(src): slice,
				})))
				require.NoError(t, err, "query: %s", c.text)
				require.ElementsMatch(t, c.want, tuples, "query: %s", c.text)
			})
		}
	}
}

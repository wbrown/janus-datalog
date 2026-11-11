package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

// A get-else on a slice source, a database holding the slice's facts, answers
// each item's value on every optimizer path and every spelling, here with
// every item holding the attribute.
func TestRefuteRoute_SliceSourceGetElseAnswersTheSlicesFact(t *testing.T) {
	type item struct {
		Name string
		Tag  string
	}
	items := []item{{Name: "one", Tag: "red"}, {Name: "two", Tag: "blue"}}
	s := qb.Source("$s")
	e, n, v := qb.NewVar("e"), qb.NewVar("n"), qb.NewVar("v")
	cases := []struct {
		name  string
		query interface{}
	}{
		{
			name:  "text, entity bound by a pattern on the source",
			query: `[:find ?n ?v :in $ $s :where [$s ?e :item/name ?n] [(get-else $s ?e :item/tag "none") ?v]]`,
		},
		{
			name:  "text, inside a subquery the source is passed to as its $",
			query: `[:find ?n ?v :in $ $s :where [$s ?e :item/name ?n] [(q [:find ?x :in $ ?e :where [$ ?e :item/name _] [(get-else $ ?e :item/tag "none") ?x]] $s ?e) [[?v]]]]`,
		},
		{
			name: "qb GetElseFrom",
			query: qb.Query().Find(n, v).In(qb.DB, s).Where(
				qb.PatFrom(s, e, qb.Kw(":item/name"), n),
				qb.GetElseFrom(s, e, qb.Kw(":item/tag"), "none").As(v),
			).MustBuild(),
		},
	}
	for _, mode := range legsFor(optimizerModes, algebraOption) {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				slice := sliceDatabase(t, items, AttributeSchema[item]{
					kw(":item/name"): func(i item) any { return i.Name },
					kw(":item/tag"):  func(i item) any { return i.Tag },
				})
				tuples, err := executor.CollectTuples(d.Query(c.query, sourceNamed("$s", slice)))
				require.NoError(t, err, "query %v", c.query)
				sort.Slice(tuples, func(i, j int) bool { return fmt.Sprint(tuples[i]) < fmt.Sprint(tuples[j]) })
				require.Equal(t, [][]interface{}{{"one", "red"}, {"two", "blue"}}, tuples, "query %v", c.query)
			})
		}
	}
}

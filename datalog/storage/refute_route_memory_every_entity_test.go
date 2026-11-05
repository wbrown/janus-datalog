package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A get-else on a memory source passed as $, every entity holding the
// attribute, answers on each optimizer path what the other answers.
func TestRefuteRoute_MemorySourceGetElseEveryEntityHoldsTheAttribute(t *testing.T) {
	one := datalog.NewIdentity("refute-route:one")
	two := datalog.NewIdentity("refute-route:two")
	facts := []datalog.Datom{
		{E: one, A: kw(":item/name"), V: "one", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: one, A: kw(":item/tag"), V: "red", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: one, A: kw(":item/tag"), V: "blue", Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
		{E: two, A: kw(":item/name"), V: "two", Tx: datalog.ElementID{Lamport: 4, ReplicaID: 1}},
		{E: two, A: kw(":item/tag"), V: "green", Tx: datalog.ElementID{Lamport: 5, ReplicaID: 1}},
	}
	cases := []struct {
		name   string
		source string
		text   string
		want   [][]interface{}
	}{
		{
			name:   "entity bound by a pattern on the source, as $m",
			source: "$m",
			text:   `[:find ?n ?v :in $ $m :where [$m ?e :item/name ?n] [(get-else $m ?e :item/tag "none") ?v]]`,
			want:   [][]interface{}{{"one", "blue"}, {"two", "green"}},
		},
		{
			name:   "entity bound by a pattern on the source, as $",
			source: "$",
			text:   `[:find ?n ?v :in $ :where [$ ?e :item/name ?n] [(get-else $ ?e :item/tag "none") ?v]]`,
			want:   [][]interface{}{{"one", "blue"}, {"two", "green"}},
		},
	}
	cases = append(cases, struct {
		name   string
		source string
		text   string
		want   [][]interface{}
	}{
		name:   "inside a subquery the named source is passed to as its $",
		source: "$m",
		text:   `[:find ?n ?v :in $ $m :where [$m ?e :item/name ?n] [(q [:find ?x :in $ ?e :where [$ ?e :item/name _] [(get-else $ ?e :item/tag "none") ?x]] $m ?e) [[?v]]]]`,
		want:   [][]interface{}{{"one", "blue"}, {"two", "green"}},
	})
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				tuples, err := executor.CollectTuples(d.Query(c.text, sourceNamed(c.source, logDatabase(t, facts))))
				require.NoError(t, err, "query: %s", c.text)
				sort.Slice(tuples, func(i, j int) bool { return fmt.Sprint(tuples[i]) < fmt.Sprint(tuples[j]) })
				require.Equal(t, c.want, tuples, "query: %s", c.text)
			})
		}
	}
}

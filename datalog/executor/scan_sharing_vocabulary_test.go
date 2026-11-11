package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
)

// A database opened without RefuseUnknownAttributes refuses nothing under the
// wrappers the execution puts around it. A subquery whose input binds a
// pattern's attribute position asks the executor's matcher, wrapped for scan
// sharing and for annotation, before it runs; over such a database the
// subquery runs and reads the datoms the attribute carries.
func TestScanSharingMatcherOverADatabaseThatRefusesNothingRefusesNothing(t *testing.T) {
	name := datalog.NewKeyword(":t/name")
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e1"), A: name, V: "alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: datalog.NewIdentity("e2"), A: name, V: "bob", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}
	const text = `[:find ?v :in $ ?a :where [(q [:find ?v :in $ ?attr :where [?e ?attr ?v]] $ ?a) [[?v] ...]]]`
	for _, mode := range OptimizerModesWhere(func(m OptimizerMode) bool { return m.Sharing() }) {
		t.Run(mode.Name(), func(t *testing.T) {
			opts := mode.PlannerOptions()
			opts.Handler = func(annotations.Event) {}
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, opts)
			q := MustParse(t, text)
			inputs, err := CreateInputRelationsFromValuesWithOptions(q, []interface{}{datalog.SymDollar, name}, exec.Options())
			require.NoError(t, err)
			rel, err := exec.ExecuteWithRelations(NewContext(), q, inputs)
			require.NoError(t, err, "a subquery over a matcher without a vocabulary runs under the wrappers")
			var tuples []Tuple
			require.NoError(t, CollectTuplesInto(&tuples, rel))
			var values []string
			for _, tuple := range tuples {
				values = append(values, tuple[0].(string))
			}
			require.ElementsMatch(t, []string{"alice", "bob"}, values, "the subquery read the datoms the bound attribute carries")
		})
	}
}

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A database OpenFacts opens with RefuseUnknownAttributes and no schema
// either refuses at open or, where it opens, answers a query naming an
// attribute it holds. Each case is the facts and the query on an attribute
// one entity holds one value of.
func TestRefuteMemory_ARefusingFactsDatabaseAnswersAnAttributeItHolds(t *testing.T) {
	one := datalog.NewIdentity("refute-memory:one")
	two := datalog.NewIdentity("refute-memory:two")
	name, tag := kw(":refute-memory/name"), kw(":refute-memory/tag")
	cases := []struct {
		name  string
		facts []datalog.Datom
		want  [][]interface{}
	}{
		{"every entity holds one value",
			[]datalog.Datom{{E: one, A: name, V: "one"}, {E: one, A: tag, V: "red"}, {E: two, A: tag, V: "blue"}},
			[][]interface{}{{one, "one"}}},
		{"one entity holds two values",
			[]datalog.Datom{{E: one, A: name, V: "one"}, {E: one, A: tag, V: "red"}, {E: one, A: tag, V: "blue"}},
			[][]interface{}{{one, "one"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := OpenFacts(c.facts, DatabaseOptions{RefuseUnknownAttributes: true})
			if err != nil {
				return
			}
			defer func() { require.NoError(t, d.Close()) }()
			got, err := executor.CollectTuples(d.Query(`[:find ?e ?n :where [?e :refute-memory/name ?n]]`))
			require.NoError(t, err)
			require.ElementsMatch(t, c.want, got)
		})
	}
}

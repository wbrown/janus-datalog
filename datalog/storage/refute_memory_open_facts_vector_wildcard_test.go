package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A wildcard pull of an entity OpenFacts holds, given no schema, answers each
// attribute the entity holds, whether another entity holds a scalar or a
// vector at another attribute.
func TestRefuteMemory_AWildcardPullAnswersEveryAttributeBesideAVector(t *testing.T) {
	one := datalog.NewIdentity("refute-memory:one")
	two := datalog.NewIdentity("refute-memory:two")
	name, path := kw(":refute-memory/name"), kw(":refute-memory/path")
	cases := []struct {
		name  string
		facts []datalog.Datom
	}{
		{"another entity holds a scalar",
			[]datalog.Datom{{E: one, A: name, V: "one"}, {E: two, A: path, V: "a"}}},
		{"another entity holds a vector",
			[]datalog.Datom{{E: one, A: name, V: "one"}, {E: two, A: path, V: []interface{}{"a", "b"}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := OpenFacts(c.facts, DatabaseOptions{})
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()
			got, err := d.Pull(one, `[*]`)
			require.NoError(t, err)
			require.Equal(t, "one", got["refute-memory/name"])
		})
	}
}

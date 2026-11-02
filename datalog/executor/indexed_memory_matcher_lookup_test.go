package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// cardinalityOneSchema declares each of attrs a cardinality-one attribute.
func cardinalityOneSchema(t *testing.T, attrs ...string) *schema.Schema {
	t.Helper()
	b := schema.NewBuilder()
	for _, attr := range attrs {
		b = b.Attribute(attr).One().Add()
	}
	s, err := b.Build()
	require.NoError(t, err)
	return s
}

// A memory matcher looks up an entity's attribute as storage does an
// attribute no schema declares otherwise: the value of its datom with the
// latest transaction, under any source name. A same-entity fetch fuses into
// a lookup only for an attribute the matcher's schema declares
// cardinality-one and each entity holds once, where the lookup answers what a
// match does: a matcher with no schema fuses nothing, as storage's does.
func TestMemoryPatternMatcher_LooksUpTheLatestValueAndFusesADeclaredAttribute(t *testing.T) {
	e1, e2 := datalog.NewIdentity("memory-lookup:e1"), datalog.NewIdentity("memory-lookup:e2")
	a, b, c := datalog.NewKeyword(":t/a"), datalog.NewKeyword(":t/b"), datalog.NewKeyword(":t/c")
	first, second := datalog.ElementID{Lamport: 1, ReplicaID: 1}, datalog.ElementID{Lamport: 2, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: e1, A: a, V: "new", Tx: second},
		{E: e1, A: a, V: "old", Tx: first},
		{E: e2, A: b, V: "only", Tx: first},
		{E: e2, A: c, V: "undeclared", Tx: first},
	}

	schemaless := NewIndexedMemoryMatcher(datoms)
	value, found, err := schemaless.LookupAttribute(e2, b)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "only", value, "a memory matcher with no schema looks up the latest value")
	require.False(t, schemaless.CanFuseAttributeFetch(datalog.SymDollar, b), "a memory matcher with no schema fuses nothing")

	m := NewIndexedMemoryMatcher(datoms).WithSchema(cardinalityOneSchema(t, ":t/a", ":t/b"))
	value, found, err = m.LookupAttribute(e1, a)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "new", value, "the datom with the latest transaction answers")
	value, found, err = m.LookupAttribute(e2, a)
	require.NoError(t, err)
	require.False(t, found, "an attribute the entity does not hold is absent")
	require.Nil(t, value)
	value, found, err = m.LookupAttribute(e2, c)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "undeclared", value, "an attribute the schema does not declare is looked up as storage looks it up")

	other, err := m.LookupSource(datalog.NewSymbol("$other"))
	require.NoError(t, err)
	value, found, err = other.LookupAttribute(e2, b)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "only", value, "the lookup of any source name is the matcher's")

	require.True(t, m.CanFuseAttributeFetch(datalog.SymDollar, b), "each entity holds :t/b once")
	require.False(t, m.CanFuseAttributeFetch(datalog.SymDollar, a), "an entity holds :t/a twice")
	require.False(t, m.CanFuseAttributeFetch(datalog.SymDollar, c), "the schema does not declare :t/c")
}

// A memory matcher takes a prefetch, building the index its lookups read,
// and reports each lookup through its handler as a storage read reports one:
// the entity, the attribute and the values served.
func TestMemoryPatternMatcher_TakesAPrefetchAndReportsItsLookups(t *testing.T) {
	e1 := datalog.NewIdentity("memory-prefetch:e1")
	a, b := datalog.NewKeyword(":t/a"), datalog.NewKeyword(":t/b")
	var lookups []annotations.Event
	m := NewIndexedMemoryMatcher([]datalog.Datom{
		{E: e1, A: a, V: "held", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	}).WithSchema(cardinalityOneSchema(t, ":t/a", ":t/b")).WithOptions(ExecutorOptions{Handler: func(e annotations.Event) {
		if e.Name == annotations.StorageResolveComplete {
			lookups = append(lookups, e)
		}
	}})

	prefetcher, ok := PatternMatcher(m).(EntityPrefetcher)
	require.True(t, ok, "a memory matcher takes a prefetch")
	require.NoError(t, prefetcher.PrefetchEntities([]datalog.Identity{e1}))

	_, found, err := m.LookupAttribute(e1, a)
	require.NoError(t, err)
	require.True(t, found)
	_, found, err = m.LookupAttribute(e1, b)
	require.NoError(t, err)
	require.False(t, found)

	require.Len(t, lookups, 2, "each lookup reports once")
	require.Equal(t, e1, lookups[0].Data[annotations.KeyEntity])
	require.Equal(t, a, lookups[0].Data[annotations.KeyAttribute])
	require.Equal(t, 1, lookups[0].Data[annotations.KeyValuesServed], "a held attribute serves its value")
	require.Equal(t, b, lookups[1].Data[annotations.KeyAttribute])
	require.Equal(t, 0, lookups[1].Data[annotations.KeyValuesServed], "an absent attribute serves nothing")
}

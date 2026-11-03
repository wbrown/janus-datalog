package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// factsDatabase opens a memory database holding facts, every one of which
// holds (storage.OpenFacts), and closes it at the test's end.
func factsDatabase(t testing.TB, facts []datalog.Datom) *storage.Database {
	t.Helper()
	d, err := storage.OpenFacts(facts, storage.DatabaseOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

// refusingDatabase opens a memory database whose schema declares the
// attributes given, cardinality-one, and that refuses every other attribute
// (RefuseUnknownAttributes), and closes it at the test's end.
func refusingDatabase(t testing.TB, declared ...datalog.Keyword) *storage.Database {
	t.Helper()
	s := schema.NewSchema()
	for _, a := range declared {
		s.Add(&schema.AttributeDefinition{Ident: a, Cardinality: schema.CardinalityOne})
	}
	d, err := storage.OpenFacts(nil, storage.DatabaseOptions{Schema: s, RefuseUnknownAttributes: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

// sliceDatabase opens the memory database holding a slice's facts under an
// attribute schema (storage.NewSliceSource), and closes it at the test's end.
func sliceDatabase[T any](t testing.TB, items []T, schema storage.AttributeSchema[T]) *storage.Database {
	t.Helper()
	d, err := storage.NewSliceSource(items, schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

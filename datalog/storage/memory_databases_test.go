package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// logDatabase opens a memory database holding a log, each datom with its own
// transaction and op, from the log's dump, and closes it at the test's end.
func logDatabase(t testing.TB, log []datalog.Datom) *Database {
	t.Helper()
	var dump strings.Builder
	for i := range log {
		dump.WriteString(FormatDatomEDN(&log[i]))
		dump.WriteByte('\n')
	}
	d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Dump: strings.NewReader(dump.String())})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

// factsDatabase opens a memory database holding facts, every one of which
// holds (OpenFacts), and closes it at the test's end.
func factsDatabase(t testing.TB, facts []datalog.Datom) *Database {
	t.Helper()
	d, err := OpenFacts(facts, DatabaseOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

// sliceDatabase opens the database holding a slice's facts under an attribute
// schema (NewSliceSource), and closes it at the test's end.
func sliceDatabase[T any](t testing.TB, items []T, schema AttributeSchema[T]) *Database {
	t.Helper()
	d, err := NewSliceSource(items, schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

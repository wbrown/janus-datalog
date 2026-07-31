package storage

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Reproducer for BUG_VECTOR_VALUES_DEGENERATE_ORDERING, end to end, because the
// defect is user-visible rather than internal. CompareValues had no vector case,
// so two vectors shared the unknown rank and compareByRank resolved them with
// strings.Compare of their rendered forms — "[10]" precedes "[2]", '1' preceding
// '2'. Ascending :order-by and (min ?v) over a cardinality-vector attribute both
// answered with the vector holding 10.
//
// One-element vectors are the sharpest case: rendered order and element order
// disagree, so ordering by text and ordering by element give opposite answers,
// and a comparator that recurses through CompareValues cannot accidentally agree
// with the old one.
func TestVectorOrderingIsElementWiseEndToEnd(t *testing.T) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: t.TempDir(),
		Schema: schema.NewBuilder().
			Attribute(":m/samples").Type(schema.TypeLong).Vector().Add().
			MustBuild(),
	})
	require.NoError(t, err)
	defer db.Close()

	attr := datalog.NewKeyword(":m/samples")
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("m-ten"), attr, int64(10)))
	require.NoError(t, tx.Add(datalog.NewIdentity("m-two"), attr, int64(2)))
	_, err = tx.Commit()
	require.NoError(t, err)

	t.Run("ascending order-by puts the vector holding 2 first", func(t *testing.T) {
		rel, err := db.Query(`[:find ?e ?v :where [?e :m/samples ?v] :order-by [[?v :asc]]]`)
		require.NoError(t, err)
		it := rel.Iterator()
		defer it.Close()

		require.True(t, it.Next(), "expected two tuples")
		require.Equal(t, int64(2), soleVectorElement(t, it.Tuple()[1]))
		require.True(t, it.Next(), "expected two tuples")
		require.Equal(t, int64(10), soleVectorElement(t, it.Tuple()[1]))
		require.NoError(t, it.Error())
	})

	t.Run("min is the vector holding 2", func(t *testing.T) {
		rel, err := db.Query(`[:find (min ?v) :where [?e :m/samples ?v]]`)
		require.NoError(t, err)
		it := rel.Iterator()
		defer it.Close()

		require.True(t, it.Next(), "expected one aggregate tuple")
		require.Equal(t, int64(2), soleVectorElement(t, it.Tuple()[0]))
		require.NoError(t, it.Error())
	})
}

// soleVectorElement requires v to be a one-element vector and returns that
// element as int64. A scalar binding fails here instead of comparing equal to the
// element, so a collapsed vector read cannot pass as a correctly ordered one.
func soleVectorElement(t *testing.T, v interface{}) int64 {
	t.Helper()
	rv := reflect.ValueOf(v)
	require.Equal(t, reflect.Slice, rv.Kind(), "expected a vector binding, got %T", v)
	require.Equal(t, 1, rv.Len(), "expected a one-element vector, got %v", v)
	n, ok := rv.Index(0).Interface().(int64)
	require.True(t, ok, "expected an int64 element, got %T", rv.Index(0).Interface())
	return n
}

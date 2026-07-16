//go:build !(js && wasm)

package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestNegativeAndPre1970ValuesRoundTripThroughStorage writes the value kinds that
// the old non-order-preserving encoding mis-sorted — negative int, negative
// float, pre-1970 time — and reads them back through a real query (so the value
// passes through index keys and the value portion). The order-preserving encoding
// must still round-trip them exactly.
func TestNegativeAndPre1970ValuesRoundTripThroughStorage(t *testing.T) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir(), ReplicaID: 1})
	require.NoError(t, err)
	defer db.Close()

	e := datalog.NewIdentity("e1")
	negInt := datalog.NewKeyword(":n/int")
	negFloat := datalog.NewKeyword(":n/float")
	oldTime := datalog.NewKeyword(":n/time")
	wantTime := time.Date(1955, 11, 5, 6, 15, 0, 0, time.UTC)

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, negInt, int64(-9000)))
	require.NoError(t, tx.Set(e, negFloat, -3.5))
	require.NoError(t, tx.Set(e, oldTime, wantTime))
	_, err = tx.Commit()
	require.NoError(t, err)

	readOne := func(a datalog.Keyword) any {
		rel, err := db.Query(`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`, e, a)
		require.NoError(t, err)
		it := rel.Iterator()
		require.True(t, it.Next(), "expected a value for %s", a)
		v := it.Tuple()[0]
		it.Close()
		return v
	}

	require.Equal(t, int64(-9000), readOne(negInt))
	require.Equal(t, -3.5, readOne(negFloat))
	gotTime, ok := readOne(oldTime).(time.Time)
	require.True(t, ok)
	require.True(t, wantTime.Equal(gotTime), "pre-1970 time round trip: want %v got %v", wantTime, gotTime)
}

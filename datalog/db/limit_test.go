package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// collectFirstValue drains a relation and returns the first value of each tuple.
func collectFirstValue(t *testing.T, rel executor.Relation) []interface{} {
	t.Helper()
	var out []interface{}
	it := rel.Iterator()
	defer it.Close()
	for it.Next() {
		out = append(out, it.Tuple()[0])
	}
	require.NoError(t, it.Error())
	return out
}

// TestQueryLimitThroughStorage exercises :limit end-to-end against real
// BadgerDB storage (the streaming-matcher path the feature targets), not a
// synthetic in-memory relation.
func TestQueryLimitThroughStorage(t *testing.T) {
	d := tempDB(t)

	kind := datalog.NewKeyword(":event/kind")
	seq := datalog.NewKeyword(":event/seq")

	tx := d.NewTransaction()
	for i := int64(1); i <= 5; i++ {
		e := datalog.NewIdentity("event:" + string(rune('a'+i-1)))
		require.NoError(t, tx.Add(e, kind, "telemetry"))
		require.NoError(t, tx.Add(e, seq, i))
	}
	_, err := tx.Commit()
	require.NoError(t, err)

	t.Run("limit caps rows without order-by", func(t *testing.T) {
		rel, err := d.Query(`[:find ?seq
		                      :where [?e :event/kind "telemetry"]
		                             [?e :event/seq ?seq]
		                      :limit 2]`)
		require.NoError(t, err)
		got := collectFirstValue(t, rel)
		require.Len(t, got, 2)
	})

	t.Run("latest record via order-by desc + limit 1", func(t *testing.T) {
		rel, err := d.Query(`[:find ?seq
		                      :where [?e :event/kind "telemetry"]
		                             [?e :event/seq ?seq]
		                      :order-by [[?seq :desc]]
		                      :limit 1]`)
		require.NoError(t, err)
		got := collectFirstValue(t, rel)
		require.Len(t, got, 1)
		require.Equal(t, int64(5), got[0])
	})

	t.Run("limit zero yields no rows", func(t *testing.T) {
		rel, err := d.Query(`[:find ?seq
		                      :where [?e :event/kind "telemetry"]
		                             [?e :event/seq ?seq]
		                      :limit 0]`)
		require.NoError(t, err)
		got := collectFirstValue(t, rel)
		require.Len(t, got, 0)
	})

	t.Run("limit above match count returns all", func(t *testing.T) {
		rel, err := d.Query(`[:find ?seq
		                      :where [?e :event/kind "telemetry"]
		                             [?e :event/seq ?seq]
		                      :limit 100]`)
		require.NoError(t, err)
		got := collectFirstValue(t, rel)
		require.Len(t, got, 5)
	})
}

// TestLimitComposesWithAsOf is the production pattern: "latest snapshot as of a
// point in time" — :order-by desc + :limit 1 against an AsOf view must return
// the latest row visible at that transaction, not the absolute latest.
func TestLimitComposesWithAsOf(t *testing.T) {
	d := tempDB(t)

	kind := datalog.NewKeyword(":snap/kind")
	seq := datalog.NewKeyword(":snap/seq")

	latestQuery := `[:find ?seq
	                 :where [?e :snap/kind "turn"]
	                        [?e :snap/seq ?seq]
	                 :order-by [[?seq :desc]]
	                 :limit 1]`

	// One snapshot entity per turn, each in its own transaction.
	var tx2ID datalog.ElementID
	for i := int64(1); i <= 3; i++ {
		e := datalog.NewIdentity("snap:" + string(rune('a'+i-1)))
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(e, kind, "turn"))
		require.NoError(t, tx.Add(e, seq, i))
		id, err := tx.Commit()
		require.NoError(t, err)
		if i == 2 {
			tx2ID = id
		}
	}

	// Live view: latest snapshot is seq 3.
	rel, err := d.Query(latestQuery)
	require.NoError(t, err)
	live := collectFirstValue(t, rel)
	require.Len(t, live, 1)
	require.Equal(t, int64(3), live[0])

	// As-of tx2: snap3 is not yet visible, so the latest is seq 2.
	rel, err = d.AsOf(tx2ID).Query(latestQuery)
	require.NoError(t, err)
	asOf := collectFirstValue(t, rel)
	require.Len(t, asOf, 1)
	require.Equal(t, int64(2), asOf[0])
}

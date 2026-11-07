package storage

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// QueryOneInto over a store whose read session fails to close answers the
// failure once per failed close.
func TestQueryOneIntoAnswersAReadSessionsCloseFailureOncePerFailedClose(t *testing.T) {
	score := datalog.NewKeyword(":item/score")
	var failing atomic.Bool
	var failed atomic.Int32
	db, err := NewDatabaseWithOptions(DatabaseOptions{Store: sessionCloseFailingStore{Store: NewBadgerEmulatorStore(nil), failing: &failing, failed: &failed}})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("query-one:first"), score, int64(3)))
	_, err = tx.Commit()
	require.NoError(t, err)
	failing.Store(true)
	defer failing.Store(false)

	var v int64
	_, err = db.QueryOneInto(&v, `[:find ?v . :where [_ :item/score ?v]]`)
	require.ErrorIs(t, err, errSessionClose, "the session's close failure reaches the caller")
	require.Equal(t, int(failed.Load()), occurrences(err, errSessionClose), "each failed close is answered once: %v", err)
}

package wasmtest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

func TestMemoryDatabaseRoundTrip(t *testing.T) {
	database, err := db.OpenMemory(db.WithReplicaID(7))
	require.NoError(t, err)
	defer database.Close()

	entity := datalog.NewIdentity("wasm:item")
	attr := datalog.NewKeyword(":item/name")
	tx := database.NewTransaction()
	require.NoError(t, tx.Set(entity, attr, "portable"))
	_, err = tx.Commit()
	require.NoError(t, err)

	var names []string
	require.NoError(t, database.QueryInto(
		&names,
		`[:find ?name :where [?entity :item/name ?name]]`,
	))
	require.Equal(t, []string{"portable"}, names)
}

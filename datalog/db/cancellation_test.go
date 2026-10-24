package db_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/db"
)

// A query under a cancelled context returns the context's error, and one
// under a live context runs as it does without the option.
func TestWithContextCancelsQuery(t *testing.T) {
	const names = `[:find ?name :where [?e :dump/name ?name]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			path := ""
			if mode.backend.Persistent {
				path = t.TempDir()
			}
			d, err := db.Open(path, db.WithBackend(mode.backend.Name), db.WithPlannerOptions(mode.plannerOptions()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = d.Close() })

			tx := d.NewTransaction()
			require.NoError(t, tx.Set(dumpEntity, dumpAttr, "held"))
			_, err = tx.Commit()
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var held []string
			require.ErrorIs(t, d.QueryInto(&held, names, db.WithContext(ctx)), context.Canceled)

			require.NoError(t, d.QueryInto(&held, names, db.WithContext(context.Background())))
			require.Equal(t, []string{"held"}, held)
		})
	}
}

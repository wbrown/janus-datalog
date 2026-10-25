package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A database opened with WithRefuseUnknownAttributes refuses a query naming
// an attribute its schema does not declare, and one opened without it returns
// no tuple for the same query.
func TestOpenRefusesUnknownAttributesWhenAsked(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s := declaredDumpSchema(t, "the subject's tags")
			path := ""
			if mode.backend.Persistent {
				path = t.TempDir()
			}
			refusing, err := db.Open(path, db.WithSchema(s), db.WithRefuseUnknownAttributes(),
				db.WithBackend(mode.backend.Name), db.WithPlannerOptions(mode.plannerOptions()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = refusing.Close() })

			var tags []string
			_, err = refusing.Query(`[:find ?t :where [?e :dump/tgas ?t]]`)
			var unknown *schema.UnknownAttributeError
			require.ErrorAs(t, err, &unknown, "expected a refusal, got %v", err)
			require.Same(t, datalog.NewKeyword(":dump/tgas"), unknown.Attribute)

			if mode.backend.Persistent {
				path = t.TempDir()
			}
			accepting, err := db.Open(path, db.WithSchema(s),
				db.WithBackend(mode.backend.Name), db.WithPlannerOptions(mode.plannerOptions()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = accepting.Close() })
			require.NoError(t, accepting.QueryInto(&tags, `[:find ?t :where [?e :dump/tgas ?t]]`))
			require.Empty(t, tags)
		})
	}
}

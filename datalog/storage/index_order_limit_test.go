package storage

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func TestIndexOrderedLimitStopsSatisfiedScan(t *testing.T) {
	const (
		entityCount = 1_000
		limit       = 10
	)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var scanned atomic.Int64
			handler := func(event annotations.Event) {
				if event.Name != annotations.StorageScanComplete {
					return
				}
				// Intake, not resolution's output: "stopped after the requested
				// tuples" is a claim about what the scan read. Resolution emits
				// one tuple per entity however deep the history is, so a scan
				// that walked the whole index would satisfy a bound on the
				// resolved count.
				if count, ok := event.Data[annotations.KeyDatomsScanned].(int); ok {
					scanned.Add(int64(count))
				}
			}

			nameAttr := datalog.NewKeyword(":person/name")
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       nameAttr,
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db := createOptimizerModeDB(t, mode, DatabaseOptions{
				Schema:            s,
				AnnotationHandler: handler,
			})

			tx := db.NewTransaction()
			for i := 0; i < entityCount; i++ {
				entity := datalog.NewIdentity(fmt.Sprintf("index-order-person:%d", i))
				require.NoError(t, tx.Set(entity, nameAttr, fmt.Sprintf("Person %d", i)))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			run := func(direction string) ([][]interface{}, int64) {
				t.Helper()
				scanned.Store(0)
				queryText := fmt.Sprintf(
					`[:find ?e :where [?e :person/name ?name] :order-by [[?e :%s]] :limit %d]`,
					direction,
					limit,
				)
				result, err := db.Query(queryText)
				require.NoError(t, err)
				tuples, err := executor.CollectTuples(result, nil)
				require.NoError(t, err)
				require.Len(t, tuples, limit)
				return tuples, scanned.Load()
			}

			ascending, ascendingScans := run("asc")
			require.LessOrEqual(t, ascendingScans, int64(limit),
				"satisfied index order must stop after the requested tuples")
			for i := 1; i < len(ascending); i++ {
				previous := ascending[i-1][0].(datalog.Identity)
				current := ascending[i][0].(datalog.Identity)
				require.LessOrEqual(t, previous.Compare(current), 0)
			}

			descending, descendingScans := run("desc")
			require.Equal(t, int64(entityCount), descendingScans,
				"unsatisfied descending order must retain bounded Top-N full-scan behavior")
			for i := 1; i < len(descending); i++ {
				previous := descending[i-1][0].(datalog.Identity)
				current := descending[i][0].(datalog.Identity)
				require.GreaterOrEqual(t, previous.Compare(current), 0)
			}
		})
	}
}

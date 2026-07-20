package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func TestSymbolValueRoundTripAndQuery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			attribute := datalog.NewKeyword(":workflow/state")
			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       attribute,
				ValueType:   schema.TypeSymbol,
				Cardinality: schema.CardinalityOne,
			})
			popts := mode.plannerOptions()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:           t.TempDir(),
				Schema:         s,
				PlannerOptions: &popts,
			})
			require.NoError(t, err)
			defer db.Close()

			entity := datalog.NewIdentity("symbol-value")
			state := datalog.NewSymbol("workflow/active")
			require.Equal(t, schema.TypeSymbol, valueTypeFromValue(state))
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(entity, attribute, state))
			_, err = tx.Commit()
			require.NoError(t, err)

			result, err := db.Query(
				`[:find ?state :in $ ?entity :where [?entity :workflow/state ?state]]`,
				entity,
			)
			require.NoError(t, err)
			rows, err := executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{state}}, rows)

			result, err = db.Query(
				`[:find ?entity :in $ ?state :where [?entity :workflow/state ?state]]`,
				state,
			)
			require.NoError(t, err)
			rows, err = executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{entity}}, rows)

			result, err = db.Query(
				`[:find ?entity :where [?entity :workflow/state workflow/active]]`,
			)
			require.NoError(t, err)
			rows, err = executor.CollectTuples(result, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{entity}}, rows)

			node := ValueNode(state)
			decoded, err := ParseValueNode(&node)
			require.NoError(t, err)
			require.True(t, datalog.ValuesEqual(state, decoded))
		})
	}
}

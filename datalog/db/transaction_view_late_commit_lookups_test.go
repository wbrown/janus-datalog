package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
)

// TestATransactionsVectorLookupsAfterAnEarlierCommitAnswerTheVectorItOpenedOn
// pins, through db.OpenMemory, that Transaction.LookupAttribute and
// LookupAllAttributes answer the vector the transaction opened on with its
// own writes applied. The committed vector is [s w]. A transaction opened
// first draws r after HEAD and commits only after a transaction opened
// second has written [s w q]. The second's lookups answer [s w q], and the
// database after its commit answers both writers' elements.
func TestATransactionsVectorLookupsAfterAnEarlierCommitAnswerTheVectorItOpenedOn(t *testing.T) {
	stack := datalog.NewIdentity("late-entry:lookup")
	for _, attr := range []datalog.Keyword{lateEntryCells, lateEntryOrder} {
		t.Run(attr.String(), func(t *testing.T) {
			d, err := db.OpenMemory(db.WithSchema(lateEntrySchema(t)))
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(stack, lateEntryName, "stack"))
			require.NoError(t, tx.Add(stack, attr, "s"))
			require.NoError(t, tx.Add(stack, attr, "w"))
			_, err = tx.Commit()
			require.NoError(t, err)

			earlier := d.NewTransaction()
			require.NoError(t, earlier.Add(stack, attr, "r"))
			later := d.NewTransaction()
			require.NoError(t, later.Set(stack, attr, []string{"s", "w", "q"}))
			_, err = earlier.Commit()
			require.NoError(t, err)

			held, found, err := later.LookupAttribute(stack, attr)
			require.NoError(t, err)
			require.True(t, found)
			heldAll, err := later.LookupAllAttributes(stack, attr)
			require.NoError(t, err)
			_, err = later.Commit()
			require.NoError(t, err)

			assert.Equal(t, []string{"s", "w", "q"}, held, "Transaction.LookupAttribute answers the vector it opened on with its own writes")
			assert.Equal(t, "[s w q]", fmt.Sprint(heldAll), "Transaction.LookupAllAttributes answers the vector it opened on with its own writes")
			committed, found, err := d.LookupAttribute(stack, attr)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, []string{"s", "w", "q", "r"}, committed, "Database.LookupAttribute after the commit: both writers' elements")
		})
	}
}

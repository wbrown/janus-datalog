package db_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	lateEntryName  = datalog.NewKeyword(":lateentry/name")
	lateEntryCells = datalog.NewKeyword(":lateentry/cells")
	lateEntryOrder = datalog.NewKeyword(":lateentry/order")
)

type lateEntryCellsRecord struct {
	ID    datalog.Identity `datalog:"-,id"`
	Cells []string         `datalog:"lateentry/cells"`
}

type lateEntryOrderRecord struct {
	ID    datalog.Identity           `datalog:"-,id"`
	Order datalog.OrderedSet[string] `datalog:"lateentry/order"`
}

func lateEntrySchema(t *testing.T) schema.SchemaProvider {
	s, err := schema.NewBuilder().
		Attribute(lateEntryName.String()).Type(schema.TypeString).Add().
		Attribute(lateEntryCells.String()).Type(schema.TypeString).Vector().Add().
		Attribute(lateEntryOrder.String()).Type(schema.TypeString).OrderedSet().Add().
		Build()
	require.NoError(t, err)
	return s
}

// TestAWholeVectorWriteAfterAnEarlierCommitKeepsTheElementItsViewNeverHeldAtEveryEntryPoint
// pins, through db.OpenMemory, that every caller-reachable whole-vector write
// is made against the value its transaction opened on. The committed vector
// is [s w]. A transaction opened first draws r after HEAD and commits only
// after a transaction opened second has written [s w q]. The second's view
// never holds that r, so its write of [s w r q] inserts an r of its own, and
// the database answers both writers' elements.
func TestAWholeVectorWriteAfterAnEarlierCommitKeepsTheElementItsViewNeverHeldAtEveryEntryPoint(t *testing.T) {
	stack := datalog.NewIdentity("late-entry:stack")
	stated := []string{"s", "w", "r", "q"}
	want := []string{"s", "w", "r", "q", "r"}
	cases := []struct {
		name   string
		attr   datalog.Keyword
		second func(t *testing.T, tx *db.Transaction, s schema.SchemaProvider) error
	}{
		{"Transaction.Set on a vector", lateEntryCells, func(t *testing.T, tx *db.Transaction, s schema.SchemaProvider) error {
			return tx.Set(stack, lateEntryCells, stated)
		}},
		{"Transaction.Set on an ordered set", lateEntryOrder, func(t *testing.T, tx *db.Transaction, s schema.SchemaProvider) error {
			return tx.Set(stack, lateEntryOrder, stated)
		}},
		{"Transaction.SaveStruct of a vector field", lateEntryCells, func(t *testing.T, tx *db.Transaction, s schema.SchemaProvider) error {
			_, err := tx.SaveStruct(&lateEntryCellsRecord{ID: stack, Cells: stated})
			return err
		}},
		{"Transaction.SaveStruct of an OrderedSet field", lateEntryOrder, func(t *testing.T, tx *db.Transaction, s schema.SchemaProvider) error {
			_, err := tx.SaveStruct(&lateEntryOrderRecord{ID: stack, Order: *datalog.OrderedSetFrom(stated)})
			return err
		}},
		{"StructWriter.UpdateAuto with the transaction as its lookup", lateEntryCells, func(t *testing.T, tx *db.Transaction, s schema.SchemaProvider) error {
			record := &lateEntryCellsRecord{ID: stack, Cells: stated}
			sw, err := dlreflect.NewStructWriter(record, s)
			require.NoError(t, err)
			_, err = sw.UpdateAuto(tx, tx, record, dlreflect.UpdateModeReplace)
			return err
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := lateEntrySchema(t)
			d, err := db.OpenMemory(db.WithSchema(s))
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()

			tx := d.NewTransaction()
			require.NoError(t, tx.Add(stack, lateEntryName, "stack"))
			require.NoError(t, tx.Add(stack, c.attr, "s"))
			require.NoError(t, tx.Add(stack, c.attr, "w"))
			_, err = tx.Commit()
			require.NoError(t, err)

			earlier := d.NewTransaction()
			require.NoError(t, earlier.Add(stack, c.attr, "r"))
			later := d.NewTransaction()
			require.NoError(t, later.Set(stack, c.attr, []string{"s", "w", "q"}))
			_, err = earlier.Commit()
			require.NoError(t, err)
			require.NoError(t, c.second(t, later, s))
			_, err = later.Commit()
			require.NoError(t, err)

			got, found, err := d.LookupAttribute(stack, c.attr)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, want, got, "Database.LookupAttribute: the later transaction's elements, and the earlier r its view never held")
			if c.attr == lateEntryCells {
				var pulled lateEntryCellsRecord
				require.NoError(t, d.PullInto(stack, &pulled))
				assert.Equal(t, want, pulled.Cells, "PullInto")
			}
		})
	}
}

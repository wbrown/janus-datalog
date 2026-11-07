package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	lateName  = datalog.NewKeyword(":late/name")
	lateCells = datalog.NewKeyword(":late/cells")
)

type lateRecord struct {
	ID    datalog.Identity `datalog:"-,id"`
	Cells []string         `datalog:"late/cells"`
}

// TestAVectorWriteAfterAnEarlierTransactionCommitsKeepsTheElementItsViewNeverHeld
// pins that a transaction's whole-vector write is made against the value it
// opened on. The committed vector is [s w]. The earlier transaction adds r
// after HEAD and commits only after the later transaction's first write. The
// later transaction opened before that commit, so its view never holds the
// earlier r: its second write inserts an r of its own, and the earlier r
// stays after s's subtree.
func TestAVectorWriteAfterAnEarlierTransactionCommitsKeepsTheElementItsViewNeverHeld(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(lateName.String()).Type(schema.TypeString).Add().
		Attribute(lateCells.String()).Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	stack := datalog.NewIdentity("late-commit:stack")

	cases := []struct {
		name   string
		first  func(tx *Transaction) error
		second func(tx *Transaction) error
		want   []string
	}{
		{
			name:   "a Set after a Set that kept the whole vector",
			first:  func(tx *Transaction) error { return tx.Set(stack, lateCells, []string{"s", "w", "q"}) },
			second: func(tx *Transaction) error { return tx.Set(stack, lateCells, []string{"s", "w", "r", "q"}) },
			want:   []string{"s", "w", "r", "q", "r"},
		},
		{
			name:   "a Set after a Set that withdrew the vector's tail",
			first:  func(tx *Transaction) error { return tx.Set(stack, lateCells, []string{"s", "q"}) },
			second: func(tx *Transaction) error { return tx.Set(stack, lateCells, []string{"s", "r", "q"}) },
			want:   []string{"s", "r", "q", "r"},
		},
		{
			name:  "a SaveStruct after a Set that kept the whole vector",
			first: func(tx *Transaction) error { return tx.Set(stack, lateCells, []string{"s", "w", "q"}) },
			second: func(tx *Transaction) error {
				_, err := tx.SaveStruct(&lateRecord{ID: stack, Cells: []string{"s", "w", "r", "q"}})
				return err
			},
			want: []string{"s", "w", "r", "q", "r"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: s})
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(stack, lateName, "stack"))
			require.NoError(t, tx.Add(stack, lateCells, "s"))
			require.NoError(t, tx.Add(stack, lateCells, "w"))
			_, err = tx.Commit()
			require.NoError(t, err)

			earlier := db.NewTransaction()
			require.NoError(t, earlier.Add(stack, lateCells, "r"))
			later := db.NewTransaction()
			require.NoError(t, c.first(later))
			_, err = earlier.Commit()
			require.NoError(t, err)
			require.NoError(t, c.second(later))
			_, err = later.Commit()
			require.NoError(t, err)

			name, _, err := db.LookupAttribute(stack, lateName)
			require.NoError(t, err)
			got, found, err := db.LookupAttribute(stack, lateCells)
			require.NoError(t, err)
			require.True(t, found, "%v holds no vector", name)
			require.Equal(t, c.want, got, "%v's vector: the later transaction's elements, and the earlier r its view never held", name)
		})
	}
}

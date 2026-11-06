package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestATransactionsLookupAllAttributesHoldsTheVectorItOpenedOnWithItsOwnWrites
// pins for LookupAllAttributes what
// TestATransactionHoldsTheVectorItOpenedOnWithItsOwnWrites pins for
// LookupAttribute: committed [a]; a transaction adds x and stays open; a
// second opens and sets [a b]; the first commits. The second's
// LookupAllAttributes answers [a b], the vector it opened on with its own
// Set, and its commit keeps the first's x beside it.
func TestATransactionsLookupAllAttributesHoldsTheVectorItOpenedOnWithItsOwnWrites(t *testing.T) {
	for _, disableCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("disable_cache_%t", disableCache), func(t *testing.T) {
			d := heldVectorDatabase(t, disableCache)
			e := datalog.NewIdentity("held-vector:lookup-all")
			first := d.NewTransaction()
			require.NoError(t, first.Add(e, heldVectorCells, "a"))
			_, err := first.Commit()
			require.NoError(t, err)
			earlier := d.NewTransaction()
			require.NoError(t, earlier.Add(e, heldVectorCells, "x"))
			held := d.NewTransaction()
			require.NoError(t, held.Set(e, heldVectorCells, []interface{}{"a", "b"}))
			_, err = earlier.Commit()
			require.NoError(t, err)
			values, err := held.LookupAllAttributes(e, heldVectorCells)
			require.NoError(t, err)
			answered := make([]string, 0, len(values))
			for _, v := range values {
				s, ok := v.(string)
				require.True(t, ok, "element %T", v)
				answered = append(answered, s)
			}
			assert.Equal(t, []string{"a", "b"}, answered, "the transaction holds the vector it opened on with its own writes")
			_, err = held.Commit()
			require.NoError(t, err)
			v, found, err := d.LookupAttribute(e, heldVectorCells)
			assert.Equal(t, []string{"a", "b", "x"}, heldVectorStrings(t, v, found, err),
				"the commit keeps the held transaction's elements and the earlier one's")
		})
	}
}

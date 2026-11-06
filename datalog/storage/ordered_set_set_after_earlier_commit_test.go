package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestAnOrderedSetSetAfterAnEarlierTransactionCommitsKeepsTheElementItsViewNeverHeld
// is TestAVectorSetAfterAnEarlierTransactionCommitsKeepsTheElementItsViewNeverHeld
// on an ordered set: committed [a]; a transaction adds x and stays open; a
// second opens and sets [a b]; the first commits; the second sets [a x b] and
// commits. The second's view never holds the first's x, so both writers' x
// stay, as two concurrent adds of one value do.
func TestAnOrderedSetSetAfterAnEarlierTransactionCommitsKeepsTheElementItsViewNeverHeld(t *testing.T) {
	members := datalog.NewKeyword(":earlier/members")
	for _, disableCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("disable_cache_%t", disableCache), func(t *testing.T) {
			s, err := schema.NewBuilder().
				Attribute(members.String()).Type(schema.TypeString).OrderedSet().Add().
				Build()
			require.NoError(t, err)
			d, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: s, DisableCache: disableCache})
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()
			e := datalog.NewIdentity("earlier-commit:ordered-set")
			first := d.NewTransaction()
			require.NoError(t, first.Add(e, members, "a"))
			_, err = first.Commit()
			require.NoError(t, err)
			earlier := d.NewTransaction()
			require.NoError(t, earlier.Add(e, members, "x"))
			held := d.NewTransaction()
			require.NoError(t, held.Set(e, members, []interface{}{"a", "b"}))
			_, err = earlier.Commit()
			require.NoError(t, err)
			require.NoError(t, held.Set(e, members, []interface{}{"a", "x", "b"}))
			_, err = held.Commit()
			require.NoError(t, err)
			v, found, err := d.LookupAttribute(e, members)
			require.NoError(t, err)
			require.True(t, found)
			var got []string
			switch vec := v.(type) {
			case []string:
				got = vec
			case []interface{}:
				for _, el := range vec {
					s, ok := el.(string)
					require.True(t, ok, "element %T", el)
					got = append(got, s)
				}
			default:
				t.Fatalf("the ordered set is %T", v)
			}
			assert.Equal(t, []string{"a", "x", "b", "x"}, got, "the ordered set after the commit: the held transaction's elements, and the earlier x its view never held")
		})
	}
}

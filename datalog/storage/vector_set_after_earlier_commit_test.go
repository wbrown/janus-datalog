package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestAVectorSetAfterAnEarlierTransactionCommitsKeepsTheElementItsViewNeverHeld
// pins that a transaction's Set of a vector is made against the value it
// opened on. Another transaction drew x after HEAD before this one opened, and
// commits between this one's two Sets. This one's view never holds that x, so
// its last Set inserts an x of its own, and the other's x stays after a's
// subtree.
func TestAVectorSetAfterAnEarlierTransactionCommitsKeepsTheElementItsViewNeverHeld(t *testing.T) {
	cells := datalog.NewKeyword(":earlier/cells")
	cases := []struct {
		name      string
		first     []string // the held transaction's first Set
		last      []string // its last Set, after the earlier transaction commits
		want      []string // the vector after both commit
		viaStruct bool
	}{
		{"set, then set the vector with the earlier element in it", []string{"a", "b"}, []string{"a", "x", "b"}, []string{"a", "x", "b", "x"}, false},
		{"set, then save the vector with the earlier element in it", []string{"a", "b"}, []string{"a", "x", "b"}, []string{"a", "x", "b", "x"}, true},
	}
	type record struct {
		ID    datalog.Identity `datalog:"-,id"`
		Cells []string         `datalog:"earlier/cells"`
	}
	for _, c := range cases {
		for _, disableCache := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disable_cache_%t", c.name, disableCache), func(t *testing.T) {
				s, err := schema.NewBuilder().
					Attribute(cells.String()).Type(schema.TypeString).Vector().Add().
					Build()
				require.NoError(t, err)
				d, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: s, DisableCache: disableCache})
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()
				e := datalog.NewIdentity("earlier-commit:entity")

				first := d.NewTransaction()
				require.NoError(t, first.Add(e, cells, "a"))
				_, err = first.Commit()
				require.NoError(t, err)

				earlier := d.NewTransaction()
				require.NoError(t, earlier.Add(e, cells, "x"))
				held := d.NewTransaction()
				toAny := func(vs []string) []interface{} {
					out := make([]interface{}, len(vs))
					for i, v := range vs {
						out[i] = v
					}
					return out
				}
				require.NoError(t, held.Set(e, cells, toAny(c.first)))
				_, err = earlier.Commit()
				require.NoError(t, err)
				if c.viaStruct {
					_, err = held.SaveStruct(&record{ID: e, Cells: append([]string{}, c.last...)})
					require.NoError(t, err)
				} else {
					require.NoError(t, held.Set(e, cells, toAny(c.last)))
				}
				_, err = held.Commit()
				require.NoError(t, err)

				v, found, err := d.LookupAttribute(e, cells)
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
					t.Fatalf("the vector is %T", v)
				}
				assert.Equal(t, c.want, got, "the vector after the commit: the held transaction's elements, and the earlier x its view never held")
			})
		}
	}
}

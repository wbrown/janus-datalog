package storage

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// heldVectorCells is the vector attribute the held-vector tests write.
var heldVectorCells = datalog.NewKeyword(":held/cells")

func heldVectorSchema(t *testing.T) schema.SchemaProvider {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(heldVectorCells.String()).Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	return s
}

func heldVectorDatabase(t *testing.T, disableCache bool) *Database {
	t.Helper()
	d, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: heldVectorSchema(t), DisableCache: disableCache})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}

// heldVectorStrings reads a lookup's answer as the vector's strings: nil
// where the lookup finds no vector.
func heldVectorStrings(t *testing.T, v interface{}, found bool, err error) []string {
	t.Helper()
	require.NoError(t, err)
	if !found {
		return nil
	}
	got, err := stringsOfVector(v)
	require.NoError(t, err)
	return got
}

// heldVectorOpenedOn answers the vector a transaction holds once it has
// written held over the value committed leaves, read from a database no other
// transaction writes: it commits committed, then held, and reads e's vector.
func heldVectorOpenedOn(t *testing.T, e datalog.Identity, committed, held []heldVectorStep) []string {
	t.Helper()
	d, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: heldVectorSchema(t)})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	for _, steps := range [][]heldVectorStep{committed, held} {
		tx := d.NewTransaction()
		for _, s := range steps {
			require.NoError(t, s.write(tx, e))
		}
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	v, found, err := d.LookupAttribute(e, heldVectorCells)
	return heldVectorStrings(t, v, found, err)
}

// heldVectorStep is one write of the generated schedules: a transaction's
// Add, Remove or Set of the vector.
type heldVectorStep struct {
	kind   vectorOpKind
	value  string
	values []string
}

func (s heldVectorStep) write(tx *Transaction, e datalog.Identity) error {
	switch s.kind {
	case vectorAdd:
		return tx.Add(e, heldVectorCells, s.value)
	case vectorRemove:
		return tx.Remove(e, heldVectorCells, s.value)
	case vectorSet:
		values := make([]interface{}, len(s.values))
		for i, v := range s.values {
			values[i] = v
		}
		return tx.Set(e, heldVectorCells, values)
	}
	panic(fmt.Sprintf("step kind %d", s.kind))
}

func (s heldVectorStep) String() string {
	switch s.kind {
	case vectorAdd:
		return "add " + s.value
	case vectorRemove:
		return "remove " + s.value
	}
	return fmt.Sprintf("set %q", s.values)
}

// TestATransactionHoldsTheVectorItOpenedOnWithItsOwnWrites pins that the
// vector a transaction answers through LookupAttribute is the vector it
// opened on with its own writes applied, though a transaction opened before
// it commits an element after it opened. That element is not in its view,
// and its commit keeps both writers' elements.
func TestATransactionHoldsTheVectorItOpenedOnWithItsOwnWrites(t *testing.T) {
	cases := []struct {
		name string
		// committed is written and committed first.
		committed []heldVectorStep
		// earlier is written by a transaction that opens before the held one and
		// commits after the held one's first `before` steps.
		earlier []heldVectorStep
		held    []heldVectorStep
		before  int
		// holds is the vector the held transaction answers before it commits,
		// and resolves the vector the database answers after.
		holds, resolves []string
	}{
		{
			name:      "a set anchored on a committed element, then an earlier transaction's head element commits",
			committed: []heldVectorStep{{kind: vectorAdd, value: "a"}},
			earlier:   []heldVectorStep{{kind: vectorAdd, value: "x"}},
			held:      []heldVectorStep{{kind: vectorSet, values: []string{"a", "b"}}},
			before:    1,
			holds:     []string{"a", "b"},
			resolves:  []string{"a", "b", "x"},
		},
		{
			name:      "an add after a set anchored on a committed element, then an earlier transaction's head element commits",
			committed: []heldVectorStep{{kind: vectorAdd, value: "a"}, {kind: vectorAdd, value: "b"}},
			earlier:   []heldVectorStep{{kind: vectorAdd, value: "x"}},
			held:      []heldVectorStep{{kind: vectorSet, values: []string{"a"}}, {kind: vectorAdd, value: "c"}},
			before:    2,
			holds:     []string{"a", "c"},
			resolves:  []string{"a", "c", "x"},
		},
	}
	for _, c := range cases {
		for _, disableCache := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disable_cache_%t", c.name, disableCache), func(t *testing.T) {
				d := heldVectorDatabase(t, disableCache)
				e := datalog.NewIdentity("held-vector:entity")
				first := d.NewTransaction()
				for _, s := range c.committed {
					require.NoError(t, s.write(first, e))
				}
				_, err := first.Commit()
				require.NoError(t, err)

				earlier := d.NewTransaction()
				for _, s := range c.earlier {
					require.NoError(t, s.write(earlier, e))
				}
				held := d.NewTransaction()
				for i, s := range c.held {
					if i == c.before {
						_, err := earlier.Commit()
						require.NoError(t, err)
					}
					require.NoError(t, s.write(held, e))
				}
				if c.before >= len(c.held) {
					_, err := earlier.Commit()
					require.NoError(t, err)
				}
				v, found, err := held.LookupAttribute(e, heldVectorCells)
				assert.Equal(t, c.holds, heldVectorStrings(t, v, found, err),
					"the transaction holds the vector it opened on with its own writes")
				_, err = held.Commit()
				require.NoError(t, err)
				v, found, err = d.LookupAttribute(e, heldVectorCells)
				assert.Equal(t, c.resolves, heldVectorStrings(t, v, found, err),
					"the commit keeps the held transaction's elements and the earlier one's")
			})
		}
	}
}

// generateHeldVectorSteps draws one to n steps of Add, Remove and Set over
// the values a to d.
func generateHeldVectorSteps(r *rand.Rand, n int) []heldVectorStep {
	values := []string{"a", "b", "c", "d"}
	steps := make([]heldVectorStep, 1+r.Intn(n))
	for i := range steps {
		switch k := r.Intn(10); {
		case k < 5:
			steps[i] = heldVectorStep{kind: vectorAdd, value: values[r.Intn(len(values))]}
		case k < 7:
			steps[i] = heldVectorStep{kind: vectorRemove, value: values[r.Intn(len(values))]}
		default:
			var set []string
			for m := r.Intn(4); m > 0; m-- {
				set = append(set, values[r.Intn(len(values))])
			}
			steps[i] = heldVectorStep{kind: vectorSet, values: set}
		}
	}
	return steps
}

// TestATransactionHoldsTheVectorItOpenedOnWithItsOwnWritesOnGeneratedSchedules
// runs the pin of TestATransactionHoldsTheVectorItOpenedOnWithItsOwnWrites
// over generated schedules: a committed vector, a transaction that writes and
// then stays open while a second opens, and the second's writes with the
// first's commit placed among them. The second's LookupAttribute answers what
// heldVectorOpenedOn reads from a database the first never writes.
func TestATransactionHoldsTheVectorItOpenedOnWithItsOwnWritesOnGeneratedSchedules(t *testing.T) {
	const seeds = 300
	var differ []string
	for seed := int64(1); seed <= seeds; seed++ {
		r := rand.New(rand.NewSource(seed))
		committed := generateHeldVectorSteps(r, 3)
		earlierSteps := generateHeldVectorSteps(r, 3)
		heldSteps := generateHeldVectorSteps(r, 4)
		before := r.Intn(len(heldSteps) + 1)

		d := heldVectorDatabase(t, false)
		e := datalog.NewIdentity("held-vector:generated")
		first := d.NewTransaction()
		for _, s := range committed {
			require.NoError(t, s.write(first, e))
		}
		_, err := first.Commit()
		require.NoError(t, err)
		earlier := d.NewTransaction()
		for _, s := range earlierSteps {
			require.NoError(t, s.write(earlier, e))
		}
		held := d.NewTransaction()
		for i, s := range heldSteps {
			if i == before {
				_, err := earlier.Commit()
				require.NoError(t, err)
			}
			require.NoError(t, s.write(held, e))
		}
		if before >= len(heldSteps) {
			_, err := earlier.Commit()
			require.NoError(t, err)
		}
		v, found, err := held.LookupAttribute(e, heldVectorCells)
		answered := heldVectorStrings(t, v, found, err)
		_, err = held.Commit()
		require.NoError(t, err)
		openedOn := heldVectorOpenedOn(t, e, committed, heldSteps)
		if !slices.Equal(answered, openedOn) {
			differ = append(differ, fmt.Sprintf("seed %d: committed %v, earlier %v, held %v with the earlier commit before step %d: held %q, opened on %q",
				seed, committed, earlierSteps, heldSteps, before, answered, openedOn))
		}
	}
	assert.Empty(t, differ, "%d of %d schedules", len(differ), seeds)
}

package storage

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A float's signed zero is one value with zero, so no entry puts one into a
// relation or the store: a -0.0 an EDN dump carries reads back as 0.0 in a
// pattern, under str and in a pull; a -0.0 a query's text grounds answers
// 0.0 and renders "0"; and a negation, a product with a negative factor or a
// division by a negative over a stored 0.0 answers 0.0. On every backend, in
// both optimizer modes, with the cache on and off.
func TestASignedZeroIsOneValueAtEveryEntry(t *testing.T) {
	e := datalog.NewIdentity("z1")
	zero := func(t *testing.T, answer interface{}) {
		t.Helper()
		f, ok := answer.(float64)
		require.True(t, ok, "the answer is a float, got %T %v", answer, answer)
		require.Equal(t, 0.0, f)
		require.False(t, math.Signbit(f), "the answer carries the sign bit")
	}
	importNegativeZero := func(t *testing.T, d *Database) {
		t.Helper()
		line := fmt.Sprintf(`[#identity "%s" :z/f -0.0 [7 0]]`, e.L85())
		require.NoError(t, d.Import(strings.NewReader(line+"\n")))
	}
	storeZero := func(t *testing.T, d *Database) {
		t.Helper()
		tx := d.NewTransaction()
		require.NoError(t, tx.Set(e, kw(":z/f"), 0.0))
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	oneTuple := func(t *testing.T, d *Database, text string) interface{} {
		t.Helper()
		tuples, err := executor.CollectTuples(d.Query(text))
		require.NoError(t, err, text)
		require.Len(t, tuples, 1, text)
		return tuples[0][0]
	}
	cases := []struct {
		name string
		run  func(t *testing.T, d *Database)
	}{
		{"an imported -0.0 matches 0.0 and reads back as 0.0", func(t *testing.T, d *Database) {
			importNegativeZero(t, d)
			zero(t, oneTuple(t, d, `[:find ?v :where [?e :z/f 0.0] [?e :z/f ?v]]`))
		}},
		{"an imported -0.0 renders as 0 under str", func(t *testing.T, d *Database) {
			importNegativeZero(t, d)
			require.Equal(t, "0", oneTuple(t, d, `[:find ?r :where [_ :z/f ?v] [(str ?v) ?r]]`))
		}},
		{"an imported -0.0 pulls as 0.0", func(t *testing.T, d *Database) {
			importNegativeZero(t, d)
			pulled, err := d.Pull(e, `[:z/f]`)
			require.NoError(t, err)
			zero(t, pulled["z/f"])
		}},
		{"a grounded -0.0 answers 0.0", func(t *testing.T, d *Database) {
			zero(t, oneTuple(t, d, `[:find ?z :where [(ground -0.0) ?z]]`))
		}},
		{"a grounded -0.0 renders as 0 under str", func(t *testing.T, d *Database) {
			require.Equal(t, "0", oneTuple(t, d, `[:find ?s :where [(ground -0.0) ?z] [(str ?z) ?s]]`))
		}},
		{"a product of a stored 0.0 with a negative factor answers 0.0", func(t *testing.T, d *Database) {
			storeZero(t, d)
			zero(t, oneTuple(t, d, `[:find ?z :where [_ :z/f ?a] [(* -1.0 ?a) ?z]]`))
		}},
		{"a negation of a stored 0.0 answers 0.0", func(t *testing.T, d *Database) {
			storeZero(t, d)
			zero(t, oneTuple(t, d, `[:find ?z :where [_ :z/f ?a] [(- ?a) ?z]]`))
		}},
		{"a division of a stored 0.0 by a negative answers 0.0", func(t *testing.T, d *Database) {
			storeZero(t, d)
			zero(t, oneTuple(t, d, `[:find ?z :where [_ :z/f ?a] [(/ ?a -1.0) ?z]]`))
		}},
	}
	for _, mode := range optimizerModes {
		for _, disableCache := range []bool{false, true} {
			cache := "cache on"
			if disableCache {
				cache = "cache off"
			}
			for _, c := range cases {
				t.Run(mode.name+"/"+cache+"/"+c.name, func(t *testing.T) {
					sch := schema.NewBuilder().Attribute(":z/f").Type(schema.TypeDouble).Add().MustBuild()
					d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: sch, DisableCache: disableCache})
					c.run(t, d)
				})
			}
		}
	}
}

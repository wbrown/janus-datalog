package db_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A signed zero inside a vector is 0.0 at every place a vector enters the
// engine: a tuple input, a relation input, a collection input whose members
// are vectors, and a vector written to a vector attribute and read back.
func TestASignedZeroInsideAVectorIsOneValueAtEveryEntry(t *testing.T) {
	alice := datalog.NewIdentity("signed-zero-entry-alice")
	s := schema.NewBuilder().
		Attribute(":t/name").Type(schema.TypeString).Add().
		Attribute(":t/vals").Type(schema.TypeDouble).Vector().Add().
		MustBuild()
	d, err := db.OpenMemory(db.WithSchema(s))
	require.NoError(t, err)
	defer d.Close()
	negativeZero := math.Copysign(0, -1)
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
	require.NoError(t, tx.Set(alice, datalog.NewKeyword(":t/vals"), []float64{negativeZero, 1.5}))
	_, err = tx.Commit()
	require.NoError(t, err)

	cases := []struct {
		name string
		run  func() ([][]interface{}, error)
	}{
		{"a tuple input", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?vec :in $ [?vec ?n] :where [?e :t/name "alice"]]`,
				[]interface{}{[]float64{negativeZero, 1.5}, int64(1)}))
		}},
		{"a relation input", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?vec :in $ [[?vec ?n]] :where [?e :t/name "alice"]]`,
				[][]interface{}{{[]float64{negativeZero, 1.5}, int64(1)}}))
		}},
		{"a collection input of vectors", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?vec :in $ [?vec ...] :where [?e :t/name "alice"]]`,
				[][]float64{{negativeZero, 1.5}}))
		}},
		{"a vector written to a vector attribute", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?vec :where [?e :t/name "alice"] [?e :t/vals ?vec]]`))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			answered, err := c.run()
			require.NoError(t, err)
			require.Len(t, answered, 1)
			element := reflect.ValueOf(answered[0][0]).Index(0).Interface().(float64)
			require.False(t, math.Signbit(element), "the vector's first element answers 0.0, got %v of %#v", element, answered[0][0])
		})
	}
}

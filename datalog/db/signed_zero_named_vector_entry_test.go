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

// namedVector is a caller's named float vector type, admitted by the domain
// as a vector of float64.
type namedVector []float64

// A signed zero inside a caller's named float vector type is 0.0 at every
// place a vector enters the engine: a tuple input, a relation input, a
// collection input whose members are named vectors, and a named vector
// written to a vector attribute and read back.
func TestASignedZeroInsideANamedVectorIsOneValueAtEveryEntry(t *testing.T) {
	alice := datalog.NewIdentity("signed-zero-named-alice")
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
	require.NoError(t, tx.Set(alice, datalog.NewKeyword(":t/vals"), namedVector{negativeZero, 1.5}))
	_, err = tx.Commit()
	require.NoError(t, err)

	cases := []struct {
		name string
		run  func() ([][]interface{}, error)
	}{
		{"a tuple input", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?vec :in $ [?vec ?n] :where [?e :t/name "alice"]]`,
				[]interface{}{namedVector{negativeZero, 1.5}, int64(1)}))
		}},
		{"a relation input", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?vec :in $ [[?vec ?n]] :where [?e :t/name "alice"]]`,
				[][]interface{}{{namedVector{negativeZero, 1.5}, int64(1)}}))
		}},
		{"a collection input of named vectors", func() ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(`[:find ?vec :in $ [?vec ...] :where [?e :t/name "alice"]]`,
				[]namedVector{{negativeZero, 1.5}}))
		}},
		{"a named vector written to a vector attribute", func() ([][]interface{}, error) {
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

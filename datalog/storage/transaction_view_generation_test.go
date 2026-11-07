package storage

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

var (
	resolutionName  = datalog.NewKeyword(":resolution/name")
	resolutionOne   = datalog.NewKeyword(":resolution/one")
	resolutionKey   = datalog.NewKeyword(":resolution/key")
	resolutionTags  = datalog.NewKeyword(":resolution/tags")
	resolutionCells = datalog.NewKeyword(":resolution/cells")
	resolutionOrder = datalog.NewKeyword(":resolution/order")
)

// resolutionAttributes are the attributes the generated writes reach, one of
// each kind a transaction's view resolves.
var resolutionAttributes = []datalog.Keyword{resolutionOne, resolutionKey, resolutionTags, resolutionCells, resolutionOrder}

func resolutionSchema(t testing.TB) schema.SchemaProvider {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(resolutionName.String()).Type(schema.TypeString).Add().
		Attribute(resolutionOne.String()).Type(schema.TypeString).Add().
		Attribute(resolutionKey.String()).Type(schema.TypeString).Unique(schema.UniqueIdentity).Add().
		Attribute(resolutionTags.String()).Type(schema.TypeString).Many().Add().
		Attribute(resolutionCells.String()).Type(schema.TypeString).Vector().Add().
		Attribute(resolutionOrder.String()).Type(schema.TypeString).OrderedSet().Add().
		Build()
	require.NoError(t, err)
	return s
}

// resolutionWrite is one generated write and the text that names it in a
// failure message.
type resolutionWrite struct {
	text  string
	apply func(tx *Transaction) error
}

// drawResolutionWrite draws one write to one of entities.
func drawResolutionWrite(r *rand.Rand, entities []datalog.Identity, attributes []datalog.Keyword) resolutionWrite {
	values := []string{"a", "b", "c", "d"}
	keys := []string{"k1", "k2", "k3"}
	n := r.Intn(len(entities))
	e := entities[n]
	attr := attributes[r.Intn(len(attributes))]
	pick := values
	if attr == resolutionKey {
		pick = keys
	}
	v := pick[r.Intn(len(pick))]
	switch r.Intn(3) {
	case 0:
		return resolutionWrite{fmt.Sprintf("e%d Add %s %s", n, attr, v), func(tx *Transaction) error { return tx.Add(e, attr, v) }}
	case 1:
		return resolutionWrite{fmt.Sprintf("e%d Remove %s %s", n, attr, v), func(tx *Transaction) error { return tx.Remove(e, attr, v) }}
	}
	switch attr {
	case resolutionOne, resolutionKey:
		return resolutionWrite{fmt.Sprintf("e%d Set %s %s", n, attr, v), func(tx *Transaction) error { return tx.Set(e, attr, v) }}
	}
	var slice []interface{}
	seen := map[string]bool{}
	for i, length := 0, r.Intn(4); i < length; i++ {
		v := values[r.Intn(len(values))]
		if attr != resolutionCells && seen[v] {
			continue
		}
		seen[v] = true
		slice = append(slice, v)
	}
	return resolutionWrite{fmt.Sprintf("e%d Set %s %v", n, attr, slice), func(tx *Transaction) error { return tx.Set(e, attr, slice) }}
}

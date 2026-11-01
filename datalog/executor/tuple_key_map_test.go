package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TupleKeyMap holds one value per key, the key read in place from the member
// the caller gave at the stored positions and probed by a tuple at the
// probing positions (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T2). Each
// contract below is the subject of one test.

// A value put under a key is answered by a probe at the probing positions,
// which need not be the held tuple's. A later put replaces it. A
// put-if-absent keeps the first value and reports the key held. A key absent
// answers nothing.
func TestTupleKeyMap_HoldsOneValuePerKeyProbedByThePositionsGiven(t *testing.T) {
	e1, e2, e3 := datalog.NewIdentity("tk:e1"), datalog.NewIdentity("tk:e2"), datalog.NewIdentity("tk:e3")
	m := NewTupleKeyMap([]int{2}, []int{0}, 0)
	m.Put(Tuple{e1}, "folded")
	m.Put(Tuple{e2}, "sewn")
	assert.Equal(t, 2, m.Len())

	value, ok := m.Get(Tuple{"first quire", "x", e1})
	assert.True(t, ok)
	assert.Equal(t, "folded", value)

	m.Put(Tuple{e1}, "pressed")
	value, ok = m.Get(Tuple{"first quire", "x", e1})
	assert.True(t, ok)
	assert.Equal(t, "pressed", value, "a later put replaces the value")
	assert.Equal(t, 2, m.Len(), "a replaced value adds no key")

	assert.True(t, m.PutIfAbsent(Tuple{e2}, "bound"), "the key is held")
	value, _ = m.Get(Tuple{"second quire", "y", e2})
	assert.Equal(t, "sewn", value, "a put-if-absent keeps the first value")
	assert.False(t, m.PutIfAbsent(Tuple{e3}, "cut"), "the key is new")
	value, ok = m.Get(Tuple{"third quire", "z", e3})
	assert.True(t, ok)
	assert.Equal(t, "cut", value)
	assert.Equal(t, 3, m.Len())

	_, ok = m.Get(Tuple{"fourth quire", "w", datalog.NewIdentity("tk:absent")})
	assert.False(t, ok)
}

// Two distinct keys sharing one hash hold two values, and a third key under
// the same hash answers nothing. int64(0) and false are such keys under
// HashValue, as is the zero ElementID.
func TestTupleKeyMap_TwoKeysUnderOneHashAreToldApart(t *testing.T) {
	zero, no, absent := int64(0), false, datalog.ElementID{}
	require.Equal(t, datalog.HashValue(zero), datalog.HashValue(no), "the fixture's two keys share one hash")
	require.Equal(t, datalog.HashValue(zero), datalog.HashValue(absent), "the fixture's absent key shares it")
	require.False(t, datalog.ValuesEqual(zero, no), "the fixture's two keys are distinct values")

	m := NewTupleKeyMap([]int{0}, []int{0}, 0)
	m.Put(Tuple{zero}, "zero")
	m.Put(Tuple{no}, "no")
	value, ok := m.Get(Tuple{zero})
	assert.True(t, ok)
	assert.Equal(t, "zero", value)
	value, ok = m.Get(Tuple{no})
	assert.True(t, ok)
	assert.Equal(t, "no", value)
	_, ok = m.Get(Tuple{absent})
	assert.False(t, ok)
	assert.Equal(t, 2, m.Len())
}

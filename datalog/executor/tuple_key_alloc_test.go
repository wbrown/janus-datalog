package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// A put of a new key allocates nothing beyond the map's growth, a probe of a
// held key at the probing positions allocates nothing, and a put-if-absent of
// a held key allocates nothing (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T3).
// The members are held at the positions 0 and 1, and probed at the positions
// 0 and 2 of a wider tuple. AllocsPerRun runs its function once unmeasured
// before the runs it counts, so the keys outnumber the runs by one.
func TestTupleKeyMap_PutAllocatesOnlyItsGrowthAndAProbeNothing(t *testing.T) {
	kwA := datalog.NewKeyword(":alloc/a")
	kwB := datalog.NewKeyword(":alloc/b")
	probePos := []int{0, 2}
	storedPos := allPositions(2)

	t.Run("a put of a new key allocates only the growth", func(t *testing.T) {
		const runs = 1000
		keys := make([]Tuple, runs+1)
		for i := range keys {
			keys[i] = Tuple{kwA, int64(i)}
		}
		m := NewTupleKeyMap(probePos, storedPos, 0)
		next := 0
		perPut := testing.AllocsPerRun(runs, func() {
			m.Put(keys[next], struct{}{})
			next++
		})
		if perPut >= 0.1 {
			t.Errorf("a put of a new key allocated %v objects per run beyond the map's growth", perPut)
		}
		if m.Len() != len(keys) {
			t.Errorf("the map holds %d keys, want %d", m.Len(), len(keys))
		}
	})

	t.Run("a probe of a held key allocates nothing", func(t *testing.T) {
		m := NewTupleKeyMap(probePos, storedPos, 0)
		m.Put(Tuple{kwA, kwB}, "stored")
		tuple := Tuple{kwA, int64(42), kwB}
		allocs := testing.AllocsPerRun(100, func() {
			if got, ok := m.Get(tuple); !ok || got != "stored" {
				t.Fatalf("the probe at the probing positions must find the held key: got %v, found %v", got, ok)
			}
		})
		if allocs != 0 {
			t.Errorf("a probe of a held key allocated %v objects per run, want 0", allocs)
		}
	})

	t.Run("a put-if-absent of a held key allocates nothing", func(t *testing.T) {
		m := NewTupleKeyMap(probePos, storedPos, 0)
		member := Tuple{kwA, kwB}
		m.PutIfAbsent(member, struct{}{})
		allocs := testing.AllocsPerRun(100, func() {
			if existed := m.PutIfAbsent(member, struct{}{}); !existed {
				t.Fatal("a held key must report held")
			}
		})
		if allocs != 0 {
			t.Errorf("a put-if-absent of a held key allocated %v objects per run, want 0", allocs)
		}
	})
}

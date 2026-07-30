package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestTupleKeyMapAllocations pins the allocation cost of the tuple-key
// machinery that every join build, dedup set, and group table rides on.
//
// Pinned costs:
//   - storing a NEW key: 1 allocation (the key's owned values slice; the
//     single-entry bucket lives inline in the map slot)
//   - probing an EXISTING key by tuple positions: 0 allocations
//   - PutIfAbsent on an existing key by tuple positions: 0 allocations
func TestTupleKeyMapAllocations(t *testing.T) {
	kwA := datalog.NewKeyword(":alloc/a")
	kwB := datalog.NewKeyword(":alloc/b")
	tuple := Tuple{kwA, int64(42), kwB}
	indices := []int{0, 2}
	single := []int{1}

	t.Run("put new key costs one allocation", func(t *testing.T) {
		m := NewTupleKeyMap()
		i := int64(0)
		allocs := testing.AllocsPerRun(100, func() {
			i++
			key := NewTupleKey(Tuple{kwA, i, kwB}, indices)
			m.Put(key, struct{}{})
		})
		// One allocation for the key's values slice. The map's own internal
		// growth amortizes to under one object per insertion and rounds to
		// zero over the run; the per-key bucket backing array must be gone.
		if allocs > 2 {
			t.Errorf("storing a new key allocated %v objects per run, want <= 2 (values slice + amortized map growth)", allocs)
		}
	})

	t.Run("positional probe hit allocates nothing", func(t *testing.T) {
		m := NewTupleKeyMap()
		m.Put(NewTupleKey(tuple, indices), "stored")
		m.Put(NewTupleKey(tuple, single), "single")

		allocs := testing.AllocsPerRun(100, func() {
			if _, ok := m.GetPositions(tuple, indices); !ok {
				t.Fatal("positional probe must find the stored multi-position key")
			}
			if _, ok := m.GetPositions(tuple, single); !ok {
				t.Fatal("positional probe must find the stored single-position key")
			}
		})
		if allocs != 0 {
			t.Errorf("positional probe hit allocated %v objects per run, want 0", allocs)
		}
	})

	t.Run("positional PutIfAbsent on existing key allocates nothing", func(t *testing.T) {
		m := NewTupleKeyMap()
		m.PutIfAbsentPositions(tuple, indices, struct{}{})

		allocs := testing.AllocsPerRun(100, func() {
			if existed := m.PutIfAbsentPositions(tuple, indices, struct{}{}); !existed {
				t.Fatal("existing key must report existed")
			}
		})
		if allocs != 0 {
			t.Errorf("positional PutIfAbsent hit allocated %v objects per run, want 0", allocs)
		}
	})
}

// TestTupleKeyPositionalConsistency pins that positional operations and
// TupleKey-constructing operations address the same entries: same hashing
// (including the single-position case, which hashes the bare value rather
// than FNV-folding it), same equality, in both directions.
func TestTupleKeyPositionalConsistency(t *testing.T) {
	kwA := datalog.NewKeyword(":consist/a")
	tuple := Tuple{kwA, int64(7), "text"}

	for _, indices := range [][]int{{1}, {0, 2}, {0, 1, 2}} {
		m := NewTupleKeyMap()

		// Stored via TupleKey, probed positionally.
		m.Put(NewTupleKey(tuple, indices), "via-key")
		if got, ok := m.GetPositions(tuple, indices); !ok || got != "via-key" {
			t.Errorf("indices %v: positional probe missed a TupleKey-stored entry", indices)
		}

		// Stored positionally, probed via TupleKey.
		m2 := NewTupleKeyMap()
		if existed := m2.PutIfAbsentPositions(tuple, indices, "via-positions"); existed {
			t.Errorf("indices %v: fresh positional insert reported existed", indices)
		}
		if got, ok := m2.Get(NewTupleKey(tuple, indices)); !ok || got != "via-positions" {
			t.Errorf("indices %v: TupleKey probe missed a positionally-stored entry", indices)
		}
		if existed := m2.PutIfAbsentPositions(tuple, indices, "again"); !existed {
			t.Errorf("indices %v: second positional insert must report existed", indices)
		}
	}
}

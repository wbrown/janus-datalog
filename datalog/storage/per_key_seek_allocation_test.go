package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// seekNPattern is [?e :seek/n ?v] over the seek fixture, with its symbols.
func seekNPattern() (*query.DataPattern, []query.Symbol) {
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	return &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":seek/n")},
		query.Variable{Name: v},
	}}, []query.Symbol{e, v}
}

// A seek of an open memory-trees scan allocates nothing, and the datoms the
// scan yields after it are the sought run's, whether the run lies after or
// before the one read last
// (docs/proposals/ONE_SCAN_SOUGHT_PER_KEY_PROOF.md, T4, L5).
func TestMemoryTreeIterator_SeekAllocatesNothing(t *testing.T) {
	mode := pinned(optimizerModes, false)[0]
	f := newSeekFixture(t, mode, mode.plannerOptions(), nil)
	m := f.db.Matcher().(*PatternMatcher)
	pattern, _ := seekNPattern()

	scan, err := OpenKeyScan(m.reader, DiscardIntake, m.patternScanBound(pattern, AETV))
	require.NoError(t, err)
	defer scan.Close()

	for _, entity := range []datalog.Identity{f.entities[5], f.entities[2]} {
		run := m.patternScanBoundWithBinding(pattern, AETV, 0, entity)
		assert.Zero(t, testing.AllocsPerRun(100, func() { scan.Seek(run) }), "allocations of a seek")
		require.NoError(t, scan.Error())
		var got []datalog.Identity
		for scan.Next() {
			datom, err := scan.Datom()
			require.NoError(t, err)
			got = append(got, datom.E)
		}
		require.NoError(t, scan.Error())
		require.Len(t, got, 1, "the run after the seek is the sought key's one datom")
		assert.True(t, got[0] == entity, "the datom after the seek is the sought key's")
	}
}

// A binding sought per key allocates per key what its bounds allocate and
// nothing more: on memory-trees a match over many keys, drained, allocates
// beyond a match over few what keyScanBounds allocates for the extra keys,
// within half an allocation per key for the growth of the keys slice
// (docs/proposals/ONE_SCAN_SOUGHT_PER_KEY_PROOF.md, T2, T4, L6). The
// bindings and the keys are built outside the measured calls, since they
// are the caller's.
func TestPerKeySeekAllocatesPerKeyWhatItsBoundsAllocate(t *testing.T) {
	mode := pinned(optimizerModes, false)[0]
	f := newSeekFixture(t, mode, mode.plannerOptions(), nil)
	m := f.db.Matcher().(*PatternMatcher)
	pattern, symbols := seekNPattern()
	const few, many = 2, 34
	extent := len(m.patternScanBound(pattern, AETV).Prefix)

	match := func(binding int) float64 {
		rel := entityBindings(f.entities[:binding])
		return testing.AllocsPerRun(20, func() {
			matched, err := m.matchWithHashJoin(pattern, rel, symbols, 0, AETV, nil)
			require.NoError(t, err)
			it := matched.Iterator()
			answered := 0
			for it.Next() {
				answered++
			}
			require.NoError(t, it.Close())
			require.Equal(t, binding, answered, "every key answers its one datom")
		})
	}
	bounds := func(binding int) float64 {
		keys := make([]interface{}, binding)
		for i, entity := range f.entities[:binding] {
			keys[i] = entity
		}
		return testing.AllocsPerRun(20, func() {
			require.NotNil(t, m.keyScanBounds(pattern, AETV, 0, keys, extent), "the index binds every key")
		})
	}

	perKey := (match(many) - match(few)) / (many - few)
	perKeyBounds := (bounds(many) - bounds(few)) / (many - few)
	assert.LessOrEqual(t, perKey, perKeyBounds+0.5,
		"allocations per key of a match beyond its bounds' %.2f per key", perKeyBounds)
}

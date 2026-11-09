package executor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// unboundScanOf is the fingerprint of the unbound scan [?e <attr> ?v] with no
// order-by and no limit.
func unboundScanOf(attr string) ScanFingerprint {
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?e")},
		query.Constant{Value: datalog.NewKeyword(attr)},
		query.Variable{Name: datalog.NewSymbol("?v")},
	}}
	return FingerprintScan(query.PatternQuery(pattern), pattern)
}

// TestScanRegistry_MissReturnsNil verifies that Get() on an empty or
// unmatched registry returns nil.
func TestScanRegistry_MissReturnsNil(t *testing.T) {
	reg := NewScanRegistry()

	shared := reg.Get(unboundScanOf(":registry/absent"))
	assert.Nil(t, shared)
}

// TestScanRegistry_PutThenGet verifies that Put() stores and Get() retrieves
// the same SharedScan object.
func TestScanRegistry_PutThenGet(t *testing.T) {
	reg := NewScanRegistry()

	seq := &LazySeq{}
	syms := []query.Symbol{datalog.NewSymbol("?t"), datalog.NewSymbol("?s")}
	properties := RelationProperties{Keys: [][]query.Symbol{{syms[0]}}}
	options := ExecutorOptions{EnableTrueStreaming: true}

	reg.Put(unboundScanOf(":registry/first"), &SharedScan{
		Seq:        seq,
		Symbols:    syms,
		Options:    options,
		Properties: properties,
	})

	shared := reg.Get(unboundScanOf(":registry/first"))
	require.NotNil(t, shared)
	assert.Equal(t, seq, shared.Seq)
	assert.Equal(t, syms, shared.Symbols)
	assert.Equal(t, options, shared.Options)
	assert.Equal(t, properties, shared.Properties)
}

// TestScanRegistry_DifferentKeysIndependent verifies that two different
// fingerprints store and retrieve independently.
func TestScanRegistry_DifferentKeysIndependent(t *testing.T) {
	reg := NewScanRegistry()

	seq1 := &LazySeq{}
	seq2 := &LazySeq{}
	syms1 := []query.Symbol{datalog.NewSymbol("?a")}
	syms2 := []query.Symbol{datalog.NewSymbol("?b")}

	reg.Put(unboundScanOf(":registry/first"), &SharedScan{Seq: seq1, Symbols: syms1})
	reg.Put(unboundScanOf(":registry/second"), &SharedScan{Seq: seq2, Symbols: syms2})

	s1 := reg.Get(unboundScanOf(":registry/first"))
	s2 := reg.Get(unboundScanOf(":registry/second"))
	require.NotNil(t, s1)
	require.NotNil(t, s2)
	assert.Same(t, seq1, s1.Seq, "the first scan should return seq1")
	assert.Same(t, seq2, s2.Seq, "the second scan should return seq2")
	assert.NotSame(t, s1.Seq, s2.Seq, "different keys should return different objects")
}

// TestScanRegistry_ConcurrentAccess verifies thread safety under concurrent
// Put/Get operations. Run with -race to detect data races.
func TestScanRegistry_ConcurrentAccess(t *testing.T) {
	reg := NewScanRegistry()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			seq := &LazySeq{}
			syms := []query.Symbol{datalog.NewSymbol("?x")}
			fp := unboundScanOf(":registry/shared")

			// All goroutines try to Put the same key and Get it
			reg.Put(fp, &SharedScan{Seq: seq, Symbols: syms})
			shared := reg.Get(fp)
			assert.NotNil(t, shared)
		}(i)
	}
	wg.Wait()

	// Verify final state is consistent
	shared := reg.Get(unboundScanOf(":registry/shared"))
	require.NotNil(t, shared)
}

// TestScanRegistry_SymbolsPreserved verifies that stored symbols are
// retrievable and match what was Put().
func TestScanRegistry_SymbolsPreserved(t *testing.T) {
	reg := NewScanRegistry()

	syms := []query.Symbol{
		datalog.NewSymbol("?entity"),
		datalog.NewSymbol("?value"),
		datalog.NewSymbol("?tx"),
	}
	reg.Put(unboundScanOf(":registry/symbols"), &SharedScan{Seq: &LazySeq{}, Symbols: syms})

	shared := reg.Get(unboundScanOf(":registry/symbols"))
	require.NotNil(t, shared)
	require.Len(t, shared.Symbols, 3)
	assert.Equal(t, datalog.NewSymbol("?entity"), shared.Symbols[0])
	assert.Equal(t, datalog.NewSymbol("?value"), shared.Symbols[1])
	assert.Equal(t, datalog.NewSymbol("?tx"), shared.Symbols[2])
}

// Twenty sites the plan lists for one scan read it at once: the scan is
// created once, and every site is served the same scan and head.
func TestScanRegistryGetOrCreateConcurrent(t *testing.T) {
	const callers = 20
	phase := &query.Query{}
	plan := &planner.RealizedPlan{Phases: []planner.RealizedPhase{{Query: phase}}}
	sites := make([]*query.DataPattern, callers)
	for caller := range sites {
		sites[caller] = &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":registry/same")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		}}
		phase.Where = append(phase.Where, sites[caller])
		plan.Phases[0].Patterns = append(plan.Phases[0].Patterns, planner.PatternPlan{Pattern: sites[caller]})
	}
	registry := NewScanRegistry()
	registry.expect(sharedScanSites(plan))

	var creates atomic.Int64
	heads := make([]*LazySeq, callers)
	scans := make([]*SharedScan, callers)
	hits := make([]bool, callers)
	errs := make([]error, callers)
	var wait sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			site := sites[index]
			heads[index], scans[index], hits[index], errs[index] = registry.GetOrCreate(patternMatchQuery(phase, site), site, func() (*SharedScan, error) {
				creates.Add(1)
				return &SharedScan{Seq: &LazySeq{}}, nil
			})
		}(caller)
	}
	wait.Wait()

	require.Equal(t, int64(1), creates.Load())
	misses := 0
	for caller := 0; caller < callers; caller++ {
		require.NoError(t, errs[caller])
		require.NotNil(t, heads[caller], "site %d is served the head", caller)
		require.Same(t, heads[0], heads[caller])
		require.Same(t, scans[0], scans[caller])
		if !hits[caller] {
			misses++
		}
	}
	require.Equal(t, 1, misses, "the one site that created the scan missed")
}

func TestScanRegistryCloseReleasesPartialScans(t *testing.T) {
	closeErr := errors.New("registry close failure")
	source := failingRelation{
		Relation:  NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}}),
		failAfter: 100,
		closeErr:  closeErr,
	}
	registry := NewScanRegistry()
	registry.Put(unboundScanOf(":registry/partial"), &SharedScan{
		Seq:     NewTupleSeq(source.Iterator(), false),
		Symbols: testSymbols(),
	})

	require.ErrorIs(t, registry.Close(), closeErr)
	require.ErrorIs(t, registry.Close(), closeErr)
}

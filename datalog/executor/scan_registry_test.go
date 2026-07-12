package executor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestScanRegistry_MissReturnsNil verifies that Get() on an empty or
// unmatched registry returns nil.
func TestScanRegistry_MissReturnsNil(t *testing.T) {
	reg := NewScanRegistry()

	shared := reg.Get("nonexistent")
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

	reg.Put("fp1", &SharedScan{
		Seq:        seq,
		Symbols:    syms,
		Options:    options,
		Properties: properties,
	})

	shared := reg.Get("fp1")
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

	reg.Put("fp1", &SharedScan{Seq: seq1, Symbols: syms1})
	reg.Put("fp2", &SharedScan{Seq: seq2, Symbols: syms2})

	s1 := reg.Get("fp1")
	s2 := reg.Get("fp2")
	require.NotNil(t, s1)
	require.NotNil(t, s2)
	assert.Same(t, seq1, s1.Seq, "fp1 should return seq1")
	assert.Same(t, seq2, s2.Seq, "fp2 should return seq2")
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
			fp := "fp_shared"

			// All goroutines try to Put the same key and Get it
			reg.Put(fp, &SharedScan{Seq: seq, Symbols: syms})
			shared := reg.Get(fp)
			assert.NotNil(t, shared)
		}(i)
	}
	wg.Wait()

	// Verify final state is consistent
	shared := reg.Get("fp_shared")
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
	reg.Put("fp_symbols", &SharedScan{Seq: &LazySeq{}, Symbols: syms})

	shared := reg.Get("fp_symbols")
	require.NotNil(t, shared)
	require.Len(t, shared.Symbols, 3)
	assert.Equal(t, datalog.NewSymbol("?entity"), shared.Symbols[0])
	assert.Equal(t, datalog.NewSymbol("?value"), shared.Symbols[1])
	assert.Equal(t, datalog.NewSymbol("?tx"), shared.Symbols[2])
}

func TestScanRegistryGetOrCreateConcurrent(t *testing.T) {
	registry := NewScanRegistry()
	var creates atomic.Int64
	const callers = 20
	results := make([]*SharedScan, callers)
	errors := make([]error, callers)
	var wait sync.WaitGroup
	for caller := 0; caller < callers; caller++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			results[index], _, errors[index] = registry.GetOrCreate("same", func() (*SharedScan, error) {
				creates.Add(1)
				return &SharedScan{Seq: &LazySeq{}}, nil
			})
		}(caller)
	}
	wait.Wait()

	require.Equal(t, int64(1), creates.Load())
	for caller := 0; caller < callers; caller++ {
		require.NoError(t, errors[caller])
		require.Same(t, results[0], results[caller])
	}
}

func TestScanRegistryCloseReleasesPartialScans(t *testing.T) {
	closeErr := errors.New("registry close failure")
	source := failingRelation{
		Relation:  NewMaterializedRelation(testSymbols(), []Tuple{{int64(1)}, {int64(2)}}),
		failAfter: 100,
		closeErr:  closeErr,
	}
	registry := NewScanRegistry()
	registry.Put("partial", &SharedScan{
		Seq:     NewTupleSeq(source.Iterator(), false),
		Symbols: testSymbols(),
	})

	require.ErrorIs(t, registry.Close(), closeErr)
	require.ErrorIs(t, registry.Close(), closeErr)
}

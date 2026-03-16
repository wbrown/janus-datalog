package executor

import (
	"sync"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// ScanRegistry holds shared scan results scoped to a single top-level query.
// When multiple subqueries scan the same unbound pattern (e.g., [?t :task/root ?s]),
// the first scan creates a LazySeq over the storage iterator and registers it.
// Subsequent scans with the same fingerprint return the registered LazySeq —
// already-realized cells are read from cache, unrealized cells advance the
// shared iterator.
type ScanRegistry struct {
	mu    sync.Mutex
	scans map[string]*SharedScan
}

// SharedScan holds a shared LazySeq and the original symbols from the first scan.
type SharedScan struct {
	Seq     *LazySeq       // shared lazy sequence over storage iterator
	Symbols []query.Symbol // original symbols from first scan
}

// NewScanRegistry creates a new empty scan registry.
func NewScanRegistry() *ScanRegistry {
	return &ScanRegistry{
		scans: make(map[string]*SharedScan),
	}
}

// Get returns the shared scan for the given fingerprint, or nil if not found.
func (r *ScanRegistry) Get(fingerprint string) *SharedScan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.scans[fingerprint]
}

// Put stores a shared scan for the given fingerprint.
func (r *ScanRegistry) Put(fingerprint string, seq *LazySeq, symbols []query.Symbol) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scans[fingerprint] = &SharedScan{
		Seq:     seq,
		Symbols: symbols,
	}
}

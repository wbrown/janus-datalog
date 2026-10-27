package executor

import (
	"sync"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// ScanRegistry holds shared scan results scoped to a single top-level query.
// When multiple subqueries scan the same unbound pattern (e.g., [?t :task/root ?s]),
// the first scan creates a LazySeq over the storage iterator and registers it.
// Subsequent scans with an equal fingerprint return the registered LazySeq —
// already-realized cells are read from cache, unrealized cells advance the
// shared iterator.
//
// Entries are bucketed by the fingerprint's hash and served only when their
// fingerprint equals the one asked for, so a hash collision costs a
// comparison, never another pattern's tuples.
type ScanRegistry struct {
	mu    sync.Mutex
	scans map[uint64][]scanEntry
}

type scanEntry struct {
	fingerprint ScanFingerprint
	scan        *SharedScan
}

// SharedScan holds a shared LazySeq and the original symbols from the first scan.
type SharedScan struct {
	Seq        *LazySeq       // shared lazy sequence over storage iterator
	Symbols    []query.Symbol // original symbols from first scan
	Options    ExecutorOptions
	Properties RelationProperties
}

// NewScanRegistry creates a new empty scan registry.
func NewScanRegistry() *ScanRegistry {
	return &ScanRegistry{
		scans: make(map[uint64][]scanEntry),
	}
}

// Get returns the shared scan under fingerprint, or nil if none is registered.
func (r *ScanRegistry) Get(fingerprint ScanFingerprint) *SharedScan {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lookup(fingerprint)
}

// lookup finds the scan registered under an equal fingerprint, under the
// lock; nil when none is.
func (r *ScanRegistry) lookup(fingerprint ScanFingerprint) *SharedScan {
	for _, entry := range r.scans[fingerprint.hash] {
		if entry.fingerprint.Equal(fingerprint) {
			return entry.scan
		}
	}
	return nil
}

// Put stores a shared scan under fingerprint, in place of one registered
// under an equal fingerprint.
func (r *ScanRegistry) Put(fingerprint ScanFingerprint, scan *SharedScan) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.register(fingerprint, scan)
}

// register stores scan under fingerprint, under the lock.
func (r *ScanRegistry) register(fingerprint ScanFingerprint, scan *SharedScan) {
	entries := r.scans[fingerprint.hash]
	for i := range entries {
		if entries[i].fingerprint.Equal(fingerprint) {
			entries[i].scan = scan
			return
		}
	}
	r.scans[fingerprint.hash] = append(entries, scanEntry{fingerprint: fingerprint, scan: scan})
}

func (r *ScanRegistry) GetOrCreate(
	fingerprint ScanFingerprint,
	create func() (*SharedScan, error),
) (scan *SharedScan, hit bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.lookup(fingerprint); existing != nil {
		return existing, true, nil
	}
	scan, err = create()
	if err != nil {
		return nil, false, err
	}
	r.register(fingerprint, scan)
	return scan, false, nil
}

func (r *ScanRegistry) Close() error {
	r.mu.Lock()
	scans := make([]*SharedScan, 0, len(r.scans))
	for _, entries := range r.scans {
		for _, entry := range entries {
			scans = append(scans, entry.scan)
		}
	}
	r.mu.Unlock()

	var firstErr error
	for _, scan := range scans {
		if scan != nil && scan.Seq != nil {
			if err := scan.Seq.Close(); firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

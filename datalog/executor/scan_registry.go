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
	Seq        *LazySeq       // shared lazy sequence over storage iterator
	Symbols    []query.Symbol // original symbols from first scan
	Options    ExecutorOptions
	Properties RelationProperties
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
func (r *ScanRegistry) Put(fingerprint string, scan *SharedScan) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scans[fingerprint] = scan
}

func (r *ScanRegistry) GetOrCreate(
	fingerprint string,
	create func() (*SharedScan, error),
) (scan *SharedScan, hit bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.scans[fingerprint]; existing != nil {
		return existing, true, nil
	}
	scan, err = create()
	if err != nil {
		return nil, false, err
	}
	r.scans[fingerprint] = scan
	return scan, false, nil
}

func (r *ScanRegistry) Close() error {
	r.mu.Lock()
	scans := make([]*SharedScan, 0, len(r.scans))
	for _, scan := range r.scans {
		scans = append(scans, scan)
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

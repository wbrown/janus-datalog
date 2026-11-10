package executor

import (
	"sync"

	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ScanRegistry holds the shared scans of a single top-level query. The
// query's plan lists, by fingerprint, the sites at which it reads an unbound
// scan, and a fingerprint read at two or more sites, or at a site repeating
// in every window of a form, is shared (sharedScanSites, expect). The first
// listed site to match creates a LazySeq over the storage iterator, and every
// listed site is handed its head: already-realized cells are read from cache,
// unrealized cells advance the shared iterator. The registry holds the head
// until every site read once has been handed it and every form ending a
// repeating site's reads has ended (ended), then drops it; each reader's
// relation keeps its own. No reader's close closes the shared iterator: it
// closes when read to its end, or when the query ends (Close).
//
// Entries are bucketed by the fingerprint's hash and served only when their
// fingerprint equals the one asked for, so a hash collision costs a
// comparison, never another pattern's tuples.
type ScanRegistry struct {
	mu      sync.Mutex
	scans   map[uint64][]*scanEntry
	bySite  map[*query.DataPattern]*scanEntry
	byScope map[*planner.RealizedPlan][]*scanEntry
	// existence is the site of each windowed pattern's existence check, as
	// the query's site walk made it (scanSites.existence).
	existence map[*query.DataPattern]*query.DataPattern
}

type scanEntry struct {
	fingerprint ScanFingerprint
	scan        *SharedScan
	// unread is each site the plan reads once, true until the site has been
	// handed the head.
	unread map[*query.DataPattern]bool
	// repeats is each site read in every window of a form; no read of one
	// is its last.
	repeats map[*query.DataPattern]bool
	// open is each scope of a repeating site, the outermost form's frame on
	// its path, with the forms still to end it.
	open map[*planner.RealizedPlan]int
	// released records that the head is dropped, whether or not a site's
	// read created the scan.
	released bool
	// close closes the scan's source. It reaches the iterator's state, not
	// the head, so the entry keeps no cell by holding it.
	close func() error
	// closed reports whether the scan's source has been closed, at the
	// chain's end or by an earlier Close; a source closed at its end answered
	// its failure to the reader that reached the end. It reaches the
	// iterator's state as close does.
	closed func() bool
}

// release drops the head once no site read once is waiting for it and no
// scope of a repeating site is open.
func (e *scanEntry) release() {
	for _, waiting := range e.unread {
		if waiting {
			return
		}
	}
	if len(e.open) > 0 {
		return
	}
	e.released = true
	if e.scan != nil {
		e.scan.Seq = nil
	}
}

// SharedScan holds a shared LazySeq and the original symbols from the first scan.
type SharedScan struct {
	Seq        *LazySeq       // the head, nil once the registry drops it
	Symbols    []query.Symbol // original symbols from first scan
	Options    ExecutorOptions
	Properties RelationProperties
}

// NewScanRegistry creates a new empty scan registry.
func NewScanRegistry() *ScanRegistry {
	return &ScanRegistry{
		scans:     make(map[uint64][]*scanEntry),
		bySite:    make(map[*query.DataPattern]*scanEntry),
		byScope:   make(map[*planner.RealizedPlan][]*scanEntry),
		existence: make(map[*query.DataPattern]*query.DataPattern),
	}
}

// expect lists the sites a query's plan reads each shared scan at, the scopes
// that end each repeating site's reads, and the site of each windowed
// pattern's existence check.
func (r *ScanRegistry) expect(sites scanSites) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for pattern, site := range sites.existence {
		r.existence[pattern] = site
	}
	for _, entries := range sites.buckets {
		for _, listing := range entries {
			entry := &scanEntry{
				fingerprint: listing.fingerprint,
				unread:      make(map[*query.DataPattern]bool, len(listing.sites)),
				repeats:     make(map[*query.DataPattern]bool, len(listing.repeats)),
				open:        make(map[*planner.RealizedPlan]int),
			}
			for _, site := range listing.sites {
				r.bySite[site] = entry
				scopes, repeating := listing.repeats[site]
				if !repeating {
					entry.unread[site] = true
					continue
				}
				entry.repeats[site] = true
				for _, scope := range scopes {
					if _, listed := entry.open[scope]; listed {
						continue
					}
					entry.open[scope] = sites.owners[scope]
					r.byScope[scope] = append(r.byScope[scope], entry)
				}
			}
			r.scans[listing.fingerprint.hash] = append(r.scans[listing.fingerprint.hash], entry)
		}
	}
}

// existenceSite answers the site a windowed pattern's existence check is
// matched at: the site the query's walk made for it, or the pattern itself
// when no walk listed the query's sites, as with scan sharing off, where no
// matcher reads a site. It takes no lock: expect writes the map once, before
// the plan runs, and every read comes after that write.
func (r *ScanRegistry) existenceSite(pattern *query.DataPattern) *query.DataPattern {
	if site, made := r.existence[pattern]; made {
		return site
	}
	return pattern
}

// ended records that one form holding scope as its frame has ended: its
// subject ran out, or it was closed. A scope every form holding it has ended
// leaves its entries, and an entry with nothing left waiting drops its head.
func (r *ScanRegistry) ended(scope *planner.RealizedPlan) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.byScope[scope] {
		remaining, open := entry.open[scope]
		if !open {
			continue
		}
		if remaining > 1 {
			entry.open[scope] = remaining - 1
			continue
		}
		delete(entry.open, scope)
		entry.release()
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
// under an equal fingerprint. A scan put here is listed for no site.
func (r *ScanRegistry) Put(fingerprint ScanFingerprint, scan *SharedScan) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.scans[fingerprint.hash] {
		if entry.fingerprint.Equal(fingerprint) {
			entry.scan = scan
			entry.close = scan.Seq.closer
			entry.closed = scan.Seq.sourceClosed
			return
		}
	}
	r.scans[fingerprint.hash] = append(r.scans[fingerprint.hash], &scanEntry{
		fingerprint: fingerprint,
		scan:        scan,
		close:       scan.Seq.closer,
		closed:      scan.Seq.sourceClosed,
	})
}

// GetOrCreate serves a match of q at site from the shared scan the plan lists
// the site for. The first listed site's match creates the scan through create,
// and hit reports whether it existed. It answers a nil head, the match not
// served, for a site the plan lists for no shared scan, for a q whose
// fingerprint is not the scan's, and for a scan whose head the registry has
// dropped. A site read once is marked handed the head, and when nothing is
// left waiting the registry drops it.
func (r *ScanRegistry) GetOrCreate(
	q *query.Query,
	site *query.DataPattern,
	create func() (*SharedScan, error),
) (head *LazySeq, scan *SharedScan, hit bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.bySite[site]
	if entry == nil || !entry.fingerprint.Equal(FingerprintScan(q, site)) {
		return nil, nil, false, nil
	}
	if entry.released {
		return nil, nil, false, nil
	}
	hit = entry.scan != nil
	if !hit {
		created, err := create()
		if err != nil {
			return nil, nil, false, err
		}
		entry.scan = created
		entry.close = created.Seq.closer
		entry.closed = created.Seq.sourceClosed
	}
	head = entry.scan.Seq
	if entry.unread[site] {
		entry.unread[site] = false
		entry.release()
	}
	return head, entry.scan, hit, nil
}

// Close closes, at the query's end, the source of every scan the registry
// holds that is still open, and answers those closes' failures. A source the
// chain's end closed answered its failure to the reader that reached the end,
// and a second Close closes nothing, so each failure reaches the query's
// reader once.
func (r *ScanRegistry) Close() error {
	r.mu.Lock()
	var closes []func() error
	for _, entries := range r.scans {
		for _, entry := range entries {
			if entry.close == nil || entry.closed() {
				continue
			}
			closes = append(closes, entry.close)
		}
	}
	r.mu.Unlock()

	var firstErr error
	for _, close := range closes {
		if err := close(); firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

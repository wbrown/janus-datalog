package storage

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// scanReport accounts for the scans opened on one arm's behalf, wherever they
// are opened.
//
// Every scan on a query path is acquired through a report, so an arm cannot
// obtain one without naming what it is for, and the completion event exists by
// construction rather than by the arm remembering to write one. Acquisition is
// the binding point rather than the iterator's lifetime because most arms do
// not hold their own iterator: they delegate to a resolver, which acquires
// through the same report and so records the run on the arm's behalf.
//
// What an arm can still get wrong is feeding resolved and matched, which shows
// as zeros under a positive intake — visible, where a missing event is not.
//
// There is no constructor and there are no accessors. An arm builds one inside
// its own handler check, together with the cause map, the clock read and the
// deferred close, because all four are observability and none of them should
// happen when nothing is listening:
//
//	var report *scanReport
//	if m.handler != nil {
//		report = &scanReport{
//			handler:  m.handler,
//			opened:   time.Now(),
//			strategy: annotations.ScanDirect,
//			cause:    map[string]interface{}{annotations.KeyPattern: pattern},
//		}
//		defer func() { report.close(matchErr == nil) }()
//	}
//
// A constructor that tested the handler itself would be handed a cause map the
// caller had already allocated — the same defect as an emit guarded inside the
// emitter while its caller builds the payload regardless.
type scanReport struct {
	handler  annotations.Handler
	opened   time.Time
	strategy annotations.ScanStrategy
	cause    map[string]interface{}

	// An arm addresses either one run or N peer runs, and which it is only the
	// arm knows: an arm that drives resolvers acquires many scans and still has
	// exactly one run, so the count cannot be inferred from acquisitions. run
	// is set by arms with a single bound; peers is counted by arms without one,
	// and stands in the bound's place. Subordinate reads are neither — they are
	// cost, carried by scanned.
	run   *ScanBound
	peers int

	// scanned accrues from each acquired scan as it closes. Taken there rather
	// than at the arm's close because a per-binding arm drops each scan before
	// opening the next, so by its own close every scan but the last is gone.
	scanned  int
	resolved int
	matched  int
}

// DiscardIntake is the report for a read no pattern asked for — schema
// reconstruction at open, the write path, export, cache warming. Passing it
// says nothing accounts for this scan.
var DiscardIntake *scanReport

// OpenScan opens a scan and attaches it to the report that accounts for it.
// Callers use this rather than the reader directly, which is what makes
// reporting a property of acquiring a scan: one way in, and it carries the
// accounting.
//
// It takes the reader rather than hanging off the matcher because the matcher
// is not the only thing that holds one — a pull batch, a prefetch and the write
// path each reach the store by their own field, and an opener only the matcher
// could call would leave every one of them with the reader as its shortest
// route. TestScanAcquisitionGoesThroughAReport enforces that there is no such
// route, which is only honest if this is reachable from wherever a scan is.
//
// The scan is unconditional because it is the work the arm asked for. Only the
// recording is tested, immediately before it, so a nil report costs the
// comparison and nothing else — no wrapper, no allocation.
func OpenScan(reader StoreReader, report *scanReport, bound ScanBound) (Iterator, error) {
	iter, err := reader.Scan(bound)
	if err != nil || report == nil {
		return iter, err
	}
	return &reportedIterator{Iterator: iter, report: report}, nil
}

// OpenKeyScan is OpenScan for reads that need no stored values.
func OpenKeyScan(reader StoreReader, report *scanReport, bound ScanBound) (Iterator, error) {
	iter, err := reader.ScanKeysOnly(bound)
	if err != nil || report == nil {
		return iter, err
	}
	return &reportedIterator{Iterator: iter, report: report}, nil
}

// close emits the arm's completion. completed says whether the arm ran to the
// end, so its counts read as a total rather than as a truncation.
//
// Callers test their report first. This builds a map and emits, and neither is
// work a discarding arm should reach.
func (r *scanReport) close(completed bool) {
	data := make(map[string]interface{}, len(r.cause)+3)
	for k, v := range r.cause {
		data[k] = v
	}
	data[annotations.KeyStrategy] = r.strategy
	if r.run != nil {
		addBoundFields(data, *r.run)
	}
	// An arm is described one way or the other: by the run it addressed, or —
	// having no single one — by how many peer runs it opened. Reported even at
	// zero, which is a real outcome on a path whose short-circuit answers
	// without opening a scan of its own.
	if r.run == nil {
		data[annotations.KeyScansOpened] = r.peers
	}
	emitScanCompletion(r.handler, annotations.StorageScanComplete, r.opened,
		scanFunnel{scanned: r.scanned, resolved: r.resolved, matched: r.matched},
		completed, data)
}

// reportedIterator hands its intake to the report when it closes. Taken at
// close rather than read by the arm later, because an arm that opens a scan per
// binding drops each one before the next and could otherwise account only for
// the last.
//
// Its report is non-nil by construction — the openers wrap only when they have
// one — so the accrual is a field increment with nothing to test.
type reportedIterator struct {
	Iterator
	report *scanReport
}

func (it *reportedIterator) Close() error {
	it.report.scanned += it.Iterator.Scanned()
	return it.Iterator.Close()
}

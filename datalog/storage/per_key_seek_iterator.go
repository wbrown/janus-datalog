package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// perKeySeekIterator yields the datoms of one narrowed scan per key, in turn,
// as one stream: the storage source of a hash join whose binding's keys are
// sought one by one (matchWithHashJoin). Each scan is acquired through the
// arm's report, so its intake accrues into the report as it closes and the
// scans are counted as the arm's peers, one run per key and no single run.
//
// A scan's failure ends the stream. The scan's deferred error and its close
// error are kept, the first of them, before the next key would be opened, so
// a failed seek never reads as the key having no datoms.
type perKeySeekIterator struct {
	reader StoreReader
	report *scanReport
	bounds []ScanBound

	next    int      // the position in bounds of the scan to open next
	scan    Iterator // the scan open now; nil between keys and once the keys are spent
	scanned int      // the intake of the scans closed so far
	err     error
}

func (it *perKeySeekIterator) Next() bool {
	for {
		if it.scan != nil {
			if it.scan.Next() {
				return true
			}
			if err := it.scan.Error(); err != nil && it.err == nil {
				it.err = err
			}
			it.scanned += it.scan.Scanned()
			if err := it.scan.Close(); err != nil && it.err == nil {
				it.err = err
			}
			it.scan = nil
			if it.err != nil {
				return false
			}
		}
		if it.next >= len(it.bounds) {
			return false
		}
		scan, err := OpenKeyScan(it.reader, it.report, it.bounds[it.next])
		it.next++
		if err != nil {
			it.err = err
			return false
		}
		if it.report != nil {
			it.report.peers++
		}
		it.scan = scan
	}
}

// Datom is the open scan's current datom, under that scan's workspace
// contract.
func (it *perKeySeekIterator) Datom() (*datalog.Datom, error) {
	return it.scan.Datom()
}

// ElementID is the open scan's, and the zero ElementID between keys.
func (it *perKeySeekIterator) ElementID() datalog.ElementID {
	if it.scan == nil {
		return datalog.ElementID{}
	}
	return it.scan.ElementID()
}

// Seek has no run to reposition: the stream is one run per key, and the hash
// join that consumes it never seeks.
func (it *perKeySeekIterator) Seek(bound ScanBound) {
	panic(fmt.Sprintf("BUG: Seek(%v) on a per-key seek stream, which has one run per key and no one run to reposition", bound))
}

// Scanned is the intake of every scan opened so far, the open one included.
func (it *perKeySeekIterator) Scanned() int {
	if it.scan == nil {
		return it.scanned
	}
	return it.scanned + it.scan.Scanned()
}

func (it *perKeySeekIterator) Error() error { return it.err }

// Close closes the scan open now, and the keys after it are never opened. The
// report is the arm's and closes with the arm.
func (it *perKeySeekIterator) Close() error {
	if it.scan == nil {
		return nil
	}
	it.scanned += it.scan.Scanned()
	err := it.scan.Close()
	it.scan = nil
	return err
}

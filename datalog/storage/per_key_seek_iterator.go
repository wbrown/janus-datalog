package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// perKeySeekIterator yields the datoms of every key's run, in turn, as one
// stream: the storage source of a hash join whose binding's keys are sought
// one by one (matchWithHashJoin). One scan is opened over the pattern's
// extent at the first pull, through the arm's report, and sought to each
// key's run in the keys' order, so the intake of every run accrues into the
// report when the one scan closes, and the keys are counted as the arm's
// peers, one run per key and no single run
// (docs/proposals/ONE_SCAN_SOUGHT_PER_KEY_PROOF.md, T1 to T3).
//
// A failure ends the stream. The scan's deferred error, a seek's among them,
// is kept before the next key would be sought, so a failed seek never reads
// as the key having no datoms.
type perKeySeekIterator struct {
	reader StoreReader
	report *scanReport
	extent ScanBound   // the bound the one scan is opened over, holding every key's run
	bounds []ScanBound // each key's run, in the keys' order

	next    int      // the position in bounds of the key to seek next
	scan    Iterator // the one scan; nil before the first pull and after Close
	inRun   bool     // whether the scan is inside a key's run it has not exhausted
	scanned int      // the scan's intake, taken at Close
	err     error
}

func (it *perKeySeekIterator) Next() bool {
	for it.err == nil {
		if it.inRun {
			if it.scan.Next() {
				return true
			}
			if err := it.scan.Error(); err != nil {
				it.err = err
				return false
			}
			it.inRun = false
		}
		if it.next >= len(it.bounds) {
			return false
		}
		if it.scan == nil {
			scan, err := OpenKeyScan(it.reader, it.report, it.extent)
			if err != nil {
				it.err = err
				return false
			}
			it.scan = scan
		}
		it.scan.Seek(it.bounds[it.next])
		it.next++
		if err := it.scan.Error(); err != nil {
			it.err = err
			return false
		}
		if it.report != nil {
			it.report.peers++
		}
		it.inRun = true
	}
	return false
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

// Scanned is the one scan's intake over every run sought so far.
func (it *perKeySeekIterator) Scanned() int {
	if it.scan == nil {
		return it.scanned
	}
	return it.scan.Scanned()
}

func (it *perKeySeekIterator) Error() error { return it.err }

// Close closes the one scan, and the keys after the one sought are never
// sought. The report is the arm's and closes with the arm.
func (it *perKeySeekIterator) Close() error {
	if it.scan == nil {
		return nil
	}
	it.scanned = it.scan.Scanned()
	err := it.scan.Close()
	it.scan = nil
	it.inRun = false
	return err
}

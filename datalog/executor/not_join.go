package executor

// The not-join composed over the modules
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.4): one body under the
// all-required header, built once per window over the window's distinct
// projection onto the header variables the subject carries, less the keys
// an earlier window ran, and probed per window tuple. The result is the
// subject's tuples with no match, in the subject's order, over the subject's
// symbols and with its properties; it reports no size, the subject is read
// once, and nothing of it is held beyond the window. A header the
// environment alone binds has the unit relation as its subject: the body is
// built once over the environment's tuple, and the unit relation's one tuple
// is dropped when the body has a result and kept when it has none.

// notJoinIterator is the composition under the not-join's policy: no
// narrowing, no record, the plan's build, and the anti emit.
type notJoinIterator struct {
	windowedIterator
}

// answer yields the window's tuples in turn whose probe of the body's index
// returns nothing, and drops those with a match; false when the window is
// answered.
func (it *notJoinIterator) answer() bool {
	for it.windowPos < len(it.reader.window) {
		tuple := it.reader.window[it.windowPos]
		it.windowPos++
		if len(it.branchCache[0].Probe(tuple)) > 0 {
			continue
		}
		it.currentTuple = tuple
		return true
	}
	return false
}

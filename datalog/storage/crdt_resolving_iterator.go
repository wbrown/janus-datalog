package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// CRDTResolvingIterator wraps a storage iterator and applies CRDT resolution
// per (E, A) group using streaming.
//
// Resolution strategies by cardinality:
//   - CardinalityOne: First entry wins (Tx descending). Emit if not tombstoned.
//   - CardinalityMany: Emit qualifying ADDs immediately. Minimal state.
//   - CardinalityVector: RGA tree reconstruction.
type CRDTResolvingIterator struct {
	source Iterator
	schema schema.SchemaProvider
	txID   datalog.ElementID // For as-of queries: only consider datoms with Tx.Lamport <= txID

	// uniqueMatcher enables unique-attribute walk-based resolution. When
	// set, CardinalityOne groups for unique attributes walk the entity's
	// (E, A) history and emit the first non-superseded assertion (rather
	// than simply emitting the EATV first-entry). Nil is permitted and
	// disables unique-walk resolution (CardinalityOne falls back to the
	// non-unique first-entry semantic).
	uniqueMatcher *PatternMatcher

	// Current (E, A) group tracking
	currentE datalog.Identity
	currentA datalog.Keyword
	hasGroup bool
	card     datalog.Keyword

	// CardinalityMany: streaming state (no buffering!)
	// Because we iterate Tx descending:
	// - First ADD for a value wins (unless tombstoned)
	// - REMOVE before ADD means remove has higher Tx
	emitted    map[any]bool   // values we've already emitted
	tombstones map[any]uint64 // value → remove Lamport (for add-wins-at-same-Tx)

	// CardinalityOne unique-walk state (reset per-group when attribute is
	// declared Unique in the schema):
	//   - uniqueMode: true if current group is a unique CardinalityOne
	//     attribute and uniqueMatcher is non-nil.
	//   - uniqueRetracted: value-byte key → highest Remove Tx seen within
	//     the current (E, A) group. Used to skip Set entries that have
	//     been cancelled by a later Remove.
	//   - uniqueEmitted: whether we have already emitted a value for the
	//     current group (emit at most one).
	uniqueMode      bool
	uniqueRetracted map[string]datalog.ElementID
	uniqueEmitted   bool

	// report accounts for the AVET supersession scans the unique walk opens,
	// one per Set entry it considers. Those are index reads this iterator
	// causes and the source knows nothing about, so without them the reported
	// intake would omit the largest read on the unique path.
	//
	// They accrue into the arm's report rather than into a count of this
	// iterator's own, because the arm's report is also what the source scan
	// accrues into — and a wrapper that added its nested reads to Scanned()
	// would be invisible to a report attached beneath it, which is where the
	// scan was acquired.
	report *scanReport

	// CardinalityVector: TODO - placeholder for discussion
	rgaElements []rgaElement

	// Emit buffer for CardinalityVector only
	emitBuffer []*datalog.Datom
	emitIdx    int

	// For detecting end of source
	sourceExhausted bool

	// Pending datom from Vector group transition: when a Vector group ends
	// and the boundary datom starts a new group, we must emit the vector
	// buffer first. This field saves the boundary datom so it is processed
	// on the next call to Next() instead of being lost.
	pendingDatom *datalog.Datom

	// Current datom being yielded
	currentDatom *datalog.Datom

	// err holds the first error encountered during walk-based resolution
	// (unique-attr max-other-Tx lookups). Surfaced via Error() (caller
	// checks after iterator exhaustion).
	err error
}

// rgaElement stores minimal state for RGA reconstruction (no datom pointers)
type rgaElement struct {
	id          datalog.ElementID
	afterRef    datalog.ElementID
	value       any
	tombstoneID *datalog.ElementID
}

// NewCRDTResolvingIterator creates a new CRDT-resolving iterator wrapper.
//
// When matcher is non-nil, CardinalityOne groups whose attribute is
// declared Unique in the schema apply the walk-based (A, V)-LWW
// resolution: the first non-superseded Set entry in each group is
// emitted, with supersession determined by an AVET sub-scan for
// max-other-Tx per candidate V. When matcher is nil, all CardinalityOne
// groups use first-entry semantics (the Unique field is ignored).
//
// report is the arm's, and is where the unique walk's own scans accrue. It is
// nil when nothing is listening, and nil is also correct for a caller whose
// scan no report accounts for.
func NewCRDTResolvingIterator(source Iterator, schema schema.SchemaProvider, txID datalog.ElementID, matcher *PatternMatcher, report *scanReport) *CRDTResolvingIterator {
	return &CRDTResolvingIterator{
		source:        source,
		schema:        schema,
		txID:          txID,
		uniqueMatcher: matcher,
		report:        report,
	}
}

// Next advances to the next CRDT-resolved datom.
func (it *CRDTResolvingIterator) Next() bool {
	// First, emit any buffered results (CardinalityVector only)
	if it.emitIdx < len(it.emitBuffer) {
		it.currentDatom = it.emitBuffer[it.emitIdx]
		it.emitIdx++
		return true
	}

	// If source is exhausted, we're done
	if it.sourceExhausted {
		return false
	}

	for {
		// Check for pending datom saved from a Vector group transition.
		// startNewGroup was already called for this datom — skip group
		// detection and go straight to cardinality processing.
		var datom *datalog.Datom
		isNewGroup := false

		if it.pendingDatom != nil {
			datom = it.pendingDatom
			it.pendingDatom = nil
			isNewGroup = true
		} else {
			if !it.source.Next() {
				// Source exhausted - emit final group if CardinalityVector.
				// Also capture any deferred error from the source so it
				// surfaces via Error() rather than being lost when
				// iteration terminates.
				if srcErr := it.source.Error(); srcErr != nil && it.err == nil {
					it.err = srcErr
				}
				it.sourceExhausted = true
				if it.hasGroup && it.card == schema.CardinalityVector {
					return it.emitRGAGroup()
				}
				return false
			}

			var err error
			datom, err = it.source.Datom()
			if err != nil {
				// Record the first Datom() decode error; abort iteration
				// so the caller can observe via Error(). Silently
				// continuing masks real storage corruption and (for
				// unique walks) can cause supersession checks to run on
				// zero-value data.
				if it.err == nil {
					it.err = err
				}
				return false
			}

			// Apply as-of filtering
			if it.txID != (datalog.ElementID{}) && it.txID.Less(datom.Tx) {
				continue
			}

			// Check if (E, A) changed
			if !it.hasGroup {
				isNewGroup = true
			} else {
				eHash := datom.E.Hash()
				currentEHash := it.currentE.Hash()
				if eHash != currentEHash || datom.A != it.currentA {
					isNewGroup = true
				}
			}

			if isNewGroup {
				// Emit pending CardinalityVector results before starting new group
				if it.hasGroup && it.card == schema.CardinalityVector {
					it.emitBuffer = it.resolveRGAGroup()
					it.emitIdx = 0
					it.startNewGroup(datom)
					// Save boundary datom — it will be processed on the next
					// call to Next() after the emit buffer is drained.
					it.pendingDatom = datom
					if len(it.emitBuffer) > 0 {
						it.currentDatom = it.emitBuffer[it.emitIdx]
						it.emitIdx++
						return true
					}
					// Empty RGA group — pendingDatom will be picked up on
					// next loop iteration.
					continue
				}
				it.startNewGroup(datom)
			}
		}

		// Process datom based on cardinality
		switch it.card {
		case schema.CardinalityOne:
			if it.uniqueMode {
				if it.uniqueEmitted {
					continue // already emitted winner for this group
				}
				if emit, err := it.processUniqueEntry(datom); err != nil {
					it.err = err
					return false
				} else if emit {
					it.uniqueEmitted = true
					return true
				}
				continue
			}
			if isNewGroup {
				if datom.Op == datalog.OpCRDTRemove {
					// Value was retracted — attribute doesn't exist. Skip group.
					continue
				}
				// First entry for this (E, A) — emit (LWW winner)
				it.currentDatom = datom
				return true
			}
			// Same (E, A) — skip (already emitted or skipped the winner)
			continue

		case schema.CardinalityMany:
			if result := it.processAddWins(datom); result != nil {
				it.currentDatom = result
				return true
			}
			continue

		case schema.CardinalityVector:
			it.accumulateRGA(datom)
			continue

		case schema.CardinalityUnknown:
			// Default is CardinalityOne (LWW) — same resolution as CardinalityOne
			if isNewGroup {
				if datom.Op == datalog.OpCRDTRemove {
					continue
				}
				it.currentDatom = datom
				return true
			}
			continue
		}
	}
}

// startNewGroup initializes state for a new (E, A) group.
func (it *CRDTResolvingIterator) startNewGroup(datom *datalog.Datom) {
	it.currentE = datom.E
	it.currentA = datom.A
	it.hasGroup = true

	// Determine cardinality from schema
	// If no schema definition exists for this attribute, use CardinalityUnknown
	// which defaults to CardinalityOne (LWW) semantics
	it.card = schema.CardinalityUnknown
	it.uniqueMode = false
	if it.schema != nil {
		if attr := it.schema.GetAttribute(datom.A); attr != nil {
			it.card = attr.Cardinality
			// Unique CardinalityOne attributes use the walk-based
			// resolution. Other cardinalities ignore Unique (uniqueness
			// applies only to single-valued attributes).
			if attr.Cardinality == schema.CardinalityOne && attr.HasUniqueConstraint() && it.uniqueMatcher != nil {
				it.uniqueMode = true
			}
		}
	}

	// Reset state based on cardinality
	switch it.card {
	case schema.CardinalityOne:
		if it.uniqueMode {
			it.uniqueRetracted = make(map[string]datalog.ElementID)
			it.uniqueEmitted = false
		}
	case schema.CardinalityMany:
		it.emitted = make(map[any]bool)
		it.tombstones = make(map[any]uint64)
	case schema.CardinalityVector:
		it.rgaElements = it.rgaElements[:0]
	case schema.CardinalityUnknown:
		// Default is CardinalityOne (LWW) — no state needed
	}
}

// processAddWins handles a datom for CardinalityMany using streaming add-wins.
// Returns the datom to emit, or nil if this datom should be skipped.
//
// Because we iterate Tx DESCENDING:
// - First ADD for a value = highest Tx = emit (unless tombstoned)
// - REMOVE before ADD = remove has higher Tx = value is removed
// - REMOVE after ADD = we already emitted = ignore remove
func (it *CRDTResolvingIterator) processAddWins(datom *datalog.Datom) *datalog.Datom {
	// Use a hashable key for map lookups. []byte is not hashable in Go,
	// but the datom itself (returned to caller) preserves the original type.
	v := datom.V
	key := v
	if b, ok := key.([]byte); ok {
		key = string(b)
	}

	if datom.Op == datalog.OpCRDTAdd {
		// Already emitted this value?
		if it.emitted[key] {
			return nil
		}

		// Check if tombstoned
		if tombstoneLamport, exists := it.tombstones[key]; exists {
			// Add wins at same Lamport, otherwise remove wins
			if datom.Tx.Lamport < tombstoneLamport {
				return nil // Remove wins
			}
			// Add wins (same or higher Lamport - but higher can't happen in descending order)
		}

		// Emit this add
		it.emitted[key] = true
		return datom

	} else if datom.Op == datalog.OpCRDTRemove {
		// Record tombstone (first one we see = highest Tx)
		if _, exists := it.tombstones[key]; !exists {
			it.tombstones[key] = datom.Tx.Lamport
		}
		return nil
	}

	return nil
}

// accumulateRGA collects RGA elements for tree reconstruction.
// Stores minimal state, not datom pointers.
func (it *CRDTResolvingIterator) accumulateRGA(datom *datalog.Datom) {
	if datom.Op == datalog.OpRGAInsert {
		it.rgaElements = append(it.rgaElements, rgaElement{
			id:       datom.Tx,
			afterRef: datom.AfterRef,
			value:    datom.V,
		})
	} else if datom.Op == datalog.OpRGATombstone {
		// Find and mark the target element, or create placeholder
		targetID := datom.AfterRef
		tombstoneID := datom.Tx
		found := false
		for i := range it.rgaElements {
			if it.rgaElements[i].id == targetID {
				if it.rgaElements[i].tombstoneID == nil || it.rgaElements[i].tombstoneID.Less(tombstoneID) {
					it.rgaElements[i].tombstoneID = &tombstoneID
				}
				found = true
				break
			}
		}
		if !found {
			// Tombstone arrived before insert
			it.rgaElements = append(it.rgaElements, rgaElement{
				id:          targetID,
				tombstoneID: &tombstoneID,
			})
		}
	}
}

// emitRGAGroup resolves the RGA group at source exhaustion.
func (it *CRDTResolvingIterator) emitRGAGroup() bool {
	it.emitBuffer = it.resolveRGAGroup()
	it.emitIdx = 0
	if len(it.emitBuffer) > 0 {
		it.currentDatom = it.emitBuffer[it.emitIdx]
		it.emitIdx++
		return true
	}
	return false
}

// resolveRGAGroup reconstructs the RGA tree and returns a single datom
// with V = the resolved vector ([]any), matching the cache path behavior.
func (it *CRDTResolvingIterator) resolveRGAGroup() []*datalog.Datom {
	if len(it.rgaElements) == 0 {
		return nil
	}

	// Deduplicate by ID (tombstone takes precedence)
	elemByID := make(map[datalog.ElementID]*rgaElement)
	for i := range it.rgaElements {
		e := &it.rgaElements[i]
		existing, exists := elemByID[e.id]
		if !exists {
			elemByID[e.id] = e
		} else {
			// Merge tombstone info
			if e.tombstoneID != nil && (existing.tombstoneID == nil || existing.tombstoneID.Less(*e.tombstoneID)) {
				existing.tombstoneID = e.tombstoneID
			}
			// Merge value/afterRef if we have it
			if existing.value == nil && e.value != nil {
				existing.value = e.value
				existing.afterRef = e.afterRef
			}
		}
	}

	// Build children map
	children := make(map[datalog.ElementID][]*rgaElement)
	for _, e := range elemByID {
		children[e.afterRef] = append(children[e.afterRef], e)
	}

	// Sort children by ID (ascending)
	for k := range children {
		sortRGAByID(children[k])
	}

	// DFS from HEAD, collect values into a single resolved list.
	// Recursion depth ≈ vector length for sequentially-appended vectors; see
	// the stack-depth note in storage/rga_reconstruct.go (ReconstructRGA).
	values := make([]any, 0)
	var maxTx datalog.ElementID
	for _, element := range elemByID {
		if maxTx.Less(element.id) {
			maxTx = element.id
		}
		if element.tombstoneID != nil && maxTx.Less(*element.tombstoneID) {
			maxTx = *element.tombstoneID
		}
	}
	visited := make(map[datalog.ElementID]bool)
	var walk func(id datalog.ElementID)
	walk = func(id datalog.ElementID) {
		if visited[id] {
			return
		}
		visited[id] = true

		for _, child := range children[id] {
			if child.tombstoneID == nil && child.value != nil {
				values = append(values, child.value)
			}
			walk(child.id)
		}
	}
	walk(HEAD)

	return []*datalog.Datom{{
		E:  it.currentE,
		A:  it.currentA,
		V:  values,
		Tx: maxTx,
	}}
}

func sortRGAByID(elems []*rgaElement) {
	// Simple insertion sort (typically small lists)
	for i := 1; i < len(elems); i++ {
		for j := i; j > 0 && elems[j].id.Less(elems[j-1].id); j-- {
			elems[j], elems[j-1] = elems[j-1], elems[j]
		}
	}
}

// Datom returns the current CRDT-resolved datom.
func (it *CRDTResolvingIterator) Datom() (*datalog.Datom, error) {
	return it.currentDatom, nil
}

// Close releases resources held by the iterator.
func (it *CRDTResolvingIterator) Close() error {
	if it.source != nil {
		return it.source.Close()
	}
	return nil
}

// Seek positions the iterator at or after the bound's start, discarding the
// group state accumulated for the run it is leaving.
func (it *CRDTResolvingIterator) Seek(bound ScanBound) {
	it.hasGroup = false
	it.emitted = nil
	it.tombstones = nil
	it.rgaElements = nil
	it.emitBuffer = nil
	it.emitIdx = 0
	it.sourceExhausted = false
	it.pendingDatom = nil
	it.currentDatom = nil
	it.source.Seek(bound)
}

// Scanned reports the source's intake, and only that.
//
// Resolution reads no index of its own on most paths, and the gap between this
// and what resolution emitted is the history depth the index made the query pay
// for. Unique CardinalityOne is the exception — processUniqueEntry opens an
// AVET supersession scan per Set entry — and those reads accrue into the arm's
// report at the point they happen, not here.
//
// They cannot be added here. The scan this iterator wraps was acquired through
// the arm's report, so the source already accrues into it; folding the walk's
// reads into this total as well would either double-count them or, for an arm
// reading the report rather than this method, lose them entirely.
func (it *CRDTResolvingIterator) Scanned() int {
	return it.source.Scanned()
}

// ElementID returns the transaction ElementID of the current entry.
func (it *CRDTResolvingIterator) ElementID() datalog.ElementID {
	if it.currentDatom != nil {
		return it.currentDatom.Tx
	}
	return datalog.ElementID{}
}

// Error returns the first error encountered during unique-walk resolution,
// or nil if iteration proceeded cleanly. Callers that expect walk-based
// resolution should check this after iterator exhaustion.
func (it *CRDTResolvingIterator) Error() error {
	return it.err
}

// processUniqueEntry applies the unique-attribute walk rule to one
// entry in the current (E, A) group via the shared walkApplyEntry
// primitive. Returns (true, nil) when the entry should be emitted
// (it.currentDatom is set), (false, nil) to skip or record a
// retraction, or (false, err) on supersession-check failure.
//
// Because the source iterator returns entries in Tx-descending order,
// the first entry that walkApplyEntry returns Emit for is the walk's
// emission — correct by construction without having to materialize
// the full group.
func (it *CRDTResolvingIterator) processUniqueEntry(datom *datalog.Datom) (bool, error) {
	var aBytes Attribute
	copy(aBytes[:], datom.A.String())
	eBytes := Entity(datom.E.Hash())

	state := &uniqueWalkState{retracted: it.uniqueRetracted}
	// Accrues whether or not the check fails: a failed supersession check
	// still read the index to fail.
	decision, err := it.uniqueMatcher.walkApplyEntry(state, datom, eBytes, aBytes, it.report)
	if err != nil {
		return false, err
	}
	if decision == walkEntryEmit {
		it.currentDatom = datom
		return true, nil
	}
	// walkEntrySkip or walkEntryRetract — no emission. The retraction
	// bookkeeping already updated it.uniqueRetracted via the shared map.
	return false, nil
}

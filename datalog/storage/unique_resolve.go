package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// walkEntryDecision is the outcome of applying the CRDT-unique walk
// rule to one (V_i, T_i, op) entry in an entity's (E, A) history.
type walkEntryDecision int

const (
	// walkEntrySkip: advance to the next entry. The walk has not
	// concluded — continue iterating.
	walkEntrySkip walkEntryDecision = iota
	// walkEntryEmit: this entry is the walk's result. For the batch
	// walk, return this entry's V and Tx as the emitted value. For the
	// streaming walk, set currentDatom and yield to the caller.
	walkEntryEmit
	// walkEntryRetract: this entry is a Remove that was just recorded
	// in the retracted state. Behaves like Skip for control flow, but
	// distinguished so callers can tell "Set skipped due to retract"
	// from "Remove recorded."
	walkEntryRetract
)

// uniqueWalkState is the per-(E, A) state tracked across entries of
// the walk: retracted values (by encoded-V key) with their highest
// Remove Tx. Both the batch primitive (walkUniqueEntityValue) and the
// streaming path (CRDTResolvingIterator.processUniqueEntry) build this
// state and feed it to walkApplyEntry.
type uniqueWalkState struct {
	// retracted maps a value to the highest Tx at which a CRDT Remove
	// tombstone was seen for it. Nothing is deleted — the name predates the
	// distinction and collides with physical deletion; see
	// BUG_RETRACT_NAMES_TWO_OPPOSITE_OPERATIONS.
	//
	// It is keyed by the value itself. Values are not all usable as Go map
	// keys — []byte and vectors are not comparable — so this uses the engine's
	// value-keyed map, which hashes content and settles collisions with
	// datalog.ValuesEqual. Rendering the value to bytes for a key would mean
	// reaching for a storage encoding to answer a value-domain question.
	retracted *executor.TupleKeyMap
}

// newUniqueWalkState constructs an empty walk state.
func newUniqueWalkState() *uniqueWalkState {
	return &uniqueWalkState{retracted: executor.NewTupleKeyMap()}
}

// walkApplyEntry applies the CRDT-unique walk rule to one entry. The
// caller is responsible for driving iteration (batch: scan loop;
// streaming: per-datom callback from the source iterator) and acting
// on the returned decision.
//
// Rules:
//   - Remove(V, T): record retracted[V] = max(retracted[V], T), return Retract.
//   - Set(V, T): return Skip if retracted[V] > T (cancelled by later Remove).
//   - Set(V, T): return Skip if any OTHER entity asserted V with Tx > T
//     (superseded). Otherwise return Emit.
//
// The (E, A) bytes parameters are required for the supersession check
// against other entities via AVET. The as-of / history-mode filtering
// is applied at the caller's iteration level (not here), because the
// streaming path already filters at its source.
//
// The supersession scan accrues into report; the decisions that short-circuit
// before it read no index.
func (m *PatternMatcher) walkApplyEntry(state *uniqueWalkState, datom *datalog.Datom, eBytes Entity, aBytes Attribute, report *scanReport) (walkEntryDecision, error) {
	if datom.Op == datalog.OpCRDTRemove {
		existing, ok := state.retracted.GetValue(datom.V)
		if !ok || existing.(datalog.ElementID).Less(datom.Tx) {
			state.retracted.PutValue(datom.V, datom.Tx)
		}
		return walkEntryRetract, nil
	}

	// Set (or default OpNone). Check if this V has been retracted at a
	// higher Tx by a later entry in this entity's own history.
	if rTx, ok := state.retracted.GetValue(datom.V); ok && datom.Tx.Less(rTx.(datalog.ElementID)) {
		return walkEntrySkip, nil
	}

	// Supersession check against other entities' assertions of V.
	maxOther, err := m.resolveMaxOtherTxForValue(aBytes, datom.V, eBytes, report)
	if err != nil {
		return walkEntrySkip, err
	}
	// Emit iff no other entity's assertion of V has a Tx greater than
	// ours. Strict < check; ties (same Lamport, different replica)
	// favor the current entity.
	if datom.Tx.Less(maxOther) {
		return walkEntrySkip, nil
	}
	return walkEntryEmit, nil
}

// walkUniqueEntityValue resolves E's current value for unique attribute
// a using the walk-based (A, V)-LWW rule (see walkApplyEntry). The
// walk iterates E's EATV history in descending Tx order and emits the
// first entry that passes retraction and supersession checks.
//
// Returns found=true with the value and the Tx of the emitted Set, suitable
// for cache-freshness tracking, or found=false when E has no current value.
//
// present is reported separately and is not the same question: it is whether
// the walk saw any datom for (E, a) within its temporal bound. A value can be
// absent while the attribute exists — every assertion retracted, or every one
// superseded by another entity's claim on the same unique value — and that is
// an attribute holding nothing, not an attribute that was never written. A
// caller that collapses the two loses the entity's resolved answer and falls
// back to a path that never applied the unique rule.
//
// Honors the matcher's temporal mode: entries with Tx > m.txID in as-of
// mode are skipped. The supersession check against other entities is
// likewise restricted via m.shouldFilterTx in resolveMaxOtherTxForValue.
// Both this walk's own EATV scan and every supersession scan walkApplyEntry
// opens underneath it acquire through report. Those AVET reads are real index
// reads on the resolution path — one per Set entry the walk considers — and an
// arm seeing only the outer scan would be told the cheapest part of the work.
func (m *PatternMatcher) walkUniqueEntityValue(eBytes Entity, aBytes Attribute, report *scanReport) (value any, tx datalog.ElementID, found bool, present bool, err error) {
	// The caller holds storage projections; both constructors return the
	// canonical interned pointer for an already-interned value.
	iter, err := OpenKeyScan(m.reader, report, ScanBound{
		Index: EATV,
		Prefix: []datalog.Value{
			datalog.NewIdentityFromHash(eBytes),
			datalog.InternKeywordFromBytes(aBytes),
		},
	})
	if err != nil {
		return nil, datalog.ElementID{}, false, false, err
	}
	defer iter.Close()

	state := newUniqueWalkState()
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, datalog.ElementID{}, false, present, err
		}
		if m.shouldFilterTx(datom.Tx) {
			continue
		}
		// A datom for (E, a) exists within the bound. Whatever the walk decides
		// about its value, the attribute is present from here on.
		present = true
		decision, err := m.walkApplyEntry(state, datom, eBytes, aBytes, report)
		if err != nil {
			return nil, datalog.ElementID{}, false, present, err
		}
		if decision == walkEntryEmit {
			return datom.V, datom.Tx, true, true, nil
		}
		// walkEntrySkip or walkEntryRetract — continue to next entry.
	}
	// Surface any deferred error from the scan.
	if err := iter.Error(); err != nil {
		return nil, datalog.ElementID{}, false, present, err
	}
	return nil, datalog.ElementID{}, false, present, nil
}

// resolveMaxOtherTxForValue scans AVET for (a, v) and returns the highest
// Tx asserted by any entity other than exceptE. Returns zero ElementID if
// no other entity has asserted this value.
//
// Honors the matcher's temporal mode — entries with Tx > m.txID in as-of
// mode are excluded.
func (m *PatternMatcher) resolveMaxOtherTxForValue(aBytes Attribute, v any, exceptE Entity, report *scanReport) (datalog.ElementID, error) {
	iter, err := OpenKeyScan(m.reader, report, ScanBound{
		Index:  AVET,
		Prefix: []datalog.Value{datalog.InternKeywordFromBytes(aBytes), v},
	})
	if err != nil {
		return datalog.ElementID{}, err
	}
	defer iter.Close()

	var maxTx datalog.ElementID
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return datalog.ElementID{}, err
		}
		if datom.E == nil {
			continue
		}
		if Entity(datom.E.Hash()) == exceptE {
			continue
		}
		if m.shouldFilterTx(datom.Tx) {
			continue
		}
		// Tombstones don't count as a claim for uniqueness ownership —
		// if alice's last op on V was Remove, she isn't currently claiming V.
		// However, she might still have an active Set(V) at a lower Tx. We
		// track the max Tx of any AVET entry for V, regardless of op; the
		// walking entity's own retraction bookkeeping handles self-retracts.
		// For *other* entities, the CRDTResolvingIterator / walk semantics
		// are applied at their respective entity-view computations, and a
		// Remove entry for (E', V) simply means E' isn't emitting V. But a
		// high-Tx Set(V) by E' that has since been retracted by E' herself
		// is still "an assertion of V" in the AVET sense — the question is
		// whether it competes with our entity's assertion at T_i.
		//
		// The simple rule "any assertion of V at Tx > T_i supersedes" is
		// the one the design doc specifies. Implement it as-is; subtleties
		// with retract-vs-competing-claim are covered by the other
		// entity's own walk resolving to something other than V, which
		// means V-view for V will correctly return nil when this entity's
		// walk also skips V.
		if maxTx.Less(datom.Tx) {
			maxTx = datom.Tx
		}
	}
	// A failed scan is not "no competing assertion" — the walk would emit a
	// value that may actually be superseded. Surface it.
	if err := iter.Error(); err != nil {
		return datalog.ElementID{}, err
	}
	return maxTx, nil
}

// resolveAVLWW returns the entity currently owning (a, v) under the
// walk-based (A, V)-LWW resolution for unique attributes.
//
// Algorithm:
//  1. AVET [a][v] scan finds the max-Tx entry for v across all entities.
//  2. Run walkUniqueEntityValue on that entity.
//  3. If the walk emits v, the entity is the canonical owner; return it.
//  4. Otherwise no entity currently owns v.
//
// The walk-based rule gives V-view and entity-view the same underlying
// semantics, guaranteeing that "E emits v" via the entity walk and
// "V is owned by E" via resolveAVLWW always agree.
// The report carries this resolution's cost and outcome. Intake is the AVET
// scan plus the ownership walk in step 2, which opens an EATV scan of its own
// and a supersession scan per Set entry within it — all acquired through the
// same report. Resolved counts 1 when the walk emitted a value for the claimant
// and matched 1 when that value is v, so the gap between them names the case a
// two-term form could not: the index held an entry for a value its claimant has
// since replaced, and rejecting it cost a scan and a walk. Reporting that as
// "nothing found" makes it read like a value nobody ever wrote, which costs
// nothing.
func (m *PatternMatcher) resolveAVLWW(a Attribute, v any, report *scanReport) (datalog.Identity, datalog.ElementID, error) {
	// Step 1: find the max-Tx entry for (a, v) across all entities.
	iter, err := OpenKeyScan(m.reader, report, ScanBound{
		Index:  AVET,
		Prefix: []datalog.Value{datalog.InternKeywordFromBytes(a), v},
	})
	if err != nil {
		return nil, datalog.ElementID{}, err
	}

	var (
		bestE   datalog.Identity
		bestTx  datalog.ElementID
		scanErr error
	)
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			scanErr = err
			break
		}
		if datom.E == nil {
			continue
		}
		if m.shouldFilterTx(datom.Tx) {
			continue
		}
		if bestE == nil || bestTx.Less(datom.Tx) {
			bestE = datom.E
			bestTx = datom.Tx
		}
	}
	// A failed scan is not "no owner" — surface it (first error wins).
	if scanErr == nil {
		scanErr = iter.Error()
	}
	iter.Close()
	if scanErr != nil {
		return nil, datalog.ElementID{}, scanErr
	}

	if bestE == nil {
		return nil, datalog.ElementID{}, nil
	}

	// Step 2 + 3: verify the max-Tx entity's walk actually emits v.
	//
	// present is discarded because it is true by construction here, which is
	// checkable rather than assumed: bestE is assigned only from the E of an
	// AVET [a][v] entry that passed m.shouldFilterTx, so a datom for
	// (bestE, a) exists within the bound. The walk scans EATV [bestE][a],
	// which covers that datom, and marks present on the first entry surviving
	// the same filter on the same matcher. It cannot miss it.
	//
	// found is the question this step asks, and it is a different one: the
	// claimant may have superseded or retracted v since asserting it.
	walkV, walkTx, found, _, err := m.walkUniqueEntityValue(Entity(bestE.Hash()), a, report)
	if err != nil {
		return nil, datalog.ElementID{}, err
	}
	if !found {
		return nil, datalog.ElementID{}, nil
	}
	// Resolution produced the claimant's current value; whether it is the value
	// asked for is the next question.
	if report != nil {
		report.resolved++
	}
	if !datalog.ValuesEqual(walkV, v) {
		return nil, datalog.ElementID{}, nil
	}
	if report != nil {
		report.matched++
	}
	return bestE, walkTx, nil
}

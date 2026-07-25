package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
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
	retracted map[string]datalog.ElementID
}

// newUniqueWalkState constructs an empty walk state.
func newUniqueWalkState() *uniqueWalkState {
	return &uniqueWalkState{retracted: make(map[string]datalog.ElementID)}
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
func (m *PatternMatcher) walkApplyEntry(state *uniqueWalkState, datom *datalog.Datom, eBytes Entity, aBytes Attribute) (walkEntryDecision, error) {
	vKey := string(encodeValueForSearch(datom.V, m.encoder))

	if datom.Op == datalog.OpCRDTRemove {
		if existing, ok := state.retracted[vKey]; !ok || existing.Less(datom.Tx) {
			state.retracted[vKey] = datom.Tx
		}
		return walkEntryRetract, nil
	}

	// Set (or default OpNone). Check if this V has been retracted at a
	// higher Tx by a later entry in this entity's own history.
	if rTx, ok := state.retracted[vKey]; ok && datom.Tx.Less(rTx) {
		return walkEntrySkip, nil
	}

	// Supersession check against other entities' assertions of V.
	maxOther, err := m.resolveMaxOtherTxForValue(aBytes, datom.V, eBytes)
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
// Returns (value, tx, true, nil) when a value is found, or
// (nil, zero, false, nil) when E has no current value. The returned tx
// is the Tx of the emitted Set, suitable for cache-freshness tracking.
//
// Honors the matcher's temporal mode: entries with Tx > m.txID in as-of
// mode are skipped. The supersession check against other entities is
// likewise restricted via m.shouldFilterTx in resolveMaxOtherTxForValue.
func (m *PatternMatcher) walkUniqueEntityValue(eBytes Entity, aBytes Attribute) (any, datalog.ElementID, bool, error) {
	start, end := m.encoder.EncodePrefixRange(EATV, eBytes[:], aBytes[:])
	iter, err := m.reader.ScanKeysOnly(EATV, start, end)
	if err != nil {
		return nil, datalog.ElementID{}, false, err
	}
	defer iter.Close()

	state := newUniqueWalkState()
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, datalog.ElementID{}, false, err
		}
		if m.shouldFilterTx(datom.Tx) {
			continue
		}
		decision, err := m.walkApplyEntry(state, datom, eBytes, aBytes)
		if err != nil {
			return nil, datalog.ElementID{}, false, err
		}
		if decision == walkEntryEmit {
			return datom.V, datom.Tx, true, nil
		}
		// walkEntrySkip or walkEntryRetract — continue to next entry.
	}
	// Surface any deferred error from the scan.
	if err := iter.Error(); err != nil {
		return nil, datalog.ElementID{}, false, err
	}
	return nil, datalog.ElementID{}, false, nil
}

// resolveMaxOtherTxForValue scans AVET for (a, v) and returns the highest
// Tx asserted by any entity other than exceptE. Returns zero ElementID if
// no other entity has asserted this value.
//
// Honors the matcher's temporal mode — entries with Tx > m.txID in as-of
// mode are excluded.
func (m *PatternMatcher) resolveMaxOtherTxForValue(aBytes Attribute, v any, exceptE Entity) (datalog.ElementID, error) {
	vBytes := encodeValueForSearch(v, m.encoder)
	start, end := m.encoder.EncodePrefixRange(AVET, aBytes[:], vBytes)
	iter, err := m.reader.ScanKeysOnly(AVET, start, end)
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
func (m *PatternMatcher) resolveAVLWW(a Attribute, vBytes []byte, v any) (datalog.Identity, datalog.ElementID, error) {
	// Step 1: find the max-Tx entry for (a, v) across all entities.
	start, end := m.encoder.EncodePrefixRange(AVET, a[:], vBytes)
	iter, err := m.reader.ScanKeysOnly(AVET, start, end)
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
	walkV, walkTx, found, err := m.walkUniqueEntityValue(Entity(bestE.Hash()), a)
	if err != nil {
		return nil, datalog.ElementID{}, err
	}
	if !found {
		return nil, datalog.ElementID{}, nil
	}
	if !datalog.ValuesEqual(walkV, v) {
		return nil, datalog.ElementID{}, nil
	}
	return bestE, walkTx, nil
}

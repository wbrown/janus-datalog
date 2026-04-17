package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// walkUniqueEntityValue resolves E's current value for unique attribute a
// using the walk-based (A, V)-LWW rule:
//
//  1. Walk E's EATV history in descending Tx order.
//  2. A Remove(V_i, T_i) entry retracts V_i for this entity at T_i.
//  3. For a Set(V_i, T_i) entry, skip if V_i was retracted at a higher Tx.
//  4. Otherwise, check whether any OTHER entity has an assertion of V_i
//     with Tx > T_i. If no, E currently owns V_i — emit it.
//  5. If no entry passes, E has no current value for this attribute.
//
// Returns (value, tx, true, nil) when a value is found, or
// (nil, zero, false, nil) when E has no current value. The returned tx is
// the Tx of the emitted Set assertion, suitable for cache-freshness
// tracking and for identifying the emitted entry.
//
// The walk honors the matcher's temporal mode: entries with Tx > m.txID in
// as-of mode are skipped, and the supersession check against other
// entities is likewise restricted to Tx ≤ m.txID.
func (m *BadgerMatcher) walkUniqueEntityValue(eBytes Entity, aBytes Attribute) (any, datalog.ElementID, bool, error) {
	start, end := m.store.encoder.EncodePrefixRange(EATV, eBytes[:], aBytes[:])
	iter, err := m.store.ScanKeysOnly(EATV, start, end)
	if err != nil {
		return nil, datalog.ElementID{}, false, err
	}
	defer iter.Close()

	// Per-entity retraction map: value-bytes key → highest Remove Tx seen.
	// We build the key using encodeValueForSearch so it matches the storage
	// layer's value-equality semantics (distinguishes int64(5) from "5", etc.).
	retracted := make(map[string]datalog.ElementID)

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
		}
		if m.shouldFilterTx(datom.Tx) {
			continue
		}

		vKey := string(encodeValueForSearch(datom.V, m.store.encoder))

		if datom.Op == datalog.OpCRDTRemove {
			if existing, ok := retracted[vKey]; !ok || existing.Less(datom.Tx) {
				retracted[vKey] = datom.Tx
			}
			continue
		}

		// Set (or default OpNone) — check retraction.
		if rTx, ok := retracted[vKey]; ok && datom.Tx.Less(rTx) {
			continue // cancelled by a later Remove
		}

		// Check supersession by another entity.
		maxOther, err := m.resolveMaxOtherTxForValue(aBytes, datom.V, eBytes)
		if err != nil {
			return nil, datalog.ElementID{}, false, err
		}
		// Emit if no other entity's assertion of V has a Tx greater than ours.
		// datom.Tx.Less(maxOther) is strict <; we emit on >=.
		if !datom.Tx.Less(maxOther) {
			return datom.V, datom.Tx, true, nil
		}
		// Superseded — continue walking.
	}
	return nil, datalog.ElementID{}, false, nil
}

// resolveMaxOtherTxForValue scans AVET for (a, v) and returns the highest
// Tx asserted by any entity other than exceptE. Returns zero ElementID if
// no other entity has asserted this value.
//
// Honors the matcher's temporal mode — entries with Tx > m.txID in as-of
// mode are excluded.
func (m *BadgerMatcher) resolveMaxOtherTxForValue(aBytes Attribute, v any, exceptE Entity) (datalog.ElementID, error) {
	vBytes := encodeValueForSearch(v, m.store.encoder)
	start, end := m.store.encoder.EncodePrefixRange(AVET, aBytes[:], vBytes)
	iter, err := m.store.ScanKeysOnly(AVET, start, end)
	if err != nil {
		return datalog.ElementID{}, err
	}
	defer iter.Close()

	var maxTx datalog.ElementID
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
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
func (m *BadgerMatcher) resolveAVLWW(a Attribute, vBytes []byte, v any) (datalog.Identity, datalog.ElementID, error) {
	// Step 1: find the max-Tx entry for (a, v) across all entities.
	start, end := m.store.encoder.EncodePrefixRange(AVET, a[:], vBytes)
	iter, err := m.store.ScanKeysOnly(AVET, start, end)
	if err != nil {
		return nil, datalog.ElementID{}, err
	}

	var (
		bestE  datalog.Identity
		bestTx datalog.ElementID
	)
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
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
	iter.Close()

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

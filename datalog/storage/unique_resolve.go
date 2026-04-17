package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// resolveAVLWW returns the entity currently owning (a, v) under
// (A, V)-LWW resolution for unique attributes.
//
// Algorithm:
//  1. AVET prefix scan for (a, v) produces candidate entities that have
//     *ever* asserted this value.
//  2. For each distinct candidate E, perform EATV-LWW lookup for (E, a)
//     to determine E's current value. An entity is a valid claimant of v
//     iff its current (E, a) value equals v.
//  3. Among valid claimants, the winner is the one whose current
//     (E, a)-LWW Tx is highest.
//
// Returns (nil Identity, zero ElementID, nil error) if no entity currently
// claims v. The zero-identity result is a normal outcome, not an error.
//
// The encoded value bytes (vBytes) must use the same encoding as the
// storage layer uses for AVET keys so the prefix scan hits matching
// entries. Use encodeValueForSearch to produce vBytes from a Go value.
func (m *BadgerMatcher) resolveAVLWW(a Attribute, vBytes []byte, v any) (datalog.Identity, datalog.ElementID, error) {
	// Step 1: AVET [A][V] prefix scan collects candidate entities.
	start, end := m.store.encoder.EncodePrefixRange(AVET, a[:], vBytes)
	iter, err := m.store.ScanKeysOnly(AVET, start, end)
	if err != nil {
		return nil, datalog.ElementID{}, err
	}

	// Dedup candidates by entity hash. The AVET ordering (A → V → E → Tx↓)
	// groups entries per E with Tx descending, so any single entry per E is
	// sufficient to mark them as a candidate; the subsequent EATV-LWW check
	// provides the authoritative claim Tx.
	seen := make(map[[20]byte]datalog.Identity)
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
		}
		if datom.E == nil {
			continue
		}
		h := datom.E.Hash()
		if _, ok := seen[h]; !ok {
			seen[h] = datom.E
		}
	}
	iter.Close()

	// Step 2 + 3: validate each candidate via EATV-LWW and pick the winner.
	var (
		bestE  datalog.Identity
		bestTx datalog.ElementID
		have   bool
	)
	for _, e := range seen {
		eBytes := Entity(e.Hash())
		winnerV, winnerTx, err := m.ResolveLWW(eBytes, a)
		if err != nil {
			return nil, datalog.ElementID{}, err
		}
		if winnerV == nil {
			continue // tombstoned or no value
		}
		if !datalog.ValuesEqual(winnerV, v) {
			continue // entity has moved on to a different value
		}
		if !have || bestTx.Less(winnerTx) {
			bestE = e
			bestTx = winnerTx
			have = true
		}
	}

	if !have {
		return nil, datalog.ElementID{}, nil
	}
	return bestE, bestTx, nil
}

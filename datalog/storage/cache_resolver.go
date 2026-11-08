package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Ensure PatternMatcher implements CacheResolver
var _ CacheResolver = (*PatternMatcher)(nil)

// GetCardinality returns the cardinality an attribute resolves under: the
// one its definition declares, and CardinalityOne for an attribute the
// schema does not declare or a matcher with no schema.
func (m *PatternMatcher) GetCardinality(attr datalog.Keyword) datalog.Keyword {
	if m.schema == nil {
		return schema.CardinalityOne
	}
	def := m.schema.GetAttribute(attr)
	if def == nil {
		return schema.CardinalityOne
	}
	return def.Cardinality
}

// ResolveLWW returns the current value for cardinality-one (highest
// ElementID wins), given def, the attribute's definition, nil for an
// attribute the schema does not declare.
//
// When def declares no unique, and in History, it uses the EATV first-entry
// shortcut (highest Tx due to descending sort). History reads raw assertions
// with no resolution, so the unique walk would produce values corresponding
// to no datom. When def declares a unique and the matcher reads resolved
// state, it delegates to walkUniqueEntityValue for walk-based resolution: the
// returned value is the entity's first non-superseded assertion, which may
// fall back to an older entry if the latest has been claimed by another
// entity, and the returned ElementID is the walk's version, the highest among
// the datoms it read, other entities' superseding claims included.
func (m *PatternMatcher) ResolveLWW(e datalog.Identity, a datalog.Keyword, def *schema.AttributeDefinition, report *scanReport) (v any, tx datalog.ElementID, present bool, resolveErr error) {
	if m.mode != historyMode && def.HasUniqueConstraint() {
		// The walk acquires through report, so its intake — the deepest read
		// on this path — reaches the arm whether or not the walk goes on to
		// fail.
		v, _, version, found, present, err := m.walkUniqueEntityValue(newUniqueWalkState(), e, a, report)
		if err != nil {
			return nil, datalog.ElementID{}, false, err
		}
		if !found {
			// No value survived the unique rule, which is not the same as no
			// datoms: an entity whose every assertion was removed or
			// superseded still has the attribute, holding nothing. Reporting
			// that as absence would leave no cache entry, and a caller finding
			// none falls through to the plain EATV first-entry scan — which
			// never applies the unique rule and hands back the superseded
			// value.
			return nil, version, present, nil
		}
		return v, version, true, nil
	}

	// Scan EATV for the first entry visible in this matcher mode (highest
	// non-filtered Tx due to descending sort). The scan accrues into the report
	// as it closes, so the failures below keep the intake they spent.
	iter, err := OpenScan(m.reader, report, ScanBound{
		Index:  EATV,
		Prefix: []datalog.Value{e, a},
	})
	if err != nil {
		return nil, datalog.ElementID{}, false, err
	}
	defer func() {
		resolveErr = errjoin.Join(resolveErr, iter.Close())
	}()

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, datalog.ElementID{}, false, err
		}
		if m.shouldFilterTx(datom.Tx) {
			continue
		}
		// Check Op: if the latest operation is a tombstone, the attribute doesn't exist.
		// Return nil value with the tombstone's ElementID for cache freshness tracking.
		// The (E, A) is still present — a tombstone is a datom, and the entry it
		// produces holds no value rather than standing for no attribute.
		if datom.Op == datalog.OpCRDTRemove {
			return nil, datom.Tx, true, nil
		}
		return datom.V, datom.Tx, true, nil
	}
	if err := iter.Error(); err != nil {
		return nil, datalog.ElementID{}, false, err
	}

	// No datom survived the temporal bound: the attribute does not exist here.
	return nil, datalog.ElementID{}, false, nil
}

// ResolveAddWins returns the current set members for cardinality-many, each
// under its memberKey, through resolveAddWinsSet.
func (m *PatternMatcher) ResolveAddWins(e datalog.Identity, a datalog.Keyword, report *scanReport) (map[string]any, datalog.ElementID, bool, error) {
	result, err := m.resolveAddWinsSet(e, a, report)
	if err != nil {
		// The scan read the index before it failed, and the report kept that
		// where the result struct it never built could not.
		return nil, datalog.ElementID{}, false, err
	}
	return result.Members, result.MaxElementID, result.Present, nil
}

// ResolveRGA returns the ordered vector for cardinality-vector
// Returns (values, positionToElementID, maxElementID, present, error)
func (m *PatternMatcher) ResolveRGA(e datalog.Identity, a datalog.Keyword, report *scanReport) ([]any, []datalog.ElementID, datalog.ElementID, bool, error) {
	// Load raw RGA elements. The bound is the resolver's own; a cache rebuild
	// has no pattern arm above it to announce one.
	elements, _, err := m.loadRGAElements(e, a, report)
	if err != nil {
		return nil, nil, datalog.ElementID{}, false, err
	}

	if len(elements) == 0 {
		// No RGA datom for this (E, A): the attribute does not exist. Reported
		// as absence rather than as an empty result, so no cache entry is built
		// and every reader sees the same answer the streaming path gives.
		//
		// The scan still happened: an (E, A) with no RGA entries is a read that
		// returned nothing, not a read that did not occur.
		return nil, nil, datalog.ElementID{}, false, nil
	}

	// Reconstruct with IDs preserved
	withIDs := ReconstructRGAWithIDs(elements)

	// Extract values and positions
	values := make([]any, len(withIDs))
	positions := make([]datalog.ElementID, len(withIDs))
	for i, ewp := range withIDs {
		values[i] = ewp.Element.Value
		positions[i] = ewp.Element.ID
	}

	// Get max ElementID for cache versioning
	maxID := FindMaxElementID(elements)

	// Elements exist, so the attribute does — whether or not any survived
	// tombstoning. An all-tombstoned vector resolves to the empty vector, which
	// is a value and not an absence.
	return values, positions, maxID, true, nil
}

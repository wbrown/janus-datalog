package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Ensure BadgerMatcher implements CacheResolver
var _ CacheResolver = (*PatternMatcher)(nil)

// GetCardinality returns the cardinality for an attribute
// Defaults to CardinalityOne if schema is not set or attribute not found
func (m *PatternMatcher) GetCardinality(a Attribute) schema.Cardinality {
	if m.schema == nil {
		return schema.CardinalityOne
	}

	// Convert Attribute to Keyword
	kw := decodeAttribute(a)
	if kw == "" {
		return schema.CardinalityOne
	}

	attr := m.schema.GetAttribute(datalog.NewKeyword(kw))
	if attr == nil {
		return schema.CardinalityOne
	}

	return attr.Cardinality
}

// ResolveLWW returns the current value for cardinality-one (highest
// ElementID wins).
//
// For non-unique attributes, uses the EATV first-entry shortcut (highest
// Tx due to descending sort). For attributes declared Unique in the
// schema, delegates to walkUniqueEntityValue for walk-based resolution:
// the returned value is the entity's first non-superseded assertion,
// which may fall back to an older entry if the latest has been claimed
// by another entity.
//
// Exception: in history mode, the walk is bypassed entirely. History
// mode exposes raw assertions without CRDT resolution; applying the
// walk would produce fallback values that don't correspond to any
// single raw datom. ResolveLWW in history mode returns the first-entry
// (highest-Tx raw Set), matching the pre-redesign behavior for
// non-unique attributes in the same mode.
func (m *PatternMatcher) ResolveLWW(e Entity, a Attribute) (any, datalog.ElementID, error) {
	// Unique-attribute walk path (applies only when schema declares
	// the attribute unique AND the matcher is not in history mode;
	// otherwise falls through to the simple first-entry path below).
	if m.schema != nil && !m.isHistoryMode() {
		kw := decodeAttribute(a)
		if kw != "" {
			if def := m.schema.GetAttribute(datalog.NewKeyword(kw)); def != nil && def.Unique != "" {
				v, tx, found, err := m.walkUniqueEntityValue(e, a)
				if err != nil {
					return nil, datalog.ElementID{}, err
				}
				if !found {
					return nil, datalog.ElementID{}, nil
				}
				return v, tx, nil
			}
		}
	}

	// Scan EATV for the first entry visible in this matcher mode (highest
	// non-filtered Tx due to descending sort). The caller holds the storage
	// projections, so the bound's components are re-interned from them; both
	// constructors return the canonical pointer for an already-interned value.
	iter, err := m.reader.Scan(ScanBound{
		Index:  EATV,
		Prefix: []datalog.Value{datalog.NewIdentityFromHash(e), datalog.InternKeywordFromBytes(a)},
	})
	if err != nil {
		return nil, datalog.ElementID{}, err
	}
	defer iter.Close()

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, datalog.ElementID{}, err
		}
		if m.shouldFilterTx(datom.Tx) {
			continue
		}
		// Check Op: if the latest operation is a tombstone, the attribute doesn't exist.
		// Return nil value with the tombstone's ElementID for cache freshness tracking.
		if datom.Op == datalog.OpCRDTRemove {
			return nil, datom.Tx, nil
		}
		return datom.V, datom.Tx, nil
	}
	if err := iter.Error(); err != nil {
		return nil, datalog.ElementID{}, err
	}

	// No value found
	return nil, datalog.ElementID{}, nil
}

// ResolveAddWins returns the current set members for cardinality-many
// Uses the existing resolveAddWinsSet method and converts to map
func (m *PatternMatcher) ResolveAddWins(e Entity, a Attribute) (map[any]any, datalog.ElementID, error) {
	result, err := m.resolveAddWinsSet(e[:], a[:])
	if err != nil {
		return nil, datalog.ElementID{}, err
	}
	return result.Members, result.MaxElementID, nil
}

// ResolveRGA returns the ordered vector for cardinality-vector
// Returns (values, positionToElementID, maxElementID, error)
func (m *PatternMatcher) ResolveRGA(e Entity, a Attribute) ([]any, []datalog.ElementID, datalog.ElementID, error) {
	// Load raw RGA elements
	elements, err := m.loadRGAElements(e[:], a[:])
	if err != nil {
		return nil, nil, datalog.ElementID{}, err
	}

	if len(elements) == 0 {
		// nil values (not empty slice) signals "never set" to callers.
		// matcher.go relies on this to distinguish never-set (nil → not found)
		// from explicitly-cleared (empty slice → found, empty).
		return nil, nil, datalog.ElementID{}, nil
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

	return values, positions, maxID, nil
}

// decodeAttribute converts Attribute bytes to a string (keyword representation)
func decodeAttribute(a Attribute) string {
	// Find the first null byte to determine actual string length
	length := 0
	for i, b := range a {
		if b == 0 {
			length = i
			break
		}
		length = i + 1
	}
	if length == 0 {
		return ""
	}
	return string(a[:length])
}

package storage

import (
	"bytes"
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
)

// PrefetchEntities loads all attributes for a set of entities into the EA cache
// in a single-pass EATV scan per entity. Entities are sorted by raw hash byte
// order (= disk order) for efficient forward-only iteration through BadgerDB.
//
// CRDT resolution happens inline as attribute boundaries are crossed during
// iteration — no second pass. Each (E, A) group is dispatched to
// Cache.PopulateFromDatoms which uses the canonical Resolve*FromDatoms functions.
//
// See ALGEBRA.md "Rule 6: Entity Prefetch into EA Cache" for design rationale.
//
// The first storage failure aborts the prefetch and returns it: an error
// from the store means the store is broken, and callers fail the query —
// prefetch being an optimization does not make store failures ignorable.
// Groups cached before the failure hold valid data.
func (m *BadgerMatcher) PrefetchEntities(entities []datalog.Identity) error {
	if m.cache == nil || len(entities) == 0 || m.isHistoryMode() {
		return nil
	}

	// Sort entities by their 20-byte hash (= disk order)
	sort.Slice(entities, func(i, j int) bool {
		return bytes.Compare(entities[i].Bytes(), entities[j].Bytes()) < 0
	})

	// Deduplicate (sorted, so duplicates are adjacent)
	deduped := make([]Entity, 0, len(entities))
	var prev Entity
	for _, eid := range entities {
		var e Entity
		copy(e[:], eid.Bytes())
		if len(deduped) > 0 && e == prev {
			continue
		}
		deduped = append(deduped, e)
		prev = e
	}

	encoder := m.encoder

	for _, e := range deduped {
		// Build prefix range for this entity on EATV: prefix(1) + E(20)
		start, end := encoder.EncodePrefixRange(EATV, e[:])

		iter, err := m.store.Scan(EATV, start, end)
		if err != nil {
			return err
		}

		// Single-pass: iterate EATV, accumulate datoms per (E, A) group,
		// dispatch to cache at each attribute boundary.
		var currentAttr Attribute
		var currentDatoms []datalog.Datom
		hasGroup := false

		for iter.Next() {
			d, err := iter.Datom()
			if err != nil {
				_ = iter.Close()
				return err
			}
			if m.shouldFilterTx(d.Tx) {
				continue
			}

			var aBytes Attribute
			copy(aBytes[:], d.A.String())

			if hasGroup && aBytes != currentAttr {
				// Attribute boundary — resolve and cache the previous group
				card := m.GetCardinality(currentAttr)
				if key, ok := m.cacheKey(e, currentAttr); ok {
					m.cache.PopulateFromDatoms(key, card, currentDatoms)
				}
				currentDatoms = currentDatoms[:0]
			}

			currentAttr = aBytes
			currentDatoms = append(currentDatoms, *d)
			hasGroup = true
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return err
		}
		// Flush last group
		if hasGroup && len(currentDatoms) > 0 {
			card := m.GetCardinality(currentAttr)
			if key, ok := m.cacheKey(e, currentAttr); ok {
				m.cache.PopulateFromDatoms(key, card, currentDatoms)
			}
		}
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}

package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ScanSharingMatcher wraps a PatternMatcher and deduplicates unbound scans
// using a ScanRegistry. When the same unbound pattern is requested multiple
// times (e.g., by 3 decorrelated subqueries all scanning [?t :task/root ?s]),
// the first call creates a LazySeq over the storage iterator and registers it.
// Subsequent calls return LazySeqRelations backed by the same shared LazySeq.
//
// Only unbound scans (bindings == nil or empty) participate in sharing.
// Bound scans with entity IDs from prior joins pass through to the inner matcher.
type ScanSharingMatcher struct {
	inner    PatternMatcher
	registry *ScanRegistry
	handler  annotations.Handler
}

// NewScanSharingMatcher creates a ScanSharingMatcher wrapping the given
// inner matcher. The registry is shared across all Match() calls within
// a single query execution.
func NewScanSharingMatcher(inner PatternMatcher, registry *ScanRegistry, handler annotations.Handler) *ScanSharingMatcher {
	return &ScanSharingMatcher{
		inner:    inner,
		registry: registry,
		handler:  handler,
	}
}

// Match implements PatternMatcher. For unbound scans, checks the registry
// first. For bound scans, delegates directly to the inner matcher.
func (m *ScanSharingMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	// Only share unbound scans — bound scans are specialized to binding values
	if bindings != nil && len(bindings) > 0 {
		return m.inner.Match(q, bindings)
	}

	fp := ScanQueryFingerprint(q, pattern)

	// Check registry for an existing shared scan
	if shared := m.registry.Get(fp); shared != nil {
		// Build remapped symbols for this caller's variable names
		remapped := remapSymbols(shared.Symbols, pattern)
		properties := remapRelationProperties(shared.Properties, shared.Symbols, remapped)
		if m.handler != nil {
			m.handler(annotations.Event{
				Name: "scan-sharing/cache-hit",
				Data: map[string]interface{}{
					"fingerprint": fp,
					"symbols":     symStrings(remapped),
				},
			})
		}
		return &LazySeqRelation{
			seq:        shared.Seq,
			symbols:    remapped,
			options:    shared.Options,
			properties: properties,
		}, nil
	}

	// First scan — delegate to inner matcher
	rel, err := m.inner.Match(q, bindings)
	if err != nil {
		return nil, err
	}

	// Wrap the relation's iterator in a LazySeq and register
	it := rel.Iterator()
	needsCopy := rel.RequiresCopy()
	seq := NewTupleSeq(it, needsCopy)
	symbols := rel.Symbols()

	m.registry.Put(fp, &SharedScan{
		Seq:        seq,
		Symbols:    symbols,
		Options:    rel.Options(),
		Properties: rel.Properties(),
	})

	if m.handler != nil {
		m.handler(annotations.Event{
			Name: "scan-sharing/cache-miss",
			Data: map[string]interface{}{
				"fingerprint": fp,
				"symbols":     symStrings(symbols),
			},
		})
	}

	return &LazySeqRelation{
		seq:        seq,
		symbols:    symbols,
		options:    rel.Options(),
		properties: rel.Properties(),
	}, nil
}

func remapRelationProperties(
	properties RelationProperties,
	originalSymbols []query.Symbol,
	remappedSymbols []query.Symbol,
) RelationProperties {
	renames := make(map[query.Symbol]query.Symbol, len(originalSymbols))
	for i, original := range originalSymbols {
		if i < len(remappedSymbols) {
			renames[original] = remappedSymbols[i]
		}
	}
	result := RelationProperties{}
	for _, clause := range properties.Ordering {
		renamed, ok := renames[clause.Variable]
		if !ok {
			break
		}
		clause.Variable = renamed
		result.Ordering = append(result.Ordering, clause)
	}
	for _, key := range properties.Keys {
		renamedKey := make([]query.Symbol, len(key))
		valid := true
		for i, symbol := range key {
			renamed, ok := renames[symbol]
			if !ok {
				valid = false
				break
			}
			renamedKey[i] = renamed
		}
		if valid {
			result.Keys = append(result.Keys, renamedKey)
		}
	}
	return result
}

// MatchWithConstraints implements PredicateAwareMatcher if the inner matcher
// supports it. Sharing only applies to unbound scans without constraints.
func (m *ScanSharingMatcher) MatchWithConstraints(
	q *query.Query,
	bindings Relations,
	constraints []StorageConstraint,
) (Relation, error) {
	// If there are constraints, don't share — the result depends on constraints
	if len(constraints) > 0 {
		if pm, ok := m.inner.(PredicateAwareMatcher); ok {
			return pm.MatchWithConstraints(q, bindings, constraints)
		}
		return m.inner.Match(q, bindings)
	}
	return m.Match(q, bindings)
}

// LookupAttribute forwards to inner matcher if it supports entity lookup.
func (m *ScanSharingMatcher) LookupAttribute(
	entity datalog.Identity,
	attr datalog.Keyword,
) (interface{}, bool, error) {
	if elm, ok := m.inner.(EntityLookupMatcher); ok {
		return elm.LookupAttribute(entity, attr)
	}
	return nil, false, nil
}

// CanFuseAttributeFetch forwards to inner matcher if it supports fusion, so
// same-entity attribute fusion still applies under scan sharing.
func (m *ScanSharingMatcher) CanFuseAttributeFetch(attr datalog.Keyword) bool {
	if f, ok := m.inner.(AttributeFetchFusable); ok {
		return f.CanFuseAttributeFetch(attr)
	}
	return false
}

// TypeDefault forwards to inner matcher for typed default conversion.
// This is critical for get-else with vector defaults: without forwarding,
// the TypedDefaulter type assertion fails and []interface{} is returned
// instead of []string{}.
func (m *ScanSharingMatcher) TypeDefault(attr datalog.Keyword, defaultVal interface{}) interface{} {
	if td, ok := m.inner.(query.TypedDefaulter); ok {
		return td.TypeDefault(attr, defaultVal)
	}
	return defaultVal
}

// PrefetchEntities forwards to inner matcher if it supports prefetch.
func (m *ScanSharingMatcher) PrefetchEntities(entities []datalog.Identity) {
	if ep, ok := m.inner.(EntityPrefetcher); ok {
		ep.PrefetchEntities(entities)
	}
}

// remapSymbols creates a new symbol list by mapping the shared scan's
// symbols to the requesting pattern's variable names positionally.
// Constants in the pattern (e.g., :task/root) are skipped — only variable
// positions get symbols.
func remapSymbols(origSymbols []query.Symbol, pattern *query.DataPattern) []query.Symbol {
	result := make([]query.Symbol, 0, len(origSymbols))
	origIdx := 0
	for _, elem := range pattern.Elements {
		if origIdx >= len(origSymbols) {
			break
		}
		if v, ok := elem.(query.Variable); ok {
			result = append(result, v.Name)
			origIdx++
		}
		// Constants don't consume a symbol position
	}
	return result
}

func symStrings(syms []query.Symbol) []string {
	s := make([]string, len(syms))
	for i, sym := range syms {
		s[i] = sym.String()
	}
	return s
}

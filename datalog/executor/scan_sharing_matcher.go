package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ScanSharingMatcher wraps a PatternMatcher and serves, from one LazySeq per
// fingerprint, the scans a query's plan reads unbound at two or more sites
// (ScanRegistry). The rules:
//
//   - A reader's close never closes a shared scan.
//   - The registry holds a shared scan's head until every site the plan lists
//     for it has read it.
//   - A fingerprint the plan reads at one site is not shared.
//
// Every other match goes to the inner matcher with its bindings, as it does
// with sharing off.
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

// Match implements PatternMatcher. A match at a site the registry serves reads
// the shared scan's head: the first listed site's read creates the scan by
// asking the inner matcher for it with no bindings, so the shared result is
// the fingerprint's alone. Any other match goes to the inner matcher with its
// bindings.
func (m *ScanSharingMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	head, shared, hit, err := m.registry.GetOrCreate(q, pattern, func() (*SharedScan, error) {
		rel, err := m.inner.Match(q, nil)
		if err != nil {
			return nil, err
		}
		return &SharedScan{
			Seq:        NewTupleSeq(rel.Iterator(), rel.RequiresCopy()),
			Symbols:    rel.Symbols(),
			Options:    rel.Options(),
			Properties: rel.Properties(),
		}, nil
	})
	if err != nil {
		return nil, err
	}
	if head == nil {
		return m.inner.Match(q, bindings)
	}
	remapped := remapSymbols(shared.Symbols, pattern)
	properties := remapRelationProperties(shared.Properties, shared.Symbols, remapped)
	if m.handler != nil {
		eventName := annotations.ScanSharingCacheMiss
		if hit {
			eventName = annotations.ScanSharingCacheHit
		}
		m.handler(annotations.Event{
			Name: eventName,
			Data: map[string]interface{}{
				"fingerprint": FingerprintScan(q, pattern).String(),
				"symbols":     symStrings(remapped),
			},
		})
	}
	return &LazySeqRelation{
		seq:        head,
		symbols:    remapped,
		options:    shared.Options,
		properties: properties,
		shared:     true,
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

// LookupAttribute forwards to the inner matcher. When the inner matcher
// cannot look up, "capability absent" is an error, never (nil, false, nil),
// which reads as the attribute being absent.
func (m *ScanSharingMatcher) LookupAttribute(
	entity datalog.Identity,
	attr datalog.Keyword,
) (interface{}, bool, error) {
	if elm, ok := m.inner.(EntityLookupMatcher); ok {
		return elm.LookupAttribute(entity, attr)
	}
	return nil, false, fmt.Errorf("entity lookup unavailable: inner matcher %T does not support attribute lookup", m.inner)
}

// AttributeLookup forwards to the inner matcher, so a reader of one
// attribute reads it under scan sharing as it does without it. An inner
// matcher that cannot look up is an error.
func (m *ScanSharingMatcher) AttributeLookup(attr datalog.Keyword) (query.AttributeLookup, error) {
	if elm, ok := m.inner.(EntityLookupMatcher); ok {
		return elm.AttributeLookup(attr)
	}
	return nil, fmt.Errorf("entity lookup unavailable: inner matcher %T does not support attribute lookup", m.inner)
}

// LookupSource forwards to the inner matcher, so a database function names
// its source under scan sharing and reads that source's own lookup. An inner
// matcher that cannot look up is an error.
func (m *ScanSharingMatcher) LookupSource(source query.Symbol) (EntityLookupMatcher, error) {
	if elm, ok := m.inner.(EntityLookupMatcher); ok {
		return elm.LookupSource(source)
	}
	return nil, fmt.Errorf("entity lookup unavailable: inner matcher %T does not support attribute lookup", m.inner)
}

// CanFuseAttributeFetch forwards to inner matcher if it supports fusion, so
// same-entity attribute fusion still applies under scan sharing.
func (m *ScanSharingMatcher) CanFuseAttributeFetch(source query.Symbol, attr datalog.Keyword) bool {
	if f, ok := m.inner.(AttributeFetchFusable); ok {
		return f.CanFuseAttributeFetch(source, attr)
	}
	return false
}

// PrefetchEntities forwards to inner matcher if it supports prefetch.
func (m *ScanSharingMatcher) PrefetchEntities(entities []datalog.Identity) error {
	if ep, ok := m.inner.(EntityPrefetcher); ok {
		return ep.PrefetchEntities(entities)
	}
	return nil
}

// RefusesUnknownAttributes implements AttributeVocabulary from the inner
// matcher, so a source refuses under scan sharing as it does without it; a
// matcher without a vocabulary refuses nothing.
func (m *ScanSharingMatcher) RefusesUnknownAttributes(source query.Symbol) bool {
	if v, ok := m.inner.(AttributeVocabulary); ok {
		return v.RefusesUnknownAttributes(source)
	}
	return false
}

// UnknownAttributes implements AttributeVocabulary from the inner matcher; a
// matcher without a vocabulary refuses nothing.
func (m *ScanSharingMatcher) UnknownAttributes(source query.Symbol, attrs []datalog.Keyword) error {
	if v, ok := m.inner.(AttributeVocabulary); ok {
		return v.UnknownAttributes(source, attrs)
	}
	return nil
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

package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// SourceRouter routes pattern queries to PatternMatchers based on pattern.Source.
// It implements PatternMatcher, so it can be used anywhere a PatternMatcher is expected.
//
// For multi-source queries, the SourceRouter maps source symbols (e.g., "$users", "$perms")
// to their PatternMatcher implementations. Patterns with an empty Source are routed to
// the default source "$".
//
// The SourceRouter is the ONLY way to construct a multi-source matcher.
// Single-source usage: NewSourceRouter(map[Symbol]PatternMatcher{"$": db})
// Multi-source usage:  NewSourceRouter(map[Symbol]PatternMatcher{"$": db, "$users": users, "$perms": perms})
type SourceRouter struct {
	sources map[query.Symbol]PatternMatcher
}

// NewSourceRouter creates a pattern matcher that routes to the given sources.
// At minimum, the map should contain a "$" entry for the default source.
func NewSourceRouter(sources map[query.Symbol]PatternMatcher) *SourceRouter {
	return &SourceRouter{sources: sources}
}

// Match implements PatternMatcher. Routes to the appropriate PatternMatcher
// based on pattern.Source. Empty source routes to "$" (the default).
func (sr *SourceRouter) Match(q *query.Query, bindings Relations) (Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	sourceSym := pattern.Source
	if sourceSym == nil {
		sourceSym = datalog.SymDollar
	}

	source, ok := sr.sources[sourceSym]
	if !ok {
		return nil, fmt.Errorf("unknown source: %s", sourceSym)
	}

	return source.Match(q, bindings)
}

// MatchWithConstraints implements PredicateAwareMatcher. Routes to the appropriate
// source based on pattern.Source, then delegates to that source's MatchWithConstraints
// if it supports predicate pushdown. Falls back to Match if it doesn't.
func (sr *SourceRouter) MatchWithConstraints(q *query.Query, bindings Relations, constraints []StorageConstraint) (Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	sourceSym := pattern.Source
	if sourceSym == nil {
		sourceSym = datalog.SymDollar
	}

	source, ok := sr.sources[sourceSym]
	if !ok {
		return nil, fmt.Errorf("unknown source: %s", sourceSym)
	}

	if pam, ok := source.(PredicateAwareMatcher); ok {
		return pam.MatchWithConstraints(q, bindings, constraints)
	}
	return source.Match(q, bindings)
}

// LookupAttribute implements EntityLookupMatcher from the default source ($).
//
// A data-answer method: when no default source exists or it cannot look up,
// "capability absent" is an error — never (nil, false, nil), which reads as
// "attribute absent" and silently flips missing?/get-else semantics.
func (sr *SourceRouter) LookupAttribute(
	entity datalog.Identity,
	attr datalog.Keyword,
) (interface{}, bool, error) {
	lookup, err := sr.LookupSource(datalog.SymDollar)
	if err != nil {
		return nil, false, err
	}
	return lookup.LookupAttribute(entity, attr)
}

// AttributeLookup implements EntityLookupMatcher with the default source's
// own lookup of attr, so a reader of attr reaches the source with no routing
// per entity. A default source the router does not hold, or one that cannot
// look up, is an error, as it is for LookupAttribute.
func (sr *SourceRouter) AttributeLookup(attr datalog.Keyword) (query.AttributeLookup, error) {
	lookup, err := sr.LookupSource(datalog.SymDollar)
	if err != nil {
		return nil, err
	}
	return lookup.AttributeLookup(attr)
}

// LookupSource implements EntityLookupMatcher with the lookup of the source
// named, which it gives as its own $, so the get-else, missing? and get-some
// that name a source read it. A source the router does not hold, or one that
// cannot look up, is an error naming the source.
func (sr *SourceRouter) LookupSource(source query.Symbol) (EntityLookupMatcher, error) {
	named, ok := sr.sources[source]
	if !ok {
		return nil, fmt.Errorf("entity lookup unavailable: no source %s registered", source)
	}
	elm, ok := named.(EntityLookupMatcher)
	if !ok {
		return nil, fmt.Errorf("entity lookup unavailable: source %s (%T) does not support attribute lookup", source, named)
	}
	return elm.LookupSource(datalog.SymDollar)
}

// CanFuseAttributeFetch implements AttributeFetchFusable by asking the source
// named, so the executor's attribute-fetch fusion and the at-most-one
// judgment of a pattern branch consult that source's schema and temporal
// mode through the router. A source the router does not hold, or one
// without the capability, answers false.
func (sr *SourceRouter) CanFuseAttributeFetch(source query.Symbol, attr datalog.Keyword) bool {
	named, ok := sr.sources[source]
	if !ok {
		return false
	}
	if f, ok := named.(AttributeFetchFusable); ok {
		return f.CanFuseAttributeFetch(source, attr)
	}
	return false
}

// RefusesUnknownAttributes implements AttributeVocabulary by asking the
// source named, which answers for itself as $. A source without the
// capability, and a source the router does not hold, refuse nothing.
func (sr *SourceRouter) RefusesUnknownAttributes(source query.Symbol) bool {
	if v, ok := sr.sources[source].(AttributeVocabulary); ok {
		return v.RefusesUnknownAttributes(datalog.SymDollar)
	}
	return false
}

// refusesAnySource reports whether a source the router holds refuses
// attributes its schema does not declare, each asked as the router asks it
// (RefusesUnknownAttributes).
func (sr *SourceRouter) refusesAnySource() bool {
	for source := range sr.sources {
		if sr.RefusesUnknownAttributes(source) {
			return true
		}
	}
	return false
}

// UnknownAttributes implements AttributeVocabulary by asking the source
// named, which answers for itself as $. A source without the capability, and
// a source the router does not hold, refuse nothing.
func (sr *SourceRouter) UnknownAttributes(source query.Symbol, attrs []datalog.Keyword) error {
	if v, ok := sr.sources[source].(AttributeVocabulary); ok {
		return v.UnknownAttributes(datalog.SymDollar, attrs)
	}
	return nil
}

// PrefetchEntities implements EntityPrefetcher from the default source, the
// source a fallback's prefetched branch reads: the default source's own
// prefetch fills its EA cache with the entities. A default source without the
// capability prefetches nothing.
func (sr *SourceRouter) PrefetchEntities(entities []datalog.Identity) error {
	if ep, ok := sr.sources[datalog.SymDollar].(EntityPrefetcher); ok {
		return ep.PrefetchEntities(entities)
	}
	return nil
}

// Compile-time verification that SourceRouter implements all matcher interfaces
var _ PatternMatcher = (*SourceRouter)(nil)
var _ PredicateAwareMatcher = (*SourceRouter)(nil)
var _ EntityLookupMatcher = (*SourceRouter)(nil)
var _ AttributeVocabulary = (*SourceRouter)(nil)
var _ EntityPrefetcher = (*SourceRouter)(nil)

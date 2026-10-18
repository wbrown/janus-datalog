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

// LookupAttribute implements EntityLookupMatcher. Delegates to the default
// source ($) for entity attribute lookups used by database functions
// (get-else, missing?, get-some).
//
// A data-answer method: when no default source exists or it cannot look up,
// "capability absent" is an error — never (nil, false, nil), which reads as
// "attribute absent" and silently flips missing?/get-else semantics.
func (sr *SourceRouter) LookupAttribute(
	entity datalog.Identity,
	attr datalog.Keyword,
) (interface{}, bool, error) {
	source, ok := sr.sources[datalog.SymDollar]
	if !ok {
		return nil, false, fmt.Errorf("entity lookup unavailable: no default source ($) registered")
	}

	if elm, ok := source.(EntityLookupMatcher); ok {
		return elm.LookupAttribute(entity, attr)
	}
	return nil, false, fmt.Errorf("entity lookup unavailable: default source %T does not support attribute lookup", source)
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

// TypeDefault implements query.TypedDefaulter. Delegates to the default
// source ($) for type-converting default values in get-else.
func (sr *SourceRouter) TypeDefault(attr datalog.Keyword, defaultVal interface{}) interface{} {
	source, ok := sr.sources[datalog.SymDollar]
	if !ok {
		return defaultVal
	}
	if td, ok := source.(query.TypedDefaulter); ok {
		return td.TypeDefault(attr, defaultVal)
	}
	return defaultVal
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

// UnknownAttributes implements AttributeVocabulary by asking the source
// named, which answers for itself as $. A source without the capability, and
// a source the router does not hold, refuse nothing.
func (sr *SourceRouter) UnknownAttributes(source query.Symbol, attrs []datalog.Keyword) error {
	if v, ok := sr.sources[source].(AttributeVocabulary); ok {
		return v.UnknownAttributes(datalog.SymDollar, attrs)
	}
	return nil
}

// Compile-time verification that SourceRouter implements all matcher interfaces
var _ PatternMatcher = (*SourceRouter)(nil)
var _ PredicateAwareMatcher = (*SourceRouter)(nil)
var _ EntityLookupMatcher = (*SourceRouter)(nil)
var _ AttributeVocabulary = (*SourceRouter)(nil)

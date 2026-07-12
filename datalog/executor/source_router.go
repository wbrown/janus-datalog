package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
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
func (sr *SourceRouter) LookupAttribute(
	entity datalog.Identity,
	attr datalog.Keyword,
) (interface{}, bool, error) {
	source, ok := sr.sources[datalog.SymDollar]
	if !ok {
		return nil, false, nil
	}

	if elm, ok := source.(EntityLookupMatcher); ok {
		return elm.LookupAttribute(entity, attr)
	}
	return nil, false, nil
}

// CanFuseAttributeFetch implements AttributeFetchFusable by delegating to the
// default source ($), so the executor's attribute-fetch fusion can consult the
// schema and temporal mode through the router.
func (sr *SourceRouter) CanFuseAttributeFetch(attr datalog.Keyword) bool {
	source, ok := sr.sources[datalog.SymDollar]
	if !ok {
		return false
	}
	if f, ok := source.(AttributeFetchFusable); ok {
		return f.CanFuseAttributeFetch(attr)
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

// SetHandler propagates the annotation handler to all underlying matchers that support it.
// This is called by WrapMatcher when annotations are enabled.
func (sr *SourceRouter) SetHandler(handler annotations.Handler) {
	for _, source := range sr.sources {
		if sh, ok := source.(interface{ SetHandler(annotations.Handler) }); ok {
			sh.SetHandler(handler)
		}
	}
}

// Compile-time verification that SourceRouter implements all matcher interfaces
var _ PatternMatcher = (*SourceRouter)(nil)
var _ PredicateAwareMatcher = (*SourceRouter)(nil)
var _ EntityLookupMatcher = (*SourceRouter)(nil)

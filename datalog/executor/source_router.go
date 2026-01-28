package executor

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// IsSourceSymbol returns true if the symbol is a data source marker (starts with "$").
// All source symbols ($, $users, $perms, etc.) are database/source references,
// not regular query variables or input columns.
func IsSourceSymbol(sym query.Symbol) bool {
	return strings.HasPrefix(string(sym), "$")
}

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
func (sr *SourceRouter) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	sourceSym := pattern.Source
	if sourceSym == "" {
		sourceSym = query.Symbol("$")
	}

	source, ok := sr.sources[sourceSym]
	if !ok {
		return nil, fmt.Errorf("unknown source: %s", sourceSym)
	}

	return source.Match(pattern, bindings)
}

// MatchWithConstraints implements PredicateAwareMatcher. Routes to the appropriate
// source based on pattern.Source, then delegates to that source's MatchWithConstraints
// if it supports predicate pushdown. Falls back to Match if it doesn't.
func (sr *SourceRouter) MatchWithConstraints(pattern *query.DataPattern, bindings Relations, constraints []StorageConstraint) (Relation, error) {
	sourceSym := pattern.Source
	if sourceSym == "" {
		sourceSym = query.Symbol("$")
	}

	source, ok := sr.sources[sourceSym]
	if !ok {
		return nil, fmt.Errorf("unknown source: %s", sourceSym)
	}

	if pam, ok := source.(PredicateAwareMatcher); ok {
		return pam.MatchWithConstraints(pattern, bindings, constraints)
	}
	return source.Match(pattern, bindings)
}

// LookupAttribute implements EntityLookupMatcher. Delegates to the default
// source ($) for entity attribute lookups used by database functions
// (get-else, missing?, get-some).
func (sr *SourceRouter) LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool) {
	source, ok := sr.sources[query.Symbol("$")]
	if !ok {
		return nil, false
	}

	if elm, ok := source.(EntityLookupMatcher); ok {
		return elm.LookupAttribute(entity, attr)
	}
	return nil, false
}

// Compile-time verification that SourceRouter implements all matcher interfaces
var _ PatternMatcher = (*SourceRouter)(nil)
var _ PredicateAwareMatcher = (*SourceRouter)(nil)
var _ EntityLookupMatcher = (*SourceRouter)(nil)

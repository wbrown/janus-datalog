package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/constraints"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// PatternMatcher is the interface for matching patterns against the database
type PatternMatcher interface {
	Match(pattern *query.DataPattern, bindings Relations) (Relation, error)
}

// StorageConstraint represents a constraint that can be pushed to storage
// Re-exported from constraints package for backward compatibility
type StorageConstraint = constraints.StorageConstraint

// PredicateAwareMatcher extends PatternMatcher with predicate pushdown capability
type PredicateAwareMatcher interface {
	PatternMatcher
	MatchWithConstraints(
		pattern *query.DataPattern,
		bindings Relations,
		constraints []StorageConstraint,
	) (Relation, error)
}

// EntityLookupMatcher extends PatternMatcher with entity attribute lookup capability.
// This is used by database functions (get-else, missing?, get-some) that need
// to lookup specific entity attributes during expression evaluation.
type EntityLookupMatcher interface {
	PatternMatcher
	// LookupAttribute retrieves the value of an attribute for an entity.
	// Returns (value, true) if the attribute exists, (nil, false) otherwise.
	LookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool)
}

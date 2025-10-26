package executor

import (
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

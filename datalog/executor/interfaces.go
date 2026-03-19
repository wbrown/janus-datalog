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

// EntityPrefetcher extends PatternMatcher with batch entity prefetch capability.
// When a large set of entity IDs is known upfront, prefetching all their
// attributes into the EA cache avoids per-entity storage scans later.
type EntityPrefetcher interface {
	PrefetchEntities(entities []datalog.Identity)
}

// EntityResolver provides CRDT-aware entity resolution.
// This is used by wildcard pulls to get all attributes for an entity with
// proper CRDT resolution (LWW for cardinality-one, add-wins for many, RGA for vector).
// Implemented by Database, not by matchers.
type EntityResolver interface {
	// ResolveAllAttributes returns all CRDT-resolved attributes for an entity.
	// The returned map uses Keyword keys and properly resolved values:
	// - CardinalityOne: single value (LWW)
	// - CardinalityMany: []interface{} (add-wins set, order undefined)
	// - CardinalityVector: []interface{} (RGA ordered list)
	ResolveAllAttributes(entity datalog.Identity) (map[datalog.Keyword]interface{}, error)
}

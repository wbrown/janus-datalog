package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/constraints"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// PatternMatcher is the interface for matching patterns against the database
type PatternMatcher interface {
	// Match executes a Datalog fragment containing exactly one DataPattern.
	// OrderBy and Limit carry structurally safe physical requirements; sources
	// that cannot exploit them may ignore them and return no ordering guarantee.
	Match(q *query.Query, bindings Relations) (Relation, error)
}

// StorageConstraint represents a constraint that can be pushed to storage
// Re-exported from constraints package for backward compatibility
type StorageConstraint = constraints.StorageConstraint

// PredicateAwareMatcher extends PatternMatcher with predicate pushdown capability
type PredicateAwareMatcher interface {
	PatternMatcher
	MatchWithConstraints(
		q *query.Query,
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
	// An absent attribute returns (nil, false, nil); storage and decode failures
	// return a non-nil error and must never masquerade as absence.
	LookupAttribute(
		entity datalog.Identity,
		attr datalog.Keyword,
	) (value interface{}, found bool, err error)
}

// AttributeFetchFusable reports whether a same-entity fetch of attr on a
// source can be fused into a per-tuple LookupAttribute binding instead of a
// separate pattern match + hash join. Fusion is valid only when the source's
// LookupAttribute returns the single value a matched pattern would, which
// requires both:
//   - the attribute is CardinalityOne (CardinalityMany/Vector must expand on
//     the join path), and
//   - the matcher is NOT in history mode (history exposes every raw version,
//     so a one-value attach would drop superseded datoms).
//
// The matcher owns both facts (schema + temporal mode), so the decision lives
// there rather than being reconstructed by the executor. The source is the
// symbol the query names it by, $ for the default source; a matcher that is
// one source answers for itself under any name, and a router answers from
// the source named, a source without the capability answering false.
type AttributeFetchFusable interface {
	CanFuseAttributeFetch(source query.Symbol, attr datalog.Keyword) bool
}

// AttributeVocabulary reports the attributes of a roster that a source's
// schema does not declare. The matcher owns the schema, so the decision lives
// there, and a matcher without the capability refuses nothing. The source is
// the symbol the query names it by, $ for the default source; a matcher that
// is one source answers for itself under any name, and a router answers from
// the source named. A source whose RefusesUnknownAttributes is false is never
// asked about a roster, so none is collected for it. A query's roster, a rule
// set's, the attributes the inputs bind and a pull pattern's are checked
// alike. UnknownAttributes returns nil when every attribute is known, and
// otherwise the error naming each unknown one.
type AttributeVocabulary interface {
	RefusesUnknownAttributes(source query.Symbol) bool
	UnknownAttributes(source query.Symbol, attrs []datalog.Keyword) error
}

// EntityPrefetcher extends PatternMatcher with batch entity prefetch capability.
// When a large set of entity IDs is known upfront, prefetching all their
// attributes into the EA cache avoids per-entity storage scans later.
// A returned error means the store failed mid-scan; callers propagate it —
// a failing store fails the query.
type EntityPrefetcher interface {
	PrefetchEntities(entities []datalog.Identity) error
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

// BatchEntityResolver extends EntityResolver with wildcard resolution for a
// complete entity set. Implementations may share storage traversal state across
// entities while preserving input order in the returned slice.
type BatchEntityResolver interface {
	EntityResolver
	ResolveAllAttributesMany(
		entities []datalog.Identity,
	) ([]map[datalog.Keyword]interface{}, error)
}

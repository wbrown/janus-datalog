package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// PullExecutor executes pull patterns against the database
type PullExecutor struct {
	matcher PatternMatcher
}

// NewPullExecutor creates a new pull executor
func NewPullExecutor(matcher PatternMatcher) *PullExecutor {
	return &PullExecutor{
		matcher: matcher,
	}
}

// Pull executes a pull pattern for a single entity
// Uses cycle detection to handle circular references
func (pe *PullExecutor) Pull(entity datalog.Identity, pattern *query.PullPattern) (map[string]interface{}, error) {
	visited := make(map[[20]byte]bool)
	return pe.pullWithVisited(entity, pattern, visited)
}

// pullWithVisited executes a pull pattern with cycle detection
func (pe *PullExecutor) pullWithVisited(entity datalog.Identity, pattern *query.PullPattern, visited map[[20]byte]bool) (map[string]interface{}, error) {
	if pattern == nil {
		return nil, nil
	}

	// Check for cycle using entity hash
	entityHash := entity.Hash()
	if visited[entityHash] {
		return nil, nil // Cycle detected, stop recursion
	}
	visited[entityHash] = true
	defer delete(visited, entityHash) // Allow revisiting from different paths

	result := make(map[string]interface{})

	for _, spec := range pattern.Specs {
		if err := pe.processSpec(entity, spec, result, visited); err != nil {
			return nil, err
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// processSpec processes a single pull spec and adds results to the map
func (pe *PullExecutor) processSpec(entity datalog.Identity, spec query.PullAttrSpec, result map[string]interface{}, visited map[[20]byte]bool) error {
	switch s := spec.(type) {
	case *query.PullAttribute:
		// Simple attribute lookup
		if val, ok := pe.lookupAttribute(entity, s.Attr); ok {
			result[query.KeyName(s.Attr)] = val
		}
		// Missing attributes are omitted (not included as nil)

	case *query.PullWildcard:
		// Get all attributes for entity
		datoms, err := pe.getAllAttributes(entity)
		if err != nil {
			return fmt.Errorf("wildcard pull failed: %w", err)
		}
		for _, datom := range datoms {
			result[query.KeyName(datom.A)] = datom.V
		}

	case *query.PullMapSpec:
		// Follow reference and pull nested pattern
		if refVal, ok := pe.lookupAttribute(entity, s.Attr); ok {
			if refEntity, ok := refVal.(datalog.Identity); ok {
				nested, err := pe.pullWithVisited(refEntity, s.Pattern, visited)
				if err != nil {
					return fmt.Errorf("nested pull for %s failed: %w", s.Attr.String(), err)
				}
				if nested != nil {
					result[query.KeyName(s.Attr)] = nested
				}
			}
			// If value is not an Identity, it's not a reference - skip
		}

	case *query.PullLimitExpr:
		// For cardinality-many (future), limit results
		// Currently just lookup single value
		if val, ok := pe.lookupAttribute(entity, s.Attr); ok {
			result[query.KeyName(s.Attr)] = val
		}

	case *query.PullDefaultExpr:
		// Lookup with default value
		if val, ok := pe.lookupAttribute(entity, s.Attr); ok {
			result[query.KeyName(s.Attr)] = val
		} else {
			result[query.KeyName(s.Attr)] = s.Default
		}

	default:
		return fmt.Errorf("unknown pull spec type: %T", spec)
	}

	return nil
}

// lookupAttribute retrieves a single attribute value using the matcher
func (pe *PullExecutor) lookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool) {
	// Use EntityLookupMatcher interface if available
	if lookupMatcher, ok := pe.matcher.(EntityLookupMatcher); ok {
		return lookupMatcher.LookupAttribute(entity, attr)
	}

	// Fallback: use pattern matching
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: "?v"},
		},
	}

	rel, err := pe.matcher.Match(pattern, nil)
	if err != nil || rel == nil {
		return nil, false
	}

	// Get the first result
	// Note: Avoid calling rel.IsEmpty() as it may consume the first tuple
	// in non-streaming mode. Instead, just try to iterate.
	it := rel.Iterator()
	defer it.Close()

	if it.Next() {
		tuple := it.Tuple()
		// Find the value column
		cols := rel.Columns()
		for i, col := range cols {
			if col == "?v" && i < len(tuple) {
				return tuple[i], true
			}
		}
	}

	return nil, false
}

// getAllAttributes retrieves all datoms for an entity (for wildcard pull)
func (pe *PullExecutor) getAllAttributes(entity datalog.Identity) ([]datalog.Datom, error) {
	// Create pattern: [entity ?a ?v] - gets all attributes
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Variable{Name: "?a"},
			query.Variable{Name: "?v"},
		},
	}

	rel, err := pe.matcher.Match(pattern, nil)
	if err != nil {
		return nil, err
	}

	if rel == nil {
		return nil, nil
	}

	// Note: Avoid calling rel.IsEmpty() as it may consume the first tuple
	// in non-streaming mode. Empty results are handled naturally by the
	// iteration loop below.

	// Find column indices
	cols := rel.Columns()
	aIdx := -1
	vIdx := -1
	for i, col := range cols {
		if col == "?a" {
			aIdx = i
		} else if col == "?v" {
			vIdx = i
		}
	}

	if aIdx < 0 || vIdx < 0 {
		return nil, fmt.Errorf("missing expected columns in result")
	}

	// Collect datoms
	var datoms []datalog.Datom
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		if aIdx < len(tuple) && vIdx < len(tuple) {
			// Handle both Keyword and *Keyword (BadgerMatcher may return pointers)
			var attr datalog.Keyword
			switch a := tuple[aIdx].(type) {
			case datalog.Keyword:
				attr = a
			case *datalog.Keyword:
				if a == nil {
					continue
				}
				attr = *a
			default:
				continue
			}
			datoms = append(datoms, datalog.Datom{
				E: entity,
				A: attr,
				V: tuple[vIdx],
			})
		}
	}

	return datoms, nil
}

// PullMany executes a pull pattern for multiple entities
func (pe *PullExecutor) PullMany(entities []datalog.Identity, pattern *query.PullPattern) ([]map[string]interface{}, error) {
	results := make([]map[string]interface{}, len(entities))
	for i, entity := range entities {
		result, err := pe.Pull(entity, pattern)
		if err != nil {
			return nil, fmt.Errorf("pull failed for entity %d: %w", i, err)
		}
		results[i] = result
	}
	return results, nil
}

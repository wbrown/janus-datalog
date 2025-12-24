package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// PullExecutor executes pull patterns against the database
type PullExecutor struct {
	matcher PatternMatcher
	ctx     PullContext
}

// NewPullExecutor creates a new pull executor
func NewPullExecutor(matcher PatternMatcher) *PullExecutor {
	return &PullExecutor{
		matcher: matcher,
		ctx:     &BasePullContext{},
	}
}

// NewPullExecutorWithHandler creates a new pull executor with annotation support
func NewPullExecutorWithHandler(matcher PatternMatcher, handler annotations.Handler) *PullExecutor {
	return &PullExecutor{
		matcher: matcher,
		ctx:     NewPullContext(handler),
	}
}

// SetHandler configures the annotation handler
func (pe *PullExecutor) SetHandler(handler annotations.Handler) {
	pe.ctx = NewPullContext(handler)
}

// Pull executes a pull pattern for a single entity
// Uses cycle detection to handle circular references
func (pe *PullExecutor) Pull(entity datalog.Identity, pattern *query.PullPattern) (map[string]interface{}, error) {
	pe.ctx.PullBegin(entity, len(pattern.Specs), false)

	visited := make(map[[20]byte]bool)
	result, err := pe.pullWithVisited(entity, pattern, visited, 0)

	attrCount := 0
	if result != nil {
		attrCount = len(result)
	}
	pe.ctx.PullComplete(entity, attrCount, false, err)

	return result, err
}

// pullWithVisited executes a pull pattern with cycle detection
func (pe *PullExecutor) pullWithVisited(entity datalog.Identity, pattern *query.PullPattern, visited map[[20]byte]bool, depth int) (map[string]interface{}, error) {
	if pattern == nil {
		return nil, nil
	}

	// Check for cycle using entity hash
	entityHash := entity.Hash()
	if visited[entityHash] {
		pe.ctx.CycleDetected(entity, depth)
		return nil, nil // Cycle detected, stop recursion
	}
	visited[entityHash] = true
	defer delete(visited, entityHash) // Allow revisiting from different paths

	pe.ctx.EntityBegin(entity, depth, len(pattern.Specs))

	result := make(map[string]interface{})

	for _, spec := range pattern.Specs {
		if err := pe.processSpec(entity, spec, result, visited, depth); err != nil {
			return nil, err
		}
	}

	pe.ctx.EntityComplete(entity, depth, len(result))

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// processSpec processes a single pull spec and adds results to the map
func (pe *PullExecutor) processSpec(entity datalog.Identity, spec query.PullAttrSpec, result map[string]interface{}, visited map[[20]byte]bool, depth int) error {
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
				pe.ctx.NestedBegin(entity, s.Attr, refEntity, depth+1, false)

				nested, err := pe.pullWithVisited(refEntity, s.Pattern, visited, depth+1)

				attrCount := 0
				if nested != nil {
					attrCount = len(nested)
				}
				pe.ctx.NestedComplete(entity, s.Attr, refEntity, depth+1, attrCount, err)

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
		var val interface{}
		var found bool
		pe.ctx.AttributeLookup(entity, attr, found, "direct", func() {
			val, found = lookupMatcher.LookupAttribute(entity, attr)
		})
		return val, found
	}

	// Fallback: use pattern matching
	var val interface{}
	var found bool
	pe.ctx.AttributeLookup(entity, attr, found, "pattern", func() {
		val, found = pe.lookupAttributeViaPattern(entity, attr)
	})
	return val, found
}

// lookupAttributeViaPattern is the fallback path using pattern matching
func (pe *PullExecutor) lookupAttributeViaPattern(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool) {
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
	it := rel.Iterator()
	defer it.Close()

	if it.Next() {
		tuple := it.Tuple()
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
	var datoms []datalog.Datom
	var err error

	pe.ctx.AllAttributes(entity, func() int {
		datoms, err = pe.getAllAttributesInternal(entity)
		return len(datoms)
	})

	return datoms, err
}

// getAllAttributesInternal is the actual implementation
func (pe *PullExecutor) getAllAttributesInternal(entity datalog.Identity) ([]datalog.Datom, error) {
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
			// Handle *Keyword only - value-type Keywords are a bug
			var attr datalog.Keyword
			switch a := tuple[aIdx].(type) {
			case datalog.Keyword:
				if a == nil {
					continue
				}
				attr = a
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

// ============================================================================
// Resolved Pull Pattern Methods
// ============================================================================
//
// These methods work with pre-resolved patterns that have cardinality/ref info
// baked in from schema resolution.

// PullResolved executes a resolved pull pattern for a single entity
// Uses pre-resolved cardinality info for proper handling of many-valued attributes
func (pe *PullExecutor) PullResolved(entity datalog.Identity, pattern *query.ResolvedPullPattern) (map[string]interface{}, error) {
	pe.ctx.PullBegin(entity, len(pattern.Specs), true)

	visited := make(map[[20]byte]bool)
	result, err := pe.pullResolvedWithVisited(entity, pattern, visited, 0)

	attrCount := 0
	if result != nil {
		attrCount = len(result)
	}
	pe.ctx.PullComplete(entity, attrCount, true, err)

	return result, err
}

// pullResolvedWithVisited executes a resolved pull pattern with cycle detection
func (pe *PullExecutor) pullResolvedWithVisited(entity datalog.Identity, pattern *query.ResolvedPullPattern, visited map[[20]byte]bool, depth int) (map[string]interface{}, error) {
	if pattern == nil {
		return nil, nil
	}

	// Check for cycle using entity hash
	entityHash := entity.Hash()
	if visited[entityHash] {
		pe.ctx.CycleDetected(entity, depth)
		return nil, nil // Cycle detected, stop recursion
	}
	visited[entityHash] = true
	defer delete(visited, entityHash) // Allow revisiting from different paths

	pe.ctx.EntityBegin(entity, depth, len(pattern.Specs))

	result := make(map[string]interface{})

	for _, spec := range pattern.Specs {
		if err := pe.processResolvedSpec(entity, spec, result, visited, depth); err != nil {
			return nil, err
		}
	}

	pe.ctx.EntityComplete(entity, depth, len(result))

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// processResolvedSpec processes a single resolved pull spec
func (pe *PullExecutor) processResolvedSpec(entity datalog.Identity, spec query.ResolvedPullAttrSpec, result map[string]interface{}, visited map[[20]byte]bool, depth int) error {
	switch s := spec.(type) {
	case *query.ResolvedPullAttribute:
		if s.IsMany {
			// Cardinality-many: get all values as array
			values := pe.lookupAllValues(entity, s.Attr)
			if len(values) > 0 {
				result[query.KeyName(s.Attr)] = values
			}
		} else {
			// Cardinality-one: get single value
			if val, ok := pe.lookupAttribute(entity, s.Attr); ok {
				result[query.KeyName(s.Attr)] = val
			}
		}

	case *query.ResolvedPullWildcard:
		// Get all attributes for entity (same as unresolved)
		datoms, err := pe.getAllAttributes(entity)
		if err != nil {
			return fmt.Errorf("wildcard pull failed: %w", err)
		}
		for _, datom := range datoms {
			result[query.KeyName(datom.A)] = datom.V
		}

	case *query.ResolvedPullMapSpec:
		if s.IsMany {
			// Cardinality-many reference: follow all refs and pull nested
			refs := pe.lookupAllValues(entity, s.Attr)
			if len(refs) > 0 {
				nestedResults := make([]interface{}, 0, len(refs))
				for _, refVal := range refs {
					if refEntity, ok := getIdentity(refVal); ok {
						pe.ctx.NestedBegin(entity, s.Attr, refEntity, depth+1, true)

						nested, err := pe.pullResolvedWithVisited(refEntity, s.Pattern, visited, depth+1)

						attrCount := 0
						if nested != nil {
							attrCount = len(nested)
						}
						pe.ctx.NestedComplete(entity, s.Attr, refEntity, depth+1, attrCount, err)

						if err != nil {
							return fmt.Errorf("nested pull for %s failed: %w", s.Attr.String(), err)
						}
						if nested != nil {
							nestedResults = append(nestedResults, nested)
						}
					}
				}
				if len(nestedResults) > 0 {
					result[query.KeyName(s.Attr)] = nestedResults
				}
			}
		} else {
			// Cardinality-one reference: follow single ref
			if refVal, ok := pe.lookupAttribute(entity, s.Attr); ok {
				if refEntity, ok := getIdentity(refVal); ok {
					pe.ctx.NestedBegin(entity, s.Attr, refEntity, depth+1, false)

					nested, err := pe.pullResolvedWithVisited(refEntity, s.Pattern, visited, depth+1)

					attrCount := 0
					if nested != nil {
						attrCount = len(nested)
					}
					pe.ctx.NestedComplete(entity, s.Attr, refEntity, depth+1, attrCount, err)

					if err != nil {
						return fmt.Errorf("nested pull for %s failed: %w", s.Attr.String(), err)
					}
					if nested != nil {
						result[query.KeyName(s.Attr)] = nested
					}
				}
			}
		}

	case *query.ResolvedPullLimitExpr:
		if s.IsMany {
			// Get all values and apply limit
			values := pe.lookupAllValues(entity, s.Attr)
			if len(values) > s.Limit {
				values = values[:s.Limit]
			}
			if len(values) > 0 {
				result[query.KeyName(s.Attr)] = values
			}
		} else {
			// Cardinality-one: limit doesn't really apply
			if val, ok := pe.lookupAttribute(entity, s.Attr); ok {
				result[query.KeyName(s.Attr)] = val
			}
		}

	case *query.ResolvedPullDefaultExpr:
		if s.IsMany {
			// Cardinality-many with default
			values := pe.lookupAllValues(entity, s.Attr)
			if len(values) > 0 {
				result[query.KeyName(s.Attr)] = values
			} else {
				result[query.KeyName(s.Attr)] = s.Default
			}
		} else {
			// Cardinality-one with default
			if val, ok := pe.lookupAttribute(entity, s.Attr); ok {
				result[query.KeyName(s.Attr)] = val
			} else {
				result[query.KeyName(s.Attr)] = s.Default
			}
		}

	default:
		return fmt.Errorf("unknown resolved pull spec type: %T", spec)
	}

	return nil
}

// lookupAllValues retrieves all values for a cardinality-many attribute
func (pe *PullExecutor) lookupAllValues(entity datalog.Identity, attr datalog.Keyword) []interface{} {
	var values []interface{}

	pe.ctx.ManyValues(entity, attr, func() int {
		values = pe.lookupAllValuesInternal(entity, attr)
		return len(values)
	})

	return values
}

// lookupAllValuesInternal is the actual implementation
func (pe *PullExecutor) lookupAllValuesInternal(entity datalog.Identity, attr datalog.Keyword) []interface{} {
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: "?v"},
		},
	}

	rel, err := pe.matcher.Match(pattern, nil)
	if err != nil || rel == nil {
		return nil
	}

	// Find value column index
	cols := rel.Columns()
	vIdx := -1
	for i, col := range cols {
		if col == "?v" {
			vIdx = i
			break
		}
	}

	if vIdx < 0 {
		return nil
	}

	// Collect all values
	var values []interface{}
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		if vIdx < len(tuple) {
			values = append(values, tuple[vIdx])
		}
	}

	return values
}

// PullResolvedMany executes a resolved pull pattern for multiple entities
func (pe *PullExecutor) PullResolvedMany(entities []datalog.Identity, pattern *query.ResolvedPullPattern) ([]map[string]interface{}, error) {
	results := make([]map[string]interface{}, len(entities))
	for i, entity := range entities {
		result, err := pe.PullResolved(entity, pattern)
		if err != nil {
			return nil, fmt.Errorf("pull failed for entity %d: %w", i, err)
		}
		results[i] = result
	}
	return results, nil
}

// getIdentity extracts an Identity from a value
func getIdentity(val interface{}) (datalog.Identity, bool) {
	switch v := val.(type) {
	case datalog.Identity:
		if v != nil {
			return v, true
		}
	}
	return nil, false
}

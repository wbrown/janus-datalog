package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// AnnotatedMatcher wraps a PatternMatcher to add transparent annotation tracking.
// This decorator pattern allows annotations to be added without modifying the
// underlying implementation, similar to Clojure's metadata-based approach.
type AnnotatedMatcher struct {
	underlying PatternMatcher
	collector  *annotations.Collector
}

// WrapMatcher creates a decorator that adds annotations to any PatternMatcher.
// If handler is nil, returns the original matcher unchanged for zero overhead.
//
// This provides Clojure-like transparent instrumentation:
//
//	matcher := storage.NewPatternMatcher(store)
//	matcher = executor.WrapMatcher(matcher, handler)  // Automatically annotated!
//
// All Match() operations on the wrapped matcher will be timed and logged.
func WrapMatcher(m PatternMatcher, handler annotations.Handler) PatternMatcher {
	if handler == nil {
		return m // Zero overhead when disabled
	}

	// Create the wrapper
	wrapper := &AnnotatedMatcher{
		underlying: m,
		collector:  annotations.NewCollector(handler),
	}

	// If the underlying matcher has a SetHandler method, configure it for detailed events
	// This allows storage layer to emit detailed events (hash join stats, etc)
	if sh, ok := m.(interface{ SetHandler(annotations.Handler) }); ok {
		sh.SetHandler(handler)
	}

	return wrapper
}

// Match implements PatternMatcher with transparent annotation.
func (m *AnnotatedMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	pattern, queryErr := q.SingleDataPattern()
	if queryErr != nil {
		return nil, queryErr
	}
	start := time.Now()

	// Collect binding information if present
	var bindingSymbols []string
	var bindingSize int

	if bindings != nil && len(bindings) > 0 {
		// Find best binding relation for context
		bindingRel := bindings.FindBestForPattern(pattern)
		if bindingRel != nil {
			bindingSyms := bindingRel.Symbols()
			bindingSymbols = make([]string, len(bindingSyms))
			for i, sym := range bindingSyms {
				bindingSymbols[i] = sym.String()
			}
			bindingSize = bindingRel.Size()
		}
	}

	// Execute the underlying match
	result, err := m.underlying.Match(q, bindings)

	// Record completion with grouped metrics
	data := m.collector.GetDataMap()
	data[annotations.KeyPattern] = pattern
	data["match.count"] = 0
	data["success"] = err == nil

	// Add binding information if it was present
	if len(bindingSymbols) > 0 {
		data["binding.symbols"] = bindingSymbols
		data[annotations.KeyBindingSize] = bindingSize
	}

	if result != nil {
		data["match.count"] = result.Size()

		// Add symbol order information for rendering
		symbolOrder := make([]string, len(result.Symbols()))
		for i, sym := range result.Symbols() {
			symbolOrder[i] = sym.String()
		}
		data["symbol.order"] = symbolOrder
	}

	if err != nil {
		data["error"] = err.Error()
	}

	m.collector.AddTiming(annotations.MatchesToRelations, start, data)

	return result, err
}

// MatchWithConstraints implements PredicateAwareMatcher if the underlying matcher supports it.
// This allows the decorator to be transparent even for extended interfaces.
func (m *AnnotatedMatcher) MatchWithConstraints(
	q *query.Query,
	bindings Relations,
	constraints []StorageConstraint,
) (Relation, error) {
	pattern, queryErr := q.SingleDataPattern()
	if queryErr != nil {
		return nil, queryErr
	}
	// Check if underlying matcher supports constraints
	if pm, ok := m.underlying.(PredicateAwareMatcher); ok {
		start := time.Now()

		// Collect binding information if present
		var bindingSymbols []string
		var bindingSize int

		if bindings != nil && len(bindings) > 0 {
			bindingRel := bindings.FindBestForPattern(pattern)
			if bindingRel != nil {
				bindingSyms := bindingRel.Symbols()
				bindingSymbols = make([]string, len(bindingSyms))
				for i, sym := range bindingSyms {
					bindingSymbols[i] = sym.String()
				}
				bindingSize = bindingRel.Size()
			}
		}

		// Execute with constraints
		result, err := pm.MatchWithConstraints(q, bindings, constraints)

		// Record completion
		data := m.collector.GetDataMap()
		data[annotations.KeyPattern] = pattern
		data["constraint.count"] = len(constraints)
		data["match.count"] = 0
		data["success"] = err == nil

		// Add binding information if it was present
		if len(bindingSymbols) > 0 {
			data["binding.symbols"] = bindingSymbols
			data[annotations.KeyBindingSize] = bindingSize
		}

		if result != nil {
			data["match.count"] = result.Size()

			symbolOrder := make([]string, len(result.Symbols()))
			for i, sym := range result.Symbols() {
				symbolOrder[i] = sym.String()
			}
			data["symbol.order"] = symbolOrder
		}

		if err != nil {
			data["error"] = err.Error()
		}

		m.collector.AddTiming(annotations.MatchesToRelations, start, data)

		return result, err
	}

	// Fall back to regular Match if constraints not supported
	return m.Match(q, bindings)
}

// Collector returns the underlying collector for context integration.
// This allows the executor context to access the collector if needed.
func (m *AnnotatedMatcher) Collector() *annotations.Collector {
	return m.collector
}

// GetHandler implements HandlerProvider interface.
// This allows storage layer to emit detailed events without breaking decorator pattern.
func (m *AnnotatedMatcher) GetHandler() annotations.Handler {
	if m.collector != nil {
		return m.collector.Handler()
	}
	return nil
}

// WithCollector implements CollectorAware for backward compatibility.
// Note: This is deprecated - use WrapMatcher at construction time instead.
func (m *AnnotatedMatcher) WithCollector(collector *annotations.Collector) CollectorAware {
	// Already wrapped, just update the collector
	m.collector = collector
	return m
}

// LookupAttribute forwards to the underlying matcher so get-else, missing?,
// and get-some work through the annotation wrapper.
//
// LookupAttribute is a data-answer method: its return states a fact about
// the database ((value, present) for the entity's attribute). When the
// underlying matcher cannot look up, "capability absent" must be an error —
// never encoded as a value in the answer domain, where (nil, false, nil)
// reads as "attribute absent" and turns every missing? true and every
// get-else into its default. Attaching an annotation handler wraps the
// matcher, so a fabricated answer here also made observability change query
// results. Best-effort forwards (PrefetchEntities' no-op, the boolean
// capability queries) are different: declining is their true answer.
func (m *AnnotatedMatcher) LookupAttribute(
	entity datalog.Identity,
	attr datalog.Keyword,
) (interface{}, bool, error) {
	if elm, ok := m.underlying.(EntityLookupMatcher); ok {
		return elm.LookupAttribute(entity, attr)
	}
	return nil, false, fmt.Errorf("entity lookup unavailable: underlying matcher %T does not support attribute lookup", m.underlying)
}

// CanFuseAttributeFetch implements AttributeFetchFusable if the underlying
// matcher supports it, so same-entity attribute fusion still applies when an
// annotation handler is attached.
func (m *AnnotatedMatcher) CanFuseAttributeFetch(attr datalog.Keyword) bool {
	if f, ok := m.underlying.(AttributeFetchFusable); ok {
		return f.CanFuseAttributeFetch(attr)
	}
	return false
}

// PrefetchEntities implements EntityPrefetcher if the underlying matcher supports it.
func (m *AnnotatedMatcher) PrefetchEntities(entities []datalog.Identity) error {
	if ep, ok := m.underlying.(EntityPrefetcher); ok {
		return ep.PrefetchEntities(entities)
	}
	return nil
}

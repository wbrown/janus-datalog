package executor

import (
	"time"

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
//	matcher := storage.NewBadgerMatcher(store)
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
func (m *AnnotatedMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	start := time.Now()

	// Collect binding information if present
	var bindingColumns []string
	var bindingSize int

	if bindings != nil && len(bindings) > 0 {
		// Find best binding relation for context
		bindingRel := bindings.FindBestForPattern(pattern)
		if bindingRel != nil {
			bindingCols := bindingRel.Columns()
			bindingColumns = make([]string, len(bindingCols))
			for i, col := range bindingCols {
				bindingColumns[i] = string(col)
			}
			bindingSize = bindingRel.Size()
		}
	}

	// Execute the underlying match
	result, err := m.underlying.Match(pattern, bindings)

	// Record completion with grouped metrics
	data := m.collector.GetDataMap()
	data["pattern"] = pattern.String()
	data["match.count"] = 0
	data["success"] = err == nil

	// Add binding information if it was present
	if len(bindingColumns) > 0 {
		data["binding.columns"] = bindingColumns
		data["binding.size"] = bindingSize
	}

	if result != nil {
		data["match.count"] = result.Size()

		// Add symbol order information for rendering
		symbolOrder := make([]string, len(result.Columns()))
		for i, col := range result.Columns() {
			symbolOrder[i] = string(col)
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
	pattern *query.DataPattern,
	bindings Relations,
	constraints []StorageConstraint,
) (Relation, error) {
	// Check if underlying matcher supports constraints
	if pm, ok := m.underlying.(PredicateAwareMatcher); ok {
		start := time.Now()

		// Collect binding information if present
		var bindingColumns []string
		var bindingSize int

		if bindings != nil && len(bindings) > 0 {
			bindingRel := bindings.FindBestForPattern(pattern)
			if bindingRel != nil {
				bindingCols := bindingRel.Columns()
				bindingColumns = make([]string, len(bindingCols))
				for i, col := range bindingCols {
					bindingColumns[i] = string(col)
				}
				bindingSize = bindingRel.Size()
			}
		}

		// Execute with constraints
		result, err := pm.MatchWithConstraints(pattern, bindings, constraints)

		// Record completion
		data := m.collector.GetDataMap()
		data["pattern"] = pattern.String()
		data["constraint.count"] = len(constraints)
		data["match.count"] = 0
		data["success"] = err == nil

		// Add binding information if it was present
		if len(bindingColumns) > 0 {
			data["binding.columns"] = bindingColumns
			data["binding.size"] = bindingSize
		}

		if result != nil {
			data["match.count"] = result.Size()

			symbolOrder := make([]string, len(result.Columns()))
			for i, col := range result.Columns() {
				symbolOrder[i] = string(col)
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
	return m.Match(pattern, bindings)
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

// WithTimeRanges implements TimeRangeAware if the underlying matcher supports it.
// This ensures decorators are transparent for all interface extensions.
func (m *AnnotatedMatcher) WithTimeRanges(ranges []TimeRange) TimeRangeAware {
	if tra, ok := m.underlying.(TimeRangeAware); ok {
		// Update underlying matcher and return a new decorated version
		updated := tra.WithTimeRanges(ranges)
		return &AnnotatedMatcher{
			underlying: updated.(PatternMatcher),
			collector:  m.collector,
		}
	}
	// Underlying doesn't support time ranges, return self
	return m
}

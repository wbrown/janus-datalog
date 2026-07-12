package executor

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// Context provides clean annotation points for query execution tracking.
type Context interface {
	// Query lifecycle
	QueryBegin(query string)
	QueryPlanCreated(plan string)
	QueryComplete(relationCount, tupleCount int, err error)

	// Relation operations
	JoinRelations(left, right Relation, fn func() Relation) Relation
	CollapseRelations(rels []Relation, fn func() []Relation) []Relation

	// Get underlying collector
	Collector() *annotations.Collector

	// ScanRegistry returns the per-query scan registry for sharing unbound
	// scan results across subqueries. Lazy-initialized on first call.
	ScanRegistry() *ScanRegistry
}

// BaseContext provides a no-op implementation with zero overhead.
type BaseContext struct {
	scanRegistry *ScanRegistry
}

// NewContext creates an appropriate context based on whether annotations are needed.
func NewContext(handler annotations.Handler) Context {
	if handler == nil {
		return &BaseContext{}
	}
	return &AnnotatedContext{
		collector: annotations.NewCollector(handler),
	}
}

// forkContext returns an independent Context for a concurrent worker. A Context
// carries per-query mutable state — AnnotatedContext.queryStart and the lazily
// created BaseContext scanRegistry — neither of which is safe for
// concurrent use. Parallel workers must therefore each get their own context.
// The annotation collector is shared (it is internally synchronized), so events
// still aggregate into one place.
func forkContext(ctx Context) Context {
	switch c := ctx.(type) {
	case *AnnotatedContext:
		return &AnnotatedContext{collector: c.collector}
	case *BaseContext:
		return &BaseContext{}
	default:
		return ctx
	}
}

// BaseContext implementations - all are simple pass-throughs

func (c *BaseContext) QueryBegin(query string) {}

func (c *BaseContext) QueryPlanCreated(plan string) {}

func (c *BaseContext) QueryComplete(relationCount, tupleCount int, err error) {}

func (c *BaseContext) JoinRelations(left, right Relation, fn func() Relation) Relation {
	return fn()
}

func (c *BaseContext) CollapseRelations(rels []Relation, fn func() []Relation) []Relation {
	return fn()
}

func (c *BaseContext) Collector() *annotations.Collector {
	return nil
}

// ScanRegistry returns the per-query scan registry, initializing lazily.
func (c *BaseContext) ScanRegistry() *ScanRegistry {
	if c.scanRegistry == nil {
		c.scanRegistry = NewScanRegistry()
	}
	return c.scanRegistry
}

// AnnotatedContext provides full annotation tracking
type AnnotatedContext struct {
	BaseContext
	collector  *annotations.Collector
	queryStart time.Time
}

func (c *AnnotatedContext) QueryBegin(query string) {
	c.queryStart = time.Now()
	c.collector.Add(annotations.Event{
		Name:  annotations.QueryInvoked,
		Start: c.queryStart,
		Data: map[string]interface{}{
			"query": query,
		},
	})
}

func (c *AnnotatedContext) QueryPlanCreated(plan string) {
	c.collector.Add(annotations.Event{
		Name:  annotations.QueryPlanCreated,
		Start: time.Now(),
		Data: map[string]interface{}{
			"plan": plan,
		},
	})
}

func (c *AnnotatedContext) QueryComplete(relationCount, tupleCount int, err error) {
	data := map[string]interface{}{
		"relations.count": relationCount,
		"tuples.count":    tupleCount,
		"success":         err == nil,
	}

	if err != nil {
		data["error"] = err.Error()
	}

	c.collector.AddTiming(annotations.QueryComplete, c.queryStart, data)
}

func (c *AnnotatedContext) JoinRelations(left, right Relation, fn func() Relation) Relation {
	start := time.Now()
	leftSize := -1 // Use -1 to indicate unknown size
	rightSize := -1

	// CRITICAL FIX: Don't call Size() on StreamingRelations before the join
	// Size() can trigger materialization which may lose tuples if the iterator was partially consumed
	// Only call Size() on materialized relations where it's safe
	if left != nil {
		if _, isStreaming := left.(*StreamingRelation); !isStreaming {
			leftSize = left.Size()
		}
	}
	if right != nil {
		if _, isStreaming := right.(*StreamingRelation); !isStreaming {
			rightSize = right.Size()
		}
	}

	result := fn()

	resultSize := 0
	if result != nil {
		resultSize = result.Size()
	}

	// Group join metrics
	data := map[string]interface{}{
		"left.size":   leftSize,
		"right.size":  rightSize,
		"result.size": resultSize,
	}

	// Calculate amplification factor
	if leftSize+rightSize > 0 {
		data["amplification"] = float64(resultSize) / float64(leftSize+rightSize)
	}

	// Add symbols being joined
	if left != nil && right != nil {
		data["left.symbols"] = left.Symbols()
		data["right.symbols"] = right.Symbols()
	}

	// Add relation attributes for rendering
	if left != nil {
		leftAttrs := make([]string, len(left.Symbols()))
		for i, sym := range left.Symbols() {
			leftAttrs[i] = sym.String()
		}
		data["left.attrs"] = leftAttrs
	}
	if right != nil {
		rightAttrs := make([]string, len(right.Symbols()))
		for i, sym := range right.Symbols() {
			rightAttrs[i] = sym.String()
		}
		data["right.attrs"] = rightAttrs
	}
	if result != nil {
		resultAttrs := make([]string, len(result.Symbols()))
		for i, sym := range result.Symbols() {
			resultAttrs[i] = sym.String()
		}
		data["result.attrs"] = resultAttrs
	}

	c.collector.AddTiming(annotations.JoinHash, start, data)
	return result
}

func (c *AnnotatedContext) CollapseRelations(rels []Relation, fn func() []Relation) []Relation {
	start := time.Now()

	inputCount := len(rels)
	inputTuples := 0
	for _, rel := range rels {
		if rel != nil {
			inputTuples += rel.Size()
		}
	}

	result := fn()

	outputCount := len(result)
	outputTuples := 0
	for _, rel := range result {
		if rel != nil {
			outputTuples += rel.Size()
		}
	}

	if outputCount < inputCount || outputTuples < inputTuples {
		c.collector.AddTiming("collapse/success", start, map[string]interface{}{
			"relations.before": inputCount,
			"relations.after":  outputCount,
			"tuples.before":    inputTuples,
			"tuples.after":     outputTuples,
			"reduction.pct":    (1.0 - float64(outputTuples)/float64(inputTuples)) * 100,
		})
	}

	return result
}

func (c *AnnotatedContext) Collector() *annotations.Collector {
	return c.collector
}

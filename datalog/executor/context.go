package executor

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Context provides clean annotation points for query execution tracking.
type Context interface {
	// Query lifecycle
	QueryBegin(query string)
	QueryPlanCreated(plan string)
	QueryComplete(relationCount, tupleCount int, err error)

	// Phase operations
	ExecutePhase(name string, phase interface{}, fn func() (Relation, error)) (Relation, error)

	// Pattern matching
	MatchPatterns(patterns []query.Pattern, fn func() ([]Relation, error)) ([]Relation, error)
	MatchPattern(pattern query.Pattern, fn func() ([]datalog.Datom, error)) ([]datalog.Datom, error)
	MatchPatternWithBindings(pattern query.Pattern, inputBindings map[query.Symbol]int, fn func() ([]datalog.Datom, error)) ([]datalog.Datom, error)

	// Relation operations
	CombineRelations(oldRels, newRels []Relation, fn func() []Relation) []Relation
	JoinRelations(left, right Relation, fn func() Relation) Relation
	FilterRelation(rel Relation, predicate string, fn func() Relation) Relation
	CollapseRelations(rels []Relation, fn func() []Relation) []Relation

	// Expression evaluation
	EvaluateExpression(expr string, tupleCount int, fn func() error) error
	EvaluateExpressionRelation(rel Relation, expr string, fn func() Relation) Relation

	// Get underlying collector
	Collector() *annotations.Collector

	// Metadata operations for passing optimization hints
	SetMetadata(key string, value interface{})
	GetMetadata(key string) (interface{}, bool)
}

// BaseContext provides a no-op implementation with zero overhead.
type BaseContext struct {
	metadata map[string]interface{}
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

// BaseContext implementations - all are simple pass-throughs

func (c *BaseContext) QueryBegin(query string) {}

func (c *BaseContext) QueryPlanCreated(plan string) {}

func (c *BaseContext) QueryComplete(relationCount, tupleCount int, err error) {}

func (c *BaseContext) ExecutePhase(name string, phase interface{}, fn func() (Relation, error)) (Relation, error) {
	return fn()
}

func (c *BaseContext) MatchPatterns(patterns []query.Pattern, fn func() ([]Relation, error)) ([]Relation, error) {
	return fn()
}

func (c *BaseContext) MatchPattern(pattern query.Pattern, fn func() ([]datalog.Datom, error)) ([]datalog.Datom, error) {
	return fn()
}

func (c *BaseContext) MatchPatternWithBindings(pattern query.Pattern, inputBindings map[query.Symbol]int, fn func() ([]datalog.Datom, error)) ([]datalog.Datom, error) {
	return fn()
}

func (c *BaseContext) CombineRelations(oldRels, newRels []Relation, fn func() []Relation) []Relation {
	return fn()
}

func (c *BaseContext) JoinRelations(left, right Relation, fn func() Relation) Relation {
	return fn()
}

func (c *BaseContext) FilterRelation(rel Relation, predicate string, fn func() Relation) Relation {
	return fn()
}

func (c *BaseContext) CollapseRelations(rels []Relation, fn func() []Relation) []Relation {
	return fn()
}

func (c *BaseContext) EvaluateExpression(expr string, tupleCount int, fn func() error) error {
	return fn()
}

func (c *BaseContext) EvaluateExpressionRelation(rel Relation, expr string, fn func() Relation) Relation {
	return fn()
}

func (c *BaseContext) Collector() *annotations.Collector {
	return nil
}

func (c *BaseContext) SetMetadata(key string, value interface{}) {
	if c.metadata == nil {
		c.metadata = make(map[string]interface{})
	}
	c.metadata[key] = value
}

func (c *BaseContext) GetMetadata(key string) (interface{}, bool) {
	if c.metadata == nil {
		return nil, false
	}
	val, ok := c.metadata[key]
	return val, ok
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

func (c *AnnotatedContext) ExecutePhase(name string, phase interface{}, fn func() (Relation, error)) (Relation, error) {
	start := time.Now()

	// Log phase details with pattern information
	data := map[string]interface{}{
		"phase": name,
	}

	// Add phase-specific information if available
	if phaseInfo, ok := phase.(planner.Phase); ok {
		data["pattern.count"] = len(phaseInfo.Patterns)
	}

	c.collector.Add(annotations.Event{
		Name:  annotations.PhaseBegin,
		Start: start,
		Data:  data,
	})

	result, err := fn()

	// Complete event with results
	completeData := map[string]interface{}{
		"phase":       name,
		"tuple.count": 0,
		"success":     err == nil,
	}

	if result != nil {
		completeData["tuple.count"] = result.Size()
	}

	if err != nil {
		completeData["error"] = err.Error()
	}

	c.collector.AddTiming(annotations.PhaseComplete, start, completeData)
	return result, err
}

func (c *AnnotatedContext) MatchPatterns(patterns []query.Pattern, fn func() ([]Relation, error)) ([]Relation, error) {
	start := time.Now()

	c.collector.Add(annotations.Event{
		Name:  annotations.PatternsToRelationsBegin,
		Start: start,
		Data: map[string]interface{}{
			"pattern.count": len(patterns),
		},
	})

	matches, err := fn()

	totalTuples := 0
	for _, rel := range matches {
		if rel != nil {
			totalTuples += rel.Size()
		}
	}

	c.collector.AddTiming(annotations.PatternsToRelationsRealized, start, map[string]interface{}{
		"pattern.count": len(patterns),
		"match.count":   len(matches),
		"tuple.count":   totalTuples,
		"success":       err == nil,
	})

	return matches, err
}

func (c *AnnotatedContext) MatchPattern(pattern query.Pattern, fn func() ([]datalog.Datom, error)) ([]datalog.Datom, error) {
	start := time.Now()
	datoms, err := fn()

	// Extract binding information
	data := map[string]interface{}{
		"pattern":     pattern.String(),
		"match.count": len(datoms),
		"success":     err == nil,
	}

	// Add symbol binding information for data patterns
	if dp, ok := pattern.(*query.DataPattern); ok {
		var symbolOrder []string // Preserve order

		// Check each element for variables
		for _, elem := range dp.Elements {
			if v, ok := elem.(query.Variable); ok {
				symbolOrder = append(symbolOrder, string(v.Name))
			}
		}

		if len(symbolOrder) > 0 {
			data["symbol.order"] = symbolOrder
		}
	}

	c.collector.AddTiming(annotations.MatchesToRelations, start, data)

	return datoms, err
}

func (c *AnnotatedContext) MatchPatternWithBindings(pattern query.Pattern, inputBindings map[query.Symbol]int, fn func() ([]datalog.Datom, error)) ([]datalog.Datom, error) {
	start := time.Now()
	datoms, err := fn()

	// Extract binding information
	data := map[string]interface{}{
		"pattern":     pattern.String(),
		"match.count": len(datoms),
		"success":     err == nil,
	}

	// Add input bindings
	if len(inputBindings) > 0 {
		data["input.binds"] = inputBindings
	}

	// Add symbol binding information for data patterns
	if dp, ok := pattern.(*query.DataPattern); ok {
		var symbolOrder []string // Preserve order

		// Check each element for variables
		for _, elem := range dp.Elements {
			if v, ok := elem.(query.Variable); ok {
				symbolOrder = append(symbolOrder, string(v.Name))
			}
		}

		if len(symbolOrder) > 0 {
			data["symbol.order"] = symbolOrder
		}
	}

	c.collector.AddTiming(annotations.MatchesToRelations, start, data)

	return datoms, err
}

func (c *AnnotatedContext) CombineRelations(oldRels, newRels []Relation, fn func() []Relation) []Relation {
	// Begin event with counts
	beginData := map[string]interface{}{
		"relations/count-old": len(oldRels),
		"relations/count-new": len(newRels),
	}

	// Add sizes if available
	oldTuples := 0
	for _, rel := range oldRels {
		if rel != nil {
			oldTuples += rel.Size()
		}
	}
	newTuples := 0
	for _, rel := range newRels {
		if rel != nil {
			newTuples += rel.Size()
		}
	}

	beginData["tuples/count-old"] = oldTuples
	beginData["tuples/count-new"] = newTuples

	c.collector.Add(annotations.Event{
		Name:  annotations.CombineRelsBegin,
		Start: time.Now(),
		Data:  beginData,
	})

	start := time.Now()
	result := fn()

	// Track collapse if reduction occurred
	totalInput := len(oldRels) + len(newRels)
	if totalInput > 0 && len(result) < totalInput {
		resultTuples := 0
		for _, rel := range result {
			if rel != nil {
				resultTuples += rel.Size()
			}
		}

		collapseData := map[string]interface{}{
			"relations/before": totalInput,
			"relations/after":  len(result),
			"tuples/before":    oldTuples + newTuples,
			"tuples/after":     resultTuples,
			"reduction.pct":    float64(totalInput-len(result)) / float64(totalInput) * 100,
		}

		c.collector.AddTiming(annotations.CombineRelsCollapsed, start, collapseData)
	}

	return result
}

func (c *AnnotatedContext) JoinRelations(left, right Relation, fn func() Relation) Relation {
	start := time.Now()
	leftSize := -1  // Use -1 to indicate unknown size
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

	// Add columns being joined
	if left != nil && right != nil {
		data["left.columns"] = left.Columns()
		data["right.columns"] = right.Columns()
	}

	// Add relation attributes for rendering
	if left != nil {
		leftAttrs := make([]string, len(left.Columns()))
		for i, col := range left.Columns() {
			leftAttrs[i] = string(col)
		}
		data["left.attrs"] = leftAttrs
	}
	if right != nil {
		rightAttrs := make([]string, len(right.Columns()))
		for i, col := range right.Columns() {
			rightAttrs[i] = string(col)
		}
		data["right.attrs"] = rightAttrs
	}
	if result != nil {
		resultAttrs := make([]string, len(result.Columns()))
		for i, col := range result.Columns() {
			resultAttrs[i] = string(col)
		}
		data["result.attrs"] = resultAttrs
	}

	c.collector.AddTiming(annotations.JoinHash, start, data)
	return result
}

func (c *AnnotatedContext) FilterRelation(rel Relation, predicate string, fn func() Relation) Relation {
	start := time.Now()
	inputSize := 0
	if rel != nil {
		inputSize = rel.Size()
	}

	result := fn()

	outputSize := 0
	if result != nil {
		outputSize = result.Size()
	}

	c.collector.AddTiming("filter/predicate", start, map[string]interface{}{
		"predicate":   predicate,
		"input.size":  inputSize,
		"output.size": outputSize,
		"filtered":    inputSize - outputSize,
		"selectivity": float64(outputSize) / float64(inputSize),
	})

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

func (c *AnnotatedContext) EvaluateExpression(expr string, tupleCount int, fn func() error) error {
	start := time.Now()
	err := fn()

	c.collector.AddTiming("expression/evaluate", start, map[string]interface{}{
		"expression":  expr,
		"tuple.count": tupleCount,
		"success":     err == nil,
	})

	return err
}

func (c *AnnotatedContext) EvaluateExpressionRelation(rel Relation, expr string, fn func() Relation) Relation {
	start := time.Now()

	inputSize := 0
	if rel != nil {
		inputSize = rel.Size()
	}

	result := fn()

	resultSize := 0
	if result != nil {
		resultSize = result.Size()
	}

	c.collector.AddTiming("expression/evaluate", start, map[string]interface{}{
		"expression":  expr,
		"input.size":  inputSize,
		"result.size": resultSize,
	})

	return result
}

func (c *AnnotatedContext) Collector() *annotations.Collector {
	return c.collector
}

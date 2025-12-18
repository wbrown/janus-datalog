package executor

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// PullContext provides annotation points for Pull API operations.
// Uses the same pattern as executor.Context - interface with no-op and annotated implementations.
type PullContext interface {
	// Top-level pull operations
	PullBegin(entity datalog.Identity, specCount int, resolved bool)
	PullComplete(entity datalog.Identity, attrCount int, resolved bool, err error)

	// Entity-level operations (with depth tracking)
	EntityBegin(entity datalog.Identity, depth, specCount int)
	EntityComplete(entity datalog.Identity, depth, attrCount int)

	// Cycle detection
	CycleDetected(entity datalog.Identity, depth int)

	// Attribute lookups
	AttributeLookup(entity datalog.Identity, attr datalog.Keyword, found bool, via string, fn func())
	AllAttributes(entity datalog.Identity, fn func() int) int
	ManyValues(entity datalog.Identity, attr datalog.Keyword, fn func() int) int

	// Nested ref traversal
	NestedBegin(parentEntity datalog.Identity, attr datalog.Keyword, refEntity datalog.Identity, depth int, isMany bool)
	NestedComplete(parentEntity datalog.Identity, attr datalog.Keyword, refEntity datalog.Identity, depth, attrCount int, err error)
}

// BasePullContext provides a no-op implementation with zero overhead.
type BasePullContext struct{}

func (c *BasePullContext) PullBegin(entity datalog.Identity, specCount int, resolved bool) {}
func (c *BasePullContext) PullComplete(entity datalog.Identity, attrCount int, resolved bool, err error) {
}
func (c *BasePullContext) EntityBegin(entity datalog.Identity, depth, specCount int)  {}
func (c *BasePullContext) EntityComplete(entity datalog.Identity, depth, attrCount int) {}
func (c *BasePullContext) CycleDetected(entity datalog.Identity, depth int)            {}

func (c *BasePullContext) AttributeLookup(entity datalog.Identity, attr datalog.Keyword, found bool, via string, fn func()) {
	fn()
}

func (c *BasePullContext) AllAttributes(entity datalog.Identity, fn func() int) int {
	return fn()
}

func (c *BasePullContext) ManyValues(entity datalog.Identity, attr datalog.Keyword, fn func() int) int {
	return fn()
}

func (c *BasePullContext) NestedBegin(parentEntity datalog.Identity, attr datalog.Keyword, refEntity datalog.Identity, depth int, isMany bool) {
}
func (c *BasePullContext) NestedComplete(parentEntity datalog.Identity, attr datalog.Keyword, refEntity datalog.Identity, depth, attrCount int, err error) {
}

// AnnotatedPullContext wraps operations with timing and event emission.
type AnnotatedPullContext struct {
	handler   annotations.Handler
	pullStart time.Time
}

// NewPullContext creates an appropriate context based on whether annotations are needed.
func NewPullContext(handler annotations.Handler) PullContext {
	if handler == nil {
		return &BasePullContext{}
	}
	return &AnnotatedPullContext{handler: handler}
}

func (c *AnnotatedPullContext) PullBegin(entity datalog.Identity, specCount int, resolved bool) {
	c.pullStart = time.Now()
	c.handler(annotations.Event{
		Name:  annotations.PullBegin,
		Start: c.pullStart,
		Data: map[string]interface{}{
			"entity":     entity.String(),
			"spec_count": specCount,
			"resolved":   resolved,
		},
	})
}

func (c *AnnotatedPullContext) PullComplete(entity datalog.Identity, attrCount int, resolved bool, err error) {
	end := time.Now()
	c.handler(annotations.Event{
		Name:    annotations.PullComplete,
		Start:   c.pullStart,
		End:     end,
		Latency: end.Sub(c.pullStart),
		Data: map[string]interface{}{
			"entity":     entity.String(),
			"attr_count": attrCount,
			"resolved":   resolved,
			"success":    err == nil,
		},
	})
}

func (c *AnnotatedPullContext) EntityBegin(entity datalog.Identity, depth, specCount int) {
	c.handler(annotations.Event{
		Name:  annotations.PullEntityBegin,
		Start: time.Now(),
		Data: map[string]interface{}{
			"entity":     entity.String(),
			"depth":      depth,
			"spec_count": specCount,
		},
	})
}

func (c *AnnotatedPullContext) EntityComplete(entity datalog.Identity, depth, attrCount int) {
	c.handler(annotations.Event{
		Name: annotations.PullEntityComplete,
		Data: map[string]interface{}{
			"entity":     entity.String(),
			"depth":      depth,
			"attr_count": attrCount,
		},
	})
}

func (c *AnnotatedPullContext) CycleDetected(entity datalog.Identity, depth int) {
	c.handler(annotations.Event{
		Name: annotations.PullCycleDetected,
		Data: map[string]interface{}{
			"entity": entity.String(),
			"depth":  depth,
		},
	})
}

func (c *AnnotatedPullContext) AttributeLookup(entity datalog.Identity, attr datalog.Keyword, found bool, via string, fn func()) {
	start := time.Now()
	fn()
	end := time.Now()
	c.handler(annotations.Event{
		Name:    annotations.PullAttributeLookup,
		Start:   start,
		End:     end,
		Latency: end.Sub(start),
		Data: map[string]interface{}{
			"entity": entity.String(),
			"attr":   attr.String(),
			"found":  found,
			"via":    via,
		},
	})
}

func (c *AnnotatedPullContext) AllAttributes(entity datalog.Identity, fn func() int) int {
	start := time.Now()
	count := fn()
	end := time.Now()
	c.handler(annotations.Event{
		Name:    annotations.PullAllAttributes,
		Start:   start,
		End:     end,
		Latency: end.Sub(start),
		Data: map[string]interface{}{
			"entity":     entity.String(),
			"attr_count": count,
		},
	})
	return count
}

func (c *AnnotatedPullContext) ManyValues(entity datalog.Identity, attr datalog.Keyword, fn func() int) int {
	start := time.Now()
	count := fn()
	end := time.Now()
	c.handler(annotations.Event{
		Name:    annotations.PullManyValues,
		Start:   start,
		End:     end,
		Latency: end.Sub(start),
		Data: map[string]interface{}{
			"entity":      entity.String(),
			"attr":        attr.String(),
			"value_count": count,
		},
	})
	return count
}

func (c *AnnotatedPullContext) NestedBegin(parentEntity datalog.Identity, attr datalog.Keyword, refEntity datalog.Identity, depth int, isMany bool) {
	c.handler(annotations.Event{
		Name:  annotations.PullNestedBegin,
		Start: time.Now(),
		Data: map[string]interface{}{
			"parent_entity": parentEntity.String(),
			"attr":          attr.String(),
			"ref_entity":    refEntity.String(),
			"depth":         depth,
			"is_many":       isMany,
		},
	})
}

func (c *AnnotatedPullContext) NestedComplete(parentEntity datalog.Identity, attr datalog.Keyword, refEntity datalog.Identity, depth, attrCount int, err error) {
	c.handler(annotations.Event{
		Name: annotations.PullNestedComplete,
		Data: map[string]interface{}{
			"parent_entity": parentEntity.String(),
			"attr":          attr.String(),
			"ref_entity":    refEntity.String(),
			"depth":         depth,
			"attr_count":    attrCount,
			"success":       err == nil,
		},
	})
}

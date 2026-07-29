package reflect

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// ReflectContext provides annotation points for reflection operations.
// Uses the same pattern as executor.Context - interface with no-op and annotated implementations.
type ReflectContext interface {
	// Read operations
	ReadBegin(structName string, fieldCount, inputKeys int)
	ReadComplete(structName string, err error)

	// Write operations
	WriteBegin(entity, structName string, fieldCount int)
	WriteComplete(entity, structName string, fieldsWritten int, err error)

	// Update operations
	UpdateBegin(entity, structName string, fieldCount int, mode string)
	UpdateComplete(entity, structName string, fieldsProcessed int, mode string, err error)
}

// BaseReflectContext provides a no-op implementation with zero overhead.
type BaseReflectContext struct{}

func (c *BaseReflectContext) ReadBegin(structName string, fieldCount, inputKeys int) {}
func (c *BaseReflectContext) ReadComplete(structName string, err error)              {}

func (c *BaseReflectContext) WriteBegin(entity, structName string, fieldCount int)                  {}
func (c *BaseReflectContext) WriteComplete(entity, structName string, fieldsWritten int, err error) {}

func (c *BaseReflectContext) UpdateBegin(entity, structName string, fieldCount int, mode string) {}
func (c *BaseReflectContext) UpdateComplete(entity, structName string, fieldsProcessed int, mode string, err error) {
}

// AnnotatedReflectContext wraps operations with timing and event emission.
type AnnotatedReflectContext struct {
	handler annotations.Handler
	start   time.Time
}

// NewReflectContext creates an appropriate context based on whether annotations are needed.
func NewReflectContext(handler annotations.Handler) ReflectContext {
	if handler == nil {
		return &BaseReflectContext{}
	}
	return &AnnotatedReflectContext{handler: handler}
}

func (c *AnnotatedReflectContext) ReadBegin(structName string, fieldCount, inputKeys int) {
	c.start = time.Now()
	c.handler(annotations.Event{
		Name:  annotations.ReflectReadBegin,
		Start: c.start,
		Data: map[string]interface{}{
			"struct_type": structName,
			"field_count": fieldCount,
			"input_keys":  inputKeys,
		},
	})
}

func (c *AnnotatedReflectContext) ReadComplete(structName string, err error) {
	end := time.Now()
	c.handler(annotations.Event{
		Name:    annotations.ReflectReadComplete,
		Start:   c.start,
		End:     end,
		Latency: end.Sub(c.start),
		Data: map[string]interface{}{
			"struct_type":          structName,
			annotations.KeySuccess: err == nil,
		},
	})
}

func (c *AnnotatedReflectContext) WriteBegin(entity, structName string, fieldCount int) {
	c.start = time.Now()
	c.handler(annotations.Event{
		Name:  annotations.ReflectWriteBegin,
		Start: c.start,
		Data: map[string]interface{}{
			"entity":      entity,
			"struct_type": structName,
			"field_count": fieldCount,
		},
	})
}

func (c *AnnotatedReflectContext) WriteComplete(entity, structName string, fieldsWritten int, err error) {
	end := time.Now()
	c.handler(annotations.Event{
		Name:    annotations.ReflectWriteComplete,
		Start:   c.start,
		End:     end,
		Latency: end.Sub(c.start),
		Data: map[string]interface{}{
			"entity":               entity,
			"struct_type":          structName,
			"fields_written":       fieldsWritten,
			annotations.KeySuccess: err == nil,
		},
	})
}

func (c *AnnotatedReflectContext) UpdateBegin(entity, structName string, fieldCount int, mode string) {
	c.start = time.Now()
	c.handler(annotations.Event{
		Name:  annotations.ReflectUpdateBegin,
		Start: c.start,
		Data: map[string]interface{}{
			"entity":      entity,
			"struct_type": structName,
			"field_count": fieldCount,
			"mode":        mode,
		},
	})
}

func (c *AnnotatedReflectContext) UpdateComplete(entity, structName string, fieldsProcessed int, mode string, err error) {
	end := time.Now()
	c.handler(annotations.Event{
		Name:    annotations.ReflectUpdateComplete,
		Start:   c.start,
		End:     end,
		Latency: end.Sub(c.start),
		Data: map[string]interface{}{
			"entity":               entity,
			"struct_type":          structName,
			"fields_processed":     fieldsProcessed,
			"mode":                 mode,
			annotations.KeySuccess: err == nil,
		},
	})
}

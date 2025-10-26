package executor

// MinimalContext provides a no-op implementation for immediate compilation fix.
// TODO: Replace with full implementation from context.go
type MinimalContext struct{}

func NewMinimalContext() *MinimalContext {
	return &MinimalContext{}
}

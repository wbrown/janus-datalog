package query

import (
	"fmt"
	"strings"
	"sync"
)

// FunctionRegistry is the function-namespace authority: metadata (name,
// arity) for parse-time validation, and implementations for user-defined
// functions invoked at evaluation time. Registration happens at runtime
// (RegisterImplementation), so all access is mutex-guarded.
type FunctionRegistry struct {
	mu              sync.RWMutex
	functions       map[string]FunctionMetadata
	implementations map[string]func([]interface{}) (interface{}, error)
}

// FunctionMetadata describes a supported function
type FunctionMetadata struct {
	Name        string
	MinArgs     int
	MaxArgs     int // -1 for unlimited
	Description string
}

// Global registry - initialized at package load
var DefaultRegistry = NewFunctionRegistry()

func NewFunctionRegistry() *FunctionRegistry {
	r := &FunctionRegistry{
		functions:       make(map[string]FunctionMetadata),
		implementations: make(map[string]func([]interface{}) (interface{}, error)),
	}

	// String functions
	r.Register(FunctionMetadata{
		Name:        "str/starts-with?",
		MinArgs:     2,
		MaxArgs:     2,
		Description: "Check if string starts with prefix",
	})

	r.Register(FunctionMetadata{
		Name:        "str/ends-with?",
		MinArgs:     2,
		MaxArgs:     2,
		Description: "Check if string ends with suffix",
	})

	r.Register(FunctionMetadata{
		Name:        "str/contains?",
		MinArgs:     2,
		MaxArgs:     2,
		Description: "Check if string contains substring",
	})

	// Time extraction functions (when used as predicates, not expressions)
	r.Register(FunctionMetadata{
		Name:        "year",
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extract year from time value",
	})

	r.Register(FunctionMetadata{
		Name:        "month",
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extract month from time value",
	})

	r.Register(FunctionMetadata{
		Name:        "day",
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extract day from time value",
	})

	r.Register(FunctionMetadata{
		Name:        "hour",
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extract hour from time value",
	})

	r.Register(FunctionMetadata{
		Name:        "minute",
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extract minute from time value",
	})

	r.Register(FunctionMetadata{
		Name:        "second",
		MinArgs:     1,
		MaxArgs:     1,
		Description: "Extract second from time value",
	})

	// Date comparison functions
	r.Register(FunctionMetadata{
		Name:        "same-date?",
		MinArgs:     2,
		MaxArgs:     2,
		Description: "Check if two time values are on the same date",
	})

	return r
}

// Register adds a function to the registry
func (r *FunctionRegistry) Register(meta FunctionMetadata) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.functions[meta.Name] = meta
}

// RegisterImplementation registers a user-defined function under name: its
// metadata (variadic — arity is the implementation's concern) and its
// callable. Registered names evaluate in predicate position
// (FunctionPredicate) and expression position (CustomFunction).
func (r *FunctionRegistry) RegisterImplementation(name string, fn func([]interface{}) (interface{}, error)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.functions[name] = FunctionMetadata{
		Name:        name,
		MinArgs:     0,
		MaxArgs:     -1,
		Description: "user-defined function",
	}
	r.implementations[name] = fn
}

// Implementation returns the user-registered callable for name.
func (r *FunctionRegistry) Implementation(name string) (func([]interface{}) (interface{}, error), bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.implementations[name]
	return fn, ok
}

// IsRegistered checks if a function name is registered
func (r *FunctionRegistry) IsRegistered(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.functions[name]
	return ok
}

// Validate checks if a function call is valid
func (r *FunctionRegistry) Validate(name string, argCount int) error {
	r.mu.RLock()
	meta, ok := r.functions[name]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown function '%s' - supported functions: %s",
			name, r.ListFunctions())
	}

	if argCount < meta.MinArgs {
		return fmt.Errorf("function '%s' requires at least %d arguments, got %d",
			name, meta.MinArgs, argCount)
	}

	if meta.MaxArgs != -1 && argCount > meta.MaxArgs {
		return fmt.Errorf("function '%s' accepts at most %d arguments, got %d",
			name, meta.MaxArgs, argCount)
	}

	return nil
}

// ListFunctions returns a comma-separated list of registered functions
func (r *FunctionRegistry) ListFunctions() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.functions))
	for name := range r.functions {
		names = append(names, name)
	}
	return strings.Join(names, ", ")
}

// GetMetadata returns metadata for a function
func (r *FunctionRegistry) GetMetadata(name string) (FunctionMetadata, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	meta, ok := r.functions[name]
	return meta, ok
}

package query

import (
	"fmt"
	"strings"
)

// FunctionRegistry tracks which functions are supported for FunctionPredicates
// This allows us to fail at query planning time rather than runtime
type FunctionRegistry struct {
	functions map[string]FunctionMetadata
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
		functions: make(map[string]FunctionMetadata),
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
	r.functions[meta.Name] = meta
}

// IsRegistered checks if a function name is registered
func (r *FunctionRegistry) IsRegistered(name string) bool {
	_, ok := r.functions[name]
	return ok
}

// Validate checks if a function call is valid
func (r *FunctionRegistry) Validate(name string, argCount int) error {
	meta, ok := r.functions[name]
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
	names := make([]string, 0, len(r.functions))
	for name := range r.functions {
		names = append(names, name)
	}
	return strings.Join(names, ", ")
}

// GetMetadata returns metadata for a function
func (r *FunctionRegistry) GetMetadata(name string) (FunctionMetadata, bool) {
	meta, ok := r.functions[name]
	return meta, ok
}

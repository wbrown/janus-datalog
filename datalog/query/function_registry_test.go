package query

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestFunctionRegistry(t *testing.T) {
	r := NewFunctionRegistry()

	tests := []struct {
		name        string
		fn          string
		argCount    int
		shouldErr   bool
		errContains string
	}{
		{
			name:      "valid str/starts-with?",
			fn:        "str/starts-with?",
			argCount:  2,
			shouldErr: false,
		},
		{
			name:        "str/starts-with? too few args",
			fn:          "str/starts-with?",
			argCount:    1,
			shouldErr:   true,
			errContains: "at least 2 arguments",
		},
		{
			name:        "str/starts-with? too many args",
			fn:          "str/starts-with?",
			argCount:    3,
			shouldErr:   true,
			errContains: "at most 2 arguments",
		},
		{
			name:      "valid year extraction",
			fn:        "year",
			argCount:  1,
			shouldErr: false,
		},
		{
			name:        "unknown function",
			fn:          "foo/bar",
			argCount:    2,
			shouldErr:   true,
			errContains: "unknown function 'foo/bar'",
		},
		{
			name:        "unknown not= (if parsed wrong)",
			fn:          "not=",
			argCount:    2,
			shouldErr:   true,
			errContains: "unknown function 'not='",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := r.Validate(tt.fn, tt.argCount)
			if tt.shouldErr {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errContains, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestFunctionRegistryListing(t *testing.T) {
	r := NewFunctionRegistry()
	list := r.ListFunctions()

	// Should contain at least these functions
	requiredFuncs := []string{
		"str/starts-with?",
		"year",
		"month",
		"day",
	}

	for _, fn := range requiredFuncs {
		if !strings.Contains(list, fn) {
			t.Errorf("Function list should contain '%s', got: %s", fn, list)
		}
	}
}

func TestIsRegistered(t *testing.T) {
	r := NewFunctionRegistry()

	tests := []struct {
		fn       string
		expected bool
	}{
		{"str/starts-with?", true},
		{"year", true},
		{"not=", false}, // not= is NOT a function, it's a predicate operator
		{"foo/bar", false},
	}

	for _, tt := range tests {
		t.Run(tt.fn, func(t *testing.T) {
			got := r.IsRegistered(tt.fn)
			if got != tt.expected {
				t.Errorf("IsRegistered(%s) = %v, want %v", tt.fn, got, tt.expected)
			}
		})
	}
}

func TestGetMetadata(t *testing.T) {
	r := NewFunctionRegistry()

	meta, ok := r.GetMetadata("str/starts-with?")
	if !ok {
		t.Fatal("Expected str/starts-with? to be registered")
	}

	if meta.MinArgs != 2 {
		t.Errorf("Expected MinArgs=2, got %d", meta.MinArgs)
	}

	if meta.MaxArgs != 2 {
		t.Errorf("Expected MaxArgs=2, got %d", meta.MaxArgs)
	}

	_, ok = r.GetMetadata("nonexistent")
	if ok {
		t.Error("Expected nonexistent function to return false")
	}
}

// TestRegisterImplementationRoundTrip pins the registry's implementation
// side: a registered callable is retrievable, carries variadic metadata, and
// an unregistered name misses without error.
func TestRegisterImplementationRoundTrip(t *testing.T) {
	r := NewFunctionRegistry()

	if _, ok := r.Implementation("test/absent?"); ok {
		t.Fatal("Implementation returned a callable for an unregistered name")
	}

	r.RegisterImplementation("test/answer", func(args []interface{}) (interface{}, error) {
		return int64(42), nil
	})

	fn, ok := r.Implementation("test/answer")
	if !ok {
		t.Fatal("registered implementation not retrievable")
	}
	result, err := fn(nil)
	if err != nil {
		t.Fatalf("registered implementation errored: %v", err)
	}
	if result != int64(42) {
		t.Fatalf("registered implementation returned %v, want 42", result)
	}

	if !r.IsRegistered("test/answer") {
		t.Error("RegisterImplementation must register metadata too")
	}
	// Variadic metadata: any arity validates.
	if err := r.Validate("test/answer", 0); err != nil {
		t.Errorf("0-arg call rejected: %v", err)
	}
	if err := r.Validate("test/answer", 7); err != nil {
		t.Errorf("7-arg call rejected: %v", err)
	}
}

// TestFunctionPredicateEvalConsultsRegistry pins the predicate-position
// wiring: a registered function evaluates (bool results filter; non-bool is
// a loud contract error; unbound arguments error), and an unregistered name
// keeps the loud unknown-function error.
func TestFunctionPredicateEvalConsultsRegistry(t *testing.T) {
	DefaultRegistry.RegisterImplementation("test/big?", func(args []interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, nil
		}
		n, ok := args[0].(int64)
		return ok && n > 100, nil
	})
	DefaultRegistry.RegisterImplementation("test/not-bool", func(args []interface{}) (interface{}, error) {
		return "yes", nil
	})

	x := datalog.NewSymbol("?x")

	pred := FunctionPredicate{Fn: "test/big?", Args: []PatternElement{Variable{Name: x}}}
	passes, err := pred.Eval(map[Symbol]interface{}{x: int64(200)})
	if err != nil {
		t.Fatalf("registered predicate errored: %v", err)
	}
	if !passes {
		t.Error("test/big?(200) = false, want true")
	}
	passes, err = pred.Eval(map[Symbol]interface{}{x: int64(5)})
	if err != nil {
		t.Fatalf("registered predicate errored: %v", err)
	}
	if passes {
		t.Error("test/big?(5) = true, want false")
	}

	// Constants resolve without bindings.
	constPred := FunctionPredicate{Fn: "test/big?", Args: []PatternElement{Constant{Value: int64(500)}}}
	passes, err = constPred.Eval(map[Symbol]interface{}{})
	if err != nil {
		t.Fatalf("constant-arg predicate errored: %v", err)
	}
	if !passes {
		t.Error("test/big?(500) = false, want true")
	}

	// An unbound argument is a loud error, not a silent non-match.
	if _, err := pred.Eval(map[Symbol]interface{}{}); err == nil {
		t.Error("unbound argument evaluated without error")
	}

	// A non-bool result is a loud contract error.
	nb := FunctionPredicate{Fn: "test/not-bool", Args: nil}
	if _, err := nb.Eval(map[Symbol]interface{}{}); err == nil {
		t.Error("non-bool predicate result evaluated without error")
	} else if !strings.Contains(err.Error(), "bool") {
		t.Errorf("non-bool contract error must name the expectation, got: %v", err)
	}

	// Unregistered names keep the loud unknown-function error.
	unknown := FunctionPredicate{Fn: "test/never-registered?", Args: nil}
	if _, err := unknown.Eval(map[Symbol]interface{}{}); err == nil {
		t.Error("unregistered predicate function evaluated without error")
	} else if !strings.Contains(err.Error(), "unknown predicate function") {
		t.Errorf("unregistered name must keep the unknown-function error, got: %v", err)
	}
}

// TestCustomFunctionEval pins the expression-position wiring: a registered
// function computes a value (normalized into the engine's canonical types),
// unresolved arguments error, and an unregistered name errors loudly.
func TestCustomFunctionEval(t *testing.T) {
	DefaultRegistry.RegisterImplementation("test/double", func(args []interface{}) (interface{}, error) {
		n, ok := args[0].(int64)
		if !ok {
			return nil, nil
		}
		// Returns Go int deliberately: Eval must normalize to int64.
		return int(n * 2), nil
	})

	x := datalog.NewSymbol("?x")
	fn := CustomFunction{Fn: "test/double", Args: []Term{VariableTerm{Symbol: x}}}

	result, err := fn.Eval(map[Symbol]interface{}{x: int64(21)})
	if err != nil {
		t.Fatalf("registered function errored: %v", err)
	}
	if result != int64(42) {
		t.Fatalf("test/double(21) = %v (%T), want int64 42", result, result)
	}

	if _, err := fn.Eval(map[Symbol]interface{}{}); err == nil {
		t.Error("unresolved argument evaluated without error")
	}

	unknown := CustomFunction{Fn: "test/never-registered", Args: nil}
	if _, err := unknown.Eval(map[Symbol]interface{}{}); err == nil {
		t.Error("unregistered expression function evaluated without error")
	}

	if got := fn.RequiredSymbols(); len(got) != 1 || got[0] != x {
		t.Errorf("RequiredSymbols = %v, want [?x]", got)
	}
}

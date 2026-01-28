package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestAndFunction(t *testing.T) {
	tests := []struct {
		name     string
		terms    []Symbol
		bindings map[Symbol]interface{}
		expected bool
		hasError bool
	}{
		{
			name:     "All true",
			terms:    []Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?a"): true, datalog.NewSymbol("?b"): true, datalog.NewSymbol("?c"): true},
			expected: true,
		},
		{
			name:     "One false",
			terms:    []Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?a"): true, datalog.NewSymbol("?b"): false, datalog.NewSymbol("?c"): true},
			expected: false,
		},
		{
			name:     "All false",
			terms:    []Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?a"): false, datalog.NewSymbol("?b"): false},
			expected: false,
		},
		{
			name:     "Empty terms",
			terms:    []Symbol{},
			bindings: map[Symbol]interface{}{},
			expected: true, // Vacuously true
		},
		{
			name:     "Single true",
			terms:    []Symbol{datalog.NewSymbol("?a")},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?a"): true},
			expected: true,
		},
		{
			name:     "Single false",
			terms:    []Symbol{datalog.NewSymbol("?a")},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?a"): false},
			expected: false,
		},
		{
			name:     "Non-boolean value",
			terms:    []Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?a"): true, datalog.NewSymbol("?b"): 42},
			expected: false, // Non-boolean treated as false
		},
		{
			name:     "Missing binding",
			terms:    []Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?a"): true},
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			andFunc := AndFunction{Terms: tt.terms}

			result, err := andFunc.Eval(tt.bindings)

			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestAndFunctionString(t *testing.T) {
	tests := []struct {
		name     string
		terms    []Symbol
		expected string
	}{
		{
			name:     "Multiple terms",
			terms:    []Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")},
			expected: "(and ?a ?b ?c)",
		},
		{
			name:     "Single term",
			terms:    []Symbol{datalog.NewSymbol("?filter")},
			expected: "?filter",
		},
		{
			name:     "Empty",
			terms:    []Symbol{},
			expected: "(and)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			andFunc := AndFunction{Terms: tt.terms}
			assert.Equal(t, tt.expected, andFunc.String())
		})
	}
}

func TestAndFunctionRequiredSymbols(t *testing.T) {
	andFunc := AndFunction{
		Terms: []Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")},
	}

	symbols := andFunc.RequiredSymbols()
	assert.Equal(t, 3, len(symbols))
	assert.Contains(t, symbols, datalog.NewSymbol("?a"))
	assert.Contains(t, symbols, datalog.NewSymbol("?b"))
	assert.Contains(t, symbols, datalog.NewSymbol("?c"))
}

func TestAndFunctionReturnType(t *testing.T) {
	andFunc := AndFunction{Terms: []Symbol{datalog.NewSymbol("?a")}}
	assert.Equal(t, "boolean", andFunc.ReturnType())
}

package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
			terms:    []Symbol{"?a", "?b", "?c"},
			bindings: map[Symbol]interface{}{"?a": true, "?b": true, "?c": true},
			expected: true,
		},
		{
			name:     "One false",
			terms:    []Symbol{"?a", "?b", "?c"},
			bindings: map[Symbol]interface{}{"?a": true, "?b": false, "?c": true},
			expected: false,
		},
		{
			name:     "All false",
			terms:    []Symbol{"?a", "?b"},
			bindings: map[Symbol]interface{}{"?a": false, "?b": false},
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
			terms:    []Symbol{"?a"},
			bindings: map[Symbol]interface{}{"?a": true},
			expected: true,
		},
		{
			name:     "Single false",
			terms:    []Symbol{"?a"},
			bindings: map[Symbol]interface{}{"?a": false},
			expected: false,
		},
		{
			name:     "Non-boolean value",
			terms:    []Symbol{"?a", "?b"},
			bindings: map[Symbol]interface{}{"?a": true, "?b": 42},
			expected: false, // Non-boolean treated as false
		},
		{
			name:     "Missing binding",
			terms:    []Symbol{"?a", "?b"},
			bindings: map[Symbol]interface{}{"?a": true},
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
			terms:    []Symbol{"?a", "?b", "?c"},
			expected: "(and ?a ?b ?c)",
		},
		{
			name:     "Single term",
			terms:    []Symbol{"?filter"},
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
		Terms: []Symbol{"?a", "?b", "?c"},
	}

	symbols := andFunc.RequiredSymbols()
	assert.Equal(t, 3, len(symbols))
	assert.Contains(t, symbols, Symbol("?a"))
	assert.Contains(t, symbols, Symbol("?b"))
	assert.Contains(t, symbols, Symbol("?c"))
}

func TestAndFunctionReturnType(t *testing.T) {
	andFunc := AndFunction{Terms: []Symbol{"?a"}}
	assert.Equal(t, "boolean", andFunc.ReturnType())
}

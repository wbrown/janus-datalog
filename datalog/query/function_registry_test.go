package query

import (
	"strings"
	"testing"
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

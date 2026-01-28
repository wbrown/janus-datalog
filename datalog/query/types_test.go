package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataPatternString(t *testing.T) {
	tests := []struct {
		name    string
		pattern DataPattern
		want    string
	}{
		{
			name: "unqualified",
			pattern: DataPattern{
				Elements: []PatternElement{
					Variable{Name: "?e"},
					Constant{Value: ":attr"},
					Variable{Name: "?v"},
				},
			},
			want: "[?e :attr ?v]",
		},
		{
			name: "source-qualified",
			pattern: DataPattern{
				Source: Symbol("$users"),
				Elements: []PatternElement{
					Variable{Name: "?e"},
					Constant{Value: ":attr"},
					Variable{Name: "?v"},
				},
			},
			want: "[$users ?e :attr ?v]",
		},
		{
			name: "empty source same as unqualified",
			pattern: DataPattern{
				Source: "",
				Elements: []PatternElement{
					Variable{Name: "?e"},
					Constant{Value: ":a"},
					Variable{Name: "?v"},
				},
			},
			want: "[?e :a ?v]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.pattern.String())
		})
	}
}

func TestDatabaseInputString(t *testing.T) {
	tests := []struct {
		name  string
		input DatabaseInput
		want  string
	}{
		{"default", DatabaseInput{Name: Symbol("$")}, "$"},
		{"named", DatabaseInput{Name: Symbol("$users")}, "$users"},
		{"named with underscore", DatabaseInput{Name: Symbol("$foo_bar")}, "$foo_bar"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.input.String())
		})
	}
}

package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
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
					Variable{Name: datalog.NewSymbol("?e")},
					Constant{Value: datalog.NewKeyword(":attr")},
					Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			want: "[?e :attr ?v]",
		},
		{
			name: "source-qualified",
			pattern: DataPattern{
				Source: datalog.NewSymbol("$users"),
				Elements: []PatternElement{
					Variable{Name: datalog.NewSymbol("?e")},
					Constant{Value: datalog.NewKeyword(":attr")},
					Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			want: "[$users ?e :attr ?v]",
		},
		{
			name: "empty source same as unqualified",
			pattern: DataPattern{
				Source: nil,
				Elements: []PatternElement{
					Variable{Name: datalog.NewSymbol("?e")},
					Constant{Value: datalog.NewKeyword(":a")},
					Variable{Name: datalog.NewSymbol("?v")},
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
		{"default", DatabaseInput{Name: datalog.NewSymbol("$")}, "$"},
		{"named", DatabaseInput{Name: datalog.NewSymbol("$users")}, "$users"},
		{"named with underscore", DatabaseInput{Name: datalog.NewSymbol("$foo_bar")}, "$foo_bar"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.input.String())
		})
	}
}

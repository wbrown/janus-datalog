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

// A query aggregates when its find holds an aggregate, beside variables or
// alone; a find of variables alone, and an empty find, do not.
func TestQueryAggregatesReportsAnAggregateInTheFind(t *testing.T) {
	e, n := datalog.NewSymbol("?e"), datalog.NewSymbol("?n")
	assert.True(t, (&Query{Find: []FindElement{FindVariable{Symbol: e}, FindAggregate{Function: datalog.SymCount, Arg: n}}}).Aggregates(), "a count beside a variable")
	assert.True(t, (&Query{Find: []FindElement{FindAggregate{Function: datalog.SymCount, Arg: n}}}).Aggregates(), "a count alone")
	assert.False(t, (&Query{Find: []FindElement{FindVariable{Symbol: e}, FindVariable{Symbol: n}}}).Aggregates(), "variables alone")
	assert.False(t, (&Query{}).Aggregates(), "an empty find")
}

// A query derived over another where carries the find, the inputs, the
// ordering, the limit and the scalar flag of the query it is derived from,
// and the new where, and leaves that query unchanged.
func TestQueryWithWhereDerivesAQueryOverTheNewWhere(t *testing.T) {
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	limit := 3
	where := []Clause{&DataPattern{Elements: []PatternElement{Variable{Name: e}, Constant{Value: datalog.NewKeyword(":a")}, Variable{Name: v}}}}
	q := &Query{
		Find:         []FindElement{FindVariable{Symbol: e}},
		In:           []InputSpec{DatabaseInput{Name: datalog.SymDollar}, ScalarInput{Symbol: v}},
		Where:        where,
		OrderBy:      []OrderByClause{{Variable: e}},
		Limit:        &limit,
		ScalarReturn: true,
	}
	derived := q.WithWhere(nil)
	assert.Equal(t, q.Find, derived.Find)
	assert.Equal(t, q.In, derived.In)
	assert.Nil(t, derived.Where, "the derived query is over the new where")
	assert.Equal(t, q.OrderBy, derived.OrderBy)
	assert.Equal(t, q.Limit, derived.Limit)
	assert.Equal(t, q.ScalarReturn, derived.ScalarReturn)
	assert.Equal(t, where, q.Where, "the query derived from is unchanged")
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

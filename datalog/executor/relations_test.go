package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestRelationsProject(t *testing.T) {
	// Create test relations
	r1 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
		[]Tuple{{1, 2}, {3, 4}},
	)
	r2 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")},
		[]Tuple{{1, 2, 3}, {4, 5, 6}},
	)
	r3 := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
		[]Tuple{{7, 8}},
	)

	relations := Relations{r1, r2, r3}

	tests := []struct {
		name     string
		symbols  []query.Symbol
		expected Relation
	}{
		{
			name:     "exact match prefers fewer symbols",
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			expected: r1, // r1 has exactly these symbols, r2 has extra
		},
		{
			name:     "requires all symbols",
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?z")},
			expected: r2, // only r2 has both ?x and ?z
		},
		{
			name:     "no match returns nil",
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?b")},
			expected: nil, // no single relation has both
		},
		{
			name:     "single symbol matches smallest relation",
			symbols:  []query.Symbol{datalog.NewSymbol("?x")},
			expected: r1, // both r1 and r2 have ?x, but r1 has fewer symbols
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := relations.Project(tt.symbols...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRelationsFindBestForPattern(t *testing.T) {
	// Create test relations
	rEntity := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e")},
		[]Tuple{{1}, {2}, {3}}, // 3 tuples
	)
	rAttribute := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?a")},
		[]Tuple{{1}, {2}}, // 2 tuples
	)
	rValue := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?v")},
		[]Tuple{{1}, {2}, {3}, {4}}, // 4 tuples
	)
	rEntityValue := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")},
		[]Tuple{{1, 10}, {2, 20}}, // 2 tuples
	)

	tests := []struct {
		name      string
		pattern   *query.DataPattern
		relations Relations
		expected  Relation
	}{
		{
			name: "prefers E binding",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: ":foo/bar"},
					query.Constant{Value: 123},
				},
			},
			relations: Relations{rEntity, rAttribute, rValue},
			expected:  rEntity, // E position is most selective
		},
		{
			name: "prefers A binding over V",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: 123},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			relations: Relations{rAttribute, rValue},
			expected:  rAttribute, // A position more selective than V
		},
		{
			name: "prefers smaller relation on tie",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: ":foo/bar"},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			relations: Relations{rEntity, rEntityValue},
			expected:  rEntityValue, // Binds both E and V, and smaller
		},
		{
			name: "returns nil for no matches",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?x")},
					query.Variable{Name: datalog.NewSymbol("?y")},
					query.Variable{Name: datalog.NewSymbol("?z")},
				},
			},
			relations: Relations{rEntity, rAttribute},
			expected:  nil, // No relation has ?x, ?y, or ?z
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.relations.FindBestForPattern(tt.pattern)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRelationsFindRelationsForSymbols(t *testing.T) {
	r1 := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}, nil)
	r2 := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}, nil)
	r3 := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")}, nil)

	relations := Relations{r1, r2, r3}

	tests := []struct {
		name     string
		symbols  []query.Symbol
		expected int // expected count
	}{
		{
			name:     "finds relations with any symbol",
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?z")},
			expected: 2, // r1 has ?x, r2 has ?z
		},
		{
			name:     "finds overlapping relations",
			symbols:  []query.Symbol{datalog.NewSymbol("?y")},
			expected: 2, // both r1 and r2 have ?y
		},
		{
			name:     "returns empty for no matches",
			symbols:  []query.Symbol{datalog.NewSymbol("?missing")},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := relations.FindRelationsForSymbols(tt.symbols...)
			assert.Equal(t, tt.expected, len(result))
		})
	}
}

func TestRelationsCollapse(t *testing.T) {
	t.Run("joins relations with shared symbols", func(t *testing.T) {
		// R1: ?person -> ?dept
		r1 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?dept")},
			[]Tuple{
				{"Alice", "Engineering"},
				{"Bob", "Sales"},
			},
		)

		// R2: ?dept -> ?building
		r2 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?dept"), datalog.NewSymbol("?building")},
			[]Tuple{
				{"Engineering", "A"},
				{"Sales", "B"},
				{"Marketing", "C"},
			},
		)

		// R3: ?building -> ?floor
		r3 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?building"), datalog.NewSymbol("?floor")},
			[]Tuple{
				{"A", 1},
				{"B", 2},
				{"C", 3},
			},
		)

		relations := Relations{r1, r2, r3}
		groups := relations.Collapse(nil)

		// Should have exactly one group since all relations share symbols
		assert.Equal(t, 1, len(groups))

		result := groups[0]
		assert.Equal(t, 2, result.Size()) // Alice and Bob's full paths

		// Check symbols
		symbols := result.Symbols()
		assert.Equal(t, 4, len(symbols)) // ?person, ?dept, ?building, ?floor
	})

	t.Run("returns multiple groups for disjoint relations", func(t *testing.T) {
		// R1 and R3 share ?y, but R2 is disjoint
		r1 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			[]Tuple{{1, 2}, {3, 4}},
		)

		r2 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
			[]Tuple{{"foo", "bar"}, {"baz", "qux"}},
		)

		r3 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")},
			[]Tuple{{2, 5}, {4, 6}},
		)

		relations := Relations{r1, r2, r3}
		groups := relations.Collapse(nil)

		// Should have 2 groups: one with r1+r3 joined, one with r2 alone
		assert.Equal(t, 2, len(groups))

		// Check the groups
		var joinedGroup, disjointGroup Relation
		for _, g := range groups {
			symbols := g.Symbols()
			if len(symbols) == 3 { // ?x, ?y, ?z
				joinedGroup = g
			} else if len(symbols) == 2 { // ?a, ?b
				disjointGroup = g
			}
		}

		assert.NotNil(t, joinedGroup)
		assert.NotNil(t, disjointGroup)
		assert.Equal(t, 2, joinedGroup.Size())
		assert.Equal(t, 2, disjointGroup.Size())
	})

	t.Run("returns empty for non-matching join", func(t *testing.T) {
		// Relations share symbols but values don't match
		r1 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			[]Tuple{{1, 2}},
		)

		r2 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?y"), datalog.NewSymbol("?z")},
			[]Tuple{{3, 4}}, // ?y=3 doesn't match ?y=2
		)

		relations := Relations{r1, r2}
		groups := relations.Collapse(nil)

		// Should have one group with empty result
		assert.Equal(t, 1, len(groups))
		assert.Equal(t, 0, groups[0].Materialize().Size())
	})

	t.Run("handles empty relations", func(t *testing.T) {
		relations := Relations{}
		groups := relations.Collapse(nil)
		assert.Equal(t, 0, len(groups))
	})

	t.Run("handles single relation", func(t *testing.T) {
		r1 := NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?x")},
			[]Tuple{{1}, {2}},
		)

		relations := Relations{r1}
		groups := relations.Collapse(nil)

		assert.Equal(t, 1, len(groups))
		assert.Equal(t, r1, groups[0])
	})
}

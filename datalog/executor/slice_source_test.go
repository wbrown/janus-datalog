package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func kw(s string) datalog.Keyword {
	return datalog.NewKeyword(s)
}

// countRelation iterates a Relation and counts tuples.
// Needed because streaming/lazy relations return Size() == -1.
func countRelation(r Relation) int {
	count := 0
	it := r.Iterator()
	for it.Next() {
		_ = it.Tuple()
		count++
	}
	it.Close()
	return count
}

func TestSliceSource_Match(t *testing.T) {
	type Rule struct {
		Key       string
		DependsOn []string
	}

	rules := []Rule{
		{Key: "region-lore", DependsOn: []string{"world-lore", "region"}},
		{Key: "character-eval", DependsOn: []string{"character"}},
	}

	source := NewSliceSource(rules, AttributeSchema[Rule]{
		kw(":rule/key"):        func(r Rule) any { return r.Key },
		kw(":rule/depends-on"): func(r Rule) any { return r.DependsOn },
	})

	tests := []struct {
		name     string
		pattern  *query.DataPattern
		wantSize int
	}{
		{
			name: "all keys",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?r"},
					query.Constant{Value: kw(":rule/key")},
					query.Variable{Name: "?key"},
				},
			},
			wantSize: 2,
		},
		{
			name: "specific key",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?r"},
					query.Constant{Value: kw(":rule/key")},
					query.Constant{Value: "region-lore"},
				},
			},
			wantSize: 1,
		},
		{
			name: "all dependencies",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?r"},
					query.Constant{Value: kw(":rule/depends-on")},
					query.Variable{Name: "?dep"},
				},
			},
			wantSize: 3, // world-lore, region, character
		},
		{
			name: "specific dependency",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?r"},
					query.Constant{Value: kw(":rule/depends-on")},
					query.Constant{Value: "character"},
				},
			},
			wantSize: 1,
		},
		{
			name: "all attributes for entity",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: datalog.NewIdentity("slice:0")},
					query.Variable{Name: "?a"},
					query.Variable{Name: "?v"},
				},
			},
			wantSize: 3, // key + 2 depends-on values
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := source.Match(tt.pattern, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if got := countRelation(result); got != tt.wantSize {
				t.Errorf("expected %d results, got %d", tt.wantSize, got)
			}
		})
	}
}

func TestSliceSource_EmptySlice(t *testing.T) {
	type Item struct{ Name string }

	source := NewSliceSource([]Item{}, AttributeSchema[Item]{
		kw(":item/name"): func(i Item) any { return i.Name },
	})

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: kw(":item/name")},
			query.Variable{Name: "?name"},
		},
	}

	result, err := source.Match(pattern, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := countRelation(result); got != 0 {
		t.Errorf("expected 0 results for empty slice, got %d", got)
	}
}

func TestSliceSourceImplementsPatternMatcher(t *testing.T) {
	type Item struct{ Name string }

	source := NewSliceSource([]Item{{Name: "test"}}, AttributeSchema[Item]{
		kw(":item/name"): func(i Item) any { return i.Name },
	})

	var pm PatternMatcher = source
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: kw(":item/name")},
			query.Variable{Name: "?name"},
		},
	}

	result, err := pm.Match(pattern, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := countRelation(result); got != 1 {
		t.Errorf("expected 1 result, got %d", got)
	}
}

package executor

import (
	"errors"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

// mockPatternMatcher is a test double for PatternMatcher
type mockPatternMatcher struct {
	matchFunc func(pattern *query.DataPattern, bindings Relations) (Relation, error)
	wasCalled bool
}

func (m *mockPatternMatcher) Match(q *query.Query, bindings Relations) (Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	m.wasCalled = true
	if m.matchFunc != nil {
		return m.matchFunc(pattern, bindings)
	}
	return NewMaterializedRelation(pattern.Symbols(), nil), nil
}

var _ PatternMatcher = (*mockPatternMatcher)(nil)

func TestSourceRouterRoutes(t *testing.T) {
	mockUsers := &mockPatternMatcher{}
	mockPerms := &mockPatternMatcher{}

	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.NewSymbol("$users"): mockUsers,
		datalog.NewSymbol("$perms"): mockPerms,
	})

	pattern := &query.DataPattern{
		Source: datalog.NewSymbol("$users"),
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err := router.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockUsers.wasCalled {
		t.Error("expected $users source to be called")
	}
	if mockPerms.wasCalled {
		t.Error("expected $perms source NOT to be called")
	}

	// Reset and route to $perms
	mockUsers.wasCalled = false
	pattern = &query.DataPattern{
		Source: datalog.NewSymbol("$perms"),
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err = router.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockUsers.wasCalled {
		t.Error("expected $users source NOT to be called")
	}
	if !mockPerms.wasCalled {
		t.Error("expected $perms source to be called")
	}
}

func TestSourceRouterDefaultSource(t *testing.T) {
	mockDefault := &mockPatternMatcher{}

	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.NewSymbol("$"): mockDefault,
	})

	// Empty source should route to "$"
	pattern := &query.DataPattern{
		Source: nil,
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err := router.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockDefault.wasCalled {
		t.Error("expected default source to be called for empty source")
	}
}

func TestSourceRouterUnknownSource(t *testing.T) {
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.NewSymbol("$"): &mockPatternMatcher{},
	})

	pattern := &query.DataPattern{
		Source: datalog.NewSymbol("$nonexistent"),
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err := router.Match(query.PatternQuery(pattern), nil)
	if err == nil {
		t.Fatal("expected error for unknown source, got nil")
	}
}

func TestSourceRouterImplementsPatternMatcher(t *testing.T) {
	// Verify SourceRouter can be used as a PatternMatcher
	mockDefault := &mockPatternMatcher{}
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.NewSymbol("$"): mockDefault,
	})

	var pm PatternMatcher = router
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: ":attr"},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	_, err := pm.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockDefault.wasCalled {
		t.Error("expected default source to be called via PatternMatcher interface")
	}
}

// vocabularyMatcher is a PatternMatcher double that also answers the
// attribute-vocabulary question, recording the query it was asked about and
// answering with a fixed error.
type vocabularyMatcher struct {
	mockPatternMatcher
	asked  *query.Query
	answer error
}

func (m *vocabularyMatcher) UnknownAttributes(q *query.Query) error {
	m.asked = q
	return m.answer
}

var _ AttributeVocabulary = (*vocabularyMatcher)(nil)

// TestSourceRouterUnknownAttributesDelegatesToDefault pins that the router
// answers the attribute-vocabulary question from its default source alone: a
// default source with the capability is asked about the very query and its
// answer is returned as is, a default source without it answers nil, and a
// named source is never consulted.
func TestSourceRouterUnknownAttributesDelegatesToDefault(t *testing.T) {
	q := query.PatternQuery(&query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":t/naem")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	})
	refusal := errors.New("unknown attribute :t/naem")

	withVocabulary := &vocabularyMatcher{answer: refusal}
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: withVocabulary,
	})
	if err := router.UnknownAttributes(q); err != refusal {
		t.Errorf("router answered %v, want the default source's own answer %v", err, refusal)
	}
	if withVocabulary.asked != q {
		t.Error("router must ask the default source about the query it was given")
	}

	router = NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: &mockPatternMatcher{},
	})
	if err := router.UnknownAttributes(q); err != nil {
		t.Errorf("a default source without a vocabulary refuses nothing, got %v", err)
	}

	named := &vocabularyMatcher{answer: refusal}
	router = NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.NewSymbol("$cache"): named,
	})
	if err := router.UnknownAttributes(q); err != nil {
		t.Errorf("a router with no default source refuses nothing, got %v", err)
	}
	if named.asked != nil {
		t.Error("a named source carries no definitions to refuse against and must not be asked")
	}
}

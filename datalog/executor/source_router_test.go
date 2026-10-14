package executor

import (
	"errors"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/annotations"
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
// attribute-vocabulary question: whether it refuses, and, for a non-empty
// roster, a fixed error, recording the source and the roster it was asked
// about. An empty roster names nothing to refuse and is answered nil, as the
// storage matcher answers it.
type vocabularyMatcher struct {
	mockPatternMatcher
	refuses     bool
	askedSource query.Symbol
	asked       []datalog.Keyword
	answer      error
}

func (m *vocabularyMatcher) RefusesUnknownAttributes(source query.Symbol) bool {
	return m.refuses
}

func (m *vocabularyMatcher) UnknownAttributes(source query.Symbol, attrs []datalog.Keyword) error {
	m.askedSource = source
	m.asked = attrs
	if len(attrs) == 0 {
		return nil
	}
	return m.answer
}

var _ AttributeVocabulary = (*vocabularyMatcher)(nil)

// TestSourceRouterRoutesTheVocabularyQuestionToTheSourceNamed pins that the
// router answers the attribute-vocabulary question from the source the
// question names, which answers for itself under its own name, $: a source
// with the capability, the default or a named one, is asked about the very
// roster and its answer returned as is; a source without the capability, and
// a source the router does not hold, refuse nothing and are not asked.
func TestSourceRouterRoutesTheVocabularyQuestionToTheSourceNamed(t *testing.T) {
	roster := []datalog.Keyword{datalog.NewKeyword(":t/naem")}
	refusal := errors.New("unknown attribute :t/naem")
	cache := datalog.NewSymbol("$cache")

	defaultSource := &vocabularyMatcher{refuses: true, answer: refusal}
	named := &vocabularyMatcher{refuses: true, answer: refusal}
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: defaultSource,
		cache:             named,
	})
	for _, source := range []struct {
		name    query.Symbol
		matcher *vocabularyMatcher
	}{{datalog.SymDollar, defaultSource}, {cache, named}} {
		if !router.RefusesUnknownAttributes(source.name) {
			t.Errorf("the router reports %s as refusing nothing, want its source's own answer", source.name)
		}
		if err := router.UnknownAttributes(source.name, roster); err != refusal {
			t.Errorf("router answered %v for %s, want the source's own answer %v", err, source.name, refusal)
		}
		if source.matcher.askedSource != datalog.SymDollar {
			t.Errorf("%s was asked as %s, want asked as itself, $", source.name, source.matcher.askedSource)
		}
		if len(source.matcher.asked) != 1 || source.matcher.asked[0] != roster[0] {
			t.Errorf("router asked %s about %v, want the roster it was given %v", source.name, source.matcher.asked, roster)
		}
	}

	router = NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar: &mockPatternMatcher{},
	})
	if router.RefusesUnknownAttributes(datalog.SymDollar) {
		t.Error("a source without a vocabulary refuses nothing")
	}
	if err := router.UnknownAttributes(datalog.SymDollar, roster); err != nil {
		t.Errorf("a source without a vocabulary refuses nothing, got %v", err)
	}
	if router.RefusesUnknownAttributes(cache) {
		t.Error("a source the router does not hold refuses nothing")
	}
	if err := router.UnknownAttributes(cache, roster); err != nil {
		t.Errorf("a source the router does not hold refuses nothing, got %v", err)
	}
}

// TestExecuteWithRelationsChecksNamedSourceAttributes pins that the executor
// asks a named source about the attributes the query names on it, and the
// source's refusal is the query's.
func TestExecuteWithRelationsChecksNamedSourceAttributes(t *testing.T) {
	refusal := errors.New("unknown attribute :t/naem")
	named := &vocabularyMatcher{refuses: true, answer: refusal}
	router := NewSourceRouter(map[query.Symbol]PatternMatcher{
		datalog.SymDollar:           &mockPatternMatcher{},
		datalog.NewSymbol("$cache"): named,
	})
	exec := NewExecutor(router, nil)
	q := mustParse(t, `[:find ?v :in $ $cache :where [$cache ?e :t/naem ?v]]`)
	_, err := exec.ExecuteWithRelations(NewContext(), q, nil)
	if err != refusal {
		t.Errorf("executor answered %v, want the named source's refusal of the attribute the query names on it", err)
	}
	if len(named.asked) != 1 || named.asked[0] != datalog.NewKeyword(":t/naem") {
		t.Errorf("the named source was asked about %v, want the attributes the query names on it", named.asked)
	}
}

// TestExecuteWithRelationsAsksNothingOfASourceThatDoesNotRefuse pins that a
// source that does not refuse is asked about no roster, so none is collected
// for it, and the query runs.
func TestExecuteWithRelationsAsksNothingOfASourceThatDoesNotRefuse(t *testing.T) {
	silent := &vocabularyMatcher{answer: errors.New("a source that does not refuse is never asked")}
	exec := NewExecutor(silent, nil)
	q := mustParse(t, `[:find ?v :where [?e :t/naem ?v]]`)
	if _, err := exec.ExecuteWithRelations(NewContext(), q, nil); err != nil {
		t.Fatalf("a query on a source that does not refuse runs: %v", err)
	}
	if silent.asked != nil {
		t.Errorf("a source that does not refuse was asked about %v, want never asked", silent.asked)
	}
}

// TestExecuteWithRelationsChecksAttributesBoundThroughInputs pins that the
// executor asks the vocabulary about the attributes the inputs bind to a
// variable in a pattern's attribute position, through a scalar input and
// through a collection input, in the order the inputs give them.
func TestExecuteWithRelationsChecksAttributesBoundThroughInputs(t *testing.T) {
	naem, aeg := datalog.NewKeyword(":t/naem"), datalog.NewKeyword(":t/aeg")
	for _, c := range []struct {
		name  string
		text  string
		value interface{}
		want  []datalog.Keyword
	}{
		{"scalar", `[:find ?v :in $ ?a :where [?e ?a ?v]]`, naem, []datalog.Keyword{naem}},
		{"collection", `[:find ?v :in $ [?a ...] :where [?e ?a ?v]]`, []interface{}{naem, aeg}, []datalog.Keyword{naem, aeg}},
	} {
		t.Run(c.name, func(t *testing.T) {
			refusal := errors.New("unknown attribute")
			withVocabulary := &vocabularyMatcher{refuses: true, answer: refusal}
			exec := NewExecutor(withVocabulary, nil)
			q := mustParse(t, c.text)
			inputs, err := createInputRelationsFromValuesWithOptions(q, []interface{}{datalog.SymDollar, c.value}, exec.options)
			if err != nil {
				t.Fatalf("binding: %v", err)
			}
			_, err = exec.ExecuteWithRelations(NewContext(), q, inputs)
			if err != refusal {
				t.Errorf("executor answered %v, want the vocabulary's refusal of the attribute the input binds", err)
			}
			if len(withVocabulary.asked) != len(c.want) {
				t.Fatalf("the vocabulary was asked about %v, want the attributes the input binds %v", withVocabulary.asked, c.want)
			}
			for i := range c.want {
				if withVocabulary.asked[i] != c.want[i] {
					t.Errorf("attribute %d asked about is %s, want %s", i, withVocabulary.asked[i], c.want[i])
				}
			}
		})
	}
}

// TestExecuteWithRelationsChecksAttributesASubqueryInputBinds pins that a
// subquery asks the vocabulary, when it runs, about the attributes its own
// inputs bind to a variable in a pattern's attribute position, and that the
// question reaches the vocabulary through the annotation and scan-sharing
// wrappers the executor puts around its matcher.
func TestExecuteWithRelationsChecksAttributesASubqueryInputBinds(t *testing.T) {
	naem := datalog.NewKeyword(":t/naem")
	const text = `[:find ?v :in $ ?a :where [(q [:find ?v :in $ ?attr :where [?e ?attr ?v]] $ ?a) [[?v] ...]]]`
	for _, mode := range optimizerModes {
		for _, wrapper := range []struct {
			name      string
			annotated bool
			sharing   bool
		}{
			{"unwrapped", false, false},
			{"annotated", true, false},
			{"scan-sharing", false, true},
		} {
			t.Run(mode.name+"/"+wrapper.name, func(t *testing.T) {
				refusal := errors.New("unknown attribute :t/naem")
				withVocabulary := &vocabularyMatcher{refuses: true, answer: refusal}
				opts := mode.plannerOptions()
				if wrapper.annotated {
					opts.Handler = func(annotations.Event) {}
				}
				opts.EnableScanSharing = wrapper.sharing
				exec := NewExecutorWithOptions(withVocabulary, nil, opts)
				q := mustParse(t, text)
				inputs, err := createInputRelationsFromValuesWithOptions(q, []interface{}{datalog.SymDollar, naem}, exec.options)
				if err != nil {
					t.Fatalf("binding: %v", err)
				}
				_, err = exec.ExecuteWithRelations(NewContext(), q, inputs)
				if !errors.Is(err, refusal) {
					t.Errorf("executor answered %v, want the vocabulary's refusal of the attribute the subquery's input binds", err)
				}
				if len(withVocabulary.asked) != 1 || withVocabulary.asked[0] != naem {
					t.Errorf("the vocabulary was asked about %v, want the attribute the subquery's input binds", withVocabulary.asked)
				}
			})
		}
	}
}

// TestExecuteWithRelationsChecksRuleBodyAttributes pins that the executor
// asks the vocabulary about the attributes the rule bodies name, not only
// the query's own.
func TestExecuteWithRelationsChecksRuleBodyAttributes(t *testing.T) {
	refusal := errors.New("unknown attribute :page/nxet")
	withVocabulary := &vocabularyMatcher{refuses: true, answer: refusal}
	exec := NewExecutor(withVocabulary, nil)
	rs := mustRules(t, `[[(follows ?a ?b) [?a :page/nxet ?b]]]`)
	q := mustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	_, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
	if err != refusal {
		t.Errorf("executor answered %v, want the vocabulary's refusal of the rule body's attribute", err)
	}
	if len(withVocabulary.asked) != 1 || withVocabulary.asked[0] != datalog.NewKeyword(":page/nxet") {
		t.Errorf("the vocabulary was asked about %v, want the rule body's roster", withVocabulary.asked)
	}
}

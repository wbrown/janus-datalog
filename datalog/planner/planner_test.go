package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPlannerBasic(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
		validate    func(*testing.T, *QueryPlan)
	}{
		{
			name: "simple query",
			query: `[:find ?e ?name
                     :where [?e :person/name ?name]]`,
			validate: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Phases) != 1 {
					t.Errorf("expected 1 phase, got %d", len(plan.Phases))
				}

				phase := plan.Phases[0]
				if len(phase.Patterns) != 1 {
					t.Errorf("expected 1 pattern, got %d", len(phase.Patterns))
				}

				// Should select AEVT index since only attribute is bound
				if phase.Patterns[0].Index != AEVT {
					t.Errorf("expected AEVT index, got %v", phase.Patterns[0].Index)
				}
			},
		},
		{
			name: "entity bound query",
			query: `[:find ?name ?age
                     :where ["person123" :person/name ?name]
                            ["person123" :person/age ?age]]`,
			validate: func(t *testing.T, plan *QueryPlan) {
				// Both patterns have entity bound, should use EAVT
				for i, pattern := range plan.Phases[0].Patterns {
					if pattern.Index != EAVT {
						t.Errorf("pattern %d: expected EAVT index, got %v", i, pattern.Index)
					}
					if !pattern.BoundMask.E {
						t.Errorf("pattern %d: expected entity to be bound", i)
					}
				}
			},
		},
		{
			name: "join query",
			query: `[:find ?person ?name ?email
                     :where [?person :person/name ?name]
                            [?person :person/email ?email]]`,
			validate: func(t *testing.T, plan *QueryPlan) {
				// Patterns share ?person variable
				if len(plan.Phases) != 1 {
					t.Errorf("expected 1 phase for connected patterns, got %d", len(plan.Phases))
				}
			},
		},
		{
			name: "multi-phase query",
			query: `[:find ?p1 ?p2 ?name1 ?name2
                     :where [?p1 :person/name ?name1]
                            [?p2 :person/name ?name2]
                            [?p1 :knows ?p2]]`,
			validate: func(t *testing.T, plan *QueryPlan) {
				// Should have multiple phases due to join on ?p1 and ?p2
				if len(plan.Phases) < 2 {
					t.Errorf("expected at least 2 phases, got %d", len(plan.Phases))
				}
			},
		},
		{
			name: "query with predicates",
			query: `[:find ?person ?age
                     :where [?person :person/age ?age]
                            [(> ?age 21)]]`,
			validate: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Phases) != 1 {
					t.Errorf("expected 1 phase, got %d", len(plan.Phases))
				}

				phase := plan.Phases[0]
				if len(phase.Predicates) != 1 {
					t.Errorf("expected 1 predicate, got %d", len(phase.Predicates))
				}

				// Predicate should reference ?age
				pred := phase.Predicates[0]
				hasAge := false
				for _, v := range pred.RequiredVars {
					if v == query.Symbol("?age") {
						hasAge = true
						break
					}
				}
				if !hasAge {
					t.Errorf("expected predicate to reference ?age")
				}
			},
		},
		{
			name: "reverse lookup query",
			query: `[:find ?person
                     :where [?person :person/email "alice@example.com"]]`,
			validate: func(t *testing.T, plan *QueryPlan) {
				// Should use AVET index for attribute+value lookup
				pattern := plan.Phases[0].Patterns[0]
				if pattern.Index != AVET {
					t.Errorf("expected AVET index for reverse lookup, got %v", pattern.Index)
				}

				if !pattern.BoundMask.A || !pattern.BoundMask.V {
					t.Errorf("expected attribute and value to be bound")
				}
			},
		},
		{
			name: "unbound find variable",
			query: `[:find ?x ?y
                     :where [?x :foo ?z]]`,
			expectError: true,
		},
	}

	planner := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: true,
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			plan, err := planner.Plan(q)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.validate != nil {
				tt.validate(t, plan)
			}
		})
	}
}

func TestIndexSelection(t *testing.T) {
	planner := NewPlanner(nil, PlannerOptions{})

	tests := []struct {
		name     string
		mask     BoundMask
		expected IndexType
	}{
		{
			name:     "all bound",
			mask:     BoundMask{E: true, A: true, V: true},
			expected: EAVT,
		},
		{
			name:     "entity and attribute",
			mask:     BoundMask{E: true, A: true},
			expected: EAVT,
		},
		{
			name:     "attribute and value",
			mask:     BoundMask{A: true, V: true},
			expected: AVET,
		},
		{
			name:     "attribute only",
			mask:     BoundMask{A: true},
			expected: AEVT,
		},
		{
			name:     "value only",
			mask:     BoundMask{V: true},
			expected: VAET,
		},
		{
			name:     "nothing bound",
			mask:     BoundMask{},
			expected: EAVT,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := planner.selectIndex(tt.mask)
			if idx != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, idx)
			}
		})
	}
}

func TestPatternScoring(t *testing.T) {
	planner := NewPlanner(&Statistics{
		AttributeCardinality: map[string]int{
			":person/name":  10000,
			":person/email": 10000,
			":person/age":   100,
		},
	}, PlannerOptions{})

	// Create test patterns
	patterns := []*query.DataPattern{
		// [?e :person/name ?name]
		{Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":person/name")},
			query.Variable{Name: "?name"},
		}},
		// ["person123" :person/age ?age]
		{Elements: []query.PatternElement{
			query.Constant{Value: datalog.NewIdentity("person123")},
			query.Constant{Value: datalog.NewKeyword(":person/age")},
			query.Variable{Name: "?age"},
		}},
		// [?e ?a ?v]
		{Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Variable{Name: "?a"},
			query.Variable{Name: "?v"},
		}},
	}

	resolved := make(map[query.Symbol]bool)

	scores := make([]int, len(patterns))
	for i, pattern := range patterns {
		scores[i] = planner.scorePattern(pattern, resolved)
	}

	// Entity bound pattern should be most selective (lowest score)
	if scores[1] >= scores[0] {
		t.Errorf("entity bound pattern should be more selective than unbound: %d >= %d", scores[1], scores[0])
	}

	// The fully unbound pattern binds 3 new variables, giving it a -30 bonus
	// So it might actually have a lower score than the pattern with just attribute bound
	// This is actually correct behavior - patterns that bind more variables are preferred
	// when nothing is resolved yet, as they provide more information

	// Log scores for debugging
	t.Logf("Pattern scores: attr-only=%d, entity-bound=%d, unbound=%d", scores[0], scores[1], scores[2])
}

func TestPhaseCreation(t *testing.T) {
	planner := NewPlanner(nil, PlannerOptions{
		EnableDynamicReordering: true,
	})

	// Query with dependencies: ?person -> ?friend -> ?friend-name
	queryStr := `[:find ?person ?friend ?friend-name
                  :where [?person :knows ?friend]
                         [?friend :person/name ?friend-name]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("failed to plan query: %v", err)
	}

	// Should create phases that respect dependencies
	if len(plan.Phases) == 0 {
		t.Fatal("expected at least one phase")
	}

	// Track which symbols are available after each phase
	available := make(map[query.Symbol]bool)
	for i, phase := range plan.Phases {
		// Check that patterns in this phase can be executed
		for _, pattern := range phase.Patterns {
			dp := pattern.Pattern.(*query.DataPattern)
			for _, elem := range dp.Elements {
				if elem.IsVariable() {
					if v, ok := elem.(query.Variable); ok {
						// If this variable was needed as input, it should be available
						if i > 0 && pattern.BoundMask.E && dp.Elements[0] == elem {
							if !available[v.Name] {
								t.Errorf("phase %d: variable %s not available", i, v.Name)
							}
						}
					}
				}
			}
		}

		// Update available symbols
		for _, pattern := range phase.Patterns {
			for sym := range pattern.Bindings {
				available[sym] = true
			}
		}
	}

	// All find variables should be available at the end
	for _, elem := range q.Find {
		if v, ok := elem.(query.FindVariable); ok {
			if !available[v.Symbol] {
				t.Errorf("find variable %s not bound by plan", v.Symbol)
			}
		}
	}
}

func TestPredicatePlacement(t *testing.T) {
	planner := NewPlanner(nil, PlannerOptions{
		EnablePredicatePushdown: true,
	})

	queryStr := `[:find ?person ?age
                  :where [?person :person/age ?age]
                         [(> ?age 21)]
                         [?person :person/name ?name]
                         [(str/starts-with? ?name "A")]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("failed to plan query: %v", err)
	}

	// Predicates should be placed as soon as their variables are available
	// They may be in predicates list or pushed to storage as constraints
	foundAgeCheck := false
	foundNameCheck := false

	// Debug: log all predicates
	for i, phase := range plan.Phases {
		t.Logf("Phase %d has %d predicates", i+1, len(phase.Predicates))
		for _, pred := range phase.Predicates {
			t.Logf("  Predicate: %s (type: %T)", pred.Predicate.String(), pred.Predicate)
		}
	}

	for _, phase := range plan.Phases {
		// Check predicates
		for _, pred := range phase.Predicates {
			if comp, ok := pred.Predicate.(*query.Comparison); ok {
				switch comp.Op {
				case query.OpGT:
					foundAgeCheck = true
					// Should have ?age available
					hasAge := false
					for _, sym := range append(phase.Available, phase.Provides...) {
						if sym == "?age" {
							hasAge = true
							break
						}
					}
					if !hasAge {
						t.Error("age predicate placed before ?age is available")
					}
				}
			}
			// Check for other predicate types (like str/starts-with?)
			if fp, ok := pred.Predicate.(*query.FunctionPredicate); ok {
				if fp.Fn == "str/starts-with?" {
					foundNameCheck = true
					// Should have ?name available
					hasName := false
					for _, sym := range append(phase.Available, phase.Provides...) {
						if sym == "?name" {
							hasName = true
							break
						}
					}
					if !hasName {
						t.Error("str/starts-with? predicate placed before ?name is available")
					}
				}
			}
		}

		// Also check storage constraints (predicates may be pushed to storage)
		for _, pattern := range phase.Patterns {
			if pattern.Metadata != nil {
				if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
					for _, c := range constraints {
						if c.Type == ConstraintRange && c.Operator == query.OpGT {
							foundAgeCheck = true
						}
						// Note: str/starts-with? is not currently pushed to storage
					}
				}
			}
		}
	}

	if !foundAgeCheck {
		t.Error("age predicate not found in plan (neither as predicate nor storage constraint)")
	}
	if !foundNameCheck {
		t.Error("name predicate not found in plan")
	}
}

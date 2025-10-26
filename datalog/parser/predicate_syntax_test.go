package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestPredicateSyntaxCoverage ensures all predicate operators parse to correct types
// This test would have caught the not= bug
func TestPredicateSyntaxCoverage(t *testing.T) {
	tests := []struct {
		name         string
		queryStr     string
		expectedType string // Type name of the predicate
		shouldError  bool
	}{
		// Equality predicates
		{
			name:         "equality with =",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(= ?x 5)]]`,
			expectedType: "*query.Comparison",
		},

		// Not-equal predicates (both syntaxes!)
		{
			name:         "not-equal with !=",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(!= ?x 5)]]`,
			expectedType: "*query.NotEqualPredicate",
		},
		{
			name:         "not-equal with not= (Clojure syntax)",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(not= ?x 5)]]`,
			expectedType: "*query.NotEqualPredicate",
		},

		// Comparison predicates
		{
			name:         "less than with <",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(< ?x 10)]]`,
			expectedType: "*query.Comparison",
		},
		{
			name:         "less than or equal with <=",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(<= ?x 10)]]`,
			expectedType: "*query.Comparison",
		},
		{
			name:         "greater than with >",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(> ?x 0)]]`,
			expectedType: "*query.Comparison",
		},
		{
			name:         "greater than or equal with >=",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(>= ?x 0)]]`,
			expectedType: "*query.Comparison",
		},

		// Chained comparisons
		{
			name:         "chained less than",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(< 0 ?x 100)]]`,
			expectedType: "*query.ChainedComparison",
		},

		// Ground/Missing predicates
		{
			name:         "ground predicate",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(ground ?x)]]`,
			expectedType: "*query.GroundPredicate",
		},
		{
			name:         "missing predicate",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(missing ?y)]]`,
			expectedType: "*query.MissingPredicate",
		},

		// Time extraction functions (these ARE valid FunctionPredicates)
		{
			name:         "year extraction",
			queryStr:     `[:find ?x :where [?e :time ?t] [(year ?t) ?x]]`,
			expectedType: "expression", // These are expressions, not predicates
		},
		{
			name:         "month extraction",
			queryStr:     `[:find ?x :where [?e :time ?t] [(month ?t) ?x]]`,
			expectedType: "expression",
		},
		{
			name:         "day extraction",
			queryStr:     `[:find ?x :where [?e :time ?t] [(day ?t) ?x]]`,
			expectedType: "expression",
		},

		// String functions
		{
			name:         "str/starts-with?",
			queryStr:     `[:find ?x :where [?e :attr ?x] [(str/starts-with? ?x "foo")]]`,
			expectedType: "*query.FunctionPredicate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.queryStr)
			if tt.shouldError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected parse error: %v", err)
			}

			if tt.expectedType == "expression" {
				// Find the expression clause
				found := false
				for _, clause := range q.Where {
					if expr, ok := clause.(*query.Expression); ok {
						found = true
						t.Logf("Found expression: %v", expr)
						break
					}
				}
				if !found {
					t.Errorf("Expected to find an expression clause, but didn't")
				}
			} else {
				// Find the predicate clause
				found := false
				for _, clause := range q.Where {
					if pred, ok := clause.(query.Predicate); ok {
						actualType := typeStr(pred)
						if actualType == tt.expectedType {
							found = true
							t.Logf("Correctly parsed as %s: %v", actualType, pred)
							break
						}
					}
				}

				if !found {
					// Show what we actually got
					t.Errorf("Expected predicate type %s, but didn't find it. Got clauses:", tt.expectedType)
					for i, clause := range q.Where {
						t.Errorf("  Clause %d: %T = %v", i, clause, clause)
					}
				}
			}
		})
	}
}

// TestNotEqualKeywordIntegration - End-to-end test with real Keywords
func TestNotEqualKeywordIntegration(t *testing.T) {
	queryStr := `[:find ?attr ?val :where [?s :symbol/ticker "TEST"] [?s ?attr ?val] [(not= ?attr :symbol/ticker)]]`

	q, err := ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Verify we got a NotEqualPredicate
	foundNotEqual := false
	for _, clause := range q.Where {
		if pred, ok := clause.(*query.NotEqualPredicate); ok {
			foundNotEqual = true
			t.Logf("Found NotEqualPredicate: %v", pred)

			// Verify it has the right structure
			if pred.Op != query.OpEQ {
				t.Errorf("NotEqualPredicate.Op should be OpEQ (for inversion), got %v", pred.Op)
			}
		}
	}

	if !foundNotEqual {
		t.Errorf("Query with 'not=' should create NotEqualPredicate")
		for i, clause := range q.Where {
			t.Logf("  Clause %d: %T = %v", i, clause, clause)
		}
	}
}

// Helper to get type as string
func typeStr(v interface{}) string {
	switch v.(type) {
	case *query.Comparison:
		return "*query.Comparison"
	case *query.NotEqualPredicate:
		return "*query.NotEqualPredicate"
	case *query.ChainedComparison:
		return "*query.ChainedComparison"
	case *query.GroundPredicate:
		return "*query.GroundPredicate"
	case *query.MissingPredicate:
		return "*query.MissingPredicate"
	case *query.FunctionPredicate:
		return "*query.FunctionPredicate"
	default:
		return "unknown"
	}
}

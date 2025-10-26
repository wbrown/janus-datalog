package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestShouldDecorrelate(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected bool
	}{
		{
			name: "no subqueries",
			query: `[:find ?name ?age
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]]`,
			expected: false,
		},
		{
			name: "single subquery",
			query: `[:find ?name ?max
			         :where [?e :person/name ?name]
			                [(q [:find (max ?age) :in $ ?n :where [?x :person/age ?age]]
			                    $ ?name) [[?max]]]]`,
			expected: false,
		},
		{
			name: "two subqueries",
			query: `[:find ?name ?max ?min
			         :where [?e :person/name ?name]
			                [(q [:find (max ?age) :in $ ?n :where [?x :person/age ?age]]
			                    $ ?name) [[?max]]]
			                [(q [:find (min ?age) :in $ ?n :where [?x :person/age ?age]]
			                    $ ?name) [[?min]]]]`,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			result := shouldDecorrelate(q.Where)
			if result != tt.expected {
				t.Errorf("shouldDecorrelate() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestAnalyzeSubqueries(t *testing.T) {
	// OHLC-style query with multiple subqueries
	queryStr := `[:find ?hour ?high ?low
	              :where
	                [?s :symbol/ticker "AAPL"]
	                [(hour ?t) ?hour]

	                [(q [:find (max ?h)
	                     :in $ ?sym ?hr
	                     :where [?b :price/symbol ?sym]
	                            [(hour ?time) ?h]
	                            [(= ?h ?hr)]
	                            [?b :price/high ?h]]
	                    $ ?s ?hour) [[?high]]]

	                [(q [:find (min ?l)
	                     :in $ ?sym ?hr
	                     :where [?b :price/symbol ?sym]
	                            [(hour ?time) ?h]
	                            [(= ?h ?hr)]
	                            [?b :price/low ?l]]
	                    $ ?s ?hour) [[?low]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	subqueries, indices, otherClauses, groups := analyzeSubqueries(q.Where)

	// Should have 2 subqueries
	if len(subqueries) != 2 {
		t.Errorf("expected 2 subqueries, got %d", len(subqueries))
	}

	// Should have 2 non-subquery clauses (pattern + expression)
	if len(otherClauses) != 2 {
		t.Errorf("expected 2 other clauses, got %d", len(otherClauses))
	}

	// Should have indices for the subqueries
	if len(indices) != 2 {
		t.Errorf("expected 2 subquery indices, got %d", len(indices))
	}

	// Both subqueries should be in the same group (same signature)
	if len(groups) != 1 {
		t.Errorf("expected 1 group, got %d", len(groups))
		for hash, group := range groups {
			t.Logf("Group %s: %d subqueries", hash, len(group.Indices))
		}
	}

	// The group should contain both subqueries
	for _, group := range groups {
		if len(group.Indices) != 2 {
			t.Errorf("expected group with 2 subqueries, got %d", len(group.Indices))
		}
	}

	t.Logf("✓ Analyzed query: %d subqueries, %d groups, %d other clauses",
		len(subqueries), len(groups), len(otherClauses))
}

func TestExtractCorrelationSignature(t *testing.T) {
	tests := []struct {
		name               string
		subqueryStr        string
		expectedInputCount int
		expectedIsGrouped  bool
	}{
		{
			name: "grouped aggregation",
			subqueryStr: `[(q [:find (max ?h)
			                   :in $ ?sym ?hr
			                   :where [?b :price/symbol ?sym]
			                          [?b :price/high ?h]]
			                  $ ?s ?hour) [[?high]]]`,
			expectedInputCount: 2, // ?sym, ?hr
			expectedIsGrouped:  false, // Only aggregate in :find, no grouping vars
		},
		{
			name: "pure aggregation",
			subqueryStr: `[(q [:find (count ?x)
			                   :in $ ?name
			                   :where [?x :person/name ?name]]
			                  $ ?n) [[?count]]]`,
			expectedInputCount: 1, // ?name
			expectedIsGrouped:  false, // Pure aggregation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse a minimal query with the subquery
			fullQuery := fmt.Sprintf(`[:find ?x :where [?e :test/attr ?x] %s]`, tt.subqueryStr)
			q, err := parser.ParseQuery(fullQuery)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			// Find the subquery clause
			var subq *query.SubqueryPattern
			for _, clause := range q.Where {
				if sq, ok := clause.(*query.SubqueryPattern); ok {
					subq = sq
					break
				}
			}

			if subq == nil {
				t.Fatal("no subquery found in parsed query")
			}

			sig := extractCorrelationSignature(subq)

			if len(sig.InputVars) != tt.expectedInputCount {
				t.Errorf("expected %d input vars, got %d: %v",
					tt.expectedInputCount, len(sig.InputVars), sig.InputVars)
			}

			if sig.IsGroupedAggregate != tt.expectedIsGrouped {
				t.Errorf("expected IsGroupedAggregate=%v, got %v",
					tt.expectedIsGrouped, sig.IsGroupedAggregate)
			}

			t.Logf("✓ Signature: InputVars=%v, IsGrouped=%v, Hash=%s",
				sig.InputVars, sig.IsGroupedAggregate, sig.Hash())
		})
	}
}

func TestGetBatchableGroups(t *testing.T) {
	// Create mock groups
	groups := map[string]*SubqueryGroup{
		"group1": {
			Signature: CorrelationSignature{
				InputVars:          []query.Symbol{"?sym", "?hr"},
				IsGroupedAggregate: true,
			},
			Indices: []int{0, 1}, // 2 subqueries - batchable
		},
		"group2": {
			Signature: CorrelationSignature{
				InputVars:          []query.Symbol{"?name"},
				IsGroupedAggregate: false, // Pure aggregation - NOT batchable
			},
			Indices: []int{2, 3}, // 2 subqueries but wrong type
		},
		"group3": {
			Signature: CorrelationSignature{
				InputVars:          []query.Symbol{"?x"},
				IsGroupedAggregate: true,
			},
			Indices: []int{4}, // Only 1 subquery - NOT batchable
		},
	}

	batchable := getBatchableGroups(groups)

	// Only group1 should be batchable
	if len(batchable) != 1 {
		t.Errorf("expected 1 batchable group, got %d", len(batchable))
	}

	if len(batchable) > 0 && len(batchable[0].Indices) != 2 {
		t.Errorf("expected batchable group to have 2 indices, got %d", len(batchable[0].Indices))
	}

	t.Logf("✓ Correctly identified %d batchable groups", len(batchable))
}

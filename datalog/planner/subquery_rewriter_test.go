package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestDetectCorrelatedAggregates tests pattern detection for correlated aggregate subqueries
func TestDetectCorrelatedAggregates(t *testing.T) {
	tests := []struct {
		name          string
		queryStr      string
		shouldDetect  bool
		numPredicates int
		aggFunction   string
	}{
		{
			name: "Simple correlated aggregate - OHLC daily high",
			queryStr: `[:find ?ticker ?year ?month ?day ?daily-high
			           :where
			           [?s :symbol/ticker ?ticker]
			           [?p :price/symbol ?s]
			           [?p :price/time ?time]
			           [(year ?time) ?year]
			           [(month ?time) ?month]
			           [(day ?time) ?day]

			           [(q [:find (max ?h)
			                :in $ ?sym ?y ?m ?d
			                :where [?p :price/symbol ?sym]
			                       [?p :price/time ?t]
			                       [(year ?t) ?py]
			                       [(month ?t) ?pm]
			                       [(day ?t) ?pd]
			                       [(= ?py ?y)]
			                       [(= ?pm ?m)]
			                       [(= ?pd ?d)]
			                       [?p :price/high ?h]]
			               ?s ?year ?month ?day) [[?daily-high]]]]`,
			shouldDetect:  true,
			numPredicates: 3, // year, month, day equality predicates
			aggFunction:   "max",
		},
		{
			name: "Non-aggregate subquery - should not detect",
			queryStr: `[:find ?ticker ?time
			           :where
			           [?s :symbol/ticker ?ticker]
			           [(q [:find ?t
			                :in $ ?sym
			                :where [?p :price/symbol ?sym]
			                       [?p :price/time ?t]]
			               ?s) [[?time] ...]]]`,
			shouldDetect: false,
		},
		{
			name: "Uncorrelated aggregate - should not detect",
			queryStr: `[:find ?ticker (max ?price)
			           :where
			           [?s :symbol/ticker ?ticker]
			           [?p :price/symbol ?s]
			           [?p :price/value ?price]]`,
			shouldDetect: false,
		},
		{
			name: "Correlated but no filter predicates - should not detect",
			queryStr: `[:find ?ticker ?max-price
			           :where
			           [?s :symbol/ticker ?ticker]
			           [(q [:find (max ?price)
			                :in $ ?sym
			                :where [?p :price/symbol ?sym]
			                       [?p :price/value ?price]]
			               ?s) [[?max-price]]]]`,
			shouldDetect: false, // No equality predicates filtering
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.queryStr)
			require.NoError(t, err)

			// Create planner and plan query
			stats := &Statistics{}
			planner := NewPlanner(stats, PlannerOptions{})
			plan, err := planner.Plan(q)
			require.NoError(t, err)

			// Look for correlated aggregates in all phases
			foundPatterns := 0
			for _, phase := range plan.Phases {
				patterns := detectCorrelatedAggregates(&phase)
				foundPatterns += len(patterns)

				if len(patterns) > 0 {
					pattern := patterns[0]

					if tt.shouldDetect {
						assert.Equal(t, tt.numPredicates, len(pattern.FilterPredicates),
							"expected %d filter predicates", tt.numPredicates)
						assert.Equal(t, tt.aggFunction, pattern.Aggregate.Function,
							"expected aggregate function %s", tt.aggFunction)
					}
				}
			}

			if tt.shouldDetect {
				assert.Greater(t, foundPatterns, 0, "expected to detect correlated aggregate pattern")
			} else {
				assert.Equal(t, 0, foundPatterns, "should not detect pattern")
			}
		})
	}
}

// TestAnalyzeSubqueryForRewriting tests the subquery analysis logic
func TestAnalyzeSubqueryForRewriting(t *testing.T) {
	// Parse a query with correlated aggregate subquery
	queryStr := `[:find ?ticker ?daily-high
	             :where
	             [?s :symbol/ticker ?ticker]
	             [?p :price/time ?time]
	             [(year ?time) ?year]
	             [(month ?time) ?month]

	             [(q [:find (max ?h)
	                  :in $ ?sym ?y ?m
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/time ?t]
	                         [(year ?t) ?py]
	                         [(month ?t) ?pm]
	                         [(= ?py ?y)]
	                         [(= ?pm ?m)]
	                         [?p :price/high ?h]]
	                 ?s ?year ?month) [[?daily-high]]]]`

	q, err := parser.ParseQuery(queryStr)
	require.NoError(t, err)

	// Plan the query to get subquery plans
	stats := &Statistics{}
	planner := NewPlanner(stats, PlannerOptions{})
	plan, err := planner.Plan(q)
	require.NoError(t, err)

	// Find a subquery plan
	var subqPlan *SubqueryPlan
	for _, phase := range plan.Phases {
		if len(phase.Subqueries) > 0 {
			subqPlan = &phase.Subqueries[0]
			break
		}
	}
	require.NotNil(t, subqPlan, "should find subquery plan")

	// Analyze it
	pattern, eligible := analyzeSubqueryForRewriting(0, subqPlan)

	assert.True(t, eligible, "subquery should be eligible for rewriting")
	assert.Equal(t, "max", pattern.Aggregate.Function)
	assert.Equal(t, 3, len(pattern.InputParams), "should have 3 input params: ?sym, ?y, ?m")
	assert.Equal(t, 2, len(pattern.FilterPredicates), "should have 2 filter predicates")

	// Check filter predicates
	predicateVars := make(map[string]string) // inner -> outer
	for _, fp := range pattern.FilterPredicates {
		predicateVars[string(fp.InnerVar)] = string(fp.OuterParam)
	}
	assert.Equal(t, "?y", predicateVars["?py"], "?py should map to ?y")
	assert.Equal(t, "?m", predicateVars["?pm"], "?pm should map to ?m")
}

// TestFindFilterPredicates tests the filter predicate detection logic
func TestFindFilterPredicates(t *testing.T) {
	// Parse subquery
	queryStr := `[:find (min ?price)
	             :in $ ?sym ?year ?month
	             :where [?p :price/symbol ?sym]
	                    [?p :price/time ?t]
	                    [(year ?t) ?y]
	                    [(month ?t) ?m]
	                    [(= ?y ?year)]
	                    [(= ?m ?month)]
	                    [?p :price/value ?price]]`

	q, err := parser.ParseQuery(queryStr)
	require.NoError(t, err)

	// Extract input params from the first :in clause after database
	inputParams := []query.Symbol{}
	for _, input := range q.In[1:] { // Skip database
		if si, ok := input.(query.ScalarInput); ok {
			inputParams = append(inputParams, si.Symbol)
		}
	}

	// Find filter predicates
	predicates, exprIndices := findFilterPredicates(q, inputParams)

	assert.Equal(t, 2, len(predicates), "should find 2 filter predicates")
	assert.Equal(t, 2, len(exprIndices), "should find 2 filter expressions")

	// Verify predicates map inner vars to outer params
	predicateMap := make(map[string]string)
	for _, fp := range predicates {
		predicateMap[string(fp.InnerVar)] = string(fp.OuterParam)
	}
	assert.Equal(t, "?year", predicateMap["?y"])
	assert.Equal(t, "?month", predicateMap["?m"])
}

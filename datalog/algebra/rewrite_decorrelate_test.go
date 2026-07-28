package algebra

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestDecorrelation_SimpleAggregate verifies the basic decorrelation:
// LateralJoin with aggregated subquery → decorrelated subquery with GROUP BY.
func TestDecorrelation_SimpleAggregate(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count
	  :where
	  [?e :entity/type :entity.type/scenario]
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])]`)
	require.NoError(t, err)

	// Compile to algebra
	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before decorrelation:\n%s", root.String())

	// Verify LateralJoin exists before optimization
	lj := findLateralJoin(root)
	require.NotNil(t, lj, "should have LateralJoin before optimization")
	ljData := lj.Data.(*LateralJoin)
	t.Logf("LateralJoin correlation vars: %v", ljData.CorrelationVars)

	// Apply decorrelation
	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After decorrelation:\n%s", optimized.String())

	// Verify no LateralJoin remains
	ljAfter := findLateralJoin(optimized)
	assert.Nil(t, ljAfter, "LateralJoin should be eliminated by decorrelation")

	// Decompile back to clauses
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}

	// Verify the decorrelated subquery has the correlation var in :find
	for _, c := range clauses {
		if sp, ok := c.(*query.SubqueryPattern); ok {
			t.Logf("SubqueryPattern found: %s", sp.Query.String())
			// The decorrelated query should have ?s (or ?e) in :find
			hasGroupBy := false
			for _, fe := range sp.Query.Find {
				if fv, ok := fe.(query.FindVariable); ok {
					t.Logf("  Find variable: %s", fv.Symbol.String())
					hasGroupBy = true
				}
			}
			assert.True(t, hasGroupBy, "decorrelated query should have grouping variable in :find")

			// The decorrelated query should NOT have ?s in :in (only $)
			for _, in := range sp.Query.In {
				if si, ok := in.(query.ScalarInput); ok {
					t.Errorf("decorrelated query should not have scalar input %s", si.Symbol.String())
				}
			}
		}

		// Check if it's wrapped in an OR (for defaults)
		if oc, ok := c.(*query.OrClause); ok {
			t.Logf("OR clause with %d branches", len(oc.Branches))
			for i, branch := range oc.Branches {
				t.Logf("  Branch %d: %d clauses", i, len(branch))
				for _, bc := range branch {
					t.Logf("    %T: %s", bc, bc.String())
				}
			}
		}
	}
}

// TestDecorrelation_NonAggregateUnchanged verifies that non-aggregate
// subqueries are NOT decorrelated (they'd produce wrong results).
func TestDecorrelation_NonAggregateUnchanged(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?name
	  :where
	  [?e :entity/type :entity.type/scenario]
	  [(q [:find ?name
	       :in $ ?s
	       :where [?s :scenario/name ?name]]
	      $ ?e) [[?name]]]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Verify LateralJoin exists
	lj := findLateralJoin(root)
	require.NotNil(t, lj, "should have LateralJoin")

	// Apply decorrelation — should leave it unchanged
	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)

	// LateralJoin should still be there (non-aggregate, can't decorrelate)
	ljAfter := findLateralJoin(optimized)
	assert.NotNil(t, ljAfter, "non-aggregate LateralJoin should remain")
}

// TestDecorrelation_MultipleSubqueries verifies that multiple correlated
// subqueries in the same query are all decorrelated.
func TestDecorrelation_MultipleSubqueries(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count ?total
	  :where
	  [?e :entity/type :entity.type/scenario]
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])
	  (or-default [(q [:find (sum ?v)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/value ?v]]
	          $ ?e) [[?total]]]
	      [(ground 0) ?total])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	// Count LateralJoins before
	ljCount := countNodes(root, RuleLateralJoin)
	t.Logf("LateralJoins before: %d", ljCount)

	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	// Count LateralJoins after — should be fewer
	ljCountAfter := countNodes(optimized, RuleLateralJoin)
	t.Logf("LateralJoins after: %d", ljCountAfter)
	assert.Less(t, ljCountAfter, ljCount, "decorrelation should reduce LateralJoin count")
}

// TestDecorrelation_NestedWithIntermediateMap tests that decorrelation
// propagates through intermediate Map nodes between LateralJoins.
// Tree: LateralJoin → Map(comparison) → LateralJoin → Scan
func TestDecorrelation_NestedWithIntermediateMap(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count ?ready
	  :where
	  [?e :entity/type :entity.type/project]
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :item/project ?s]
	                  [?t :item/status :status/done]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])
	  [[(> ?count 0)] ?ready]
	  (or-default [(q [:find (sum ?c)
	           :in $ ?s
	           :where [?t :item/project ?s]
	                  [?t :item/cost ?c]]
	          $ ?e) [[?total]]]
	      [(ground 0) ?total])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	ljBefore := countNodes(root, RuleLateralJoin)
	t.Logf("LateralJoin count before: %d", ljBefore)
	require.Equal(t, 2, ljBefore)

	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	ljAfter := countNodes(optimized, RuleLateralJoin)
	t.Logf("LateralJoin count after: %d", ljAfter)

	assert.Equal(t, 0, ljAfter,
		"LateralJoins separated by Map should still be decorrelated")
}

// TestDecorrelation_NestedWithNotAndGetElse adds NOT and get-else to the
// nested LateralJoin test to narrow down what breaks propagation.
func TestDecorrelation_NestedWithNotAndGetElse(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?label ?count ?ready ?total
	  :where
	  [?e :entity/type :entity.type/project]
	  (not [?e :entity/deleted true])
	  [(get-else $ ?e :project/label "") ?label]
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :item/project ?s]
	                  [?t :item/status :status/done]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])
	  [[(> ?count 0)] ?ready]
	  (or-default [(q [:find (sum ?c)
	           :in $ ?s
	           :where [?t :item/project ?s]
	                  [?t :item/cost ?c]]
	          $ ?e) [[?total]]]
	      [(ground 0) ?total])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	ljBefore := countNodes(root, RuleLateralJoin)
	t.Logf("LateralJoin count before: %d", ljBefore)
	require.Equal(t, 2, ljBefore)

	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	ljAfter := countNodes(optimized, RuleLateralJoin)
	t.Logf("LateralJoin count after: %d", ljAfter)

	assert.Equal(t, 0, ljAfter,
		"LateralJoins with NOT and get-else should still be decorrelated")
}

// TestDecorrelation_MixedAggreateAndNonAggregate tests decorrelation when
// some LateralJoins have aggregates (decorrelatable) and one doesn't
// (skipped). The non-aggregate LateralJoin at the top should survive,
// and the aggregate ones below should still be decorrelated.
func TestDecorrelation_MixedAggregateAndNonAggregate(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count ?lastKey
	  :where
	  [?e :entity/type :entity.type/project]
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :item/project ?s]
	                  [?t :item/status :status/done]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])
	  (or-default [(q [:find ?key
	           :in $ ?s
	           :where [?t :item/project ?s]
	                  [?t :item/key ?key]]
	          $ ?e) [[?lastKey]]]
	      [(ground :none) ?lastKey])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	ljBefore := countNodes(root, RuleLateralJoin)
	t.Logf("LateralJoin count before: %d", ljBefore)

	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	ljAfter := countNodes(optimized, RuleLateralJoin)
	t.Logf("LateralJoin count after: %d", ljAfter)

	// The aggregate LateralJoin (count) should be decorrelated.
	// The non-aggregate LateralJoin (find ?key) should remain.
	assert.Less(t, ljAfter, ljBefore,
		"aggregate LateralJoins should be decorrelated even with non-aggregate siblings")
}

// TestDecorrelation_ProductionStructure verifies that decorrelation fires
// on a query structurally identical to the production query: NOT clause,
// multiple get-else expressions, and multiple OR-fallback subqueries with
// correlation, aggregation, and get-else inside them.
//
// This test catches the EBNF transform propagation issue where the optimizer
// applies the decorrelation transform (decorrelate-apply fires) but the
// rewritten nodes are lost during tree reconstruction (changed:false).
func TestDecorrelation_ProductionStructure(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?project ?label ?createdAt ?priority ?category ?region ?owner ?notes ?itemCount ?totalCost ?totalWeight ?inputUnits ?outputUnits ?ready ?lastKey ?lastUpdatedAt
	  :where
	  [?project :entity/type :entity.type/project]
	  (not [?project :entity/deleted true])
	  [(get-else $ ?project :project/label "") ?label]
	  [?project :project/created-at ?createdAt]
	  [(get-else $ ?project :project/priority 0) ?priority]
	  [(get-else $ ?project :project/category "") ?category]
	  [(get-else $ ?project :project/region "") ?region]
	  [(get-else $ ?project :project/owner "") ?owner]
	  [(get-else $ ?project :project/notes "") ?notes]
	  (or-default [(q [:find (count ?i) (sum ?c) (sum ?w) (sum ?iu) (sum ?ou)
	           :in $ ?p
	           :where [?i :item/project ?p]
	                  [?i :item/status :status/done]
	                  (not [?i :entity/deleted true])
	                  [(get-else $ ?i :item/cost 0) ?c]
	                  [(get-else $ ?i :item/weight 0) ?w]
	                  [(get-else $ ?i :item/input-units 0) ?iu]
	                  [(get-else $ ?i :item/output-units 0) ?ou]]
	          $ ?project) [[?itemCount ?totalCost ?totalWeight ?inputUnits ?outputUnits]]]
	      [(ground [0 0 0 0 0]) [[?itemCount ?totalCost ?totalWeight ?inputUnits ?outputUnits]]])
	  (or-default [(q [:find (count ?i)
	           :in $ ?p
	           :where [?i :item/project ?p]
	                  [?i :item/key :step/init]
	                  [?i :item/status :status/done]
	                  (not [?i :entity/deleted true])]
	          $ ?project) [[?initCount]]]
	      [(ground 0) ?initCount])
	  [[(> ?initCount 0)] ?ready]
	  (or-default [(q [:find ?key ?ca
	           :in $ ?p
	           :where [?i :item/project ?p]
	                  [?i :item/status :status/done]
	                  [?i :item/completed-at ?ca]
	                  [?i :item/key ?key]
	                  (not [?i :entity/deleted true])
	                  [(q [:find (max ?ca)
	                       :in $ ?p
	                       :where [?i :item/project ?p]
	                              [?i :item/status :status/done]
	                              [?i :item/completed-at ?ca]
	                              (not [?i :entity/deleted true])]
	                      $ ?p) [[?maxCa]]]
	                  [(= ?ca ?maxCa)]]
	          $ ?project) [[?lastKey ?lastUpdatedAt]]]
	      [(ground [:none :none]) [[?lastKey ?lastUpdatedAt]]])
	  :order-by [[?lastUpdatedAt :desc]]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	ljBefore := countNodes(root, RuleLateralJoin)
	t.Logf("LateralJoin count before: %d", ljBefore)
	require.Greater(t, ljBefore, 0, "should have LateralJoin nodes before optimization")

	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	ljAfter := countNodes(optimized, RuleLateralJoin)
	t.Logf("LateralJoin count after: %d", ljAfter)

	// THE KEY ASSERTION: decorrelation must actually eliminate aggregate LateralJoins.
	// If this fails, the EBNF transform is not propagating rewrites in deep trees.
	assert.Less(t, ljAfter, ljBefore,
		"decorrelation must eliminate aggregate LateralJoins — if equal, transform propagation is broken")

	// The 3rd LateralJoin (non-aggregate argmax pattern) should survive.
	// The 2 aggregate LateralJoins should be eliminated.
	assert.Equal(t, 1, ljAfter,
		"exactly 1 LateralJoin should remain (the non-aggregate argmax pattern)")

	// Verify decompilation succeeds
	optimizedClauses, err := Decompile(optimized)
	require.NoError(t, err)
	t.Logf("Decompiled %d clauses", len(optimizedClauses))
}

// TestDecorrelation_NestedLateralJoins tests that decorrelation works when
// LateralJoins are nested (each OR-fallback subquery creates a LateralJoin
// that is a child of the next). This is the minimal reproduction of the
// production query propagation bug.
func TestDecorrelation_NestedLateralJoins(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count ?total
	  :where
	  [?e :entity/type :entity.type/project]
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :item/project ?s]
	                  [?t :item/status :status/done]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])
	  (or-default [(q [:find (sum ?c)
	           :in $ ?s
	           :where [?t :item/project ?s]
	                  [?t :item/cost ?c]]
	          $ ?e) [[?total]]]
	      [(ground 0) ?total])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	ljBefore := countNodes(root, RuleLateralJoin)
	t.Logf("LateralJoin count before: %d", ljBefore)
	require.Equal(t, 2, ljBefore, "should have 2 nested LateralJoins")

	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	ljAfter := countNodes(optimized, RuleLateralJoin)
	t.Logf("LateralJoin count after: %d", ljAfter)

	assert.Equal(t, 0, ljAfter,
		"both aggregate LateralJoins should be eliminated by decorrelation")
}

// TestDecorrelation_ArgmaxPattern tests decorrelation of the argmax pattern:
// a non-aggregate outer subquery containing a nested aggregate subquery.
// The outer subquery should be decorrelated even though it has no aggregates
// in :find, because it contains a nested correlated subquery.
func TestDecorrelation_ArgmaxPattern(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?lastKey ?lastUpdatedAt
	  :where
	  [?e :entity/type :entity.type/project]
	  (or-default [(q [:find ?key ?ca
	           :in $ ?s
	           :where [?t :item/project ?s]
	                  [?t :item/completed-at ?ca]
	                  [?t :item/key ?key]
	                  [(q [:find (max ?ca)
	                       :in $ ?s
	                       :where [?t :item/project ?s]
	                              [?t :item/completed-at ?ca]]
	                      $ ?s) [[?maxCa]]]
	                  [(= ?ca ?maxCa)]]
	          $ ?e) [[?lastKey ?lastUpdatedAt]]]
	      [(ground [:none :none]) [[?lastKey ?lastUpdatedAt]]])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	ljBefore := countNodes(root, RuleLateralJoin)
	t.Logf("LateralJoin count before: %d", ljBefore)
	require.Greater(t, ljBefore, 0)

	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	ljAfter := countNodes(optimized, RuleLateralJoin)
	t.Logf("LateralJoin count after: %d", ljAfter)

	// The outer LateralJoin should be decorrelated into a LeftOuterJoin.
	// The inner result is an uncorrelated LateralJoin (CorrelationVars=nil),
	// so countNodes still finds 1 LateralJoin. Check for correlated ones.
	correlatedAfter := countCorrelatedLateralJoins(optimized)
	assert.Equal(t, 0, correlatedAfter,
		"no CORRELATED LateralJoins should remain after argmax decorrelation")
}

// TestDecorrelation_PureDataPatternSkipped tests that a correlated subquery
// with only bare DataPatterns (no predicates, expressions, or nested subqueries)
// is NOT decorrelated — the correlated indexed lookup is faster than full scan.
func TestDecorrelation_PureDataPatternSkipped(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?val
	  :where
	  [?e :entity/type :entity.type/project]
	  (or-default [(q [:find ?v
	           :in $ ?s
	           :where [?s :project/value ?v]]
	          $ ?e) [[?val]]]
	      [(ground 0) ?val])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	ljBefore := countNodes(root, RuleLateralJoin)

	optimizer := NewOptimizer(DecorrelationPass(nil))
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)

	ljAfter := countNodes(optimized, RuleLateralJoin)

	// Pure DataPattern subquery should NOT be decorrelated
	assert.Equal(t, ljBefore, ljAfter,
		"pure DataPattern subquery should stay correlated — indexed lookup is faster")
}

// TestDecorrelation_EqualityBoundTranslation pins the equality-bound
// correlation translation on the OHLC flagship shape: correlation parameters
// consumed only by [(= ?inner ?param)] predicates are translated — the inner
// side becomes the group-by symbol, the predicate is consumed as the join
// condition, and the binding positionally renames it to the outer name.
// Subqueries with extra non-correlation consumption (?smod/?emod
// inequalities) decline and stay correlated. See
// docs/bugs/BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md.
func TestDecorrelation_EqualityBoundTranslation(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?datetime ?open-price ?hour-high ?hour-low
	 :where
	    [?s :symbol/ticker "CRWV"]
	    [?first-bar :price/symbol ?s]
	    [?first-bar :price/time ?t]
	    [(year ?t) ?year]
	    [(month ?t) ?month]
	    [(day ?t) ?day]
	    [(hour ?t) ?hour]
	    [?first-bar :price/minute-of-day ?mod]
	    [(* ?hour 60) ?hour-start]
	    [(+ ?hour-start 4) ?open-end]
	    [(>= ?mod ?hour-start)]
	    [(<= ?mod ?open-end)]
	    [(str ?year "-" ?month "-" ?day " " ?hour ":00") ?datetime]

	    [(q [:find (max ?h) (min ?l)
	         :in $ ?sym ?y ?m ?d ?hr
	         :where [?b :price/symbol ?sym]
	                [?b :price/time ?time]
	                [(year ?time) ?py]
	                [(month ?time) ?pm]
	                [(day ?time) ?pd]
	                [(hour ?time) ?ph]
	                [(= ?py ?y)]
	                [(= ?pm ?m)]
	                [(= ?pd ?d)]
	                [(= ?ph ?hr)]
	                [?b :price/high ?h]
	                [?b :price/low ?l]]
	        $ ?s ?year ?month ?day ?hour) [[?hour-high ?hour-low]]]

	    [(q [:find (min ?o)
	         :in $ ?sym ?y ?m ?d ?hr ?smod ?emod
	         :where [?b :price/symbol ?sym]
	                [?b :price/time ?time]
	                [(year ?time) ?py]
	                [(month ?time) ?pm]
	                [(day ?time) ?pd]
	                [(hour ?time) ?ph]
	                [(= ?py ?y)]
	                [(= ?pm ?m)]
	                [(= ?pd ?d)]
	                [(= ?ph ?hr)]
	                [?b :price/minute-of-day ?bmod]
	                [(>= ?bmod ?smod)]
	                [(<= ?bmod ?emod)]
	                [?b :price/open ?o]]
	        $ ?s ?year ?month ?day ?hour ?hour-start ?open-end) [[?open-price]]]]`)
	require.NoError(t, err)

	// Mirror the bridge: compile the WHERE clauses only, optimize with the
	// default passes, decompile back to clauses.
	root, err := Compile(&query.Query{Where: q.Where})
	require.NoError(t, err)
	t.Logf("Before:\n%s", root.String())

	optimizer := NewOptimizer(DefaultPasses(nil)...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("After:\n%s", optimized.String())

	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	for i, c := range clauses {
		t.Logf("clause [%d] %T: %s", i, c, c.String())
	}

	var uncorrelated, correlated []*query.SubqueryPattern
	for _, c := range clauses {
		sp, ok := c.(*query.SubqueryPattern)
		if !ok {
			continue
		}
		hasVarInput := false
		for _, in := range sp.Inputs {
			if v, ok := in.(query.Variable); ok && !v.Name.IsSource() {
				hasVarInput = true
			}
		}
		if hasVarInput {
			correlated = append(correlated, sp)
		} else {
			uncorrelated = append(uncorrelated, sp)
		}
	}

	// The equality-bound subquery is translated into one uncorrelated
	// grouped subquery; the ?smod/?emod subquery declines and stays
	// correlated per-combination.
	require.Len(t, uncorrelated, 1, "the equality-bound subquery translates to one uncorrelated grouped subquery")
	require.Len(t, correlated, 1, "the extras subquery declines decorrelation and stays correlated")

	grouped := uncorrelated[0]

	// Group-by keys are the inner symbols, in parameter order, followed by
	// the original aggregates.
	wantFind := []query.Symbol{
		datalog.NewSymbol("?sym"),
		datalog.NewSymbol("?py"),
		datalog.NewSymbol("?pm"),
		datalog.NewSymbol("?pd"),
		datalog.NewSymbol("?ph"),
	}
	var gotFindVars []query.Symbol
	aggCount := 0
	for _, fe := range grouped.Query.Find {
		switch e := fe.(type) {
		case query.FindVariable:
			gotFindVars = append(gotFindVars, e.Symbol)
		case query.FindAggregate:
			aggCount++
		}
	}
	assert.Equal(t, wantFind, gotFindVars, "group-by symbols are the inner equality sides in parameter order")
	assert.Equal(t, 2, aggCount, "both aggregates preserved")

	// The correlation parameters are gone from :in, and the consumed
	// correlation equalities are gone from :where.
	for _, in := range grouped.Query.In {
		if si, ok := in.(query.ScalarInput); ok {
			t.Errorf("translated query should have no scalar inputs, found %s", si.Symbol.String())
		}
	}
	for _, c := range grouped.Query.Where {
		if cmp, ok := c.(*query.Comparison); ok {
			t.Errorf("translated query should have no remaining comparisons, found %s", cmp.String())
		}
	}

	// The binding positionally renames group symbols to the outer
	// correlation names, then the aggregate outputs.
	rb, ok := grouped.Binding.(query.RelationBinding)
	require.True(t, ok, "grouped subquery binds as a relation")
	assert.Equal(t,
		[]query.Symbol{
			datalog.NewSymbol("?s"),
			datalog.NewSymbol("?year"),
			datalog.NewSymbol("?month"),
			datalog.NewSymbol("?day"),
			datalog.NewSymbol("?hour"),
			datalog.NewSymbol("?hour-high"),
			datalog.NewSymbol("?hour-low"),
		},
		rb.Variables,
		"binding renames group symbols to outer names positionally")

	// The declined subquery keeps its full input list — all seven variables.
	assert.Len(t, correlated[0].Inputs, 8, "declined subquery keeps $ plus its seven variable inputs")
}

// countCorrelatedLateralJoins counts LateralJoin nodes with non-empty CorrelationVars.
func countCorrelatedLateralJoins(n *Node) int {
	if n == nil {
		return 0
	}
	count := 0
	if n.Op == RuleLateralJoin {
		if lj, ok := n.Data.(*LateralJoin); ok && len(lj.CorrelationVars) > 0 {
			count++
		}
	}
	for _, child := range n.Children {
		count += countCorrelatedLateralJoins(child)
	}
	return count
}

// countNodes counts nodes with a given Op in the tree.
func countNodes(n *Node, op string) int {
	if n == nil {
		return 0
	}
	count := 0
	if n.Op == op {
		count++
	}
	for _, child := range n.Children {
		count += countNodes(child, op)
	}
	return count
}

func walkNodes(n *Node, fn func(*Node)) {
	if n == nil {
		return
	}
	fn(n)
	for _, child := range n.Children {
		walkNodes(child, fn)
	}
}

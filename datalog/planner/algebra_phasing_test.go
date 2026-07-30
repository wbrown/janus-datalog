package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestPhaserSymbolExtraction checks what symbols the phaser extracts
// for the specific query.
func TestPhaserSymbolExtraction(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count
	       :where [?e :entity/type :entity.type/scenario]
	              (not [?e :entity/deleted true])
	              (or [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?e) [[?count]]]
	                  [(ground 0) ?count])]`)
	require.NoError(t, err)

	// Compile + decorrelate
	root, _ := algebra.Compile(q)
	opt := algebra.NewOptimizer(algebra.DefaultPasses(nil)...)
	optimized, _ := opt.Optimize(root)
	clauses, _ := algebra.Decompile(optimized)

	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		scope := query.ScopeOf(c)
		t.Logf("  [%d] %T provides=%v correlates=%v", i, c, scope.Provides, scope.Correlates)
	}
}

// TestPhaserWithDecorrelatedClauses checks that the phaser can handle
// the clause list produced by the algebra decorrelation pass.
func TestPhaserWithDecorrelatedClauses(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens ?totalDuration ?complete ?lastKey ?lastUpdatedAt
	  :where
	  [?scenario :entity/type :entity.type/scenario]
	  [?scenario :scenario/title ?title]
	  [?scenario :scenario/created-at ?createdAt]
	  (or [(q [:find (count ?t) (sum ?tok) (sum ?dur)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]
	                  [(get-else $ ?t :task/token-count 0) ?tok]
	                  [(get-else $ ?t :task/duration 0) ?dur]]
	          $ ?scenario) [[?taskCount ?totalTokens ?totalDuration]]]
	      [(ground [0 0 0]) [[?taskCount ?totalTokens ?totalDuration]]])
	  (or [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/key :task/opening]
	                  [?t :task/status :status/complete]]
	          $ ?scenario) [[?openingCount]]]
	      [(ground 0) ?openingCount])
	  [[(> ?openingCount 0)] ?complete]
	  (or [(q [:find ?key ?ca
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]
	                  [?t :task/completed-at ?ca]
	                  [?t :task/key ?key]
	                  [(q [:find (max ?ca)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]
	                              [?t :task/completed-at ?ca]]
	                      $ ?s) [[?maxCa]]]
	                  [(= ?ca ?maxCa)]]
	          $ ?scenario) [[?lastKey ?lastUpdatedAt]]]
	      [(ground [:none :none]) [[?lastKey ?lastUpdatedAt]]])
	  :order-by [[?lastUpdatedAt :desc]]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Compile + decorrelate
	root, err := algebra.Compile(q)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	t.Logf("Before:\n%s", root.String())

	optimizer := algebra.NewOptimizer(algebra.DefaultPasses(nil)...)
	optimized, err := optimizer.Optimize(root)
	if err != nil {
		t.Fatalf("optimize: %v", err)
	}
	t.Logf("After:\n%s", optimized.String())

	clauses, err := algebra.Decompile(optimized)
	if err != nil {
		t.Fatalf("decompile: %v", err)
	}
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		scope := query.ScopeOf(c)
		t.Logf("  [%d] %T provides=%v correlates=%v", i, c, scope.Provides, scope.Correlates)
	}

	// Try phasing
	findSymbols := make([]query.Symbol, 0)
	for _, fe := range q.Find {
		if fv, ok := fe.(query.FindVariable); ok {
			findSymbols = append(findSymbols, fv.Symbol)
		}
	}
	inputSymbols := map[query.Symbol]bool{}

	phases, err := createPhasesGreedy(clauses, findSymbols, inputSymbols)
	if err != nil {
		t.Fatalf("phasing failed: %v", err)
	}
	t.Logf("Created %d phases", len(phases))
}

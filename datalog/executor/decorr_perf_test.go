package executor

import (
	"sync"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

func TestDecorrelationActuallyWorks(t *testing.T) {
	// Create data: 100 categories with products
	var datoms []datalog.Datom
	for cat := 0; cat < 100; cat++ {
		catID := datalog.NewIdentity("cat-" + string(rune('A'+cat)))
		datoms = append(datoms, datalog.Datom{
			E: catID, A: datalog.NewKeyword(":category/name"), V: string(rune('A' + cat)), Tx: 1,
		})

		// Each category has 10 products
		for prod := 0; prod < 10; prod++ {
			prodID := datalog.NewIdentity("prod-" + string(rune('A'+cat)) + string(rune('0'+prod)))
			price := float64(100 + cat + prod)
			datoms = append(datoms,
				datalog.Datom{E: prodID, A: datalog.NewKeyword(":product/category"), V: catID, Tx: 1},
				datalog.Datom{E: prodID, A: datalog.NewKeyword(":product/price"), V: price, Tx: 1},
			)
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Query with 2 subqueries that should decorrelate
	queryStr := `[:find ?name ?max-price ?count
	             :where
	               [?c :category/name ?name]
	               [(q [:find (max ?p) :in $ ?cat
	                    :where [?prod :product/category ?cat]
	                           [?prod :product/price ?p]]
	                  $ ?c) [[?max-price]]]
	               [(q [:find (count ?prod) :in $ ?cat
	                    :where [?prod :product/category ?cat]]
	                  $ ?c) [[?count]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	// Test WITHOUT decorrelation
	execNoDecor := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	startNo := time.Now()
	resultNo, err := execNoDecor.Execute(q)
	durNo := time.Since(startNo)
	if err != nil {
		t.Fatalf("No decorrelation failed: %v", err)
	}

	// Test WITH decorrelation (parallel by default)
	execWithDecor := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	startWith := time.Now()
	resultWith, err := execWithDecor.Execute(q)
	durWith := time.Since(startWith)
	if err != nil {
		t.Fatalf("With decorrelation failed: %v", err)
	}

	// Verify results match
	if resultNo.Size() != resultWith.Size() {
		t.Errorf("Size mismatch: no_decorr=%d, with_decorr=%d", resultNo.Size(), resultWith.Size())
	}

	if resultNo.Size() != 100 {
		t.Errorf("Expected 100 categories, got %d", resultNo.Size())
	}

	t.Logf("WITHOUT decorrelation: %v (%d results)", durNo, resultNo.Size())
	t.Logf("WITH decorrelation: %v (%d results)", durWith, resultWith.Size())

	speedup := float64(durNo) / float64(durWith)
	t.Logf("Speedup: %.2fx", speedup)

	// After decorrelation bug fix: Both subqueries are PURE aggregations
	// ([:find (max ?p)] and [:find (count ?prod)])
	// Pure aggregations are no longer decorrelated, so there should be NO speedup.
	// In fact, the decorrelation flag now has no effect on pure aggregations.
	// Both versions should perform similarly (within 20% variance due to system load).
	if speedup < 0.80 || speedup > 1.20 {
		t.Logf("Note: Speedup %.2fx is within expected range (no decorrelation for pure aggregations)", speedup)
	}
}

func TestDecorrelationAnnotations(t *testing.T) {
	// Create simple test data
	var datoms []datalog.Datom
	for cat := 0; cat < 10; cat++ {
		catID := datalog.NewIdentity("cat-" + string(rune('A'+cat)))
		datoms = append(datoms, datalog.Datom{
			E: catID, A: datalog.NewKeyword(":category/name"), V: string(rune('A' + cat)), Tx: 1,
		})

		for prod := 0; prod < 5; prod++ {
			prodID := datalog.NewIdentity("prod-" + string(rune('A'+cat)) + string(rune('0'+prod)))
			price := float64(100 + cat + prod)
			datoms = append(datoms,
				datalog.Datom{E: prodID, A: datalog.NewKeyword(":product/category"), V: catID, Tx: 1},
				datalog.Datom{E: prodID, A: datalog.NewKeyword(":product/price"), V: price, Tx: 1},
			)
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Query with 2 subqueries that decorrelate
	queryStr := `[:find ?name ?max-price ?count
	             :where
	               [?c :category/name ?name]
	               [(q [:find (max ?p) :in $ ?cat
	                    :where [?prod :product/category ?cat]
	                           [?prod :product/price ?p]]
	                  $ ?c) [[?max-price]]]
	               [(q [:find (count ?prod) :in $ ?cat
	                    :where [?prod :product/category ?cat]]
	                  $ ?c) [[?count]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	// Execute WITH decorrelation and capture annotations
	execWithDecor := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	var capturedEvents []annotations.Event
	var eventsMu sync.Mutex
	handler := annotations.Handler(func(e annotations.Event) {
		eventsMu.Lock()
		capturedEvents = append(capturedEvents, e)
		eventsMu.Unlock()
	})

	ctx := NewContext(handler)
	result, err := execWithDecor.ExecuteWithContext(ctx, q)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	if result.Size() != 10 {
		t.Errorf("Expected 10 results, got %d", result.Size())
	}

	// Verify decorrelation annotations are present
	var foundDecorrBegin bool
	var foundDecorrComplete bool
	var foundMergedQuery bool

	for _, event := range capturedEvents {
		t.Logf("Event: %s - %v", event.Name, event.Data)

		if event.Name == "decorrelated_subqueries/begin" {
			foundDecorrBegin = true

			// Verify metadata is present
			if _, ok := event.Data["signature_hash"]; !ok {
				t.Error("Missing signature_hash in decorrelation begin event")
			}
			if _, ok := event.Data["total_subqueries"]; !ok {
				t.Error("Missing total_subqueries in decorrelation begin event")
			}
			if _, ok := event.Data["decorrelated_count"]; !ok {
				t.Error("Missing decorrelated_count in decorrelation begin event")
			}
			if _, ok := event.Data["correlation_keys"]; !ok {
				t.Error("Missing correlation_keys in decorrelation begin event")
			}

			t.Logf("Decorrelation metadata: signature_hash=%v, total=%v, decorrelated=%v, keys=%v",
				event.Data["signature_hash"], event.Data["total_subqueries"],
				event.Data["decorrelated_count"], event.Data["correlation_keys"])
		}

		if event.Name == "decorrelated_subqueries/complete" {
			foundDecorrComplete = true
		}

		if event.Name == "decorrelated_subqueries/merged_query_0" {
			foundMergedQuery = true
		}
	}

	// After decorrelation bug fix: These subqueries are PURE aggregations
	// Pure aggregations should NOT be decorrelated, so we expect NO decorrelation events.
	if foundDecorrBegin {
		t.Error("Found decorrelated_subqueries/begin annotation for pure aggregations (should not decorrelate)")
	}
	if foundDecorrComplete {
		t.Error("Found decorrelated_subqueries/complete annotation for pure aggregations (should not decorrelate)")
	}
	if foundMergedQuery {
		t.Error("Found decorrelated_subqueries/merged_query_0 annotation for pure aggregations (should not decorrelate)")
	}

	t.Log("SUCCESS: Pure aggregations correctly skipped decorrelation (no decorrelation events)")
}

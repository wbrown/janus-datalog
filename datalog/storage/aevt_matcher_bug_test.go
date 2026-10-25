package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestAEVTMatcherBug directly tests the storage layer bug
// Calls MatchWithBindings directly to reproduce the issue
func TestAEVTMatcherBug(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create test data: 10 entities, each with 5 attributes
			// Total: 50 datoms
			tx := db.NewTransaction()
			entities := make([]datalog.Identity, 10)

			for i := 0; i < 10; i++ {
				entityID := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
				entities[i] = entityID

				// Each entity has 5 different attributes
				tx.Add(entityID, datalog.NewKeyword(":person/name"), fmt.Sprintf("Person%d", i))
				tx.Add(entityID, datalog.NewKeyword(":person/age"), int64(20+i))
				tx.Add(entityID, datalog.NewKeyword(":person/city"), fmt.Sprintf("City%d", i%3))
				tx.Add(entityID, datalog.NewKeyword(":person/active"), true)
				tx.Add(entityID, datalog.NewKeyword(":person/score"), int64(100*i))
			}

			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit: %v", err)
			}

			// Register the handler on the matcher's options. The index-selection event
			// this test asserts on is emitted inside Match(), so it comes from the
			// matcher's own handler — a decorator around Match() cannot see it.
			var events []annotations.Event
			handler := func(event annotations.Event) {
				events = append(events, event)
			}
			matcher := executor.WrapMatcher(
				NewPatternMatcherWithOptions(db.Store(), executor.ExecutorOptions{Handler: handler}),
				handler,
			).(executor.PatternMatcher)

			// Create pattern: [?e :person/age ?age]
			// This is the problematic pattern when ?e is bound
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":person/age")},
					query.Variable{Name: datalog.NewSymbol("?age")},
				},
			}

			// Create binding relation with 3 entities
			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{datalog.NewSymbol("?e")},
				[]executor.Tuple{
					{entities[0]},
					{entities[5]},
					{entities[9]},
				},
			)

			// Call Match with bindings - this triggers the bug
			result, err := matcher.Match(query.PatternQuery(pattern), executor.Relations{bindingRel})
			if err != nil {
				t.Fatalf("Match failed: %v", err)
			}

			// Consume the iterator to build the cache
			it := result.Iterator()
			var collectedResults []executor.Tuple
			for it.Next() {
				tuple := it.Tuple()
				tupleCopy := make(executor.Tuple, len(tuple))
				copy(tupleCopy, tuple)
				collectedResults = append(collectedResults, tupleCopy)
			}
			it.Close()

			// Verify results
			if len(collectedResults) != 3 {
				t.Errorf("Expected 3 results, got %d", len(collectedResults))
			}

			// Now Size() should work
			if result.Size() != 3 {
				t.Errorf("Expected Size()=3, got %d", result.Size())
			}

			// Check annotations
			t.Logf("Total events: %d", len(events))

			var datomsScanned, scansOpened int
			var indexUsed string

			for i, event := range events {
				t.Logf("Event %d: %s - %+v", i, event.Name, event.Data)

				// The index is the dispatch's choice, carried by the join-strategy
				// event. The per-key seek's completion carries the scan statistics
				// this test reads, and names no index of its own, since it opens
				// one run per key. Matched by strategy, not by name: one event name
				// covers every scan, so the name alone would also take the direct
				// scans the query performs around this one.
				//
				// datoms.scanned, not datoms.resolved: the assertion below is about how
				// much of the index the scans read, and resolution's output is a lower
				// number by the history depth. They coincide here only because the
				// fixture writes each entity once.
				if event.Name == annotations.StorageJoinStrategy {
					if idx, ok := event.Data[annotations.KeyIndex].(IndexType); ok {
						indexUsed = idx.String()
					}
					continue
				}
				if event.Name != annotations.StorageScanComplete {
					continue
				}
				if s, _ := event.Data[annotations.KeyStrategy].(annotations.ScanStrategy); s == annotations.ScanPerKeySeek {
					if scanned, ok := event.Data[annotations.KeyDatomsScanned].(int); ok {
						datomsScanned = scanned
					}
					if opened, ok := event.Data[annotations.KeyScansOpened].(int); ok {
						scansOpened = opened
					}
				}
			}

			t.Logf("Index used: %s", indexUsed)
			t.Logf("Datoms scanned: %d over %d scans", datomsScanned, scansOpened)
			t.Logf("Entities bound: 3")
			t.Logf("Total datoms in DB: 50")

			// Assertions
			// AETV is the correct index for "E bound, A constant" pattern with CRDT semantics
			// AETV has Tx descending, so first entry for each (A, E) is the LWW winner
			if indexUsed != "AETV" {
				t.Errorf("Expected AETV index (CRDT-aware A-primary), got %s", indexUsed)
			}

			// Three keys are sought one by one, each by its (A, E) run, so the
			// intake is the three bound entities' own :person/age datoms: not the
			// attribute's ten, and not the database's fifty.
			if scansOpened != 3 {
				t.Errorf("Expected one scan per bound entity (3), got %d", scansOpened)
			}
			if datomsScanned != 3 {
				t.Errorf("🚨 EXCESSIVE SCANNING: Scanned %d datoms for 3 bound entities (expected 3, one per key)",
					datomsScanned)
			}
		})
	}
}

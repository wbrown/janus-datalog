package tests

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestBadgerRelationInputProfile profiles a relation-input query over badger
// to find bottlenecks, and checks the answer it profiles: each input tuple
// matches one entity, so the grouped max answers that entity's age.
//
// It writes a CPU profile and a duration. The backend is fixed so successive
// profiles are comparable, which is also why it stays off the backend axis —
// three stores would produce three runs overwriting one profile.
// TestRelationInputOverStorage carries the correctness claim across every
// backend.
func TestBadgerRelationInputProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping profiling test in short mode")
	}

	// Create database with test data
	db, err := storage.NewDatabase(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close the database: %v", err)
		}
	}()

	// Create 500 entries, one entity per (name, year, month) with its age.
	names := []string{
		"Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack",
	}

	tx := db.NewTransaction()
	var want [][]interface{}
	idCounter := 0
	for _, name := range names {
		for year := 2020; year <= 2029; year++ {
			for month := 1; month <= 5; month++ {
				entityID := datalog.NewIdentity(fmt.Sprintf("person:%d", idCounter))
				age := int64(25 + idCounter%15)
				require.NoError(t, tx.Add(entityID, datalog.NewKeyword(":name"), name))
				require.NoError(t, tx.Add(entityID, datalog.NewKeyword(":year"), int64(year)))
				require.NoError(t, tx.Add(entityID, datalog.NewKeyword(":month"), int64(month)))
				require.NoError(t, tx.Add(entityID, datalog.NewKeyword(":age"), age))
				want = append(want, []interface{}{name, int64(year), int64(month), age})
				idCounter++
			}
		}
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit test data: %v", err)
	}

	matcher := storage.NewPatternMatcher(db.Store())
	ctx := executor.NewContext()

	queryStr := `[:find ?n ?y ?m (max ?age)
	              :in $ [[?n ?y ?m] ...]
	              :where [?e :name ?n]
	                     [?e :year ?y]
	                     [?e :month ?m]
	                     [?e :age ?age]]`

	parsed, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Create input relation
	var inputTuples []executor.Tuple
	for _, name := range names {
		for year := 2020; year <= 2029; year++ {
			for month := 1; month <= 5; month++ {
				inputTuples = append(inputTuples, executor.Tuple{name, int64(year), int64(month)})
			}
		}
	}
	inputRel := executor.NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y"), datalog.NewSymbol("?m")}, inputTuples)

	for _, mode := range legsFor(fusionOption, aggregationOption) {
		t.Run(mode.name, func(t *testing.T) {
			// Profile the query
			f, err := os.Create("badger_relation_input.prof")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := f.Close(); err != nil {
					t.Errorf("close the profile: %v", err)
				}
			}()

			if err := pprof.StartCPUProfile(f); err != nil {
				t.Fatal(err)
			}
			defer pprof.StopCPUProfile()

			popts := mode.plannerOptions()
			popts.Handler = recordingReach(t, nil)
			exec := executor.NewExecutorWithOptions(matcher, nil, popts)

			start := time.Now()
			tuples, err := executor.CollectTuples(exec.ExecuteWithRelations(ctx, parsed, []executor.Relation{inputRel}))
			duration := time.Since(start)

			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			t.Logf("Relation input over badger: %v (%d results)", duration, len(tuples))
			t.Logf("CPU profile written to badger_relation_input.prof")
			t.Logf("Analyze with: go tool pprof badger_relation_input.prof")
			assert.ElementsMatch(t, want, tuples, "each input tuple answers its one entity's age")
		})
	}
}

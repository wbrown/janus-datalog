package tests

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestAlgebraMatrix_ComparisonBindingOr runs the comparison binding + or
// subquery query with and without the algebra optimizer to isolate whether
// the bridge causes the failure.
func TestAlgebraMatrix_ComparisonBindingOr(t *testing.T) {
	dir, err := os.MkdirTemp("", "algebra-matrix-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	s1 := datalog.NewIdentity("scenario:1")
	s2 := datalog.NewIdentity("scenario:2")
	s3 := datalog.NewIdentity("scenario:3")
	t1 := datalog.NewIdentity("task:1")
	t2 := datalog.NewIdentity("task:2")
	t3 := datalog.NewIdentity("task:3")

	nameAttr := datalog.NewKeyword(":scenario/name")
	taskAttr := datalog.NewKeyword(":scenario/task")

	tx.Add(s1, nameAttr, "Scenario One")
	tx.Add(s1, taskAttr, t1)
	tx.Add(s1, taskAttr, t2)
	tx.Add(s2, nameAttr, "Scenario Two")
	tx.Add(s2, taskAttr, t3)
	tx.Add(s3, nameAttr, "Scenario Three")
	tx.Commit()

	queryStr := `[:find ?scenario ?name ?taskCount ?complete
	              :where [?scenario :scenario/name ?name]
	                     (or [(q [:find (count ?t)
	                              :in $ ?scenario
	                              :where [?scenario :scenario/task ?t]]
	                            $ ?scenario) [[?taskCount]]]
	                         [(ground 0) ?taskCount])
	                     [(> ?taskCount 0) ?complete]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	for _, algebra := range []bool{false, true} {
		name := "without-algebra"
		if algebra {
			name = "with-algebra"
		}
		t.Run(name, func(t *testing.T) {
			opts := storage.DefaultPlannerOptions()
			opts.EnableAlgebraOptimizer = algebra
			matcher := storage.NewPatternMatcher(db.Store())
			exec := executor.NewExecutorWithOptions(matcher, nil, opts)
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			iter := result.Iterator()
			var count int
			for iter.Next() {
				tuple := iter.Tuple()
				t.Logf("  %v", tuple)
				count++
			}
			iter.Close()
			t.Logf("Total: %d tuples", count)
		})
	}
}

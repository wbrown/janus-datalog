package storage

import (
	"math"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// NaN is never storable and is not a datalog value. Every boundary where a
// float enters relational flow rejects it loudly: transaction writes, query
// inputs, and expression outputs. ±Inf remains a value.

func TestWritesRejectNaN(t *testing.T) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	e := datalog.NewIdentity("m:1")
	attr := datalog.NewKeyword(":m/value")
	nan := math.NaN()

	tx := db.NewTransaction()
	defer tx.Rollback()

	if err := tx.Add(e, attr, nan); err == nil {
		t.Error("Add must reject NaN")
	} else if !strings.Contains(err.Error(), "NaN") {
		t.Errorf("Add error should name NaN; got: %v", err)
	}
	if err := tx.Set(e, attr, nan); err == nil {
		t.Error("Set must reject NaN")
	}
	if err := tx.Remove(e, attr, nan); err == nil {
		t.Error("Remove must reject NaN")
	}
	if err := tx.Retract(e, attr, nan); err == nil {
		t.Error("Retract must reject NaN")
	}

	// ±Inf is a value: self-equal, totally ordered, storable.
	if err := tx.Add(e, attr, math.Inf(1)); err != nil {
		t.Errorf("Add must accept +Inf: %v", err)
	}
}

func TestInputsRejectNaN(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)

			e := datalog.NewIdentity("m:1")
			attr := datalog.NewKeyword(":m/value")
			tx := db.NewTransaction()
			tx.Add(e, attr, 1.5)
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			_, err := executor.CollectTuples(db.Query(
				`[:find ?e :in $ ?v :where [?e :m/value ?v]]`,
				math.NaN(),
			))
			if err == nil {
				t.Fatal("expected an error for a NaN input, got none")
			}
			if !strings.Contains(err.Error(), "NaN") {
				t.Errorf("error should name NaN; got: %v", err)
			}
		})
	}
}

func TestExpressionProducingNaNIsError(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)

			// Division by zero already errors in the expression layer (a stricter,
			// pre-existing contract than IEEE), so the NaN-producing shape that can
			// actually occur is arithmetic over Inf-valued data: Inf - Inf is NaN.
			e := datalog.NewIdentity("m:1")
			attr := datalog.NewKeyword(":m/value")
			tx := db.NewTransaction()
			tx.Add(e, attr, math.Inf(1))
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			_, err := executor.CollectTuples(db.Query(
				`[:find ?r :where [?e :m/value ?v] [(- ?v ?v) ?r]]`,
			))
			if err == nil {
				t.Fatal("expected an error for an expression producing NaN, got none")
			}
			if !strings.Contains(err.Error(), "NaN") {
				t.Errorf("error should name NaN; got: %v", err)
			}

			// Inf itself is a value and flows through arithmetic.
			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?r :where [?e :m/value ?v] [(+ ?v 1.0) ?r]]`,
			))
			if err != nil {
				t.Fatalf("+Inf must flow through expressions: %v", err)
			}
			if len(tuples) != 1 {
				t.Fatalf("expected 1 tuple, got %d", len(tuples))
			}
			if r, ok := tuples[0][0].(float64); !ok || !math.IsInf(r, 1) {
				t.Errorf("expected +Inf, got %v", tuples[0][0])
			}
		})
	}
}

// Constant-bindable expressions — scalar :in values hoisted to constant
// bindings — evaluate once on the executor's constant paths rather than
// through the streaming function iterator. The result crosses the same
// boundary into relational flow, so a NaN produced from ±Inf inputs is
// rejected loudly on every shape instead of entering joins and comparisons
// (where the comparator treats NaN as an unreachable domain violation and
// panics).
func TestConstantExpressionProducingNaNIsError(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)

			e := datalog.NewIdentity("m:1")
			attr := datalog.NewKeyword(":m/value")
			tx := db.NewTransaction()
			tx.Add(e, attr, 1.5)
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			posInf := math.Inf(1)

			assertNaNError := func(t *testing.T, queryStr string) {
				t.Helper()
				_, err := executor.CollectTuples(db.Query(queryStr, posInf))
				if err == nil {
					t.Fatal("expected an error for a constant expression producing NaN, got none")
				}
				if !strings.Contains(err.Error(), "NaN") {
					t.Errorf("error should name NaN; got: %v", err)
				}
			}

			t.Run("no patterns", func(t *testing.T) {
				assertNaNError(t, `[:find ?r :in $ ?x :where [(- ?x ?x) ?r]]`)
			})

			t.Run("alongside patterns", func(t *testing.T) {
				assertNaNError(t, `[:find ?v ?r :in $ ?x :where [?e :m/value ?v] [(- ?x ?x) ?r]]`)
			})

			t.Run("feeding a comparison", func(t *testing.T) {
				assertNaNError(t, `[:find ?r :in $ ?x :where [(- ?x ?x) ?r] [(< ?r 1.0)]]`)
			})
		})
	}
}

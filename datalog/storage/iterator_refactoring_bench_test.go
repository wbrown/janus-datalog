//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Benchmark to measure function call overhead for iterator validation logic

// benchValidateDatomWithConstraints validates a datom against temporal and constraint filters
func benchValidateDatomWithConstraints(
	datom *datalog.Datom,
	txID *datalog.ElementID,
	constraints []executor.StorageConstraint,
) bool {
	// Check transaction validity
	if txID != nil && *txID != (datalog.ElementID{}) && txID.Less(datom.Tx) {
		return false
	}

	// Apply constraints
	for _, constraint := range constraints {
		if !constraint.Evaluate(datom) {
			return false
		}
	}
	return true
}

// Inline version (current approach)
func validateDatomInline(
	datom *datalog.Datom,
	txID *datalog.ElementID,
	constraints []executor.StorageConstraint,
) bool {
	// Check transaction validity
	if txID != nil && *txID != (datalog.ElementID{}) && txID.Less(datom.Tx) {
		return false
	}

	// Apply constraints
	for _, constraint := range constraints {
		if !constraint.Evaluate(datom) {
			return false
		}
	}
	return true
}

// Mock constraint for testing
type mockConstraint struct {
	shouldPass bool
}

func (c *mockConstraint) Evaluate(d *datalog.Datom) bool {
	return c.shouldPass
}

func (c *mockConstraint) String() string {
	return "mock-constraint"
}

func BenchmarkIteratorValidation(b *testing.B) {
	// Setup test data
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")
	datom := &datalog.Datom{
		E:  alice,
		A:  nameAttr,
		V:  "Alice",
		Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	asOfTx := datalog.ElementID{Lamport: 50, ReplicaID: 1}
	scenarios := []struct {
		name           string
		txID           *datalog.ElementID
		constraintCnt  int
		constraintPass bool
	}{
		{"no_tx_check_no_constraints", nil, 0, true},
		{"with_tx_check_no_constraints", &asOfTx, 0, true},
		{"no_tx_check_1_constraint", nil, 1, true},
		{"no_tx_check_3_constraints", nil, 3, true},
		{"with_tx_check_3_constraints", &asOfTx, 3, true},
		{"with_tx_check_5_constraints", &asOfTx, 5, true},
	}

	for _, scenario := range scenarios {
		// Build constraints
		constraints := make([]executor.StorageConstraint, scenario.constraintCnt)
		for i := 0; i < scenario.constraintCnt; i++ {
			constraints[i] = &mockConstraint{shouldPass: scenario.constraintPass}
		}

		b.Run(scenario.name+"/inline", func(b *testing.B) {
			passed := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if validateDatomInline(datom, scenario.txID, constraints) {
					passed++
				}
			}
			b.ReportMetric(float64(passed)/float64(b.N), "pass_rate")
		})

		b.Run(scenario.name+"/shared", func(b *testing.B) {
			passed := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if benchValidateDatomWithConstraints(datom, scenario.txID, constraints) {
					passed++
				}
			}
			b.ReportMetric(float64(passed)/float64(b.N), "pass_rate")
		})
	}
}

// Benchmark realistic iterator loop
func BenchmarkIteratorLoop(b *testing.B) {
	// Create test datoms
	datoms := make([]*datalog.Datom, 1000)
	for i := 0; i < 1000; i++ {
		datoms[i] = &datalog.Datom{
			E:  datalog.NewIdentity("user:alice"),
			A:  datalog.NewKeyword(":user/name"),
			V:  "Alice",
			Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1},
		}
	}

	constraints := []executor.StorageConstraint{
		&mockConstraint{shouldPass: true},
		&mockConstraint{shouldPass: true},
		&mockConstraint{shouldPass: true},
	}

	b.Run("inline", func(b *testing.B) {
		matched := 0
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for _, datom := range datoms {
				// Inline validation (current approach)
				if 500 > 0 && datom.Tx.Lamport > 500 {
					continue
				}

				satisfiesAll := true
				for _, constraint := range constraints {
					if !constraint.Evaluate(datom) {
						satisfiesAll = false
						break
					}
				}

				if satisfiesAll {
					matched++
				}
			}
		}
		b.ReportMetric(float64(matched)/float64(b.N), "matched/iter")
	})

	b.Run("shared", func(b *testing.B) {
		matched := 0
		benchTxID := datalog.ElementID{Lamport: 500, ReplicaID: 1}
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for _, datom := range datoms {
				// Shared function approach
				if benchValidateDatomWithConstraints(datom, &benchTxID, constraints) {
					matched++
				}
			}
		}
		b.ReportMetric(float64(matched)/float64(b.N), "matched/iter")
	})
}

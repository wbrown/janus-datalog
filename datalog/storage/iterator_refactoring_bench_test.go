package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Benchmark to measure function call overhead for iterator validation logic

// validateDatomWithConstraintsHelper is the proposed helper function
func validateDatomWithConstraintsHelper(
	datom *datalog.Datom,
	txID uint64,
	constraints []executor.StorageConstraint,
) bool {
	// Check transaction validity
	if txID > 0 && datom.Tx > txID {
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
	txID uint64,
	constraints []executor.StorageConstraint,
) bool {
	// Check transaction validity
	if txID > 0 && datom.Tx > txID {
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
		Tx: 100,
	}

	scenarios := []struct {
		name           string
		txID           uint64
		constraintCnt  int
		constraintPass bool
	}{
		{"no_tx_check_no_constraints", 0, 0, true},
		{"with_tx_check_no_constraints", 50, 0, true},
		{"no_tx_check_1_constraint", 0, 1, true},
		{"no_tx_check_3_constraints", 0, 3, true},
		{"with_tx_check_3_constraints", 50, 3, true},
		{"with_tx_check_5_constraints", 50, 5, true},
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

		b.Run(scenario.name+"/helper", func(b *testing.B) {
			passed := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if validateDatomWithConstraintsHelper(datom, scenario.txID, constraints) {
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
			Tx: uint64(i),
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
				if 500 > 0 && datom.Tx > 500 {
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

	b.Run("helper", func(b *testing.B) {
		matched := 0
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for _, datom := range datoms {
				// Helper function approach
				if validateDatomWithConstraintsHelper(datom, 500, constraints) {
					matched++
				}
			}
		}
		b.ReportMetric(float64(matched)/float64(b.N), "matched/iter")
	})
}

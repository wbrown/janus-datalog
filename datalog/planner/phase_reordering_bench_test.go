package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
)

// BenchmarkPhaseReordering_CrossProduct benchmarks the cross-product scenario
func BenchmarkPhaseReordering_CrossProduct(b *testing.B) {
	queryStr := `[:find ?name ?price
                  :where [?person :person/name ?name]
                         [?product :product/price ?price]
                         [?person :bought ?product]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("failed to parse query: %v", err)
	}

	b.Run("without_reordering", func(b *testing.B) {
		planner := NewPlanner(nil, PlannerOptions{
			EnableDynamicReordering: false,
			MaxPhases:               10,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := planner.Plan(q)
			if err != nil {
				b.Fatalf("planning failed: %v", err)
			}
		}
	})

	b.Run("with_reordering", func(b *testing.B) {
		planner := NewPlanner(nil, PlannerOptions{
			EnableDynamicReordering: true,
			MaxPhases:               10,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := planner.Plan(q)
			if err != nil {
				b.Fatalf("planning failed: %v", err)
			}
		}
	})
}

// BenchmarkPhaseReordering_MultiBranch benchmarks a multi-branch join scenario
func BenchmarkPhaseReordering_MultiBranch(b *testing.B) {
	queryStr := `[:find ?x ?y ?z
                  :where [?a :attr1 ?x]
                         [?b :attr2 ?y]
                         [?c :attr3 ?z]
                         [?a :connects ?b]
                         [?b :connects ?c]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("failed to parse query: %v", err)
	}

	b.Run("without_reordering", func(b *testing.B) {
		planner := NewPlanner(nil, PlannerOptions{
			EnableDynamicReordering: false,
			MaxPhases:               10,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := planner.Plan(q)
			if err != nil {
				b.Fatalf("planning failed: %v", err)
			}
		}
	})

	b.Run("with_reordering", func(b *testing.B) {
		planner := NewPlanner(nil, PlannerOptions{
			EnableDynamicReordering: true,
			MaxPhases:               10,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := planner.Plan(q)
			if err != nil {
				b.Fatalf("planning failed: %v", err)
			}
		}
	})
}

// BenchmarkPhaseReordering_AlreadyOptimal benchmarks an already optimal query
func BenchmarkPhaseReordering_AlreadyOptimal(b *testing.B) {
	queryStr := `[:find ?name ?age
                  :where [?e :type :person]
                         [?e :person/name ?name]
                         [?e :person/age ?age]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("failed to parse query: %v", err)
	}

	b.Run("without_reordering", func(b *testing.B) {
		planner := NewPlanner(nil, PlannerOptions{
			EnableDynamicReordering: false,
			MaxPhases:               10,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := planner.Plan(q)
			if err != nil {
				b.Fatalf("planning failed: %v", err)
			}
		}
	})

	b.Run("with_reordering", func(b *testing.B) {
		planner := NewPlanner(nil, PlannerOptions{
			EnableDynamicReordering: true,
			MaxPhases:               10,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := planner.Plan(q)
			if err != nil {
				b.Fatalf("planning failed: %v", err)
			}
		}
	})
}

// BenchmarkPhaseReordering_Complex benchmarks a complex query with many patterns
func BenchmarkPhaseReordering_Complex(b *testing.B) {
	queryStr := `[:find ?name ?price ?quantity ?date
                  :where [?person :person/name ?name]
                         [?person :person/purchases ?purchase]
                         [?purchase :purchase/product ?product]
                         [?product :product/price ?price]
                         [?purchase :purchase/quantity ?quantity]
                         [?purchase :purchase/date ?date]
                         [?product :product/category :electronics]
                         [(> ?price 100)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("failed to parse query: %v", err)
	}

	b.Run("without_reordering", func(b *testing.B) {
		planner := NewPlanner(nil, PlannerOptions{
			EnableDynamicReordering: false,
			MaxPhases:               10,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := planner.Plan(q)
			if err != nil {
				b.Fatalf("planning failed: %v", err)
			}
		}
	})

	b.Run("with_reordering", func(b *testing.B) {
		planner := NewPlanner(nil, PlannerOptions{
			EnableDynamicReordering: true,
			MaxPhases:               10,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := planner.Plan(q)
			if err != nil {
				b.Fatalf("planning failed: %v", err)
			}
		}
	})
}

package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Benchmark constraint evaluation performance
func BenchmarkConstraintEvaluation(b *testing.B) {
	// Create test datoms with different value types
	datoms := []*datalog.Datom{
		{
			E:  datalog.NewIdentity("entity1"),
			A:  datalog.NewKeyword(":person/age"),
			V:  int64(25),
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("entity2"),
			A:  datalog.NewKeyword(":person/name"),
			V:  "Alice",
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("entity3"),
			A:  datalog.NewKeyword(":person/active"),
			V:  true,
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("entity4"),
			A:  datalog.NewKeyword(":person/created"),
			V:  time.Now(),
			Tx: 1,
		},
	}

	// Create constraints for each type
	constraints := []executor.StorageConstraint{
		&equalityConstraint{position: 2, value: int64(25)},
		&equalityConstraint{position: 2, value: "Alice"},
		&equalityConstraint{position: 2, value: true},
	}

	b.Run("Int64Equality", func(b *testing.B) {
		constraint := constraints[0]
		datom := datoms[0]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = constraint.Evaluate(datom)
		}
	})

	b.Run("StringEquality", func(b *testing.B) {
		constraint := constraints[1]
		datom := datoms[1]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = constraint.Evaluate(datom)
		}
	})

	b.Run("BoolEquality", func(b *testing.B) {
		constraint := constraints[2]
		datom := datoms[2]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = constraint.Evaluate(datom)
		}
	})

	b.Run("TimeEqualityWithValuesEqual", func(b *testing.B) {
		// This will use the ValuesEqual fallback
		constraint := &equalityConstraint{position: 2, value: datoms[3].V}
		datom := datoms[3]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = constraint.Evaluate(datom)
		}
	})

	b.Run("MixedTypes1000Datoms", func(b *testing.B) {
		// Simulate scanning 1000 datoms with constraint
		constraint := &equalityConstraint{position: 2, value: int64(25)}

		// Create 1000 datoms with varying ages
		manyDatoms := make([]*datalog.Datom, 1000)
		for i := 0; i < 1000; i++ {
			manyDatoms[i] = &datalog.Datom{
				E:  datalog.NewIdentity("person" + string(rune(i))),
				A:  datalog.NewKeyword(":person/age"),
				V:  int64(i%100 + 1),
				Tx: uint64(i),
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matches := 0
			for _, d := range manyDatoms {
				if constraint.Evaluate(d) {
					matches++
				}
			}
			if matches != 10 {
				b.Fatalf("Expected 10 matches, got %d", matches)
			}
		}
	})

	// Compare with unoptimized versions
	b.Run("UnoptimizedInt64", func(b *testing.B) {
		constraint := &unoptimizedEqualityConstraint{position: 2, value: int64(25)}
		datom := datoms[0]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = constraint.Evaluate(datom)
		}
	})

	b.Run("UnoptimizedString", func(b *testing.B) {
		constraint := &unoptimizedEqualityConstraint{position: 2, value: "Alice"}
		datom := datoms[1]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = constraint.Evaluate(datom)
		}
	})

	b.Run("Unoptimized1000Datoms", func(b *testing.B) {
		// Simulate scanning 1000 datoms with unoptimized constraint
		constraint := &unoptimizedEqualityConstraint{position: 2, value: int64(25)}

		// Create 1000 datoms with varying ages
		manyDatoms := make([]*datalog.Datom, 1000)
		for i := 0; i < 1000; i++ {
			manyDatoms[i] = &datalog.Datom{
				E:  datalog.NewIdentity("person" + string(rune(i))),
				A:  datalog.NewKeyword(":person/age"),
				V:  int64(i%100 + 1),
				Tx: uint64(i),
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matches := 0
			for _, d := range manyDatoms {
				if constraint.Evaluate(d) {
					matches++
				}
			}
			if matches != 10 {
				b.Fatalf("Expected 10 matches, got %d", matches)
			}
		}
	})
}

// equalityConstraint implementation (copied from executor package for benchmarking)
type equalityConstraint struct {
	position int
	value    interface{}
}

func (c *equalityConstraint) String() string {
	pos := []string{"E", "A", "V", "T"}[c.position]
	return fmt.Sprintf("%s = %v", pos, c.value)
}

func (c *equalityConstraint) Evaluate(datom *datalog.Datom) bool {
	switch c.position {
	case 0: // Entity
		if id, ok := c.value.(datalog.Identity); ok {
			return datom.E.Equal(id)
		}
	case 1: // Attribute
		if kw, ok := c.value.(datalog.Keyword); ok {
			return datom.A.String() == kw.String()
		}
	case 2: // Value
		// Fast path for common integer comparisons
		if iv, ok := c.value.(int64); ok {
			dv, ok := datom.V.(int64)
			return ok && dv == iv
		}
		// Fast path for string comparisons
		if sv, ok := c.value.(string); ok {
			dv, ok := datom.V.(string)
			return ok && dv == sv
		}
		// Fast path for bool comparisons
		if bv, ok := c.value.(bool); ok {
			dv, ok := datom.V.(bool)
			return ok && dv == bv
		}
		return datalog.ValuesEqual(datom.V, c.value)
	case 3: // Transaction
		if tx, ok := c.value.(uint64); ok {
			return datom.Tx == tx
		}
	}
	return false
}

// Unoptimized version for comparison
type unoptimizedEqualityConstraint struct {
	position int
	value    interface{}
}

func (c *unoptimizedEqualityConstraint) String() string {
	pos := []string{"E", "A", "V", "T"}[c.position]
	return fmt.Sprintf("%s = %v (unoptimized)", pos, c.value)
}

func (c *unoptimizedEqualityConstraint) Evaluate(datom *datalog.Datom) bool {
	switch c.position {
	case 0: // Entity
		if id, ok := c.value.(datalog.Identity); ok {
			return datom.E.Equal(id)
		}
	case 1: // Attribute
		if kw, ok := c.value.(datalog.Keyword); ok {
			return datom.A.String() == kw.String()
		}
	case 2: // Value
		// Always use ValuesEqual - no fast paths
		return datalog.ValuesEqual(datom.V, c.value)
	case 3: // Transaction
		if tx, ok := c.value.(uint64); ok {
			return datom.Tx == tx
		}
	}
	return false
}

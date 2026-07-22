package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func BenchmarkSubqueryInputCombinationExtraction(b *testing.B) {
	x := datalog.NewSymbol("?x")
	y := datalog.NewSymbol("?y")
	leftIdentity := datalog.NewSymbol("?left-identity")
	rightIdentity := datalog.NewSymbol("?right-identity")
	for _, shape := range []struct {
		leftRows    int
		rightRows   int
		leftValues  int
		rightValues int
	}{
		{leftRows: 100, rightRows: 100, leftValues: 10, rightValues: 10},
		{leftRows: 1_000, rightRows: 100, leftValues: 100, rightValues: 10},
	} {
		name := fmt.Sprintf("product=%d", shape.leftRows*shape.rightRows)
		b.Run(name, func(b *testing.B) {
			leftTuples := make([]Tuple, shape.leftRows)
			for i := range leftTuples {
				leftTuples[i] = Tuple{int64(i % shape.leftValues), int64(i)}
			}
			rightTuples := make([]Tuple, shape.rightRows)
			for i := range rightTuples {
				rightTuples[i] = Tuple{int64(i % shape.rightValues), int64(i)}
			}
			left := NewMaterializedRelation(
				[]query.Symbol{x, leftIdentity},
				leftTuples,
			)
			right := NewMaterializedRelation(
				[]query.Symbol{y, rightIdentity},
				rightTuples,
			)
			for _, eager := range []bool{true, false} {
				mode := "stream-product"
				if eager {
					mode = "eager-product"
				}
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					dataSymbols := filterSourceSymbols([]query.Symbol{x, y})
					for b.Loop() {
						var input Relation = NewProductRelation([]Relation{left, right})
						if eager {
							input = input.Materialize()
						}
						// Unique input combinations are the input relation
						// projected onto the data symbols — Project's set
						// semantics is the dedup.
						combinations, err := input.Project(dataSymbols)
						if err != nil {
							b.Fatal(err)
						}
						got := 0
						it := combinations.Iterator()
						for it.Next() {
							got++
						}
						iterErr := it.Error()
						it.Close()
						if iterErr != nil {
							b.Fatal(iterErr)
						}
						want := shape.leftValues * shape.rightValues
						if got != want {
							b.Fatalf("got %d combinations, want %d", got, want)
						}
					}
				})
			}
		})
	}
}

package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func BenchmarkStorageHashJoinCompiledMatching(b *testing.B) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:         b.TempDir(),
		DisableCache: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	groupAttr := datalog.NewKeyword(":item/group")
	groups := make([]datalog.Identity, 100)
	for i := range groups {
		groups[i] = datalog.NewIdentity(fmt.Sprintf("group-%d", i))
	}
	tx := db.NewTransaction()
	for i := 0; i < 10_000; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("item-%d", i))
		if err := tx.Add(entity, groupAttr, groups[i%len(groups)]); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	groupSymbol := datalog.NewSymbol("?group")
	noiseSymbol := datalog.NewSymbol("?noise")
	entitySymbol := datalog.NewSymbol("?e")
	bindingTuples := make([]executor.Tuple, 50)
	for i := range bindingTuples {
		bindingTuples[i] = executor.Tuple{int64(i), groups[i]}
	}
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{noiseSymbol, groupSymbol},
		bindingTuples,
	)
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entitySymbol},
		query.Constant{Value: groupAttr},
		query.Variable{Name: groupSymbol},
	}}
	resultSymbols := []query.Symbol{entitySymbol, groupSymbol}
	matcher := db.Matcher().(*PatternMatcher)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := matcher.matchWithHashJoin(
			pattern,
			bindingRel,
			resultSymbols,
			2,
			AVET,
			nil,
		)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		it := result.Iterator()
		for it.Next() {
			count++
		}
		if err := it.Error(); err != nil {
			b.Fatal(err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
		if count != 5_000 {
			b.Fatalf("got %d matches, want 5000", count)
		}
	}
}

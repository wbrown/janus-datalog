package storage

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// answerBytes is what n answers of a cache match weigh: one tuple of two
// values per binding and the slice element holding it. A key map over the
// answers holds an entry per answer beside them, so a match allocating within
// answerBytes plus the relation built none.
func answerBytes(n int) int64 {
	return int64(n) * int64(2*unsafe.Sizeof(any(nil))+unsafe.Sizeof(executor.Tuple(nil)))
}

// besideAnswers is the allowance over answerBytes for the relation, the
// binding iterator, and the size-class rounding of the answers' slice.
const besideAnswers = 8 << 10

// bytesPerMatch is the bytes one match allocates, measured over a run of
// them.
func bytesPerMatch(match func()) int64 {
	return testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			match()
		}
	}).AllocedBytesPerOp()
}

// A cache match over bindings carrying the entity alone, under a
// cardinality-one attribute, allocates its answers, one tuple each, their
// slice and the relation, and nothing of the answers' weight beside them: the
// bindings pass through unprojected and unselected, the result slice is
// sized to them, and no key map is built
// (docs/proposals/BINDING_MATCH_PROOF.md, T5).
func TestCacheMatchAllocatesItsAnswersAlone(t *testing.T) {
	const n = 1000
	rank := datalog.NewKeyword(":bindery/rank")
	db := createOptimizerModeDB(t, optimizerModes[0], DatabaseOptions{})
	tx := db.NewTransaction()
	tuples := make([]executor.Tuple, n)
	for i := range tuples {
		quire := datalog.NewIdentity(fmt.Sprintf("bindery:quire%d", i))
		require.NoError(t, tx.Add(quire, rank, int64(i)))
		tuples[i] = executor.Tuple{quire}
	}
	_, err := tx.Commit()
	require.NoError(t, err)

	matcher := db.matcherWithExecOptions(db.effectivePlannerOptions()).(*PatternMatcher)
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	q := query.PatternQuery(&query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e}, query.Constant{Value: rank}, query.Variable{Name: v},
	}})
	bindings := executor.NewMaterializedRelation([]query.Symbol{e}, tuples)
	match := func() int {
		rel, err := matcher.MatchWithConstraints(q, executor.Relations{bindings}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return rel.Size()
	}
	require.Equal(t, n, match(), "one answer per binding, with the cache warm after it")

	perMatch := bytesPerMatch(func() { match() })
	assert.LessOrEqual(t, perMatch, answerBytes(n)+besideAnswers, "bytes per match of %d bindings: %d, against answers of %d", n, perMatch, answerBytes(n))
}

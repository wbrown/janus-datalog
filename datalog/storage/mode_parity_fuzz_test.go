package storage

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/internal/queryfuzz"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// modeParityEntities writes the dataset every drawn query runs over: three
// entities carrying :a as 1, 2 and 3, :b as a ref from the first to the
// second and from the third to the first, and :c as 0, 1 and 2, the third
// carrying 1 beside its 2.
func modeParityEntities(t *testing.T, d *Database) {
	t.Helper()
	ids := []datalog.Identity{datalog.NewIdentity("parity:p"), datalog.NewIdentity("parity:q"), datalog.NewIdentity("parity:s")}
	tx := d.NewTransaction()
	for i, id := range ids {
		require.NoError(t, tx.Add(id, kw(":a"), int64(i+1)))
		if i != 1 {
			require.NoError(t, tx.Add(id, kw(":b"), ids[(i+1)%3]))
		}
		require.NoError(t, tx.Add(id, kw(":c"), int64(i)))
		if i == 2 {
			require.NoError(t, tx.Add(id, kw(":c"), int64(1)))
		}
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

// modeParityInputs are the Go values a drawn query's :in takes: the rule
// set's text for %, 1 for a scalar, [1 2] for a collection, a tuple of ones
// for a tuple input, and two tuples of k+j for a relation input.
func modeParityInputs(q *query.Query) []interface{} {
	var inputs []interface{}
	for _, in := range q.In {
		switch in := in.(type) {
		case query.RulesInput:
			inputs = append(inputs, queryfuzz.Rules)
		case query.ScalarInput:
			inputs = append(inputs, int64(1))
		case query.CollectionInput:
			inputs = append(inputs, []interface{}{int64(1), int64(2)})
		case query.TupleInput:
			tuple := make([]interface{}, len(in.Symbols))
			for j := range in.Symbols {
				tuple[j] = int64(1)
			}
			inputs = append(inputs, tuple)
		case query.RelationInput:
			var relation []interface{}
			for k := 1; k <= 2; k++ {
				tuple := make([]interface{}, len(in.Symbols))
				for j := range in.Symbols {
					tuple[j] = int64(k + j)
				}
				relation = append(relation, tuple)
			}
			inputs = append(inputs, relation)
		}
	}
	return inputs
}

// modeParityAnswer runs text on d and renders its answer as its tuples'
// renderings in sorted order, or returns the refusal.
func modeParityAnswer(d *Database, text string, inputs []interface{}) (string, error) {
	tuples, err := executor.CollectTuples(d.Query(text, inputs...))
	if err != nil {
		return "", err
	}
	rendered := make([]string, len(tuples))
	for i, tuple := range tuples {
		rendered[i] = fmt.Sprintf("%v", tuple)
	}
	sort.Strings(rendered)
	return strings.Join(rendered, " | "), nil
}

// The two optimizer paths answer every query queryfuzz draws alike on one
// dataset, on every backend: both answer the same tuples, or both refuse. A
// refusal on one path beside an answer on the other, and two answers that
// differ, are reported with the query, each class with its count and its
// first instances. The texts the parser refuses are the parser's and run on
// neither path.
func TestQueriesAnswerAlikeUnderBothOptimizerModes(t *testing.T) {
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			dbs := map[bool]*Database{}
			for _, algebra := range []bool{true, false} {
				mode := optimizerMode{name: backend.Name, algebra: algebra, backend: backend}
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				modeParityEntities(t, d)
				dbs[algebra] = d
			}
			var oneRefusal, differing []string
			ran, answered, refused := 0, 0, 0
			for _, seed := range queryfuzz.Seeds {
				draw := queryfuzz.NewDraw(seed)
				for i := 0; i < queryfuzz.PerSeed; i++ {
					text := draw.QueryText()
					q, err := parser.ParseQuery(text)
					if err != nil {
						continue
					}
					ran++
					inputs := modeParityInputs(q)
					on, errOn := modeParityAnswer(dbs[true], text, inputs)
					off, errOff := modeParityAnswer(dbs[false], text, inputs)
					switch {
					case errOn != nil && errOff != nil:
						refused++
					case errOn == nil && errOff == nil && on == off:
						answered++
					case errOn == nil && errOff == nil:
						differing = append(differing, fmt.Sprintf("%s\n    algebra on:  %s\n    algebra off: %s", text, on, off))
					default:
						oneRefusal = append(oneRefusal, fmt.Sprintf("%s\n    algebra on:  %v %s\n    algebra off: %v %s", text, errOn, on, errOff, off))
					}
				}
			}
			t.Logf("%d texts ran: %d answered alike, %d refused on both paths, %d refused on one, %d answered differently", ran, answered, refused, len(oneRefusal), len(differing))
			require.Greater(t, ran, 0, "the parser accepted no drawn query")
			require.Greater(t, answered, 0, "no drawn query answered on both paths, so the comparison compared nothing")
			report := func(label string, failures []string) {
				if len(failures) == 0 {
					return
				}
				shown := failures
				if len(shown) > 3 {
					shown = shown[:3]
				}
				t.Errorf("%s: %d queries\n  %s", label, len(failures), strings.Join(shown, "\n  "))
			}
			report("one path refuses where the other answers", oneRefusal)
			report("the paths answer differently", differing)
		})
	}
}

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

// Every leg of the optimizer matrix, the algebra optimizer and scan sharing
// each on and off, answers every query queryfuzz draws alike on one dataset,
// on every backend: each leg answers the tuples the leg with both off
// answers, or refuses where it refuses. A refusal on one leg beside an answer
// on the other, and two answers that differ, are reported with the query and
// both legs, each class with its count and its first instances. The texts the
// parser refuses are the parser's and run on no leg.
func TestQueriesAnswerAlikeUnderBothOptimizerModes(t *testing.T) {
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			modes := buildOptimizerModes([]Backend{backend})
			dbs := make([]*Database, len(modes))
			reference := -1
			for i, mode := range modes {
				dbs[i] = createOptimizerModeDB(t, mode, DatabaseOptions{})
				modeParityEntities(t, dbs[i])
				if !mode.algebra && !mode.sharing {
					reference = i
				}
			}
			require.GreaterOrEqual(t, reference, 0, "the matrix holds the leg with the algebra optimizer and scan sharing off")
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
					want, errWant := modeParityAnswer(dbs[reference], text, inputs)
					for leg, mode := range modes {
						if leg == reference {
							continue
						}
						got, errGot := modeParityAnswer(dbs[leg], text, inputs)
						switch {
						case errGot != nil && errWant != nil:
							refused++
						case errGot == nil && errWant == nil && got == want:
							answered++
						case errGot == nil && errWant == nil:
							differing = append(differing, fmt.Sprintf("%s\n    %s: %s\n    %s: %s", text, mode.name, got, modes[reference].name, want))
						default:
							oneRefusal = append(oneRefusal, fmt.Sprintf("%s\n    %s: %v %s\n    %s: %v %s", text, mode.name, errGot, got, modes[reference].name, errWant, want))
						}
					}
				}
			}
			t.Logf("%d texts ran: %d comparisons answered alike, %d refused on both legs, %d refused on one, %d answered differently", ran, answered, refused, len(oneRefusal), len(differing))
			require.Greater(t, ran, 0, "the parser accepted no drawn query")
			require.Greater(t, answered, 0, "no drawn query answered on two legs, so the comparison compared nothing")
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

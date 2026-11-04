package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// reviewMemoryValuePairs is one pair of distinct values per Go kind a caller
// can hand a fact: the value domain's kinds, the integer widths
// datalog.NormalizeValue widens, and kinds outside the domain.
var reviewMemoryValuePairs = []struct {
	name string
	pair [2]interface{}
}{
	{"int64", [2]interface{}{int64(1), int64(2)}},
	{"int", [2]interface{}{1, 2}},
	{"int8", [2]interface{}{int8(1), int8(2)}},
	{"int16", [2]interface{}{int16(1), int16(2)}},
	{"int32", [2]interface{}{int32(1), int32(2)}},
	{"uint", [2]interface{}{uint(1), uint(2)}},
	{"uint8", [2]interface{}{uint8(1), uint8(2)}},
	{"uint16", [2]interface{}{uint16(1), uint16(2)}},
	{"uint32", [2]interface{}{uint32(1), uint32(2)}},
	{"uint64", [2]interface{}{uint64(1), uint64(2)}},
	{"float64", [2]interface{}{1.5, 2.5}},
	{"float32", [2]interface{}{float32(1.5), float32(2.5)}},
	{"bool", [2]interface{}{true, false}},
	{"string", [2]interface{}{"x", "y"}},
	{"bytes", [2]interface{}{[]byte("x"), []byte("y")}},
	{"instant", [2]interface{}{time.Unix(1, 0).UTC(), time.Unix(2, 0).UTC()}},
	{"nil and a string", [2]interface{}{nil, "y"}},
	{"a string and nil", [2]interface{}{"x", nil}},
}

// OpenFacts writes its facts through a transaction, so for each pair of
// values one entity holds at one attribute it answers as the transaction
// does: where Transaction.Add takes both values, OpenFacts holds both, and
// where Add refuses one, OpenFacts answers an error. Neither panics.
func TestReviewMemory_OpenFactsAnswersEachValueAsATransactionDoes(t *testing.T) {
	one := datalog.NewIdentity("review-memory:one")
	v := kw(":review-memory/v")
	for _, c := range reviewMemoryValuePairs {
		t.Run(c.name, func(t *testing.T) {
			reference, err := OpenFacts(nil, DatabaseOptions{})
			require.NoError(t, err)
			defer func() { require.NoError(t, reference.Close()) }()
			tx := reference.NewTransaction()
			var addErr error
			require.NotPanics(t, func() {
				for _, value := range c.pair {
					if addErr = tx.Add(one, v, value); addErr != nil {
						return
					}
				}
			}, "Transaction.Add")
			require.NoError(t, tx.Rollback())

			facts := []datalog.Datom{{E: one, A: v, V: c.pair[0]}, {E: one, A: v, V: c.pair[1]}}
			var d *Database
			var openErr error
			require.NotPanics(t, func() { d, openErr = OpenFacts(facts, DatabaseOptions{}) }, "OpenFacts")
			if addErr != nil {
				require.Error(t, openErr, "Transaction.Add refuses: %v", addErr)
				return
			}
			require.NoError(t, openErr)
			defer func() { require.NoError(t, d.Close()) }()
			got, err := executor.CollectTuples(d.Query(`[:find ?v :where [_ :review-memory/v ?v]]`))
			require.NoError(t, err)
			require.Len(t, got, 2, "both values hold")
		})
	}
}

// A database refusing unknown attributes requires a schema, and opening one
// with none is an error (CLAUDE.md, "Schema as facts"). OpenFacts given no
// schema and the option refuses alike whether or not one entity holds two
// values of an attribute.
func TestReviewMemory_OpenFactsRefusingWithNoSchemaIsAnErrorWhateverTheFactsHold(t *testing.T) {
	one, two := datalog.NewIdentity("review-memory:one"), datalog.NewIdentity("review-memory:two")
	name, tag := kw(":review-memory/name"), kw(":review-memory/tag")
	cases := []struct {
		name  string
		facts []datalog.Datom
	}{
		{"every entity holds one value", []datalog.Datom{{E: one, A: name, V: "one"}, {E: one, A: tag, V: "red"}, {E: two, A: tag, V: "blue"}}},
		{"one entity holds two values", []datalog.Datom{{E: one, A: name, V: "one"}, {E: one, A: tag, V: "red"}, {E: one, A: tag, V: "blue"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, err := OpenFacts(c.facts, DatabaseOptions{RefuseUnknownAttributes: true})
			if err == nil {
				require.NoError(t, d.Close())
			}
			require.Error(t, err)
		})
	}
}

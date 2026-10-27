package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A pattern whose positions are bound, from an input or written as literals,
// answers the tuples the unbound pattern answers that agree with the binding
// at the bound positions. The route a binding selects is the engine's choice
// and never the answer's, so the reference is the unbound pattern's answer
// filtered in Go, in the view the query reads. The store carries x and y with
// a long at :r and t with the vector [1 2] at :tags, declared a vector of
// longs: a vector at the value position names t's collection under :tags,
// nothing under :r, and under History, where every datom is read as written,
// nothing at all.
func TestABoundPatternAnswersWhatTheUnboundPatternAnswersUnderTheBinding(t *testing.T) {
	x, y, tagged := datalog.NewIdentity("x"), datalog.NewIdentity("y"), datalog.NewIdentity("t")
	vector := []interface{}{int64(1), int64(2)}
	const e, a, v = 0, 1, 2
	cases := []boundPatternCase{
		{"the vector bound, the attribute open", []int{e}, bindingTuples{{{v, vector}}}, false},
		{"the vector bound under the scalar attribute", []int{e}, bindingTuples{{{a, kw(":r")}, {v, vector}}}, false},
		{"the vector bound under the vector attribute", []int{e}, bindingTuples{{{a, kw(":tags")}, {v, vector}}}, false},
		{"the vector bound on an entity carrying a scalar", []int{a}, bindingTuples{{{e, x}, {v, vector}}}, false},
		{"the vector bound on the entity carrying it", []int{a}, bindingTuples{{{e, tagged}, {v, vector}}}, false},
		{"the entity carrying the vector bound, the value open", []int{v}, bindingTuples{{{e, tagged}}}, false},
		{"a relation binding the vector beside a scalar", []int{e}, bindingTuples{{{a, kw(":r")}, {v, vector}}, {{a, kw(":r")}, {v, int64(1)}}}, false},
		{"a relation binding the vector under each attribute", []int{e}, bindingTuples{{{a, kw(":r")}, {v, vector}}, {{a, kw(":tags")}, {v, vector}}}, false},
		{"a stored scalar bound", []int{e}, bindingTuples{{{v, int64(2)}}}, false},
		{"the vector written, the attribute open", []int{e}, bindingTuples{{{v, vector}}}, true},
		{"the vector written under the scalar attribute", []int{e}, bindingTuples{{{a, kw(":r")}, {v, vector}}}, true},
		{"the vector written under the vector attribute", []int{e}, bindingTuples{{{a, kw(":tags")}, {v, vector}}}, true},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s := schema.NewBuilder().Attribute(":tags").Type(schema.TypeLong).Vector().Add().MustBuild()
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(x, kw(":r"), int64(1)))
			require.NoError(t, tx.Add(y, kw(":r"), int64(2)))
			require.NoError(t, tx.Set(tagged, kw(":tags"), []int64{1, 2}))
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, view := range []struct {
				name string
				db   *Database
			}{{"current", d}, {"history", d.History()}} {
				t.Run(view.name, func(t *testing.T) {
					unbound, err := executor.CollectTuples(view.db.Query(`[:find ?e ?a ?v :where [?e ?a ?v]]`))
					require.NoError(t, err)
					for _, c := range cases {
						t.Run(c.name, func(t *testing.T) {
							text, inputs := c.query(t)
							got, err := executor.CollectTuples(view.db.Query(text, inputs...))
							require.NoError(t, err, text)
							want := c.reference(unbound)
							require.Truef(t, tuplesAgree(got, want), "%s\nanswered %v\nthe unbound pattern joined with the binding answers %v", text, got, want)
						})
					}
				})
			}
		})
	}
}

// boundValue is a value at one position of a data pattern: the entity, the
// attribute or the value position.
type boundValue struct {
	position int
	value    interface{}
}

// bindingTuples is a binding relation over the positions its tuples bind, one
// tuple per entry.
type bindingTuples [][]boundValue

// boundPatternCase is one pattern over [?e ?a ?v], bound at the positions its
// tuples carry and projected to find. One tuple binds scalar inputs, several
// bind a relation input, and a literal case writes its one tuple's values
// into the pattern in place of the variables.
type boundPatternCase struct {
	name    string
	find    []int
	tuples  bindingTuples
	literal bool
}

var patternPositionNames = []string{"?e", "?a", "?v"}

// query is the case's query text and the inputs it takes.
func (c boundPatternCase) query(t *testing.T) (string, []interface{}) {
	t.Helper()
	findNames := make([]string, len(c.find))
	for i, position := range c.find {
		findNames[i] = patternPositionNames[position]
	}
	find := strings.Join(findNames, " ")
	if c.literal {
		require.Len(t, c.tuples, 1, "a literal case writes one tuple into the pattern")
		positions := append([]string(nil), patternPositionNames...)
		for _, b := range c.tuples[0] {
			positions[b.position] = ednLiteral(t, b.value)
		}
		return fmt.Sprintf("[:find %s :where [%s]]", find, strings.Join(positions, " ")), nil
	}
	boundNames := make([]string, len(c.tuples[0]))
	for i, b := range c.tuples[0] {
		boundNames[i] = patternPositionNames[b.position]
	}
	if len(c.tuples) == 1 {
		inputs := make([]interface{}, len(c.tuples[0]))
		for i, b := range c.tuples[0] {
			inputs[i] = b.value
		}
		return fmt.Sprintf("[:find %s :in $ %s :where [?e ?a ?v]]", find, strings.Join(boundNames, " ")), inputs
	}
	relation := make([]interface{}, len(c.tuples))
	for i, tuple := range c.tuples {
		values := make([]interface{}, len(tuple))
		for j, b := range tuple {
			values[j] = b.value
		}
		relation[i] = values
	}
	return fmt.Sprintf("[:find %s :in $ [[%s]] :where [?e ?a ?v]]", find, strings.Join(boundNames, " ")), []interface{}{relation}
}

// reference is the unbound pattern's answer, filtered to the tuples agreeing
// with some binding tuple at every bound position and projected to find.
func (c boundPatternCase) reference(unbound [][]interface{}) [][]interface{} {
	var want [][]interface{}
	for _, tuple := range c.tuples {
		for _, u := range unbound {
			agrees := true
			for _, b := range tuple {
				if !datalog.ValuesEqual(u[b.position], b.value) {
					agrees = false
					break
				}
			}
			if !agrees {
				continue
			}
			projected := make([]interface{}, len(c.find))
			for i, position := range c.find {
				projected[i] = u[position]
			}
			if !containsTuple(want, projected) {
				want = append(want, projected)
			}
		}
	}
	return want
}

// ednLiteral renders a keyword, a long or a vector of them as query text.
func ednLiteral(t *testing.T, value interface{}) string {
	t.Helper()
	switch value := value.(type) {
	case datalog.Keyword:
		return value.String()
	case int64:
		return fmt.Sprintf("%d", value)
	case []interface{}:
		elements := make([]string, len(value))
		for i, element := range value {
			elements[i] = ednLiteral(t, element)
		}
		return "[" + strings.Join(elements, " ") + "]"
	}
	t.Fatalf("no literal for a %T", value)
	return ""
}

// tuplesAgree reports whether two answers are the same set of tuples under
// value equality, which reads a resolved vector and the vector it was written
// from as one value.
func tuplesAgree(got, want [][]interface{}) bool {
	if len(got) != len(want) {
		return false
	}
	for _, tuple := range got {
		if !containsTuple(want, tuple) {
			return false
		}
	}
	for _, tuple := range want {
		if !containsTuple(got, tuple) {
			return false
		}
	}
	return true
}

func containsTuple(tuples [][]interface{}, tuple []interface{}) bool {
	for _, candidate := range tuples {
		if len(candidate) != len(tuple) {
			continue
		}
		same := true
		for i := range tuple {
			if !datalog.ValuesEqual(candidate[i], tuple[i]) {
				same = false
				break
			}
		}
		if same {
			return true
		}
	}
	return false
}

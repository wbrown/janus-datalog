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
// at the bound positions, the entity position included. The route a binding
// selects is the engine's choice and never the answer's, so the reference is
// the unbound pattern's answer filtered in Go, in the view the query reads.
// The store carries x and y with a long at :r and t with the vector [1 2] at
// :tags, declared a vector of longs: a vector at the value position names t's
// collection under :tags, nothing under :r, and under History, where every
// datom is read as written, nothing at all.
func TestABoundPatternAnswersWhatTheUnboundPatternAnswersUnderTheBinding(t *testing.T) {
	x, y, tagged := datalog.NewIdentity("x"), datalog.NewIdentity("y"), datalog.NewIdentity("t")
	vector := []interface{}{int64(1), int64(2)}
	const e, a, v = 0, 1, 2
	cases := []boundPatternCase{
		{"the vector bound, the attribute open", []int{e}, bindingTuples{{{v, vector}}}, nil},
		{"the vector bound under the scalar attribute", []int{e}, bindingTuples{{{a, kw(":r")}, {v, vector}}}, nil},
		{"the vector bound under the vector attribute", []int{e}, bindingTuples{{{a, kw(":tags")}, {v, vector}}}, nil},
		{"the vector bound on an entity carrying a scalar", []int{a}, bindingTuples{{{e, x}, {v, vector}}}, nil},
		{"the vector bound on the entity carrying it", []int{a}, bindingTuples{{{e, tagged}, {v, vector}}}, nil},
		{"the entity carrying the vector bound, the value open", []int{v}, bindingTuples{{{e, tagged}}}, nil},
		{"a relation binding the vector beside a scalar", []int{e}, bindingTuples{{{a, kw(":r")}, {v, vector}}, {{a, kw(":r")}, {v, int64(1)}}}, nil},
		{"a relation binding the vector under each attribute", []int{e}, bindingTuples{{{a, kw(":r")}, {v, vector}}, {{a, kw(":tags")}, {v, vector}}}, nil},
		{"a stored scalar bound", []int{e}, bindingTuples{{{v, int64(2)}}}, nil},
		{"the vector written, the attribute open", []int{e}, bindingTuples{{{v, vector}}}, []int{v}},
		{"the vector written under the scalar attribute", []int{e}, bindingTuples{{{a, kw(":r")}, {v, vector}}}, []int{a, v}},
		{"the vector written under the vector attribute", []int{e}, bindingTuples{{{a, kw(":tags")}, {v, vector}}}, []int{a, v}},
		{"an entity carrying a scalar written beside the vector bound", []int{a}, bindingTuples{{{e, x}, {v, vector}}}, []int{e}},
		{"the entity carrying the vector written beside the vector bound", []int{a}, bindingTuples{{{e, tagged}, {v, vector}}}, []int{e}},
		{"an entity carrying a scalar written beside the vector written", []int{a}, bindingTuples{{{e, x}, {v, vector}}}, []int{e, v}},
		{"the entity carrying the vector written beside the vector written", []int{a}, bindingTuples{{{e, tagged}, {v, vector}}}, []int{e, v}},
		{"an entity carrying a scalar written under the written vector attribute, the vector bound", []int{v}, bindingTuples{{{e, x}, {a, kw(":tags")}, {v, vector}}}, []int{e, a}},
		{"the entity carrying the vector written under the written vector attribute, the vector bound", []int{v}, bindingTuples{{{e, tagged}, {a, kw(":tags")}, {v, vector}}}, []int{e, a}},
		{"an entity written beside a scalar bound it does not carry, the attribute open", []int{a}, bindingTuples{{{e, x}, {v, int64(2)}}}, []int{e}},
		{"an entity written beside a scalar bound it carries, the attribute open", []int{a}, bindingTuples{{{e, y}, {v, int64(2)}}}, []int{e}},
		{"an entity written under the written scalar attribute, a scalar bound it does not carry", []int{v}, bindingTuples{{{e, x}, {a, kw(":r")}, {v, int64(2)}}}, []int{e, a}},
		{"an entity written under the written scalar attribute, a scalar bound it carries", []int{v}, bindingTuples{{{e, y}, {a, kw(":r")}, {v, int64(2)}}}, []int{e, a}},
		{"an entity written under the written unique attribute, another entity's code bound", []int{v}, bindingTuples{{{e, x}, {a, kw(":code")}, {v, "y's"}}}, []int{e, a}},
		{"an entity written under the written unique attribute, its own code bound", []int{v}, bindingTuples{{{e, x}, {a, kw(":code")}, {v, "x's"}}}, []int{e, a}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			s := schema.NewBuilder().
				Attribute(":tags").Type(schema.TypeLong).Vector().Add().
				Attribute(":code").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
				MustBuild()
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(x, kw(":r"), int64(1)))
			require.NoError(t, tx.Add(x, kw(":code"), "x's"))
			require.NoError(t, tx.Add(y, kw(":r"), int64(2)))
			require.NoError(t, tx.Add(y, kw(":code"), "y's"))
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
// tuples carry and projected to find. A position in written carries its one
// tuple's value in the pattern in place of the variable; every other bound
// position is an input, scalar inputs for one tuple and a relation input for
// several.
type boundPatternCase struct {
	name    string
	find    []int
	tuples  bindingTuples
	written []int
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
	written := map[int]bool{}
	for _, position := range c.written {
		written[position] = true
	}
	if len(written) > 0 {
		require.Len(t, c.tuples, 1, "a case writing a position into the pattern has one tuple")
	}
	positions := append([]string(nil), patternPositionNames...)
	var boundNames []string
	for _, b := range c.tuples[0] {
		if written[b.position] {
			positions[b.position] = ednLiteral(t, b.value)
			continue
		}
		boundNames = append(boundNames, patternPositionNames[b.position])
	}
	pattern := strings.Join(positions, " ")
	if len(boundNames) == 0 {
		return fmt.Sprintf("[:find %s :where [%s]]", find, pattern), nil
	}
	if len(c.tuples) == 1 {
		var inputs []interface{}
		for _, b := range c.tuples[0] {
			if !written[b.position] {
				inputs = append(inputs, b.value)
			}
		}
		return fmt.Sprintf("[:find %s :in $ %s :where [%s]]", find, strings.Join(boundNames, " "), pattern), inputs
	}
	relation := make([]interface{}, len(c.tuples))
	for i, tuple := range c.tuples {
		values := make([]interface{}, len(tuple))
		for j, b := range tuple {
			values[j] = b.value
		}
		relation[i] = values
	}
	return fmt.Sprintf("[:find %s :in $ [[%s]] :where [%s]]", find, strings.Join(boundNames, " "), pattern), []interface{}{relation}
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

// ednLiteral renders an identity, a keyword, a long or a vector of them as
// query text.
func ednLiteral(t *testing.T, value interface{}) string {
	t.Helper()
	switch value := value.(type) {
	case datalog.Identity:
		return fmt.Sprintf("#identity %q", value.L85())
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

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An aggregating subquery whose :find writes an aggregate before a grouping
// variable binds each name its binding writes to the :find element at that
// name's position, on both optimizer paths (CLAUDE.md, Semantic
// Preservation; a binding is the relation over the binding's symbols in the
// inner :find's order, NESTED_SCOPES_AS_QUERIES.md §2.5). The reference is
// the unoptimized path on the same tree.
func TestRefuteSources_AnAggregateBeforeAGroupingVariableBindsByFindPosition(t *testing.T) {
	cases := []struct {
		name string
		text string
	}{
		{"one parameter, aggregate first", `[:find ?q ?m ?n :where [?q :p/k ?b] [(q [:find (count ?z) ?v :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v]] $ ?b) [[?m ?n]]]]`},
		{"one parameter, aggregate first, correlation in find", `[:find ?q ?m ?n :where [?q :p/k ?b] [(q [:find (count ?z) ?b :in $ ?b :where [?z :p/k ?b]] $ ?b) [[?m ?n]]]]`},
		{"one parameter, aggregate between", `[:find ?q ?m ?n ?o :where [?q :p/k ?b] [(q [:find ?v (count ?z) ?b :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v]] $ ?b) [[?m ?n ?o]]]]`},
		{"two parameters, aggregate first", `[:find ?q ?m ?n :where [?q :p/k ?b] [?q :p/v ?w] [(q [:find (count ?z) ?a :in $ ?a ?c :where [?z :p/k ?a] [?z :p/v ?c]] $ ?b ?w) [[?m ?n]]]]`},
		{"two parameters, aggregate first, binding names a correlation variable", `[:find ?q ?n :where [?q :p/k ?b] [?q :p/v ?w] [(q [:find (count ?z) ?a :in $ ?a ?c :where [?z :p/k ?a] [?z :p/v ?c]] $ ?b ?w) [[?n ?b]]]]`},
		{"one parameter, aggregate first, the find names its input, the binding the correlation variable", `[:find ?q ?n :where [?q :p/k ?b] [(q [:find (count ?z) ?b :in $ ?b :where [?z :p/k ?b]] $ ?b) [[?n ?b]]]]`},
		{"one parameter, aggregate first, the binding names one name twice", `[:find ?q ?m :where [?q :p/k ?b] [(q [:find (count ?z) ?v :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v]] $ ?b) [[?m ?m]]]]`},
		{"one parameter, aggregate first, or-default-join", `[:find ?q ?n ?m :where [?q :p/k ?b] (or-default-join [[?b] ?n ?m] [(q [:find ?b (count ?z) ?v :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v]] $ ?b) [[?b ?n ?m]]] (and [(ground -1) ?n] [(ground -1) ?m]))]`},
		{"two parameters, aggregate last", `[:find ?q ?m ?n :where [?q :p/k ?b] [?q :p/v ?w] [(q [:find ?a (count ?z) :in $ ?a ?c :where [?z :p/k ?a] [?z :p/v ?c]] $ ?b ?w) [[?m ?n]]]]`},
	}
	answers := map[bool]map[string]string{}
	for _, mode := range optimizerModes {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{})
		commitFacts(t, d, refuteTwoParamFacts())
		answers[mode.algebra] = map[string]string{}
		for _, c := range cases {
			answers[mode.algebra][c.name] = refuteOutcome(t, d, c.text)
		}
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.NotContains(t, answers[false][c.name], "error:")
			require.Equal(t, answers[false][c.name], answers[true][c.name], "optimizer on answers what optimizer off answers")
		})
	}
}

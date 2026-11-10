package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An aggregating subquery whose :find writes an aggregate before a grouping
// variable binds each name its binding writes to the :find element at that
// name's position, on every leg (CLAUDE.md, Semantic Preservation; a binding
// is the relation over the binding's symbols in the inner :find's order,
// NESTED_SCOPES_AS_QUERIES.md §2.5). The reference is the reference leg on
// the same tree, every option off.
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
	// answers is what each case answers on a mode.
	answers := func(t *testing.T, mode optimizerMode) map[string]string {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{})
		commitFacts(t, d, refuteTwoParamFacts())
		answered := map[string]string{}
		for _, c := range cases {
			answered[c.name] = refuteOutcome(t, d, c.text)
		}
		return answered
	}
	reference := answers(t, referenceMode)
	for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption) {
		answered := answers(t, mode)
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				require.NotContains(t, reference[c.name], "error:")
				require.Equal(t, reference[c.name], answered[c.name], "the leg answers what the reference leg answers")
			})
		}
	}
}

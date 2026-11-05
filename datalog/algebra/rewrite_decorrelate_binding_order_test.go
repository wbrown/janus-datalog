package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Rule 1 decorrelates an aggregating subquery into an Aggregate whose
// bindings name its group keys and then its functions, the layout the
// Aggregate documents. Each name the call's binding wrote binds the element
// of the decorrelated :find it was written against, whatever the :find's
// order, in the Aggregate and in the call it decompiles to. The decorrelated
// :find leads with the inner parameter each correlation variable binds,
// named for the outer variable the call passes there.
func TestADecorrelatedAggregateBindsEachNameToTheElementItNamed(t *testing.T) {
	sym := datalog.NewSymbol
	variable := func(name string) query.FindElement { return query.FindVariable{Symbol: sym(name)} }
	countOf := func(name string) query.FindElement {
		return query.FindAggregate{Function: datalog.SymCount, Arg: sym(name)}
	}
	cases := []struct {
		name string
		text string
		// want maps each element of the decorrelated :find to the name its
		// binding gives it.
		want map[query.FindElement]query.Symbol
	}{
		{"aggregate first",
			`[:find ?q ?m ?n :where [?q :p/k ?k] [(q [:find (count ?z) ?v :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v]] $ ?k) [[?m ?n]]]]`,
			map[query.FindElement]query.Symbol{variable("?b"): sym("?k"), countOf("?z"): sym("?m"), variable("?v"): sym("?n")}},
		{"aggregate between",
			`[:find ?q ?m ?n ?o :where [?q :p/k ?k] [(q [:find ?v (count ?z) ?w :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v] [?z :p/w ?w]] $ ?k) [[?m ?n ?o]]]]`,
			map[query.FindElement]query.Symbol{variable("?b"): sym("?k"), variable("?v"): sym("?m"), countOf("?z"): sym("?n"), variable("?w"): sym("?o")}},
		{"aggregate last",
			`[:find ?q ?m ?n :where [?q :p/k ?k] [(q [:find ?v (count ?z) :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v]] $ ?k) [[?m ?n]]]]`,
			map[query.FindElement]query.Symbol{variable("?b"): sym("?k"), variable("?v"): sym("?m"), countOf("?z"): sym("?n")}},
		{"two parameters, aggregate first",
			`[:find ?q ?m ?n :where [?q :p/k ?k] [?q :p/v ?c] [(q [:find (count ?z) ?w :in $ ?b ?d :where [?z :p/k ?b] [?z :p/v ?d] [?z :p/w ?w]] $ ?k ?c) [[?m ?n]]]]`,
			map[query.FindElement]query.Symbol{variable("?b"): sym("?k"), variable("?d"): sym("?c"), countOf("?z"): sym("?m"), variable("?w"): sym("?n")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			parsed, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			prepared, err := query.Prepare(parsed, nil)
			require.NoError(t, err)
			root, err := Compile(prepared, nil)
			require.NoError(t, err)
			optimized, err := NewOptimizer(DecorrelationPass(nil)).Optimize(root)
			require.NoError(t, err)

			var aggregates []*Aggregate
			var walk func(n *Node)
			walk = func(n *Node) {
				if aggregate, isAggregate := n.Data.(*Aggregate); isAggregate {
					aggregates = append(aggregates, aggregate)
				}
				for _, child := range n.Children {
					walk(child)
				}
			}
			walk(optimized)
			require.Len(t, aggregates, 1, "the subquery is decorrelated")
			aggregate := aggregates[0]
			require.Len(t, aggregate.Bindings, len(aggregate.GroupBy)+len(aggregate.Functions))
			inAggregate := map[query.FindElement]query.Symbol{}
			for i, key := range aggregate.GroupBy {
				inAggregate[query.FindVariable{Symbol: key}] = aggregate.Bindings[i]
			}
			for i, function := range aggregate.Functions {
				inAggregate[function] = aggregate.Bindings[len(aggregate.GroupBy)+i]
			}
			require.Equal(t, c.want, inAggregate, "the Aggregate's bindings name each element as the call wrote it")

			clauses, err := Decompile(optimized)
			require.NoError(t, err)
			var calls []*query.SubqueryPattern
			for _, clause := range clauses {
				if call, isCall := clause.(*query.SubqueryPattern); isCall {
					calls = append(calls, call)
				}
			}
			require.Len(t, calls, 1, "the decorrelated call decompiles as one subquery")
			binding, isRelation := calls[0].Binding.(query.RelationBinding)
			require.True(t, isRelation, "the decorrelated call binds the relation form, got %T", calls[0].Binding)
			require.Len(t, binding.Variables, len(calls[0].Query.Find))
			inCall := map[query.FindElement]query.Symbol{}
			for i, element := range calls[0].Query.Find {
				inCall[element] = binding.Variables[i]
			}
			require.Equal(t, c.want, inCall, "the decompiled call binds each name to the element the call wrote it against")
		})
	}
}

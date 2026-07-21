package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestCustomFunctionQueryEndToEnd pins the registry wiring through real
// queries on both optimizer modes: a registered function filters in
// predicate position and computes in expression position, through the same
// RegisterCustomFunction API downstream consumers use. Registration precedes
// parsing — the parser admits expression-position names from the registry.
func TestCustomFunctionQueryEndToEnd(t *testing.T) {
	RegisterCustomFunction("test/expensive?", func(args []interface{}) (interface{}, error) {
		price, ok := args[0].(float64)
		return ok && price > 100.0, nil
	})
	RegisterCustomFunction("test/discounted", func(args []interface{}) (interface{}, error) {
		price, ok := args[0].(float64)
		if !ok {
			return nil, nil
		}
		return price * 0.5, nil
	})

	nameAttr := datalog.NewKeyword(":product/name")
	priceAttr := datalog.NewKeyword(":product/price")
	laptop := datalog.NewIdentity("prod:laptop")
	cable := datalog.NewIdentity("prod:cable")

	matcher := NewMemoryPatternMatcher([]datalog.Datom{
		{E: laptop, A: nameAttr, V: "Laptop", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: laptop, A: priceAttr, V: 1200.0, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: cable, A: nameAttr, V: "Cable", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: cable, A: priceAttr, V: 8.0, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
	})

	t.Run("predicate position filters", func(t *testing.T) {
		q, err := parser.ParseQuery(`[:find ?name
			:where [?p :product/name ?name]
			       [?p :product/price ?price]
			       [(test/expensive? ?price)]]`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}

		for _, mode := range optimizerModes {
			t.Run(mode.name, func(t *testing.T) {
				exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
				result, err := exec.Execute(q)
				if err != nil {
					t.Fatalf("execute: %v", err)
				}
				if result.Size() != 1 {
					t.Fatalf("expected 1 expensive product, got %d", result.Size())
				}
				if got := result.Get(0)[0].(string); got != "Laptop" {
					t.Errorf("expected Laptop, got %s", got)
				}
			})
		}
	})

	t.Run("expression position computes", func(t *testing.T) {
		q, err := parser.ParseQuery(`[:find ?name ?half
			:where [?p :product/name ?name]
			       [?p :product/price ?price]
			       [(test/discounted ?price) ?half]]`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}

		for _, mode := range optimizerModes {
			t.Run(mode.name, func(t *testing.T) {
				exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
				result, err := exec.Execute(q)
				if err != nil {
					t.Fatalf("execute: %v", err)
				}
				halves := make(map[string]float64)
				for i := 0; i < result.Size(); i++ {
					tuple := result.Get(i)
					halves[tuple[0].(string)] = tuple[1].(float64)
				}
				if halves["Laptop"] != 600.0 {
					t.Errorf("discounted Laptop = %v, want 600", halves["Laptop"])
				}
				if halves["Cable"] != 4.0 {
					t.Errorf("discounted Cable = %v, want 4", halves["Cable"])
				}
			})
		}
	})

	t.Run("unregistered expression name still rejected at parse", func(t *testing.T) {
		_, err := parser.ParseQuery(`[:find ?y
			:where [?p :product/price ?price]
			       [(test/never-registered ?price) ?y]]`)
		if err == nil {
			t.Fatal("unregistered expression function parsed without error")
		}
	})
}

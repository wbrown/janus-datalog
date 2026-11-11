package parser

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPositionSymbols(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		rules       string
		entity      []string
		attr        []string
		transaction []string
		neither     []string
		refused     string
	}{
		{
			name:    "direct pattern",
			query:   `[:find ?n :in $ ?e :where [?e :user/name ?n]]`,
			entity:  []string{"?e"},
			neither: []string{"?n"},
		},
		{
			name:    "value position is not entity position",
			query:   `[:find ?u :in $ ?g :where [?u :user/group ?g]]`,
			entity:  []string{"?u"},
			neither: []string{"?g"},
		},
		{
			name:    "attribute variable",
			query:   `[:find ?v :in $ ?a :where [?e ?a ?v]]`,
			entity:  []string{"?e"},
			attr:    []string{"?a"},
			neither: []string{"?v"},
		},
		{
			name:        "transaction variable",
			query:       `[:find ?v :in $ ?t :where [?e :user/name ?v ?t]]`,
			entity:      []string{"?e"},
			transaction: []string{"?t"},
			neither:     []string{"?v"},
		},
		{
			name: "inside not and or",
			query: `[:find ?n :in $ ?e ?x
			         :where [?e :user/name ?n]
			                (not [?x :user/deleted true])
			                (or [?e :user/city "NYC"]
			                    [?e :user/city "SF"])]`,
			entity: []string{"?e", "?x"},
		},
		{
			name: "subquery argument mapped to inner entity position",
			query: `[:find ?s ?count
			         :in $ ?s
			         :where [(q [:find (count ?t)
			                     :in $ ?scenario
			                     :where [?t :task/scenario ?scenario]
			                            [?scenario :scenario/active true]]
			                    $ ?s) [[?count]]]]`,
			entity:  []string{"?s"},
			neither: []string{"?count"},
		},
		{
			name: "subquery argument mapped to inner attribute position",
			query: `[:find ?e ?count
			         :in $ ?attr
			         :where [?e :task/id _]
			                [(q [:find (count ?t)
			                     :in $ ?a
			                     :where [?t ?a _]]
			                    $ ?attr) [[?count]]]]`,
			entity:  []string{"?e"},
			attr:    []string{"?attr"},
			neither: []string{"?count"},
		},
		{
			name: "subquery argument mapped to inner transaction position",
			query: `[:find ?e ?count
			         :in $ ?when
			         :where [?e :task/id _]
			                [(q [:find (count ?t)
			                     :in $ ?tx
			                     :where [?t :task/id _ ?tx]]
			                    $ ?when) [[?count]]]]`,
			entity:      []string{"?e"},
			transaction: []string{"?when"},
			neither:     []string{"?count"},
		},
		{
			name: "subquery argument feeding only value positions is not position-bound",
			query: `[:find ?e ?count
			         :in $ ?status
			         :where [?e :task/id _]
			                [(q [:find (count ?t)
			                     :in $ ?st
			                     :where [?t :task/status ?st]]
			                    $ ?status) [[?count]]]]`,
			entity:  []string{"?e"},
			neither: []string{"?status"},
		},
		{
			name:        "rule arguments mapped to the body's entity, attribute and transaction positions",
			query:       `[:find ?v :in $ % ?e ?a ?t :where (at ?e ?a ?v ?t)]`,
			rules:       `[[(at ?e ?a ?v ?t) [?e ?a ?v ?t]]]`,
			entity:      []string{"?e"},
			attr:        []string{"?a"},
			transaction: []string{"?t"},
			neither:     []string{"?v"},
		},
		{
			name:    "rule argument feeding only a value position is not position-bound",
			query:   `[:find ?e :in $ % ?v :where (named ?e ?v)]`,
			rules:   `[[(named ?e ?v) [?e :user/name ?v]]]`,
			entity:  []string{"?e"},
			neither: []string{"?v"},
		},
		{
			name:        "rule argument mapped through a rule the body invokes",
			query:       `[:find ?e :in $ % ?t :where (at ?e ?t)]`,
			rules:       `[[(at ?e ?t) (atx ?e ?t)] [(atx ?e ?t) [?e :user/name _ ?t]]]`,
			entity:      []string{"?e"},
			transaction: []string{"?t"},
		},
		{
			name:    "rule argument mapped through a recursive rule's other rule",
			query:   `[:find ?b :in $ % ?x :where (reach ?x ?b)]`,
			rules:   `[[(reach ?a ?c) [?a :user/next ?b] (reach ?b ?c)] [(reach ?a ?b) [?a :user/next ?b]]]`,
			entity:  []string{"?x"},
			neither: []string{"?b"},
		},
		{
			name:        "rule argument mapped under a subquery's own rule set",
			query:       `[:find ?e :in $ % ?t :where [(q [:find ?x :in $ % ?tx :where (at ?x ?tx)] $ % ?t) [[?e]]]]`,
			rules:       `[[(at ?e ?t) [?e :user/name _ ?t]]]`,
			transaction: []string{"?t"},
			neither:     []string{"?e"},
		},
		{
			name:    "rule argument reaching a not-join local of the head variable's name is not position-bound",
			query:   `[:find ?b :in $ % ?a :where (r ?a ?b)]`,
			rules:   `[[(r ?a ?b) [?b :user/name ?a] (not-join [?b] [?a :user/next ?b])]]`,
			entity:  []string{"?b"},
			neither: []string{"?a"},
		},
		{
			name:   "rule argument reaching a not-join header variable is position-bound",
			query:  `[:find ?b :in $ % ?a :where (r ?a ?b)]`,
			rules:  `[[(r ?a ?b) [?b :user/name ?a] (not-join [?a ?b] [?a :user/next ?b])]]`,
			entity: []string{"?a", "?b"},
		},
		{
			name:    "rule argument reaching an or-join local of the head variable's name is not position-bound",
			query:   `[:find ?b :in $ % ?a :where (r ?a ?b)]`,
			rules:   `[[(r ?a ?b) [?b :user/name ?a] (or-join [?b] [?a :user/next ?b] [?b :user/city _])]]`,
			entity:  []string{"?b"},
			neither: []string{"?a"},
		},
		{
			name:    "rule argument reaching an or-default-join local of the head variable's name is not position-bound",
			query:   `[:find ?b ?v :in $ % ?a :where (r ?a ?b ?v)]`,
			rules:   `[[(r ?a ?b ?v) [?b :user/name ?a] (or-default-join [[?b] ?v] (and [?a :user/next ?b] [(ground 1) ?v]) [(ground 0) ?v])]]`,
			entity:  []string{"?b"},
			neither: []string{"?a", "?v"},
		},
		{
			name:    "input reaching a not-join local of its name in the query is not position-bound",
			query:   `[:find ?b :in $ ?a :where [?b :user/name ?a] (not-join [?b] [?a :user/next ?b])]`,
			entity:  []string{"?b"},
			neither: []string{"?a"},
		},
		{
			name:    "input reaching a local of a header form nested in a header form is not position-bound",
			query:   `[:find ?b :in $ ?a :where [?b :user/name ?a] (not-join [?b] (or-join [?b] [?a :user/next ?b] [?b :user/city _]))]`,
			entity:  []string{"?b"},
			neither: []string{"?a"},
		},
		{
			name:        "rule argument named at the value and transaction positions of one pattern is bound at the transaction position",
			query:       `[:find ?e :in $ % ?t :where (at ?e ?t)]`,
			rules:       `[[(at ?e ?t) [?e :user/name ?t ?t]]]`,
			entity:      []string{"?e"},
			transaction: []string{"?t"},
		},
		{
			name:    "rule argument named at the entity and attribute positions of one pattern is bound at both",
			query:   `[:find ?v :in $ % ?x :where (r ?x ?v)]`,
			rules:   `[[(r ?x ?v) [?x ?x ?v]]]`,
			entity:  []string{"?x"},
			attr:    []string{"?x"},
			neither: []string{"?v"},
		},
		{
			name:        "rule argument named twice in a subquery the body runs is bound at the later position",
			query:       `[:find ?b :in $ % ?a :where (r ?a ?b)]`,
			rules:       `[[(r ?a ?b) [?b :user/city ?a] [(q [:find ?y :in $ ?x :where [?y :user/name ?x ?x]] $ ?a) [[?y]]]]]`,
			entity:      []string{"?b"},
			transaction: []string{"?a"},
		},
		{
			name:    "a constant of another type at a rule argument's typed position is refused",
			query:   `[:find ?e :in $ % :where (at ?e "x")]`,
			rules:   `[[(at ?e ?t) [?e :user/name _ ?t]]]`,
			refused: "transaction position",
		},
		{
			name:    "a constant of another type at a subquery argument's typed parameter is refused",
			query:   `[:find ?e :where [(q [:find ?x :in $ ?tx :where [?x :user/name _ ?tx]] $ "x") [[?e]]]]`,
			refused: "transaction position",
		},
		{
			name:    "a constant of another type at a subquery's tuple parameter's symbol is refused",
			query:   `[:find ?e :where [(q [:find ?x :in $ [?tx ?d] :where [?x :user/name ?d ?tx]] $ "x" 1) [[?e]]]]`,
			refused: "transaction position",
		},
		{
			name:    "a constant of another type at a subquery's relation parameter's symbol is refused",
			query:   `[:find ?e :where [(q [:find ?x :in $ [[?tx ?d]] :where [?x :user/name ?d ?tx]] $ "x" 1) [[?e]]]]`,
			refused: "transaction position",
		},
		{
			name:    "a constant of another type at a subquery's tuple parameter of one symbol is refused",
			query:   `[:find ?e :where [(q [:find ?x :in $ [?tx] :where [?x :user/name _ ?tx]] $ "x") [[?e]]]]`,
			refused: "transaction position",
		},
		{
			name:    "a constant of another type in a subquery's collection parameter declared after a tuple parameter is refused",
			query:   `[:find ?e :where [(q [:find ?x :in $ [?a ?b] [?tx ...] :where [?x :user/name ?a ?tx] [?x :user/age ?b]] $ 1 2 ["x"]) [[?e]]]]`,
			refused: "transaction position",
		},
		{
			name:        "subquery argument at a tuple parameter's symbol mapped to the inner transaction position",
			query:       `[:find ?e :in $ ?when :where [(q [:find ?x :in $ [?tx ?d] :where [?x :user/name ?d ?tx]] $ ?when 1) [[?e]]]]`,
			transaction: []string{"?when"},
			neither:     []string{"?e"},
		},
		{
			name:    "a constant at a subquery's tuple parameter's symbol feeding only a value position is not refused",
			query:   `[:find ?e :where [(q [:find ?x :in $ [?v ?d] :where [?x :user/name ?v] [?x :user/age ?d]] $ "x" 1) [[?e]]]]`,
			neither: []string{"?e"},
		},
		{
			name:   "a constant of another type in a predicate the query does not invoke is not read",
			query:  `[:find ?o :in $ % :where (g ?o)]`,
			rules:  `[[(g ?o) [?o :user/age 5]] [(wr ?o) (at ?o "x")] [(at ?o ?t) [?o :user/name _ ?t]]]`,
			entity: []string{"?o"},
		},
		{
			name:    "a constant of another type in a predicate the query reaches through a chain of invocations is refused",
			query:   `[:find ?o :in $ % :where (a ?o)]`,
			rules:   `[[(a ?o) (wr ?o)] [(wr ?o) (at ?o "x")] [(at ?o ?t) [?o :user/name _ ?t]]]`,
			refused: "transaction position",
		},
		{
			name:    "a user variable in the unify namespace at the entity position is refused as the engine's namespace",
			query:   `[:find ?v :in $ ?foo :where [?unify/foo.1 :user/name ?v] [?e :user/name ?foo]]`,
			refused: "which the engine mints into",
		},
		{
			name:    "a user variable with its own namespace under unify at the transaction position is refused as the engine's namespace",
			query:   `[:find ?v :in $ ?mine/foo :where [?e :user/name ?v ?unify.mine/foo.1] [?e :user/name ?mine/foo]]`,
			refused: "which the engine mints into",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("ParseQuery failed: %v", err)
			}
			var rules *query.RuleSet
			if tt.rules != "" {
				rules, err = ParseRules(tt.rules)
				if err != nil {
					t.Fatalf("ParseRules failed: %v", err)
				}
			}
			entity, attr, transaction, err := query.PositionSymbols(q, rules)
			if tt.refused != "" {
				if err == nil || !strings.Contains(err.Error(), tt.refused) {
					t.Fatalf("expected a refusal naming %q, got %v", tt.refused, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("PositionSymbols failed: %v", err)
			}
			for _, sym := range tt.entity {
				if !entity[datalog.NewSymbol(sym)] {
					t.Errorf("expected %s in entity-position set, got %v", sym, entity)
				}
			}
			for _, sym := range tt.attr {
				if !attr[datalog.NewSymbol(sym)] {
					t.Errorf("expected %s in attribute-position set, got %v", sym, attr)
				}
			}
			for _, sym := range tt.transaction {
				if !transaction[datalog.NewSymbol(sym)] {
					t.Errorf("expected %s in transaction-position set, got %v", sym, transaction)
				}
			}
			for _, sym := range tt.neither {
				if entity[datalog.NewSymbol(sym)] {
					t.Errorf("%s must not be in entity-position set, got %v", sym, entity)
				}
				if attr[datalog.NewSymbol(sym)] {
					t.Errorf("%s must not be in attribute-position set, got %v", sym, attr)
				}
				if transaction[datalog.NewSymbol(sym)] {
					t.Errorf("%s must not be in transaction-position set, got %v", sym, transaction)
				}
			}
		})
	}
}

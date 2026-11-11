package parser

import (
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
			entity, attr, transaction := query.PositionSymbols(q, rules)
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

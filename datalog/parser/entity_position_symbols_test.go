package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPositionSymbols(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		entity  []string
		attr    []string
		neither []string
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("ParseQuery failed: %v", err)
			}
			entity, attr := query.PositionSymbols(q)
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
			for _, sym := range tt.neither {
				if entity[datalog.NewSymbol(sym)] {
					t.Errorf("%s must not be in entity-position set, got %v", sym, entity)
				}
				if attr[datalog.NewSymbol(sym)] {
					t.Errorf("%s must not be in attribute-position set, got %v", sym, attr)
				}
			}
		})
	}
}

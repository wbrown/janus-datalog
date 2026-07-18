package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestEntityPositionSymbols(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
		not   []string
	}{
		{
			name:  "direct pattern",
			query: `[:find ?n :in $ ?e :where [?e :user/name ?n]]`,
			want:  []string{"?e"},
			not:   []string{"?n"},
		},
		{
			name: "value position is not entity position",
			query: `[:find ?u :in $ ?g :where [?u :user/group ?g]]`,
			want:  []string{"?u"},
			not:   []string{"?g"},
		},
		{
			name: "inside not and or",
			query: `[:find ?n :in $ ?e ?x
			         :where [?e :user/name ?n]
			                (not [?x :user/deleted true])
			                (or [?e :user/city "NYC"]
			                    [?e :user/city "SF"])]`,
			want: []string{"?e", "?x"},
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
			want: []string{"?s"},
			not:  []string{"?count"},
		},
		{
			name: "subquery argument feeding only value positions is not entity-bound",
			query: `[:find ?e ?count
			         :in $ ?status
			         :where [?e :task/id _]
			                [(q [:find (count ?t)
			                     :in $ ?st
			                     :where [?t :task/status ?st]]
			                    $ ?status) [[?count]]]]`,
			want: []string{"?e"},
			not:  []string{"?status"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("ParseQuery failed: %v", err)
			}
			got := query.EntityPositionSymbols(q)
			for _, sym := range tt.want {
				if !got[datalog.NewSymbol(sym)] {
					t.Errorf("expected %s in entity-position set, got %v", sym, got)
				}
			}
			for _, sym := range tt.not {
				if got[datalog.NewSymbol(sym)] {
					t.Errorf("%s must not be in entity-position set, got %v", sym, got)
				}
			}
		})
	}
}

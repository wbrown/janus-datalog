package executor

import (
	"fmt"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The entry runs a query with one relation per :in entry that binds
// variables, in the :in's order, and refuses any other count naming the count
// given and the count the :in binds. A slice shorter than the :in never binds
// nothing for the inputs past its end.
func TestExecuteWithRelationsRefusesARelationCountThatDiffersFromTheInputs(t *testing.T) {
	matcher := NewMemoryPatternMatcher(chainDatoms(3))
	one := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?p")}, []Tuple{{page(1)}})
	pair := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?p"), datalog.NewSymbol("?q")}, []Tuple{{page(1), page(2)}})
	cases := []struct {
		name      string
		text      string
		relations []Relation
		given     int
		bound     int
	}{
		{
			name:  "one scalar input and no relation",
			text:  `[:find ?k :in $ ?p :where [?p :page/kind ?k]]`,
			given: 0, bound: 1,
		},
		{
			name:      "one scalar input and two relations",
			text:      `[:find ?k :in $ ?p :where [?p :page/kind ?k]]`,
			relations: []Relation{one, one},
			given:     2, bound: 1,
		},
		{
			name:      "a tuple input and a relation input and one relation",
			text:      `[:find ?k :in $ [?p ?q] [[?r ?s] ...] :where [?p :page/kind ?k] [?q :page/kind ?k] [?r :page/kind ?k] [?s :page/kind ?k]]`,
			relations: []Relation{pair},
			given:     1, bound: 2,
		},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions())
				_, err := exec.ExecuteWithRelations(NewContext(), mustParse(t, c.text), c.relations)
				if err == nil {
					t.Fatalf("%d relations for a :in binding %d were accepted, want the refusal", c.given, c.bound)
				}
				for _, want := range []string{
					fmt.Sprintf("%d input relations were given", c.given),
					fmt.Sprintf("binds %d", c.bound),
				} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the refusal %q does not say %q", err.Error(), want)
					}
				}
			})
		}
	}
}

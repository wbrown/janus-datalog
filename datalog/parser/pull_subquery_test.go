package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestPullRejectedInSubqueryFind pins the value-domain boundary from
// BUG_PULL_WITH_ORDER_BY_PANICS.md: pull is result presentation,
// producing map values that are not datalog values. A subquery's result
// feeds the enclosing query's relational pipeline (joins, dedup), so a pull
// inside a subquery find would inject non-values into relational flow —
// rejected at parse time, like :limit in subqueries.
func TestPullRejectedInSubqueryFind(t *testing.T) {
	_, err := ParseQuery(`[:find ?task ?info
	                       :where [?task :task/status :pending]
	                              [(q [:find (pull ?t [:task/name])
	                                   :in $ ?task
	                                   :where [?t :task/parent ?task]]
	                                  $ ?task) [[?info] ...]]]`)
	if err == nil {
		t.Fatal("expected parse error for (pull ...) inside a subquery find, got success")
	}
	if !strings.Contains(err.Error(), "pull") {
		t.Fatalf("error should name the pull restriction, got: %v", err)
	}
}

// Pull in a top-level find must remain valid, and parses to the pull of the
// entity variable over its attributes.
func TestPullAcceptedInTopLevelFind(t *testing.T) {
	q, err := ParseQuery(`[:find (pull ?task [:task/name]) ?status
	                       :where [?task :task/status ?status]]`)
	require.NoError(t, err, "pull in top-level find must parse")
	assert.Equal(t, []query.FindElement{
		query.FindPull{
			Variable: datalog.NewSymbol("?task"),
			Pattern: &query.PullPattern{Specs: []query.PullAttrSpec{
				&query.PullAttribute{Attr: datalog.NewKeyword(":task/name")},
			}},
		},
		query.FindVariable{Symbol: datalog.NewSymbol("?status")},
	}, q.Find)
}

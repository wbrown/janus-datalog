package parser

import (
	"strings"
	"testing"
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

// Pull in a top-level find must remain valid.
func TestPullAcceptedInTopLevelFind(t *testing.T) {
	_, err := ParseQuery(`[:find (pull ?task [:task/name]) ?status
	                       :where [?task :task/status ?status]]`)
	if err != nil {
		t.Fatalf("pull in top-level find must parse: %v", err)
	}
}

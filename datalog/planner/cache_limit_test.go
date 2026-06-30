package planner

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestPlanCacheDistinguishesLimit guards a correctness trap: the realized plan
// carries the query (RealizedPlan.Query), and the executor reads the limit from
// that cached query. If :limit is not part of the cache key, two queries that
// differ only by limit collide and the wrong limit is applied. The cache must
// treat different limits as different keys.
func TestPlanCacheDistinguishesLimit(t *testing.T) {
	cache := NewPlanCache(10, 1*time.Minute)
	opts := PlannerOptions{}

	mkQuery := func(limit *int) *query.Query {
		return &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: datalog.NewSymbol("?e")},
			},
			Where: []query.Clause{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":person/name")},
						query.Variable{Name: datalog.NewSymbol("?n")},
					},
				},
			},
			Limit: limit,
		}
	}

	one, five := 1, 5
	q1 := mkQuery(&one)
	q5 := mkQuery(&five)

	plan1 := &RealizedPlan{Query: q1}
	cache.SetWithOptions(q1, plan1, opts)

	// A query differing only by limit must NOT hit the cached plan.
	if _, ok := cache.GetWithOptions(q5, opts); ok {
		t.Error("queries differing only by :limit must not share a cache entry")
	}

	// The same limit must still hit.
	if _, ok := cache.GetWithOptions(mkQuery(&one), opts); !ok {
		t.Error("identical query (same :limit) should hit the cache")
	}

	// No-limit must also be distinct from a limited query.
	if _, ok := cache.GetWithOptions(mkQuery(nil), opts); ok {
		t.Error("unlimited query must not share a cache entry with a limited one")
	}
}

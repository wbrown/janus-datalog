package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// BenchmarkSubqueryDeferralScheduling pins the clause-scheduling shape where
// greedy scoring — not symbol dependency — decides subquery placement: after
// the pattern binds ?e, a NOT filter and a correlated subquery are
// simultaneously executable. The NOT eliminates 90% of entities; a correlated
// subquery executes once per input combination, so running it before the NOT
// does ~10× the nested-query work. Dependency ordering cannot save this —
// both clauses need only ?e.
func BenchmarkSubqueryDeferralScheduling(b *testing.B) {
	const (
		users        = 500
		blockedEvery = 10 // 9 of every 10 users are blocked
		ordersPer    = 5
	)
	activeAttr := datalog.NewKeyword(":user/active")
	blockedAttr := datalog.NewKeyword(":user/blocked")
	orderUser := datalog.NewKeyword(":order/user")
	orderAmount := datalog.NewKeyword(":order/amount")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	var datoms []datalog.Datom
	for i := 0; i < users; i++ {
		user := datalog.NewIdentity(fmt.Sprintf("user:%d", i))
		datoms = append(datoms, datalog.Datom{E: user, A: activeAttr, V: true, Tx: tx})
		if i%blockedEvery != 0 {
			datoms = append(datoms, datalog.Datom{E: user, A: blockedAttr, V: true, Tx: tx})
		}
		for j := 0; j < ordersPer; j++ {
			order := datalog.NewIdentity(fmt.Sprintf("order:%d:%d", i, j))
			datoms = append(datoms,
				datalog.Datom{E: order, A: orderUser, V: user, Tx: tx},
				datalog.Datom{E: order, A: orderAmount, V: int64(j + 1), Tx: tx},
			)
		}
	}
	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher, nil)

	q, err := parser.ParseQuery(`[:find ?e ?total
	 :where [?e :user/active true]
	        (not [?e :user/blocked true])
	        [(q [:find (sum ?amt)
	             :in $ ?e
	             :where [?o :order/user ?e]
	                    [?o :order/amount ?amt]]
	            $ ?e) [[?total]]]]`)
	if err != nil {
		b.Fatalf("parse: %v", err)
	}

	const wantRows = users / blockedEvery

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(q)
		if err != nil {
			b.Fatalf("execute: %v", err)
		}
		if result.Size() != wantRows {
			b.Fatalf("expected %d rows, got %d", wantRows, result.Size())
		}
	}
}

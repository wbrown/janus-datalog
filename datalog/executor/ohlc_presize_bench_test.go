package executor_test

import (
	"testing"

	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// BenchmarkOHLCWithPreSizing measures the full OHLC query impact
func BenchmarkOHLCWithPreSizing(b *testing.B) {
	// This reuses the OHLC test from ohlc_chain_profile_test.go
	// but focuses on allocation metrics

	datoms := createOHLCData(0, 260) // 260 hours
	matcher := factsDatabase(b, datoms)
	exec := NewExecutor(matcher, nil)

	queryStr := `
	[:find ?day ?hour ?open ?high ?low ?close
	 :where
	 [?s :symbol/ticker "TEST"]
	 [?b :price/symbol ?s]
	 [?b :price/time ?t]
	 [(day ?t) ?day]
	 [(hour ?t) ?hour]

	 [(q [:find (min ?o)
	      :in $ ?sym ?d ?h
	      :where
	      [?bar :price/symbol ?sym]
	      [?bar :price/time ?time]
	      [(day ?time) ?bd]
	      [(hour ?time) ?bh]
	      [(= ?bd ?d)]
	      [(= ?bh ?h)]
	      [?bar :price/open ?o]]
	     $ ?s ?day ?hour) [[?open]]]

	 [(q [:find (max ?hv)
	      :in $ ?sym ?d ?h
	      :where
	      [?bar :price/symbol ?sym]
	      [?bar :price/time ?time]
	      [(day ?time) ?bd]
	      [(hour ?time) ?bh]
	      [(= ?bd ?d)]
	      [(= ?bh ?h)]
	      [?bar :price/high ?hv]]
	     $ ?s ?day ?hour) [[?high]]]

	 [(q [:find (min ?l)
	      :in $ ?sym ?d ?h
	      :where
	      [?bar :price/symbol ?sym]
	      [?bar :price/time ?time]
	      [(day ?time) ?bd]
	      [(hour ?time) ?bh]
	      [(= ?bd ?d)]
	      [(= ?bh ?h)]
	      [?bar :price/low ?l]]
	     $ ?s ?day ?hour) [[?low]]]

	 [(q [:find (max ?c)
	      :in $ ?sym ?d ?h
	      :where
	      [?bar :price/symbol ?sym]
	      [?bar :price/time ?time]
	      [(day ?time) ?bd]
	      [(hour ?time) ?bh]
	      [(= ?bd ?d)]
	      [(= ?bh ?h)]
	      [?bar :price/close ?c]]
	     $ ?s ?day ?hour) [[?close]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(q)
		if err != nil {
			b.Fatalf("Query execution failed: %v", err)
		}
	}
}

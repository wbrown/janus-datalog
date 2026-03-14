package parser

import (
	"testing"

	"github.com/wbrown/ebnf"
	"github.com/wbrown/ebnf/parse"
	ednpkg "github.com/wbrown/janus-datalog/datalog/edn"
)

// BenchmarkEDNParsing compares the hand-rolled EDN parser against the
// EBNF-based EDN parser for representative Datalog query strings.
//
// The hand-rolled parser (datalog/edn) is the current production parser.
// The EBNF parser (ebnf/examples/edn.ebnf) is a potential replacement that
// would bring the transform framework for query rewriting.
func BenchmarkEDNParsing(b *testing.B) {
	queries := map[string]string{
		"simple": `[:find ?e ?name :where [?e :person/name ?name]]`,

		"medium": `[:find ?e ?name ?age
		            :where [?e :person/name ?name]
		                   [?e :person/age ?age]
		                   [(> ?age 21)]]`,

		"complex": `[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens
		  :where [?scenario :entity/type :entity.type/scenario]
		         [?scenario :scenario/title ?title]
		         [?scenario :scenario/created-at ?createdAt]
		         (or [(q [:find (count ?t) (sum ?tok)
		                  :in $ ?s
		                  :where [?t :task/root ?s]
		                         [?t :task/status :status/complete]
		                         [(get-else $ ?t :task/token-count 0) ?tok]]
		                 $ ?scenario) [[?taskCount ?totalTokens]]]
		             [(ground [0 0]) [[?taskCount ?totalTokens]]])]`,

		"datom": `[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/name "Alice" 1]`,

		"datom-inst": `[#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" :person/created #inst "2025-01-15T10:30:00Z" 1]`,
	}

	// Set up EBNF parser (one-time cost, not benchmarked)
	ednGrammar := loadEDNGrammar(b)
	ebnfParser := parse.New(ednGrammar)

	for name, queryStr := range queries {
		b.Run("handrolled/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := ednpkg.Parse(queryStr)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("ebnf/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := ebnfParser.Parse(queryStr, "edn")
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("handrolled+query/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := ParseQuery(queryStr)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func loadEDNGrammar(b *testing.B) *ebnf.Grammar {
	b.Helper()
	g, err := ebnf.LoadGrammar("../../examples/edn.ebnf")
	if err != nil {
		// Try from repo root
		g, err = ebnf.LoadGrammar("../../../ebnf/examples/edn.ebnf")
		if err != nil {
			b.Skipf("EDN grammar not found: %v", err)
		}
	}
	return g
}

package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// Same-entity attribute fetch: pattern joins vs Pull
// =============================================================================
//
// Measures the cost of getting K attributes off an entity two ways:
//
//   patterns: [?e :place/type "room"] [?e :place/code ?a0] ... [?e :place/aK ?aK]
//             — anchor produces N entities; each extra same-?e pattern is its own
//               narrow (?e, vi) relation hash-joined on ?e. K attributes = K joins.
//
//   pull:     [:find (pull ?e [:place/code ... :place/aK]) :where [?e :place/type "room"]]
//             — anchor produces N entities; Pull fetches all K attributes per
//               entity in one go (no inter-attribute join).
//
// Both produce N tuples carrying the same K attribute values; the anchor is
// identical, so the patterns-minus-pull delta is the per-attribute join cost —
// the prize available to a same-entity attribute-fusion optimization.
//
// Run:
//   go test ./datalog/storage/ -run '^$' -bench BenchmarkAttrFetch -benchmem

// attrFetchAttrs are the CardinalityOne string attributes fetched off each
// entity (first K used per case).
var attrFetchAttrs = []string{"code", "name", "brief", "notes", "lore", "summary"}

func attrFetchSchema() *schema.Schema {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":place/type"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	for _, a := range attrFetchAttrs {
		s.Add(&schema.AttributeDefinition{
			Ident:       datalog.NewKeyword(":place/" + a),
			ValueType:   schema.TypeString,
			Cardinality: schema.CardinalityOne,
		})
	}
	return s
}

func attrFetchPatternsQuery(k int) string {
	var sb strings.Builder
	sb.WriteString("[:find ?e")
	for j := 0; j < k; j++ {
		fmt.Fprintf(&sb, " ?a%d", j)
	}
	sb.WriteString(` :where [?e :place/type "room"]`)
	for j := 0; j < k; j++ {
		fmt.Fprintf(&sb, " [?e :place/%s ?a%d]", attrFetchAttrs[j], j)
	}
	sb.WriteString("]")
	return sb.String()
}

func attrFetchPullQuery(k int) string {
	var sb strings.Builder
	sb.WriteString("[:find (pull ?e [")
	for j := 0; j < k; j++ {
		if j > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(":place/" + attrFetchAttrs[j])
	}
	sb.WriteString(`]) :where [?e :place/type "room"]]`)
	return sb.String()
}

func benchmarkAttrFetch(b *testing.B, n, k int, mode string, fusion bool) {
	opts := DefaultPlannerOptions()
	opts.EnableAttributeFetchFusion = fusion
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           b.TempDir(),
		Schema:         attrFetchSchema(),
		ReplicaID:      1,
		PlannerOptions: &opts,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	for i := 0; i < n; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("place-%d", i))
		if err := tx.Add(e, datalog.NewKeyword(":place/type"), "room"); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < k; j++ {
			if err := tx.Add(e, datalog.NewKeyword(":place/"+attrFetchAttrs[j]), fmt.Sprintf("v%d-%d", i, j)); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	var query string
	switch mode {
	case "patterns":
		query = attrFetchPatternsQuery(k)
	case "pull":
		query = attrFetchPullQuery(k)
	default:
		b.Fatalf("unknown mode %q", mode)
	}

	// Warm caches so the measured loop is steady-state.
	if got := countVBoundQuery(b, db, query); got != n {
		b.Fatalf("warmup: got %d tuples, want %d", got, n)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := countVBoundQuery(b, db, query); got != n {
			b.Fatalf("got %d tuples, want %d", got, n)
		}
	}
}

func BenchmarkAttrFetch(b *testing.B) {
	for _, n := range []int{100, 1000} {
		for _, k := range []int{1, 3, 6} {
			b.Run(fmt.Sprintf("N=%d/K=%d/patterns", n, k), func(b *testing.B) {
				benchmarkAttrFetch(b, n, k, "patterns", false)
			})
			b.Run(fmt.Sprintf("N=%d/K=%d/patterns-fused", n, k), func(b *testing.B) {
				benchmarkAttrFetch(b, n, k, "patterns", true)
			})
			b.Run(fmt.Sprintf("N=%d/K=%d/pull", n, k), func(b *testing.B) {
				benchmarkAttrFetch(b, n, k, "pull", false)
			})
		}
	}
}

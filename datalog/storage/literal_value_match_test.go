package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestLiteralValueMatchesPredicateForm guards against a regression where a
// query pattern with a literal value in V position returns a different
// multiset than the equivalent variable-bind + equality-predicate form.
//
// Concretely: for every supported value type, these two query shapes MUST
// return the same set of entities:
//
//	[:find ?e :where [?e :attr <literal>]]                      // pattern V
//	[:find ?e :where [?e :attr ?v] [(= ?v <literal>)]]          // predicate
//
// A version-skew bug observed in the wild surfaced as the literal-pattern
// form returning empty for keyword values while the predicate form found
// the entity. Semantic equivalence of these two shapes is load-bearing
// for query authoring, and the asymmetry can hide in any value type
// where the EDN parser's literal-handling diverges from the path taken
// by values bound via `:in` parameters.
//
// Coverage: Keyword, Identity (ref), String, Int64, Time. Each type is
// exercised both schemaless (default behavior) and schema-aware
// (attribute declared with the matching `:db.type/*`).
func TestLiteralValueMatchesPredicateForm(t *testing.T) {
	attr := datalog.NewKeyword(":entity/attr")

	// Stable identities so Identity literal text is reproducible.
	matchEntity := datalog.NewIdentity("entity:match-target")
	otherEntity := datalog.NewIdentity("entity:other-target")

	matchTime := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	otherTime := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		schemaType schema.ValueType
		match      interface{}
		other      interface{}
		literal    string // EDN text for the matching value in pattern V position
	}{
		{
			name:       "Keyword",
			schemaType: schema.TypeKeyword,
			match:      datalog.NewKeyword(":entity.type/dungeon"),
			other:      datalog.NewKeyword(":entity.type/town"),
			literal:    ":entity.type/dungeon",
		},
		{
			name:       "Identity",
			schemaType: schema.TypeRef,
			match:      matchEntity,
			other:      otherEntity,
			literal:    fmt.Sprintf(`#identity %q`, matchEntity.L85()),
		},
		{
			name:       "String",
			schemaType: schema.TypeString,
			match:      "dungeon",
			other:      "town",
			literal:    `"dungeon"`,
		},
		{
			name:       "Int64",
			schemaType: schema.TypeLong,
			match:      int64(42),
			other:      int64(99),
			literal:    "42",
		},
		{
			name:       "Time",
			schemaType: schema.TypeInstant,
			match:      matchTime,
			other:      otherTime,
			literal:    fmt.Sprintf(`#inst %q`, matchTime.Format(time.RFC3339Nano)),
		},
	}

	for _, tc := range cases {
		for _, schemaMode := range []string{"schemaless", "schema_aware"} {
			t.Run(tc.name+"/"+schemaMode, func(t *testing.T) {
				dir, err := os.MkdirTemp("", "literal-value-*")
				if err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(dir)

				var db *Database
				if schemaMode == "schema_aware" {
					sch := schema.NewSchema().Add(&schema.AttributeDefinition{
						Ident:       attr,
						ValueType:   tc.schemaType,
						Cardinality: schema.CardinalityOne,
					})
					db, err = NewDatabaseWithSchema(dir, sch)
				} else {
					db, err = NewDatabase(dir)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()

				// Insert two entities with the matching value and one with
				// the other value. Both query forms must find exactly the
				// two matching entities.
				match1 := datalog.NewIdentity("entity:match-1")
				match2 := datalog.NewIdentity("entity:match-2")
				nonMatch := datalog.NewIdentity("entity:nonmatch")

				tx := db.NewTransaction()
				tx.Add(match1, attr, tc.match)
				tx.Add(match2, attr, tc.match)
				tx.Add(nonMatch, attr, tc.other)
				if _, err := tx.Commit(); err != nil {
					t.Fatalf("commit failed: %v", err)
				}

				literalQuery := fmt.Sprintf(
					`[:find ?e :where [?e :entity/attr %s]]`,
					tc.literal,
				)
				predicateQuery := fmt.Sprintf(
					`[:find ?e :where [?e :entity/attr ?v] [(= ?v %s)]]`,
					tc.literal,
				)

				literalResult, err := executor.CollectTuples(db.Query(literalQuery))
				if err != nil {
					t.Fatalf("literal-pattern query failed: %v\nquery: %s",
						err, literalQuery)
				}
				predicateResult, err := executor.CollectTuples(db.Query(predicateQuery))
				if err != nil {
					t.Fatalf("predicate-form query failed: %v\nquery: %s",
						err, predicateQuery)
				}

				literalIDs := identitiesFromTuples(t, literalResult)
				predicateIDs := identitiesFromTuples(t, predicateResult)

				if !sameIdentitySet(literalIDs, predicateIDs) {
					t.Errorf("literal vs predicate form returned different "+
						"results.\n  literal-query: %s\n  predicate-query: %s"+
						"\n  literal results:   %v\n  predicate results: %v",
						literalQuery, predicateQuery, literalIDs, predicateIDs)
				}
				// Sanity: the predicate form must find both matches and
				// not the non-matching entity.
				if len(predicateIDs) != 2 {
					t.Errorf("predicate form should find 2 entities, got %d: %v",
						len(predicateIDs), predicateIDs)
				}
			})
		}
	}
}

func identitiesFromTuples(t *testing.T, tuples [][]interface{}) map[string]struct{} {
	t.Helper()
	out := make(map[string]struct{}, len(tuples))
	for _, tup := range tuples {
		if len(tup) == 0 {
			t.Fatalf("empty tuple in result")
		}
		switch v := tup[0].(type) {
		case datalog.Identity:
			out[v.L85()] = struct{}{}
		case *datalog.Identity:
			if v != nil {
				out[(*v).L85()] = struct{}{}
			}
		default:
			t.Fatalf("unexpected ?e type in result: %T", tup[0])
		}
	}
	return out
}

func sameIdentitySet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

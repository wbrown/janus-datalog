package storage

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// An unsigned integer within int64's range is the integer it equals, at a
// write and at a query input, since the engine's equality holds uint64(5)
// equal to int64(5). A value outside the domain, an unsigned integer above
// the range or a value of a type the domain has no place for, is refused
// where it enters, at a transaction's write and at a query's input
// conversion, naming the value or the type.

func TestAnUnsignedIntegerWithinRangeIsTheIntegerItEquals(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			signed := datalog.NewIdentity("u:signed")
			unsigned := datalog.NewIdentity("u:unsigned")
			attr := datalog.NewKeyword(":u/value")

			tx := db.NewTransaction()
			if err := tx.Add(signed, attr, int64(5)); err != nil {
				t.Fatal(err)
			}
			if err := tx.Add(unsigned, attr, uint64(5)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			t.Run("a query input finds both", func(t *testing.T) {
				tuples, err := executor.CollectTuples(db.Query(
					`[:find ?e :in $ ?v :where [?e :u/value ?v]]`,
					uint64(5),
				))
				if err != nil {
					t.Fatal(err)
				}
				assertEntitiesFound(t, tuples, signed, unsigned)
			})

			t.Run("a pattern constant finds both", func(t *testing.T) {
				tuples, err := executor.CollectTuples(db.Query(
					`[:find ?e :where [?e :u/value 5]]`,
				))
				if err != nil {
					t.Fatal(err)
				}
				assertEntitiesFound(t, tuples, signed, unsigned)
			})

			t.Run("the stored value reads back as int64", func(t *testing.T) {
				tuples, err := executor.CollectTuples(db.Query(
					`[:find ?v :in $ ?e :where [?e :u/value ?v]]`,
					unsigned,
				))
				if err != nil {
					t.Fatal(err)
				}
				if len(tuples) != 1 {
					t.Fatalf("expected 1 tuple, got %d", len(tuples))
				}
				if tuples[0][0] != int64(5) {
					t.Errorf("expected int64(5), got %#v", tuples[0][0])
				}
			})
		})
	}
}

func assertEntitiesFound(t *testing.T, tuples [][]interface{}, want ...datalog.Identity) {
	t.Helper()
	found := make(map[datalog.Identity]bool, len(tuples))
	for _, tuple := range tuples {
		found[tuple[0].(datalog.Identity)] = true
	}
	if len(found) != len(want) {
		t.Errorf("expected %d entities, found %d: %v", len(want), len(found), tuples)
	}
	for _, e := range want {
		if !found[e] {
			t.Errorf("entity %s not found", e)
		}
	}
}

func TestAValueOutsideTheDomainIsRefusedWhereItEnters(t *testing.T) {
	aboveRange := uint64(math.MaxInt64) + 1
	cases := []struct {
		name  string
		value interface{}
		named string
	}{
		{"an unsigned integer above int64's range", aboveRange, strconv.FormatUint(aboveRange, 10)},
		{"a float32", float32(1), "float32"},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			e := datalog.NewIdentity("u:1")
			attr := datalog.NewKeyword(":u/value")
			tx := db.NewTransaction()
			if err := tx.Add(e, attr, int64(1)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					tx := db.NewTransaction()
					defer tx.Rollback()

					writes := []struct {
						name  string
						write func() error
					}{
						{"Add", func() error { return tx.Add(e, attr, c.value) }},
						{"Set", func() error { return tx.Set(e, attr, c.value) }},
						{"Remove", func() error { return tx.Remove(e, attr, c.value) }},
						{"Retract", func() error { return tx.Retract(e, attr, c.value) }},
					}
					for _, w := range writes {
						err := w.write()
						if err == nil {
							t.Errorf("%s accepted %#v", w.name, c.value)
							continue
						}
						if !strings.Contains(err.Error(), c.named) {
							t.Errorf("%s's refusal should name %s; got: %v", w.name, c.named, err)
						}
					}

					_, err := executor.CollectTuples(db.Query(
						`[:find ?e :in $ ?v :where [?e :u/value ?v]]`,
						c.value,
					))
					if err == nil {
						t.Errorf("the query accepted the input %#v", c.value)
					} else if !strings.Contains(err.Error(), c.named) {
						t.Errorf("the input's refusal should name %s; got: %v", c.named, err)
					}
				})
			}
		})
	}
}

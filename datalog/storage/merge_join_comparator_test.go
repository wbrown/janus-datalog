package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Merge join is restricted to entity-position join keys. The merge advance is
// only correct when three orders agree: the binding sort (CompareValues via
// Sorted()), the advance comparator, and the storage scan order of the probe
// stream. That agreement is provable for Identity keys — probe datoms arrive
// in hash-byte order, which is exactly Identity's CompareValues order — and
// deliberately does not hold for value-position keys, whose on-disk type-tag
// order differs from CompareValues' rank order (see datalog/compare.go).

// TestMergeJoinMixedEntityBindingsMatchOnlyIdentities pins the advance
// comparator's cross-type behavior: non-Identity values in an entity-position
// binding position sort before every Identity under the canonical rank order and
// never compare equal to a probe — including a string that is exactly an
// entity's L85 text, which the old Sprintf fallback compared equal.
func TestMergeJoinMixedEntityBindingsMatchOnlyIdentities(t *testing.T) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	alice := datalog.NewIdentity("user:alice")
	bob := datalog.NewIdentity("user:bob")
	nameAttr := datalog.NewKeyword(":user/name")
	tx := db.NewTransaction()
	tx.Add(alice, nameAttr, "Alice")
	tx.Add(bob, nameAttr, "Bob")
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	matcher := NewPatternMatcher(db.Store())
	strategy := MergeJoin
	matcher.ForceJoinStrategy(&strategy)
	defer matcher.ForceJoinStrategy(nil)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: nameAttr},
			query.Variable{Name: datalog.NewSymbol("?n")},
		},
	}
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e")},
		[]executor.Tuple{
			{alice},
			{"user:alice"}, // seed text: never an entity
			{alice.L85()},  // L85 text of a real entity: never an entity either
			{bob},
		},
	)

	tuples, err := executor.CollectTuples(matcher.MatchWithConstraints(
		query.PatternQuery(pattern),
		executor.Relations{bindingRel},
		nil,
	))
	if err != nil {
		t.Fatalf("mixed entity bindings must join, not error: %v", err)
	}
	if len(tuples) != 2 {
		t.Fatalf("expected exactly the 2 Identity-bound tuples, got %d: %v", len(tuples), tuples)
	}
	for _, tuple := range tuples {
		e, ok := tuple[0].(datalog.Identity)
		if !ok {
			t.Fatalf("expected Identity in entity position, got %T", tuple[0])
		}
		if !e.Equal(alice) && !e.Equal(bob) {
			t.Errorf("unexpected entity %s in result", e)
		}
	}
}

// TestChooseJoinStrategySelectsMergeJoinOnlyForEntityPosition pins the
// provable restriction: high-selectivity large binding sets take merge join
// for the entity position and the order-free hash join everywhere else.
func TestChooseJoinStrategySelectsMergeJoinOnlyForEntityPosition(t *testing.T) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	nameAttr := datalog.NewKeyword(":user/name")
	tx := db.NewTransaction()
	tx.Add(datalog.NewIdentity("user:alice"), nameAttr, "Alice")
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	matcher := NewPatternMatcher(db.Store())

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: nameAttr},
			query.Variable{Name: datalog.NewSymbol("?n")},
		},
	}

	// estimatePatternCardinality returns a fixed 10K for A-constant patterns,
	// so >5000 bindings puts selectivity above the 50% merge-join threshold.
	tuples := make([]executor.Tuple, 0, 6000)
	for i := 0; i < 6000; i++ {
		tuples = append(tuples, executor.Tuple{datalog.NewIdentity(fmt.Sprintf("user:%d", i))})
	}
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?e")},
		tuples,
	)

	if got := matcher.chooseJoinStrategy(pattern, bindingRel, 0); got != MergeJoin {
		t.Errorf("entity position with large high-selectivity bindings should merge join, got %v", got)
	}
	if got := matcher.chooseJoinStrategy(pattern, bindingRel, 2); got != HashJoinScan {
		t.Errorf("value position must never merge join (scan order is type-tag order, not rank order), got %v", got)
	}
}

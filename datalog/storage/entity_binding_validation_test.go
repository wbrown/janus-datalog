package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Entity-position typing is enforced at the two user boundaries — query-text
// constants (validated at match entry) and :in inputs (validated at query
// entry). Binding relations reaching the matcher are interior data flow: a
// non-Identity value bound into entity position names no entity, so it
// contributes no tuples — the typed non-match of the equality join — never an
// error and never a panic. Every join strategy must agree on that result,
// because the strategy chosen is an optimization detail.

func TestBoundEntityNonMatchAgreesAcrossJoinStrategies(t *testing.T) {
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

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: nameAttr},
			query.Variable{Name: datalog.NewSymbol("?n")},
		},
	}
	// Mixed entity position: two real entities, a seed string, and the L85 text of a
	// real entity. Only the identities may join.
	mixedBindings := func() executor.Relation {
		return executor.NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?e")},
			[]executor.Tuple{
				{alice},
				{"user:alice"},
				{alice.L85()},
				{bob},
			},
		)
	}

	assertIdentityTuplesOnly := func(t *testing.T, rel executor.Relation, err error) {
		t.Helper()
		tuples, err := executor.CollectTuples(rel, err)
		if err != nil {
			t.Fatalf("interior mixed bindings must join, not error: %v", err)
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

	for _, strategy := range []JoinStrategy{HashJoinScan, MergeJoin} {
		strategy := strategy
		t.Run(strategy.String(), func(t *testing.T) {
			matcher.ForceJoinStrategy(&strategy)
			defer matcher.ForceJoinStrategy(nil)

			rel, err := matcher.MatchWithConstraints(
				query.PatternQuery(pattern),
				executor.Relations{mixedBindings()},
				nil,
			)
			assertIdentityTuplesOnly(t, rel, err)
		})
	}

	// Multi-position bindings take the NoReuse path (nonReusingIterator).
	t.Run("NoReuse", func(t *testing.T) {
		multiRel := executor.NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?n")},
			[]executor.Tuple{
				{alice, "Alice"},
				{"user:alice", "Alice"},
				{alice.L85(), "Alice"},
				{bob, "Bob"},
			},
		)
		rel, err := matcher.MatchWithConstraints(
			query.PatternQuery(pattern),
			executor.Relations{multiRel},
			nil,
		)
		assertIdentityTuplesOnly(t, rel, err)
	})
}

// TestVEJoinOverMixedDataMatchesOnlyRefs pins the language-level shape that
// produces mixed entity bindings: a value joined from V position into E
// position over schemaless data holding both refs and strings — including
// strings that are the L85 text of real entities. The join keeps ref-partnered
// tuples and drops the rest; no error.
func TestVEJoinOverMixedDataMatchesOnlyRefs(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			alice := datalog.NewIdentity("user:alice")
			group := datalog.NewIdentity("group:admins")
			refAttr := datalog.NewKeyword(":user/group")
			nameAttr := datalog.NewKeyword(":group/name")

			tx := db.NewTransaction()
			tx.Add(group, nameAttr, "Admins")
			tx.Add(alice, refAttr, group)                                    // a real ref
			tx.Add(datalog.NewIdentity("user:bob"), refAttr, "group:admins") // a seed string
			tx.Add(datalog.NewIdentity("user:carol"), refAttr, group.L85())  // an L85 text
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			tuples, err := executor.CollectTuples(db.Query(
				`[:find ?name :where [?u :user/group ?g] [?g :group/name ?name]]`,
			))
			if err != nil {
				t.Fatalf("mixed V→E join must not error: %v", err)
			}
			if len(tuples) != 1 {
				t.Fatalf("expected 1 tuple (only the real ref joins), got %d: %v", len(tuples), tuples)
			}
			if name, ok := tuples[0][0].(string); !ok || name != "Admins" {
				t.Errorf("expected \"Admins\", got %v", tuples[0][0])
			}
		})
	}
}

// TestBoundEntityNonMatchOnValuePositionJoin pins the contract when the join
// lands on the V position while the binding relation also binds E: the probe
// verifies non-join positions from the binding tuple, and a non-Identity value
// in entity position is a typed non-match — that tuple contributes no tuples,
// never an error and never a panic. The seek strategies drop such tuples at
// construction; the hash join must agree.
func TestBoundEntityNonMatchOnValuePositionJoin(t *testing.T) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")
	tx := db.NewTransaction()
	tx.Add(alice, nameAttr, "Alice")
	tx.Add(datalog.NewIdentity("user:bob"), nameAttr, "Bob")
	tx.Add(datalog.NewIdentity("user:carol"), nameAttr, "Carol")
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
	// ?e and ?n both bind pattern positions with equal distinct counts, so the
	// multi-position strategy joins on V and verifies E from each binding
	// tuple. Only the Identity-bound tuple may join.
	mixedBindings := func() executor.Relation {
		return executor.NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?n")},
			[]executor.Tuple{
				{alice, "Alice"},
				{"user:alice", "Carol"},
				{alice.L85(), "Bob"},
			},
		)
	}

	for _, strategy := range []JoinStrategy{HashJoinScan} {
		strategy := strategy
		t.Run(strategy.String(), func(t *testing.T) {
			matcher.ForceJoinStrategy(&strategy)
			defer matcher.ForceJoinStrategy(nil)

			rel, err := matcher.MatchWithConstraints(
				query.PatternQuery(pattern),
				executor.Relations{mixedBindings()},
				nil,
			)
			tuples, err := executor.CollectTuples(rel, err)
			if err != nil {
				t.Fatalf("interior mixed bindings must join, not error: %v", err)
			}
			if len(tuples) != 1 {
				t.Fatalf("expected exactly the Identity-bound tuple, got %d: %v", len(tuples), tuples)
			}
			if e, ok := tuples[0][0].(datalog.Identity); !ok || !e.Equal(alice) {
				t.Errorf("expected alice in entity position, got %v", tuples[0][0])
			}
			if n, ok := tuples[0][1].(string); !ok || n != "Alice" {
				t.Errorf("expected \"Alice\", got %v", tuples[0][1])
			}
		})
	}
}

// TestBoundAttributeNonMatchAgreesAcrossJoinStrategies pins the A-position
// analogue: an E-position join whose binding tuples also bind the attribute
// variable treats a non-Keyword attribute value as a typed non-match on every
// join strategy — no tuples from that tuple, never an error and never a panic.
func TestBoundAttributeNonMatchAgreesAcrossJoinStrategies(t *testing.T) {
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

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Variable{Name: datalog.NewSymbol("?a")},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	// ?e has three distinct values to ?a's two, so the strategy joins on E and
	// verifies A from each binding tuple. The string in attribute position and
	// the L85 text in entity position both name nothing; only bob's tuple joins.
	mixedBindings := func() executor.Relation {
		return executor.NewMaterializedRelation(
			[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?a")},
			[]executor.Tuple{
				{alice, "not-a-keyword"},
				{bob, nameAttr},
				{bob.L85(), nameAttr},
			},
		)
	}

	for _, strategy := range []JoinStrategy{HashJoinScan, MergeJoin} {
		strategy := strategy
		t.Run(strategy.String(), func(t *testing.T) {
			matcher.ForceJoinStrategy(&strategy)
			defer matcher.ForceJoinStrategy(nil)

			rel, err := matcher.MatchWithConstraints(
				query.PatternQuery(pattern),
				executor.Relations{mixedBindings()},
				nil,
			)
			tuples, err := executor.CollectTuples(rel, err)
			if err != nil {
				t.Fatalf("interior mixed bindings must join, not error: %v", err)
			}
			if len(tuples) != 1 {
				t.Fatalf("expected exactly bob's tuple, got %d: %v", len(tuples), tuples)
			}
			if e, ok := tuples[0][0].(datalog.Identity); !ok || !e.Equal(bob) {
				t.Errorf("expected bob in entity position, got %v", tuples[0][0])
			}
			if a, ok := tuples[0][1].(datalog.Keyword); !ok || a != nameAttr {
				t.Errorf("expected :user/name in attribute position, got %v", tuples[0][1])
			}
			if v, ok := tuples[0][2].(string); !ok || v != "Bob" {
				t.Errorf("expected \"Bob\", got %v", tuples[0][2])
			}
		})
	}
}

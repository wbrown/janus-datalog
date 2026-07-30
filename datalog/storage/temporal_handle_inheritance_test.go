package storage

import (
	"reflect"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// A temporal handle is the parent database with a pinned transaction view and
// its own cache scope — every other field is the parent's. These tests pin
// that contract two ways: behaviorally for the fields whose loss is
// observable, and structurally for the whole field set, so a new Database
// field cannot silently ship unclassified (the constructor-drops-new-fields
// class produced BUG_TEMPORAL_DATABASE_HANDLES_ARE_SHALLOW (resolved) and
// then regenerated as docs/bugs/BUG_TEMPORAL_HANDLES_DROP_PLANNER_OPTIONS.md
// — plannerOptions was named in the first entry and still fell through).

// TestTemporalHandlesInheritPlannerOptions pins the options-threading
// contract: a handle derived from a configured database observes the parent's
// planner options, not the defaults.
func TestTemporalHandlesInheritPlannerOptions(t *testing.T) {
	popts := DefaultPlannerOptions()
	popts.EnableAlgebraOptimizer = false
	popts.EnableScanSharing = !popts.EnableScanSharing

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		PlannerOptions: &popts,
	})
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// DeepEqual rather than ==: PlannerOptions carries Handler, a func, and a
	// struct holding one is not comparable. This database registers no handler,
	// and DeepEqual holds two nil funcs equal, so the comparison is exact.
	want := db.effectivePlannerOptions()
	asOf := db.AsOf(datalog.ElementID{Lamport: 1, ReplicaID: 1})
	if got := asOf.effectivePlannerOptions(); !reflect.DeepEqual(got, want) {
		t.Errorf("AsOf handle dropped the parent's planner options:\n  parent: %+v\n  handle: %+v", want, got)
	}
	hist := db.History()
	if got := hist.effectivePlannerOptions(); !reflect.DeepEqual(got, want) {
		t.Errorf("History handle dropped the parent's planner options:\n  parent: %+v\n  handle: %+v", want, got)
	}
}

// temporalFieldClass is the ruled disposition of one Database field on a
// derived temporal handle.
type temporalFieldClass int

const (
	// fieldInherited: the handle carries the parent's value.
	fieldInherited temporalFieldClass = iota
	// fieldZeroed: deliberately zero on the handle — write-path and
	// lifecycle coordination state that temporal (read-only) handles must
	// not share or use. NewTransaction panics before any of it is touched.
	fieldZeroed
	// fieldPerHandle: the handle sets its own value by design (documented
	// per field below).
	fieldPerHandle
)

// temporalFieldContract classifies every Database field. A field missing
// from this table fails TestTemporalHandleFieldClassification: adding a
// Database field requires deciding, here, what AsOf/History do with it.
var temporalFieldContract = map[string]temporalFieldClass{
	"store":          fieldInherited,
	"encoder":        fieldInherited,
	"planCache":      fieldInherited,
	"parseCache":     fieldInherited, // shared parse results are immutable post-parse
	"schema":         fieldInherited,
	"plannerOptions": fieldInherited, // carries the handler; the handle inherits both together
	"clock":          fieldInherited,
	"replicaID":      fieldInherited,

	"txCounter":          fieldZeroed, // write path; atomic, non-copyable
	"mu":                 fieldZeroed, // lock; non-copyable
	"activeTx":           fieldZeroed, // writes prohibited on temporal handles
	"onCommitWindow":     fieldZeroed, // test-only commit-window hook (write path)
	"rollbackInProgress": fieldZeroed, // rollback coordination (write path)
	"drainCond":          fieldZeroed,
	"rollbackMu":         fieldZeroed,

	// builderCache is snapshot-independent (keyed on pattern structure), so
	// handles share the parent's builder population.
	"builderCache": fieldInherited,

	// temporalTxID is the handle's defining value: AsOf pins the snapshot,
	// History pins the zero ElementID sentinel.
	"temporalTxID": fieldPerHandle,
	// cache is deliberately divergent: AsOf owns a private snapshot-scoped
	// EA cache (immutable view, fills lazily, freed with the handle);
	// History shares the parent's cache. Asserted behaviorally below.
	"cache": fieldPerHandle,
}

// TestTemporalHandleFieldClassification enumerates Database's fields against
// the contract table (structural completeness) and asserts the inherited and
// per-handle dispositions behaviorally where they are observable.
func TestTemporalHandleFieldClassification(t *testing.T) {
	dbType := reflect.TypeOf(Database{})
	for i := 0; i < dbType.NumField(); i++ {
		name := dbType.Field(i).Name
		if _, classified := temporalFieldContract[name]; !classified {
			t.Errorf("Database field %q is not classified in temporalFieldContract: decide what AsOf/History do with it (inherit, zero, or per-handle) and add it to the table", name)
		}
	}
	for name := range temporalFieldContract {
		if _, exists := dbType.FieldByName(name); !exists {
			t.Errorf("temporalFieldContract entry %q no longer matches a Database field; remove or rename it", name)
		}
	}

	popts := DefaultPlannerOptions()
	popts.EnableAlgebraOptimizer = false

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		PlannerOptions: &popts,
	})
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	for handleName, handle := range map[string]*Database{
		"AsOf":    db.AsOf(datalog.ElementID{Lamport: 1, ReplicaID: 1}),
		"History": db.History(),
	} {
		if handle.store != db.store {
			t.Errorf("%s: store not inherited", handleName)
		}
		if handle.encoder != db.encoder {
			t.Errorf("%s: encoder not inherited", handleName)
		}
		if handle.planCache != db.planCache {
			t.Errorf("%s: planCache not inherited", handleName)
		}
		if handle.parseCache != db.parseCache {
			t.Errorf("%s: parseCache not inherited", handleName)
		}
		if handle.schema != db.schema {
			t.Errorf("%s: schema not inherited", handleName)
		}
		if handle.plannerOptions != db.plannerOptions {
			t.Errorf("%s: plannerOptions not inherited", handleName)
		}
		if handle.clock != db.clock {
			t.Errorf("%s: clock not inherited", handleName)
		}
		if handle.replicaID != db.replicaID {
			t.Errorf("%s: replicaID not inherited", handleName)
		}
		if handle.builderCache != db.builderCache {
			t.Errorf("%s: builderCache not inherited", handleName)
		}
		if handle.temporalTxID == nil {
			t.Errorf("%s: temporalTxID not set — the handle's defining field", handleName)
		}
		if handle.activeTx != nil {
			t.Errorf("%s: activeTx must be zero on a read-only temporal handle", handleName)
		}
	}

	if asOf := db.AsOf(datalog.ElementID{Lamport: 1, ReplicaID: 1}); asOf.cache == db.cache || asOf.cache == nil {
		t.Error("AsOf: cache must be a fresh snapshot-scoped cache, not the parent's and not nil")
	}
	if hist := db.History(); hist.cache != db.cache {
		t.Error("History: cache must be the parent's (shared)")
	}
}

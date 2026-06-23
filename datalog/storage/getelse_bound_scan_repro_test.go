package storage

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// indexForAttrPattern returns the index chosen for the pattern/index-selection
// event whose pattern mentions attrFragment, plus the scan range. Empty index
// means no scan was issued for that attribute (e.g. a fused per-entity lookup).
func indexForAttrPattern(events []annotations.Event, attrFragment string) (index, scanStart, scanEnd string) {
	for _, e := range events {
		if e.Name != "pattern/index-selection" {
			continue
		}
		p, _ := e.Data["pattern"].(string)
		if !strings.Contains(p, attrFragment) {
			continue
		}
		index = fmt.Sprint(e.Data["index"])
		scanStart = fmt.Sprint(e.Data["scan.start"])
		scanEnd = fmt.Sprint(e.Data["scan.end"])
	}
	return index, scanStart, scanEnd
}

// TestGetElseBoundEntityScanNotNarrowed reproduces the performance bug in
// docs/bugs/BUG_GETELSE_SCAN_REWRITE_NOT_NARROWED_BY_BOUND_CHILD.md.
//
// get-else on a single bound entity is rewritten to LeftOuterJoin + Scan, and
// the produced Scan([?e :repro/note ?note]) is driven with ?e FREE — so it
// enumerates the entire :repro/note extent (an attribute-primary index, AETV)
// and is only narrowed by the join on ?e afterward. For a singleton child this
// should instead be an EATV point lookup, identical in cost to the plain
// [?e :repro/note ?note] pattern.
//
// The assertion is on the mechanism (which index the :repro/note scan uses),
// not wall-clock, so it is not flaky. The wall-clock contrast against the plain
// pattern is logged to show the human-visible impact, which scales with the
// :repro/note extent rather than with the (singleton) result.
func TestGetElseBoundEntityScanNotNarrowed(t *testing.T) {
	dir, err := os.MkdirTemp("", "getelse-narrowing-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	defer db.Close()

	// Inflate the :repro/note extent: every entity carries the optional field.
	// Only the target additionally carries the anchor attribute :repro/kind, so
	// the child relation [?e :repro/kind _] is a singleton — N=1 point lookup is
	// exactly what get-else should cost here.
	const extent = 3000
	kind := datalog.NewKeyword(":repro/kind")
	note := datalog.NewKeyword(":repro/note")

	tx := db.NewTransaction()
	var target datalog.Identity
	for i := 0; i < extent; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("repro-e-%d", i))
		if err := tx.Add(e, note, fmt.Sprintf("note-%d", i)); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			target = e
			if err := tx.Add(e, kind, "task"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	captureQuery := func(q string, args ...interface{}) (results [][]interface{}, dur time.Duration, events []annotations.Event) {
		db.SetAnnotationHandler(func(e annotations.Event) {
			events = append(events, e)
		})
		start := time.Now()
		results, err := executor.CollectTuples(db.Query(q, args...))
		dur = time.Since(start)
		db.SetAnnotationHandler(nil)
		if err != nil {
			t.Fatalf("query failed: %v\nquery: %s", err, q)
		}
		return results, dur, events
	}

	// Baseline: plain pattern read of the same optional field on the same bound
	// entity. ?e is bound by [?e :repro/kind _], so [?e :repro/note ?note] is a
	// point lookup. This is the cost get-else should match.
	plainResults, plainDur, plainEvents := captureQuery(
		`[:find ?note
		  :in $ ?e
		  :where [?e :repro/kind _]
		         [?e :repro/note ?note]]`,
		target,
	)
	if len(plainResults) != 1 || plainResults[0][0] != "note-0" {
		t.Fatalf("plain: expected [[note-0]], got %v", plainResults)
	}
	plainIdx, _, _ := indexForAttrPattern(plainEvents, ":repro/note")
	t.Logf("plain    [?e :repro/note ?note]: index=%s  wall=%s  results=%d",
		plainIdx, plainDur, len(plainResults))

	// get-else read of the same optional field on the same bound entity.
	geResults, geDur, geEvents := captureQuery(
		`[:find ?note
		  :in $ ?e
		  :where [?e :repro/kind _]
		         [(get-else $ ?e :repro/note "none") ?note]]`,
		target,
	)
	if len(geResults) != 1 || geResults[0][0] != "note-0" {
		t.Fatalf("get-else: expected [[note-0]], got %v", geResults)
	}
	geIdx, geStart, geEnd := indexForAttrPattern(geEvents, ":repro/note")
	t.Logf("get-else (get-else $ ?e :repro/note): index=%s  wall=%s  results=%d",
		geIdx, geDur, len(geResults))
	t.Logf(":repro/note extent=%d datoms; get-else scan range [%s, %s)", extent, geStart, geEnd)
	if plainDur > 0 {
		t.Logf("get-else / plain wall-time ratio: %.1fx", float64(geDur)/float64(plainDur))
	}

	// The plain pattern, on a bound ?e, must NOT full-scan :repro/note. In
	// practice it emits no :repro/note scan event at all (empty index) because
	// the executor's tryFuseAttributeFetch serves it as a per-entity, cache-
	// backed LookupAttribute — a pure point lookup. That (or an EATV-narrowed
	// scan) is the cost get-else should match.
	attrPrimary := map[string]bool{"AETV": true, "AEVT": true, "AVET": true, "ATEV": true}
	if attrPrimary[plainIdx] {
		t.Fatalf("setup invalid: the plain pattern itself full-scanned :repro/note "+
			"via %s; the baseline is supposed to be a bound-entity point lookup", plainIdx)
	}

	// The bug: get-else scans :repro/note via an attribute-primary index (the
	// whole extent) instead of narrowing to the single bound ?e. When fixed, the
	// :repro/note scan should be narrowed to ?e (EATV) or not scan-matched at all
	// (fused per-entity lookup → empty index, matching the plain baseline).
	if attrPrimary[geIdx] {
		t.Fatalf("BUG REPRODUCED: get-else scanned :repro/note via attribute-primary "+
			"index %s — the full extent of %d datoms — for a single bound entity; "+
			"the equivalent plain pattern used %s (point lookup). "+
			"get-else on a bound entity should be a point lookup, not a full attribute scan.",
			geIdx, extent, plainIdx)
	}
}

// setupGetElseNarrowingDB builds a database for the multi-entity / differential
// get-else narrowing tests: five "kind" entities (the first three carry the
// optional :repro/note, the last two don't → default), plus a large block of
// filler entities that carry :repro/note but no :repro/kind. The filler inflates
// the :repro/note extent so a full-attribute scan is observably different from a
// narrowed, entity-bound scan, and the filler must never appear in results.
//
// Identities are interned by hash, so the returned `want` keys (fresh
// NewIdentity values) are the same pointers as the entities read back from
// storage — safe to use as map keys for result assertions.
func setupGetElseNarrowingDB(t *testing.T) (db *Database, want map[datalog.Identity]string, cleanup func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "getelse-narrowing-*")
	if err != nil {
		t.Fatal(err)
	}
	db, err = NewDatabase(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("create db: %v", err)
	}

	kind := datalog.NewKeyword(":repro/kind")
	note := datalog.NewKeyword(":repro/note")

	tx := db.NewTransaction()
	want = make(map[datalog.Identity]string)

	for i := 0; i < 5; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("narrow-kind-%d", i))
		if err := tx.Add(e, kind, "task"); err != nil {
			t.Fatal(err)
		}
		if i < 3 {
			v := fmt.Sprintf("note-%d", i)
			if err := tx.Add(e, note, v); err != nil {
				t.Fatal(err)
			}
			want[e] = v
		} else {
			want[e] = "MISSING"
		}
	}

	// Filler: :repro/note without :repro/kind. Never in the result; a narrowed
	// scan must not touch it.
	for i := 0; i < 1500; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("narrow-filler-%d", i))
		if err := tx.Add(e, note, fmt.Sprintf("filler-note-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	cleanup = func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, want, cleanup
}

// TestGetElseMultiEntityScanNarrowed confirms the fix isn't singleton-only: a
// get-else over several pattern-bound entities returns correct values (stored
// note or default) AND narrows the :repro/note access to the bound entities
// instead of scanning the whole attribute extent.
func TestGetElseMultiEntityScanNarrowed(t *testing.T) {
	db, want, cleanup := setupGetElseNarrowingDB(t)
	defer cleanup()

	var events []annotations.Event
	db.SetAnnotationHandler(func(e annotations.Event) { events = append(events, e) })
	results, err := executor.CollectTuples(db.Query(
		`[:find ?e ?note
		  :where [?e :repro/kind _]
		         [(get-else $ ?e :repro/note "MISSING") ?note]]`,
	))
	db.SetAnnotationHandler(nil)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// One row per kind-bearing entity, each with its stored note or the default.
	got := make(map[datalog.Identity]string, len(results))
	for _, row := range results {
		e, ok := row[0].(datalog.Identity)
		if !ok {
			t.Fatalf("expected Identity in ?e, got %T", row[0])
		}
		n, ok := row[1].(string)
		if !ok {
			t.Fatalf("expected string in ?note, got %T", row[1])
		}
		got[e] = n
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d rows, got %d: %v", len(want), len(got), results)
	}
	for e, w := range want {
		if got[e] != w {
			t.Errorf("entity %v: expected note %q, got %q", e, w, got[e])
		}
	}

	// Bound matcher paths emit storage/reuse-strategy or cache events, never the
	// unbound pattern/index-selection (emitted only by matchUnboundAsRelation —
	// the full attribute scan). So an attribute-primary index-selection on
	// :repro/note means the scan was not narrowed to the bound entities.
	// Primary, direct assertion: the or-fallback branch reports it narrowed the
	// scan to the bound join keys (one per kind-bearing entity), rather than
	// silently falling back to a full attribute scan.
	ev := narrowingEvent(events)
	if ev == nil {
		t.Fatalf("expected an or-fallback/branch.narrowed annotation; none emitted")
	}
	if narrowed, _ := ev.Data["narrowed"].(bool); !narrowed {
		t.Fatalf("get-else branch was NOT narrowed to the bound entities (fell back to a full scan): %v", ev.Data)
	}
	if jk, _ := ev.Data["join_keys"].(int); jk != len(want) {
		t.Errorf("expected join_keys=%d (one per bound entity), got %v", len(want), ev.Data["join_keys"])
	}

	// Defense in depth: a narrowed branch takes the bound matcher path (cache /
	// reuse strategy), which never emits the unbound pattern/index-selection that
	// matchUnboundAsRelation (the full attribute scan) emits.
	idx, _, _ := indexForAttrPattern(events, ":repro/note")
	attrPrimary := map[string]bool{"AETV": true, "AEVT": true, "AVET": true, "ATEV": true}
	if attrPrimary[idx] {
		t.Fatalf("get-else over %d bound entities scanned :repro/note via attribute-primary "+
			"index %s (full extent) instead of narrowing to the bound entities", len(want), idx)
	}
}

// narrowingEvent returns the or-fallback/branch.narrowed annotation emitted when
// a cacheable DataPattern-only or-join branch (the get-else scan branch) decides
// whether to narrow its single evaluation to the outer join keys. Returns nil if
// no such event was emitted.
func narrowingEvent(events []annotations.Event) *annotations.Event {
	for i := range events {
		if events[i].Name == "or-fallback/branch.narrowed" {
			return &events[i]
		}
	}
	return nil
}

// TestGetElseScanNarrowing_SemanticPreservation is the differential / structural
// test the project's optimization-testing guidance calls for: the rewrite +
// narrowing (algebra optimizer ON) must return exactly the same (?e, ?note)
// rows — defaults included — as the un-rewritten, per-tuple get-else (algebra
// optimizer OFF).
func TestGetElseScanNarrowing_SemanticPreservation(t *testing.T) {
	db, _, cleanup := setupGetElseNarrowingDB(t)
	defer cleanup()

	q := `[:find ?e ?note
	       :where [?e :repro/kind _]
	              [(get-else $ ?e :repro/note "MISSING") ?note]]`

	db.ClearPlanCache()
	baseRel, err := queryWithoutAlgebra(db, q)
	if err != nil {
		t.Fatalf("baseline (optimizer off) failed: %v", err)
	}
	baseline, err := executor.CollectTuples(baseRel, nil)
	if err != nil {
		t.Fatal(err)
	}

	db.ClearPlanCache()
	optRel, err := queryWithAlgebra(db, q)
	if err != nil {
		t.Fatalf("optimized (optimizer on + narrowing) failed: %v", err)
	}
	optimized, err := executor.CollectTuples(optRel, nil)
	if err != nil {
		t.Fatal(err)
	}

	norm := func(rows [][]interface{}) map[datalog.Identity]string {
		m := make(map[datalog.Identity]string, len(rows))
		for _, r := range rows {
			m[r[0].(datalog.Identity)] = r[1].(string)
		}
		return m
	}
	baseMap, optMap := norm(baseline), norm(optimized)
	if len(baseMap) != len(optMap) {
		t.Fatalf("row count differs: optimizer-off=%d optimizer-on=%d", len(baseMap), len(optMap))
	}
	for e, bv := range baseMap {
		if ov, ok := optMap[e]; !ok || ov != bv {
			t.Errorf("entity %v: optimizer-off=%q optimizer-on=%q (present=%v)", e, bv, ov, ok)
		}
	}
}
